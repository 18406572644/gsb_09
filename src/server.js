'use strict';

const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { WebSocketServer } = require('ws');

const { ChatDB } = require('./db');
const { Hub, Connection } = require('./hub');
const defaultConfig = require('./config');
const {
  randomId,
  randomSecret,
  signToken,
  verifyToken,
  isNonEmptyString,
  parseFrame,
  now,
} = require('./util');

/** 业务错误：handler 抛出，统一转成 error 帧回给客户端 */
class ChatError extends Error {
  constructor(code, message) {
    super(message);
    this.code = code;
  }
}
const fail = (code, message) => {
  throw new ChatError(code, message);
};

/** 令牌桶限流（按用户），防刷屏 */
class TokenBucket {
  constructor(ratePerSec, burst) {
    this.rate = ratePerSec;
    this.burst = burst;
    this.buckets = new Map();
  }
  take(key) {
    const t = now();
    let b = this.buckets.get(key);
    if (!b) {
      b = { tokens: this.burst, updated: t };
      this.buckets.set(key, b);
    }
    b.tokens = Math.min(this.burst, b.tokens + ((t - b.updated) / 1000) * this.rate);
    b.updated = t;
    if (b.tokens < 1) return false;
    b.tokens -= 1;
    return true;
  }
}

/** 数据库消息行 -> 下发帧 */
function msgFrame(m) {
  return {
    type: 'msg',
    roomId: m.roomId,
    seq: m.seq,
    clientMsgId: m.clientMsgId,
    from: m.from,
    fromName: m.fromName,
    content: m.content,
    ts: m.ts,
  };
}

/** 角色权限矩阵默认值：permissions 为每群可覆盖的「权限点 -> 允许角色数组」 */
const DEFAULT_PERMISSIONS = {
  sendMessage: ['owner', 'admin', 'member'],
  inviteMembers: ['owner', 'admin', 'member'],
  editProfile: ['owner', 'admin'],
  manageMembers: ['owner', 'admin'],
  postAnnouncement: ['owner', 'admin'],
};
const ROLES = ['owner', 'admin', 'member'];
const JOIN_MODES = ['public', 'approval', 'private'];

function roomPerms(room) {
  return { ...DEFAULT_PERMISSIONS, ...JSON.parse(room.permissions || '{}') };
}

/** owner 拥有一切；其余角色必须出现在该权限点的角色数组中 */
function can(room, role, perm) {
  if (role === 'owner') return true;
  const list = roomPerms(room)[perm];
  return Array.isArray(list) ? list.includes(role) : false;
}

/** 角色等级：数字越大权限越高，用于禁言/踢人的越权保护 */
const ROLE_RANK = { owner: 3, admin: 2, member: 1 };

/**
 * 规范化权限矩阵：只接受已知权限点，值为 ROLES 的去重子集。
 * 非法输入直接 BAD_REQUEST（而非静默丢弃），便于客户端发现配置错误。
 */
function sanitizePermissions(input) {
  if (input === undefined || input === null) return undefined;
  if (typeof input !== 'object') fail('BAD_REQUEST', 'permissions must be an object');
  const out = {};
  for (const [perm, roles] of Object.entries(input)) {
    if (!Object.prototype.hasOwnProperty.call(DEFAULT_PERMISSIONS, perm)) {
      fail('BAD_REQUEST', `unknown permission: ${perm}`);
    }
    if (!Array.isArray(roles)) fail('BAD_REQUEST', `permission ${perm} must be an array of roles`);
    const clean = [...new Set(roles)].filter((r) => ROLES.includes(r));
    if (clean.length === 0) fail('BAD_REQUEST', `permission ${perm} must allow at least one role`);
    out[perm] = clean;
  }
  return out;
}

function createChatServer(overrides = {}) {
  const config = { ...defaultConfig, ...overrides };
  const db = new ChatDB(config.dbPath);
  const hub = new Hub(config);
  const limiter = new TokenBucket(config.rateLimitPerSec, config.rateLimitBurst);
  const publicDir = path.join(__dirname, '..', 'public');

  // ---------------------------------------------------------------- 消息处理

  /** 断线补发：把 roomId 中 seq > fromSeq 的消息按序推给连接，分批，客户端按 sync_done 续拉 */
  function replayRoom(conn, roomId, fromSeq) {
    const batch = db.getMessagesAfter(roomId, fromSeq, config.syncBatchSize + 1);
    const hasMore = batch.length > config.syncBatchSize;
    const slice = hasMore ? batch.slice(0, config.syncBatchSize) : batch;
    for (const m of slice) hub.send(conn, msgFrame(m), { track: true, roomId, seq: m.seq });
    const lastSeq = slice.length ? slice[slice.length - 1].seq : fromSeq;
    hub.send(conn, { type: 'sync_done', roomId, lastSeq, hasMore });
  }

  function requireMember(conn, roomId) {
    const member = db.getMember(roomId, conn.userId);
    if (!member) fail('NOT_MEMBER', 'not a member of this group');
    return member;
  }

  /** 取未解散群组，否则按错误码拒绝 */
  function requireRoom(roomId, { lookupName = false } = {}) {
    let room = null;
    if (lookupName) room = db.getRoomByName(roomId) || db.getRoom(roomId);
    else room = db.getRoom(roomId);
    if (!room) fail('NO_SUCH_ROOM', 'group not found');
    if (room.dissolved) fail('GROUP_DISSOLVED', 'group has been dissolved');
    return room;
  }

  function requireRole(conn, roomId, roles) {
    const member = requireMember(conn, roomId);
    if (!roles.includes(member.role)) fail('FORBIDDEN', 'insufficient role');
    return member;
  }

  const requireAdmin = (conn, roomId) => requireRole(conn, roomId, ['owner', 'admin']);
  const requireOwner = (conn, roomId) => requireRole(conn, roomId, ['owner']);

  /** 权限点校验：owner 恒通过，其余角色查群组权限矩阵 */
  function requirePerm(conn, roomId, perm) {
    const room = requireRoom(roomId);
    const member = requireMember(conn, roomId);
    if (!can(room, member.role, perm)) fail('FORBIDDEN', `role not allowed to ${perm}`);
    return { room, member };
  }

  /** 入群前的统一门槛：群组存在、未满员 */
  function ensureJoinable(room) {
    if (room.dissolved) fail('GROUP_DISSOLVED', 'group has been dissolved');
    if (db.countMembers(room.id) >= room.max_members) {
      fail('GROUP_FULL', `group is full (max ${room.max_members})`);
    }
  }

  /**
   * 完成入群：写成员关系 + 让该用户所有已连接设备订阅房间 + 各设备下发 joined 并补发。
   * 用于公开直入、邀请加入、申请审批通过三个入口，保证行为一致。
   */
  function admitUser(roomId, userId, role, opts = {}) {
    db.addMember(roomId, userId, role);
    const room = db.getRoom(roomId);
    const member = db.getMember(roomId, userId);
    const conns = hub.byUser.get(userId);
    if (conns) {
      for (const conn of conns) {
        if (!conn.rooms.has(roomId)) hub.joinRoom(conn, roomId);
        hub.send(conn, {
          type: 'joined',
          roomId: room.id,
          name: room.name,
          role: member.role,
          mutedUntil: member.muted_until,
          lastSeq: room.last_seq,
          group: db.roomView(room),
        });
        pushCurrentAnnouncement(conn, roomId);
        const fromSeq = Number.isInteger(opts.lastSeq)
          ? opts.lastSeq
          : db.getCursor(roomId, userId);
        if (fromSeq < room.last_seq) replayRoom(conn, roomId, fromSeq);
      }
    }
    return member;
  }

  /** 房间成员变更广播（在线/角色/禁言态变化后刷新成员表） */
  function emitMembersChanged(roomId, event, extra = {}) {
    hub.broadcast(roomId, { type: 'notice', roomId, event, ...extra });
  }

  /** 入房成功帧 + 按需断线补发（join 直入与成员重入共用） */
  function sendJoined(conn, room, member, msg) {
    hub.send(conn, {
      type: 'joined',
      roomId: room.id,
      name: room.name,
      role: member.role,
      mutedUntil: member.muted_until,
      lastSeq: room.last_seq,
      group: db.roomView(room),
    });
    pushCurrentAnnouncement(conn, room.id);
    const fromSeq = Number.isInteger(msg.lastSeq) ? msg.lastSeq : db.getCursor(room.id, conn.userId);
    if (fromSeq < room.last_seq) replayRoom(conn, room.id, fromSeq);
  }

  /** 新成员/重连成员入群时补发当前公告（若有） */
  function pushCurrentAnnouncement(conn, roomId) {
    const announcement = db.getAnnouncement(roomId);
    if (announcement) hub.send(conn, { type: 'announcement', roomId, announcement });
  }

  const INVITE_ERRORS = {
    NOT_FOUND: ['INVITE_NOT_FOUND', 'invitation does not exist'],
    EXPIRED: ['INVITE_EXPIRED', 'invitation has expired'],
    DEPLETED: ['INVITE_DEPLETED', 'invitation has reached its use limit'],
    WRONG_INVITEE: ['INVITE_NOT_YOURS', 'this invitation was sent to someone else'],
  };

  /** 凭邀请码加入：校验归属/有效期/次数，消耗后入群 */
  function redeemAndJoin(conn, room, msg) {
    // 先查一次归属（不消耗），避免错误码把别的群邀请消耗掉
    const invite = db.getInvite(msg.code);
    if (!invite) fail('INVITE_NOT_FOUND', 'invitation does not exist');
    if (invite.room_id !== room.id) fail('INVITE_MISMATCH', 'invitation is for a different group');
    const result = db.redeemInvite(msg.code, conn.userId);
    if (!result.ok) {
      const [code, text] = INVITE_ERRORS[result.reason] || ['INVITE_INVALID', 'invalid invitation'];
      fail(code, text);
    }
    const member = db.joinRoom(room.id, conn.userId);
    hub.joinRoom(conn, room.id);
    sendJoined(conn, room, member, msg);
    emitMembersChanged(room.id, 'member_joined', {
      userId: conn.userId, name: conn.name, by: conn.userId, viaInvite: true,
    });
  }

  const handlers = {
    ping(conn, msg) {
      hub.send(conn, { type: 'pong', t: msg.t });
    },

    // ---------------------------------------------------------------- 群组生命周期

    /**
     * 建群（旧协议 create_room 保留为仅传 name 的简写）。
     * body: { name, description?, avatar?, joinMode?, maxMembers?, retainDays?, permissions? }
     */
    create_room(conn, msg) {
      handlers.group_create(conn, msg);
    },

    group_create(conn, msg) {
      if (!isNonEmptyString(msg.name, 64)) fail('BAD_REQUEST', 'invalid group name');
      if (db.getRoomByName(msg.name)) fail('ROOM_EXISTS', 'group name already taken');
      const joinMode = msg.joinMode || 'public';
      if (!JOIN_MODES.includes(joinMode)) fail('BAD_REQUEST', 'invalid joinMode');
      const maxMembers = msg.maxMembers ?? 200;
      if (!Number.isInteger(maxMembers) || maxMembers < 2 || maxMembers > config.groupMaxMembersCap) {
        fail('BAD_REQUEST', `maxMembers must be 2..${config.groupMaxMembersCap}`);
      }
      const retainDays = msg.retainDays ?? 0;
      if (!Number.isInteger(retainDays) || retainDays < 0 || retainDays > 3650) {
        fail('BAD_REQUEST', 'retainDays must be 0..3650 (0 = forever)');
      }
      const description = typeof msg.description === 'string' ? msg.description.slice(0, 500) : '';
      const avatar = typeof msg.avatar === 'string' ? msg.avatar.slice(0, 500) : '';
      const permissions = sanitizePermissions(msg.permissions);
      const room = db.createRoom(randomId('g_'), msg.name, conn.userId, {
        description, avatar, joinMode, maxMembers, retainDays, permissions,
      });
      hub.joinRoom(conn, room.id);
      hub.send(conn, {
        type: 'joined',
        roomId: room.id,
        name: room.name,
        role: 'owner',
        mutedUntil: 0,
        lastSeq: 0,
        group: db.roomView(room),
      });
    },

    /** 编辑群组资料：name/description/avatar/joinMode/maxMembers/retainDays/permissions */
    group_update(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      const { room } = requirePerm(conn, msg.roomId, 'editProfile');
      const fields = {};
      if (msg.name !== undefined) {
        if (!isNonEmptyString(msg.name, 64)) fail('BAD_REQUEST', 'invalid group name');
        const other = db.getRoomByName(msg.name);
        if (other && other.id !== room.id) fail('ROOM_EXISTS', 'group name already taken');
        fields.name = msg.name;
      }
      if (msg.description !== undefined) {
        if (typeof msg.description !== 'string' || msg.description.length > 500) {
          fail('BAD_REQUEST', 'description too long');
        }
        fields.description = msg.description;
      }
      if (msg.avatar !== undefined) fields.avatar = String(msg.avatar).slice(0, 500);
      if (msg.joinMode !== undefined) {
        if (!JOIN_MODES.includes(msg.joinMode)) fail('BAD_REQUEST', 'invalid joinMode');
        fields.joinMode = msg.joinMode;
      }
      if (msg.maxMembers !== undefined) {
        if (!Number.isInteger(msg.maxMembers) || msg.maxMembers < 2 || msg.maxMembers > config.groupMaxMembersCap) {
          fail('BAD_REQUEST', `maxMembers must be 2..${config.groupMaxMembersCap}`);
        }
        // 不得把上限调到低于当前人数（否则老成员被变相踢出）
        if (msg.maxMembers < db.countMembers(room.id)) fail('BAD_REQUEST', 'maxMembers below current member count');
        fields.maxMembers = msg.maxMembers;
      }
      if (msg.retainDays !== undefined) {
        if (!Number.isInteger(msg.retainDays) || msg.retainDays < 0 || msg.retainDays > 3650) {
          fail('BAD_REQUEST', 'retainDays must be 0..3650');
        }
        fields.retainDays = msg.retainDays;
        db.pruneMessages(room.id, msg.retainDays);
      }
      if (msg.permissions !== undefined) fields.permissions = sanitizePermissions(msg.permissions);

      const updated = db.updateRoomProfile(room.id, fields);
      const view = db.roomView(updated);
      // 仅广播（操作者本人也订阅了房间，会收到同一帧）；不再单回 group_info，
      // 否则客户端会把「保存确认」误当成「打开设置」请求而再次弹窗。
      hub.broadcast(room.id, { type: 'group_updated', roomId: room.id, group: view });
    },

    group_info(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      const room = requireRoom(msg.roomId);
      // 私有群资料仅成员可见；公开/申请群允许查询（供发现与申请决策）
      if (!db.getMember(room.id, conn.userId) && room.join_mode === 'private') {
        fail('FORBIDDEN', 'not a member of this private group');
      }
      const view = db.roomView(room);
      view.announcement = db.getAnnouncement(room.id);
      const member = db.getMember(room.id, conn.userId);
      view.myRole = member ? member.role : null;
      hub.send(conn, { type: 'group_info', roomId: room.id, group: view });
    },

    /** 公开群组发现 */
    group_discover(conn) {
      hub.send(conn, { type: 'groups', groups: db.listPublicRooms(config.groupListLimit) });
    },

    /**
     * 入群。room 可为 id 或名称；支持三种准入：
     * - public：直接加入（满员拒绝）
     * - approval：创建入群申请，等待管理员审批
     * - private：拒绝，须凭邀请
     * 另支持 code（邀请码/定向邀请）直接加入任意模式群组。
     */
    join(conn, msg) {
      let room;
      // 仅凭邀请码加入（邀请链接场景）：由邀请码反查群组
      if (!isNonEmptyString(msg.room, 128) && isNonEmptyString(msg.code, 128)) {
        const inv = db.getInvite(msg.code);
        if (!inv) fail('INVITE_NOT_FOUND', 'invitation does not exist');
        room = db.getActiveRoom(inv.room_id);
        if (!room) fail('GROUP_DISSOLVED', 'group no longer exists');
      } else {
        if (!isNonEmptyString(msg.room, 128)) fail('BAD_REQUEST', 'invalid room');
        room = requireRoom(msg.room, { lookupName: true });
      }
      const existing = db.getMember(room.id, conn.userId);
      if (existing) {
        // 已是成员（多端/重入）：直接订阅并补发，不重复走准入
        hub.joinRoom(conn, room.id);
        sendJoined(conn, room, existing, msg);
        return;
      }

      // —— 凭邀请加入（任意模式）：先校验满员再兑换邀请 ——
      if (isNonEmptyString(msg.code, 128)) {
        ensureJoinable(room);
        redeemAndJoin(conn, room, msg);
        return;
      }

      if (room.join_mode === 'public') {
        // 公开群直入：满员即拒
        ensureJoinable(room);
        const member = db.joinRoom(room.id, conn.userId);
        hub.joinRoom(conn, room.id);
        sendJoined(conn, room, member, msg);
        // 通知房间内其他成员
        emitMembersChanged(room.id, 'member_joined', {
          userId: conn.userId, name: conn.name, by: conn.userId,
        });
      } else if (room.join_mode === 'approval') {
        // 申请模式允许在满员时提交申请（审批时再校验名额），由管理员决定是否批准
        const pending = db.stmt.pendingRequestByUser.get(room.id, conn.userId);
        const id = pending ? pending.id : randomId('q_');
        db.upsertJoinRequest(id, room.id, conn.userId, msg.message);
        hub.send(conn, { type: 'request_submitted', roomId: room.id, requestId: id });
        // 通知在线管理员/群主
        for (const m of db.listMembers(room.id)) {
          if (m.role === 'owner' || m.role === 'admin') {
            hub.sendToUser(m.userId, {
              type: 'request_received', roomId: room.id, roomName: room.name,
              requestId: id, userId: conn.userId, userName: conn.name,
              message: typeof msg.message === 'string' ? msg.message.slice(0, 200) : '',
            });
          }
        }
      } else {
        fail('GROUP_PRIVATE', 'this group is private; an invitation is required');
      }
    },

    /** 主动退群（群主不可直接退，须先转让或解散） */
    leave(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      const member = requireMember(conn, msg.roomId);
      const room = requireRoom(msg.roomId);
      if (member.role === 'owner') {
        fail('FORBIDDEN', 'owner must transfer ownership or dissolve the group first');
      }
      db.removeMember(room.id, conn.userId);
      // 让该用户所有设备退出订阅
      for (const c of hub.byUser.get(conn.userId) || []) {
        if (c.rooms.has(room.id)) hub.leaveRoom(c, room.id);
        hub.send(c, { type: 'left', roomId: room.id, reason: 'self' });
      }
      emitMembersChanged(room.id, 'member_left', { userId: conn.userId, name: conn.name });
    },

    msg(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      if (!isNonEmptyString(msg.clientMsgId, 64)) fail('BAD_REQUEST', 'invalid clientMsgId');
      if (!isNonEmptyString(msg.content, config.maxContentLength)) {
        fail('BAD_REQUEST', `content must be 1..${config.maxContentLength} chars`);
      }
      const room = requireRoom(msg.roomId);
      const member = requireMember(conn, msg.roomId);
      if (!can(room, member.role, 'sendMessage')) {
        fail('FORBIDDEN', 'your role may not send messages in this group');
      }
      if (member.muted_until > now()) {
        fail('MUTED', `you are muted until ${new Date(member.muted_until).toISOString()}`);
      }
      // 全员禁言：仅 owner/admin 可发言
      if (room.muted_until > now() && ROLE_RANK[member.role] <= ROLE_RANK.member) {
        fail('MUTED', `the group is muted until ${new Date(room.muted_until).toISOString()}`);
      }
      if (!limiter.take(conn.userId)) fail('RATE_LIMITED', 'sending too fast, slow down');

      // 先落库（同事务分配 seq），再 ACK，再广播 —— 崩溃也不丢已确认消息
      const { message, duplicate } = db.insertMessage({
        roomId: msg.roomId,
        clientMsgId: msg.clientMsgId,
        senderId: conn.userId,
        content: msg.content,
      });
      hub.send(conn, {
        type: 'ack',
        roomId: msg.roomId,
        clientMsgId: msg.clientMsgId,
        seq: message.seq,
        ts: message.ts,
      });
      if (!duplicate) {
        // 重复提交（客户端重试）只回 ACK，不再广播 —— 发送幂等
        hub.broadcast(msg.roomId, msgFrame(message), { track: true, seq: message.seq });
      }
    },

    // 客户端累积 ACK：清除未确认队列 + 持久化游标（断线补发的兜底依据）
    ack(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128) || !Number.isInteger(msg.seq)) return;
      if (!conn.rooms.has(msg.roomId)) return; // 只处理本连接已加入的房间
      conn.ack(msg.roomId, msg.seq);
      db.saveCursor(msg.roomId, conn.userId, msg.seq);
    },

    sync(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      requireMember(conn, msg.roomId);
      const fromSeq = Number.isInteger(msg.lastSeq) ? msg.lastSeq : db.getCursor(msg.roomId, conn.userId);
      replayRoom(conn, msg.roomId, fromSeq);
    },

    history(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      requireMember(conn, msg.roomId);
      const limit = Math.min(Math.max(1, msg.limit || 50), config.historyMaxLimit);
      const before = Number.isInteger(msg.beforeSeq) ? msg.beforeSeq : Number.MAX_SAFE_INTEGER;
      const messages = db.getMessagesBefore(msg.roomId, before, limit);
      hub.send(conn, { type: 'history', roomId: msg.roomId, messages, hasMore: messages.length === limit });
    },

    rooms(conn) {
      hub.send(conn, { type: 'rooms', rooms: db.listRoomsForUser(conn.userId) });
    },

    members(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      requireMember(conn, msg.roomId);
      const online = new Set(hub.onlineUserIds(msg.roomId));
      const members = db.listMembers(msg.roomId).map((m) => ({ ...m, online: online.has(m.userId) }));
      hub.send(conn, { type: 'members', roomId: msg.roomId, members });
    },

    // ---------------------------------------------------------------- 禁言

    mute(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      requireAdmin(conn, msg.roomId);
      const target = db.getMember(msg.roomId, msg.userId);
      if (!target) fail('NOT_MEMBER', 'target is not a member');
      // 禁言只针对普通成员：管理员与群主不可被禁言（降级后才可）
      if (target.role !== 'member') fail('FORBIDDEN', 'cannot mute an admin or owner');
      const minutes = Number(msg.minutes);
      if (!Number.isFinite(minutes) || minutes < 1 || minutes > 1440) {
        fail('BAD_REQUEST', 'minutes must be 1..1440');
      }
      const until = now() + Math.round(minutes * 60_000);
      db.setMuted(msg.roomId, msg.userId, until);
      hub.broadcast(msg.roomId, {
        type: 'notice', roomId: msg.roomId, event: 'muted',
        userId: msg.userId, until, by: conn.userId,
      });
    },

    unmute(conn, msg) {
      requireAdmin(conn, msg.roomId);
      const target = db.getMember(msg.roomId, msg.userId);
      if (!target) fail('NOT_MEMBER', 'target is not a member');
      db.setMuted(msg.roomId, msg.userId, 0);
      hub.broadcast(msg.roomId, {
        type: 'notice', roomId: msg.roomId, event: 'unmuted',
        userId: msg.userId, by: conn.userId,
      });
    },

    /** 全员禁言（分钟；0=解除）。仅 owner/admin，且 owner 与 admin 自身不受限 */
    group_mute_all(conn, msg) {
      requireAdmin(conn, msg.roomId);
      const minutes = Number(msg.minutes);
      if (!Number.isFinite(minutes) || minutes < 0 || minutes > 1440 * 30) {
        fail('BAD_REQUEST', 'minutes must be 0..43200');
      }
      const until = minutes === 0 ? 0 : now() + Math.round(minutes * 60_000);
      db.setRoomMuted(msg.roomId, until);
      hub.broadcast(msg.roomId, {
        type: 'notice', roomId: msg.roomId,
        event: until ? 'group_muted' : 'group_unmuted', until, by: conn.userId,
      });
    },

    // ---------------------------------------------------------------- 成员管理

    /** 踢出成员。不能踢同级或更高级角色；不能踢群主 */
    group_kick(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      if (!isNonEmptyString(msg.userId, 64)) fail('BAD_REQUEST', 'invalid userId');
      const { member: actor } = requirePerm(conn, msg.roomId, 'manageMembers');
      const target = db.getMember(msg.roomId, msg.userId);
      if (!target) fail('NOT_MEMBER', 'target is not a member');
      if (target.role === 'owner') fail('FORBIDDEN', 'cannot kick the owner');
      if (ROLE_RANK[target.role] >= ROLE_RANK[actor.role]) {
        fail('FORBIDDEN', 'cannot kick a member with equal or higher role');
      }
      db.removeMember(msg.roomId, msg.userId);
      hub.leaveRoomForUser(msg.userId, msg.roomId);
      // 通知被踢者所有设备
      hub.sendToUser(msg.userId, { type: 'kicked', roomId: msg.roomId, by: conn.userId });
      emitMembersChanged(msg.roomId, 'member_kicked', {
        userId: msg.userId, by: conn.userId,
      });
    },

    /** 设置/取消管理员。仅群主；不能对自己操作 */
    group_set_admin(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      requireOwner(conn, msg.roomId);
      const target = db.getMember(msg.roomId, msg.userId);
      if (!target) fail('NOT_MEMBER', 'target is not a member');
      if (target.role === 'owner') fail('FORBIDDEN', 'target is the owner');
      const make = msg.make !== false; // 默认设为管理员
      db.setMemberRole(msg.roomId, msg.userId, make ? 'admin' : 'member');
      if (!make) db.setMuted(msg.roomId, msg.userId, 0); // 降为成员时连带解除禁言
      hub.sendToUser(msg.userId, {
        type: 'role_changed', roomId: msg.roomId, role: make ? 'admin' : 'member', by: conn.userId,
      });
      hub.broadcast(msg.roomId, {
        type: 'notice', roomId: msg.roomId,
        event: make ? 'admin_promoted' : 'admin_demoted',
        userId: msg.userId, role: make ? 'admin' : 'member', by: conn.userId,
      });
    },

    /**
     * 转让群主。仅群主本人；新群主必须是现存 admin 或 member。
     * 旧群主降为 member（非 admin，避免「退位仍掌管理权」的歧义）。
     */
    group_transfer(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      if (!isNonEmptyString(msg.userId, 64)) fail('BAD_REQUEST', 'invalid userId');
      requireOwner(conn, msg.roomId);
      if (msg.userId === conn.userId) fail('BAD_REQUEST', 'cannot transfer to yourself');
      const target = db.getMember(msg.roomId, msg.userId);
      if (!target) fail('NOT_MEMBER', 'target is not a member');
      const updated = db.transferOwner(msg.roomId, conn.userId, msg.userId);
      if (!updated) fail('INTERNAL', 'transfer failed');
      // 通知新群主所有设备
      hub.sendToUser(msg.userId, {
        type: 'role_changed', roomId: msg.roomId, role: 'owner', by: conn.userId,
      });
      hub.sendToUser(conn.userId, {
        type: 'role_changed', roomId: msg.roomId, role: 'member', by: conn.userId,
      });
      hub.broadcast(msg.roomId, {
        type: 'notice', roomId: msg.roomId, event: 'owner_transferred',
        userId: msg.userId, by: conn.userId,
      });
    },

    /** 群主解散群组：广播解散、踢除全部订阅、物理清理数据 */
    group_dissolve(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      requireOwner(conn, msg.roomId);
      const room = requireRoom(msg.roomId);
      const memberIds = db.listMemberIds(room.id);
      // 先广播（仍能投递到在线成员），再标记并清理
      hub.broadcastAndCloseRoom(room.id, {
        type: 'group_dissolved', roomId: room.id, name: room.name, by: conn.userId,
      });
      db.dissolveRoom(room.id);
      db.purgeRoom(room.id);
      // 给所有曾在线成员的用户连接兜底（广播只覆盖当时在房连接，其余设备靠此帧）
      for (const uid of memberIds) {
        hub.sendToUser(uid, { type: 'group_dissolved', roomId: room.id, name: room.name, by: conn.userId });
      }
    },

    // ---------------------------------------------------------------- 邀请

    /**
     * 创建邀请。
     * - 带 userId：定向邀请（仅该用户可领取，单次）；
     * - 不带 userId：邀请码，maxUses（0=不限）/ ttlMs 可控。
     * 需有 inviteMembers 权限点。
     */
    group_invite_create(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      const { room } = requirePerm(conn, msg.roomId, 'inviteMembers');
      ensureJoinable(room);
      const ttl = msg.ttlMs ?? config.groupInviteTtlMs;
      if (!Number.isInteger(ttl) || ttl < 0 || ttl > config.groupInviteMaxTtlMs) {
        fail('BAD_REQUEST', `ttlMs must be 0..${config.groupInviteMaxTtlMs}`);
      }
      const expiresAt = ttl === 0 ? 0 : now() + ttl;

      let inviteeId = null;
      if (msg.userId !== undefined && msg.userId !== null) {
        if (!isNonEmptyString(msg.userId, 64)) fail('BAD_REQUEST', 'invalid userId');
        if (!db.getUserById(msg.userId)) fail('NO_SUCH_USER', 'user not found');
        if (db.getMember(room.id, msg.userId)) fail('ALREADY_MEMBER', 'user is already a member');
        inviteeId = msg.userId;
      } else if (isNonEmptyString(msg.userName, 32)) {
        // 客户端按昵称邀请：解析为用户 id
        const target = db.getUserByName(msg.userName);
        if (!target) fail('NO_SUCH_USER', 'user not found');
        if (db.getMember(room.id, target.id)) fail('ALREADY_MEMBER', 'user is already a member');
        inviteeId = target.id;
      }

      let maxUses;
      if (inviteeId) {
        maxUses = 1;
      } else {
        maxUses = msg.maxUses ?? 1;
        if (!Number.isInteger(maxUses) || maxUses < 0 || maxUses > 1000) {
          fail('BAD_REQUEST', 'maxUses must be 0..1000 (0 = unlimited)');
        }
      }

      const code = randomId('inv_') + randomId('').slice(0, 8);
      db.createInvite({ code, roomId: room.id, inviterId: conn.userId, inviteeId, expiresAt, maxUses });

      const payload = {
        type: 'invite_created', roomId: room.id, roomName: room.name, code,
        expiresAt, maxUses, inviteeId,
      };
      hub.send(conn, payload);
      if (inviteeId) {
        // 定向邀请：即便被邀请人不在线也已落库，上线后可经 invites 拉取；在线即时推送
        hub.sendToUser(inviteeId, {
          type: 'invite_received', roomId: room.id, roomName: room.name, code,
          expiresAt, inviterId: conn.userId, inviterName: conn.name,
        });
      }
    },

    /** 我收到的未过期定向邀请 */
    invites(conn) {
      hub.send(conn, { type: 'invites', invites: db.listInvitesForUser(conn.userId) });
    },

    /** 群内现存邀请（管理用，需 manageMembers） */
    group_invites(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      requirePerm(conn, msg.roomId, 'manageMembers');
      hub.send(conn, { type: 'group_invites', roomId: msg.roomId, invites: db.listInvitesOfRoom(msg.roomId) });
    },

    /** 撤销邀请（邀请人本人或管理员） */
    group_invite_revoke(conn, msg) {
      if (!isNonEmptyString(msg.code, 128)) fail('BAD_REQUEST', 'invalid code');
      const inv = db.getInvite(msg.code);
      if (!inv) fail('INVITE_NOT_FOUND', 'invitation does not exist');
      const member = db.getMember(inv.room_id, conn.userId);
      const isManager = member && (member.role === 'owner' || member.role === 'admin');
      if (inv.inviter_id !== conn.userId && !isManager) fail('FORBIDDEN', 'not your invitation');
      db.revokeInvite(msg.code);
      hub.send(conn, { type: 'invite_revoked', code: msg.code });
      if (inv.invitee_id) {
        hub.sendToUser(inv.invitee_id, { type: 'invite_revoked', code: msg.code, roomId: inv.room_id });
      }
    },

    // ---------------------------------------------------------------- 入群申请

    /** 群内待审批申请列表（owner/admin） */
    group_requests(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      requireAdmin(conn, msg.roomId);
      hub.send(conn, {
        type: 'requests', roomId: msg.roomId, requests: db.listPendingRequests(msg.roomId),
      });
    },

    /** 我提交过的申请及状态 */
    my_requests(conn) {
      hub.send(conn, { type: 'my_requests', requests: db.listRequestsOfUser(conn.userId) });
    },

    /** 审批入群申请。approve=true 时再次校验满员 */
    group_request_handle(conn, msg) {
      if (!isNonEmptyString(msg.requestId, 64)) fail('BAD_REQUEST', 'invalid requestId');
      const req = db.getRequest(msg.requestId);
      if (!req) fail('NO_SUCH_REQUEST', 'request not found');
      requireAdmin(conn, req.room_id);
      if (req.status !== 'pending') fail('REQUEST_HANDLED', 'request already handled');
      const approve = msg.approve !== false;

      if (approve) {
        const room = requireRoom(req.room_id);
        // 满员时无法准入：驳回申请（原因 GROUP_FULL），通知申请人，再向管理员报错
        if (db.countMembers(room.id) >= room.max_members) {
          db.setRequestStatus(req.id, 'rejected', conn.userId);
          hub.sendToUser(req.user_id, {
            type: 'request_rejected', roomId: req.room_id, requestId: req.id,
            reason: 'GROUP_FULL', by: conn.userId,
          });
          fail('GROUP_FULL', `group is full (max ${room.max_members})`);
        }
        db.setRequestStatus(req.id, 'approved', conn.userId);
        // 已被批准但对方已是成员（例如期间凭邀请加入）—— 仅回结果，不重复入群
        if (!db.getMember(room.id, req.user_id)) {
          admitUser(room.id, req.user_id, 'member');
          emitMembersChanged(room.id, 'member_joined', {
            userId: req.user_id, by: conn.userId, viaApproval: true,
          });
        }
      } else {
        db.setRequestStatus(req.id, 'rejected', conn.userId);
      }
      // 通知申请人（在线即时；不在线则可经 my_requests 查到结果）
      hub.sendToUser(req.user_id, {
        type: approve ? 'request_approved' : 'request_rejected',
        roomId: req.room_id, requestId: req.id, by: conn.userId,
      });
      hub.send(conn, {
        type: 'request_handled', requestId: req.id, status: approve ? 'approved' : 'rejected',
      });
    },

    // ---------------------------------------------------------------- 公告

    /** 发布/更新群组公告（传空内容视为清除） */
    group_announce(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      const { room } = requirePerm(conn, msg.roomId, 'postAnnouncement');
      const content = typeof msg.content === 'string' ? msg.content.trim() : '';
      if (content.length > 2000) fail('BAD_REQUEST', 'announcement too long (max 2000)');
      const announcement = content ? db.setAnnouncement(room.id, content, conn.userId) : null;
      if (!content) db.clearAnnouncement(room.id);
      hub.broadcast(room.id, {
        type: 'announcement', roomId: room.id, announcement, by: conn.userId,
      });
      hub.send(conn, { type: 'announcement_saved', roomId: room.id, announcement });
    },

    /** 查询当前公告（成员可查；非成员仅公开/申请群可查） */
    group_announcement(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      const room = requireRoom(msg.roomId);
      if (!db.getMember(room.id, conn.userId) && room.join_mode === 'private') {
        fail('FORBIDDEN', 'not a member of this private group');
      }
      hub.send(conn, { type: 'announcement', roomId: room.id, announcement: db.getAnnouncement(room.id) });
    },
  };

  function onFrame(conn, raw) {
    const msg = parseFrame(raw);
    if (!msg) {
      hub.send(conn, { type: 'error', code: 'BAD_FRAME', message: 'invalid JSON frame' });
      return;
    }
    const handler = handlers[msg.type];
    if (!handler) {
      hub.send(conn, { type: 'error', code: 'UNKNOWN_TYPE', message: `unknown type: ${msg.type}` });
      return;
    }
    try {
      handler(conn, msg);
    } catch (err) {
      if (err instanceof ChatError) {
        hub.send(conn, {
          type: 'error',
          code: err.code,
          message: err.message,
          ref: msg.clientMsgId || msg.roomId || undefined,
        });
      } else {
        console.error('[handler error]', msg.type, err);
        hub.send(conn, { type: 'error', code: 'INTERNAL', message: 'internal error' });
      }
    }
  }

  // ---------------------------------------------------------------- HTTP 层

  const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css' };

  function readBody(req, limit = 4096) {
    return new Promise((resolve, reject) => {
      let size = 0;
      const chunks = [];
      req.on('data', (c) => {
        size += c.length;
        if (size > limit) {
          reject(new Error('body too large'));
          req.destroy();
          return;
        }
        chunks.push(c);
      });
      req.on('end', () => resolve(Buffer.concat(chunks).toString()));
      req.on('error', reject);
    });
  }

  const httpServer = http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://localhost');
    const json = (code, obj) => {
      res.writeHead(code, { 'content-type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(obj));
    };

    if (req.method === 'POST' && url.pathname === '/api/login') {
      // 演示级登录：按用户名创建/复用账号，返回签名 token
      try {
        const body = JSON.parse(await readBody(req));
        if (!isNonEmptyString(body.name, 32)) return json(400, { error: 'invalid name' });
        let user = db.getUserByName(body.name);
        if (!user) user = db.createUser(randomId('u_'), body.name, randomSecret());
        const token = signToken(user.id, user.token_random, config.authSecret);
        return json(200, { userId: user.id, name: user.name, token });
      } catch {
        return json(400, { error: 'bad request' });
      }
    }

    if (req.method === 'GET' && url.pathname === '/healthz') {
      return json(200, { ok: true, ...hub.stats() });
    }

    if (req.method === 'GET') {
      const rel = url.pathname === '/' ? 'index.html' : url.pathname.slice(1);
      const file = path.resolve(publicDir, rel);
      if (!file.startsWith(publicDir) || !MIME[path.extname(file)]) {
        res.writeHead(404).end('not found');
        return;
      }
      fs.readFile(file, (err, data) => {
        if (err) {
          res.writeHead(404).end('not found');
          return;
        }
        res.writeHead(200, { 'content-type': MIME[path.extname(file)] });
        res.end(data);
      });
      return;
    }

    res.writeHead(404).end('not found');
  });

  // ---------------------------------------------------------------- WS 层

  const wss = new WebSocketServer({ noServer: true });

  httpServer.on('upgrade', (req, socket, head) => {
    const reject = (code, text) => {
      socket.write(`HTTP/1.1 ${code} ${text}\r\nConnection: close\r\n\r\n`);
      socket.destroy();
    };
    const url = new URL(req.url, 'http://localhost');
    if (url.pathname !== '/ws') return reject(404, 'Not Found');

    const userId = verifyToken(url.searchParams.get('token'), config.authSecret);
    const user = userId && db.getUserById(userId);
    if (!user) return reject(401, 'Unauthorized');

    const denied = hub.checkAdmission(user.id);
    if (denied) return reject(503, denied);

    wss.handleUpgrade(req, socket, head, (ws) => {
      const conn = new Connection(ws, user);
      hub.add(conn);

      ws.on('pong', () => {
        conn.lastPong = now();
      });
      ws.on('message', (raw) => onFrame(conn, raw));
      ws.on('close', () => hub.remove(conn));
      ws.on('error', () => {}); // 错误后必随 close，统一在 close 清理

      hub.send(conn, { type: 'welcome', userId: user.id, name: user.name, serverTime: now() });
    });
  });

  // ---------------------------------------------------------------- 定时任务

  /** 消息保留期清理：对配置了 retainDays 的群组物理删除过期消息 */
  function retentionSweep() {
    for (const { roomId, retainDays } of db.roomsWithRetention()) {
      try {
        db.pruneMessages(roomId, retainDays);
      } catch (err) {
        console.error('[retention sweep]', roomId, err);
      }
    }
  }

  const timers = [
    setInterval(() => hub.heartbeatSweep(), config.heartbeatIntervalMs),
    setInterval(() => hub.resendSweep(), config.ackResendIntervalMs),
    setInterval(retentionSweep, config.retentionSweepMs),
  ];
  for (const t of timers) t.unref();

  // ---------------------------------------------------------------- 生命周期

  function start() {
    return new Promise((resolve) => {
      httpServer.listen(config.port, config.host, () => {
        const addr = httpServer.address();
        console.log(`[chat] listening on http://${addr.address}:${addr.port}  (db: ${config.dbPath})`);
        resolve(addr);
      });
    });
  }

  function stop() {
    for (const t of timers) clearInterval(t);
    for (const conn of [...hub.all]) {
      hub.send(conn, { type: 'server_shutdown' });
      conn.ws.terminate();
    }
    wss.close();
    httpServer.close();
    db.close();
  }

  return { config, db, hub, httpServer, wss, start, stop };
}

// 直接运行：node src/server.js
if (require.main === module) {
  const server = createChatServer();
  server.start();
  const shutdown = () => {
    console.log('\n[chat] shutting down...');
    server.stop();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

module.exports = { createChatServer };
