'use strict';

const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { WebSocketServer } = require('ws');

const { ChatDB } = require('./db');
const { Hub, Connection } = require('./hub');
const perms = require('./perms');
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

/** rooms 表行（snake_case）-> 对外群组视图（camelCase） */
function roomView(r) {
  let rolePermissions = {};
  const rawPerms = r.permissions;
  if (typeof rawPerms === 'string') {
    try { rolePermissions = JSON.parse(rawPerms) || {}; } catch { rolePermissions = {}; }
  } else if (rawPerms && typeof rawPerms === 'object') {
    rolePermissions = rawPerms;
  }
  return {
    id: r.id,
    name: r.name,
    description: r.description ?? '',
    joinMode: r.join_mode ?? r.joinMode,
    maxMembers: r.max_members ?? r.maxMembers,
    retentionDays: r.retention_days ?? r.retentionDays,
    muteAllUntil: r.mute_all_until ?? r.muteAllUntil ?? 0,
    announcement: r.announcement ?? '',
    announcementAt: r.announcement_at ?? r.announcementAt ?? 0,
    announcementBy: r.announcement_by ?? r.announcementBy ?? null,
    rolePermissions,
    lastSeq: r.last_seq ?? r.lastSeq ?? 0,
    createdAt: r.created_at ?? r.createdAt,
  };
}

const JOIN_MODES = ['open', 'apply', 'private'];

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

  /** 取群组 + 成员，并校验自定义权限；owner 恒通过 */
  function requirePerm(conn, roomId, perm) {
    if (!isNonEmptyString(roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
    const room = db.getRoom(roomId);
    if (!room) fail('NO_SUCH_ROOM', 'group not found');
    const member = db.getMember(roomId, conn.userId);
    if (!member) fail('NOT_MEMBER', 'not a member of this group');
    if (!perms.can(room, member, perm)) fail('FORBIDDEN', `permission ${perm} required`);
    return { room, member };
  }

  /** 仅群主 */
  function requireOwner(conn, roomId) {
    const member = requireMember(conn, roomId);
    if (member.role !== 'owner') fail('FORBIDDEN', 'owner role required');
    return { room: db.getRoom(roomId), member };
  }

  function assertCapacity(room) {
    if (db.countMembers(room.id) >= room.max_members) {
      fail('ROOM_FULL', 'group has reached its member limit');
    }
  }

  /** 向群内广播一条 notice 控制事件 */
  function notice(roomId, event, extra = {}) {
    hub.broadcast(roomId, { type: 'notice', roomId, event, ...extra });
  }

  /** 拥有某项权限的成员 userId 列表（owner 恒在其中） */
  function staffUserIds(room, perm) {
    return db.listMembers(room.id)
      .filter((m) => perms.can(room, m, perm))
      .map((m) => m.userId);
  }

  /** 成员入群后的统一收口：订阅当前连接、下发 joined、按需补发 */
  function admit(conn, room, { replay = true, lastSeq = undefined } = {}) {
    hub.joinRoom(conn, room.id);
    const member = db.getMember(room.id, conn.userId);
    hub.send(conn, joinedFrame(conn, room, member));
    if (replay) {
      const fromSeq = Number.isInteger(lastSeq) ? lastSeq : db.getCursor(room.id, conn.userId);
      if (fromSeq < room.last_seq) replayRoom(conn, room.id, fromSeq);
    }
  }

  /** joined 帧：附带群设置与该成员的实际权限表，客户端据此渲染管理界面 */
  function joinedFrame(conn, room, member) {
    return {
      type: 'joined',
      roomId: room.id,
      name: room.name,
      role: member.role,
      mutedUntil: member.muted_until,
      lastSeq: room.last_seq,
      group: roomView(room),
      permissions: Object.fromEntries(
        perms.ALL_PERMISSION_KEYS.map((k) => [k, perms.can(room, member, k)])
      ),
    };
  }

  /** 入群申请创建后通知全部有审批权限的在线管理员 */
  function notifyRequestCreated(room, req) {
    const frame = {
      type: 'request_created',
      roomId: room.id,
      request: {
        id: req.id,
        userId: req.user_id,
        userName: db.getUserById(req.user_id)?.name,
        message: req.message,
        createdAt: req.created_at,
      },
    };
    for (const uid of staffUserIds(room, 'request.review')) hub.sendToUser(uid, frame);
  }

  const handlers = {
    ping(conn, msg) {
      hub.send(conn, { type: 'pong', t: msg.t });
    },

    create_room(conn, msg) {
      if (!isNonEmptyString(msg.name, 64)) fail('BAD_REQUEST', 'invalid group name');
      if (db.getRoomByName(msg.name)) fail('ROOM_EXISTS', 'group name already taken');
      const description = msg.description == null ? '' : msg.description;
      if (typeof description !== 'string' || description.length > config.maxDescriptionLength) {
        fail('BAD_REQUEST', `description must be <= ${config.maxDescriptionLength} chars`);
      }
      const joinMode = msg.joinMode || 'open';
      if (!JOIN_MODES.includes(joinMode)) fail('BAD_REQUEST', 'joinMode must be open|apply|private');
      const maxMembers = msg.maxMembers == null ? config.defaultMaxMembers : Number(msg.maxMembers);
      if (!Number.isInteger(maxMembers) || maxMembers < 2 || maxMembers > config.maxMembersHardLimit) {
        fail('BAD_REQUEST', `maxMembers must be 2..${config.maxMembersHardLimit}`);
      }
      const retentionDays = msg.retentionDays == null ? 0 : Number(msg.retentionDays);
      if (!Number.isInteger(retentionDays) || retentionDays < 0 || retentionDays > 3650) {
        fail('BAD_REQUEST', 'retentionDays must be an integer between 0 and 3650');
      }

      const room = db.createRoom(randomId('r_'), msg.name, conn.userId, {
        description, joinMode, maxMembers, retentionDays,
      });
      hub.joinRoom(conn, room.id);
      hub.send(conn, joinedFrame(conn, room, db.getMember(room.id, conn.userId)));
    },

    /** 发现公开/申请加入的群组（自己尚未加入的） */
    discover(conn, msg) {
      const limit = Math.min(Math.max(1, Number(msg.limit) || config.discoverLimit), config.discoverLimit);
      const keyword = isNonEmptyString(msg.keyword, 64) ? msg.keyword : null;
      hub.send(conn, { type: 'discover', groups: db.discoverGroups(conn.userId, keyword, limit) });
    },

    /** 群组资料：成员取完整视图，非成员取公开视图（申请/被邀请前查看） */
    group_info(conn, msg) {
      if (!isNonEmptyString(msg.room, 128)) fail('BAD_REQUEST', 'invalid room');
      const room = db.resolveRoom(msg.room);
      if (!room) fail('NO_SUCH_ROOM', 'group not found');
      const member = db.getMember(room.id, conn.userId);
      const group = roomView(room);
      group.memberCount = db.countMembers(room.id);
      if (member) {
        group.member = true;
        group.myRole = member.role;
        group.mutedUntil = member.muted_until;
        group.permissions = Object.fromEntries(
          perms.ALL_PERMISSION_KEYS.map((k) => [k, perms.can(room, member, k)])
        );
      } else {
        group.member = false;
      }
      hub.send(conn, { type: 'group_info', group });
    },

    join(conn, msg) {
      if (!isNonEmptyString(msg.room, 128)) fail('BAD_REQUEST', 'invalid room');
      const room = db.resolveRoom(msg.room);
      if (!room) fail('NO_SUCH_ROOM', 'group not found');

      // 已是成员：幂等重入（重连/换设备订阅 + 补发），不广播事件
      if (db.getMember(room.id, conn.userId)) {
        admit(conn, room, { lastSeq: msg.lastSeq });
        return;
      }

      if (room.join_mode === 'private') {
        fail('JOIN_PRIVATE', 'this is a private group, an invitation is required');
      }
      if (room.join_mode === 'apply') {
        const existing = db.getPendingRequest(room.id, conn.userId);
        if (existing) {
          hub.send(conn, { type: 'join_requested', requestId: existing.id, roomId: room.id, status: existing.status });
          return;
        }
        const message = isNonEmptyString(msg.message, 200) ? msg.message : '';
        const req = db.createJoinRequest(randomId('q_'), { roomId: room.id, userId: conn.userId, message });
        hub.send(conn, { type: 'join_requested', requestId: req.id, roomId: room.id, status: 'pending' });
        notifyRequestCreated(room, req);
        return;
      }

      // open：直接加入（满员拒绝）
      assertCapacity(room);
      db.addMember(room.id, conn.userId);
      admit(conn, room, { lastSeq: msg.lastSeq });
      notice(room.id, 'member_joined', { userId: conn.userId, userName: conn.name });
    },

    /** 退群（删除成员关系；群主须先转让或解散） */
    leave(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      const room = db.getRoom(msg.roomId);
      if (!room) fail('NO_SUCH_ROOM', 'group not found');
      const member = db.getMember(room.id, conn.userId);
      if (!member) {
        hub.leaveRoom(conn, room.id);
        hub.send(conn, { type: 'left', roomId: room.id });
        return;
      }
      if (member.role === 'owner') {
        fail('OWNER_MUST_TRANSFER', 'owner must transfer ownership or disband the group before leaving');
      }
      db.removeMember(room.id, conn.userId);
      hub.forceLeaveRoom(room.id, conn.userId, { type: 'left', roomId: room.id });
      notice(room.id, 'member_left', { userId: conn.userId, userName: conn.name });
    },

    msg(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      if (!isNonEmptyString(msg.clientMsgId, 64)) fail('BAD_REQUEST', 'invalid clientMsgId');
      if (!isNonEmptyString(msg.content, config.maxContentLength)) {
        fail('BAD_REQUEST', `content must be 1..${config.maxContentLength} chars`);
      }
      const room = db.getRoom(msg.roomId);
      if (!room) fail('NO_SUCH_ROOM', 'group not found');
      const member = requireMember(conn, msg.roomId);
      if (!perms.can(room, member, 'msg.send')) fail('FORBIDDEN', 'you are not allowed to send messages');
      if (member.muted_until > now()) {
        fail('MUTED', `you are muted until ${new Date(member.muted_until).toISOString()}`);
      }
      // 全员禁言：仅管理员及以上可发言
      if (room.mute_all_until > now() && perms.roleRank(member.role) < perms.roleRank('admin')) {
        fail('MUTED', 'all members are muted in this group');
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

    /** 我加入的群组（含群设置、我的角色与有效权限） */
    rooms(conn) {
      const rooms = db.listRoomsForUser(conn.userId).map((row) => ({
        ...roomView(row),
        role: row.role,
        mutedUntil: row.mutedUntil,
        joinedAt: row.joinedAt,
        permissions: Object.fromEntries(
          perms.ALL_PERMISSION_KEYS.map((k) => [k, perms.can(row, { role: row.role }, k)])
        ),
      }));
      hub.send(conn, { type: 'rooms', rooms });
    },

    members(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      requireMember(conn, msg.roomId);
      const online = new Set(hub.onlineUserIds(msg.roomId));
      const members = db.listMembers(msg.roomId).map((m) => ({ ...m, online: online.has(m.userId) }));
      hub.send(conn, { type: 'members', roomId: msg.roomId, members });
    },

    // ------------------------------------------------------------ 群资料/设置

    edit_group(conn, msg) {
      const { room } = requirePerm(conn, msg.roomId, 'group.edit');
      const name = msg.name == null ? room.name : msg.name;
      const description = msg.description == null ? room.description : msg.description;
      if (!isNonEmptyString(name, 64)) fail('BAD_REQUEST', 'invalid group name');
      if (typeof description !== 'string' || description.length > config.maxDescriptionLength) {
        fail('BAD_REQUEST', `description must be <= ${config.maxDescriptionLength} chars`);
      }
      const taken = db.getRoomByName(name);
      if (taken && taken.id !== room.id) fail('ROOM_EXISTS', 'group name already taken');
      const updated = db.updateRoomProfile(room.id, name, description);
      hub.broadcast(room.id, { type: 'group_updated', roomId: room.id, group: roomView(updated) });
    },

    group_settings(conn, msg) {
      const { room } = requirePerm(conn, msg.roomId, 'group.edit');
      const joinMode = msg.joinMode == null ? room.join_mode : msg.joinMode;
      if (!JOIN_MODES.includes(joinMode)) fail('BAD_REQUEST', 'joinMode must be open|apply|private');
      const maxMembers = msg.maxMembers == null ? room.max_members : Number(msg.maxMembers);
      if (!Number.isInteger(maxMembers) || maxMembers < 2 || maxMembers > config.maxMembersHardLimit) {
        fail('BAD_REQUEST', `maxMembers must be 2..${config.maxMembersHardLimit}`);
      }
      const retentionDays = msg.retentionDays == null ? room.retention_days : Number(msg.retentionDays);
      if (!Number.isInteger(retentionDays) || retentionDays < 0 || retentionDays > 3650) {
        fail('BAD_REQUEST', 'retentionDays must be an integer between 0 and 3650');
      }
      const updated = db.updateRoomSettings(room.id, { joinMode, maxMembers, retentionDays });
      hub.broadcast(room.id, { type: 'group_updated', roomId: room.id, group: roomView(updated) });
      notice(room.id, 'settings_changed', { by: conn.userId });
    },

    /** 配置角色权限的稀疏覆盖：{admin:{...}, member:{...}} */
    set_permissions(conn, msg) {
      const { room } = requirePerm(conn, msg.roomId, 'group.edit');
      let overrides;
      try {
        overrides = perms.parseOverrides(msg.permissions);
      } catch (err) {
        fail('BAD_REQUEST', err.message);
      }
      const json = JSON.stringify(overrides || {});
      const updated = db.updatePermissions(room.id, json);
      hub.broadcast(room.id, { type: 'group_updated', roomId: room.id, group: roomView(updated), permissions: overrides || {} });
      notice(room.id, 'permissions_changed', { by: conn.userId });
    },

    announcement(conn, msg) {
      const { room } = requirePerm(conn, msg.roomId, 'group.announce');
      const content = msg.content == null ? '' : msg.content;
      if (typeof content !== 'string' || content.length > config.maxAnnouncementLength) {
        fail('BAD_REQUEST', `announcement must be <= ${config.maxAnnouncementLength} chars`);
      }
      const updated = db.updateAnnouncement(room.id, content, conn.userId);
      hub.broadcast(room.id, {
        type: 'announcement',
        roomId: room.id,
        content: updated.announcement,
        at: updated.announcement_at,
        by: conn.userId,
        byName: conn.name,
      });
    },

    /** 全员禁言开关：minutes=0 关闭，否则 1..1440 分钟 */
    mute_all(conn, msg) {
      const { room } = requirePerm(conn, msg.roomId, 'member.mute');
      const minutes = Number(msg.minutes);
      if (!Number.isInteger(minutes) || minutes < 0 || minutes > 1440) {
        fail('BAD_REQUEST', 'minutes must be 0..1440');
      }
      const until = minutes === 0 ? 0 : now() + minutes * 60_000;
      db.setMuteAll(room.id, until);
      notice(room.id, until ? 'mute_all_on' : 'mute_all_off', { until, by: conn.userId });
    },

    // ------------------------------------------------------------ 成员治理

    mute(conn, msg) {
      const { room, member: actor } = requirePerm(conn, msg.roomId, 'member.mute');
      const target = db.getMember(room.id, msg.userId);
      if (!target) fail('NOT_MEMBER', 'target is not a member');
      // 不能禁言同级或更高级角色（管理员不能互禁/禁群主，群主除外）
      if (perms.roleRank(target.role) >= perms.roleRank(actor.role)) {
        fail('FORBIDDEN', 'cannot mute a member with equal or higher role');
      }
      const minutes = Number(msg.minutes);
      if (!Number.isFinite(minutes) || minutes < 1 || minutes > 1440) {
        fail('BAD_REQUEST', 'minutes must be 1..1440');
      }
      const until = now() + Math.round(minutes * 60_000);
      db.setMuted(room.id, msg.userId, until);
      notice(room.id, 'muted', { userId: msg.userId, until, by: conn.userId });
    },

    unmute(conn, msg) {
      const { room } = requirePerm(conn, msg.roomId, 'member.mute');
      const target = db.getMember(room.id, msg.userId);
      if (!target) fail('NOT_MEMBER', 'target is not a member');
      db.setMuted(room.id, msg.userId, 0);
      notice(room.id, 'unmuted', { userId: msg.userId, by: conn.userId });
    },

    /** 设置/撤销管理员 */
    set_admin(conn, msg) {
      const { room, member: actor } = requirePerm(conn, msg.roomId, 'member.promote');
      const target = db.getMember(room.id, msg.userId);
      if (!target) fail('NOT_MEMBER', 'target is not a member');
      if (target.role === 'owner') fail('FORBIDDEN', 'cannot change the owner role');
      if (perms.roleRank(target.role) >= perms.roleRank(actor.role)) {
        fail('FORBIDDEN', 'cannot change role of an equal or higher member');
      }
      const makeAdmin = msg.isAdmin === true;
      if ((makeAdmin && target.role === 'admin') || (!makeAdmin && target.role === 'member')) return;
      db.setMemberRole(room.id, msg.userId, makeAdmin ? 'admin' : 'member');
      notice(room.id, 'role_changed', {
        userId: msg.userId,
        userName: db.getUserById(msg.userId)?.name || msg.userId,
        role: makeAdmin ? 'admin' : 'member', by: conn.userId,
      });
    },

    /** 群主转让：旧群主降为管理员 */
    transfer_owner(conn, msg) {
      const { room } = requireOwner(conn, msg.roomId);
      const target = db.getMember(room.id, msg.userId);
      if (!target) fail('NOT_MEMBER', 'target is not a member');
      if (target.role === 'owner') fail('BAD_REQUEST', 'target is already the owner');
      db.transferOwnership(room.id, conn.userId, msg.userId);
      notice(room.id, 'owner_changed', {
        from: conn.userId, fromName: conn.name,
        to: msg.userId, toName: db.getUserById(msg.userId)?.name || msg.userId,
      });
    },

    /** 移出成员（被踢者全部在线连接立即退订并收到 removed） */
    kick(conn, msg) {
      const { room, member: actor } = requirePerm(conn, msg.roomId, 'member.kick');
      const target = db.getMember(room.id, msg.userId);
      if (!target) fail('NOT_MEMBER', 'target is not a member');
      if (msg.userId === conn.userId) fail('BAD_REQUEST', 'use leave to leave the group');
      if (target.role === 'owner') fail('FORBIDDEN', 'cannot kick the owner');
      if (perms.roleRank(target.role) >= perms.roleRank(actor.role)) {
        fail('FORBIDDEN', 'cannot kick a member with equal or higher role');
      }
      const name = db.getUserById(msg.userId)?.name || msg.userId;
      db.removeMember(room.id, msg.userId);
      hub.forceLeaveRoom(room.id, msg.userId, {
        type: 'removed', roomId: room.id, name: room.name, by: conn.userId,
      });
      notice(room.id, 'member_removed', { userId: msg.userId, userName: name, by: conn.userId });
    },

    /** 解散群组（仅群主）：通知全部在线成员并级联清除成员/消息/邀请/申请 */
    disband(conn, msg) {
      const { room } = requireOwner(conn, msg.roomId);
      hub.closeRoom(room.id, { type: 'group_disbanded', roomId: room.id, name: room.name, by: conn.userId });
      db.deleteRoom(room.id);
    },

    // ------------------------------------------------------------ 邀请

    /** 邀请用户（按用户名或 ID）。可带 ttlMinutes 与留言 */
    invite(conn, msg) {
      const { room } = requirePerm(conn, msg.roomId, 'member.invite');
      if (!isNonEmptyString(msg.user, 128)) fail('BAD_REQUEST', 'user (name or id) is required');
      const target = db.getUserById(msg.user) || db.getUserByName(msg.user);
      if (!target) fail('USER_NOT_FOUND', 'user not found');
      if (target.id === conn.userId) fail('BAD_REQUEST', 'cannot invite yourself');
      if (db.getMember(room.id, target.id)) fail('ALREADY_MEMBER', 'user is already a member');

      // 幂等：已有 pending 邀请直接返回原邀请
      const existing = db.getPendingInvitation(room.id, target.id);
      let inv = existing;
      if (!inv) {
        let ttl = Number(msg.ttlMinutes);
        if (!Number.isInteger(ttl) || ttl <= 0) ttl = config.inviteDefaultTtlMinutes;
        ttl = Math.min(Math.max(ttl, config.inviteMinTtlMinutes), config.inviteMaxTtlMinutes);
        const message = isNonEmptyString(msg.message, 200) ? msg.message : '';
        inv = db.createInvitation(randomId('iv_'), {
          roomId: room.id, inviteeId: target.id, inviterId: conn.userId,
          message, expiresAt: now() + ttl * 60_000,
        });
      }
      const inviteView = {
        id: inv.id, roomId: room.id, roomName: room.name,
        inviterId: conn.userId, inviterName: conn.name,
        message: inv.message, expiresAt: inv.expires_at,
      };
      hub.sendToUser(target.id, { type: 'invitation', invitation: inviteView });
      hub.send(conn, { type: 'invite_sent', invitation: inviteView });
    },

    /** 我的待处理邀请 */
    invitations(conn) {
      hub.send(conn, { type: 'invitations', invitations: db.listPendingInvitationsForUser(conn.userId) });
    },

    accept_invite(conn, msg) {
      if (!isNonEmptyString(msg.invitationId, 128)) fail('BAD_REQUEST', 'invalid invitationId');
      const inv = db.getInvitation(msg.invitationId);
      if (!inv) fail('INVITATION_NOT_FOUND', 'invitation not found');
      if (inv.invitee_id !== conn.userId) fail('FORBIDDEN', 'this invitation is not for you');
      const room = db.getRoom(inv.room_id);
      if (!room) {
        db.setInvitationStatus(inv.id, 'expired');
        fail('NO_SUCH_ROOM', 'group no longer exists');
      }
      if (inv.status !== 'pending') {
        fail('INVITATION_INVALID', `invitation is ${inv.status}`);
      }
      if (inv.expires_at <= now()) {
        db.setInvitationStatus(inv.id, 'expired');
        fail('INVITATION_EXPIRED', 'invitation has expired');
      }

      // 已是成员（例如期间通过公开加入）：把邀请收敛为 accepted，幂等重入即可
      if (db.getMember(room.id, conn.userId)) {
        db.setInvitationStatus(inv.id, 'accepted');
        admit(conn, room, { lastSeq: msg.lastSeq });
        return;
      }
      assertCapacity(room);
      db.addMember(room.id, conn.userId);
      db.setInvitationStatus(inv.id, 'accepted');
      admit(conn, room, { lastSeq: msg.lastSeq });
      hub.sendToUser(inv.inviter_id, {
        type: 'invitation_accepted', invitationId: inv.id, roomId: room.id,
        userId: conn.userId, userName: conn.name,
      });
      notice(room.id, 'member_joined', { userId: conn.userId, userName: conn.name, by: inv.inviter_id });
    },

    decline_invite(conn, msg) {
      if (!isNonEmptyString(msg.invitationId, 128)) fail('BAD_REQUEST', 'invalid invitationId');
      const inv = db.getInvitation(msg.invitationId);
      if (!inv) fail('INVITATION_NOT_FOUND', 'invitation not found');
      if (inv.invitee_id !== conn.userId) fail('FORBIDDEN', 'this invitation is not for you');
      if (inv.status !== 'pending') fail('INVITATION_INVALID', `invitation is ${inv.status}`);
      db.setInvitationStatus(inv.id, 'declined');
      hub.sendToUser(inv.inviter_id, {
        type: 'invitation_declined', invitationId: inv.id, roomId: inv.room_id,
        userId: conn.userId, userName: conn.name,
      });
    },

    revoke_invite(conn, msg) {
      if (!isNonEmptyString(msg.invitationId, 128)) fail('BAD_REQUEST', 'invalid invitationId');
      const inv = db.getInvitation(msg.invitationId);
      if (!inv) fail('INVITATION_NOT_FOUND', 'invitation not found');
      const room = db.getRoom(inv.room_id);
      const allowed = inv.inviter_id === conn.userId ||
        (room && perms.can(room, db.getMember(room.id, conn.userId), 'group.edit'));
      if (!allowed) fail('FORBIDDEN', 'only the inviter or group admins can revoke this invitation');
      if (inv.status !== 'pending') fail('INVITATION_INVALID', `invitation is ${inv.status}`);
      db.setInvitationStatus(inv.id, 'revoked');
      hub.sendToUser(inv.invitee_id, {
        type: 'invitation_revoked', invitationId: inv.id, roomId: inv.room_id,
      });
    },

    // ------------------------------------------------------------ 入群申请

    requests(conn, msg) {
      const { room } = requirePerm(conn, msg.roomId, 'request.review');
      hub.send(conn, {
        type: 'requests', roomId: room.id, requests: db.listPendingRequestsForRoom(room.id),
      });
    },

    approve_request(conn, msg) {
      if (!isNonEmptyString(msg.requestId, 128)) fail('BAD_REQUEST', 'invalid requestId');
      const req0 = db.getJoinRequest(msg.requestId);
      if (!req0) fail('REQUEST_INVALID', 'join request not found');
      const { room } = requirePerm(conn, req0.room_id, 'request.review');
      if (req0.status !== 'pending') fail('REQUEST_INVALID', `request is ${req0.status}`);
      assertCapacity(room); // 满员时保持 pending，稍后可重试审批
      db.addMember(room.id, req0.user_id);
      db.setJoinRequestStatus(req0.id, 'approved', conn.userId);
      const userName = db.getUserById(req0.user_id)?.name;
      // 在线申请设备：直接自动入群（joined + 历史补发）；离线设备上线后凭 request_approved 主动 join
      for (const targetConn of hub.userConns(req0.user_id)) {
        if (!targetConn.rooms.has(room.id)) admit(targetConn, room, { replay: true });
      }
      hub.sendToUser(req0.user_id, {
        type: 'request_approved', requestId: req0.id, roomId: room.id, name: room.name,
      });
      notice(room.id, 'member_joined', { userId: req0.user_id, userName, by: conn.userId });
    },

    reject_request(conn, msg) {
      if (!isNonEmptyString(msg.requestId, 128)) fail('BAD_REQUEST', 'invalid requestId');
      const req0 = db.getJoinRequest(msg.requestId);
      if (!req0) fail('REQUEST_INVALID', 'join request not found');
      const { room } = requirePerm(conn, req0.room_id, 'request.review');
      if (req0.status !== 'pending') fail('REQUEST_INVALID', `request is ${req0.status}`);
      db.setJoinRequestStatus(req0.id, 'rejected', conn.userId);
      hub.sendToUser(req0.user_id, {
        type: 'request_rejected', requestId: req0.id, roomId: room.id, name: room.name,
      });
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
          ref: msg.clientMsgId || msg.roomId || msg.invitationId || msg.requestId || undefined,
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

  const timers = [
    setInterval(() => hub.heartbeatSweep(), config.heartbeatIntervalMs),
    setInterval(() => hub.resendSweep(), config.ackResendIntervalMs),
    // 过期消息清扫：按各群 retention_days 删除超期消息
    setInterval(() => {
      try { db.sweepExpiredMessages(); } catch (err) { console.error('[retention sweep]', err); }
    }, config.retentionSweepIntervalMs),
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
