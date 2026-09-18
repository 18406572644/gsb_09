'use strict';

const { DatabaseSync } = require('node:sqlite');
const { now } = require('./util');

/**
 * 持久层：SQLite（WAL 模式）。
 *
 * 可靠性设计要点：
 * 1. 消息与房间序号 seq 在同一事务中「先落库、后广播」——进程崩溃也不丢已确认消息。
 * 2. messages 上 (room_id, sender_id, client_msg_id) 唯一约束——客户端重试/网络重复
 *    提交同一条消息时不会产生重复记录，实现发送幂等。
 * 3. seq 为每房间单调递增序号，由 rooms.last_seq 计数器在事务内分配——保证房间内
 *    消息全序（时序可控），客户端可凭 seq 检测空洞并触发补发。
 *
 * 群组模型：
 * - rooms 增加准入模式 join_mode（public/approval/private）、人数上限、消息保留期、
 *   成员角色权限矩阵、全员禁言、描述/头像、解散状态等字段；
 * - members.role 扩展为 owner/admin/member；
 * - group_invites：邀请（定向邀请或邀请码，带过期与使用次数）；
 * - join_requests：入群申请（approval 模式 / 私有群定向审批）；
 * - announcements：群组公告，仅保留最新一条（per-room 唯一）。
 * 旧库通过 ALTER TABLE / 建表 IF NOT EXISTS 自动迁移。
 */

const SCHEMA = `
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
PRAGMA synchronous = FULL;

CREATE TABLE IF NOT EXISTS users (
  id           TEXT PRIMARY KEY,
  name         TEXT NOT NULL UNIQUE,
  token_random TEXT NOT NULL,
  created_at   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS rooms (
  id            TEXT PRIMARY KEY,
  name          TEXT NOT NULL UNIQUE,
  description   TEXT NOT NULL DEFAULT '',
  avatar        TEXT NOT NULL DEFAULT '',
  created_by    TEXT NOT NULL REFERENCES users(id),
  created_at    INTEGER NOT NULL,
  last_seq      INTEGER NOT NULL DEFAULT 0,
  join_mode     TEXT NOT NULL DEFAULT 'public' CHECK (join_mode IN ('public','approval','private')),
  max_members   INTEGER NOT NULL DEFAULT 200,
  retain_days   INTEGER NOT NULL DEFAULT 0,
  permissions   TEXT NOT NULL DEFAULT '{}',
  muted_until   INTEGER NOT NULL DEFAULT 0,
  dissolved     INTEGER NOT NULL DEFAULT 0,
  dissolved_at  INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS members (
  room_id        TEXT NOT NULL REFERENCES rooms(id),
  user_id        TEXT NOT NULL REFERENCES users(id),
  role           TEXT NOT NULL DEFAULT 'member' CHECK (role IN ('owner','admin','member')),
  muted_until    INTEGER NOT NULL DEFAULT 0,
  joined_at      INTEGER NOT NULL,
  last_read_seq  INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (room_id, user_id)
);

CREATE TABLE IF NOT EXISTS messages (
  room_id       TEXT NOT NULL REFERENCES rooms(id),
  seq           INTEGER NOT NULL,
  client_msg_id TEXT NOT NULL,
  sender_id     TEXT NOT NULL REFERENCES users(id),
  content       TEXT NOT NULL,
  ts            INTEGER NOT NULL,
  PRIMARY KEY (room_id, seq),
  UNIQUE (room_id, sender_id, client_msg_id)  -- 幂等键
);

-- 服务端保存的每用户每房间已确认游标（断线补发的兜底依据）
CREATE TABLE IF NOT EXISTS cursors (
  room_id      TEXT NOT NULL REFERENCES rooms(id),
  user_id      TEXT NOT NULL REFERENCES users(id),
  last_ack_seq INTEGER NOT NULL DEFAULT 0,
  updated_at   INTEGER NOT NULL,
  PRIMARY KEY (room_id, user_id)
);

-- 群组邀请：invitee_id 为空表示邀请码（任何人凭码加入）；uses/max_uses 控制次数
CREATE TABLE IF NOT EXISTS group_invites (
  code        TEXT PRIMARY KEY,
  room_id     TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
  inviter_id  TEXT NOT NULL REFERENCES users(id),
  invitee_id  TEXT REFERENCES users(id),          -- NULL = 邀请码（公开领取）
  created_at  INTEGER NOT NULL,
  expires_at  INTEGER NOT NULL,                   -- 0 = 永不过期
  max_uses    INTEGER NOT NULL DEFAULT 1,         -- 0 = 不限次数（仅邀请码）
  uses        INTEGER NOT NULL DEFAULT 0
);

-- 入群申请
CREATE TABLE IF NOT EXISTS join_requests (
  id          TEXT PRIMARY KEY,
  room_id     TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
  user_id     TEXT NOT NULL REFERENCES users(id),
  message     TEXT NOT NULL DEFAULT '',
  status      TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','approved','rejected')),
  created_at  INTEGER NOT NULL,
  handled_by  TEXT,
  handled_at  INTEGER NOT NULL DEFAULT 0,
  UNIQUE (room_id, user_id)                        -- 一群一用户同时只有一条申请
);

-- 群组公告：每群仅保留最新一条（room_id 唯一，发布即覆盖）
CREATE TABLE IF NOT EXISTS announcements (
  room_id     TEXT PRIMARY KEY REFERENCES rooms(id) ON DELETE CASCADE,
  content     TEXT NOT NULL,
  updated_by  TEXT NOT NULL REFERENCES users(id),
  updated_at  INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_invites_room ON group_invites(room_id);
CREATE INDEX IF NOT EXISTS idx_invites_invitee ON group_invites(invitee_id);
CREATE INDEX IF NOT EXISTS idx_requests_room ON join_requests(room_id, status);
CREATE INDEX IF NOT EXISTS idx_requests_user ON join_requests(user_id, status);
CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(room_id, ts);
`;

/** 旧库迁移：为已存在的 rooms/members 表补齐新增列（SQLite 不支持 IF NOT EXISTS 加列） */
const MIGRATIONS = [
  { table: 'rooms', column: 'description', ddl: "ALTER TABLE rooms ADD COLUMN description TEXT NOT NULL DEFAULT ''" },
  { table: 'rooms', column: 'avatar', ddl: "ALTER TABLE rooms ADD COLUMN avatar TEXT NOT NULL DEFAULT ''" },
  { table: 'rooms', column: 'join_mode', ddl: "ALTER TABLE rooms ADD COLUMN join_mode TEXT NOT NULL DEFAULT 'public'" },
  { table: 'rooms', column: 'max_members', ddl: 'ALTER TABLE rooms ADD COLUMN max_members INTEGER NOT NULL DEFAULT 200' },
  { table: 'rooms', column: 'retain_days', ddl: "ALTER TABLE rooms ADD COLUMN retain_days INTEGER NOT NULL DEFAULT 0" },
  { table: 'rooms', column: 'permissions', ddl: "ALTER TABLE rooms ADD COLUMN permissions TEXT NOT NULL DEFAULT '{}'" },
  { table: 'rooms', column: 'muted_until', ddl: "ALTER TABLE rooms ADD COLUMN muted_until INTEGER NOT NULL DEFAULT 0" },
  { table: 'rooms', column: 'dissolved', ddl: "ALTER TABLE rooms ADD COLUMN dissolved INTEGER NOT NULL DEFAULT 0" },
  { table: 'rooms', column: 'dissolved_at', ddl: "ALTER TABLE rooms ADD COLUMN dissolved_at INTEGER NOT NULL DEFAULT 0" },
  { table: 'members', column: 'last_read_seq', ddl: "ALTER TABLE members ADD COLUMN last_read_seq INTEGER NOT NULL DEFAULT 0" },
];

const MSG_SELECT = `
  SELECT m.room_id AS roomId, m.seq, m.client_msg_id AS clientMsgId,
         m.sender_id AS "from", u.name AS fromName, m.content, m.ts
    FROM messages m JOIN users u ON u.id = m.sender_id
`;

/** members.role 的旧 CHECK 约束只允许 admin/member，迁移时需重建表以纳入 owner */
function migrateMemberRoleConstraint(d) {
  const cols = d.prepare("PRAGMA table_info(members)").all();
  if (!cols.length) return;
  const sql = d.prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name='members'").get();
  if (sql && sql.sql.includes("'owner'")) return;
  d.exec(`
    CREATE TABLE members_new (
      room_id     TEXT NOT NULL REFERENCES rooms(id),
      user_id     TEXT NOT NULL REFERENCES users(id),
      role        TEXT NOT NULL DEFAULT 'member' CHECK (role IN ('owner','admin','member')),
      muted_until INTEGER NOT NULL DEFAULT 0,
      joined_at   INTEGER NOT NULL,
      last_read_seq INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (room_id, user_id)
    );
    INSERT INTO members_new (room_id, user_id, role, muted_until, joined_at, last_read_seq)
      SELECT room_id, user_id, role, muted_until, joined_at, 0 FROM members;
    DROP TABLE members;
    ALTER TABLE members_new RENAME TO members;
  `);
}

/** 旧库中建房人（admin）提升为 owner，并回填 created_by 对应的 owner 关系 */
function migrateOwners(d) {
  d.exec(`
    UPDATE members SET role = 'owner'
      WHERE role = 'admin'
        AND user_id = (SELECT created_by FROM rooms WHERE rooms.id = members.room_id);
  `);
}

class ChatDB {
  constructor(dbPath) {
    this.db = new DatabaseSync(dbPath);
    this.db.exec(SCHEMA);
    this._migrate();
    this._prepare();
  }

  _migrate() {
    const d = this.db;
    for (const m of MIGRATIONS) {
      const cols = d.prepare(`PRAGMA table_info(${m.table})`).all();
      if (cols.length && !cols.some((c) => c.name === m.column)) d.exec(m.ddl);
    }
    migrateMemberRoleConstraint(d);
    migrateOwners(d);
  }

  _prepare() {
    const d = this.db;
    this.stmt = {
      insertUser: d.prepare('INSERT INTO users (id, name, token_random, created_at) VALUES (?, ?, ?, ?)'),
      userByName: d.prepare('SELECT * FROM users WHERE name = ?'),
      userById: d.prepare('SELECT * FROM users WHERE id = ?'),
      usersByIds: d.prepare('SELECT id, name FROM users WHERE id IN (?)'),

      insertRoom: d.prepare(
        `INSERT INTO rooms (id, name, description, avatar, created_by, created_at, join_mode, max_members, retain_days, permissions)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      ),
      roomById: d.prepare('SELECT * FROM rooms WHERE id = ?'),
      activeRoomById: d.prepare('SELECT * FROM rooms WHERE id = ? AND dissolved = 0'),
      roomByName: d.prepare('SELECT * FROM rooms WHERE name = ?'),
      updateRoom: d.prepare(
        `UPDATE rooms SET name = ?, description = ?, avatar = ?, join_mode = ?,
               max_members = ?, retain_days = ?, permissions = ? WHERE id = ?`
      ),
      setRoomMuted: d.prepare('UPDATE rooms SET muted_until = ? WHERE id = ?'),
      dissolveRoom: d.prepare('UPDATE rooms SET dissolved = 1, dissolved_at = ? WHERE id = ?'),
      publicRooms: d.prepare(
        `SELECT r.id, r.name, r.description, r.avatar, r.join_mode AS joinMode,
                r.max_members AS maxMembers, r.muted_until AS mutedUntil,
                r.retain_days AS retainDays,
                (SELECT COUNT(*) FROM members m WHERE m.room_id = r.id) AS memberCount
           FROM rooms r
          WHERE r.dissolved = 0 AND r.join_mode = 'public'
          ORDER BY r.created_at DESC LIMIT ?`
      ),
      roomsForUser: d.prepare(
        `SELECT r.id, r.name, r.description, r.avatar, r.join_mode AS joinMode,
                r.max_members AS maxMembers, r.retain_days AS retainDays,
                r.last_seq AS lastSeq, r.muted_until AS roomMutedUntil,
                r.permissions AS permissionsJson,
                (SELECT COUNT(*) FROM members c WHERE c.room_id = r.id) AS memberCount,
                m.role, m.muted_until AS mutedUntil
           FROM rooms r JOIN members m ON m.room_id = r.id
          WHERE m.user_id = ? AND r.dissolved = 0 ORDER BY m.joined_at`
      ),
      memberCount: d.prepare('SELECT COUNT(*) AS n FROM members WHERE room_id = ?'),

      upsertMember: d.prepare(
        `INSERT INTO members (room_id, user_id, role, muted_until, joined_at)
         VALUES (?, ?, ?, 0, ?)
         ON CONFLICT (room_id, user_id) DO NOTHING`
      ),
      insertMemberNow: d.prepare(
        'INSERT INTO members (room_id, user_id, role, muted_until, joined_at) VALUES (?, ?, ?, ?, ?)'
      ),
      member: d.prepare('SELECT * FROM members WHERE room_id = ? AND user_id = ?'),
      setMemberRole: d.prepare('UPDATE members SET role = ? WHERE room_id = ? AND user_id = ?'),
      setMuted: d.prepare('UPDATE members SET muted_until = ? WHERE room_id = ? AND user_id = ?'),
      deleteMember: d.prepare('DELETE FROM members WHERE room_id = ? AND user_id = ?'),
      membersOfRoom: d.prepare(
        `SELECT m.user_id AS userId, u.name, m.role, m.muted_until AS mutedUntil,
                m.joined_at AS joinedAt
           FROM members m JOIN users u ON u.id = m.user_id WHERE m.room_id = ?
          ORDER BY m.joined_at`
      ),
      memberIdsOfRoom: d.prepare('SELECT user_id AS userId FROM members WHERE room_id = ?'),

      // —— 消息写入（事务内使用）——
      msgByClientId: d.prepare(
        `${MSG_SELECT} WHERE m.room_id = ? AND m.sender_id = ? AND m.client_msg_id = ?`
      ),
      bumpSeq: d.prepare('UPDATE rooms SET last_seq = last_seq + 1 WHERE id = ? RETURNING last_seq'),
      insertMsg: d.prepare(
        'INSERT INTO messages (room_id, seq, client_msg_id, sender_id, content, ts) VALUES (?, ?, ?, ?, ?, ?)'
      ),
      deleteMessagesBeforeTs: d.prepare('DELETE FROM messages WHERE room_id = ? AND ts < ?'),
      oldestMsgTs: d.prepare('SELECT MIN(ts) AS ts FROM messages WHERE room_id = ?'),

      // —— 消息读取 ——
      msgsAfter: d.prepare(`${MSG_SELECT} WHERE m.room_id = ? AND m.seq > ? ORDER BY m.seq LIMIT ?`),
      msgsBefore: d.prepare(
        `${MSG_SELECT} WHERE m.room_id = ? AND m.seq < ? ORDER BY m.seq DESC LIMIT ?`
      ),

      // —— 游标 ——
      upsertCursor: d.prepare(
        `INSERT INTO cursors (room_id, user_id, last_ack_seq, updated_at) VALUES (?, ?, ?, ?)
         ON CONFLICT (room_id, user_id)
         DO UPDATE SET last_ack_seq = MAX(last_ack_seq, excluded.last_ack_seq), updated_at = excluded.updated_at`
      ),
      cursor: d.prepare('SELECT last_ack_seq AS lastAckSeq FROM cursors WHERE room_id = ? AND user_id = ?'),

      // —— 邀请 ——
      insertInvite: d.prepare(
        `INSERT INTO group_invites (code, room_id, inviter_id, invitee_id, created_at, expires_at, max_uses, uses)
         VALUES (?, ?, ?, ?, ?, ?, ?, 0)`
      ),
      inviteByCode: d.prepare('SELECT * FROM group_invites WHERE code = ?'),
      invitesForInvitee: d.prepare(
        `SELECT i.code, i.room_id AS roomId, r.name AS roomName, i.inviter_id AS inviterId,
                u.name AS inviterName, i.expires_at AS expiresAt, i.max_uses AS maxUses, i.uses,
                i.created_at AS createdAt
           FROM group_invites i
           JOIN rooms r ON r.id = i.room_id AND r.dissolved = 0
           JOIN users u ON u.id = i.inviter_id
          WHERE i.invitee_id = ? AND i.expires_at > ?
          ORDER BY i.created_at DESC`
      ),
      consumeInvite: d.prepare(
        'UPDATE group_invites SET uses = uses + 1 WHERE code = ? AND uses < max_uses'
      ),
      consumeInviteUnlimited: d.prepare(
        'UPDATE group_invites SET uses = uses + 1 WHERE code = ? AND max_uses = 0'
      ),
      deleteInvite: d.prepare('DELETE FROM group_invites WHERE code = ?'),
      invitesOfRoom: d.prepare(
        `SELECT i.code, i.invitee_id AS inviteeId, u.name AS inviteeName,
                i.inviter_id AS inviterId, i.expires_at AS expiresAt,
                i.max_uses AS maxUses, i.uses, i.created_at AS createdAt
           FROM group_invites i LEFT JOIN users u ON u.id = i.invitee_id
          WHERE i.room_id = ? ORDER BY i.created_at DESC`
      ),

      // —— 入群申请 ——
      insertRequest: d.prepare(
        `INSERT INTO join_requests (id, room_id, user_id, message, status, created_at)
         VALUES (?, ?, ?, ?, 'pending', ?)
         ON CONFLICT (room_id, user_id) DO UPDATE
           SET message = excluded.message, status = 'pending', created_at = excluded.created_at`
      ),
      requestById: d.prepare('SELECT * FROM join_requests WHERE id = ?'),
      pendingRequestByUser: d.prepare(
        "SELECT * FROM join_requests WHERE room_id = ? AND user_id = ? AND status = 'pending'"
      ),
      pendingRequestsOfRoom: d.prepare(
        `SELECT q.id, q.room_id AS roomId, q.user_id AS userId, u.name AS userName,
                q.message, q.created_at AS createdAt
           FROM join_requests q JOIN users u ON u.id = q.user_id
          WHERE q.room_id = ? AND q.status = 'pending' ORDER BY q.created_at`
      ),
      requestsOfUser: d.prepare(
        `SELECT q.id, q.room_id AS roomId, r.name AS roomName, q.status,
                q.message, q.created_at AS createdAt, q.handled_at AS handledAt
           FROM join_requests q JOIN rooms r ON r.id = q.room_id
          WHERE q.user_id = ? ORDER BY q.created_at DESC LIMIT 50`
      ),
      setRequestStatus: d.prepare(
        'UPDATE join_requests SET status = ?, handled_by = ?, handled_at = ? WHERE id = ?'
      ),

      // —— 公告 ——
      upsertAnnouncement: d.prepare(
        `INSERT INTO announcements (room_id, content, updated_by, updated_at) VALUES (?, ?, ?, ?)
         ON CONFLICT (room_id) DO UPDATE SET content = excluded.content,
           updated_by = excluded.updated_by, updated_at = excluded.updated_at`
      ),
      announcement: d.prepare(
        `SELECT content, updated_by AS updatedBy, updated_at AS updatedAt
           FROM announcements WHERE room_id = ?`
      ),
    };
  }

  /** 在 IMMEDIATE 事务中执行 fn，失败回滚。node:sqlite 为同步驱动，单进程内无并发交错。 */
  _tx(fn) {
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const r = fn();
      this.db.exec('COMMIT');
      return r;
    } catch (err) {
      try { this.db.exec('ROLLBACK'); } catch { /* 已回滚 */ }
      throw err;
    }
  }

  // ---------- 用户 ----------

  createUser(id, name, tokenRandom) {
    this.stmt.insertUser.run(id, name, tokenRandom, now());
    return this.stmt.userById.get(id);
  }

  getUserByName(name) { return this.stmt.userByName.get(name); }
  getUserById(id) { return this.stmt.userById.get(id); }

  // ---------- 群组与成员 ----------

  /**
   * 建群。opts: { description, avatar, joinMode, maxMembers, retainDays, permissions }
   * 创建者自动成为 owner。
   */
  createRoom(id, name, creatorId, opts = {}) {
    const t = now();
    return this._tx(() => {
      this.stmt.insertRoom.run(
        id,
        name,
        opts.description || '',
        opts.avatar || '',
        creatorId,
        t,
        opts.joinMode || 'public',
        opts.maxMembers ?? 200,
        opts.retainDays ?? 0,
        JSON.stringify(opts.permissions || {})
      );
      // 创建者即群主
      this.stmt.upsertMember.run(id, creatorId, 'owner', t);
      return this.stmt.roomById.get(id);
    });
  }

  getRoom(id) { return this.stmt.roomById.get(id); }
  getActiveRoom(id) { return this.stmt.activeRoomById.get(id); }
  getRoomByName(name) { return this.stmt.roomByName.get(name); }
  listRoomsForUser(userId) {
    return this.stmt.roomsForUser.all(userId).map((r) => {
      const { permissionsJson, ...rest } = r;
      return { ...rest, permissions: JSON.parse(permissionsJson || '{}') };
    });
  }
  listPublicRooms(limit = 50) { return this.stmt.publicRooms.all(limit); }
  countMembers(roomId) { return this.stmt.memberCount.get(roomId).n; }

  /** 群组资料输出（解析 permissions JSON） */
  roomView(room) {
    if (!room) return null;
    return {
      id: room.id,
      name: room.name,
      description: room.description,
      avatar: room.avatar,
      joinMode: room.join_mode,
      maxMembers: room.max_members,
      retainDays: room.retain_days,
      mutedUntil: room.muted_until,
      createdAt: room.created_at,
      createdBy: room.created_by,
      lastSeq: room.last_seq,
      dissolved: !!room.dissolved,
      permissions: JSON.parse(room.permissions || '{}'),
      memberCount: this.countMembers(room.id),
    };
  }

  updateRoomProfile(roomId, fields) {
    const room = this.stmt.roomById.get(roomId);
    if (!room) return null;
    this.stmt.updateRoom.run(
      fields.name ?? room.name,
      fields.description ?? room.description,
      fields.avatar ?? room.avatar,
      fields.joinMode ?? room.join_mode,
      fields.maxMembers ?? room.max_members,
      fields.retainDays ?? room.retain_days,
      JSON.stringify(fields.permissions ?? JSON.parse(room.permissions || '{}')),
      roomId
    );
    return this.stmt.roomById.get(roomId);
  }

  setRoomMuted(roomId, mutedUntil) {
    this.stmt.setRoomMuted.run(mutedUntil, roomId);
    return this.stmt.roomById.get(roomId);
  }

  dissolveRoom(roomId) {
    this.stmt.dissolveRoom.run(now(), roomId);
  }

  /**
   * 转让群主：旧群主降为普通成员，目标成员升为 owner（事务）。
   * 目标必须是该群现存成员。
   */
  transferOwner(roomId, fromId, toId) {
    return this._tx(() => {
      const target = this.stmt.member.get(roomId, toId);
      if (!target) return null;
      this.stmt.setMemberRole.run('member', roomId, fromId);
      this.stmt.setMemberRole.run('owner', roomId, toId);
      this.stmt.setMuted.run(0, roomId, toId); // 群主不能处于禁言态
      return this.stmt.member.get(roomId, toId);
    });
  }

  /** 群主解散后的数据清理：成员、消息、游标、邀请、申请、公告连同房间记录一并删除 */
  purgeRoom(roomId) {
    return this._tx(() => {
      this.db.prepare('DELETE FROM messages WHERE room_id = ?').run(roomId);
      this.db.prepare('DELETE FROM cursors WHERE room_id = ?').run(roomId);
      this.db.prepare('DELETE FROM members WHERE room_id = ?').run(roomId);
      this.db.prepare('DELETE FROM group_invites WHERE room_id = ?').run(roomId);
      this.db.prepare('DELETE FROM join_requests WHERE room_id = ?').run(roomId);
      this.db.prepare('DELETE FROM announcements WHERE room_id = ?').run(roomId);
      this.db.prepare('DELETE FROM rooms WHERE id = ?').run(roomId);
    });
  }

  /** 需做消息保留期清理的群组（retain_days > 0 且未解散） */
  roomsWithRetention() {
    return this.db
      .prepare('SELECT id AS roomId, retain_days AS retainDays FROM rooms WHERE dissolved = 0 AND retain_days > 0')
      .all();
  }

  joinRoom(roomId, userId) {
    this.stmt.upsertMember.run(roomId, userId, 'member', now());
    return this.stmt.member.get(roomId, userId);
  }

  /** 审批通过 / 凭邀请加入：直接插入成员（幂等），返回是否新加入 */
  addMember(roomId, userId, role = 'member') {
    return this._tx(() => {
      const existed = this.stmt.member.get(roomId, userId);
      if (existed) return { member: existed, added: false };
      const m = { muted_until: 0 };
      this.stmt.insertMemberNow.run(roomId, userId, role, 0, now());
      return { member: this.stmt.member.get(roomId, userId), added: true };
    });
  }

  getMember(roomId, userId) { return this.stmt.member.get(roomId, userId); }

  setMemberRole(roomId, userId, role) {
    this.stmt.setMemberRole.run(role, roomId, userId);
    return this.stmt.member.get(roomId, userId);
  }

  /** 设置禁言截止时间（0 表示解除禁言） */
  setMuted(roomId, userId, mutedUntil) {
    this.stmt.setMuted.run(mutedUntil, roomId, userId);
    return this.stmt.member.get(roomId, userId);
  }

  removeMember(roomId, userId) {
    this.stmt.deleteMember.run(roomId, userId);
  }

  listMembers(roomId) { return this.stmt.membersOfRoom.all(roomId); }
  listMemberIds(roomId) { return this.stmt.memberIdsOfRoom.all(roomId).map((r) => r.userId); }

  // ---------- 邀请 ----------

  createInvite({ code, roomId, inviterId, inviteeId = null, expiresAt, maxUses = 1 }) {
    this.stmt.insertInvite.run(code, roomId, inviterId, inviteeId, now(), expiresAt, maxUses);
    return this.stmt.inviteByCode.get(code);
  }

  getInvite(code) { return this.stmt.inviteByCode.get(code); }

  listInvitesForUser(userId) {
    return this.stmt.invitesForInvitee.all(userId, now());
  }

  listInvitesOfRoom(roomId) { return this.stmt.invitesOfRoom.all(roomId); }

  revokeInvite(code) { this.stmt.deleteInvite.run(code); }

  /**
   * 校验并消耗一次邀请。返回 {ok:true} 或 {ok:false, reason}：
   * NOT_FOUND / EXPIRED / DEPLETED / WRONG_INVITEE 由调用方转错误码。
   * 消耗与入组成员数检查需配合 consume* 的影响行数判断。
   */
  redeemInvite(code, userId) {
    return this._tx(() => {
      const inv = this.stmt.inviteByCode.get(code);
      if (!inv) return { ok: false, reason: 'NOT_FOUND' };
      if (inv.expires_at !== 0 && inv.expires_at <= now()) return { ok: false, reason: 'EXPIRED' };
      if (inv.invitee_id && inv.invitee_id !== userId) return { ok: false, reason: 'WRONG_INVITEE' };
      if (inv.max_uses === 0) {
        this.stmt.consumeInviteUnlimited.run(code);
      } else {
        const r = this.stmt.consumeInvite.run(code);
        if (r.changes === 0) return { ok: false, reason: 'DEPLETED' };
      }
      return { ok: true, invite: this.stmt.inviteByCode.get(code) };
    });
  }

  // ---------- 入群申请 ----------

  upsertJoinRequest(id, roomId, userId, message) {
    this.stmt.insertRequest.run(id, roomId, userId, message || '', now());
  }

  getRequest(id) { return this.stmt.requestById.get(id); }
  listPendingRequests(roomId) { return this.stmt.pendingRequestsOfRoom.all(roomId); }
  listRequestsOfUser(userId) { return this.stmt.requestsOfUser.all(userId); }

  setRequestStatus(id, status, handlerId) {
    this.stmt.setRequestStatus.run(status, handlerId, now(), id);
    return this.stmt.requestById.get(id);
  }

  // ---------- 公告 ----------

  setAnnouncement(roomId, content, userId) {
    this.stmt.upsertAnnouncement.run(roomId, content, userId, now());
    return this.stmt.announcement.get(roomId);
  }

  getAnnouncement(roomId) { return this.stmt.announcement.get(roomId) || null; }

  clearAnnouncement(roomId) {
    this.db.prepare('DELETE FROM announcements WHERE room_id = ?').run(roomId);
  }

  // ---------- 消息 ----------

  /**
   * 幂等写入消息。
   * 返回 { message, duplicate }：
   *  - duplicate=false：新消息，已分配 seq 并落库（调用方负责广播）；
   *  - duplicate=true ：同 clientMsgId 的消息已存在，直接返回原消息（调用方只回 ACK，不再广播）。
   */
  insertMessage({ roomId, clientMsgId, senderId, content }) {
    return this._tx(() => {
      const existing = this.stmt.msgByClientId.get(roomId, senderId, clientMsgId);
      if (existing) return { message: existing, duplicate: true };

      const { last_seq: seq } = this.stmt.bumpSeq.get(roomId);
      const ts = now();
      this.stmt.insertMsg.run(roomId, seq, clientMsgId, senderId, content, ts);
      const message = this.stmt.msgByClientId.get(roomId, senderId, clientMsgId);
      return { message, duplicate: false };
    });
  }

  /** 断线补发：取 seq > afterSeq 的消息（升序，最多 limit 条） */
  getMessagesAfter(roomId, afterSeq, limit) {
    return this.stmt.msgsAfter.all(roomId, afterSeq, limit);
  }

  /** 历史翻页：取 seq < beforeSeq 的消息，返回时按升序排列 */
  getMessagesBefore(roomId, beforeSeq, limit) {
    return this.stmt.msgsBefore.all(roomId, beforeSeq, limit).reverse();
  }

  /**
   * 按保留期清理消息。retainDays=0 表示永久保留。
   * 返回删除条数；cutoff 之前的消息被物理删除（seq 不回收，补发以 seq 缺口外的现存行为准）。
   */
  pruneMessages(roomId, retainDays) {
    if (!retainDays || retainDays <= 0) return 0;
    const cutoff = now() - retainDays * 86_400_000;
    return this._tx(() => this.stmt.deleteMessagesBeforeTs.run(roomId, cutoff).changes);
  }

  // ---------- 游标 ----------

  saveCursor(roomId, userId, lastAckSeq) {
    this.stmt.upsertCursor.run(roomId, userId, lastAckSeq, now());
  }

  getCursor(roomId, userId) {
    const row = this.stmt.cursor.get(roomId, userId);
    return row ? row.lastAckSeq : 0;
  }

  close() {
    try { this.db.exec('PRAGMA wal_checkpoint(TRUNCATE)'); } catch { /* 内存库无 WAL */ }
    this.db.close();
  }
}

module.exports = { ChatDB };
