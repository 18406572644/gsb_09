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
 * 群组化扩展（schema user_version = 1）：
 * - rooms 增加群资料/准入模式/人数上限/保留期限/全员禁言/公告/角色权限覆盖；
 * - members 角色扩展为 owner/admin/member，所有子表 ON DELETE CASCADE 以支持解散群；
 * - invitations（邀请）、join_requests（入群申请）两张新表，部分唯一索引保证
 *   「同一用户在同一群至多一条 pending 邀请/申请」。
 */

const PRAGMAS = `
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
PRAGMA synchronous = FULL;
`;

const SCHEMA = `
CREATE TABLE IF NOT EXISTS users (
  id           TEXT PRIMARY KEY,
  name         TEXT NOT NULL UNIQUE,
  token_random TEXT NOT NULL,
  created_at   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS rooms (
  id               TEXT PRIMARY KEY,
  name             TEXT NOT NULL UNIQUE,
  description      TEXT NOT NULL DEFAULT '',
  join_mode        TEXT NOT NULL DEFAULT 'open' CHECK (join_mode IN ('open','apply','private')),
  max_members      INTEGER NOT NULL DEFAULT 200,
  retention_days   INTEGER NOT NULL DEFAULT 0,   -- 消息保留天数，0 表示永久保留
  mute_all_until   INTEGER NOT NULL DEFAULT 0,   -- 全员禁言截止时间戳，0 表示未开启
  announcement     TEXT NOT NULL DEFAULT '',
  announcement_at  INTEGER NOT NULL DEFAULT 0,
  announcement_by  TEXT,
  permissions      TEXT NOT NULL DEFAULT '{}',   -- 角色权限稀疏覆盖 JSON
  created_by       TEXT NOT NULL REFERENCES users(id),
  created_at       INTEGER NOT NULL,
  last_seq         INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS members (
  room_id     TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
  user_id     TEXT NOT NULL REFERENCES users(id),
  role        TEXT NOT NULL DEFAULT 'member' CHECK (role IN ('owner','admin','member')),
  muted_until INTEGER NOT NULL DEFAULT 0,
  joined_at   INTEGER NOT NULL,
  PRIMARY KEY (room_id, user_id)
);

CREATE TABLE IF NOT EXISTS messages (
  room_id       TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
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
  room_id      TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
  user_id      TEXT NOT NULL REFERENCES users(id),
  last_ack_seq INTEGER NOT NULL DEFAULT 0,
  updated_at   INTEGER NOT NULL,
  PRIMARY KEY (room_id, user_id)
);

-- 入群邀请：同一邀请对象在同一群至多一条 pending（撤销/拒绝后可重新发起）
CREATE TABLE IF NOT EXISTS invitations (
  id         TEXT PRIMARY KEY,
  room_id    TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
  invitee_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  inviter_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  message    TEXT NOT NULL DEFAULT '',
  created_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  status     TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending','accepted','declined','revoked','expired')),
  decided_at INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_invitation_pending
  ON invitations(room_id, invitee_id) WHERE status = 'pending';

-- 入群申请：同一用户在同一群至多一条 pending
CREATE TABLE IF NOT EXISTS join_requests (
  id          TEXT PRIMARY KEY,
  room_id     TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
  user_id     TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  message     TEXT NOT NULL DEFAULT '',
  status      TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending','approved','rejected')),
  decided_by  TEXT,
  created_at  INTEGER NOT NULL,
  decided_at  INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_request_pending
  ON join_requests(room_id, user_id) WHERE status = 'pending';
`;

const MSG_SELECT = `
  SELECT m.room_id AS roomId, m.seq, m.client_msg_id AS clientMsgId,
         m.sender_id AS "from", u.name AS fromName, m.content, m.ts
    FROM messages m JOIN users u ON u.id = m.sender_id
`;

class ChatDB {
  constructor(dbPath) {
    this.db = new DatabaseSync(dbPath);
    this.db.exec(PRAGMAS);
    this.db.exec(SCHEMA);
    this._migrate();
    this._prepare();
  }

  /**
   * 轻量迁移：以 PRAGMA user_version 为版本号。
   * v0（单一聊天室时期）→ v1（群组化）：rooms 补列、members 等子表重建为三级角色
   * 并补 ON DELETE CASCADE；原房间创建者升级为 owner。
   */
  _migrate() {
    const { user_version: version } = this.db.prepare('PRAGMA user_version').get();
    if (version >= 1) return;
    const cols = this.db.prepare('PRAGMA table_info(rooms)').all().map((c) => c.name);
    if (!cols.includes('join_mode')) {
      this._tx(() => {
        const addCol = (ddl) => { try { this.db.exec(`ALTER TABLE rooms ADD COLUMN ${ddl}`); } catch { /* 列已存在 */ } };
        addCol("description TEXT NOT NULL DEFAULT ''");
        addCol("join_mode TEXT NOT NULL DEFAULT 'open'");
        addCol('max_members INTEGER NOT NULL DEFAULT 200');
        addCol('retention_days INTEGER NOT NULL DEFAULT 0');
        addCol('mute_all_until INTEGER NOT NULL DEFAULT 0');
        addCol("announcement TEXT NOT NULL DEFAULT ''");
        addCol('announcement_at INTEGER NOT NULL DEFAULT 0');
        addCol('announcement_by TEXT');
        addCol("permissions TEXT NOT NULL DEFAULT '{}'");

        // members：旧 CHECK 只有 admin/member，需重建；创建者升级为 owner
        this.db.exec(`
          CREATE TABLE members_new (
            room_id     TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
            user_id     TEXT NOT NULL REFERENCES users(id),
            role        TEXT NOT NULL DEFAULT 'member' CHECK (role IN ('owner','admin','member')),
            muted_until INTEGER NOT NULL DEFAULT 0,
            joined_at   INTEGER NOT NULL,
            PRIMARY KEY (room_id, user_id)
          );
          INSERT INTO members_new (room_id, user_id, role, muted_until, joined_at)
            SELECT m.room_id, m.user_id,
                   CASE WHEN m.user_id = r.created_by THEN 'owner' ELSE m.role END,
                   m.muted_until, m.joined_at
              FROM members m JOIN rooms r ON r.id = m.room_id;
          DROP TABLE members;
          ALTER TABLE members_new RENAME TO members;
        `);

        // messages / cursors：补 ON DELETE CASCADE（解散群需要）
        this.db.exec(`
          CREATE TABLE messages_new (
            room_id TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
            seq INTEGER NOT NULL, client_msg_id TEXT NOT NULL,
            sender_id TEXT NOT NULL REFERENCES users(id),
            content TEXT NOT NULL, ts INTEGER NOT NULL,
            PRIMARY KEY (room_id, seq),
            UNIQUE (room_id, sender_id, client_msg_id)
          );
          INSERT INTO messages_new SELECT room_id, seq, client_msg_id, sender_id, content, ts FROM messages;
          DROP TABLE messages;
          ALTER TABLE messages_new RENAME TO messages;

          CREATE TABLE cursors_new (
            room_id TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
            user_id TEXT NOT NULL REFERENCES users(id),
            last_ack_seq INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (room_id, user_id)
          );
          INSERT INTO cursors_new SELECT room_id, user_id, last_ack_seq, updated_at FROM cursors;
          DROP TABLE cursors;
          ALTER TABLE cursors_new RENAME TO cursors;
        `);
      });
    }
    this.db.exec('PRAGMA user_version = 1');
  }

  _prepare() {
    const d = this.db;
    this.stmt = {
      insertUser: d.prepare('INSERT INTO users (id, name, token_random, created_at) VALUES (?, ?, ?, ?)'),
      userByName: d.prepare('SELECT * FROM users WHERE name = ?'),
      userById: d.prepare('SELECT * FROM users WHERE id = ?'),

      // —— 群组 ——
      insertRoom: d.prepare(
        `INSERT INTO rooms (id, name, description, join_mode, max_members, retention_days, created_by, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
      ),
      roomById: d.prepare('SELECT * FROM rooms WHERE id = ?'),
      roomByName: d.prepare('SELECT * FROM rooms WHERE name = ?'),
      roomsForUser: d.prepare(
        `SELECT r.*, m.role AS role, m.muted_until AS mutedUntil, m.joined_at AS joinedAt
           FROM rooms r JOIN members m ON m.room_id = r.id
          WHERE m.user_id = ? ORDER BY r.created_at`
      ),
      discoverAll: d.prepare(
        `SELECT r.id, r.name, r.description, r.join_mode AS joinMode, r.max_members AS maxMembers,
                (SELECT COUNT(*) FROM members m WHERE m.room_id = r.id) AS memberCount
           FROM rooms r
          WHERE r.join_mode IN ('open','apply')
            AND NOT EXISTS (SELECT 1 FROM members m WHERE m.room_id = r.id AND m.user_id = ?)
          ORDER BY r.created_at DESC LIMIT ?`
      ),
      discoverSearch: d.prepare(
        `SELECT r.id, r.name, r.description, r.join_mode AS joinMode, r.max_members AS maxMembers,
                (SELECT COUNT(*) FROM members m WHERE m.room_id = r.id) AS memberCount
           FROM rooms r
          WHERE r.join_mode IN ('open','apply')
            AND NOT EXISTS (SELECT 1 FROM members m WHERE m.room_id = r.id AND m.user_id = ?)
            AND instr(lower(r.name), lower(?)) > 0
          ORDER BY r.created_at DESC LIMIT ?`
      ),
      updateRoomCreator: d.prepare('UPDATE rooms SET created_by = ? WHERE id = ?'),
      updateRoomProfile: d.prepare('UPDATE rooms SET name = ?, description = ? WHERE id = ?'),
      updateRoomSettings: d.prepare(
        'UPDATE rooms SET join_mode = ?, max_members = ?, retention_days = ? WHERE id = ?'
      ),
      updateAnnouncement: d.prepare(
        'UPDATE rooms SET announcement = ?, announcement_at = ?, announcement_by = ? WHERE id = ?'
      ),
      updatePermissions: d.prepare('UPDATE rooms SET permissions = ? WHERE id = ?'),
      setMuteAll: d.prepare('UPDATE rooms SET mute_all_until = ? WHERE id = ?'),
      countMembers: d.prepare('SELECT COUNT(*) AS n FROM members WHERE room_id = ?'),
      deleteRoom: d.prepare('DELETE FROM rooms WHERE id = ?'),

      // —— 成员 ——
      upsertMember: d.prepare(
        `INSERT INTO members (room_id, user_id, role, muted_until, joined_at)
         VALUES (?, ?, ?, 0, ?)
         ON CONFLICT (room_id, user_id) DO NOTHING`
      ),
      member: d.prepare('SELECT * FROM members WHERE room_id = ? AND user_id = ?'),
      setMemberRole: d.prepare('UPDATE members SET role = ? WHERE room_id = ? AND user_id = ?'),
      setMuted: d.prepare('UPDATE members SET muted_until = ? WHERE room_id = ? AND user_id = ?'),
      deleteMember: d.prepare('DELETE FROM members WHERE room_id = ? AND user_id = ?'),
      membersOfRoom: d.prepare(
        `SELECT m.user_id AS userId, u.name, m.role, m.muted_until AS mutedUntil, m.joined_at AS joinedAt
           FROM members m JOIN users u ON u.id = m.user_id WHERE m.room_id = ?`
      ),

      // —— 邀请 ——
      insertInvitation: d.prepare(
        `INSERT INTO invitations (id, room_id, invitee_id, inviter_id, message, created_at, expires_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)`
      ),
      invitationById: d.prepare('SELECT * FROM invitations WHERE id = ?'),
      pendingInvitation: d.prepare(
        `SELECT * FROM invitations WHERE room_id = ? AND invitee_id = ? AND status = 'pending'`
      ),
      pendingInvitationsForUser: d.prepare(
        `SELECT i.id, i.room_id AS roomId, r.name AS roomName, i.inviter_id AS inviterId,
                u.name AS inviterName, i.message, i.expires_at AS expiresAt, i.created_at AS createdAt
           FROM invitations i
           JOIN rooms r ON r.id = i.room_id
           JOIN users u ON u.id = i.inviter_id
          WHERE i.invitee_id = ? AND i.status = 'pending' AND i.expires_at > ?
          ORDER BY i.created_at DESC`
      ),
      setInvitationStatus: d.prepare(
        'UPDATE invitations SET status = ?, decided_at = ? WHERE id = ?'
      ),

      // —— 入群申请 ——
      insertJoinRequest: d.prepare(
        `INSERT INTO join_requests (id, room_id, user_id, message, created_at)
         VALUES (?, ?, ?, ?, ?)`
      ),
      joinRequestById: d.prepare('SELECT * FROM join_requests WHERE id = ?'),
      pendingRequest: d.prepare(
        `SELECT * FROM join_requests WHERE room_id = ? AND user_id = ? AND status = 'pending'`
      ),
      pendingRequestsForRoom: d.prepare(
        `SELECT q.id, q.user_id AS userId, u.name AS userName, q.message, q.created_at AS createdAt
           FROM join_requests q JOIN users u ON u.id = q.user_id
          WHERE q.room_id = ? AND q.status = 'pending'
          ORDER BY q.created_at`
      ),
      setJoinRequestStatus: d.prepare(
        'UPDATE join_requests SET status = ?, decided_by = ?, decided_at = ? WHERE id = ?'
      ),
      invalidateRequestsForUser: d.prepare(
        `UPDATE join_requests SET status = 'rejected', decided_at = ?
          WHERE room_id = ? AND user_id = ? AND status = 'pending'`
      ),

      // —— 消息写入（事务内使用）——
      msgByClientId: d.prepare(
        `${MSG_SELECT} WHERE m.room_id = ? AND m.sender_id = ? AND m.client_msg_id = ?`
      ),
      bumpSeq: d.prepare('UPDATE rooms SET last_seq = last_seq + 1 WHERE id = ? RETURNING last_seq'),
      insertMsg: d.prepare(
        'INSERT INTO messages (room_id, seq, client_msg_id, sender_id, content, ts) VALUES (?, ?, ?, ?, ?, ?)'
      ),

      // —— 消息读取 / 保留期清扫 ——
      msgsAfter: d.prepare(`${MSG_SELECT} WHERE m.room_id = ? AND m.seq > ? ORDER BY m.seq LIMIT ?`),
      msgsBefore: d.prepare(
        `${MSG_SELECT} WHERE m.room_id = ? AND m.seq < ? ORDER BY m.seq DESC LIMIT ?`
      ),
      roomsWithRetention: d.prepare('SELECT id, retention_days AS retentionDays FROM rooms WHERE retention_days > 0'),
      deleteMessagesBefore: d.prepare('DELETE FROM messages WHERE room_id = ? AND ts < ?'),

      // —— 游标 ——
      upsertCursor: d.prepare(
        `INSERT INTO cursors (room_id, user_id, last_ack_seq, updated_at) VALUES (?, ?, ?, ?)
         ON CONFLICT (room_id, user_id)
         DO UPDATE SET last_ack_seq = MAX(last_ack_seq, excluded.last_ack_seq), updated_at = excluded.updated_at`
      ),
      cursor: d.prepare('SELECT last_ack_seq AS lastAckSeq FROM cursors WHERE room_id = ? AND user_id = ?'),
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
   * 建群。options: { description, joinMode, maxMembers, retentionDays }
   * 创建者以 owner 身份入群。
   */
  createRoom(id, name, creatorId, options = {}) {
    const {
      description = '',
      joinMode = 'open',
      maxMembers = 200,
      retentionDays = 0,
    } = options;
    return this._tx(() => {
      this.stmt.insertRoom.run(id, name, description, joinMode, maxMembers, retentionDays, creatorId, now());
      this.stmt.upsertMember.run(id, creatorId, 'owner', now());
      return this.stmt.roomById.get(id);
    });
  }

  getRoom(id) { return this.stmt.roomById.get(id); }
  getRoomByName(name) { return this.stmt.roomByName.get(name); }

  /** 解析「ID 或群名」定位群组 */
  resolveRoom(ref) {
    return this.stmt.roomById.get(ref) || this.stmt.roomByName.get(ref);
  }

  listRoomsForUser(userId) { return this.stmt.roomsForUser.all(userId); }

  discoverGroups(userId, keyword, limit) {
    return keyword
      ? this.stmt.discoverSearch.all(userId, keyword, limit)
      : this.stmt.discoverAll.all(userId, limit);
  }

  listMembers(roomId) { return this.stmt.membersOfRoom.all(roomId); }
  countMembers(roomId) { return this.stmt.countMembers.get(roomId).n; }

  updateRoomProfile(roomId, name, description) {
    this.stmt.updateRoomProfile.run(name, description, roomId);
    return this.stmt.roomById.get(roomId);
  }

  updateRoomSettings(roomId, { joinMode, maxMembers, retentionDays }) {
    this.stmt.updateRoomSettings.run(joinMode, maxMembers, retentionDays, roomId);
    return this.stmt.roomById.get(roomId);
  }

  updateAnnouncement(roomId, text, by) {
    this.stmt.updateAnnouncement.run(text, now(), by, roomId);
    return this.stmt.roomById.get(roomId);
  }

  updatePermissions(roomId, permissionsJson) {
    this.stmt.updatePermissions.run(permissionsJson, roomId);
    return this.stmt.roomById.get(roomId);
  }

  setMuteAll(roomId, until) {
    this.stmt.setMuteAll.run(until, roomId);
    return this.stmt.roomById.get(roomId);
  }

  /** 解散群组：子表全部 ON DELETE CASCADE（成员/消息/游标/邀请/申请） */
  deleteRoom(roomId) {
    return this._tx(() => this.stmt.deleteRoom.run(roomId).changes > 0);
  }

  /** 加入成员（公开群直接加入 / 申请批准 / 邀请接受统一入口），已是成员则幂等无操作 */
  addMember(roomId, userId, role = 'member') {
    return this._tx(() => {
      this.stmt.upsertMember.run(roomId, userId, role, now());
      // 加入成功后其 pending 入群申请自动作废，避免悬挂审批项
      this.stmt.invalidateRequestsForUser.run(now(), roomId, userId);
      return this.stmt.member.get(roomId, userId);
    });
  }

  /** 转让群主：旧群主降为 admin，目标成员升为 owner，并同步 created_by */
  transferOwnership(roomId, fromId, toId) {
    return this._tx(() => {
      this.stmt.setMemberRole.run('admin', roomId, fromId);
      this.stmt.setMemberRole.run('owner', roomId, toId);
      this.stmt.updateRoomCreator.run(toId, roomId);
      return this.stmt.member.get(roomId, toId);
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

  /** 移出成员；同时将其 pending 入群申请作废（被踢后需重新申请） */
  removeMember(roomId, userId) {
    return this._tx(() => {
      const changed = this.stmt.deleteMember.run(roomId, userId).changes > 0;
      this.stmt.invalidateRequestsForUser.run(now(), roomId, userId);
      return changed;
    });
  }

  // ---------- 邀请 ----------

  createInvitation(id, { roomId, inviteeId, inviterId, message = '', expiresAt }) {
    this.stmt.insertInvitation.run(id, roomId, inviteeId, inviterId, message, now(), expiresAt);
    return this.stmt.invitationById.get(id);
  }

  getInvitation(id) { return this.stmt.invitationById.get(id); }
  getPendingInvitation(roomId, inviteeId) { return this.stmt.pendingInvitation.get(roomId, inviteeId); }
  listPendingInvitationsForUser(userId, at = now()) { return this.stmt.pendingInvitationsForUser.all(userId, at); }

  setInvitationStatus(id, status) {
    this.stmt.setInvitationStatus.run(status, now(), id);
    return this.stmt.invitationById.get(id);
  }

  // ---------- 入群申请 ----------

  createJoinRequest(id, { roomId, userId, message = '' }) {
    this.stmt.insertJoinRequest.run(id, roomId, userId, message, now());
    return this.stmt.joinRequestById.get(id);
  }

  getJoinRequest(id) { return this.stmt.joinRequestById.get(id); }
  getPendingRequest(roomId, userId) { return this.stmt.pendingRequest.get(roomId, userId); }
  listPendingRequestsForRoom(roomId) { return this.stmt.pendingRequestsForRoom.all(roomId); }

  setJoinRequestStatus(id, status, decidedBy) {
    this.stmt.setJoinRequestStatus.run(status, decidedBy, now(), id);
    return this.stmt.joinRequestById.get(id);
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

  /** 按各群 retention_days 清扫过期消息，返回删除条数 */
  sweepExpiredMessages() {
    return this._tx(() => {
      let removed = 0;
      for (const { id, retentionDays } of this.stmt.roomsWithRetention.all()) {
        const cutoff = now() - retentionDays * 86_400_000;
        removed += this.stmt.deleteMessagesBefore.run(id, cutoff).changes;
      }
      return removed;
    });
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
