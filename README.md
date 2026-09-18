# 可靠消息聊天室（Node + ws + SQLite）

基于 WebSocket 的可靠消息投递聊天室。不引入 MQ，以 SQLite 为唯一持久化设施，实现：

- **消息不丢失**：先落库、再 ACK、后广播；服务重启后消息完整可补发
- **ACK 确认**：双向确认 —— 发送方收服务端 ACK（含分配的 seq）；接收方对推送做累积 ACK
- **断线补发**：重连后按 `lastSeq` 增量回放缺口，分批拉取
- **幂等去重**：`clientMsgId` 唯一约束防发送重试产生重复；客户端按 `seq` 过滤重复投递
- **消息时序可控**：每房间单调递增 `seq`，由计数器在写事务内分配，保证房间内全序
- **连接管理**：心跳保活、全局/单用户连接数上限、背压断开、优雅退出
- **群组化管理**：创建/编辑群组、公开/申请加入/私有三种准入、邀请码与定向邀请、入群申请审批
- **角色与权限**：群主（owner）/管理员（admin）/成员（member）三级角色 + 每群可配置的权限矩阵
- **成员治理**：禁言/解禁、全员禁言、设置/取消管理员、踢人、群主转让、群主解散
- **群组配置**：群组公告、最大人数（满员拒绝）、消息保留期限（定时物理清理）
- **发送限流**：按用户令牌桶

## 快速开始

```bash
npm install
npm start          # http://localhost:8080
npm test           # 40 个集成测试
```

浏览器打开 `http://localhost:8080`，用不同昵称开两个标签页即可体验：创建群组（选择准入模式、
人数上限、保留期与权限矩阵）、在「发现」中加入公开群、处理邀请与入群申请、设置管理员、
发布公告、转让/解散等。断网/刷新页面后自动重连并补发离线期间的消息。

要求 Node.js ≥ 22.13（使用内置 `node:sqlite`，唯一第三方依赖是 `ws`）。

## 架构

```
src/
├── config.js   配置（端口、连接上限、心跳、重发、限流、群组策略，均可环境变量覆盖）
├── db.js       SQLite 持久层：schema 与旧库迁移、群组/邀请/申请/公告、幂等写入、seq 分配
├── hub.js      连接注册中心：房间/用户索引、广播、定向通知、跨连接退订、心跳/重发扫描
├── server.js   HTTP + WS 服务：认证、群组协议路由、角色/权限矩阵校验、限流、生命周期
└── util.js     token 签名、帧解析等工具
public/index.html   群组化客户端（可靠投递协议 + 群组/邀请/申请/成员/公告管理界面）
test/
├── helpers.js       测试服务器与 WS 客户端夹具
├── chat.test.js     可靠投递基础协议集成测试
└── group.test.js    群组功能集成测试（准入/邀请/申请/角色/转让/解散/保留期等）
```

### 数据模型

| 表 | 说明 |
|---|---|
| `users` | 用户（演示级 token 认证） |
| `rooms` | 群组：`join_mode`（public/approval/private）、`max_members`、`retain_days`、`permissions`（JSON 权限矩阵）、`muted_until`（全员禁言）、`dissolved`，以及 `last_seq` 消息序号计数器 |
| `members` | 成员关系：`role`（owner/admin/member）+ `muted_until`（个人禁言截止时间） |
| `messages` | 消息。主键 `(room_id, seq)`；唯一键 `(room_id, sender_id, client_msg_id)` 为幂等键 |
| `cursors` | 每用户每房间已确认游标 `last_ack_seq`，断线补发的服务端兜底依据 |
| `group_invites` | 邀请：`invitee_id` 为空表示邀请码；`expires_at`（0=长期）、`max_uses`（0=不限）、`uses` |
| `join_requests` | 入群申请：一群一用户同时仅一条 pending，状态 pending/approved/rejected |
| `announcements` | 群组公告，每群仅保留最新一条（room_id 唯一，发布即覆盖） |

旧版本数据库会在启动时自动迁移：补齐新列、把 `members.role` 的 CHECK 约束扩展为
owner/admin/member、并将原建房管理员提升为 owner；历史消息与成员关系保留。

## 可靠性设计

### 1. 不丢失：持久化先于广播

发送路径在一个 SQLite 事务内完成「递增 `rooms.last_seq` 分配 seq + 写入 messages」，
**提交后**才向发送方回 ACK、向房间广播。因此：凡是客户端收到 ACK 的消息，必然已落库，
进程崩溃/重启后不丢（WAL + `synchronous=FULL`）。广播失败的连接由补发机制兜底。

### 2. 发送幂等：clientMsgId 唯一约束

客户端为每条消息生成唯一 `clientMsgId`，未收到 ACK 时以**同一 ID** 重发。
服务端命中 `(room_id, sender_id, client_msg_id)` 唯一约束时直接返回原消息的 ACK
（含原 seq），不重复写入、不重复广播。网络重试、双击、超时重发都不会产生重复消息。

### 3. 至少一次投递 + 幂等消费 = 效果上的恰好一次

- 服务端向在线连接推送消息后登记「未 ACK 队列」，超时未收到该连接的累积 ACK 则重发；
  超过最大重发次数判定连接不可用并断开，等客户端重连走补发。
- 客户端按房间维护 `lastSeenSeq`，凡是 `seq <= lastSeenSeq` 的投递一律丢弃 ——
  重发、补发重叠都不会重复上屏。

### 4. 断线补发：sync 协议

客户端持久化每个房间的 `lastSeenSeq`。重连后：

```
client → {type:'join', room, lastSeq: 41}
server → {type:'joined', ...}
server → {type:'msg', seq: 42} ... {type:'msg', seq: 57}   （缺口回放，按序）
server → {type:'sync_done', roomId, lastSeq: 57, hasMore: false}
```

`hasMore=true` 时客户端用新的 `lastSeq` 继续 `sync` 拉取下一批（单批上限
`SYNC_BATCH_SIZE`，默认 500）。`lastSeq` 缺省时使用服务端保存的确认游标
（新设备场景）；历史消息可用 `history` 向前翻页。

### 5. 时序可控

`seq` 由 `rooms.last_seq` 在写事务内递增分配（单写者 + 事务 = 无空洞、无并发交错），
房间内消息严格全序。客户端凭 seq 即可检测空洞并触发补发，无需依赖时钟。

## 协议（JSON 文本帧）

### 客户端 → 服务端

| 类型 | 字段 | 说明 |
|---|---|---|
| `ping` | `t` | 应用层心跳，回 `pong` |
| `group_create`（`create_room` 为其简写） | `name, description?, avatar?, joinMode?, maxMembers?, retainDays?, permissions?` | 创建群组，创建者为 owner，回 `joined` |
| `group_update` | `roomId, name?…`（任一群组字段） | 编辑资料/准入/人数/保留期/权限矩阵（需 editProfile） |
| `group_info` | `roomId` | 查询群组资料（私有群仅成员），回 `group_info` |
| `group_discover` | — | 公开群组发现，回 `groups` |
| `join` | `room?, code?, lastSeq?, message?` | 入群。公开群直入、申请群提交申请、私有群须带邀请 `code`；也可只带 `code`（邀请链接） |
| `leave` | `roomId` | 退群（owner 须先转让或解散） |
| `msg` | `roomId, clientMsgId, content` | 发消息（受 sendMessage 权限与禁言约束），回 `ack` |
| `ack` | `roomId, seq` | 累积确认：seq 及之前均已收到 |
| `sync` | `roomId, lastSeq?` | 请求补发 |
| `history` | `roomId, beforeSeq?, limit?` | 历史翻页（升序返回） |
| `rooms` | — | 我加入的群组列表 |
| `members` | `roomId` | 成员列表（含在线状态） |
| `mute` / `unmute` | `roomId, userId, minutes?` | 禁言/解禁普通成员（owner/admin，1..1440 分钟） |
| `group_mute_all` | `roomId, minutes` | 全员禁言（0=解除，仅 owner/admin 可发言） |
| `group_kick` | `roomId, userId` | 移出下级角色成员（需 manageMembers） |
| `group_set_admin` | `roomId, userId, make` | 设置/取消管理员（仅 owner） |
| `group_transfer` | `roomId, userId` | 转让群主（仅 owner，旧群主降为 member） |
| `group_dissolve` | `roomId` | 解散群组（仅 owner，物理清理全部数据） |
| `group_invite_create` | `roomId, userId?/userName?, ttlMs?, maxUses?` | 定向邀请（指定用户，单次）或邀请码（可多人、可限次） |
| `group_invites` | `roomId` | 群内现存邀请（需 manageMembers） |
| `group_invite_revoke` | `code` | 撤销邀请（邀请人或管理员） |
| `invites` | — | 我收到的未过期定向邀请 |
| `group_requests` | `roomId` | 群内待审批申请（owner/admin） |
| `group_request_handle` | `requestId, approve` | 批准/驳回入群申请（批准时再校验满员） |
| `my_requests` | — | 我的申请及状态 |
| `group_announce` | `roomId, content` | 发布/更新公告（空串=清除，需 postAnnouncement） |
| `group_announcement` | `roomId` | 查询当前公告 |

### 服务端 → 客户端

| 类型 | 说明 |
|---|---|
| `welcome` | 连接建立：`{userId, name, serverTime}` |
| `joined` | 入群成功：`{roomId, name, role, mutedUntil, lastSeq, group}`（含完整群组资料，并在有公告时随后下发 `announcement`） |
| `msg` | 群组消息：`{roomId, seq, clientMsgId, from, fromName, content, ts}` |
| `ack` | 发送确认：`{roomId, clientMsgId, seq, ts}` |
| `sync_done` | 一批补发结束：`{roomId, lastSeq, hasMore}` |
| `history` / `rooms` / `members` / `groups` / `group_info` | 对应查询的响应 |
| `group_updated` | 群组资料被编辑：`{roomId, group}`（群内广播） |
| `notice` | 成员/治理事件：`member_joined` `member_left` `member_kicked` `muted` `unmuted` `group_muted` `group_unmuted` `admin_promoted` `admin_demoted` `owner_transferred` |
| `kicked` | 本人被移出：`{roomId, by}`（定向到本人所有设备并退订） |
| `role_changed` | 本人角色变更：`{roomId, role, by}` |
| `group_dissolved` | 群组被解散：`{roomId, name, by}`（全体成员，客户端本地删除群组） |
| `request_submitted` / `request_received` | 申请已提交（申请人）/ 有新申请（在线管理员） |
| `request_approved` / `request_rejected` / `request_handled` | 审批结果通知申请人 / 回执操作者；满员驳回带 `reason:'GROUP_FULL'` |
| `requests` / `my_requests` | 待审批申请列表 / 我的申请 |
| `invite_created` / `invite_received` / `invites` / `group_invites` | 邀请创建回执（含 code/链接）/ 收到定向邀请 / 列表 |
| `invite_revoked` | 邀请被撤销：`{code, roomId?}` |
| `announcement` / `announcement_saved` | 公告推送（新成员入群也会补发）/ 发布回执 |
| `error` | `{code, message, ref?}`，code 见下 |
| `server_shutdown` | 服务即将关闭，请准备重连 |

错误码：`BAD_FRAME` `BAD_REQUEST` `UNKNOWN_TYPE` `NOT_MEMBER` `NO_SUCH_ROOM`
`ROOM_EXISTS` `FORBIDDEN` `MUTED` `RATE_LIMITED` `INTERNAL`；
群组相关：`GROUP_PRIVATE`（私有群须邀请）、`GROUP_FULL`（群组满员）、`GROUP_DISSOLVED`（已解散）、
`ALREADY_MEMBER`、`NO_SUCH_USER`、`NO_SUCH_REQUEST`、`REQUEST_HANDLED`、
`INVITE_NOT_FOUND` `INVITE_EXPIRED` `INVITE_DEPLETED` `INVITE_NOT_YOURS` `INVITE_MISMATCH`；
升级阶段拒绝：`401`（认证失败）、`503 SERVER_FULL` / `503 TOO_MANY_DEVICES`。

### 准入与角色规则

- **公开 public**：任何人直接加入（满员拒绝 `GROUP_FULL`）。
- **申请 approval**：提交入群申请 → owner/admin 收到 `request_received` → 批准时再次校验满员，
  通过则申请人**自动入群**（在线即时下发 `joined` 并补发），满员则驳回并通知。
- **私有 private**：仅可凭定向邀请或邀请码加入。
- 邀请：定向邀请仅被邀请人可领取且单次；邀请码可设置 `maxUses`（0=不限）与有效期（0=长期），
  过期/用尽/归属不符分别返回 `INVITE_EXPIRED` / `INVITE_DEPLETED` / `INVITE_NOT_YOURS`。
- 角色：**owner** 恒拥有全部权限且唯一，可设/撤管理员、转让、解散；**admin** 可成员治理；
  **member** 权限由群组权限矩阵决定。禁言仅作用于普通成员；踢人只能移除下级角色；
  owner 不能直接退群，须先**转让**（旧 owner 降为 member）或**解散**。

### 连接建立

```
POST /api/login {"name":"alice"}  →  {userId, name, token}
GET  /ws?token=<token>            →  WebSocket 升级
```

## 关键配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `PORT` / `HOST` | `8080` / `0.0.0.0` | 监听地址 |
| `CHAT_DB_PATH` | `chat.db` | SQLite 路径（`:memory:` 用于测试） |
| `MAX_CONNECTIONS` | `1000` | 全局并发连接上限 |
| `MAX_CONNECTIONS_PER_USER` | `3` | 单用户连接上限（多端） |
| `HEARTBEAT_INTERVAL_MS` / `HEARTBEAT_TIMEOUT_MS` | `30000` / `75000` | 心跳周期 / 判死超时 |
| `ACK_RESEND_AFTER_MS` / `ACK_MAX_RESEND` | `3000` / `5` | 未 ACK 重发阈值 / 最大次数 |
| `MAX_UNACKED_PER_CONN` | `1000` | 单连接未确认积压上限（背压） |
| `RATE_LIMIT_PER_SEC` / `RATE_LIMIT_BURST` | `10` / `20` | 发送限流令牌桶 |
| `SYNC_BATCH_SIZE` | `500` | 补发单批条数 |
| `GROUP_MAX_MEMBERS_CAP` | `5000` | 最大人数可设置的硬上限 |
| `GROUP_INVITE_TTL_MS` / `GROUP_INVITE_MAX_TTL_MS` | `7天` / `30天` | 邀请默认 / 最大有效期 |
| `RETENTION_SWEEP_MS` | `3600000` | 消息保留期清理扫描周期 |
| `GROUP_LIST_LIMIT` | `50` | 公开群组发现列表条数 |
| `AUTH_SECRET` | — | token HMAC 密钥，**生产必须设置** |

## 关键场景的消息与数据处理

- **邀请失效**：兑换时按「不存在 / 已过期 / 已用尽 / 非本人 / 群不匹配」分别返回明确错误码，
  且校验归属先于消耗（不会误扣其他群的邀请）；撤销邀请时向被邀请人推送 `invite_revoked`。
- **群组满员**：公开直入与邀请加入当场拒绝 `GROUP_FULL`；申请模式允许先提交申请，
  但管理员批准时若已无名额则自动驳回（标记 rejected、通知申请人 `reason=GROUP_FULL`），
  不会产生「批准了却进不来」的悬挂成员关系。
- **管理员权限转让**：`group_transfer` 在一个事务内将旧 owner 降为 member、新 owner 上任
  （并清除其禁言态），随后向双方所有设备定向推送 `role_changed` 并群内广播，旧 owner 方可正常退群。
- **群组解散**：先向房间内在线连接广播 `group_dissolved` 并清空订阅索引与未 ACK 队列，
  再标记并物理删除房间及成员/消息/游标/邀请/申请/公告；另向全体成员的其他在线设备兜底推送，
  客户端删除本地群组（帧按群组是否仍存在做幂等处理）。
- **消息保留期**：设置或扫描时按保留天数物理删除过期消息；`seq` 与 `last_seq` 不回收，
  客户端以现存 seq 进行补发/去重，不因清理产生错误全序假设。

## 已知边界（演示级取舍）

- 认证为演示级（用户名即账号、HMAC token），生产应替换为正式账号体系；
- 单进程架构，多实例部署需引入外部 Pub/Sub 做跨节点广播（DB 层无需改动）；
- 消息保留期为小时级定时扫描的**物理删除**（非按用户可见性过滤），删除后历史不可翻回；
- 解散为硬删除且不可逆，未做回收站/冷静期；邀请不做推送唤醒（不在线时上线后经 `invites` 拉取）。
