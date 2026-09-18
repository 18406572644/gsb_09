# 群组聊天平台（Node + ws + SQLite）

基于 WebSocket 的可靠消息投递 **群组化聊天平台**。不引入 MQ，以 SQLite 为唯一持久化设施，实现：

- **消息不丢失**：先落库、再 ACK、后广播；服务重启后消息完整可补发
- **ACK 确认**：双向确认 —— 发送方收服务端 ACK（含分配的 seq）；接收方对推送做累积 ACK
- **断线补发**：重连后按 `lastSeq` 增量回放缺口，分批拉取
- **幂等去重**：`clientMsgId` 唯一约束防发送重试产生重复；客户端按 `seq` 过滤重复投递
- **消息时序可控**：每群单调递增 `seq`，由计数器在写事务内分配，保证群内全序
- **连接管理**：心跳保活、全局/单用户连接数上限、背压断开、优雅退出
- **群组治理**：群主 / 管理员 / 成员三级角色，自定义权限矩阵，禁言与全员禁言
- **三种准入模式**：公开（直接加入）、申请加入（管理员审批）、私有（凭邀请）
- **邀请体系**：定向邀请、有效期、接受/拒绝/撤销/过期失效
- **群组配置**：群资料、人数上限、消息保留期限、群组公告
- **成员生命周期**：邀请、审批、设置管理员、群主转让、踢人、退群、解散（级联清理）
- **发送限流**：按用户令牌桶

## 快速开始

```bash
npm install
npm start          # http://localhost:8080
npm test           # 27 个集成测试
```

浏览器打开 `http://localhost:8080`，用不同昵称开多个标签页即可体验：建群（选择准入
模式）、发现群组、发邀请、审批申请、公告、权限矩阵、转让群主、解散等。断网/刷新页面
后自动重连并补发离线期间的消息。

要求 Node.js ≥ 22.13（使用内置 `node:sqlite`，唯一第三方依赖是 `ws`）。

## 架构

```
src/
├── config.js   配置（端口、连接上限、心跳、重发、限流、群组参数，均可环境变量覆盖）
├── db.js       SQLite 持久层：schema + user_version 迁移、幂等写入、seq、游标、
│               群组/成员/邀请/申请、保留期清扫
├── hub.js      连接注册中心：群索引、广播、按用户定向投递、强制退订、关群、心跳/重发
├── perms.js    角色与权限：权限目录、默认矩阵、稀疏覆盖解析、can() 判定
├── server.js   HTTP + WS 服务：认证、协议路由、权限校验、限流、生命周期
└── util.js     token 签名、帧解析等工具
public/index.html   群组管理客户端（完整可靠投递协议 + 群组 UI）
test/chat.test.js   集成测试（node:test）
```

### 数据模型（schema user_version = 1）

| 表 | 说明 |
|---|---|
| `users` | 用户（演示级 token 认证） |
| `rooms` | 群组：`name`、`description`、`join_mode`(open/apply/private)、`max_members`、`retention_days`、`mute_all_until`、`announcement(_at/_by)`、`permissions`(JSON 覆盖)、`created_by`、`last_seq` |
| `members` | 成员关系：`role`(**owner**/admin/member) + `muted_until`；复合主键 `(room_id,user_id)` |
| `messages` | 消息。主键 `(room_id, seq)`；唯一键 `(room_id, sender_id, client_msg_id)` 为幂等键 |
| `cursors` | 每用户每群已确认游标，断线补发的服务端兜底依据 |
| `invitations` | 邀请：`invitee/inviter`、`message`、`expires_at`、`status`(pending/accepted/declined/revoked/expired)；部分唯一索引保证每人每群至多一条 pending |
| `join_requests` | 入群申请：`message`、`status`(pending/approved/rejected)、`decided_by`；同样有 pending 部分唯一索引 |

成员、消息、游标、邀请、申请均对 `rooms` 设 `ON DELETE CASCADE` —— **解散群组即一条
DELETE 级联清除全部关联数据**。

旧版（v0，admin/member 两角色）数据库启动时自动迁移：rooms 补群设置列、members 等子表
重建为三级角色并补 CASCADE、原房间创建者由 admin 升级为 owner，历史消息与游标完整保留。

### 角色与权限

- **owner（群主，唯一）**：权限恒定全部允许，不可通过权限矩阵收窄；只能通过转让更换。
- **admin（管理员）**：默认可发公告、邀请、审批申请、禁言、踢人；权限可由群主在矩阵中收窄。
- **member（成员）**：默认仅可发言、被邀请入群。

八项可配置权限：`msg.send`、`group.announce`、`group.edit`、`member.invite`、
`request.review`、`member.promote`、`member.mute`、`member.kick`。群 `permissions` 字段
保存「相对默认矩阵的稀疏覆盖」（仅记录与默认值不同的勾选项），owner 不允许出现在覆盖中。
禁言/踢人/角色变更除校验权限外，还要求操作者角色等级 **严格高于** 目标（管理员不能互禁、
不能踢群主）。

## 可靠性设计

### 不丢失 / 幂等 / 恰好一次 / 补发 / 全序

与群组化之前一致：写事务内「递增 `rooms.last_seq` + 写 messages」提交后才 ACK 并广播；
`clientMsgId` 唯一约束保证发送重试幂等；未 ACK 队列超时重发（超限断连）+ 客户端按 seq
去重消费达到效果上恰好一次；重连发 `join`（带 `lastSeq`）触发分批 `sync_done` 回放。
详见下文协议。群内新增的所有控制事件（禁言、公告、角色变更、成员变动、解散）通过
`notice` / 专用帧即时广播，不占用消息 seq。

## 协议（JSON 文本帧）

### 客户端 → 服务端

| 类型 | 字段 | 说明 |
|---|---|---|
| `ping` | `t` | 应用层心跳，回 `pong` |
| `create_room` | `name, description?, joinMode?, maxMembers?, retentionDays?` | 建群，创建者为 owner，回 `joined` |
| `discover` | `keyword?, limit?` | 发现公开/申请群（排除已加入、私有群） |
| `group_info` | `room` | 群资料（成员附有效权限表，非成员仅公开字段） |
| `join` | `room, lastSeq?, message?` | 公开群直接加入；申请群转为提交申请；私有群报 `JOIN_PRIVATE`；已是成员则幂等重入+补发 |
| `leave` | `roomId` | 退群（删除成员关系并退订）；群主须先转让或解散 |
| `msg` | `roomId, clientMsgId, content` | 发消息（受权限/禁言/全员禁言/限流约束），回 `ack` |
| `ack` / `sync` / `history` | 同原协议 | 累积确认 / 请求补发 / 历史翻页 |
| `rooms` | — | 我加入的群列表（含群设置、我的角色与有效权限） |
| `members` | `roomId` | 成员列表（含角色、禁言、在线状态） |
| `edit_group` | `roomId, name?, description?` | 编辑群资料（`group.edit`） |
| `group_settings` | `roomId, joinMode?, maxMembers?, retentionDays?` | 准入模式/人数上限/保留期（`group.edit`） |
| `set_permissions` | `roomId, permissions` | 角色权限稀疏覆盖 `{admin:{…}, member:{…}}` |
| `announcement` | `roomId, content` | 发布/清空公告（`group.announce`） |
| `mute_all` | `roomId, minutes` | 全员禁言（0 关闭，否则 1..1440 分钟；管理员及以上豁免） |
| `mute` / `unmute` | `roomId, userId, minutes?` | 单人禁言/解禁（`member.mute`，不可对同级或上级） |
| `set_admin` | `roomId, userId, isAdmin` | 设置/撤销管理员（`member.promote`） |
| `transfer_owner` | `roomId, userId` | 转让群主（仅 owner；旧群主降为 admin） |
| `kick` | `roomId, userId` | 移出成员（`member.kick`；目标所有在线连接立即收 `removed` 并退订） |
| `disband` | `roomId` | 解散群组（仅 owner；全员收 `group_disbanded`，数据级联删除） |
| `invite` | `roomId, user, ttlMinutes?, message?` | 按昵称/ID 邀请；同对象已有 pending 邀请时幂等返回 |
| `invitations` | — | 我的待处理邀请 |
| `accept_invite` / `decline_invite` | `invitationId, lastSeq?` | 接受（满员报 `ROOM_FULL`，过期报 `INVITATION_EXPIRED`）/ 拒绝 |
| `revoke_invite` | `invitationId` | 撤销邀请（邀请人本人或群管理员） |
| `requests` | `roomId` | 群的待审批入群申请（`request.review`） |
| `approve_request` / `reject_request` | `requestId` | 审批；通过时若申请人在线则自动下发 `joined` 并入群 |

### 服务端 → 客户端

| 类型 | 说明 |
|---|---|
| `welcome` | 连接建立：`{userId, name, serverTime}` |
| `joined` | 入群成功：`{roomId, name, role, mutedUntil, lastSeq, group, permissions}`（附完整群设置与本人有效权限表） |
| `msg` / `ack` / `sync_done` / `history` / `rooms` / `members` | 消息投递与查询响应 |
| `discover` / `group_info` | 群组发现 / 资料 |
| `announcement` | 新公告：`{roomId, content, at, by, byName}` |
| `invitation` / `invite_sent` / `invitations` | 收到邀请 / 邀请发出回执 / 邀请列表 |
| `invitation_accepted` / `invitation_declined` / `invitation_revoked` | 邀请流转通知（定向投递邀请人/被邀请人） |
| `join_requested` | 申请已提交：`{requestId, roomId, status}`（重复申请返回同一条 pending） |
| `request_created` | 新申请通知（定向投递给全部有审批权的管理员） |
| `requests` | 待审批申请列表 |
| `request_approved` / `request_rejected` | 审批结果（定向投递申请人；在线批准时随后自动收到 `joined`） |
| `removed` | 被移出群：`{roomId, name, by}` |
| `group_disbanded` | 群已解散：`{roomId, name, by}` |
| `group_updated` | 群资料/设置/权限已更新，附新 `group` 视图（权限覆盖变更时另附 `permissions`） |
| `left` | 退群应答 |
| `notice` | 群内广播事件：`member_joined` / `member_left` / `member_removed` / `muted` / `unmuted` / `mute_all_on` / `mute_all_off` / `role_changed` / `owner_changed` / `settings_changed` / `permissions_changed` |
| `error` | `{code, message, ref?}`，ref 尽量关联 `clientMsgId`/`roomId`/`invitationId`/`requestId` |
| `server_shutdown` | 服务即将关闭，请准备重连 |

### 错误码

通用：`BAD_FRAME` `BAD_REQUEST` `UNKNOWN_TYPE` `INTERNAL`
群与成员：`NOT_MEMBER` `NO_SUCH_ROOM` `ROOM_EXISTS` `FORBIDDEN` `MUTED` `RATE_LIMITED`
群组化新增：

| 码 | 含义 |
|---|---|
| `JOIN_PRIVATE` | 私有群不允许直接加入，需要邀请 |
| `ROOM_FULL` | 群达到人数上限（直接加入/邀请接受/申请批准时校验） |
| `OWNER_MUST_TRANSFER` | 群主不能直接退群，须先转让或解散 |
| `USER_NOT_FOUND` / `ALREADY_MEMBER` | 邀请对象不存在 / 已是成员 |
| `INVITATION_NOT_FOUND` / `INVITATION_INVALID` / `INVITATION_EXPIRED` | 邀请不存在 / 已被接受、拒绝、撤销 / 已过期（接受时落库为 expired） |
| `REQUEST_INVALID` | 入群申请不存在或已处理 |

升级阶段拒绝：`401`（认证失败）、`503 SERVER_FULL` / `503 TOO_MANY_DEVICES`。

### 关键场景的通知与数据处理

- **邀请失效**：接受时若状态非 pending 返回 `INVITATION_INVALID`；超过 `expires_at` 返回
  `INVITATION_EXPIRED` 并把状态置为 expired；撤销后被邀请人收到 `invitation_revoked`；
  群在接受前被解散则返回 `NO_SUCH_ROOM`。
- **群组满员**：公开加入、邀请接受、申请批准三条入群路径统一先做容量校验，满员时申请
  保持 pending（可待群扩容或有人退出后重试审批），邀请不被消耗。
- **群主转让**：事务内旧群主降 admin、目标升 owner 并更新 `created_by`，群广播
  `owner_changed`；转让后旧群主失去解散/踢新群主等 owner 能力。
- **解散**：仅 owner；Hub 先向所有订阅连接广播 `group_disbanded` 并清空内存房间索引，
  再删除 rooms 行，members/messages/cursors/invitations/join_requests 由外键级联删除。
- **踢人**：目标用户的全部在线连接收到 `removed` 并立即退订（即使重连也因不再是成员而
  无法订阅），群内广播 `member_removed`，其 pending 入群申请一并作废。

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
| `MAX_CONNECTIONS` / `MAX_CONNECTIONS_PER_USER` | `1000` / `3` | 全局 / 单用户连接上限 |
| `HEARTBEAT_INTERVAL_MS` / `HEARTBEAT_TIMEOUT_MS` | `30000` / `75000` | 心跳周期 / 判死超时 |
| `ACK_RESEND_AFTER_MS` / `ACK_MAX_RESEND` | `3000` / `5` | 未 ACK 重发阈值 / 最大次数 |
| `MAX_UNACKED_PER_CONN` | `1000` | 单连接未确认积压上限（背压） |
| `RATE_LIMIT_PER_SEC` / `RATE_LIMIT_BURST` | `10` / `20` | 发送限流令牌桶 |
| `SYNC_BATCH_SIZE` / `HISTORY_MAX_LIMIT` | `500` / `100` | 补发单批 / 历史翻页上限 |
| `GROUP_DEFAULT_MAX_MEMBERS` / `GROUP_MAX_MEMBERS_HARD_LIMIT` | `200` / `10000` | 建群默认人数 / 人数硬顶 |
| `MAX_ANNOUNCEMENT_LENGTH` | `2000` | 公告最大字符数 |
| `DISCOVER_LIMIT` | `50` | 发现群组单次条数 |
| `INVITE_DEFAULT_TTL_MINUTES` / `INVITE_MIN_TTL_MINUTES` / `INVITE_MAX_TTL_MINUTES` | `10080` / `1` / `43200` | 邀请有效期默认 / 下限 / 上限（分钟） |
| `RETENTION_SWEEP_INTERVAL_MS` | `3600000` | 过期消息清扫周期（按各群 `retention_days`） |
| `AUTH_SECRET` | — | token HMAC 密钥，**生产必须设置** |

## 已知边界（演示级取舍）

- 认证为演示级（用户名即账号、HMAC token），生产应替换为正式账号体系；
- 单进程架构，多实例部署需引入外部 Pub/Sub 做跨节点广播（DB 层无需改动）；
- 消息保留期按时间清扫（默认每小时一次），不是按条数；清扫不回收 seq，补发游标依旧有效。
