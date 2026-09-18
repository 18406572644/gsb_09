'use strict';

/**
 * 全局配置。全部支持环境变量覆盖，便于测试与部署。
 */
module.exports = {
  // 服务监听
  port: Number(process.env.PORT || 8080),
  host: process.env.HOST || '0.0.0.0',

  // SQLite 文件路径，':memory:' 仅用于测试
  dbPath: process.env.CHAT_DB_PATH || 'chat.db',

  // 连接管理
  maxConnections: Number(process.env.MAX_CONNECTIONS || 1000), // 全局最大并发连接
  maxConnectionsPerUser: Number(process.env.MAX_CONNECTIONS_PER_USER || 3), // 单用户最大连接（多端）
  heartbeatIntervalMs: Number(process.env.HEARTBEAT_INTERVAL_MS || 30_000), // ping 周期
  heartbeatTimeoutMs: Number(process.env.HEARTBEAT_TIMEOUT_MS || 75_000), // 超过该时长无 pong 判定死亡

  // 可靠投递
  ackResendIntervalMs: Number(process.env.ACK_RESEND_INTERVAL_MS || 2_000), // 未 ACK 重发扫描周期
  ackResendAfterMs: Number(process.env.ACK_RESEND_AFTER_MS || 3_000), // 发送后多久未收到 ACK 触发重发
  ackMaxResend: Number(process.env.ACK_MAX_RESEND || 5), // 单条消息最大重发次数，超限断开连接
  maxUnackedPerConn: Number(process.env.MAX_UNACKED_PER_CONN || 1_000), // 单连接未 ACK 积压上限（背压）

  // 消息
  maxContentLength: Number(process.env.MAX_CONTENT_LENGTH || 4_000), // 单条消息最大字符数
  syncBatchSize: Number(process.env.SYNC_BATCH_SIZE || 500), // 断线补发单批最大条数
  historyMaxLimit: Number(process.env.HISTORY_MAX_LIMIT || 100), // 历史消息单次拉取上限

  // 发送限流（令牌桶，按用户）
  rateLimitPerSec: Number(process.env.RATE_LIMIT_PER_SEC || 10),
  rateLimitBurst: Number(process.env.RATE_LIMIT_BURST || 20),

  // 群组管理
  defaultMaxMembers: Number(process.env.GROUP_DEFAULT_MAX_MEMBERS || 200), // 建群时默认人数上限
  maxMembersHardLimit: Number(process.env.GROUP_MAX_MEMBERS_HARD_LIMIT || 10_000), // 人数上限硬顶
  maxDescriptionLength: 500, // 群简介最大字符数
  maxAnnouncementLength: Number(process.env.MAX_ANNOUNCEMENT_LENGTH || 2_000), // 公告最大字符数
  discoverLimit: Number(process.env.DISCOVER_LIMIT || 50), // 发现群组单次返回条数
  inviteDefaultTtlMinutes: Number(process.env.INVITE_DEFAULT_TTL_MINUTES || 60 * 24 * 7), // 邀请有效期默认 7 天
  inviteMinTtlMinutes: Number(process.env.INVITE_MIN_TTL_MINUTES || 1), // 邀请有效期下限（分钟，测试可调 0）
  inviteMaxTtlMinutes: Number(process.env.INVITE_MAX_TTL_MINUTES || 60 * 24 * 30), // 邀请有效期上限 30 天
  retentionSweepIntervalMs: Number(process.env.RETENTION_SWEEP_INTERVAL_MS || 3_600_000), // 过期消息清扫周期 1h

  // 演示用鉴权：token 签名密钥（生产环境务必替换）
  authSecret: process.env.AUTH_SECRET || 'dev-secret-change-me',
};
