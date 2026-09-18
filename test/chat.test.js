'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const WebSocket = require('ws');
const { createChatServer } = require('../src/server');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** 启动一个隔离的测试服务器（内存库、随机端口、默认关闭重发以免干扰计数） */
async function startServer(overrides = {}) {
  const server = createChatServer({
    port: 0,
    host: '127.0.0.1',
    dbPath: ':memory:',
    heartbeatIntervalMs: 60_000,
    ackResendAfterMs: 60_000, // 默认不在测试内重发；重发场景单独配置
    ...overrides,
  });
  const addr = await server.start();
  return { server, port: addr.port };
}

async function login(port, name) {
  const res = await fetch(`http://127.0.0.1:${port}/api/login`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', connection: 'close' },
    body: JSON.stringify({ name }),
  });
  assert.equal(res.status, 200);
  return res.json();
}

/** 测试客户端：手动 ACK（测试可控）。log 全量记录供断言；waitFor 消费式匹配（每帧至多满足一个等待者） */
class Client {
  static async connect(port, token) {
    const c = new Client();
    c.log = []; // 全部帧（断言用）
    c.pending = []; // 未被 waitFor 消费的帧
    c.waiters = [];
    c.closed = new Promise((res) => (c._onClosed = res));
    c.ws = new WebSocket(`ws://127.0.0.1:${port}/ws?token=${encodeURIComponent(token)}`);
    c.ws.on('message', (raw) => {
      const m = JSON.parse(raw.toString());
      c.log.push(m);
      for (const w of [...c.waiters]) {
        if (w.pred(m)) {
          c.waiters.splice(c.waiters.indexOf(w), 1);
          clearTimeout(w.timer);
          w.resolve(m);
          return;
        }
      }
      c.pending.push(m);
    });
    c.ws.on('close', () => c._onClosed());
    await new Promise((res, rej) => {
      c.ws.once('open', res);
      c.ws.once('error', rej);
    });
    await c.waitFor((m) => m.type === 'welcome');
    return c;
  }

  send(obj) {
    this.ws.send(JSON.stringify(obj));
  }

  waitFor(pred, timeout = 3000) {
    const idx = this.pending.findIndex(pred);
    if (idx >= 0) {
      const [m] = this.pending.splice(idx, 1);
      return Promise.resolve(m);
    }
    return new Promise((resolve, reject) => {
      const w = { pred, resolve };
      w.timer = setTimeout(() => {
        reject(new Error('waitFor: timed out'));
      }, timeout);
      this.waiters.push(w);
    });
  }

  /** 已收到的某房间消息帧（seq 列表） */
  roomSeqs(roomId) {
    return this.log.filter((m) => m.type === 'msg' && m.roomId === roomId).map((m) => m.seq);
  }

  close() {
    this.ws.close();
    return this.closed;
  }
}

async function createRoom(client, name) {
  client.send({ type: 'create_room', name });
  const joined = await client.waitFor((m) => m.type === 'joined' && m.name === name);
  return joined.roomId;
}

async function createGroup(client, name, opts = {}) {
  client.send({ type: 'create_room', name, ...opts });
  const joined = await client.waitFor((m) => m.type === 'joined' && m.name === name);
  return joined;
}

async function joinRoom(client, room, lastSeq = 0) {
  client.send({ type: 'join', room, lastSeq });
  return client.waitFor((m) => m.type === 'joined');
}

/** 从调用时刻之后到达的帧中等待匹配（避免被历史 error/notice 帧污染断言） */
async function waitForNew(client, pred, timeout = 3000) {
  const start = client.log.length;
  const deadline = Date.now() + timeout;
  for (;;) {
    for (let i = start; i < client.log.length; i++) {
      if (pred(client.log[i])) return client.log[i];
    }
    if (Date.now() > deadline) {
      throw new Error('waitForNew: timed out; tail=' + JSON.stringify(client.log.slice(-5)));
    }
    await sleep(15);
  }
}

const nextError = (client) => waitForNew(client, (m) => m.type === 'error');
const nextNotice = (client, event, roomId) =>
  waitForNew(client, (m) => m.type === 'notice' && m.event === event && (!roomId || m.roomId === roomId));

// ---------------------------------------------------------------- 测试用例

test('登录、连接、建群后成为群主', async () => {
  const { server, port } = await startServer();
  try {
    const u = await login(port, 'alice');
    assert.ok(u.userId && u.token);
    const a = await Client.connect(port, u.token);
    const roomId = await createRoom(a, 'general');
    const joined = a.log.find((m) => m.type === 'joined');
    assert.equal(joined.role, 'owner');
    assert.ok(roomId);
    await a.close();
  } finally {
    server.stop();
  }
});

test('发送收到 ACK，房间内广播按 seq 全序投递', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    for (let i = 1; i <= 3; i++) {
      a.send({ type: 'msg', roomId, clientMsgId: `m${i}`, content: `hello ${i}` });
    }
    // 发送者收到 3 个 ACK，seq 递增
    for (let i = 1; i <= 3; i++) {
      const ack = await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === `m${i}`);
      assert.equal(ack.seq, i);
    }
    // 接收者按序收到 1,2,3
    await b.waitFor((m) => m.type === 'msg' && m.roomId === roomId && m.seq === 3);
    assert.deepEqual(b.roomSeqs(roomId), [1, 2, 3]);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('重复 clientMsgId 幂等：返回同一 seq，不重复广播', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'msg', roomId, clientMsgId: 'dup-1', content: 'hello' });
    const ack1 = await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'dup-1');
    // 网络重试：同 clientMsgId 重发
    a.send({ type: 'msg', roomId, clientMsgId: 'dup-1', content: 'hello' });
    const ack2 = await a.waitFor(
      (m) => m.type === 'ack' && m.clientMsgId === 'dup-1' && m !== ack1
    );
    assert.equal(ack1.seq, ack2.seq);

    await b.waitFor((m) => m.type === 'msg' && m.roomId === roomId);
    await sleep(300);
    assert.deepEqual(b.roomSeqs(roomId), [1], '接收端只应收到一次广播');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('断线补发：重连后按序补齐离线期间的消息，且不重复', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    let b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'msg', roomId, clientMsgId: 'm1', content: 'online' });
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1);
    await b.close(); // —— B 掉线 ——

    for (const [i, c] of [2, 3, 4].entries()) {
      a.send({ type: 'msg', roomId, clientMsgId: `m${i + 2}`, content: `offline ${c}` });
    }
    await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'm4');

    // —— B 重连，携带本地进度 lastSeq=1 ——
    b = await Client.connect(port, ub.token);
    await joinRoom(b, roomId, 1);
    await b.waitFor((m) => m.type === 'sync_done' && m.roomId === roomId);
    assert.deepEqual(b.roomSeqs(roomId), [2, 3, 4], '补发且仅补发缺口，按序到达');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('已追平的连接重连后不再收到旧消息', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    let b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'msg', roomId, clientMsgId: 'm1', content: 'hi' });
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1);
    await b.close();

    b = await Client.connect(port, ub.token);
    await joinRoom(b, roomId, 1); // 已追平
    await sleep(300);
    assert.deepEqual(b.roomSeqs(roomId), [], '不应有任何补发');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('服务端对未 ACK 消息重发，ACK 后停止', async () => {
  const { server, port } = await startServer({
    ackResendIntervalMs: 50,
    ackResendAfterMs: 100,
    ackMaxResend: 10,
  });
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'msg', roomId, clientMsgId: 'm1', content: 'hi' });
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1);
    // 不 ACK，等服务端重发
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1, 2000);
    assert.ok(b.roomSeqs(roomId).length >= 2, '应观察到至少一次重发');

    b.send({ type: 'ack', roomId, seq: 1 });
    await sleep(100);
    const countAfterAck = b.roomSeqs(roomId).length;
    await sleep(400);
    assert.equal(b.roomSeqs(roomId).length, countAfterAck, 'ACK 后不应再有重发');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('禁言：管理员可禁言/解禁，被禁言者发送被拒', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'mute', roomId, userId: ub.userId, minutes: 10 });
    await b.waitFor((m) => m.type === 'notice' && m.event === 'muted' && m.userId === ub.userId);

    b.send({ type: 'msg', roomId, clientMsgId: 'x1', content: 'am i muted?' });
    const err = await b.waitFor((m) => m.type === 'error');
    assert.equal(err.code, 'MUTED');

    a.send({ type: 'unmute', roomId, userId: ub.userId });
    await b.waitFor((m) => m.type === 'notice' && m.event === 'unmuted');

    b.send({ type: 'msg', roomId, clientMsgId: 'x2', content: 'free again' });
    const ack = await b.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'x2');
    assert.equal(ack.seq, 1);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('权限：普通成员不能禁言他人，管理员不可被禁言', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const uc = await login(port, 'carol');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const c = await Client.connect(port, uc.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);
    await joinRoom(c, roomId);

    b.send({ type: 'mute', roomId, userId: uc.userId, minutes: 5 });
    const err1 = await b.waitFor((m) => m.type === 'error');
    assert.equal(err1.code, 'FORBIDDEN');

    a.send({ type: 'mute', roomId, userId: ua.userId, minutes: 5 });
    const err2 = await a.waitFor((m) => m.type === 'error');
    assert.equal(err2.code, 'FORBIDDEN');
    await Promise.all([a.close(), b.close(), c.close()]);
  } finally {
    server.stop();
  }
});

test('连接数限制：单用户连接数超限被拒绝', async () => {
  const { server, port } = await startServer({ maxConnectionsPerUser: 2 });
  try {
    const u = await login(port, 'alice');
    const c1 = await Client.connect(port, u.token);
    const c2 = await Client.connect(port, u.token);
    await assert.rejects(
      Client.connect(port, u.token),
      /503|TOO_MANY_DEVICES|Unexpected server response/
    );
    await c1.close();
    await c2.close();
  } finally {
    server.stop();
  }
});

test('发送限流：突发超过令牌桶被拒绝', async () => {
  const { server, port } = await startServer({ rateLimitPerSec: 1, rateLimitBurst: 2 });
  try {
    const u = await login(port, 'alice');
    const a = await Client.connect(port, u.token);
    const roomId = await createRoom(a, 'general');

    for (let i = 0; i < 5; i++) {
      a.send({ type: 'msg', roomId, clientMsgId: `r${i}`, content: `spam ${i}` });
    }
    const err = await a.waitFor((m) => m.type === 'error' && m.code === 'RATE_LIMITED');
    assert.ok(err);
    await sleep(300);
    const ackCount = a.log.filter((m) => m.type === 'ack').length;
    assert.equal(ackCount, 2, '突发容量为 2，其余应被限流');
    await a.close();
  } finally {
    server.stop();
  }
});

test('历史消息分页拉取', async () => {
  const { server, port } = await startServer();
  try {
    const u = await login(port, 'alice');
    const a = await Client.connect(port, u.token);
    const roomId = await createRoom(a, 'general');
    for (let i = 1; i <= 5; i++) {
      a.send({ type: 'msg', roomId, clientMsgId: `h${i}`, content: `msg ${i}` });
    }
    await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'h5');

    a.send({ type: 'history', roomId, beforeSeq: 4, limit: 2 });
    const h = await a.waitFor((m) => m.type === 'history');
    assert.deepEqual(h.messages.map((m) => m.seq), [2, 3], '升序返回 beforeSeq 之前的一页');
    assert.equal(h.hasMore, true);
    await a.close();
  } finally {
    server.stop();
  }
});

test('服务端游标兜底：新设备不带 lastSeq 时从已确认进度继续', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    let b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'msg', roomId, clientMsgId: 'm1', content: 'first' });
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1);
    b.send({ type: 'ack', roomId, seq: 1 }); // 上报确认进度
    await sleep(100);
    await b.close();

    a.send({ type: 'msg', roomId, clientMsgId: 'm2', content: 'second' });
    await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'm2');

    // 新设备重连，不带 lastSeq —— 应使用服务端游标，只补 seq 2
    b = await Client.connect(port, ub.token);
    b.send({ type: 'join', room: roomId });
    await b.waitFor((m) => m.type === 'sync_done' && m.roomId === roomId);
    assert.deepEqual(b.roomSeqs(roomId), [2]);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('持久化：服务重启后消息不丢失', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'chat-test-'));
  const dbPath = path.join(dir, 'test.db');
  try {
    let token, roomId;
    {
      const { server, port } = await startServer({ dbPath });
      const u = await login(port, 'alice');
      token = u.token;
      const a = await Client.connect(port, token);
      roomId = await createRoom(a, 'persist');
      for (let i = 1; i <= 3; i++) {
        a.send({ type: 'msg', roomId, clientMsgId: `p${i}`, content: `durable ${i}` });
      }
      await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'p3');
      await a.close();
      server.stop();
    }
    {
      const { server, port } = await startServer({ dbPath });
      const a = await Client.connect(port, token); // 同一 token 仍有效
      await joinRoom(a, roomId, 0);
      await a.waitFor((m) => m.type === 'sync_done' && m.roomId === roomId);
      assert.deepEqual(a.roomSeqs(roomId), [1, 2, 3], '重启后历史消息完整可补发');
      await a.close();
      server.stop();
    }
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

// ============================================================ 群组化功能测试

async function threeUsers(port) {
  const ua = await login(port, 'alice');
  const ub = await login(port, 'bob');
  const uc = await login(port, 'carol');
  const [a, b, c] = await Promise.all([
    Client.connect(port, ua.token),
    Client.connect(port, ub.token),
    Client.connect(port, uc.token),
  ]);
  return { ua, ub, uc, a, b, c };
}

test('准入模式：公开群直接加入，私有群拒绝直接加入', async () => {
  const { server, port } = await startServer();
  try {
    const { a, b } = await threeUsers(port);
    const open = await createGroup(a, 'open-g', { joinMode: 'open' });
    const priv = await createGroup(a, 'priv-g', { joinMode: 'private' });

    b.send({ type: 'join', room: open.roomId, lastSeq: 0 });
    await b.waitFor((m) => m.type === 'joined' && m.roomId === open.roomId);
    await nextNotice(a, 'member_joined', open.roomId);

    b.send({ type: 'join', room: priv.roomId });
    const err = await nextError(b);
    assert.equal(err.code, 'JOIN_PRIVATE');
    await Promise.all([a.close(), b.close()]);
  } finally {
    server.stop();
  }
});

test('申请加入：提交申请、在线审批后自动入群；拒绝则收到 request_rejected', async () => {
  const { server, port } = await startServer();
  try {
    const t = await threeUsers(port);
    const { a, b, c } = t;
    const g = await createGroup(a, 'apply-g', { joinMode: 'apply' });

    // bob 申请，alice 收到 request_created
    b.send({ type: 'join', room: g.roomId, message: 'please' });
    const jreq = await b.waitFor((m) => m.type === 'join_requested');
    assert.equal(jreq.status, 'pending');
    await a.waitFor((m) => m.type === 'request_created' && m.request.userName === 'bob');

    // 重复申请幂等：返回同一条 pending 申请
    b.send({ type: 'join', room: g.roomId });
    const jreqAgain = await waitForNew(b, (m) => m.type === 'join_requested');
    assert.equal(jreqAgain.requestId, jreq.requestId);
    assert.equal(jreqAgain.status, 'pending');

    // carol 也申请；alice 拒绝 carol
    c.send({ type: 'join', room: g.roomId });
    await a.waitFor((m) => m.type === 'request_created' && m.request.userName === 'carol');
    a.send({ type: 'requests', roomId: g.roomId });
    const list = await a.waitFor((m) => m.type === 'requests');
    assert.equal(list.requests.length, 2);
    const carolReq = list.requests.find((q) => q.userName === 'carol');
    a.send({ type: 'reject_request', requestId: carolReq.id });
    await c.waitFor((m) => m.type === 'request_rejected');

    // 通过 bob：bob 在线，自动收到 joined（无需再发 join）
    const bobReq = list.requests.find((q) => q.userName === 'bob');
    a.send({ type: 'approve_request', requestId: bobReq.id });
    await b.waitFor((m) => m.type === 'joined' && m.roomId === g.roomId);
    await b.waitFor((m) => m.type === 'request_approved');
    await nextNotice(a, 'member_joined', g.roomId);

    // 已处理的申请再次审批报错
    a.send({ type: 'approve_request', requestId: bobReq.id });
    assert.equal((await nextError(a)).code, 'REQUEST_INVALID');
    await Promise.all([a.close(), b.close(), c.close()]);
  } finally {
    server.stop();
  }
});

test('邀请：发送、接受入群；撤销后接受失效；重复邀请幂等', async () => {
  const { server, port } = await startServer();
  try {
    const t = await threeUsers(port);
    const { a, b } = t;
    const g = await createGroup(a, 'inv-g', { joinMode: 'private' });

    a.send({ type: 'invite', roomId: g.roomId, user: 'bob', message: 'join us' });
    const ivFrame = await b.waitFor((m) => m.type === 'invitation');
    const invId = ivFrame.invitation.id;
    assert.equal(ivFrame.invitation.roomName, 'inv-g');
    await a.waitFor((m) => m.type === 'invite_sent');

    // 再次邀请同一人：返回同一邀请（幂等）
    a.send({ type: 'invite', roomId: g.roomId, user: 'bob' });
    const iv2 = await b.waitFor((m) => m.type === 'invitation');
    assert.equal(iv2.invitation.id, invId);

    // bob 接受 → 入群，alice 收到 invitation_accepted
    b.send({ type: 'accept_invite', invitationId: invId, lastSeq: 0 });
    await b.waitFor((m) => m.type === 'joined' && m.roomId === g.roomId);
    await a.waitFor((m) => m.type === 'invitation_accepted');

    // 已接受的邀请不能再接受
    b.send({ type: 'accept_invite', invitationId: invId });
    assert.equal((await nextError(b)).code, 'INVITATION_INVALID');

    // 撤销路径：邀请 carol 后撤销，carol 接受报失效
    a.send({ type: 'invite', roomId: g.roomId, user: 'carol' });
    const iv3 = await t.c.waitFor((m) => m.type === 'invitation');
    a.send({ type: 'revoke_invite', invitationId: iv3.invitation.id });
    await t.c.waitFor((m) => m.type === 'invitation_revoked');
    t.c.send({ type: 'accept_invite', invitationId: iv3.invitation.id });
    assert.equal((await nextError(t.c)).code, 'INVITATION_INVALID');
    await Promise.all([a.close(), b.close(), t.c.close()]);
  } finally {
    server.stop();
  }
});

test('邀请过期：接受超过有效期的邀请报 INVITATION_EXPIRED', async () => {
  const { server, port } = await startServer();
  try {
    const t = await threeUsers(port);
    const { a, b } = t;
    const g = await createGroup(a, 'exp-g', { joinMode: 'private' });
    a.send({ type: 'invite', roomId: g.roomId, user: 'bob' });
    const iv = await b.waitFor((m) => m.type === 'invitation');
    // 直接把 expires_at 改成过去时刻模拟过期
    server.db.db.prepare('UPDATE invitations SET expires_at = 1 WHERE id = ?').run(iv.invitation.id);
    b.send({ type: 'accept_invite', invitationId: iv.invitation.id });
    assert.equal((await nextError(b)).code, 'INVITATION_EXPIRED');
    await Promise.all([a.close(), b.close(), t.c.close()]);
  } finally {
    server.stop();
  }
});

test('群组满员：公开加入与申请审批均返回 ROOM_FULL', async () => {
  const { server, port } = await startServer();
  try {
    const t = await threeUsers(port);
    const { a, b, c } = t;
    const g = await createGroup(a, 'full-g', { joinMode: 'open', maxMembers: 2 });
    b.send({ type: 'join', room: g.roomId });
    await b.waitFor((m) => m.type === 'joined');
    c.send({ type: 'join', room: g.roomId });
    assert.equal((await nextError(c)).code, 'ROOM_FULL');

    // 申请群同样在审批时校验满员，申请保持 pending
    const g2 = await createGroup(a, 'full-a', { joinMode: 'apply', maxMembers: 2 });
    b.send({ type: 'join', room: g2.roomId });
    await b.waitFor((m) => m.type === 'join_requested');
    a.send({ type: 'requests', roomId: g2.roomId });
    const reqs = await a.waitFor((m) => m.type === 'requests');
    a.send({ type: 'approve_request', requestId: reqs.requests[0].id });
    // 群里只有 alice，容量 2，可以批一个
    await b.waitFor((m) => m.type === 'joined' && m.roomId === g2.roomId);
    // 再批 carol 时满员
    c.send({ type: 'join', room: g2.roomId });
    await a.waitFor((m) => m.type === 'request_created' && m.request.userName === 'carol');
    a.send({ type: 'requests', roomId: g2.roomId });
    const reqs2 = await a.waitFor((m) => m.type === 'requests' && m.requests.some((q) => q.userName === 'carol'));
    a.send({ type: 'approve_request', requestId: reqs2.requests.find((q) => q.userName === 'carol').id });
    assert.equal((await nextError(a)).code, 'ROOM_FULL');
    await Promise.all([a.close(), b.close(), c.close()]);
  } finally {
    server.stop();
  }
});

test('成员治理：设置管理员、禁言同级限制、踢人后被踢连接立即退订', async () => {
  const { server, port } = await startServer();
  try {
    const t = await threeUsers(port);
    const { a, b, c } = t;
    const g = await createGroup(a, 'mod-g', { joinMode: 'open' });
    b.send({ type: 'join', room: g.roomId });
    c.send({ type: 'join', room: g.roomId });
    await b.waitFor((m) => m.type === 'joined');
    await c.waitFor((m) => m.type === 'joined');

    // 群主把 bob 设为管理员
    a.send({ type: 'set_admin', roomId: g.roomId, userId: t.ub.userId, isAdmin: true });
    await nextNotice(a, 'role_changed', g.roomId);

    // bob（管理员）禁言 carol（成员）允许；禁言 alice（群主）拒绝
    b.send({ type: 'mute', roomId: g.roomId, userId: t.uc.userId, minutes: 5 });
    await nextNotice(b, 'muted', g.roomId);
    b.send({ type: 'mute', roomId: g.roomId, userId: t.ua.userId, minutes: 5 });
    assert.equal((await nextError(b)).code, 'FORBIDDEN');

    // carol 被禁言期间发消息被拒
    c.send({ type: 'msg', roomId: g.roomId, clientMsgId: 'mm1', content: 'x' });
    assert.equal((await nextError(c)).code, 'MUTED');

    // 管理员互禁：bob 不能禁言同级（这里只有一个管理员，改为验证不能踢群主）
    b.send({ type: 'kick', roomId: g.roomId, userId: t.ua.userId });
    assert.equal((await nextError(b)).code, 'FORBIDDEN');

    // 群主踢 carol：carol 立即收到 removed
    a.send({ type: 'kick', roomId: g.roomId, userId: t.uc.userId });
    const removed = await c.waitFor((m) => m.type === 'removed');
    assert.equal(removed.roomId, g.roomId);
    // 被踢后再发消息：NOT_MEMBER
    c.send({ type: 'msg', roomId: g.roomId, clientMsgId: 'mm2', content: 'y' });
    assert.equal((await nextError(c)).code, 'NOT_MEMBER');
    await Promise.all([a.close(), b.close(), c.close()]);
  } finally {
    server.stop();
  }
});

test('群主转让：旧群主降为管理员且不能解散，新群主可解散并通知全员、级联删数据', async () => {
  const { server, port } = await startServer();
  try {
    const t = await threeUsers(port);
    const { a, b } = t;
    const g = await createGroup(a, 'own-g', { joinMode: 'private' });
    a.send({ type: 'invite', roomId: g.roomId, user: 'bob' });
    const iv = await b.waitFor((m) => m.type === 'invitation');
    b.send({ type: 'accept_invite', invitationId: iv.invitation.id });
    await b.waitFor((m) => m.type === 'joined');

    // 群主不能直接退群
    a.send({ type: 'leave', roomId: g.roomId });
    assert.equal((await nextError(a)).code, 'OWNER_MUST_TRANSFER');

    // 转让给 bob
    a.send({ type: 'transfer_owner', roomId: g.roomId, userId: t.ub.userId });
    const oc = await nextNotice(a, 'owner_changed', g.roomId);
    assert.equal(oc.from, t.ua.userId);
    assert.equal(oc.to, t.ub.userId);

    // alice（现 admin）解散被拒
    a.send({ type: 'disband', roomId: g.roomId });
    assert.equal((await nextError(a)).code, 'FORBIDDEN');

    // 新群主 bob 解散：双方收到 group_disbanded，DB 级联清除
    b.send({ type: 'disband', roomId: g.roomId });
    await a.waitFor((m) => m.type === 'group_disbanded');
    await b.waitFor((m) => m.type === 'group_disbanded');
    assert.equal(server.db.getRoom(g.roomId), undefined);
    assert.equal(server.db.countMembers(g.roomId), 0);
    assert.deepEqual(server.db.getMessagesAfter(g.roomId, 0, 10), []);
    await a.close(); await b.close(); await t.c.close();
  } finally {
    server.stop();
  }
});

test('公告与全员禁言：按权限放行/拒绝，全员禁言时管理员仍可发言', async () => {
  const { server, port } = await startServer();
  try {
    const t = await threeUsers(port);
    const { a, b, c } = t;
    const g = await createGroup(a, 'ann-g', { joinMode: 'open' });
    b.send({ type: 'join', room: g.roomId });
    await b.waitFor((m) => m.type === 'joined');
    c.send({ type: 'join', room: g.roomId });
    await c.waitFor((m) => m.type === 'joined');

    // 普通成员不能发公告
    b.send({ type: 'announcement', roomId: g.roomId, content: 'hi' });
    assert.equal((await nextError(b)).code, 'FORBIDDEN');
    // 群主发公告：广播
    a.send({ type: 'announcement', roomId: g.roomId, content: '欢迎' });
    const an = await b.waitFor((m) => m.type === 'announcement');
    assert.equal(an.content, '欢迎');

    // 全员禁言 10 分钟：成员不能发，群主可发
    a.send({ type: 'mute_all', roomId: g.roomId, minutes: 10 });
    await nextNotice(b, 'mute_all_on', g.roomId);
    c.send({ type: 'msg', roomId: g.roomId, clientMsgId: 'ma1', content: 'x' });
    assert.equal((await nextError(c)).code, 'MUTED');
    a.send({ type: 'msg', roomId: g.roomId, clientMsgId: 'ma2', content: 'owner speaks' });
    const ack = await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'ma2');
    assert.ok(ack.seq > 0);
    // 解除后成员恢复
    a.send({ type: 'mute_all', roomId: g.roomId, minutes: 0 });
    await nextNotice(c, 'mute_all_off', g.roomId);
    c.send({ type: 'msg', roomId: g.roomId, clientMsgId: 'ma3', content: 'free' });
    await c.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'ma3');
    await Promise.all([a.close(), b.close(), c.close()]);
  } finally {
    server.stop();
  }
});

test('自定义角色权限：收窄 member.msg.send 后成员被拒，恢复后可发；非法权限键报错', async () => {
  const { server, port } = await startServer();
  try {
    const t = await threeUsers(port);
    const { a, b } = t;
    const g = await createGroup(a, 'perm-g', { joinMode: 'open' });
    b.send({ type: 'join', room: g.roomId });
    await b.waitFor((m) => m.type === 'joined');

    // 默认矩阵下成员可发言
    b.send({ type: 'msg', roomId: g.roomId, clientMsgId: 'p1', content: 'ok' });
    await b.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'p1');

    // 群主收窄成员发消息权限
    a.send({ type: 'set_permissions', roomId: g.roomId, permissions: { member: { 'msg.send': false } } });
    await nextNotice(b, 'permissions_changed', g.roomId);
    b.send({ type: 'msg', roomId: g.roomId, clientMsgId: 'p2', content: 'denied' });
    assert.equal((await nextError(b)).code, 'FORBIDDEN');

    // 恢复
    a.send({ type: 'set_permissions', roomId: g.roomId, permissions: { member: { 'msg.send': true } } });
    await nextNotice(b, 'permissions_changed', g.roomId);
    b.send({ type: 'msg', roomId: g.roomId, clientMsgId: 'p3', content: 'again' });
    await b.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'p3');

    // 非法权限键
    a.send({ type: 'set_permissions', roomId: g.roomId, permissions: { member: { 'nope.perm': true } } });
    assert.equal((await nextError(a)).code, 'BAD_REQUEST');
    await a.close(); await b.close(); await t.c.close();
  } finally {
    server.stop();
  }
});

test('群资料编辑与设置：改名去重、准入模式/保留期更新广播 group_updated', async () => {
  const { server, port } = await startServer();
  try {
    const t = await threeUsers(port);
    const { a, b } = t;
    const g = await createGroup(a, 'edit-g', { joinMode: 'open' });
    const other = await createGroup(a, 'other-g');
    b.send({ type: 'join', room: g.roomId });
    await b.waitFor((m) => m.type === 'joined');

    a.send({ type: 'edit_group', roomId: g.roomId, name: 'edited-g', description: 'd' });
    const upd = await b.waitFor((m) => m.type === 'group_updated');
    assert.equal(upd.group.name, 'edited-g');
    assert.equal(upd.group.description, 'd');

    // 改成已占用的名字
    a.send({ type: 'edit_group', roomId: g.roomId, name: 'other-g' });
    assert.equal((await nextError(a)).code, 'ROOM_EXISTS');

    // 改准入模式/保留期
    a.send({ type: 'group_settings', roomId: g.roomId, joinMode: 'private', maxMembers: 50, retentionDays: 7 });
    const upd2 = await b.waitFor((m) => m.type === 'group_updated');
    assert.equal(upd2.group.joinMode, 'private');
    assert.equal(upd2.group.retentionDays, 7);
    assert.equal(upd2.group.maxMembers, 50);

    // 非法枚举
    a.send({ type: 'group_settings', roomId: g.roomId, joinMode: 'weird' });
    assert.equal((await nextError(a)).code, 'BAD_REQUEST');
    await a.close(); await b.close(); await t.c.close();
  } finally {
    server.stop();
  }
});

test('发现群组：仅返回公开/申请群且排除已加入与私有群', async () => {
  const { server, port } = await startServer();
  try {
    const t = await threeUsers(port);
    const { a, b } = t;
    await createGroup(a, 'open-one', { joinMode: 'open' });
    await createGroup(a, 'apply-one', { joinMode: 'apply' });
    await createGroup(a, 'secret-one', { joinMode: 'private' });
    b.send({ type: 'discover' });
    const d = await b.waitFor((m) => m.type === 'discover');
    const names = d.groups.map((g) => g.name).sort();
    assert.deepEqual(names, ['apply-one', 'open-one']);
    b.send({ type: 'discover', keyword: 'open' });
    const d2 = await b.waitFor((m) => m.type === 'discover');
    assert.deepEqual(d2.groups.map((g) => g.name), ['open-one']);
    // 每条带 memberCount
    assert.equal(d2.groups[0].memberCount, 1);
    await a.close(); await b.close(); await t.c.close();
  } finally {
    server.stop();
  }
});

test('退群：成员可退且广播 member_left，退群后 Hub 不再收到该群消息', async () => {
  const { server, port } = await startServer();
  try {
    const t = await threeUsers(port);
    const { a, b } = t;
    const g = await createGroup(a, 'leave-g', { joinMode: 'open' });
    b.send({ type: 'join', room: g.roomId });
    await b.waitFor((m) => m.type === 'joined');

    b.send({ type: 'leave', roomId: g.roomId });
    await b.waitFor((m) => m.type === 'left');
    await nextNotice(a, 'member_left', g.roomId);

    // a 再发消息，b 不应收到
    a.send({ type: 'msg', roomId: g.roomId, clientMsgId: 'lv1', content: 'after leave' });
    await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'lv1');
    await sleep(200);
    assert.equal(b.roomSeqs(g.roomId).length, 0);
    // b 已不是成员：拉历史被拒
    b.send({ type: 'history', roomId: g.roomId });
    assert.equal((await nextError(b)).code, 'NOT_MEMBER');
    await a.close(); await b.close(); await t.c.close();
  } finally {
    server.stop();
  }
});

test('消息保留期限：sweepExpiredMessages 删除超过保留天数的消息', async () => {
  const { server, port } = await startServer();
  try {
    const u = await login(port, 'alice');
    const a = await Client.connect(port, u.token);
    const g = await createGroup(a, 'ret-g', { retentionDays: 7 });
    a.send({ type: 'msg', roomId: g.roomId, clientMsgId: 'r1', content: 'old' });
    await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'r1');
    // 把消息 ts 改为 30 天前
    server.db.db.prepare('UPDATE messages SET ts = ? WHERE room_id = ?').run(Date.now() - 30 * 86_400_000, g.roomId);
    assert.equal(server.db.getMessagesAfter(g.roomId, 0, 10).length, 1);
    const removed = server.db.sweepExpiredMessages();
    assert.equal(removed, 1);
    assert.equal(server.db.getMessagesAfter(g.roomId, 0, 10).length, 0);
    await a.close();
  } finally {
    server.stop();
  }
});

test('数据库迁移：v0 旧库重启后创建者升级为 owner、新列齐备、历史消息保留', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'chat-mig-'));
  const dbPath = path.join(dir, 'old.db');
  try {
    // 手工构造 v0 旧 schema
    const { DatabaseSync } = require('node:sqlite');
    const old = new DatabaseSync(dbPath);
    old.exec(`
      CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, token_random TEXT NOT NULL, created_at INTEGER NOT NULL);
      CREATE TABLE rooms (id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, created_by TEXT NOT NULL, created_at INTEGER NOT NULL, last_seq INTEGER NOT NULL DEFAULT 0);
      CREATE TABLE members (room_id TEXT NOT NULL, user_id TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'member' CHECK (role IN ('admin','member')),
        muted_until INTEGER NOT NULL DEFAULT 0, joined_at INTEGER NOT NULL, PRIMARY KEY (room_id, user_id));
      CREATE TABLE messages (room_id TEXT NOT NULL, seq INTEGER NOT NULL, client_msg_id TEXT NOT NULL,
        sender_id TEXT NOT NULL, content TEXT NOT NULL, ts INTEGER NOT NULL,
        PRIMARY KEY (room_id, seq), UNIQUE (room_id, sender_id, client_msg_id));
      CREATE TABLE cursors (room_id TEXT NOT NULL, user_id TEXT NOT NULL, last_ack_seq INTEGER NOT NULL DEFAULT 0,
        updated_at INTEGER NOT NULL, PRIMARY KEY (room_id, user_id));
      INSERT INTO users VALUES ('u1','alice','rnd',1000);
      INSERT INTO users VALUES ('u2','bob','rnd2',1001);
      INSERT INTO rooms VALUES ('r1','general','u1',1000,1);
      INSERT INTO members VALUES ('r1','u1','admin',0,1000);
      INSERT INTO members VALUES ('r1','u2','member',0,1001);
      INSERT INTO messages VALUES ('r1',1,'c1','u2','history',1002);
      INSERT INTO cursors VALUES ('r1','u2',1,1003);
    `);
    old.close();

    const { server } = await startServer({ dbPath });
    try {
      assert.equal(server.db.getMember('r1', 'u1').role, 'owner', '旧 admin 创建者应升级为 owner');
      assert.equal(server.db.getMember('r1', 'u2').role, 'member');
      const room = server.db.getRoom('r1');
      assert.equal(room.join_mode, 'open');
      assert.equal(room.max_members, 200);
      assert.equal(room.retention_days, 0);
      assert.equal(room.announcement, '');
      const msgs = server.db.getMessagesAfter('r1', 0, 10);
      assert.equal(msgs.length, 1);
      assert.equal(msgs[0].content, 'history');
      const version = server.db.db.prepare('PRAGMA user_version').get().user_version;
      assert.equal(version, 1, '迁移后 user_version 应为 1');
      // 迁移后新功能可用：再开一个群不受 CHECK 约束影响
      const r2 = server.db.createRoom('r2', 'new-group', 'u1', { joinMode: 'private', maxMembers: 10, retentionDays: 0 });
      assert.equal(r2.join_mode, 'private');
      assert.equal(server.db.getMember('r2', 'u1').role, 'owner');
    } finally {
      server.stop();
    }
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
