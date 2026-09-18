'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const {
  sleep, startServer, login, Client, createRoom, joinRoom,
} = require('./helpers');

/** 便捷：登录 n 个用户并全部连上 WS，返回 [{u,c}] */
async function usersUp(port, names) {
  const out = [];
  for (const name of names) {
    const u = await login(port, name);
    const c = await Client.connect(port, u.token);
    out.push({ u, c });
  }
  return out;
}

const closeAll = async (cs) => {
  await Promise.all(cs.map((x) => x.c.close()));
};

// ---------------------------------------------------------------- 群组资料

test('建群可配置准入模式/人数上限/保留期/权限矩阵，joined 带回 group 资料', async () => {
  const { server, port } = await startServer();
  try {
    const [{ u, c }] = await usersUp(port, ['alice']);
    c.send({
      type: 'group_create', name: 'team', joinMode: 'approval',
      maxMembers: 5, retainDays: 30,
      permissions: { sendMessage: ['owner', 'admin'] },
      description: 'd', avatar: 'a',
    });
    const j = await c.waitFor((m) => m.type === 'joined');
    assert.equal(j.role, 'owner');
    assert.equal(j.group.joinMode, 'approval');
    assert.equal(j.group.maxMembers, 5);
    assert.equal(j.group.retainDays, 30);
    assert.deepEqual(j.group.permissions.sendMessage, ['owner', 'admin']);
    assert.equal(j.group.memberCount, 1);
    await c.close();
  } finally {
    server.stop();
  }
});

test('非法准入模式 / 越界人数上限被拒', async () => {
  const { server, port } = await startServer();
  try {
    const [{ c }] = await usersUp(port, ['alice']);
    c.send({ type: 'group_create', name: 'g1', joinMode: 'weird' });
    let e = await c.waitFor((m) => m.type === 'error');
    assert.equal(e.code, 'BAD_REQUEST');

    c.send({ type: 'group_create', name: 'g2', maxMembers: 1 });
    e = await c.waitFor((m) => m.type === 'error');
    assert.equal(e.code, 'BAD_REQUEST');

    c.send({ type: 'group_create', name: 'g3', permissions: { sendMessage: ['ghost'] } });
    e = await c.waitFor((m) => m.type === 'error');
    assert.equal(e.code, 'BAD_REQUEST');
    await c.close();
  } finally {
    server.stop();
  }
});

test('编辑群组资料后群内广播 group_updated，非授权角色被拒', async () => {
  const { server, port } = await startServer();
  try {
    const [a, b] = await usersUp(port, ['alice', 'bob']);
    const roomId = await createRoom(a.c, 'team');
    await joinRoom(b.c, roomId);

    b.c.send({ type: 'group_update', roomId, description: 'hack' });
    const err = await b.c.waitFor((m) => m.type === 'error');
    assert.equal(err.code, 'FORBIDDEN');

    a.c.send({ type: 'group_update', roomId, description: 'new desc', joinMode: 'private' });
    const upd = await b.c.waitFor((m) => m.type === 'group_updated');
    assert.equal(upd.group.description, 'new desc');
    assert.equal(upd.group.joinMode, 'private');
    await closeAll([a, b]);
  } finally {
    server.stop();
  }
});

// ---------------------------------------------------------------- 三种准入模式

test('公开群：任何人可直接加入', async () => {
  const { server, port } = await startServer();
  try {
    const [a, b] = await usersUp(port, ['alice', 'bob']);
    const roomId = await createRoom(a.c, 'open');
    await joinRoom(b.c, roomId);
    const rooms = await getRooms(b.c);
    assert.ok(rooms.some((r) => r.id === roomId));
    await closeAll([a, b]);
  } finally {
    server.stop();
  }
});

test('申请群：提交申请 -> 管理员收到 request_received -> 批准后申请人自动入群', async () => {
  const { server, port } = await startServer();
  try {
    const [a, b] = await usersUp(port, ['alice', 'bob']);
    const roomId = await createRoom(a.c, 'guild', { joinMode: 'approval' });

    b.c.send({ type: 'join', room: roomId, message: 'let me in' });
    const submitted = await b.c.waitFor((m) => m.type === 'request_submitted');
    assert.equal(submitted.roomId, roomId);
    const recv = await a.c.waitFor((m) => m.type === 'request_received');
    assert.equal(recv.userId, b.u.userId);
    assert.equal(recv.message, 'let me in');

    // 重复提交不产生新申请（幂等）
    b.c.send({ type: 'join', room: roomId, message: 'again' });
    const again = await b.c.waitFor((m) => m.type === 'request_submitted');
    assert.equal(again.requestId, submitted.requestId);

    // 批准
    a.c.send({ type: 'group_request_handle', requestId: recv.requestId, approve: true });
    const approved = await b.c.waitFor((m) => m.type === 'request_approved');
    assert.equal(approved.roomId, roomId);
    const joined = await b.c.waitFor((m) => m.type === 'joined');
    assert.equal(joined.roomId, roomId);
    await closeAll([a, b]);
  } finally {
    server.stop();
  }
});

test('申请被拒：申请人收到 request_rejected 且不是成员', async () => {
  const { server, port } = await startServer();
  try {
    const [a, b] = await usersUp(port, ['alice', 'bob']);
    const roomId = await createRoom(a.c, 'guild', { joinMode: 'approval' });
    b.c.send({ type: 'join', room: roomId });
    const recv = await a.c.waitFor((m) => m.type === 'request_received');
    a.c.send({ type: 'group_request_handle', requestId: recv.requestId, approve: false });
    const rej = await b.c.waitFor((m) => m.type === 'request_rejected');
    assert.ok(rej);

    b.c.send({ type: 'members', roomId });
    const err = await b.c.waitFor((m) => m.type === 'error');
    assert.equal(err.code, 'NOT_MEMBER');
    await closeAll([a, b]);
  } finally {
    server.stop();
  }
});

test('私有群：无邀请直接加入被拒（GROUP_PRIVATE）', async () => {
  const { server, port } = await startServer();
  try {
    const [a, b] = await usersUp(port, ['alice', 'bob']);
    const roomId = await createRoom(a.c, 'secret', { joinMode: 'private' });
    b.c.send({ type: 'join', room: roomId });
    const err = await b.c.waitFor((m) => m.type === 'error');
    assert.equal(err.code, 'GROUP_PRIVATE');
    await closeAll([a, b]);
  } finally {
    server.stop();
  }
});

// ---------------------------------------------------------------- 邀请

test('定向邀请：被邀请人收到 invite_received，凭 code 加入私有群', async () => {
  const { server, port } = await startServer();
  try {
    const [a, b] = await usersUp(port, ['alice', 'bob']);
    const roomId = await createRoom(a.c, 'secret', { joinMode: 'private' });

    a.c.send({ type: 'group_invite_create', roomId, userId: b.u.userId });
    const created = await a.c.waitFor((m) => m.type === 'invite_created');
    const recv = await b.c.waitFor((m) => m.type === 'invite_received');
    assert.equal(recv.code, created.code);

    await joinRoom(b.c, roomId, 0, { code: created.code });
    await closeAll([a, b]);
  } finally {
    server.stop();
  }
});

test('定向邀请他人不能领取（INVITE_NOT_YOURS）', async () => {
  const { server, port } = await startServer();
  try {
    const [a, b, d] = await usersUp(port, ['alice', 'bob', 'dan']);
    const roomId = await createRoom(a.c, 'secret', { joinMode: 'private' });
    a.c.send({ type: 'group_invite_create', roomId, userId: b.u.userId });
    const created = await a.c.waitFor((m) => m.type === 'invite_created');

    d.c.send({ type: 'join', room: roomId, code: created.code });
    const err = await d.c.waitFor((m) => m.type === 'error');
    assert.equal(err.code, 'INVITE_NOT_YOURS');
    await closeAll([a, b, d]);
  } finally {
    server.stop();
  }
});

test('邀请码：maxUses 次后失效（INVITE_DEPLETED）', async () => {
  const { server, port } = await startServer();
  try {
    const [a, b, d] = await usersUp(port, ['alice', 'bob', 'dan']);
    const roomId = await createRoom(a.c, 'secret', { joinMode: 'private', maxMembers: 10 });
    a.c.send({ type: 'group_invite_create', roomId, maxUses: 1, ttlMs: 0 });
    const created = await a.c.waitFor((m) => m.type === 'invite_created');
    assert.equal(created.maxUses, 1);

    await joinRoom(b.c, roomId, 0, { code: created.code });
    d.c.send({ type: 'join', room: roomId, code: created.code });
    const err = await d.c.waitFor((m) => m.type === 'error');
    assert.equal(err.code, 'INVITE_DEPLETED');
    await closeAll([a, b, d]);
  } finally {
    server.stop();
  }
});

test('邀请链接：仅凭 code（不带 room）即可加入', async () => {
  const { server, port } = await startServer();
  try {
    const [a, b] = await usersUp(port, ['alice', 'bob']);
    const roomId = await createRoom(a.c, 'secret', { joinMode: 'private' });
    a.c.send({ type: 'group_invite_create', roomId, userId: b.u.userId });
    const created = await a.c.waitFor((m) => m.type === 'invite_created');

    b.c.send({ type: 'join', code: created.code });
    const joined = await b.c.waitFor((m) => m.type === 'joined');
    assert.equal(joined.roomId, roomId);
    await closeAll([a, b]);
  } finally {
    server.stop();
  }
});

test('按用户名（userName）创建定向邀请', async () => {
  const { server, port } = await startServer();
  try {
    const [a, b] = await usersUp(port, ['alice', 'bob']);
    const roomId = await createRoom(a.c, 'secret', { joinMode: 'private' });
    a.c.send({ type: 'group_invite_create', roomId, userName: 'bob' });
    const created = await a.c.waitFor((m) => m.type === 'invite_created');
    assert.equal(created.inviteeId, b.u.userId);
    const recv = await b.c.waitFor((m) => m.type === 'invite_received');
    assert.equal(recv.code, created.code);
    await closeAll([a, b]);
  } finally {
    server.stop();
  }
});

test('过期邀请被拒（INVITE_EXPIRED）', async () => {
  const { server, port } = await startServer();
  try {
    const [a, b] = await usersUp(port, ['alice', 'bob']);
    const roomId = await createRoom(a.c, 'secret', { joinMode: 'private' });
    a.c.send({ type: 'group_invite_create', roomId, userId: b.u.userId, ttlMs: 1 });
    const created = await a.c.waitFor((m) => m.type === 'invite_created');
    await sleep(20);

    b.c.send({ type: 'join', room: roomId, code: created.code });
    const err = await b.c.waitFor((m) => m.type === 'error');
    assert.equal(err.code, 'INVITE_EXPIRED');
    await closeAll([a, b]);
  } finally {
    server.stop();
  }
});

test('不存在的邀请码被拒（INVITE_NOT_FOUND），撤销后定向邀请推送 invite_revoked', async () => {
  const { server, port } = await startServer();
  try {
    const [a, b] = await usersUp(port, ['alice', 'bob']);
    const roomId = await createRoom(a.c, 'secret', { joinMode: 'private' });

    b.c.send({ type: 'join', room: roomId, code: 'inv_nope' });
    const err = await b.c.waitFor((m) => m.type === 'error');
    assert.equal(err.code, 'INVITE_NOT_FOUND');

    a.c.send({ type: 'group_invite_create', roomId, userId: b.u.userId });
    const created = await a.c.waitFor((m) => m.type === 'invite_created');
    await b.c.waitFor((m) => m.type === 'invite_received');

    a.c.send({ type: 'group_invite_revoke', code: created.code });
    await b.c.waitFor((m) => m.type === 'invite_revoked');
    b.c.send({ type: 'join', room: roomId, code: created.code });
    const err2 = await b.c.waitFor((m) => m.type === 'error');
    assert.equal(err2.code, 'INVITE_NOT_FOUND');
    await closeAll([a, b]);
  } finally {
    server.stop();
  }
});

// ---------------------------------------------------------------- 满员

test('群组满员：公开群直入与申请批准均被 GROUP_FULL 拒绝', async () => {
  const { server, port } = await startServer();
  try {
    const [a, b] = await usersUp(port, ['alice', 'bob']);
    // maxMembers=2：alice 建房占一席，bob 加入即满
    const roomId = await createRoom(a.c, 'tiny', { joinMode: 'public', maxMembers: 2 });
    await joinRoom(b.c, roomId);

    const uc = await login(port, 'carol');
    const c = await Client.connect(port, uc.token);
    c.send({ type: 'join', room: roomId });
    const err = await c.waitFor((m) => m.type === 'error');
    assert.equal(err.code, 'GROUP_FULL');

    // 申请群在批准时再次校验满员
    a.c.send({ type: 'group_update', roomId, joinMode: 'approval', maxMembers: 2 });
    await a.c.waitFor((m) => m.type === 'group_updated');
    c.send({ type: 'join', room: roomId });
    const recv = await a.c.waitFor((m) => m.type === 'request_received');
    a.c.send({ type: 'group_request_handle', requestId: recv.requestId, approve: true });
    const rej = await c.waitFor((m) => m.type === 'request_rejected');
    assert.ok(rej, '满员时批准失败应通知申请人');
    await c.close();
    await closeAll([a, b]);
  } finally {
    server.stop();
  }
});

// ---------------------------------------------------------------- 角色与权限

test('设置/取消管理员：目标收到 role_changed，管理员可禁言普通成员', async () => {
  const { server, port } = await startServer();
  try {
    const [a, b, d] = await usersUp(port, ['alice', 'bob', 'dan']);
    const roomId = await createRoom(a.c, 'team');
    await joinRoom(b.c, roomId);
    await joinRoom(d.c, roomId);

    a.c.send({ type: 'group_set_admin', roomId, userId: b.u.userId, make: true });
    await b.c.waitFor((m) => m.type === 'role_changed' && m.role === 'admin');
    await d.c.waitFor((m) => m.type === 'notice' && m.event === 'admin_promoted');

    // bob 现为管理员，可禁言 dan
    b.c.send({ type: 'mute', roomId, userId: d.u.userId, minutes: 5 });
    await d.c.waitFor((m) => m.type === 'notice' && m.event === 'muted');
    d.c.send({ type: 'msg', roomId, clientMsgId: 'x', content: 'muted?' });
    const err = await d.c.waitFor((m) => m.type === 'error');
    assert.equal(err.code, 'MUTED');

    // 取消管理员后 bob 不能再禁言
    a.c.send({ type: 'group_set_admin', roomId, userId: b.u.userId, make: false });
    await b.c.waitFor((m) => m.type === 'role_changed' && m.role === 'member');
    b.c.send({ type: 'mute', roomId, userId: d.u.userId, minutes: 5 });
    const err2 = await b.c.waitFor((m) => m.type === 'error');
    assert.equal(err2.code, 'FORBIDDEN');
    await closeAll([a, b, d]);
  } finally {
    server.stop();
  }
});

test('管理员不能被禁言，普通成员不能设置管理员', async () => {
  const { server, port } = await startServer();
  try {
    const [a, b, d] = await usersUp(port, ['alice', 'bob', 'dan']);
    const roomId = await createRoom(a.c, 'team');
    await joinRoom(b.c, roomId);
    await joinRoom(d.c, roomId);
    a.c.send({ type: 'group_set_admin', roomId, userId: b.u.userId, make: true });
    await b.c.waitFor((m) => m.type === 'role_changed');

    a.c.send({ type: 'mute', roomId, userId: b.u.userId, minutes: 5 });
    const err = await a.c.waitFor((m) => m.type === 'error');
    assert.equal(err.code, 'FORBIDDEN');

    d.c.send({ type: 'group_set_admin', roomId, userId: d.u.userId, make: true });
    const err2 = await d.c.waitFor((m) => m.type === 'error');
    assert.equal(err2.code, 'FORBIDDEN');
    await closeAll([a, b, d]);
  } finally {
    server.stop();
  }
});

test('自定义权限矩阵：剥夺普通成员发消息权限后发送被拒', async () => {
  const { server, port } = await startServer();
  try {
    const [a, b] = await usersUp(port, ['alice', 'bob']);
    const roomId = await createRoom(a.c, 'team', {
      permissions: { sendMessage: ['owner', 'admin'] },
    });
    await joinRoom(b.c, roomId);
    b.c.send({ type: 'msg', roomId, clientMsgId: 'p1', content: 'hi' });
    const err = await b.c.waitFor((m) => m.type === 'error');
    assert.equal(err.code, 'FORBIDDEN');

    // owner 仍可发
    a.c.send({ type: 'msg', roomId, clientMsgId: 'p2', content: 'owner speaks' });
    const ack = await a.c.waitFor((m) => m.type === 'ack');
    assert.equal(ack.seq, 1);
    await closeAll([a, b]);
  } finally {
    server.stop();
  }
});

test('全员禁言：普通成员被拒，管理员可发言，解除后恢复', async () => {
  const { server, port } = await startServer();
  try {
    const [a, b] = await usersUp(port, ['alice', 'bob']);
    const roomId = await createRoom(a.c, 'team');
    await joinRoom(b.c, roomId);

    a.c.send({ type: 'group_mute_all', roomId, minutes: 10 });
    await b.c.waitFor((m) => m.type === 'notice' && m.event === 'group_muted');

    b.c.send({ type: 'msg', roomId, clientMsgId: 'p1', content: 'hi' });
    const err = await b.c.waitFor((m) => m.type === 'error');
    assert.equal(err.code, 'MUTED');

    a.c.send({ type: 'msg', roomId, clientMsgId: 'p2', content: 'admin ok' });
    await a.c.waitFor((m) => m.type === 'ack');

    a.c.send({ type: 'group_mute_all', roomId, minutes: 0 });
    await b.c.waitFor((m) => m.type === 'notice' && m.event === 'group_unmuted');
    b.c.send({ type: 'msg', roomId, clientMsgId: 'p3', content: 'free' });
    await b.c.waitFor((m) => m.type === 'ack');
    await closeAll([a, b]);
  } finally {
    server.stop();
  }
});

// ---------------------------------------------------------------- 踢人 / 退群

test('踢人：被踢者收到 kicked 并退订，不能踢同级或群主', async () => {
  const { server, port } = await startServer();
  try {
    const [a, b, d] = await usersUp(port, ['alice', 'bob', 'dan']);
    const roomId = await createRoom(a.c, 'team');
    await joinRoom(b.c, roomId);
    await joinRoom(d.c, roomId);
    a.c.send({ type: 'group_set_admin', roomId, userId: b.u.userId, make: true });
    await b.c.waitFor((m) => m.type === 'role_changed');

    // admin 踢普通成员 dan
    b.c.send({ type: 'group_kick', roomId, userId: d.u.userId });
    const kicked = await d.c.waitFor((m) => m.type === 'kicked');
    assert.equal(kicked.roomId, roomId);

    // dan 已退订：后续房间消息收不到，且成员接口拒绝
    a.c.send({ type: 'msg', roomId, clientMsgId: 'm1', content: 'after kick' });
    await a.c.waitFor((m) => m.type === 'ack');
    await sleep(100);
    assert.equal(d.c.roomSeqs(roomId).length, 0);

    // admin 不能踢群主
    b.c.send({ type: 'group_kick', roomId, userId: a.u.userId });
    const err = await b.c.waitFor((m) => m.type === 'error');
    assert.equal(err.code, 'FORBIDDEN');
    await closeAll([a, b, d]);
  } finally {
    server.stop();
  }
});

test('群主不能直接退群，须先转让或解散', async () => {
  const { server, port } = await startServer();
  try {
    const [a] = await usersUp(port, ['alice']);
    const roomId = await createRoom(a.c, 'team');
    a.c.send({ type: 'leave', roomId });
    const err = await a.c.waitFor((m) => m.type === 'error');
    assert.equal(err.code, 'FORBIDDEN');
    await a.c.close();
  } finally {
    server.stop();
  }
});

// ---------------------------------------------------------------- 转让 / 解散

test('转让群主：新群主获 owner，旧群主降为 member 并可退群', async () => {
  const { server, port } = await startServer();
  try {
    const [a, b] = await usersUp(port, ['alice', 'bob']);
    const roomId = await createRoom(a.c, 'team');
    await joinRoom(b.c, roomId);

    a.c.send({ type: 'group_transfer', roomId, userId: b.u.userId });
    const toB = await b.c.waitFor((m) => m.type === 'role_changed' && m.role === 'owner');
    const toA = await a.c.waitFor((m) => m.type === 'role_changed' && m.role === 'member');
    assert.ok(toB && toA);
    await a.c.waitFor((m) => m.type === 'notice' && m.event === 'owner_transferred');

    // 旧群主现在可退群
    a.c.send({ type: 'leave', roomId });
    const left = await a.c.waitFor((m) => m.type === 'left');
    assert.equal(left.roomId, roomId);

    // 新群主可解散
    b.c.send({ type: 'group_dissolve', roomId });
    const dissolved = await b.c.waitFor((m) => m.type === 'group_dissolved');
    assert.equal(dissolved.roomId, roomId);
    await closeAll([a, b]);
  } finally {
    server.stop();
  }
});

test('解散群组：全体在线成员收到 group_dissolved，群组与成员数据被清理', async () => {
  const { server, port } = await startServer();
  try {
    const [a, b, d] = await usersUp(port, ['alice', 'bob', 'dan']);
    const roomId = await createRoom(a.c, 'team');
    await joinRoom(b.c, roomId);
    await joinRoom(d.c, roomId);
    a.c.send({ type: 'msg', roomId, clientMsgId: 'm1', content: 'before dissolve' });
    await a.c.waitFor((m) => m.type === 'ack');

    a.c.send({ type: 'group_dissolve', roomId });
    await b.c.waitFor((m) => m.type === 'group_dissolved');
    await d.c.waitFor((m) => m.type === 'group_dissolved');

    // 数据已物理清理
    assert.equal(server.db.getRoom(roomId), undefined);
    assert.equal(server.db.listMembers(roomId).length, 0);
    assert.equal(server.db.getMessagesAfter(roomId, 0, 10).length, 0);

    // 旧 id 不可再加入
    b.c.send({ type: 'join', room: roomId });
    const err = await b.c.waitFor((m) => m.type === 'error');
    assert.equal(err.code, 'NO_SUCH_ROOM');
    await closeAll([a, b, d]);
  } finally {
    server.stop();
  }
});

// ---------------------------------------------------------------- 公告

test('公告：管理员发布后群内广播，成员可查询，非成员私有群被拒', async () => {
  const { server, port } = await startServer();
  try {
    const [a, b, d] = await usersUp(port, ['alice', 'bob', 'dan']);
    const roomId = await createRoom(a.c, 'secret', { joinMode: 'private' });

    a.c.send({ type: 'group_announce', roomId, content: '欢迎大家' });
    const saved = await a.c.waitFor((m) => m.type === 'announcement_saved');
    assert.equal(saved.announcement.content, '欢迎大家');

    // 邀请 bob 入群查看
    a.c.send({ type: 'group_invite_create', roomId, userId: b.u.userId });
    const inv = await a.c.waitFor((m) => m.type === 'invite_created');
    await joinRoom(b.c, roomId, 0, { code: inv.code });
    const push = await b.c.waitFor((m) => m.type === 'announcement');
    assert.equal(push.announcement.content, '欢迎大家');

    // 非成员 dan 查询私有群公告被拒
    d.c.send({ type: 'group_announcement', roomId });
    const err = await d.c.waitFor((m) => m.type === 'error');
    assert.equal(err.code, 'FORBIDDEN');

    // 普通成员不能发布公告
    b.c.send({ type: 'group_announce', roomId, content: 'spam' });
    const err2 = await b.c.waitFor((m) => m.type === 'error');
    assert.equal(err2.code, 'FORBIDDEN');
    await closeAll([a, b, d]);
  } finally {
    server.stop();
  }
});

// ---------------------------------------------------------------- 公开发现

test('消息保留期：pruneMessages 物理删除过期消息，seq 不回收', async () => {
  const { server, port } = await startServer();
  try {
    const [a] = await usersUp(port, ['alice']);
    const roomId = await createRoom(a.c, 'team', { retainDays: 30 });
    a.c.send({ type: 'msg', roomId, clientMsgId: 'old', content: 'old msg' });
    await a.c.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'old');

    // 把该消息时间戳改到 31 天前
    const oldTs = Date.now() - 31 * 86_400_000;
    server.db.db.prepare('UPDATE messages SET ts = ? WHERE room_id = ?').run(oldTs, roomId);

    const removed = server.db.pruneMessages(roomId, 30);
    assert.equal(removed, 1);
    assert.equal(server.db.getMessagesAfter(roomId, 0, 10).length, 0);

    // 新消息 seq 继续递增（last_seq 不回收）
    a.c.send({ type: 'msg', roomId, clientMsgId: 'new', content: 'new msg' });
    const ack = await a.c.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'new');
    assert.equal(ack.seq, 2);
    await a.c.close();
  } finally {
    server.stop();
  }
});

test('group_discover 仅列出公开群，私有/申请群不出现', async () => {
  const { server, port } = await startServer();
  try {
    const [a] = await usersUp(port, ['alice']);
    await createRoom(a.c, 'open1', { joinMode: 'public' });
    await createRoom(a.c, 'appr1', { joinMode: 'approval' });
    await createRoom(a.c, 'priv1', { joinMode: 'private' });

    a.c.send({ type: 'group_discover' });
    const res = await a.c.waitFor((m) => m.type === 'groups');
    const names = res.groups.map((g) => g.name);
    assert.ok(names.includes('open1'));
    assert.ok(!names.includes('appr1'));
    assert.ok(!names.includes('priv1'));
    assert.ok(typeof res.groups[0].memberCount === 'number');
    await a.c.close();
  } finally {
    server.stop();
  }
});

// ---------------------------------------------------------------- 辅助

function getRooms(c) {
  c.send({ type: 'rooms' });
  return c.waitFor((m) => m.type === 'rooms').then((m) => m.rooms);
}
