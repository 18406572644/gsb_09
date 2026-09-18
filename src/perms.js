'use strict';

/**
 * 群组角色与自定义权限。
 *
 * 三级角色：owner（群主，唯一，权限恒为全部允许且不可配置）、
 *           admin（管理员，权限可由群主在默认矩阵上收窄/放开）、
 *           member（普通成员）。
 *
 * 群组房间的 permissions 字段以 JSON 保存「相对于默认矩阵的稀疏覆盖」：
 *   { "admin": { "member.kick": false }, "member": { "member.invite": true } }
 * owner 不允许出现在覆盖表中——群主权限不可让渡给配置（只能通过转让更换 owner）。
 */

const ROLES = ['owner', 'admin', 'member'];
const ROLE_RANK = { owner: 3, admin: 2, member: 1 };

/**
 * 权限目录。键即协议中使用的权限标识；title 供客户端展示。
 * 管理类权限默认只给管理员；msg.send 默认全员。
 */
const PERMISSIONS = {
  'msg.send':        { title: '发送消息' },
  'group.announce':  { title: '发布公告' },
  'group.edit':      { title: '编辑群资料与设置' }, // 含准入模式/人数上限/保留期限/权限配置
  'member.invite':   { title: '邀请成员' },
  'request.review':  { title: '审批入群申请' },
  'member.promote':  { title: '设置/撤销管理员' },
  'member.mute':     { title: '禁言/解禁成员' },
  'member.kick':     { title: '移出成员' },
};

const ALL_KEYS = Object.keys(PERMISSIONS);

/** 各角色默认权限矩阵（owner 不查此表，恒为全部允许） */
const DEFAULT_PERMISSIONS = {
  owner: Object.fromEntries(ALL_KEYS.map((k) => [k, true])),
  admin: {
    'msg.send': true,
    'group.announce': true,
    'group.edit': false,
    'member.invite': true,
    'request.review': true,
    'member.promote': false,
    'member.mute': true,
    'member.kick': true,
  },
  member: {
    'msg.send': true,
    'group.announce': false,
    'group.edit': false,
    'member.invite': false,
    'request.review': false,
    'member.promote': false,
    'member.mute': false,
    'member.kick': false,
  },
};

const isRole = (r) => typeof r === 'string' && ROLE.includes(r);
const roleRank = (r) => ROLE_RANK[r] ?? 0;

/** 解析并校验客户端提交的权限覆盖表；非法键/值直接抛 BAD_REQUEST 语义字符串 */
function parseOverrides(input) {
  if (input == null) return null;
  if (typeof input !== 'object' || Array.isArray(input)) {
    throw new Error('permissions must be an object keyed by role');
  }
  const out = {};
  for (const role of ['admin', 'member']) {
    const part = input[role];
    if (part == null) continue;
    if (typeof part !== 'object' || Array.isArray(part)) {
      throw new Error(`permissions.${role} must be an object`);
    }
    for (const [key, val] of Object.entries(part)) {
      if (!PERMISSIONS[key]) throw new Error(`unknown permission: ${key}`);
      if (typeof val !== 'boolean') throw new Error(`permissions.${role}.${key} must be boolean`);
      (out[role] ||= {})[key] = val;
    }
  }
  // input.owner 一律忽略：群主权限不可配置
  return out;
}

/** 取某角色在群覆盖下的完整权限表（给客户端渲染/调试用） */
function effectiveForRole(role, overrides) {
  if (role === 'owner') return { ...DEFAULT_PERMISSIONS.owner };
  const merged = { ...DEFAULT_PERMISSIONS[role] || DEFAULT_PERMISSIONS.member };
  if (overrides && overrides[role]) Object.assign(merged, overrides[role]);
  return merged;
}

/**
 * 判定成员是否拥有某项权限。
 * @param {object} room   房间行（含 permissions 覆盖 JSON 字符串/对象）
 * @param {object} member 成员行（含 role）
 */
function can(room, member, perm) {
  if (!member) return false;
  if (member.role === 'owner') return true;
  if (!PERMISSIONS[perm]) return false;
  let overrides = room.permissions;
  if (typeof overrides === 'string') {
    try { overrides = JSON.parse(overrides); } catch { overrides = null; }
  }
  const table = effectiveForRole(member.role, overrides);
  return table[perm] === true;
}

module.exports = {
  ROLES,
  PERMISSIONS,
  ALL_PERMISSION_KEYS: ALL_KEYS,
  DEFAULT_PERMISSIONS,
  isRole,
  roleRank,
  parseOverrides,
  effectiveForRole,
  can,
};
