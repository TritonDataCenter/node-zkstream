/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
 */

module.exports = { ZKBuffer: ZKBuffer };

const mod_assert = require('assert-plus');
const mod_jsbn = require('jsbn');
const mod_util = require('util');
const JuteBuffer = require('./jute-buffer').JuteBuffer;
const mod_consts = require('./zk-consts');

function ZKBuffer(opts) {
	JuteBuffer.call(this, opts);
}
mod_util.inherits(ZKBuffer, JuteBuffer);

ZKBuffer.prototype.readConnectRequest = function () {
	var pkt = {};
	pkt.protocolVersion = this.readInt();
	pkt.lastZxidSeen = this.readLongBuffer();
	pkt.timeOut = this.readInt();
	pkt.sessionId = this.readLongBuffer();
	pkt.passwd = this.readBuffer();
	return (pkt);
};

ZKBuffer.prototype.writeConnectRequest = function (pkt) {
	mod_assert.object(pkt, 'packet');
	this.writeInt(pkt.protocolVersion, 'protocolVersion');
	this.writeLong(pkt.lastZxidSeen);
	this.writeInt(pkt.timeOut, 'timeOut');
	this.writeLong(pkt.sessionId);
	this.writeBuffer(pkt.passwd, 'passwd');
};

ZKBuffer.prototype.readConnectResponse = function () {
	var pkt = {};
	pkt.protocolVersion = this.readInt();
	pkt.timeOut = this.readInt();
	pkt.sessionId = this.readLongBuffer();
	pkt.passwd = this.readBuffer();
	return (pkt);
};

ZKBuffer.prototype.writeConnectResponse = function (pkt) {
	mod_assert.object(pkt, 'packet');
	this.writeInt(pkt.protocolVersion, 'protocolVersion');
	this.writeInt(pkt.timeOut, 'timeOut');
	this.writeLong(pkt.sessionId);
	this.writeBuffer(pkt.passwd, 'passwd');
};

ZKBuffer.prototype.readRequest = function () {
	var pkt = {};
	pkt.xid = this.readInt();
	pkt.opcode = mod_consts.OP_CODE_LOOKUP[this.readInt()];
	switch (pkt.opcode) {
	case 'GET_CHILDREN':
	case 'GET_CHILDREN2':
		this.readGetChildrenRequest(pkt);
		break;
	case 'CREATE':
		this.readCreateRequest(pkt);
		break;
	case 'DELETE':
		this.readDeleteRequest(pkt);
		break;
	case 'GET_ACL':
		this.readGetACLRequest(pkt);
		break;
	case 'SET_WATCHES':
		this.readSetWatchesRequest(pkt);
		break;
	case 'GET_DATA':
		this.readGetDataRequest(pkt);
		break;
	case 'EXISTS':
		this.readExistsRequest(pkt);
		break;
	case 'SET_DATA':
		this.readSetDataRequest(pkt);
		break;
	case 'SYNC':
		this.readSyncRequest(pkt);
		break;
	default:
		throw (new Error('Unsupported opcode ' + pkt.opcode));
	}
	return (pkt);
};

ZKBuffer.prototype.writeRequest = function (pkt) {
	this.writeInt(pkt.xid);
	this.writeInt(mod_consts.OP_CODES[pkt.opcode]);
	switch (pkt.opcode) {
	case 'GET_CHILDREN':
	case 'GET_CHILDREN2':
		this.writeGetChildrenRequest(pkt);
		break;
	case 'CREATE':
		this.writeCreateRequest(pkt);
		break;
	case 'DELETE':
		this.writeDeleteRequest(pkt);
		break;
	case 'GET_ACL':
		this.writeGetACLRequest(pkt);
		break;
	case 'SET_WATCHES':
		this.writeSetWatchesRequest(pkt);
		break;
	case 'GET_DATA':
		this.writeGetDataRequest(pkt);
		break;
	case 'EXISTS':
		this.writeExistsRequest(pkt);
		break;
	case 'SET_DATA':
		this.writeSetDataRequest(pkt);
		break;
	case 'SYNC':
		this.writeSyncRequest(pkt);
		break;
	case 'CLOSE_SESSION':
	case 'PING':
		/* No extra data is included: the header is sufficient. */
		break;
	default:
		throw (new Error('Unsupported opcode ' + pkt.opcode));
	}
};

ZKBuffer.prototype.readGetChildrenRequest = function (pkt) {
	pkt.path = this.readUString();
	pkt.watch = this.readBool();
};

ZKBuffer.prototype.writeGetChildrenRequest = function (pkt) {
	this.writeUString(pkt.path, 'path');
	this.writeBool(pkt.watch, 'watch');
};

ZKBuffer.prototype.readCreateRequest = function (pkt) {
	pkt.path = this.readUString();
	pkt.data = this.readBuffer();
	pkt.acl = this.readACL();
	pkt.flags = [];
	var flags = this.readInt();
	Object.keys(mod_consts.CREATE_FLAGS).forEach(function (k) {
		var mask = mod_consts.CREATE_FLAGS[k];
		if ((flags & mask) === mask)
			pkt.flags.push(k);
	});
};

ZKBuffer.prototype.writeCreateRequest = function (pkt) {
	this.writeUString(pkt.path, 'path');
	this.writeBuffer(pkt.data, 'data');
	this.writeACL(pkt.acl, 'acl');
	var flags = 0;
	mod_assert.arrayOfString(pkt.flags, 'flags');
	pkt.flags.forEach(function (k) {
		var mask = mod_consts.CREATE_FLAGS[k];
		mod_assert.number(mask, 'unknown flag ' + k);
		flags |= mask;
	});
	this.writeInt(flags);
};

ZKBuffer.prototype.readDeleteRequest = function (pkt) {
	pkt.path = this.readUString();
	pkt.version = this.readInt();
};

ZKBuffer.prototype.writeDeleteRequest = function (pkt) {
	this.writeUString(pkt.path, 'name');
	this.writeInt(pkt.version, 'version');
};

ZKBuffer.prototype.readGetACLRequest = function (pkt) {
	pkt.path = this.readUString();
};

ZKBuffer.prototype.writeGetACLRequest = function (pkt) {
	this.writeUString(pkt.path, 'path');
};

ZKBuffer.prototype.readGetDataRequest = function (pkt) {
	pkt.path = this.readUString();
	pkt.watch = this.readBool();
};

ZKBuffer.prototype.writeGetDataRequest = function (pkt) {
	this.writeUString(pkt.path, 'path');
	this.writeBool(pkt.watch, 'watch');
};

ZKBuffer.prototype.readExistsRequest = function (pkt) {
	pkt.path = this.readUString();
	pkt.watch = this.readBool();
};

ZKBuffer.prototype.writeExistsRequest = function (pkt) {
	this.writeUString(pkt.path, 'path');
	this.writeBool(pkt.watch, 'watch');
};

ZKBuffer.prototype.readSetDataRequest = function (pkt) {
	pkt.path = this.readUString();
	pkt.data = this.readBuffer();
	pkt.version = this.readInt();
};

ZKBuffer.prototype.writeSetDataRequest = function (pkt) {
	this.writeUString(pkt.path, 'path');
	this.writeBuffer(pkt.data, 'data');
	this.writeInt(pkt.version, 'version');
};

ZKBuffer.prototype.readSyncRequest = function (pkt) {
	pkt.path = this.readUString();
};

ZKBuffer.prototype.writeSyncRequest = function (pkt) {
	this.writeUString(pkt.path, 'path');
};

ZKBuffer.prototype.readSetWatchesRequest = function (pkt) {
	var i, count;

	pkt.relZxid = this.readLongBuffer();

	count = this.readInt();
	pkt.events = {};
	pkt.events.dataChanged = [];
	for (i = 0; i < count; ++i)
		pkt.events.dataChanged.push(this.readUString());

	count = this.readInt();
	pkt.events.createdOrDestroyed = [];
	for (i = 0; i < count; ++i)
		pkt.events.createdOrDestroyed.push(this.readUString());

	count = this.readInt();
	pkt.events.childrenChanged = [];
	for (i = 0; i < count; ++i)
		pkt.events.childrenChanged.push(this.readUString());
};

ZKBuffer.prototype.writeSetWatchesRequest = function (pkt) {
	var evt, i;
	this.writeLong(pkt.relZxid, 'relZxid');

	evt = pkt.events.dataChanged || [];
	this.writeInt(evt.length);
	for (i = 0; i < evt.length; ++i)
		this.writeUString(evt[i], 'dataChanged[' + i + ']');

	evt = pkt.events.createdOrDestroyed || [];
	this.writeInt(evt.length);
	for (i = 0; i < evt.length; ++i)
		this.writeUString(evt[i], 'createdOrDestroyed[' + i + ']');

	evt = pkt.events.childrenChanged || [];
	this.writeInt(evt.length);
	for (i = 0; i < evt.length; ++i)
		this.writeUString(evt[i], 'childrenChanged[' + i + ']');
};

const SPECIAL_XIDS = {};
SPECIAL_XIDS[mod_consts.XID_NOTIFICATION] = 'NOTIFICATION';
SPECIAL_XIDS[mod_consts.XID_PING] = 'PING';
SPECIAL_XIDS[mod_consts.XID_AUTHENTICATION] = 'AUTH';
SPECIAL_XIDS[mod_consts.XID_SET_WATCHES] = 'SET_WATCHES';

ZKBuffer.prototype.readResponse = function (xidMap) {
	mod_assert.object(xidMap, 'xidMap');

	var pkt = {};
	pkt.xid = this.readInt();
	pkt.zxid = this.readLongBuffer();
	pkt.err = mod_consts.ERR_LOOKUP[this.readInt()];
	pkt.opcode = SPECIAL_XIDS[pkt.xid];
	if (pkt.opcode === undefined)
		pkt.opcode = xidMap[pkt.xid];
	mod_assert.ok(pkt.opcode, 'reply packet must match a request');
	if (pkt.err === 'OK') {
		switch (pkt.opcode) {
		case 'GET_CHILDREN':
		case 'GET_CHILDREN2':
			this.readGetChildrenResponse(pkt);
			break;
		case 'CREATE':
			this.readCreateResponse(pkt);
			break;
		case 'GET_ACL':
			this.readGetACLResponse(pkt);
			break;
		case 'GET_DATA':
			this.readGetDataResponse(pkt);
			break;
		case 'NOTIFICATION':
			this.readNotification(pkt);
			break;
		case 'EXISTS':
			this.readExistsResponse(pkt);
			break;
		case 'SET_DATA':
			this.readSetDataResponse(pkt);
			break;
		case 'SET_WATCHES':
		case 'PING':
		case 'SYNC':
		case 'DELETE':
		case 'CLOSE_SESSION':
			/*
			 * No special response packet format -- error code in
			 * header determines status of the request.
			 */
			break;
		default:
			throw (new Error('Unsupported opcode ' + pkt.opcode));
		}
	}
	return (pkt);
};

ZKBuffer.prototype.readGetChildrenResponse = function (pkt) {
	var count = this.readInt();
	pkt.children = [];
	for (var i = 0; i < count; ++i)
		pkt.children.push(this.readUString());
	if (pkt.opcode === 'GET_CHILDREN2')
		pkt.stat = this.readStat();
};

ZKBuffer.prototype.readCreateResponse = function (pkt) {
	pkt.path = this.readUString();
};

ZKBuffer.prototype.readExistsResponse = function (pkt) {
	pkt.stat = this.readStat();
};

ZKBuffer.prototype.readSetDataResponse = function (pkt) {
	pkt.stat = this.readStat();
};

ZKBuffer.prototype.readGetACLResponse = function (pkt) {
	pkt.acl = this.readACL();
	pkt.stat = this.readStat();
};

ZKBuffer.prototype.readGetDataResponse = function (pkt) {
	pkt.data = this.readBuffer();
	pkt.stat = this.readStat();
};

ZKBuffer.prototype.readNotification = function (pkt) {
	var type = this.readInt();
	pkt.type = mod_consts.NOTIFICATION_TYPE_LOOKUP[type];
	var state = this.readInt();
	pkt.state = mod_consts.STATE_LOOKUP[state];
	pkt.path = this.readUString();
};

ZKBuffer.prototype.readACL = function () {
	var count = this.readInt();
	var acl = [];
	for (var i = 0; i < count; ++i) {
		var line = {};
		line.perms = this.readPerms();
		line.id = this.readID();
		acl.push(line);
	}
	return (acl);
};

ZKBuffer.prototype.writeACL = function (acl) {
	mod_assert.arrayOfObject(acl, 'acl');
	var count = acl.length;
	this.writeInt(count);
	for (var i = 0; i < count; ++i) {
		var line = acl[i];
		this.writePerms(line.perms);
		this.writeID(line.id);
	}
};

ZKBuffer.prototype.readPerms = function () {
	var perms = [];
	var val = this.readInt();
	Object.keys(mod_consts.PERM_MASKS).forEach(function (k) {
		if (val & mod_consts.PERM_MASKS[k] != 0)
			perms.push(k);
	});
	return (perms);
};

ZKBuffer.prototype.writePerms = function (perms) {
	mod_assert.arrayOfString(perms, 'permissions');
	var val = 0;
	perms.forEach(function (k) {
		var mask = mod_consts.PERM_MASKS[k.toUpperCase()];
		mod_assert.number(mask, 'permission mask for ' + k);
		val |= mask;
	});
	this.writeInt(val);
};

ZKBuffer.prototype.readID = function () {
	var id = {};
	id.scheme = this.readUString();
	id.id = this.readUString();
	return (id);
};

ZKBuffer.prototype.writeID = function (id) {
	this.writeUString(id.scheme, 'id.scheme');
	this.writeUString(id.id, 'id');
};

ZKBuffer.prototype.readStat = function () {
	var stat = {};
	stat.czxid = this.readLongBuffer();
	stat.mzxid = this.readLongBuffer();
	stat.ctime = this.readLongDate();
	stat.mtime = this.readLongDate();
	stat.version = this.readInt();
	stat.cversion = this.readInt();
	stat.aversion = this.readInt();
	stat.ephemeralOwner = this.readLongBuffer();
	stat.dataLength = this.readInt();
	stat.numChildren = this.readInt();
	stat.pzxid = this.readLongBuffer();
	return (stat);
};
