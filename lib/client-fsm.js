/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
 */

module.exports = { ClientFSM: ZKClientFSM };

const mod_fsm = require('mooremachine');
const mod_assert = require('assert-plus');
const mod_consts = require('./zk-consts');
const mod_util = require('util');
const mod_stream = require('stream');
const mod_errors = require('./errors');
const mod_jsbn = require('jsbn');
const mod_events = require('events');
const mod_zkstreams = require('./zk-streams');

function ZKClientFSM(opts) {
	mod_assert.object(opts, 'options');
	this.zs_decoder = new mod_zkstreams.ZKDecodeStream({
		fsm: this
	});
	this.zs_encoder = new mod_zkstreams.ZKEncodeStream({
		fsm: this
	});

	var self = this;
	function onStreamError() {
		self.gotoState('error');
	}
	this.zs_decoder.on('error', this.emit.bind(this, 'error'));
	this.zs_decoder.on('error', onStreamError);
	this.zs_encoder.on('error', this.emit.bind(this, 'error'));
	this.zs_encoder.on('error', onStreamError);

	this.zs_xidMap = {};
	this.zs_xid = 1;
	this.zs_reqs = {};
	this.zs_watchers = {};
	this.zs_socket = undefined;
	mod_fsm.FSM.call(this, 'connecting');
}
mod_util.inherits(ZKClientFSM, mod_fsm.FSM);

ZKClientFSM.prototype.attach = function (socket) {
	var self = this;
	this.zs_socket = socket;
	socket.pipe(this.zs_decoder);
	this.zs_encoder.pipe(socket);
	socket.on('error', this.emit.bind(this, 'error'));
	socket.on('error', function (err) {
		self.zs_lastError = err;
		self.gotoState('error');
	});
};

ZKClientFSM.prototype.close = function (cb) {
	mod_assert.object(this.zs_socket, 'an open connection');
	mod_assert.optionalFunc(cb, 'callback');
	if (cb)
		this.onState('closed', cb);
	this.zs_socket.end();
};

ZKClientFSM.prototype.nextXid = function () {
	return (this.zs_xid++);
};

ZKClientFSM.prototype.state_connecting = function (on) {
	var self = this;
	on(this.zs_decoder, 'readable', function () {
		var pkt = self.zs_decoder.read();
		if (self.zs_decoder.read() !== null) {
			self.emit('error', new mod_errors.ZKProtocolError(
			    'UNEXPECTED_PACKET', 'Received unexpected ' +
			    'additional packet during connect phase'));
			self.gotoState('error');
			return;
		}
		if (pkt.protocolVersion !== 0) {
			self.emit('error', new mod_errors.ZKProtocolError(
			    'VERSION_INCOMPAT', 'Server version is not ' +
			    'compatible'));
			self.gotoState('error');
			return;
		}
		self.zs_cr = pkt;
		self.gotoState('connected');
	});
	on(this.zs_decoder, 'error', function (err) {
		self.zs_lastError = err;
		self.gotoState('error');
	});
	on(this.zs_decoder, 'end', function () {
		self.gotoState('closed');
	});
	this.zs_encoder.write({
		protocolVersion: 0,
		lastZxidSeen: new mod_jsbn.BigInteger('0'),
		timeOut: 30000,
		sessionId: new mod_jsbn.BigInteger('0'),
		passwd: new Buffer(16).fill(0)
	});
};

ZKClientFSM.prototype.state_connected = function (on) {
	var self = this;

	var pingInterval = this.zs_cr.timeOut / 2;
	if (pingInterval < 2000)
		pingInterval = 2000;
	this.zs_pingTimer = setInterval(function () {
		self.ping();
	}, pingInterval);
	this.zs_pingTimer.unref();

	on(this.zs_decoder, 'readable', function () {
		var pkt;
		while ((pkt = self.zs_decoder.read()) !== null) {
			if (pkt.opcode === 'NOTIFICATION') {
				if (pkt.state !== 'SYNC_CONNECTED') {
					continue;
				}
				var watcher = self.zs_watchers[pkt.path];
				var evt = pkt.type.toLowerCase().
				    replace(/_[a-z]/g, function (s) {
					return (s.slice(1).toUpperCase());
				    });
				if (watcher)
					watcher.emit(evt);
				continue;
			}
			var req = self.zs_reqs[pkt.xid];
			if (req === undefined) {
				self.emit('packet', pkt);
			} else {
				if (pkt.err === 'OK') {
					req.emit('reply', pkt);
					continue;
				}
				var err = new mod_errors.ZKError(pkt.err,
				    mod_consts.ERR_TEXT[pkt.err]);
				req.emit('error', err, pkt);
			}
		}
	});
	on(this.zs_decoder, 'end', function () {
		self.gotoState('closed');
	});
	on(this.zs_decoder, 'error', function (err) {
		self.zs_lastError = err;
		self.gotoState('error');
	});
};

ZKClientFSM.prototype.state_error = function () {
	var self = this;
	clearInterval(this.zs_pingTimer);
	this.emit('error', this.zs_lastError);
	Object.keys(this.zs_reqs).forEach(function (xid) {
		self.zs_reqs[xid].emit('error', self.zs_lastError);
	});
	if (this.zs_socket)
		this.zs_socket.destroy();
};

ZKClientFSM.prototype.state_closed = function () {
	this.zs_encoder.end();
	clearInterval(this.zs_pingTimer);
	this.emit('close');
};

ZKClientFSM.prototype.ping = function (cb) {
	mod_assert.optionalFunc(cb, 'callback');
	var self = this;
	var req = new mod_events.EventEmitter();
	var pkt = {
		xid: mod_consts.XID_PING,
		opcode: 'PING'
	};
	if (this.zs_reqs[pkt.xid] !== undefined) {
		this.zs_reqs[pkt.xid].once('reply', function () {
			cb();
		});
		this.zs_reqs[pkt.xid].once('error', cb);
		return;
	}
	this.zs_reqs[pkt.xid] = req;
	req.once('reply', onPacket);
	req.once('error', onError);
	var timer = setTimeout(onTimeout, this.zs_cr.timeOut / 4);
	var t1 = new Date();
	this.zs_encoder.write(pkt);
	function onPacket(pkt2) {
		delete (self.zs_reqs[pkt.xid]);
		var t2 = new Date();
		clearTimeout(timer);
		if (cb)
			cb(null, (t2 - t1));
	}
	function onTimeout() {
		req.removeListener('reply', onPacket);
		self.zs_lastError = new Error('Ping timeout');
		self.gotoState('error');
		/*
		 * Moving to error state will spit an error on the
		 * outstanding request.
		 */
	}
	function onError(err) {
		delete (self.zs_reqs[pkt.xid]);
		clearTimeout(timer);
		if (cb)
			cb(err);
	}
};

ZKClientFSM.prototype.setWatches = function (events, cb) {
	mod_assert.object(events, 'events');
	mod_assert.func(cb, 'callback');
	var self = this;
	var req = new mod_events.EventEmitter();
	var pkt = {
		xid: mod_consts.XID_SET_WATCHES,
		opcode: 'SET_WATCHES',
		relZxid: new mod_jsbn.BigInteger('0'),
		events: events
	};
	if (this.zs_reqs[pkt.xid] !== undefined) {
		this.zs_reqs[pkt.xid].once('reply', function () {
			self.setWatches(events, cb);
		});
		this.zs_reqs[pkt.xid].once('error', cb);
		return;
	}
	this.zs_reqs[pkt.xid] = req;
	req.once('reply', onPacket);
	req.once('error', onError);
	this.zs_encoder.write(pkt);
	function onPacket(pkt2) {
		delete (self.zs_reqs[pkt.xid]);
		cb(null);
	}
	function onTimeout() {
		req.removeListener('reply', onPacket);
		self.zs_lastError = new Error('Ping timeout');
		self.gotoState('error');
	}
	function onError(err) {
		delete (self.zs_reqs[pkt.xid]);
		cb(err);
	}
};

function ZKRequest() {
	mod_events.EventEmitter.call(this);
}
mod_util.inherits(ZKRequest, mod_events.EventEmitter);

ZKClientFSM.prototype.request = function (pkt) {
	mod_assert.object(pkt, 'packet');
	var self = this;
	var req = new ZKRequest();
	pkt.xid = this.nextXid();
	this.zs_reqs[pkt.xid] = req;
	req.once('reply', endRequest);
	req.once('error', endRequest);
	this.zs_encoder.write(pkt);
	return (req);

	function endRequest() {
		delete (self.zs_reqs[pkt.xid]);
	}
};

ZKClientFSM.prototype.list = function (path, cb) {
	mod_assert.string(path, 'path');
	mod_assert.optionalFunc(cb, 'callback');
	var req = this.request({
		opcode: 'GET_CHILDREN',
		path: path,
		watch: false
	});
	if (cb) {
		req.once('reply', function (pkt) {
			cb(null, pkt.children);
		});
		req.once('error', cb);
	}
	return (req);
};

ZKClientFSM.prototype.get = function (path, cb) {
	mod_assert.string(path, 'path');
	mod_assert.optionalFunc(cb, 'callback');
	var req = this.request({
		opcode: 'GET_DATA',
		path: path,
		watch: false
	});
	if (cb) {
		req.once('reply', function (pkt) {
			cb(null, pkt.data);
		});
		req.once('error', cb);
	}
	return (req);
};

ZKClientFSM.prototype.create = function (path, data, options, cb) {
	mod_assert.string(path, 'path');
	mod_assert.buffer(data, 'data');
	mod_assert.optionalObject(options, 'options');
	mod_assert.optionalFunc(cb, 'callback');
	if (options === undefined)
		options = {};
	mod_assert.optionalArrayOfObject(options.acl, 'options.acl');
	mod_assert.optionalArrayOfString(options.flags, 'options.flags');

	if (options.acl === undefined) {
		options.acl = [ {
		    id: { scheme: 'world', id: 'anyone' },
		    perms: ['read', 'write', 'create', 'delete', 'admin']
		} ];
	}

	if (options.flags === undefined) {
		options.flags = [];
	}

	var req = this.request({
		opcode: 'CREATE',
		path: path,
		data: data,
		acl: options.acl,
		flags: options.flags
	});
	if (cb) {
		req.once('reply', function (pkt) {
			cb(null, pkt.path);
		});
		req.once('error', cb);
	}
	return (req);
};

ZKClientFSM.prototype.delete = function (path, version, cb) {
	mod_assert.string(path, 'path');
	mod_assert.number(version, 'version');
	mod_assert.optionalFunc(cb, 'callback');

	var req = this.request({
		opcode: 'DELETE',
		path: path,
		version: version
	});
	if (cb) {
		req.once('reply', function (pkt) {
			cb(null);
		});
		req.once('error', cb);
	}
	return (req);
};

ZKClientFSM.prototype.stat = function (path, cb) {
	mod_assert.string(path, 'path');
	mod_assert.optionalFunc(cb, 'callback');

	var req = this.request({
		opcode: 'EXISTS',
		path: path,
		watch: false
	});
	if (cb) {
		req.once('reply', function (pkt) {
			cb(null, pkt.stat);
		});
		req.once('error', cb);
	}
	return (req);
};

function ZKWatcher(client, path) {
	this.zkw_path = path;
	this.zkw_client = client;
	mod_events.EventEmitter.call(this);
}
mod_util.inherits(ZKWatcher, mod_events.EventEmitter);

ZKWatcher.prototype.once = function () {
	throw (new Error('ZKWatcher does not support once() (use on)'))
};

ZKWatcher.prototype.on = function (evt, cb) {
	mod_assert.string(evt, 'event');
	mod_assert.func(cb, 'callback');
	var first = (this.listeners(evt).length < 1);
	mod_events.EventEmitter.prototype.on.call(this, evt, cb);
	if (evt !== 'error' && first)
		this.emit(evt);
	return (this);
};

ZKWatcher.prototype.emit = function (evt) {
	var self = this;
	var req = this.zkw_client.request(this.reqPacket(evt));
	req.once('reply', function (pkt) {
		var args = [evt];
		switch (evt) {
		case 'created':
			args.push(pkt.stat);
			break;
		case 'deleted':
			return;
		case 'dataChanged':
			args.push(pkt.data);
			break;
		case 'childrenChanged':
			args.push(pkt.children);
			break;
		default:
			throw (new Error('Unknown watcher event ' + evt));
		}
		mod_events.EventEmitter.prototype.emit.apply(self, args);
	});
	req.once('error', function (err) {
		if (evt === 'created' && err.code === 'NO_NODE') {
			return;
		}
		if (evt === 'deleted' && err.code === 'NO_NODE') {
			mod_events.EventEmitter.prototype.emit.call(self, evt);
			return;
		}
		mod_events.EventEmitter.prototype.emit.call(self, 'error', err);
	});
};

ZKWatcher.prototype.reqPacket = function (evt) {
	var pkt = {
		path: this.zkw_path,
		watch: true
	};
	switch (evt) {
	case 'created':
	case 'deleted':
		pkt.opcode = 'EXISTS';
		break;
	case 'dataChanged':
		pkt.opcode = 'GET_DATA';
		break;
	case 'childrenChanged':
		pkt.opcode = 'GET_CHILDREN';
		break;
	default:
		throw (new Error('Unknown watcher event ' + evt));
	}
	return (pkt);
};

ZKClientFSM.prototype.watcher = function (path) {
	var w = this.zs_watchers[path];
	if (w)
		return (w);
	w = new ZKWatcher(this, path);
	this.zs_watchers[path] = w;
	return (w);
};
