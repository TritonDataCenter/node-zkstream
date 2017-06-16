/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
 */

module.exports = { Client: ZKClientFSM };

const mod_fsm = require('mooremachine');
const mod_assert = require('assert-plus');
const mod_consts = require('./zk-consts');
const mod_util = require('util');
const mod_stream = require('stream');
const mod_errors = require('./errors');
const mod_jsbn = require('jsbn');
const mod_events = require('events');
const mod_zkstreams = require('./zk-streams');
const mod_net = require('net');
const mod_bunyan = require('bunyan');

function ZKClientFSM(opts) {
	mod_assert.object(opts, 'options');

	mod_assert.optionalObject(opts.log, 'options.log');
	if (opts.log == undefined) {
		this.zs_log = mod_bunyan.createLogger({
			name: 'zkstream',
			component: 'ZKClientFSM'
		});
	} else {
		this.zs_log = opts.log.child({
			component: 'ZKClientFSM'
		});
	}

	mod_assert.optionalArrayOfObject(opts.servers, 'options.servers');
	if (opts.servers === undefined) {
		mod_assert.string(opts.host, 'options.host');
		mod_assert.number(opts.port, 'options.port');
		this.zs_servers = [
			{ host: opts.host, port: opts.port }
		];
	} else {
		this.zs_servers = opts.servers;
		this.zs_servers.forEach(function (srv) {
			mod_assert.string(srv.host, 'servers[].host');
			mod_assert.number(srv.port, 'servers[].port');
		});
	}
	shuffle(this.zs_servers);
	mod_assert.optionalNumber(opts.timeout, 'options.timeout');
	this.zs_timeout = opts.timeout;
	if (this.zs_timeout === undefined)
		this.zs_timeout = 30000;
	this.zs_decoder = undefined;
	this.zs_encoder = undefined;
	this.zs_xidMap = {};
	this.zs_xid = 0;
	this.zs_reqs = {};
	this.zs_watchers = {};
	this.zs_socket = undefined;
	this.zs_server = undefined;

	this.zs_cr = undefined;
	this.zs_lastPkt = undefined;
	this.zs_lastZxid = new mod_jsbn.BigInteger('0');

	mod_fsm.FSM.call(this, 'init');
}
mod_util.inherits(ZKClientFSM, mod_fsm.FSM);

ZKClientFSM.prototype.connect = function () {
	mod_assert.ok(this.isInState('closed') || this.isInState('init'));
	this.emit('connectAsserted');
};

ZKClientFSM.prototype.close = function () {
	mod_assert.ok(!this.isInState('closed'));
	this.emit('closeAsserted');
};

ZKClientFSM.prototype.destroy = function () {
	if (this.isInState('closed'))
		return;
	this.emit('closeAsserted');
};

ZKClientFSM.prototype.nextXid = function () {
	return (this.zs_xid++);
};

ZKClientFSM.prototype.state_init = function (S) {
	S.on(this, 'connectAsserted', function () {
		S.gotoState('connecting');
	});
};

ZKClientFSM.prototype.state_closed = function (S) {
	S.on(this, 'connectAsserted', function () {
		S.gotoState('connecting');
	});

	this.zs_log.info('closing connection');

	this.zs_encoder.end();
	this.zs_encoder = undefined;
	this.zs_socket.destroy();
	this.zs_socket = undefined;
	this.zs_decoder = undefined;

	this.zs_cr = undefined;
	this.zs_lastPkt = undefined;
	this.zs_lastZxid = new mod_jsbn.BigInteger('0');

	this.zs_xidMap = {};
	this.zs_xid = 0;
	this.zs_reqs = {};
	this.zs_watchers = {};

	var self = this;
	S.immediate(function () {
		self.emit('close');
	});
};

ZKClientFSM.prototype.state_connecting = function (S) {
	var self = this;
	this.zs_decoder = new mod_zkstreams.ZKDecodeStream({
		fsm: this
	});
	this.zs_encoder = new mod_zkstreams.ZKEncodeStream({
		fsm: this
	});

	this.zs_server = this.zs_servers.shift();
	this.zs_servers.push(this.zs_server);

	this.zs_log = this.zs_log.child({
		zkHost: this.zs_server.host,
		zkPort: this.zs_server.port
	});
	this.zs_log.trace('attempting new connection');

	this.zs_socket = mod_net.connect({
		host: this.zs_server.host,
		port: this.zs_server.port,
		allowHalfOpen: true
	});
	S.on(this.zs_socket, 'connect', function () {
		S.gotoState('handshaking');
	});
	S.on(this.zs_socket, 'error', function (err) {
		self.zs_lastError = err;
		S.gotoState('error');
	});
	S.on(this.zs_socket, 'close', function () {
		S.gotoState('closed');
	});
	S.on(this, 'closeAsserted', function () {
		S.gotoState('closed');
	});
};

ZKClientFSM.prototype.state_handshaking = function (S) {
	var self = this;
	S.on(this.zs_decoder, 'readable', function zkReadConnectResp() {
		var pkt = self.zs_decoder.read();
		if (pkt === null)
			return;
		if (self.zs_decoder.read() !== null) {
			self.zs_lastError = new mod_errors.ZKProtocolError(
			    'UNEXPECTED_PACKET', 'Received unexpected ' +
			    'additional packet during connect phase');
			S.gotoState('error');
			return;
		}
		if (pkt.protocolVersion !== 0) {
			self.zs_lastError = new mod_errors.ZKProtocolError(
			    'VERSION_INCOMPAT', 'Server version is not ' +
			    'compatible');
			S.gotoState('error');
			return;
		}
		if (pkt.sessionId.toString('hex') === '0000000000000000') {
			self.zs_lastError = new mod_errors.ZKProtocolError(
			    'SESSION_EXPIRED', 'Attempted to resume a ' +
			    'session that had already expired');
			S.gotoState('closed');
			return;
		}
		var verb = 'created';
		if (self.zs_cr !== undefined) {
			verb = 'resumed';
		}
		self.zs_log.info('%s zookeeper session %s with timeout %d ms',
		    verb, pkt.sessionId.toString('hex'), pkt.timeOut);
		self.zs_log = self.zs_log.child({
			zkSessionId: pkt.sessionId.toString('hex')
		});
		self.zs_cr = pkt;
		self.zs_lastPkt = new Date();
		S.gotoState('connected');
	});
	S.on(this.zs_decoder, 'error', function (err) {
		self.zs_lastError = err;
		S.gotoState('error');
	});
	S.on(this.zs_encoder, 'error', function (err) {
		self.zs_lastError = err;
		S.gotoState('error');
	});
	S.on(this.zs_decoder, 'end', function () {
		S.gotoState('closed');
	});
	S.on(this.zs_socket, 'error', function (err) {
		self.zs_lastError = err;
		S.gotoState('error');
	});
	S.on(this, 'closeAsserted', function () {
		S.gotoState('closed');
	});
	this.zs_socket.pipe(this.zs_decoder);
	this.zs_encoder.pipe(this.zs_socket);
	if (this.zs_cr !== undefined) {
		this.zs_log.trace('attempting to resume session %s',
		    this.zs_cr.sessionId.toString('hex'));
	}
	this.zs_encoder.write({
		protocolVersion: 0,
		lastZxidSeen: this.zs_lastZxid,
		timeOut: this.zs_timeout,
		sessionId: this.zs_cr ?
		    this.zs_cr.sessionId : new mod_jsbn.BigInteger('0'),
		passwd: this.zs_cr ?
		    this.zs_cr.passwd : new Buffer(16).fill(0)
	});
};

ZKClientFSM.prototype.state_connected = function (S) {
	var self = this;

	var pingInterval = this.zs_cr.timeOut / 4;
	if (pingInterval < 2000)
		pingInterval = 2000;
	var pingTimer = S.interval(pingInterval, function () {
		self.ping();
	});
	pingTimer.unref();

	S.on(this.zs_decoder, 'readable', function onZkReadable() {
		var pkt;
		while (self.zs_decoder &&
		    (pkt = self.zs_decoder.read()) !== null) {

			self.zs_lastPkt = new Date();

			if (pkt.opcode === 'NOTIFICATION') {
				if (pkt.state !== 'SYNC_CONNECTED') {
					self.zs_log.warn({
						xid: pkt.xid,
						errorCode: pkt.err,
						state: pkt.state,
						type: pkt.type,
						zxid: pkt.zxid.toString('hex')
					}, 'received notification with bad ' +
					    'state %s', pkt.state);
					continue;
				}
				var watcher = self.zs_watchers[pkt.path];
				var evt = pkt.type.toLowerCase().
				    replace(/_[a-z]/g, function (s) {
					return (s.slice(1).toUpperCase());
				    });
				self.zs_log.trace({
					xid: pkt.xid,
					errorCode: pkt.err,
					state: pkt.state,
					zxid: pkt.zxid.toString('hex'),
					type: pkt.type
				}, 'notification %s for %s', evt, pkt.path);
				if (watcher)
					watcher.emit(evt);
				continue;
			}

			var zxid = new mod_jsbn.BigInteger(pkt.zxid);
			if (self.zs_lastZxid === undefined ||
			    zxid.compareTo(self.zs_lastZxid) > 0) {
				self.zs_lastZxid = zxid;
			}

			var req = self.zs_reqs[pkt.xid];
			self.zs_log.trace({
				xid: pkt.xid,
				opcode: req.packet.opcode,
				errorCode: pkt.err
			}, 'server replied to request');
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
	S.on(this.zs_decoder, 'end', function () {
		self.zs_lastError = new mod_errors.ZKProtocolError(
		    'CONNECTION_LOSS', 'Connection closed unexpectedly.');
		S.gotoState('error');
	});
	S.on(this.zs_decoder, 'error', function (err) {
		self.zs_lastError = err;
		S.gotoState('error');
	});
	S.on(this.zs_encoder, 'error', function (err) {
		self.zs_lastError = err;
		S.gotoState('error');
	});
	S.on(this.zs_socket, 'error', function (err) {
		self.zs_lastError = err;
		S.gotoState('error');
	});
	S.on(this.zs_socket, 'close', function () {
		self.zs_lastError = new mod_errors.ZKProtocolError(
		    'CONNECTION_LOSS', 'Connection closed unexpectedly.');
		S.gotoState('error');
	});
	S.on(this, 'closeAsserted', function () {
		S.gotoState('closing');
	});
	S.on(this, 'pingTimeout', function () {
		self.zs_lastError = new mod_errors.ZKPingTimeoutError();
		S.gotoState('error');
	});

	this.resumeWatchers();
	S.immediate(function () {
		self.emit('connect');
	});
};

ZKClientFSM.prototype.state_closing = function (S) {
	var self = this;
	var xid = this.nextXid();
	S.on(this.zs_decoder, 'readable', function () {
		var pkt;
		while (self.zs_decoder &&
		    (pkt = self.zs_decoder.read()) !== null) {
			if (pkt.xid !== xid) {
				var req = self.zs_reqs[pkt.xid];
				if (req !== undefined) {
					if (pkt.err === 'OK') {
						req.emit('reply', pkt);
						continue;
					}
					var err = new mod_errors.ZKError(
					    pkt.err,
					    mod_consts.ERR_TEXT[pkt.err]);
					req.emit('error', err, pkt);
				}
			} else {
				S.gotoState('closed');
				return;
			}
		}
	});
	S.on(this.zs_decoder, 'error', function (err) {
		self.zs_lastError = err;
		S.gotoState('closed');
	});
	S.on(this.zs_encoder, 'error', function (err) {
		self.zs_lastError = err;
		S.gotoState('closed');
	});
	S.on(this.zs_decoder, 'end', function () {
		S.gotoState('closed');
	});
	S.on(this.zs_socket, 'error', function (err) {
		self.zs_lastError = err;
		S.gotoState('closed');
	});
	this.zs_log.info({ xid: xid }, 'sent CLOSE_SESSION request');
	this.zs_encoder.write({
		opcode: 'CLOSE_SESSION',
		xid: xid
	});
	this.zs_encoder.end();
};

ZKClientFSM.prototype.state_error = function (S) {
	var self = this;
	this.zs_log.warn(this.zs_lastError, 'error communicating with ZK, ' +
	    'cancelling all outstanding requests');
	Object.keys(this.zs_reqs).forEach(function (xid) {
		self.zs_reqs[xid].emit('error', self.zs_lastError);
	});
	this.zs_reqs = {};
	if (this.zs_cr !== undefined && this.zs_lastPkt !== undefined) {
		var now = new Date();
		var delta = now.getTime() - this.zs_lastPkt.getTime();
		if (delta < this.zs_cr.timeOut) {
			this.zs_log.trace('has been %d ms since last ' +
			    'session renewal, will retry (session timeout = ' +
			    '%d ms)', delta, this.zs_cr.timeOut);
			S.gotoState('connecting');
			return;
		}
	}
	/*
	 * Don't use S.immediate here, we always want to make sure this gets
	 * emitted, even though we're leaving this state.
	 */
	setImmediate(function () {
		this.emit('error', this.zs_lastError);
	});

	S.gotoState('closed');
};

ZKClientFSM.prototype.ping = function (cb) {
	mod_assert.optionalFunc(cb, 'callback');
	if (!this.isInState('connected'))
		throw (new Error('Client must be connected to send packets'));
	var self = this;
	var pkt = {
		xid: mod_consts.XID_PING,
		opcode: 'PING'
	};
	var req = new ZKRequest(pkt);
	if (this.zs_reqs[pkt.xid] !== undefined) {
		this.zs_reqs[pkt.xid].once('reply', function () {
			if (cb !== undefined)
				cb();
		});
		this.zs_reqs[pkt.xid].once('error', function (err) {
			if (cb !== undefined)
				cb(err);
		});
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
		self.zs_log.trace('ping ok in %d ms', (t2 - t1));
		if (cb !== undefined)
			cb(null, (t2 - t1));
	}
	function onTimeout() {
		req.removeListener('reply', onPacket);
		self.emit('pingTimeout');
	}
	function onError(err) {
		delete (self.zs_reqs[pkt.xid]);
		clearTimeout(timer);
		if (cb !== undefined)
			cb(err);
	}
};

ZKClientFSM.prototype.resumeWatchers = function () {
	var self = this;
	var events = {
		dataChanged: [],
		createdOrDestroyed: [],
		childrenChanged: []
	};
	var count = 0;
	var paths = Object.keys(this.zs_watchers);
	paths.forEach(function (path) {
		var w = self.zs_watchers[path];
		var evts = w.eventsActive();
		var cod = false;
		evts.forEach(function (evt) {
			if (evt === 'created' || evt === 'deleted') {
				if (cod)
					return;
				events.createdOrDestroyed.push(path);
				++count;
				cod = true;
			} else if (evt === 'dataChanged') {
				events.dataChanged.push(path);
				++count;
			} else if (evt === 'childrenChanged') {
				events.childrenChanged.push(path);
				++count;
			}
		});
	});
	if (count < 1)
		return;
	var zxid = this.zs_lastZxid;
	var hex = zxid.toString(16);
	this.zs_log.info('re-arming %d node watchers at zxid %s', count, hex);
	this.setWatches(events, zxid, function (err) {
		if (err) {
			self.emit('pingTimeout');
		}
	});
};

ZKClientFSM.prototype.setWatches = function (events, zxid, cb) {
	mod_assert.object(events, 'events');
	mod_assert.func(cb, 'callback');
	if (!this.isInState('connected'))
		throw (new Error('Client must be connected to send packets'));
	var self = this;
	var pkt = {
		xid: mod_consts.XID_SET_WATCHES,
		opcode: 'SET_WATCHES',
		relZxid: zxid,
		events: events
	};
	var req = new ZKRequest(pkt);
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
	function onError(err) {
		delete (self.zs_reqs[pkt.xid]);
		cb(err);
	}
};

function ZKRequest(packet) {
	mod_events.EventEmitter.call(this);
	this.packet = packet;
}
mod_util.inherits(ZKRequest, mod_events.EventEmitter);

ZKClientFSM.prototype.request = function (pkt) {
	mod_assert.object(pkt, 'packet');
	var self = this;
	if (!this.isInState('connected'))
		throw (new Error('Client must be connected to send packets'));

	var req = new ZKRequest(pkt);
	pkt.xid = this.nextXid();
	this.zs_reqs[pkt.xid] = req;
	req.once('reply', endRequest);
	req.once('error', endRequest);

	this.zs_log.trace({
		xid: pkt.xid,
		opcode: pkt.opcode
	}, 'sent request to server');

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
		opcode: 'GET_CHILDREN2',
		path: path,
		watch: false
	});
	if (cb) {
		req.once('reply', function (pkt) {
			cb(null, pkt.children, pkt.stat);
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
			cb(null, pkt.data, pkt.stat);
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

ZKClientFSM.prototype.set = function (path, data, version, cb) {
	mod_assert.string(path, 'path');
	mod_assert.buffer(data, 'data');
	mod_assert.optionalNumber(version, 'version');
	mod_assert.optionalFunc(cb, 'callback');

	if (version === undefined)
		version = -1;

	var req = this.request({
		opcode: 'SET_DATA',
		path: path,
		data: data,
		version: version
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

ZKClientFSM.prototype.sync = function (path, cb) {
	mod_assert.string(path, 'path');
	mod_assert.optionalFunc(cb, 'callback');

	var req = this.request({
		opcode: 'SYNC',
		path: path
	});
	if (cb) {
		req.once('reply', function (pkt) {
			cb(null);
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

ZKWatcher.prototype.eventsActive = function () {
	var self = this;
	var evts = ['created', 'deleted', 'dataChanged', 'childrenChanged'];
	evts = evts.filter(function (evt) {
		return (self.listeners(evt).length > 0);
	});
	return (evts);
};

ZKWatcher.prototype.once = function () {
	throw (new Error('ZKWatcher does not support once() (use on)'));
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
	if (this.listeners(evt).length < 1)
		return;
	if (!this.zkw_client.isInState('connected')) {
		function onStateChanged(st) {
			if (st === 'connected') {
				self.emit(evt);
				self.zkw_client.removeListener('stateChanged',
				    onStateChanged);
			}
		}
		this.zkw_client.zs_log.trace({
			event: evt,
			clientState: this.zkw_client.getState()
		}, 'deferring watcher refresh until after reconnect');
		this.zkw_client.on('stateChanged', onStateChanged);
		return;
	}
	var qpkt = this.reqPacket(evt);
	var req = this.zkw_client.request(qpkt);
	req.on('reply', function (pkt) {
		var args = [evt];
		switch (evt) {
		case 'created':
			args.push(pkt.stat);
			break;
		case 'deleted':
			return;
		case 'dataChanged':
			args.push(pkt.data, pkt.stat);
			break;
		case 'childrenChanged':
			args.push(pkt.children, pkt.stat);
			break;
		default:
			throw (new Error('Unknown watcher event ' + evt));
		}
		mod_events.EventEmitter.prototype.emit.apply(self, args);
	});
	req.on('error', function (err) {
		if (err.code === 'PING_TIMEOUT') {
			self.emit(evt);
			return;
		}
		if (evt === 'created' && err.code === 'NO_NODE') {
			return;
		}
		if (evt === 'deleted' && err.code === 'NO_NODE') {
			mod_events.EventEmitter.prototype.emit.call(self, evt);
			return;
		}
		if (err.code === 'NO_NODE') {
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
		pkt.opcode = 'GET_CHILDREN2';
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

/* A Fisher-Yates shuffle. */
function shuffle(array) {
	var i = array.length;
	while (i > 0) {
		var j = Math.floor(Math.random() * i);
		--i;
		var temp = array[i];
		array[i] = array[j];
		array[j] = temp;
	}
	return (array);
}
