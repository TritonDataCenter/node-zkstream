/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
 */

module.exports = { Client: ZKClient };

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
const mod_cueball = require('cueball');
const mod_zks = require('./zk-session');
const mod_verror = require('verror');
const mod_vasync = require('vasync');

function ZKClient(opts) {
	mod_assert.object(opts, 'options');

	mod_assert.optionalObject(opts.log, 'options.log');
	if (opts.log == undefined) {
		this.zc_log = mod_bunyan.createLogger({
			name: 'zkstream',
			component: 'ZKClient'
		});
	} else {
		this.zc_log = opts.log.child({
			component: 'ZKClient'
		});
	}

	mod_assert.optionalArrayOfObject(opts.servers, 'options.servers');
	if (opts.servers === undefined) {
		mod_assert.string(opts.address, 'options.address');
		mod_assert.number(opts.port, 'options.port');
		this.zc_servers = [
			{ address: opts.address, port: opts.port }
		];
	} else {
		this.zc_servers = opts.servers;
		this.zc_servers.forEach(function (srv) {
			mod_assert.string(srv.address, 'servers[].address');
			mod_assert.number(srv.port, 'servers[].port');
		});
	}
	mod_assert.optionalNumber(opts.sessionTimeout,
	    'options.sessionTimeout');
	this.zc_sessionTimeout = opts.sessionTimeout;
	if (this.zc_sessionTimeout === undefined)
		this.zc_sessionTimeout = 30000;

	this.zc_session = undefined;
	this.zc_oldSession = undefined;

	this.zc_resolver = new mod_cueball.StaticIpResolver({
		defaultPort: 2181,
		backends: this.zc_servers
	});

	this.zc_set = new mod_cueball.ConnectionSet({
		resolver: this.zc_resolver,
		log: this.zc_log,
		recovery: {
			connect: {
				timeout: 3000,
				retries: 3,
				delay: 500
			},
			default: {
				timeout: 5000,
				retries: 3,
				delay: 1000
			}
		},
		target: 1,
		maximum: 3,
		connectionHandlesError: true,
		constructor: this._makeConnection.bind(this)
	});

	this.zc_set.on('added', this._onSetAdded.bind(this));
	this.zc_set.on('removed', this._onSetRemoved.bind(this));
	this.zc_set.on('stateChanged', this._onSetStateChanged.bind(this));

	this.zc_conns = {};
	this.zc_hdls = {};

	mod_fsm.FSM.call(this, 'normal');
}
mod_util.inherits(ZKClient, mod_fsm.FSM);

ZKClient.prototype.state_normal = function (S) {
	this._newSession();
	this.zc_resolver.start();
	S.on(this, 'closeAsserted', function () {
		S.gotoState('closing');
	});
};

ZKClient.prototype.state_closing = function (S) {
	var done = 0;
	var self = this;

	S.on(this.zc_session, 'stateChanged', function (st) {
		if ((st === 'closed' || st === 'expired') && ++done == 3) {
			S.gotoState('closed');
		}
	});

	S.on(this.zc_set, 'stateChanged', function (st) {
		if (st === 'stopped' && ++done == 3) {
			S.gotoState('closed');
		}
	});

	S.on(this.zc_resolver, 'stateChanged', function (st) {
		if (st === 'stopped' && ++done == 3) {
			S.gotoState('closed');
		}
	});

	if (this.zc_session.isInState('closed') ||
	    this.zc_session.isInState('expired')) {
		++done;
	}
	if (this.zc_set.isInState('stopped'))
		++done;
	if (this.zc_resolver.isInState('stopped'))
		++done;
	if (done == 3) {
		S.gotoState('closed');
	} else {
		this.zc_set.stop();
		this.zc_resolver.stop();
		this.zc_session.close();
	}

	S.interval(5000, function () {
		self.zc_log.trace('still waiting for zk client to shut down, ' +
		    '%d/3 done', done);
	});
};

ZKClient.prototype.state_closed = function (S) {
	this.emit('close');
};

ZKClient.prototype.close = function () {
	this.emit('closeAsserted');
};

ZKClient.prototype._newSession = function () {
	var self = this;
	if (!this.isInState('normal'))
		return;
	var s = new mod_zks.ZKSession({
		timeout: this.zc_sessionTimeout,
		log: this.zc_log
	});
	this.zc_session = s;
	function initialHandler(st) {
		if (st === 'attached') {
			s.removeListener('stateChanged', initialHandler);
			s.on('stateChanged', finalHandler);
			var c = s.getConnection();
			if (c.isInState('connected')) {
				setImmediate(function () {
					self.emit('connect');
				});
			} else {
				c.on('stateChanged', onConnCh);
				function onConnCh(st) {
					if (st === 'connected') {
						c.removeListener(
						    'stateChanged', onConnCh);
						self.emit('connect');
					}
				}
			}
		}
	}
	function finalHandler(st) {
		if (st === 'expired') {
			self.emit('expire');
		}
	}
	this.zc_session.on('stateChanged', initialHandler);
};

ZKClient.prototype.getSession = function () {
	if (!this.isInState('normal'))
		return (undefined);
	if (this.zc_session.isInState('expired') ||
	    this.zc_session.isInState('closed')) {
		this.zc_oldSession = this.zc_session;
		this._newSession();
	}
	return (this.zc_session);
};

ZKClient.prototype._onSetAdded = function (key, conn, hdl) {
	this.zc_conns[key] = conn;
	this.zc_hdls[key] = hdl;
};

ZKClient.prototype._onSetRemoved = function (key) {
	var hdl = this.zc_hdls[key];
	var conn = this.zc_conns[key];
	mod_assert.object(hdl);
	delete (this.zc_hdls[key]);
	conn.destroy();
	delete (this.zc_conns[key]);
	hdl.release();
};

ZKClient.prototype._onSetStateChanged = function (st) {
	var self = this;
	if (st === 'failed') {
		setImmediate(function () {
			self.emit('failed', new Error('Failed to connect to ' +
			    'ZK (exhausted initial retry policy)'));
		});
	}
};

ZKClient.prototype._makeConnection = function (backend) {
	var c = new ZKConnectionFSM({
		client: this,
		backend: backend,
		log: this.zc_log
	});
	c.connect();
	return (c);
};

ZKClient.prototype.currentConnection = function () {
	return (this.getSession().getConnection());
};

ZKClient.prototype.ping = function (cb) {
	var conn = this.currentConnection();
	if (conn === undefined)
		throw (new Error('Not connected to ZooKeeper'));
	return (conn.ping(cb));
};

ZKClient.prototype.list = function (path, cb) {
	mod_assert.string(path, 'path');
	mod_assert.optionalFunc(cb, 'callback');
	var conn = this.currentConnection();
	if (conn === undefined)
		throw (new Error('Not connected to ZooKeeper'));
	var req = conn.request({
		opcode: 'GET_CHILDREN2',
		path: path,
		watch: false
	});
	if (cb) {
		req.once('reply', function onReply(pkt) {
			cb(null, pkt.children, pkt.stat);
		});
		req.once('error', cb);
	}
	return (req);
};

ZKClient.prototype.get = function (path, cb) {
	mod_assert.string(path, 'path');
	mod_assert.optionalFunc(cb, 'callback');
	var conn = this.currentConnection();
	if (conn === undefined)
		throw (new Error('Not connected to ZooKeeper'));
	var req = conn.request({
		opcode: 'GET_DATA',
		path: path,
		watch: false
	});
	if (cb) {
		req.once('reply', function onReply(pkt) {
			cb(null, pkt.data, pkt.stat);
		});
		req.once('error', cb);
	}
	return (req);
};

ZKClient.prototype.create = function (path, data, options, cb) {
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

	var conn = this.currentConnection();
	if (conn === undefined)
		throw (new Error('Not connected to ZooKeeper'));
	var req = conn.request({
		opcode: 'CREATE',
		path: path,
		data: data,
		acl: options.acl,
		flags: options.flags
	});
	if (cb) {
		req.once('reply', function onReply(pkt) {
			cb(null, pkt.path);
		});
		req.once('error', cb);
	}
	return (req);
};

ZKClient.prototype.set = function (path, data, version, cb) {
	mod_assert.string(path, 'path');
	mod_assert.buffer(data, 'data');
	mod_assert.optionalNumber(version, 'version');
	mod_assert.optionalFunc(cb, 'callback');

	if (version === undefined)
		version = -1;

	var conn = this.currentConnection();
	if (conn === undefined)
		throw (new Error('Not connected to ZooKeeper'));
	var req = conn.request({
		opcode: 'SET_DATA',
		path: path,
		data: data,
		version: version
	});
	if (cb) {
		req.once('reply', function onReply(pkt) {
			cb(null, pkt.path);
		});
		req.once('error', cb);
	}
	return (req);
};

ZKClient.prototype.delete = function (path, version, cb) {
	mod_assert.string(path, 'path');
	mod_assert.number(version, 'version');
	mod_assert.optionalFunc(cb, 'callback');

	var conn = this.currentConnection();
	if (conn === undefined)
		throw (new Error('Not connected to ZooKeeper'));
	var req = conn.request({
		opcode: 'DELETE',
		path: path,
		version: version
	});
	if (cb) {
		req.once('reply', function onReply(pkt) {
			cb(null);
		});
		req.once('error', cb);
	}
	return (req);
};

ZKClient.prototype.stat = function (path, cb) {
	mod_assert.string(path, 'path');
	mod_assert.optionalFunc(cb, 'callback');

	var conn = this.currentConnection();
	if (conn === undefined)
		throw (new Error('Not connected to ZooKeeper'));
	var req = conn.request({
		opcode: 'EXISTS',
		path: path,
		watch: false
	});
	if (cb) {
		req.once('reply', function onReply(pkt) {
			cb(null, pkt.stat);
		});
		req.once('error', cb);
	}
	return (req);
};

ZKClient.prototype.sync = function (path, cb) {
	mod_assert.string(path, 'path');
	mod_assert.optionalFunc(cb, 'callback');

	var conn = this.currentConnection();
	if (conn === undefined)
		throw (new Error('Not connected to ZooKeeper'));
	var req = conn.request({
		opcode: 'SYNC',
		path: path
	});
	if (cb) {
		req.once('reply', function onReply(pkt) {
			cb(null);
		});
		req.once('error', cb);
	}
	return (req);
};

ZKClient.prototype.watcher = function (path) {
	return (this.getSession().watcher(path));
};



function ZKConnectionFSM(opts) {
	mod_assert.object(opts, 'options');
	mod_assert.object(opts.log, 'options.log');
	mod_assert.object(opts.client, 'options.client');
	mod_assert.object(opts.backend, 'options.backend');

	this.zcf_client = opts.client;
	this.zcf_log = opts.log.child({
		component: 'ZKConnectionFSM'
	});
	this.zcf_decoder = undefined;
	this.zcf_encoder = undefined;
	this.zcf_xidMap = {};
	this.zcf_xid = 0;
	this.zcf_reqs = {};
	this.zcf_socket = undefined;
	this.zcf_server = opts.backend;
	this.zcf_session = undefined;

	mod_fsm.FSM.call(this, 'init');
}
mod_util.inherits(ZKConnectionFSM, mod_fsm.FSM);

ZKConnectionFSM.prototype.connect = function () {
	mod_assert.ok(this.isInState('closed') || this.isInState('init'));
	this.emit('connectAsserted');
};

ZKConnectionFSM.prototype.close = function () {
	mod_assert.ok(!this.isInState('closed'));
	this.emit('closeAsserted');
};

ZKConnectionFSM.prototype.destroy = function () {
	if (this.isInState('closed'))
		return;
	this.emit('closeAsserted');
};

ZKConnectionFSM.prototype.nextXid = function () {
	return (this.zcf_xid++);
};

ZKConnectionFSM.prototype.state_init = function (S) {
	S.on(this, 'connectAsserted', function () {
		S.gotoState('connecting');
	});
};

ZKConnectionFSM.prototype.state_connecting = function (S) {
	var self = this;
	this.zcf_decoder = new mod_zkstreams.ZKDecodeStream({
		fsm: this
	});
	this.zcf_encoder = new mod_zkstreams.ZKEncodeStream({
		fsm: this
	});

	this.zcf_log = this.zcf_log.child({
		zkAddress: this.zcf_server.address,
		zkPort: this.zcf_server.port
	});
	this.zcf_log.trace('attempting new connection');

	this.zcf_socket = mod_net.connect({
		host: this.zcf_server.host,
		port: this.zcf_server.port,
		allowHalfOpen: true
	});
	S.on(this.zcf_socket, 'connect', function () {
		S.gotoState('handshaking');
	});
	S.on(this.zcf_socket, 'error', function (err) {
		self.zcf_lastError = err;
		S.gotoState('error');
	});
	S.on(this.zcf_socket, 'close', function () {
		S.gotoState('closed');
	});
	S.on(this, 'closeAsserted', function () {
		S.gotoState('closed');
	});
};

ZKConnectionFSM.prototype.state_handshaking = function (S) {
	var self = this;

	S.on(this.zcf_decoder, 'readable', function zkReadConnectResp() {
		var pkt = self.zcf_decoder.read();
		if (pkt === null)
			return;
		if (self.zcf_decoder.read() !== null) {
			self.zcf_lastError = new mod_errors.ZKProtocolError(
			    'UNEXPECTED_PACKET', 'Received unexpected ' +
			    'additional packet during connect phase');
			S.gotoState('error');
			return;
		}
		if (pkt.protocolVersion !== 0) {
			self.zcf_lastError = new mod_errors.ZKProtocolError(
			    'VERSION_INCOMPAT', 'Server version is not ' +
			    'compatible');
			S.gotoState('error');
			return;
		}
		self.emit('packet', pkt);
	});
	function onError(err) {
		self.zcf_lastError = err;
		S.gotoState('error');
	}
	S.on(this.zcf_decoder, 'error', onError);
	S.on(this.zcf_encoder, 'error', onError);
	S.on(this.zcf_socket, 'error', onError);
	S.on(this.zcf_socket, 'end', function () {
		self.zcf_lastError = new mod_errors.ZKProtocolError(
		    'CONNECTION_LOSS', 'Connection closed unexpectedly.');
		S.gotoState('error');
	});
	S.on(this, 'closeAsserted', function () {
		S.gotoState('closed');
	});
	this.zcf_socket.pipe(this.zcf_decoder);
	this.zcf_encoder.pipe(this.zcf_socket);

	this.zcf_session = this.zcf_client.getSession();
	if (this.zcf_session === undefined) {
		S.gotoState('closed');
		return;
	}

	S.on(this.zcf_session, 'stateChanged', function (st) {
		if (st === 'attached') {
			S.gotoState('connected');
		}
	});

	this.zcf_session.attachAndSendCR(this);
};

ZKConnectionFSM.prototype.state_connected = function (S) {
	var self = this;

	var pingInterval = this.zcf_session.getTimeout() / 4;
	if (pingInterval < 2000)
		pingInterval = 2000;
	var pingTimer = S.interval(pingInterval, function () {
		self.ping();
	});
	pingTimer.unref();

	this.zcf_log = this.zcf_log.child({
		sessionId: this.zcf_session.getSessionId()
	});

	S.on(this.zcf_decoder, 'readable', function onZkReadable() {
		var pkt;
		while (self.zcf_decoder &&
		    (pkt = self.zcf_decoder.read()) !== null) {
			self.emit('packet', pkt);

			/*
			 * Watchers and notifications are processed by
			 * the ZKSession instance.
			 */
			if (pkt.opcode === 'NOTIFICATION')
				continue;

			/* Everything else we need to emit on the req object */
			self.processReply(pkt);
		}
	});
	S.on(this.zcf_decoder, 'end', function () {
		self.zcf_lastError = new mod_errors.ZKProtocolError(
		    'CONNECTION_LOSS', 'Connection closed unexpectedly.');
		S.gotoState('error');
	});
	function onError(err) {
		self.zcf_lastError = err;
		S.gotoState('error');
	}
	S.on(this.zcf_decoder, 'error', onError);
	S.on(this.zcf_encoder, 'error', onError);
	S.on(this.zcf_socket, 'error', onError);
	S.on(this.zcf_socket, 'end', function () {
		self.zcf_lastError = new mod_errors.ZKProtocolError(
		    'CONNECTION_LOSS', 'Connection closed unexpectedly.');
		S.gotoState('error');
	});
	S.on(this, 'closeAsserted', function () {
		S.gotoState('closing');
	});
	S.on(this, 'pingTimeout', function () {
		self.zcf_lastError = new mod_errors.ZKPingTimeoutError();
		S.gotoState('error');
	});

	S.immediate(function () {
		self.emit('connect');
	});
};

ZKConnectionFSM.prototype.state_closing = function (S) {
	var self = this;
	var xid;
	S.on(this.zcf_decoder, 'readable', function () {
		var pkt;
		while (self.zcf_decoder &&
		    (pkt = self.zcf_decoder.read()) !== null) {
			if (pkt.xid !== xid) {
				self.processReply(pkt);

				if (Object.keys(self.zcf_reqs).length < 1)
					sendCloseSession();
			} else {
				S.gotoState('closed');
				return;
			}
		}
	});
	S.on(this.zcf_decoder, 'error', function (err) {
		self.zcf_lastError = err;
		S.gotoState('closed');
	});
	S.on(this.zcf_encoder, 'error', function (err) {
		self.zcf_lastError = err;
		S.gotoState('closed');
	});
	S.on(this.zcf_decoder, 'end', function () {
		S.gotoState('closed');
	});
	S.on(this.zcf_socket, 'error', function (err) {
		self.zcf_lastError = err;
		S.gotoState('closed');
	});
	if (Object.keys(self.zcf_reqs).length < 1)
		sendCloseSession();

	function sendCloseSession() {
		xid = self.nextXid();
		self.zcf_log.info({ xid: xid }, 'sent CLOSE_SESSION request');
		self.zcf_encoder.write({
			opcode: 'CLOSE_SESSION',
			xid: xid
		});
		self.zcf_encoder.end();
	}
};

ZKConnectionFSM.prototype.state_error = function (S) {
	var self = this;
	this.zcf_log.warn(this.zcf_lastError, 'error communicating with ZK');
	Object.keys(this.zcf_reqs).forEach(function (xid) {
		self.zcf_reqs[xid].emit('error', self.zcf_lastError);
	});
	this.zcf_reqs = {};

	/*
	 * Don't use S.immediate here, we always want to make sure this gets
	 * emitted, even though we're leaving this state.
	 */
	setImmediate(function () {
		self.emit('error', self.zcf_lastError);
	});

	S.gotoState('closed');
};

ZKConnectionFSM.prototype.state_closed = function (S) {
	if (this.zcf_encoder)
		this.zcf_encoder.end();
	this.zcf_encoder = undefined;
	if (this.zcf_socket)
		this.zcf_socket.destroy();
	this.zcf_socket = undefined;
	this.zcf_decoder = undefined;

	var self = this;
	S.immediate(function () {
		self.emit('close');
	});
};

ZKConnectionFSM.prototype.processReply = function (pkt) {
	var self = this;

	var req = self.zcf_reqs[pkt.xid];

	self.zcf_log.trace({
		xid: pkt.xid,
		opcode: req.packet.opcode,
		errorCode: pkt.err
	}, 'server replied to request');

	if (req !== undefined) {
		if (pkt.err === 'OK') {
			req.emit('reply', pkt);
			return;
		}
		var err = new mod_errors.ZKError(
		    pkt.err, mod_consts.ERR_TEXT[pkt.err]);
		req.emit('error', err, pkt);
	}
};

function ZKRequest(packet) {
	mod_events.EventEmitter.call(this);
	this.packet = packet;
}
mod_util.inherits(ZKRequest, mod_events.EventEmitter);

ZKConnectionFSM.prototype.request = function (pkt) {
	mod_assert.object(pkt, 'packet');
	var self = this;
	if (!this.isInState('connected'))
		throw (new Error('Client must be connected to send requests'));

	var req = new ZKRequest(pkt);
	pkt.xid = this.nextXid();
	this.zcf_reqs[pkt.xid] = req;
	req.once('reply', endRequest);
	req.once('error', endRequest);

	this.zcf_log.trace({
		xid: pkt.xid,
		opcode: pkt.opcode
	}, 'sent request to server');

	this.zcf_encoder.write(pkt);

	return (req);

	function endRequest() {
		delete (self.zcf_reqs[pkt.xid]);
	}
};

ZKConnectionFSM.prototype.send = function (pkt) {
	mod_assert.object(pkt, 'packet');
	this.zcf_encoder.write(pkt);
};

ZKConnectionFSM.prototype.ping = function (cb) {
	mod_assert.optionalFunc(cb, 'callback');
	if (!this.isInState('connected'))
		throw (new Error('Client must be connected to send packets'));
	var self = this;
	var pkt = {
		xid: mod_consts.XID_PING,
		opcode: 'PING'
	};
	var req = new ZKRequest(pkt);
	if (this.zcf_reqs[pkt.xid] !== undefined) {
		this.zcf_reqs[pkt.xid].once('reply', function () {
			if (cb !== undefined)
				cb();
		});
		this.zcf_reqs[pkt.xid].once('error', function (err) {
			if (cb !== undefined)
				cb(err);
		});
		return;
	}
	this.zcf_reqs[pkt.xid] = req;
	req.once('reply', onPacket);
	req.once('error', onError);
	var timeout = this.zcf_session.getTimeout() / 8;
	if (timeout < 2000)
		timeout = 2000;
	var timer = setTimeout(onTimeout, this.zcf_session.getTimeout() / 8);
	var t1 = new Date();
	this.zcf_encoder.write(pkt);
	function onPacket(pkt2) {
		delete (self.zcf_reqs[pkt.xid]);
		var t2 = new Date();
		clearTimeout(timer);
		self.zcf_log.trace('ping ok in %d ms', (t2 - t1));
		if (cb !== undefined)
			cb(null, (t2 - t1));
	}
	function onTimeout() {
		req.removeListener('reply', onPacket);
		self.emit('pingTimeout');
	}
	function onError(err) {
		delete (self.zcf_reqs[pkt.xid]);
		clearTimeout(timer);
		if (cb !== undefined)
			cb(err);
	}
};

ZKConnectionFSM.prototype.setWatches = function (events, zxid, cb) {
	mod_assert.object(events, 'events');
	mod_assert.func(cb, 'callback');
	if (!this.isInState('connected')) {
		throw (new mod_verror.VError('Client must be connected to ' +
		    'send packets (is in state %s)', this.getState()));
	}
	var self = this;
	var pkt = {
		xid: mod_consts.XID_SET_WATCHES,
		opcode: 'SET_WATCHES',
		relZxid: zxid,
		events: events
	};
	var req = new ZKRequest(pkt);
	if (this.zcf_reqs[pkt.xid] !== undefined) {
		this.zcf_reqs[pkt.xid].once('reply', function () {
			self.setWatches(events, zxid, cb);
		});
		this.zcf_reqs[pkt.xid].once('error', cb);
		return;
	}
	this.zcf_reqs[pkt.xid] = req;
	req.once('reply', onPacket);
	req.once('error', onError);
	this.zcf_encoder.write(pkt);
	function onPacket(pkt2) {
		delete (self.zcf_reqs[pkt.xid]);
		cb(null);
	}
	function onError(err) {
		delete (self.zcf_reqs[pkt.xid]);
		cb(err);
	}
};
