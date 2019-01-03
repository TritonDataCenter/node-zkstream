/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
 */

module.exports = { ZKConnectionFSM: ZKConnectionFSM };

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
	if (this.isInState('closed'))
		return;
	this.emit('closeAsserted');
};

ZKConnectionFSM.prototype.destroy = function () {
	if (this.isInState('closed'))
		return;
	this.emit('destroyAsserted');
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
		host: this.zcf_server.address,
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
	S.on(this, 'destroyAsserted', function () {
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
	S.on(this, 'destroyAsserted', function () {
		S.gotoState('closed');
	});
	this.zcf_socket.pipe(this.zcf_decoder);
	this.zcf_encoder.pipe(this.zcf_socket);

	this.zcf_session = this.zcf_client.getSession();
	if (this.zcf_session === undefined) {
		S.gotoState('closed');
		return;
	}

	if (this.zcf_session.isAttaching()) {
		this.zcf_log.debug('found ZKSession in state ' +
		    this.zcf_session.getState() + ' while handshaking');
		S.gotoState('error');
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
	S.on(this, 'destroyAsserted', function () {
		S.gotoState('closed');
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
		self.zcf_encoder.end({
			opcode: 'CLOSE_SESSION',
			xid: xid
		});
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
	var opcode;
	if (req !== undefined && req.packet !== undefined)
		opcode = req.packet.opcode;

	self.zcf_log.trace({
		xid: pkt.xid,
		opcode: opcode,
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
			if (cb)
				cb();
		});
		this.zcf_reqs[pkt.xid].once('error', function (err) {
			if (cb)
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
	var timer = setTimeout(onTimeout, timeout);
	var t1 = new Date();
	this.zcf_encoder.write(pkt);
	function onPacket(pkt2) {
		delete (self.zcf_reqs[pkt.xid]);
		var t2 = new Date();
		clearTimeout(timer);
		self.zcf_log.trace('ping ok in %d ms', (t2 - t1));
		if (cb)
			cb(null, (t2 - t1));
	}
	function onTimeout() {
		req.removeListener('reply', onPacket);
		self.emit('pingTimeout');
	}
	function onError(err) {
		delete (self.zcf_reqs[pkt.xid]);
		clearTimeout(timer);
		if (cb)
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
