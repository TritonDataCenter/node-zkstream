/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
 */

module.exports = {
	ZKSession: ZKSession,
	ZKWatcher: ZKWatcher
};

const mod_fsm = require('mooremachine');
const mod_assert = require('assert-plus');
const mod_consts = require('./zk-consts');
const mod_util = require('util');
const mod_stream = require('stream');
const mod_errors = require('./errors');
const mod_jsbn = require('jsbn');
const mod_events = require('events');
const mod_bunyan = require('bunyan');
const mod_verror = require('verror');

function ZKSession(opts) {
	mod_assert.object(opts, 'options');
	mod_assert.number(opts.timeout, 'options.timeout');
	mod_assert.object(opts.log, 'options.log');

	this.zs_conn = undefined;
	this.zs_oldConn = undefined;

	this.zs_lastPkt = undefined;
	this.zs_expiryTimer = new mod_events.EventEmitter();
	this.zs_watchers = {};
	this.zs_timeout = opts.timeout;
	this.zs_log = opts.log.child({
		component: 'ZKSession'
	});

	this.zs_lastZxid = new mod_jsbn.BigInteger('0');
	this.zs_sessionId = new mod_jsbn.BigInteger('0');
	this.zs_passwd = new Buffer(8).fill(0);

	mod_fsm.FSM.call(this, 'detached');
}
mod_util.inherits(ZKSession, mod_fsm.FSM);

ZKSession.prototype.isAttaching = function () {
	return (this.isInState('attaching') || this.isInState('reattaching'));
};

ZKSession.prototype.isAlive = function () {
	if (this.zs_sessionId === undefined)
		return (false);
	if (this.zs_lastPkt === undefined)
		return (false);
	var now = new Date();
	var delta = now.getTime() - this.zs_lastPkt.getTime();
	if (delta < this.zs_timeout)
		return (true);
	return (false);
};

ZKSession.prototype.attachAndSendCR = function (conn) {
	if (!this.isInState('detached') &&
	    !this.isInState('attached')) {
		throw (new Error('ZKSession#attachAndSendCR may only be ' +
		    'called in state "attached" or "detached" (is in ' +
		    this.getState() + ')'));
	}
	this.emit('assertAttach', conn);
};

ZKSession.prototype.resetExpiryTimer = function () {
	var self = this;
	this.zs_lastPkt = new Date();
	if (this.zs_expiryTimer.ref)
		clearTimeout(this.zs_expiryTimer.ref);
	this.zs_expiryTimer.ref = setTimeout(function () {
		self.zs_expiryTimer.ref = undefined;
		self.zs_expiryTimer.emit('timeout');
	}, this.zs_timeout * 2);
};

ZKSession.prototype.getTimeout = function () {
	return (this.zs_timeout);
};

ZKSession.prototype.getConnection = function () {
	if (!this.isInState('attached'))
		return (undefined);
	return (this.zs_conn);
};

ZKSession.prototype.getSessionId = function () {
	var b = new Buffer(this.zs_sessionId.toByteArray());
	return (b.toString('hex'));
};

ZKSession.prototype.close = function () {
	this.emit('closeAsserted');
};

ZKSession.prototype.state_detached = function (S) {
	var self = this;
	if (this.zs_conn)
		this.zs_conn.destroy();
	this.zs_conn = undefined;
	S.on(this, 'assertAttach', function (client) {
		self.zs_conn = client;
		S.gotoState('attaching');
	});
	S.on(this, 'closeAsserted', function () {
		S.gotoState('closed');
	});
	S.on(this.zs_expiryTimer, 'timeout', function () {
		S.gotoState('expired');
	});
};

ZKSession.prototype.state_attaching = function (S) {
	var self = this;

	function onError() {
		if (self.isAlive()) {
			S.gotoState('detached');
		} else if (!self.zs_sessionId.equals(
		    mod_jsbn.BigInteger.ZERO)) {
			S.gotoState('expired');
		} else {
			S.gotoState('detached');
		}
	}

	S.on(this.zs_conn, 'error', onError);
	/*
	 * This can happen separately to an 'error' event when cueball
	 * times out the connect attempt.
	 */
	S.on(this.zs_conn, 'close', onError);

	S.on(this.zs_conn, 'packet', function (pkt) {
		var sid = new mod_jsbn.BigInteger(pkt.sessionId);
		if (sid.equals(mod_jsbn.BigInteger.ZERO)) {
			S.gotoState('expired');
			return;
		}
		var verb = 'created';
		if (!self.zs_sessionId.equals(mod_jsbn.BigInteger.ZERO))
			verb = 'resumed';
		self.zs_log.info('%s zookeeper session %s with timeout ' +
		    '%d ms', verb, pkt.sessionId.toString('hex'), pkt.timeOut);
		self.zs_log = self.zs_log.child({
			id: pkt.sessionId.toString('hex')
		});
		self.zs_timeout = pkt.timeOut;
		self.zs_sessionId = sid;
		self.zs_passwd = pkt.passwd;
		self.resetExpiryTimer();
		S.gotoState('attached');
	});

	S.on(this.zs_expiryTimer, 'timeout', function () {
		S.gotoState('expired');
	});

	S.on(this, 'closeAsserted', function () {
		S.gotoState('closing');
	});

	this.zs_conn.send({
		protocolVersion: 0,
		lastZxidSeen: this.zs_lastZxid,
		timeOut: this.zs_timeout,
		sessionId: this.zs_sessionId,
		passwd: this.zs_passwd
	});
};

ZKSession.prototype.state_attached = function (S) {
	var self = this;

	S.on(this.zs_conn, 'close', function () {
		if (self.isAlive()) {
			S.gotoState('detached');
		} else {
			S.gotoState('expired');
		}
	});

	S.on(this.zs_conn, 'error', function () {
		if (self.isAlive()) {
			S.gotoState('detached');
		} else {
			S.gotoState('expired');
		}
	});

	S.on(this.zs_conn, 'packet', function (pkt) {
		self.resetExpiryTimer();
		if (pkt.opcode !== 'NOTIFICATION') {
			var zxid = new mod_jsbn.BigInteger(pkt.zxid);
			if (self.zs_lastZxid === undefined ||
			    zxid.compareTo(self.zs_lastZxid) > 0) {
				self.zs_lastZxid = zxid;
			}
			return;
		}
		self.processNotification(pkt);
	});

	S.on(this.zs_expiryTimer, 'timeout', function () {
		S.gotoState('expired');
	});

	S.on(this, 'closeAsserted', function () {
		S.gotoState('closing');
	});

	S.on(this.zs_conn, 'stateChanged', function (st) {
		if (st === 'connected') {
			self.resumeWatches();
		}
	});

	S.on(this, 'assertAttach', function (client) {
		self.zs_oldConn = self.zs_conn;
		self.zs_conn = client;
		S.gotoState('reattaching');
	});
};

ZKSession.prototype.state_reattaching = function (S) {
	var self = this;

	S.on(this.zs_conn, 'packet', function (pkt) {
		var sid = new mod_jsbn.BigInteger(pkt.sessionId);
		if (sid.equals(mod_jsbn.BigInteger.ZERO)) {
			revert();
			return;
		}
		/*
		 * We don't do anything to oldConn here: cueball will handle
		 * destroying it for us once this new connection has emitted
		 * 'connect'.
		 */
		self.zs_log.info('moved zookeeper session %s to more ' +
		    'preferred backend (%s:%d) with timeout %d ms',
		    pkt.sessionId.toString('hex'),
		    self.zs_conn.zcf_server.address,
		    self.zs_conn.zcf_server.port,
		    pkt.timeOut);
		self.zs_log = self.zs_log.child({
			id: pkt.sessionId.toString('hex')
		});
		self.zs_timeout = pkt.timeOut;
		self.zs_sessionId = sid;
		self.zs_passwd = pkt.passwd;
		self.resetExpiryTimer();
		S.gotoState('attached');
	});

	function revert() {
		if (self.isAlive() &&
		    self.zs_oldConn.isInState('connected')) {
			self.zs_conn = self.zs_oldConn;
			S.gotoState('attached');
		} else if (self.isAlive()) {
			self.zs_oldConn.close();
			S.gotoState('detached');
		} else {
			self.zs_oldConn.close();
			S.gotoState('expired');
		}
	}
	S.on(this.zs_conn, 'error', revert);
	S.on(this.zs_conn, 'close', revert);
	S.on(this.zs_expiryTimer, 'timeout', revert);

	S.on(this, 'closeAsserted', function () {
		self.zs_oldConn.close();
		S.gotoState('closing');
	});

	this.zs_conn.send({
		protocolVersion: 0,
		lastZxidSeen: this.zs_lastZxid,
		timeOut: this.zs_timeout,
		sessionId: this.zs_sessionId,
		passwd: this.zs_passwd
	});
};

ZKSession.prototype.state_closing = function (S) {
	S.on(this.zs_conn, 'error', function () {
		S.gotoState('closed');
	});

	S.on(this.zs_conn, 'close', function () {
		S.gotoState('closed');
	});

	S.on(this.zs_expiryTimer, 'timeout', function () {
		S.gotoState('closed');
	});

	this.zs_conn.close();
};

ZKSession.prototype.state_expired = function (S) {
	if (this.zs_conn)
		this.zs_conn.destroy();
	this.zs_conn = undefined;
	if (this.zs_expiryTimer.ref)
		clearTimeout(this.zs_expiryTimer.ref);
	this.zs_expiryTimer = undefined;
	this.zs_log.warn('ZK session expired');
};

ZKSession.prototype.state_closed = function (S) {
	if (this.zs_conn)
		this.zs_conn.destroy();
	this.zs_conn = undefined;
	if (this.zs_expiryTimer.ref)
		clearTimeout(this.zs_expiryTimer.ref);
	this.zs_expiryTimer = undefined;
	this.zs_log.info('ZK session closed');
};

ZKSession.prototype.processNotification = function (pkt) {
	var self = this;
	if (pkt.state !== 'SYNC_CONNECTED') {
		self.zs_log.warn({
			xid: pkt.xid,
			errorCode: pkt.err,
			state: pkt.state,
			type: pkt.type,
			zxid: pkt.zxid.toString('hex')
		}, 'received notification with bad state %s', pkt.state);
		return;
	}
	var watcher = self.zs_watchers[pkt.path];
	var evt = pkt.type.toLowerCase().replace(/_[a-z]/g, function (s) {
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
};

ZKSession.prototype.resumeWatches = function () {
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
	this.zs_conn.setWatches(events, zxid, function (err) {
		if (err) {
			self.emit('pingTimeout');
		}
	});
};

ZKSession.prototype.watcher = function (path) {
	var w = this.zs_watchers[path];
	if (w)
		return (w);
	w = new ZKWatcher(this, path);
	this.zs_watchers[path] = w;
	return (w);
};

function ZKWatcher(session, path) {
	this.zkw_path = path;
	this.zkw_session = session;
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
	if (!this.zkw_session.isInState('attached')) {
		function onStateChanged(st) {
			if (st === 'connected') {
				self.emit(evt);
				self.zkw_session.removeListener('stateChanged',
				    onStateChanged);
			}
		}
		this.zkw_session.zs_log.trace({
			event: evt,
			clientState: this.zkw_session.getState()
		}, 'deferring watcher refresh until after reconnect');
		this.zkw_session.on('stateChanged', onStateChanged);
		return;
	}
	var conn = this.zkw_session.getConnection();
	if (!conn.isInState('connected')) {
		setImmediate(function () {
			self.emit(evt);
		});
		return;
	}
	var qpkt = this._reqPacket(evt);
	var req = conn.request(qpkt);
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
			function onCreated() {
				self.removeListener('created', onCreated);
				self.emit(evt);
			}
			self.on('created', onCreated);
			return;
		}
		setImmediate(function () {
			self.emit(evt);
		});
		self.zkw_session.zs_log.trace(err, 'watcher attach failure; ' +
		    'will retry');
	});
};

ZKWatcher.prototype._reqPacket = function (evt) {
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
