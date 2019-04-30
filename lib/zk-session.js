/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2018, Joyent, Inc.
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

var METRIC_ZK_NOTIFICATION_COUNTER = 'zookeeper_notifications';

function ZKSession(opts) {
	mod_assert.object(opts, 'options');
	mod_assert.number(opts.timeout, 'options.timeout');
	mod_assert.object(opts.log, 'options.log');
	mod_assert.object(opts.collector, 'options.collector');

	this.zs_conn = undefined;
	this.zs_oldConn = undefined;

	this.zs_lastPkt = undefined;
	this.zs_expiryTimer = new mod_events.EventEmitter();
	this.zs_watchers = {};
	this.zs_timeout = opts.timeout;
	this.zs_log = opts.log.child({
		component: 'ZKSession'
	});
	this.zs_collector = opts.collector;
	this.zs_lastAttach = 0;

	this.zs_lastZxid = new mod_jsbn.BigInteger('0');
	this.zs_sessionId = new mod_jsbn.BigInteger('0');
	this.zs_passwd = new Buffer(8).fill(0);

	/* Create a counter for tracking ZooKeeper notifications. */
	this.zs_collector.counter({
		name: METRIC_ZK_NOTIFICATION_COUNTER,
		help: 'Notifications received from ZooKeeper'
	});

	mod_fsm.FSM.call(this, 'detached');
}
mod_util.inherits(ZKSession, mod_fsm.FSM);

ZKSession.prototype.isAttaching = function () {
	if (this.isInState('attaching') || this.isInState('reattaching'))
		return (true);
	return (false);
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
	}, this.zs_timeout);
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
	this.watchersDisconnected();
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
	self.zs_lastAttach = Date.now();

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
			if (self.zs_oldConn !== undefined) {
				self.zs_oldConn.destroy();
				self.zs_oldConn = undefined;
			}
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
	var curSid = new Buffer(this.zs_sessionId.toByteArray());
	mod_assert.ok(this.zs_oldConn, 'reattaching requires oldConn');

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
		self.watchersDisconnected();
		S.gotoState('attached');
	});

	function revert() {
		if (self.isAlive() &&
		    self.zs_oldConn.isInState('connected')) {
			self.zs_log.warn('reverted move of session %s (on ' +
			    '(%s:%d) to new backend (%s:%d)',
			    curSid.toString('hex'),
			    self.zs_oldConn.zcf_server.address,
			    self.zs_oldConn.zcf_server.port,
			    self.zs_conn.zcf_server.address,
			    self.zs_conn.zcf_server.port);
			self.zs_conn = self.zs_oldConn;
			S.gotoState('attached');
		} else if (self.isAlive()) {
			self.zs_oldConn.destroy();
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

	this.zs_log.debug('attempting to move zookeeper session %s from ' +
	    '%s:%d to %s:%d', curSid.toString('hex'),
	    self.zs_oldConn.zcf_server.address, self.zs_oldConn.zcf_server.port,
	    self.zs_conn.zcf_server.address, self.zs_conn.zcf_server.port);

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

ZKSession.prototype.watchersDisconnected = function () {
	var paths = Object.keys(this.zs_watchers);
	var self = this;
	paths.forEach(function (path) {
		var w = self.zs_watchers[path];
		var evts = w.events();
		evts.forEach(function (event) {
			event.disconnected();
		});
	});
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

	var notifications = this.zs_collector.getCollector(
	    METRIC_ZK_NOTIFICATION_COUNTER);
	notifications.increment({event: evt});

	if (watcher)
		watcher.notify(evt);
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
	var allEvts = [];
	paths.forEach(function (path) {
		var w = self.zs_watchers[path];
		var evts = w.events();
		var cod = false;
		evts.forEach(function (event) {
			if (!event.isInState('resuming'))
				return;
			var evt = event.getEvent();
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
			allEvts.push(event);
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
			return;
		}
		allEvts.forEach(function (event) {
			event.resume();
		});
	});
};

ZKSession.prototype.watcher = function (path) {
	var w = this.zs_watchers[path];
	if (w)
		return (w);
	w = new ZKWatcher(this, path, this.zs_log);
	this.zs_watchers[path] = w;
	return (w);
};

/*
 * The ZKWatcher provides the EventEmitter that is returned by
 * ZKSession#watcher. It's not an FSM, but it spins up and interacts with
 * many ZKWatchEvent FSMs under its zkw_events lookup property.
 *
 * The available events emitted to end-users of the API are:
 *  - created
 *  - deleted
 *  - dataChanged
 *  - childrenChanged
 *
 * However, in the ZK protocol itself the created and deleted events are
 * collapsed into a single "existence" watcher, so we only spin up a single
 * FSM for both of those if either one of them or both are watched.
 *
 * The ZK server in many early versions we have to deal with only has two
 * real kinds of watches internally, though -- despite the protocol making it
 * seem like existence and data watches are separate concepts, it only has one
 * list which contains both. We try to be conservative and always re-arm our
 * watchers to make certain we have coverage on the event the user asked us
 * for, even if it potentially means duplicate notifications (which we filter
 * out by looking at the zxids).
 *
 * Older ZK versions:
 *
 *                 created     deleted     dataCh      childrenCh
 * GET_DATA           X           X           X
 * EXISTS             X           X           X
 * GET_CHILDREN2                  X                        X
 *
 * Newer ZK versions (>=3.5?):
 *
 *                 created     deleted     dataCh      childrenCh
 * GET_DATA                       X           X
 * EXISTS             X           X
 * GET_CHILDREN2                  X                        X
 *
 * We have to know about this, because when watchers fire they are removed
 * and we have to re-watch the node again. If we are trying to keep only a watch
 * on the existence of a node and suddenly get a dataChanged notification
 * about it (see the table above), we have to discard that event (we're not
 * watching for data changes), but we *must* proceed to set up the existence
 * watcher again, because from the server's perspective the watch has fired and
 * is finished.
 */
function ZKWatcher(session, path, log) {
	this.zkw_path = path;
	this.zkw_session = session;
	this.zkw_events = {};
	this.zkw_log = log.child({
	    component: 'ZKWatcher',
	    path: path
	});
	mod_events.EventEmitter.call(this);
}
mod_util.inherits(ZKWatcher, mod_events.EventEmitter);

/* Returns all our running watch FSMs. */
ZKWatcher.prototype.events = function () {
	var evts = [];
	var self = this;
	['createdOrDeleted', 'dataChanged', 'childrenChanged'].forEach(
	    function (evt) {
		if (self.zkw_events[evt])
			evts.push(self.zkw_events[evt]);
	});
	return (evts);
};

ZKWatcher.prototype.once = function () {
	throw (new Error('ZKWatcher does not support once() (use on)'));
};

/* Called by the ZKSession when a notification is received. */
ZKWatcher.prototype.notify = function (evt) {
	var toNotify = [];
	switch (evt) {
	case 'created':
		toNotify = ['createdOrDeleted', 'dataChanged'];
		break;
	case 'deleted':
		toNotify = ['createdOrDeleted', 'dataChanged',
		    'childrenChanged'];
		break;
	case 'dataChanged':
		toNotify = ['dataChanged', 'createdOrDeleted'];
		break;
	case 'childrenChanged':
		toNotify = ['childrenChanged'];
		break;
	default:
		throw (new Error('Unknown notification type: ' + evt));
	}
	var self = this;
	var notified = false;
	toNotify.forEach(function (type) {
		var event = self.zkw_events[type];
		if (event && !event.isInState('disarmed')) {
			event.notify();
			notified = true;
		}
	});
	if (!notified) {
		/*
		 * This is bad: it means our picture of which events can hit
		 * which kinds of watchers in ZK is wrong. We can't guarantee
		 * we can have a working watcher in this condition, so abort.
		 */
		throw (new Error('Got notification for ' + evt + ' but ' +
		    'have no matching events on ' + this.zkw_path));
	}
};

ZKWatcher.prototype.on = function (evt, cb) {
	mod_assert.string(evt, 'event');
	mod_assert.func(cb, 'callback');
	var first = (this.listeners(evt).length < 1);
	mod_events.EventEmitter.prototype.on.call(this, evt, cb);
	if (evt !== 'error' && first)
		this._armEvent(evt);
	return (this);
};

ZKWatcher.prototype._armEvent = function (evt) {
	if (evt === 'deleted' || evt === 'created')
		evt = 'createdOrDeleted';
	if (this.zkw_events[evt] === undefined) {
		this.zkw_events[evt] = new ZKWatchEvent(this.zkw_session,
		    this.zkw_path, this, evt, this.zkw_log);
	}
	if (this.zkw_events[evt].isInState('disarmed'))
		this.zkw_events[evt].arm();
};

/*
 * This FSM represents the state of an individual watch we're trying to
 * maintain with the ZK server.
 *
 * It lives as long as the session does, and operates in a loop re-arming
 * itself each time something happens that disarms the watcher on the server
 * side (e.g. the watcher firing a notification or losing our connection).
 *
 *        +----------+
 *        |          |
 *        | disarmed |
 *        |          |
 *        +--+-------+
 *           |
 *           |
 *           | arm() (from ZKWatcher)
 *           v
 *        +--+-----------+
 *        |              |
 *  +---->+ wait_session +<---------<----+--<---------+
 *  |     |              |                \           |
 *  |     +--+-----------+                 |          |
 *  |        |                             ^ node     |
 *  |        | session attached            | created  |
 *  |        v                             |          ^
 *  |     +--+-------------+         +-----+-----+    |
 *  |     |                |         |           |    |
 *  ^     | wait_connected |         | wait_node |    |
 *  |     |                |         |           |    |
 *  |     +--+-------------+         +-----+-----+    |
 *  |        |                             ^          |
 *  |        | connected                   |          |
 *  |        v                             |          |
 *  |     +--+-----+                       ^          ^
 *  |     |        |                       |          |
 *  +--<--+ arming +--->----------->-------+          |
 * error  |        |      NO_NODE && !(existence)     |
 *        +-+------+                                  |
 *          |                                         |
 *          | response from ZK  +---->  emit!         ^
 *          v                                         |
 *        +-+-------+                                 |
 *        |         | notify() (from ZKWatcher)      /|
 *        |  armed  +--->--------->----------->-----/ |
 *        |         +<------------<----------<----\   |
 *        +----+----+                             |   |
 *             |                         resume() |   ^ notify()
 *             |                 (from ZKSession) ^   | (from ZKSession)
 *             v disconnected()                   |   |
 *             | (from ZKSession)               +-+---+----+
 *             |                                |          |
 *             +--------->------------->------->+ resuming |
 *                                              |          |
 *                                              +----------+
 *
 */
function ZKWatchEvent(session, path, emitter, evt, log) {
	this.zkwe_path = path;
	this.zkwe_session = session;
	this.zkwe_emitter = emitter;
	this.zkwe_evt = evt;
	this.zkwe_prevZxid = null;
	this.zkwe_log = log.child({ event: evt });
	mod_fsm.FSM.call(this, 'disarmed');
}
mod_util.inherits(ZKWatchEvent, mod_fsm.FSM);

ZKWatchEvent.prototype.getEvent = function () {
	return (this.zkwe_evt);
};

/* Called by the watch emitter below when someone does .on('event', ...); */
ZKWatchEvent.prototype.arm = function () {
	this.emit('armAsserted');
};

/*
 * Called by the watch emitter when a notification is given to it by the
 * ZKSession which matches this event.
 *
 * Note that in e.g. wait_session we also use events emitted by the ZKSession
 * to sync up with it. We get a call here because receiving notifications
 * does not change the state of the ZKSession (unlike attaching to a new conn).
 */
ZKWatchEvent.prototype.notify = function () {
	if (this.isInState('armed') || this.isInState('resuming'))
		this.emit('notifyAsserted');
	/*
	 * In other states we ignore this: we're already in transition to
	 * set the watcher up (or re-arm it), so we don't need to be woken
	 * up.
	 */
};

/*
 * This is called by the ZKSession above to let us know that the session has
 * become detached and we are on the list to be auto-resumed (with catch-up)
 * after reconnection.
 *
 * We can't just use the state change of the ZKSession because we need to be
 * certain that we are actually on its list of watchers to re-arm after
 * reconnect -- instead we have it tell us explicitly through this function.
 */
ZKWatchEvent.prototype.disconnected = function () {
	if (this.isInState('armed'))
		this.emit('disconnectAsserted');
	/*
	 * In any other state, we do nothing. The ZKSession will only
	 * auto-resume watchers that were armed when we disconnected. Others
	 * will receive an error in their current state and retry.
	 */
};
/* And then the ZKSession lets us know when auto-resume is complete. */
ZKWatchEvent.prototype.resume = function () {
	if (this.isInState('resuming'))
		this.emit('resumeAsserted');
	/*
	 * We might have gotten a notification before resumption finished
	 * and thus went around to re-run our command. If we did, ignore
	 * the resume() call.
	 */
};

/*
 * We start here: the watch has not been established with the ZK server, and
 * we're not yet ready to start trying.
 *
 * This is the only state when we should never receive notifications.
 */
ZKWatchEvent.prototype.state_disarmed = function (S) {
	var self = this;
	S.on(this, 'armAsserted', function () {
		self.zkwe_log.trace('arming watcher');
		S.gotoState('wait_session');
	});
};

/*
 * We can't "arm" the watcher (send the command to the ZK server with the
 * watch flag set) until we know the session and connection are up and ready.
 *
 * So we go through wait_session and wait_connected first before arming.
 */
ZKWatchEvent.prototype.state_wait_session = function (S) {
	if (this.zkwe_session.isInState('attached')) {
		S.gotoState('wait_connected');
		return;
	}
	S.on(this.zkwe_session, 'stateChanged', function (state) {
		if (state === 'attached') {
			S.gotoState('wait_connected');
		}
	});
	this.zkwe_session.zs_log.trace({
		event: this.zkwe_evt,
		clientState: this.zkwe_session.getState()
	}, 'deferring watcher arm until after reconnect');
};

ZKWatchEvent.prototype.state_wait_connected = function (S) {
	var conn = this.zkwe_session.getConnection();
	if (!conn.isInState('connected')) {
		/*
		 * Don't transition synchronously back to wait_session, we
		 * want to give the connection a chance to get to the right
		 * state (it might have only just connected in this turn of
		 * the event loop and we saw the session change before it)
		 */
		S.immediate(function () {
			S.gotoState('wait_session');
		});
		return;
	}
	S.gotoState('arming');
};

/*
 * In this state we send the actual command with the watch flag set, then
 * double-check that the zxid we're looking at has actually changed, emit
 * events on the emitter, and transition to "armed".
 *
 * We also handle some errors that can occur (not all of which mean we failed
 * to arm the watcher, for various ZK protocol reasons described below).
 */
ZKWatchEvent.prototype.state_arming = function (S) {
	/*
	 * We can rely on the fact that wait_connected checked for the
	 * connection's state before synchronously transitioning to us.
	 *
	 * So calling conn.request() won't throw.
	 */
	var conn = this.zkwe_session.getConnection();
	var qpkt = this.toPacket();

	var req = conn.request(qpkt);
	var evt = this.zkwe_evt;
	var self = this;
	/*
	 * In ZK to "arm" a watcher on a node and receive a notification when
	 * it changes, you have to do an initial request for the same data you
	 * want to watch.
	 *
	 * Once you get a valid reply to it (or certain kinds of errors), you
	 * can consider the watcher "armed".
	 */
	S.on(req, 'reply', function (pkt) {
		var args = [evt];
		var zxid;
		switch (evt) {
		case 'createdOrDeleted':
			/* EXISTS returned ok, so the node exists. */
			args[0] = 'created';
			zxid = pkt.stat.czxid;
			args.push(pkt.stat);
			break;
		case 'dataChanged':
			zxid = pkt.stat.mzxid;
			args.push(pkt.data, pkt.stat);
			break;
		case 'childrenChanged':
			zxid = pkt.stat.pzxid;
			args.push(pkt.children, pkt.stat);
			break;
		default:
			throw (new Error('Unknown watcher event ' + evt));
		}
		self.zkwe_log.trace({ zxid: zxid.toString('hex'),
		    prevZxid: self.zkwe_prevZxid ?
		    self.zkwe_prevZxid.toString('hex') : null },
		    'got reply to arm request');
		if (self.zkwe_prevZxid && zxid.equals(self.zkwe_prevZxid)) {
			S.gotoState('armed');
			return;
		}
		mod_events.EventEmitter.prototype.emit.apply(self.zkwe_emitter,
		    args);
		self.zkwe_prevZxid = zxid;
		S.gotoState('armed');
	});
	S.on(req, 'error', function (err) {
		/* We lost our connection, so try again. */
		if (err.code === 'PING_TIMEOUT') {
			S.gotoState('wait_session');
			return;
		}
		/*
		 * Creation/destruction watchers are armed even if we get a
		 * NO_NODE error (this means it doesn't currently exist and
		 * we're now watching it for existence).
		 */
		if (evt === 'createdOrDeleted' && err.code === 'NO_NODE') {
			mod_events.EventEmitter.prototype.emit.call(
			    self.zkwe_emitter, 'deleted');
			S.gotoState('armed');
			return;
		}
		/*
		 * Other types of watchers won't attach to a node that
		 * doesn't exist yet -- we'll have to try arming this watcher
		 * again once the node exists.
		 */
		if (err.code === 'NO_NODE') {
			S.gotoState('wait_node');
			return;
		}
		self.zkwe_log.trace(err, 'watcher attach failure; ' +
		    'will retry');
		S.gotoState('wait_session');
	});
};

ZKWatchEvent.prototype.state_wait_node = function (S) {
	S.on(this.zkwe_emitter, 'created', function () {
		S.gotoState('wait_session');
	});
};

/*
 * In the armed state, we have established our watcher with the ZK server and
 * are waiting either for a notification to come in (in which case we re-fetch
 * the node information and then emit the event), or a disconnect/detach.
 */
ZKWatchEvent.prototype.state_armed = function (S) {
	S.on(this, 'notifyAsserted', function () {
		S.gotoState('wait_session');
	});
	S.on(this, 'disconnectAsserted', function () {
		S.gotoState('resuming');
	});
};

/*
 * ZKSession told us it detached due to disconnection and we are going to be
 * auto-resumed. Sit tight until it tells us what to do next (or we get a
 * catch-up notification).
 */
ZKWatchEvent.prototype.state_resuming = function (S) {
	S.on(this, 'resumeAsserted', function () {
		S.gotoState('armed');
	});
	S.on(this, 'notifyAsserted', function () {
		S.gotoState('wait_session');
	});
};

ZKWatchEvent.prototype.toPacket = function () {
	var pkt = {
		path: this.zkwe_path,
		watch: true
	};
	switch (this.zkwe_evt) {
	case 'createdOrDeleted':
		pkt.opcode = 'EXISTS';
		break;
	case 'dataChanged':
		pkt.opcode = 'GET_DATA';
		break;
	case 'childrenChanged':
		pkt.opcode = 'GET_CHILDREN2';
		break;
	default:
		throw (new Error('Unknown watcher event ' + this.zkwe_evt));
	}
	return (pkt);
};
