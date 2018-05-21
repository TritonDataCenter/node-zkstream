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
const mod_artedi = require('artedi');
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
const mod_cfsm = require('./connection-fsm');

function ZKClient(opts) {
	mod_assert.object(opts, 'options');

	mod_assert.optionalObject(opts.log, 'options.log');
	if (opts.log === undefined || opts.log === null) {
		this.zc_log = mod_bunyan.createLogger({
			name: 'zkstream',
			component: 'ZKClient'
		});
	} else {
		this.zc_log = opts.log.child({
			component: 'ZKClient'
		});
	}

	/*
	 * You have the right to a ZooKeeper notifications counter.  If you can
	 * not afford one, one will be appointed to you by ZKClient.
	 */
	mod_assert.optionalObject(opts.zk_notification_counter);
	if (opts.zk_notification_counter === undefined ||
	    opts.zk_notification_counter === null) {
		this.zc_collector = mod_artedi.createCollector();
		this.zc_collector.counter({
			name: 'zookeeper_notifications',
			help: 'Notifications received from ZooKeeper'
		});
		this.zc_notification_counter = this.collector.getCollector(
		    'zookeeper_notifications');
	} else {
		this.zc_notification_counter = opts.zk_notification_counter;
	}

	mod_assert.optionalArrayOfObject(opts.servers, 'options.servers');
	if (opts.servers === undefined || opts.servers === null) {
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
	if (this.zc_sessionTimeout === undefined ||
	    this.zc_sessionTimeout === null) {
		this.zc_sessionTimeout = 30000;
	}

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
		log: this.zc_log,
		zk_notification_counter: this.zc_notification_counter
	});
	this.zc_session = s;
	function initialHandler(st) {
		if (st === 'attached') {
			s.removeListener('stateChanged', initialHandler);
			s.on('stateChanged', finalHandler);
			self._emitAfterConnected('session');
			self._emitAfterConnected('connect');
		}
	}
	function finalHandler(st) {
		if (st === 'attached') {
			self._emitAfterConnected('connect');
		} else if (st === 'detached') {
			self.emit('disconnect');
		} else if (st === 'expired') {
			self.emit('expire');
		}
	}
	this.zc_session.on('stateChanged', initialHandler);
};

ZKClient.prototype.isConnected = function () {
	var conn = this.getConnection();
	return (conn !== undefined && conn.isInState('connected'));
};

ZKClient.prototype._emitAfterConnected = function (evt) {
	/*
	 * We don't want to emit 'connect' until someone can safely call
	 * .list() or similar functions on us.
	 *
	 * So we have to sync up with the ConnectionFSM here.
	 */
	var self = this;
	var c = this.currentConnection();
	if (c.isInState('connected')) {
		setImmediate(function () {
			self.emit(evt);
		});
	} else {
		c.on('stateChanged', onConnCh);
		function onConnCh(cst) {
			if (cst === 'connected') {
				c.removeListener(
				    'stateChanged', onConnCh);
				self.emit(evt);
			}
		}
	}
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
	var c = new mod_cfsm.ZKConnectionFSM({
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
	mod_assert.func(cb, 'callback');
	var conn = this.currentConnection();
	if (conn === undefined || !conn.isInState('connected')) {
		setImmediate(cb, new mod_errors.ZKNotConnectedError());
		return;
	}
	var req = conn.request({
		opcode: 'GET_CHILDREN2',
		path: path,
		watch: false
	});
	req.once('reply', function onReply(pkt) {
		cb(null, pkt.children, pkt.stat);
	});
	req.once('error', cb);
};

ZKClient.prototype.get = function (path, cb) {
	mod_assert.string(path, 'path');
	mod_assert.func(cb, 'callback');
	var conn = this.currentConnection();
	if (conn === undefined || !conn.isInState('connected')) {
		setImmediate(cb, new mod_errors.ZKNotConnectedError());
		return;
	}
	var req = conn.request({
		opcode: 'GET_DATA',
		path: path,
		watch: false
	});
	req.once('reply', function onReply(pkt) {
		cb(null, pkt.data, pkt.stat);
	});
	req.once('error', cb);
};

ZKClient.prototype.create = function (path, data, options, cb) {
	mod_assert.string(path, 'path');
	mod_assert.buffer(data, 'data');
	mod_assert.optionalObject(options, 'options');
	mod_assert.func(cb, 'callback');
	if (options === undefined || options === null)
		options = {};
	mod_assert.optionalArrayOfObject(options.acl, 'options.acl');
	mod_assert.optionalArrayOfString(options.flags, 'options.flags');

	if (options.acl === undefined || options.acl === null) {
		options.acl = [ {
		    id: { scheme: 'world', id: 'anyone' },
		    perms: ['read', 'write', 'create', 'delete', 'admin']
		} ];
	}

	if (options.flags === undefined || options.flags === null) {
		options.flags = [];
	}

	var conn = this.currentConnection();
	if (conn === undefined || !conn.isInState('connected')) {
		setImmediate(cb, new mod_errors.ZKNotConnectedError());
		return;
	}
	var req = conn.request({
		opcode: 'CREATE',
		path: path,
		data: data,
		acl: options.acl,
		flags: options.flags
	});
	req.once('reply', function onReply(pkt) {
		cb(null, pkt.path);
	});
	req.once('error', cb);
};

ZKClient.prototype.createWithEmptyParents = function (path, data, options, cb) {
	mod_assert.string(path, 'path');
	mod_assert.buffer(data, 'data');
	mod_assert.optionalObject(options, 'options');
	mod_assert.func(cb, 'callback');
	if (options === undefined || options === null)
		options = {};
	mod_assert.optionalArrayOfObject(options.acl, 'options.acl');
	mod_assert.optionalArrayOfString(options.flags, 'options.flags');

	if (options.acl === undefined || options.acl === null) {
		options.acl = [ {
			id: { scheme: 'world', id: 'anyone' },
			perms: ['read', 'write', 'create', 'delete', 'admin']
		} ];
	}

	if (options.flags === undefined || options.flags === null) {
		options.flags = [];
	}

	var conn = this.currentConnection();
	if (conn === undefined || !conn.isInState('connected')) {
		setImmediate(cb, new mod_errors.ZKNotConnectedError());
		return;
	}

	var currentPath = '';
	var nodes = path.split('/').slice(1);
	var nullBuffer = new Buffer('null', 'ascii');
	var count = 0;

	var self = this;
	mod_vasync.forEachPipeline({
		func: function (node, next) {
			currentPath = currentPath + '/' + node;
			count++;
			var last = (count === nodes.length);
			var nodeData = (last) ? data : nullBuffer;

			/*
			 * All newly created parent nodes are ordinary
			 * persistent nodes. The options are only applied to
			 * the final node.
			 */
			var opts = (last) ? options : {};

			self.create(currentPath, nodeData, opts,
			    function (err, pktPath) {
				if (err && (last ||
				    err.code !== 'NODE_EXISTS')) {
					next(err);
					return;
				}
				next(null, pktPath);
			});
		},
		inputs: nodes
	}, function (err, results) {
		if (err) {
			cb(err);
		} else {
			/*
			 * Last entry contains the absolute path of the final
			 * created node in the happy path.
			 */
			cb(null, results.successes[results.successes.length-1]);
		}
	});
};

ZKClient.prototype.set = function (path, data, version, cb) {
	mod_assert.string(path, 'path');
	mod_assert.buffer(data, 'data');
	mod_assert.optionalNumber(version, 'version');
	mod_assert.func(cb, 'callback');

	if (version === undefined || version === null)
		version = -1;

	var conn = this.currentConnection();
	if (conn === undefined || !conn.isInState('connected')) {
		setImmediate(cb, new mod_errors.ZKNotConnectedError());
		return;
	}
	var req = conn.request({
		opcode: 'SET_DATA',
		path: path,
		data: data,
		version: version
	});
	req.once('reply', function onReply(pkt) {
		cb(null, pkt.path);
	});
	req.once('error', cb);
};

ZKClient.prototype.delete = function (path, version, cb) {
	mod_assert.string(path, 'path');
	mod_assert.number(version, 'version');
	mod_assert.func(cb, 'callback');

	var conn = this.currentConnection();
	if (conn === undefined || !conn.isInState('connected')) {
		setImmediate(cb, new mod_errors.ZKNotConnectedError());
		return;
	}
	var req = conn.request({
		opcode: 'DELETE',
		path: path,
		version: version
	});
	req.once('reply', function onReply(pkt) {
		cb(null);
	});
	req.once('error', cb);
};

ZKClient.prototype.stat = function (path, cb) {
	mod_assert.string(path, 'path');
	mod_assert.func(cb, 'callback');

	var conn = this.currentConnection();
	if (conn === undefined || !conn.isInState('connected')) {
		setImmediate(cb, new mod_errors.ZKNotConnectedError());
		return;
	}
	var req = conn.request({
		opcode: 'EXISTS',
		path: path,
		watch: false
	});
	req.once('reply', function onReply(pkt) {
		cb(null, pkt.stat);
	});
	req.once('error', cb);
};

ZKClient.prototype.getACL = function (path, cb) {
	mod_assert.string(path, 'path');
	mod_assert.func(cb, 'callback');

	var conn = this.currentConnection();
	if (conn === undefined || !conn.isInState('connected')) {
		setImmediate(cb, new mod_errors.ZKNotConnectedError());
		return;
	}
	var req = conn.request({
		opcode: 'GET_ACL',
		path: path,
		watch: false
	});
	req.once('reply', function onReply(pkt) {
		cb(null, pkt.acl);
	});
	req.once('error', cb);
};

ZKClient.prototype.sync = function (path, cb) {
	mod_assert.string(path, 'path');
	mod_assert.func(cb, 'callback');

	var conn = this.currentConnection();
	if (conn === undefined || !conn.isInState('connected')) {
		setImmediate(cb, new mod_errors.ZKNotConnectedError());
		return;
	}
	var req = conn.request({
		opcode: 'SYNC',
		path: path
	});
	req.once('reply', function onReply(pkt) {
		cb(null);
	});
	req.once('error', cb);
};

ZKClient.prototype.watcher = function (path) {
	return (this.getSession().watcher(path));
};
