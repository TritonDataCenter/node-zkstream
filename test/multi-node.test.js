/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
 */

const mod_tape = require('tape');
const mod_zk = require('./zkserver');
const mod_zkc = require('../lib/index');
const mod_net = require('net');
const mod_bunyan = require('bunyan');
const mod_vasync = require('vasync');

var log = mod_bunyan.createLogger({
	name: 'zkstream-test',
	level: process.env.LOG_LEVEL || 'info'
});

var zk1, zk2, zk3;

var zks = {
	'1': {
		'clientPort': 2182,
		'quorumPort': 12182,
		'electionPort': 12192
	},
	'2': {
		'clientPort': 2183,
		'quorumPort': 12183,
		'electionPort': 12193
	},
	'3': {
		'clientPort': 2184,
		'quorumPort': 12184,
		'electionPort': 12194
	}
};

mod_tape.test('start zk servers', function (t) {
	var running = 0;
	zk1 = new mod_zk.ZKServer({
		servers: zks,
		serverId: '1'
	});
	zk2 = new mod_zk.ZKServer({
		servers: zks,
		serverId: '2'
	});
	zk3 = new mod_zk.ZKServer({
		servers: zks,
		serverId: '3'
	});
	zk1.on('stateChanged', function (st) {
		if (st === 'running' && ++running == 3)
			t.end();
	});
	zk2.on('stateChanged', function (st) {
		if (st === 'running' && ++running == 3)
			t.end();
	});
	zk3.on('stateChanged', function (st) {
		if (st === 'running' && ++running == 3)
			t.end();
	});
});

mod_tape.test('simple connect and ping #1', function (t) {
	var zkc = new mod_zkc.Client({
		log: log,
		address: '127.0.0.1',
		port: zks['1'].clientPort
	});

	zkc.on('close', function () {
		t.end();
	});

	zkc.on('connect', function () {
		zkc.ping(function (err) {
			t.error(err);
			zkc.close();
		});
	});
});

mod_tape.test('simple connect and ping #3', function (t) {
	var zkc = new mod_zkc.Client({
		log: log,
		address: '127.0.0.1',
		port: zks['3'].clientPort
	});

	zkc.on('close', function () {
		t.end();
	});

	zkc.on('connect', function () {
		zkc.ping(function (err) {
			t.error(err);
			zkc.close();
		});
	});
});

mod_tape.test('write visibility', function (t) {
	var connected = 0, closed = 0;

	var zkc1 = new mod_zkc.Client({
		log: log,
		address: '127.0.0.1',
		port: zks['1'].clientPort
	});
	var zkc2 = new mod_zkc.Client({
		log: log,
		address: '127.0.0.1',
		port: zks['2'].clientPort
	});

	zkc1.on('close', function () {
		if (++closed >= 2)
			t.end();
	});

	zkc2.on('close', function () {
		if (++closed >= 2)
			t.end();
	});

	zkc1.on('connect', function () {
		if (++connected == 2) {
			create();
		}
	});

	zkc2.on('connect', function () {
		if (++connected == 2) {
			create();
		}
	});

	function create() {
		var data = new Buffer('hello world');
		zkc1.create('/foo', data, {}, function (err, path) {
			t.error(err);
			t.strictEqual(path, '/foo');
			zkc1.sync('/foo', function (err2) {
				t.error(err2);
				get();
			});
		});
	}

	function get() {
		zkc2.get('/foo', function (err, data, stat) {
			t.error(err);
			t.strictEqual(data.toString(), 'hello world');
			t.strictEqual(stat.version, 0);

			zkc2.close();
			zkc1.close();
		});
	}
});

mod_tape.test('cross-server data watch', function (t) {
	var connected = 0, closed = 0;

	var zkc1 = new mod_zkc.Client({
		log: log,
		address: '127.0.0.1',
		port: zks['1'].clientPort
	});
	var zkc2 = new mod_zkc.Client({
		log: log,
		address: '127.0.0.1',
		port: zks['2'].clientPort
	});

	zkc1.on('close', function () {
		if (++closed >= 2)
			t.end();
	});

	zkc2.on('close', function () {
		if (++closed >= 2)
			t.end();
	});

	zkc1.on('connect', function () {
		if (++connected == 2) {
			setup();
		}
	});

	zkc2.on('connect', function () {
		if (++connected == 2) {
			setup();
		}
	});

	function setup() {
		mod_vasync.parallel({
			funcs: [watch1, write2]
		}, function (err) {
			t.error(err);
			zkc1.close();
			zkc2.close();
		});
		function watch1(cb) {
			zkc1.watcher('/foo').on('dataChanged',
			    function (newData, stat) {
				if (newData.toString() === 'testing') {
					t.ok(stat.version > 0);
					cb();
				}
			});
		}
		function write2(cb) {
			var data = new Buffer('testing');
			zkc2.set('/foo', data, 0, function (err) {
				t.error(err);
				zkc2.sync('/foo', function (err2) {
					t.error(err2);
					cb();
				});
			});
		}
	}
});

mod_tape.test('ephemeral node failover', function (t) {
	var closed = 0, connected = 0;
	var created = 0, deleted = 0;

	var zkc1 = new mod_zkc.Client({
		log: log,
		servers: Object.keys(zks).map(function (k) {
			return ({
			    address: '127.0.0.1',
			    port: zks[k].clientPort
			});
		}),
		sessionTimeout: 10000
	});

	/* Testing hack: force cueball to connect to backends in order */
	setImmediate(function () {
		zkc1.zc_set.cs_keys.sort();
		var k0 = zkc1.zc_set.cs_keys[0];
		var zkPort = zkc1.zc_set.cs_backends[k0].port;
		t.strictEqual(zkPort, zks['1'].clientPort);
		t.notStrictEqual(zkPort, zks['3'].clientPort);
	});

	var zkc2 = new mod_zkc.Client({
		log: log,
		address: '127.0.0.1',
		port: zks['3'].clientPort
	});

	zkc1.on('close', function () {
		if (++closed >= 2)
			t.end();
	});

	zkc2.on('close', function () {
		if (++closed >= 2)
			t.end();
	});

	zkc1.on('connect', function () {
		if (++connected == 2) {
			setup();
		}
	});

	zkc2.on('connect', function () {
		if (++connected == 2) {
			setup();
		}
	});

	function setup() {
		var data = new Buffer('hello world');
		var opts = { flags: ['EPHEMERAL'] };
		zkc2.watcher('/foo.ephem').on('created', function () {
			++created;
			zkc2.watcher('/foo.ephem').on('deleted', function () {
				++deleted;
			});
		});
		zkc1.create('/foo.ephem', data, opts, function (err, path) {
			t.error(err);
			t.strictEqual(path, '/foo.ephem');
			zkc1.sync('/foo.ephem', function (err2) {
				t.error(err2);
				zkc2.sync('/foo.ephem', function (err3) {
					t.error(err3);
					t.ok(created > 0, 'saw created');
					t.ok(deleted === 0, 'saw no deleted');
					kill();
				});
			});
		});
	}

	function kill() {
		zk1.on('stateChanged', function (st) {
			if (st === 'stopped') {
				setTimeout(check, 11000);
			}
		});
		zk1.stop();
	}

	function check() {
		t.ok(created > 0, 'saw created');
		t.ok(deleted === 0, 'saw no deleted');
		zkc2.get('/foo.ephem', function (err, data) {
			t.error(err);
			t.strictEqual(data.toString(), 'hello world');
			unkill();
		});
	}

	function unkill() {
		zk1 = new mod_zk.ZKServer({
			servers: zks,
			serverId: '1'
		});
		zk1.on('stateChanged', function (st) {
			if (st === 'running') {
				setTimeout(checkAgain, 10000);
			}
		});
	}

	function checkAgain() {
		t.ok(created > 0, 'saw created');
		t.ok(deleted === 0, 'saw no deleted');
		zkc1.get('/foo.ephem', function (err, data) {
			t.error(err);
			t.strictEqual(data.toString(), 'hello world');
			zkc1.close();
			zkc2.close();
		});
	}
});

mod_tape.test('stop zk servers', function (t) {
	var stopped = 0;
	zk1.on('stateChanged', function (st) {
		if (st === 'stopped' && ++stopped == 3)
			t.end();
	});
	zk2.on('stateChanged', function (st) {
		if (st === 'stopped' && ++stopped == 3)
			t.end();
	});
	zk3.on('stateChanged', function (st) {
		if (st === 'stopped' && ++stopped == 3)
			t.end();
	});
	zk1.stop();
	zk2.stop();
	zk3.stop();
});
