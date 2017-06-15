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
		host: 'localhost',
		port: zks['1'].clientPort
	});
	zkc.connect();

	zkc.on('stateChanged', function (st) {
		if (st === 'connected') {
			zkc.ping(function (err) {
				t.error(err);
				zkc.close();
			});
		} else if (st === 'closed') {
			t.end();
		}
	});
});

mod_tape.test('simple connect and ping #3', function (t) {
	var zkc = new mod_zkc.Client({
		log: log,
		host: 'localhost',
		port: zks['3'].clientPort
	});
	zkc.connect();

	zkc.on('stateChanged', function (st) {
		if (st === 'connected') {
			zkc.ping(function (err) {
				t.error(err);
				zkc.close();
			});
		} else if (st === 'closed') {
			t.end();
		}
	});
});

mod_tape.test('write visibility', function (t) {
	var connected = 0, closed = 0;

	var zkc1 = new mod_zkc.Client({
		log: log,
		host: 'localhost',
		port: zks['1'].clientPort
	});
	zkc1.connect();
	var zkc2 = new mod_zkc.Client({
		log: log,
		host: 'localhost',
		port: zks['2'].clientPort
	});
	zkc2.connect();

	zkc1.on('stateChanged', function (st) {
		if (st === 'closed' && ++closed >= 2)
			t.end();
		if (st !== 'connected')
			return;

		if (++connected == 2) {
			create();
		}
	});

	zkc2.on('stateChanged', function (st) {
		if (st === 'closed' && ++closed >= 2)
			t.end();
		if (st !== 'connected')
			return;

		if (++connected == 2) {
			create();
		}
	});

	function create() {
		var data = new Buffer('hello world');
		zkc1.create('/foo', data, {}, function (err, path) {
			t.error(err);
			t.strictEqual(path, '/foo');
			get();
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
		host: 'localhost',
		port: zks['1'].clientPort
	});
	zkc1.connect();
	var zkc2 = new mod_zkc.Client({
		log: log,
		host: 'localhost',
		port: zks['2'].clientPort
	});
	zkc2.connect();

	zkc1.on('stateChanged', function (st) {
		if (st === 'closed' && ++closed >= 2)
			t.end();
		if (st !== 'connected')
			return;

		if (++connected == 2) {
			setup();
		}
	});

	zkc2.on('stateChanged', function (st) {
		if (st === 'closed' && ++closed >= 2)
			t.end();
		if (st !== 'connected')
			return;

		if (++connected == 2) {
			setup();
		}
	});

	function setup() {
		var watchFired = false;
		zkc1.watcher('/foo').on('dataChanged',
		    function (newData, stat) {
			if (newData.toString() === 'testing') {
				t.ok(stat.version > 0);
				watchFired = true;
			}
		});
		var data = new Buffer('testing');
		zkc2.set('/foo', data, 0, function (err) {
			t.error(err);
			zkc2.sync('/foo', function (err2) {
				t.error(err2);
				t.ok(watchFired);
				zkc1.close();
				zkc2.close();
			});
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
