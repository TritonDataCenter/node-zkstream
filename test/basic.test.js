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

var zk;

mod_tape.test('start zk server', function (t) {
	zk = new mod_zk.ZKServer();
	zk.on('stateChanged', function (st) {
		if (st === 'running')
			t.end();
	});
});

mod_tape.test('simple connect and ping', function (t) {
	var zkc = new mod_zkc.Client({
		log: log,
		host: 'localhost',
		port: 2181
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

mod_tape.test('set up test object', function (t) {
	zk.cli('create', '/foo', 'hi', 'world:anyone:cdrwa', function (err) {
		t.error(err);
		t.end();
	});
});

mod_tape.test('find the test object', function (t) {
	var zkc = new mod_zkc.Client({
		log: log,
		host: 'localhost',
		port: 2181
	});
	zkc.connect();

	zkc.on('stateChanged', function (st) {
		if (st === 'closed')
			t.end();
		if (st !== 'connected')
			return;

		var req = zkc.list('/');
		req.once('reply', function (pkt) {
			t.strictEqual(pkt.opcode, 'GET_CHILDREN');
			t.deepEqual(pkt.children.sort(), ['foo', 'zookeeper']);

			var req2 = zkc.get('/foo');
			req2.once('reply', function (pkt2) {
				t.strictEqual(
				    pkt2.data.toString('ascii'), 'hi');
				zkc.close();
			});
			req2.once('error', function (err) {
				t.error(err);
				zkc.close();
			});
		});
		req.once('error', function (err) {
			t.error(err);
			zkc.close();
		});
	});
});

mod_tape.test('delete the test object', function (t) {
	var zkc = new mod_zkc.Client({
		log: log,
		host: 'localhost',
		port: 2181
	});
	zkc.connect();

	zkc.on('stateChanged', function (st) {
		if (st === 'closed')
			t.end();
		if (st !== 'connected')
			return;
		zkc.delete('/foo', 0, function (err) {
			t.error(err);
			zkc.close();
		});
	});
});

mod_tape.test('ask for a non-existent node', function (t) {
	var zkc = new mod_zkc.Client({
		log: log,
		host: 'localhost',
		port: 2181
	});
	zkc.connect();

	zkc.on('stateChanged', function (st) {
		if (st === 'closed')
			t.end();
		if (st !== 'connected')
			return;

		var req = zkc.stat('/foo');
		req.once('reply', function (pkt) {
			t.fail('Expected an error');
			zkc.close();
		});
		req.once('error', function (err) {
			t.ok(err);
			t.strictEqual(err.code, 'NO_NODE');
			zkc.close();
		});
	});
});

mod_tape.test('create a new node', function (t) {
	var zkc = new mod_zkc.Client({
		log: log,
		host: 'localhost',
		port: 2181
	});
	zkc.connect();

	zkc.on('stateChanged', function (st) {
		if (st === 'closed')
			t.end();
		if (st !== 'connected')
			return;

		var d = new Buffer('hi there', 'ascii');
		zkc.create('/foo', d, {}, function (err, path) {
			t.error(err);
			t.strictEqual(path, '/foo');
			zk.cli('get', '/foo', function (err2, output) {
				t.error(err2);
				t.strictEqual(output, 'hi there\n');
				zkc.close();
			});
		});
	});
});

mod_tape.test('data watcher', function (t) {
	var zkc = new mod_zkc.Client({
		log: log,
		host: 'localhost',
		port: 2181
	});
	zkc.connect();

	var data = new Buffer('hi there', 'ascii');
	var count = 0;

	zkc.on('stateChanged', function (st) {
		if (st === 'closed')
			t.end();
		if (st !== 'connected')
			return;
		zkc.watcher('/foo').on('dataChanged', function (newData) {
			t.ok(Buffer.isBuffer(newData));
			t.strictEqual(newData.toString('base64'),
			    data.toString('base64'));
			if (++count === 1) {
				data = new Buffer('hi', 'ascii');
			}
		});
		zk.cli('set', '/foo', 'hi', function (err) {
			t.error(err);
			t.strictEqual(count, 2);
			zkc.close();
		});
	});
});

mod_tape.test('delete it while watching', function (t) {
	var zkc = new mod_zkc.Client({
		log: log,
		host: 'localhost',
		port: 2181
	});
	zkc.connect();

	zkc.on('stateChanged', function (st) {
		if (st === 'closed')
			t.end();
		if (st !== 'connected')
			return;

		zkc.watcher('/foo').on('deleted', function () {
			zkc.close();
		});
		zkc.stat('/foo', function (err, stat) {
			t.error(err);
			zkc.delete('/foo', stat.version, function (err2) {
				t.error(err2);
			});
		});
	});
});

mod_tape.test('set up test object', function (t) {
	zk.cli('create', '/foobar', 'hi', 'world:anyone:cdrwa', function (err) {
		t.error(err);
		t.end();
	});
});

mod_tape.test('delete it while watching data', function (t) {
	var zkc = new mod_zkc.Client({
		log: log,
		host: 'localhost',
		port: 2181
	});
	zkc.connect();

	zkc.on('stateChanged', function (st) {
		if (st === 'closed')
			t.end();
		if (st !== 'connected')
			return;

		var dwFired = 0;
		var w = zkc.watcher('/foobar');
		w.on('dataChanged', function (data, stat) {
			++dwFired;
		});
		w.on('deleted', function () {
			t.equal(dwFired, 1);
			zkc.close();
		});
		zkc.stat('/foobar', function (err, stat) {
			t.error(err);
			zkc.delete('/foobar', stat.version, function (err2) {
				t.error(err2);
			});
		});
	});
});

mod_tape.test('set up test object', function (t) {
	zk.cli('create', '/foobar', 'hi', 'world:anyone:cdrwa', function (err) {
		t.error(err);
		t.end();
	});
});

mod_tape.test('children watcher', function (t) {
	var zkc = new mod_zkc.Client({
		log: log,
		host: 'localhost',
		port: 2181
	});
	zkc.connect();

	zkc.on('stateChanged', function (st) {
		if (st === 'closed')
			t.end();
		if (st !== 'connected')
			return;

		var sawFoobar, sawFoo, sawNone;
		var w = zkc.watcher('/');
		w.on('childrenChanged', function (kids, stat) {
			if (kids.indexOf('foobar') !== -1)
				sawFoobar = stat.cversion;
			if (kids.indexOf('foo') !== -1)
				sawFoo = stat.cversion;
			if (kids.length === 1 && kids[0] === 'zookeeper')
				sawNone = stat.cversion;

			if (sawFoobar !== undefined &&
			    sawFoo !== undefined &&
			    sawNone !== undefined) {
				t.ok(sawFoo > sawFoobar);
				t.ok(sawNone > sawFoo);
				zkc.close();
			}
		});
		zkc.stat('/foobar', function (err, stat) {
			t.error(err);
			zkc.delete('/foobar', stat.version, function (err2) {
				t.error(err2);
			});
		});
		zkc.create('/foo', new Buffer('hi'), {}, function (err) {
			t.error(err);
			zkc.delete('/foo', -1, function (err2) {
				t.error(err2);
			});
		});
	});
});

mod_tape.test('session resumption with watcher', function (t) {
	var connected = 0;
	var closed = 0;

	var zkc1 = new mod_zkc.Client({
		log: log,
		host: 'localhost',
		port: 2181
	});
	zkc1.connect();

	var zkc2 = new mod_zkc.Client({
		log: log,
		host: 'localhost',
		port: 2181
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
		var ret = 0;
		var d = new Buffer('hi there', 'ascii');
		var w = zkc2.watcher('/foo');
		function onCreated() {
			if (++ret == 2) {
				w.removeListener('created', onCreated);
				ready();
			}
		}
		w.on('created', onCreated);
		zkc1.watcher('/foo').on('dataChanged', function (data) {
			if (data.toString('utf-8') === 'hello again') {
				zkc1.close();
			}
		});
		zkc1.create('/foo', d, {}, function (err, path) {
			t.error(err);
			t.strictEqual(path, '/foo');
			if (++ret == 2) {
				w.removeListener('created', onCreated);
				ready();
			}
		});
	}

	function ready() {
		zkc2.stat('/foo', function (err, stat) {
			t.error(err);

			var sock = zkc1.zs_socket;
			sock.emit('error', new Error('I killed it'));
			sock.destroy();

			var data = new Buffer('hello again');
			zkc2.set('/foo', data, stat.version, function (err2) {
				t.error(err2);
				zkc2.close();
			});
		});
	}
});

mod_tape.test('stop zk server', function (t) {
	zk.on('stateChanged', function (st) {
		if (st === 'stopped')
			t.end();
	});
	zk.stop();
});
