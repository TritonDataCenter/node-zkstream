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
var connCount = 0;

mod_tape.test('connect failure: refused', function (t) {
	var zkc = new mod_zkc.Client({
		log: log,
		address: '127.0.0.1',
		port: 2181
	});

	zkc.on('connect', function (st) {
		t.fail();
	});

	zkc.on('failed', function () {
		zkc.close();
	});

	zkc.on('close', function () {
		t.end();
	});
});

mod_tape.test('start awful zk server', function (t) {
	zk = mod_net.createServer();
	zk.on('connection', function (sock) {
		++connCount;
		sock.destroy();
	});
	zk.listen(2181, function () {
		t.end();
	});
});

mod_tape.test('connect failure: immediate close', function (t) {
	var zkc = new mod_zkc.Client({
		log: log,
		address: '127.0.0.1',
		port: 2181
	});

	zkc.on('close', function () {
		t.end();
	});

	zkc.on('connect', function () {
		t.fail();
	});

	zkc.on('failed', function () {
		zkc.close();
	});
});

mod_tape.test('stop awful zk server', function (t) {
	zk.close();
	zk = undefined;
	t.end();
});

mod_tape.test('start zk server', function (t) {
	zk = new mod_zk.ZKServer();
	zk.on('stateChanged', function (st) {
		if (st === 'running')
			t.end();
	});
});

mod_tape.test('simple connect and ping', function (t) {
	var pinged = false;

	var zkc = new mod_zkc.Client({
		log: log,
		address: '127.0.0.1',
		port: 2181
	});

	zkc.on('close', function () {
		t.ok(pinged);
		t.end();
	});

	zkc.on('connect', function () {
		zkc.ping(function (err) {
			t.error(err);
			pinged = true;
			zkc.close();
		});
	});
});

mod_tape.test('double ping', function (t) {
	var pinged = 0;

	var zkc = new mod_zkc.Client({
		log: log,
		address: '127.0.0.1',
		port: 2181
	});

	zkc.on('close', function () {
		t.strictEqual(pinged, 2);
		t.end();
	});

	zkc.on('connect', function () {
		zkc.ping(function (err) {
			t.error(err);
			if (++pinged == 2)
				zkc.close();
		});
		zkc.ping(function (err) {
			t.error(err);
			if (++pinged == 2)
				zkc.close();
		});
	});
});

mod_tape.test('simple connect and ping, with death', function (t) {
	var stopped = false;
	var t1, t2;

	var zkc = new mod_zkc.Client({
		log: log,
		address: '127.0.0.1',
		port: 2181,
		sessionTimeout: 5000
	});

	zkc.on('close', function () {
		t.end();
	});

	zkc.on('expire', function () {
		t.ok(stopped);
		t2 = new Date();
		var delta = t2.getTime() - t1.getTime();
		t.ok(delta >= 5000);
		zkc.close();
	});

	zkc.on('connect', function () {
		zkc.ping(function (err) {
			t1 = new Date();
			t.error(err);
			stopped = true;
			zk.stop();
		});
	});
});

mod_tape.test('start zk server', function (t) {
	zk = new mod_zk.ZKServer();
	zk.on('stateChanged', function (st) {
		if (st === 'running')
			t.end();
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
		address: '127.0.0.1',
		port: 2181
	});

	zkc.on('close', function () {
		t.end();
	});

	zkc.on('connect', function () {
		zkc.list('/', function (err, kids, stat) {
			t.error(err);
			t.deepEqual(kids.sort(), ['foo', 'zookeeper']);

			zkc.get('/foo', function (err2, data) {
				t.error(err2);
				t.strictEqual(data.toString('ascii'), 'hi');
				zkc.close();
			});
		});
	});
});

mod_tape.test('get acl for the test object', function (t) {
	var zkc = new mod_zkc.Client({
		log: log,
		address: '127.0.0.1',
		port: 2181
	});

	zkc.on('close', function () {
		t.end();
	});

	zkc.on('connect', function () {
		zkc.getACL('/foo', function (err, acl) {
			t.error(err);
			t.ok(Array.isArray(acl));
			t.equal(acl.length, 1);
			t.strictEqual(acl[0].id.scheme, 'world');
			t.strictEqual(acl[0].id.id, 'anyone');
			t.deepEqual(acl[0].perms.sort(),
			    ['ADMIN', 'CREATE', 'DELETE', 'READ', 'WRITE']);
			zkc.close();
		});
	});
});

mod_tape.test('delete the test object', function (t) {
	var zkc = new mod_zkc.Client({
		log: log,
		address: '127.0.0.1',
		port: 2181
	});

	zkc.on('close', function () {
		t.end();
	});

	zkc.on('connect', function () {
		zkc.delete('/foo', 0, function (err) {
			t.error(err);
			zkc.close();
		});
	});
});

mod_tape.test('ask for a non-existent node', function (t) {
	var zkc = new mod_zkc.Client({
		log: log,
		address: '127.0.0.1',
		port: 2181
	});

	zkc.on('close', function () {
		t.end();
	});

	zkc.on('connect', function () {
		zkc.stat('/foo', function (err, stat) {
			t.ok(err);
			t.strictEqual(err.code, 'NO_NODE');
			zkc.close();
		});
	});
});

mod_tape.test('create a new node', function (t) {
	var zkc = new mod_zkc.Client({
		log: log,
		address: '127.0.0.1',
		port: 2181
	});

	zkc.on('close', function () {
		t.end();
	});

	zkc.on('connect', function () {
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

mod_tape.test('create a large node', function (t) {
	var zkc = new mod_zkc.Client({
		log: log,
		address: '127.0.0.1',
		port: 2181
	});

	zkc.on('close', function () {
		t.end();
	});

	zkc.on('connect', function () {
		var d = new Buffer(9000);
		d.fill(5);
		zkc.create('/bignode', d, {}, function (err, path) {
			t.error(err);
			t.strictEqual(path, '/bignode');
			zkc.get('/bignode', function (err2, output) {
				t.error(err2);
				t.strictEqual(output.length, 9000);
				t.strictEqual(output[5], 5);

				zkc.delete('/bignode', -1, function (err3) {
					t.error(err3);
					zkc.close();
				});
			});
		});
	});
});

mod_tape.test('data watcher', function (t) {
	var zkc = new mod_zkc.Client({
		log: log,
		address: '127.0.0.1',
		port: 2181
	});

	zkc.on('close', function () {
		t.end();
	});

	var data = new Buffer('hi there', 'ascii');
	var count = 0;

	zkc.on('connect', function () {
		zkc.watcher('/foo').on('dataChanged', function (newData) {
			t.ok(Buffer.isBuffer(newData));
			t.strictEqual(newData.toString('base64'),
			    data.toString('base64'));
			if (++count === 1) {
				data = new Buffer('hi', 'ascii');
				console.log('doing set');
				zk.cli('set', '/foo', 'hi', function (err) {
					t.error(err);
					t.strictEqual(count, 2);
					zkc.close();
				});
			}
		});
	});
});

mod_tape.test('delete it while watching', function (t) {
	var zkc = new mod_zkc.Client({
		log: log,
		address: '127.0.0.1',
		port: 2181
	});

	zkc.on('close', function () {
		t.end();
	});

	zkc.on('connect', function () {
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
		address: '127.0.0.1',
		port: 2181
	});

	zkc.on('close', function () {
		t.end();
	});

	zkc.on('connect', function () {
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
		address: '127.0.0.1',
		port: 2181
	});

	zkc.on('close', function () {
		t.end();
	});

	zkc.on('connect', function () {
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

mod_tape.test('children watcher no node', function (t) {
	var zkc = new mod_zkc.Client({
		log: log,
		address: '127.0.0.1',
		port: 2181
	});

	zkc.on('close', function () {
		t.end();
	});

	zkc.on('connect', function () {
		var noKids, allKids;
		var w = zkc.watcher('/parent');
		w.on('childrenChanged', function (kids, stat) {
			if (kids.length === 0) {
				noKids = stat.cversion;
			}
			if (kids.indexOf('foobar') !== -1 &&
			    kids.indexOf('foo') !== -1) {
				allKids = stat.cversion;
			}

			if (noKids !== undefined &&
			    allKids !== undefined) {
				t.ok(allKids > noKids);
				zkc.close();
			}
		});
		setTimeout(function () {
			zkc.create('/parent', new Buffer(0), {},
			    function (err) {
				t.error(err);
				setTimeout(createKids, 2000);
			});
		}, 2000);

		function createKids() {
			zkc.create('/parent/foo', new Buffer('hi'), {},
			    function (err2) {
				t.error(err2);
			});
			zkc.create('/parent/foobar', new Buffer('hi'), {},
			    function (err2) {
				t.error(err2);
			});
		}
	});
});

mod_tape.test('session resumption with watcher', function (t) {
	var connected = 0;
	var closed = 0;

	var zkc1 = new mod_zkc.Client({
		log: log,
		address: '127.0.0.1',
		port: 2181
	});

	var zkc2 = new mod_zkc.Client({
		log: log,
		address: '127.0.0.1',
		port: 2181
	});

	var ev1 = [];
	zkc1.on('connect', ev1.push.bind(ev1, 'connect'));
	zkc1.on('session', ev1.push.bind(ev1, 'session'));
	zkc1.on('expire', ev1.push.bind(ev1, 'expire'));
	zkc1.on('disconnect', ev1.push.bind(ev1, 'disconnect'));

	zkc1.on('close', function () {
		t.deepEqual(ev1,
		    ['session', 'connect', 'disconnect', 'connect']);
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

			var sock = zkc1.getSession().getConnection().zcf_socket;
			t.ok(sock.listeners('error').length > 0);
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

mod_tape.test('session resumption with watcher (ping timeout)', function (t) {
	var connected = 0;
	var closed = 0;

	var zkc1 = new mod_zkc.Client({
		log: log,
		address: '127.0.0.1',
		port: 2181
	});

	var zkc2 = new mod_zkc.Client({
		log: log,
		address: '127.0.0.1',
		port: 2181
	});

	var ev1 = [];
	zkc1.on('connect', ev1.push.bind(ev1, 'connect'));
	zkc1.on('session', ev1.push.bind(ev1, 'session'));
	zkc1.on('expire', ev1.push.bind(ev1, 'expire'));
	zkc1.on('disconnect', ev1.push.bind(ev1, 'disconnect'));

	zkc1.on('close', function () {
		t.deepEqual(ev1,
		    ['session', 'connect', 'disconnect', 'connect']);
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
		var ret = 0;
		var d = new Buffer('hi there', 'ascii');
		var w = zkc2.watcher('/foo3');
		function onCreated() {
			if (++ret == 2) {
				w.removeListener('created', onCreated);
				ready();
			}
		}
		w.on('created', onCreated);
		zkc1.watcher('/foo3').on('dataChanged', function (data) {
			if (data.toString('utf-8') === 'hello again') {
				zkc1.close();
			}
		});
		zkc1.create('/foo3', d, {}, function (err, path) {
			t.error(err);
			t.strictEqual(path, '/foo3');
			if (++ret == 2) {
				w.removeListener('created', onCreated);
				ready();
			}
		});
	}

	function ready() {
		zkc2.stat('/foo3', function (err, stat) {
			t.error(err);

			var sock = zkc1.getSession().getConnection().zcf_socket;
			t.ok(sock.listeners('error').length > 0);
			sock.destroy();

			var data = new Buffer('hello again');
			zkc2.set('/foo3', data, stat.version, function (err2) {
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
