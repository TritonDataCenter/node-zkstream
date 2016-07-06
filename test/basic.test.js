/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
 */

const mod_tape = require('tape');
const mod_zk = require('./zkserver');
const mod_client = require('../lib/client-fsm');
const mod_net = require('net');

var zk;

mod_tape.test('start zk server', function (t) {
	zk = new mod_zk.ZKServer();
	zk.onState('running', function () {
		t.end();
	});
});

mod_tape.test('simple connect and ping', function (t) {
	var zkc = new mod_client.ClientFSM({});
	var sock = mod_net.connect({
		host: 'localhost',
		port: 2181
	});
	zkc.attach(sock);

	zkc.onState('connected', function () {
		zkc.ping(function (err) {
			t.error(err);
			sock.end();
			zkc.onState('closed', function () {
				t.end();
			});
		});
	});
});

mod_tape.test('set up test object', function (t) {
	zk.cli('create', '/foo', 'hi', 'world:anyone:cdrwa', function (err) {
		t.error(err);
		t.end();
	});
});

mod_tape.test('find the test object', function (t) {
	var zkc = new mod_client.ClientFSM({});
	var sock = mod_net.connect({
		host: 'localhost',
		port: 2181
	});
	zkc.attach(sock);

	zkc.onState('connected', function () {
		var req = zkc.list('/');
		req.once('reply', function (pkt) {
			t.strictEqual(pkt.opcode, 'GET_CHILDREN');
			t.deepEqual(pkt.children.sort(), ['foo', 'zookeeper']);

			var req2 = zkc.get('/foo');
			req2.once('reply', function (pkt2) {
				t.strictEqual(
				    pkt2.data.toString('ascii'), 'hi');
				sock.end();
				zkc.onState('closed', function () {
					t.end();
				});
			});
			req2.once('error', function (err) {
				t.error(err);
				t.end();
			});
		});
		req.once('error', function (err) {
			t.error(err);
			t.end();
		});
	});
});

mod_tape.test('delete the test object', function (t) {
	var zkc = new mod_client.ClientFSM({});
	var sock = mod_net.connect({
		host: 'localhost',
		port: 2181
	});
	zkc.attach(sock);

	zkc.onState('connected', function () {
		zkc.delete('/foo', 0, function (err) {
			t.error(err);
			sock.end();
			zkc.onState('closed', function () {
				t.end();
			});
		});
	});
});

mod_tape.test('ask for a non-existent node', function (t) {
	var zkc = new mod_client.ClientFSM({});
	var sock = mod_net.connect({
		host: 'localhost',
		port: 2181
	});
	zkc.attach(sock);

	zkc.onState('connected', function () {
		var req = zkc.stat('/foo');
		req.once('reply', function (pkt) {
			t.fail('Expected an error');
			sock.end();
			zkc.onState('closed', function () {
				t.end();
			});
		});
		req.once('error', function (err) {
			t.ok(err);
			t.strictEqual(err.code, 'NO_NODE');
			sock.end();
			zkc.onState('closed', function () {
				t.end();
			});
		});
	});
});

mod_tape.test('create a new node', function (t) {
	var zkc = new mod_client.ClientFSM({});
	var sock = mod_net.connect({
		host: 'localhost',
		port: 2181
	});
	zkc.attach(sock);

	zkc.onState('connected', function () {
		var d = new Buffer('hi there', 'ascii');
		zkc.create('/foo', d, {}, function (err, path) {
			t.error(err);
			t.strictEqual(path, '/foo');
			zk.cli('get', '/foo', function (err, output) {
				t.error(err);
				t.strictEqual(output, 'hi there\n');
				sock.end();
				zkc.onState('closed', function () {
					t.end();
				});
			});
		});
	});
});

mod_tape.test('data watcher', function (t) {
	var zkc = new mod_client.ClientFSM({});
	var sock = mod_net.connect({
		host: 'localhost',
		port: 2181
	});
	zkc.attach(sock);

	var data = new Buffer('hi there', 'ascii');
	var count = 0;

	zkc.onState('connected', function () {
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
			sock.end();
			zkc.onState('closed', function () {
				t.end();
			});
		})
	});
});

mod_tape.test('delete it while watching', function (t) {
	var zkc = new mod_client.ClientFSM({});
	var sock = mod_net.connect({
		host: 'localhost',
		port: 2181
	});
	zkc.attach(sock);

	zkc.onState('connected', function () {
		zkc.watcher('/foo').on('deleted', function () {
			sock.end();
			zkc.onState('closed', function () {
				t.end();
			});
		});
		zkc.stat('/foo', function (err, stat) {
			t.error(err);
			zkc.delete('/foo', stat.version, function (err2) {
				t.error(err2);
			});
		});
	});
});

mod_tape.test('stop zk server', function (t) {
	zk.onState('stopped', function () {
		t.end();
	});
	zk.stop();
});
