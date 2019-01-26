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
const mod_zkbuf = require('../lib/zk-buffer');
const mod_zkstreams = require('../lib/zk-streams');
const mod_crypto = require('crypto');

var log = mod_bunyan.createLogger({
	name: 'zkstream-test',
	level: process.env.LOG_LEVEL || 'info'
});

var zk;
var pkt, pkt2;
var connCount = 0;
var connCb;

/*
 * Create a dummy TCP listener and close the listening socket after a short
 * timeout. Allow time for this change to be noticed by the node-zkstream
 * CueballConnectionSet. This should result in the corresponding
 * ConnectionSlotFSM being marked dead and a monitor mode ConnectionSlotFSM
 * being created. When the dummy TCP listener comes back online, this change
 * should be noticed by the ZKClient StaticIpResolver which will result in the
 * monitor mode connection and the connection marked dead to race to attach to a
 * session. This can result in one of the ConnectionSlotFSMs seeing the session
 * state 'attaching' or 'reattaching'. This test verifies that the client
 * doesn't crash in this case.
 */
mod_tape.test('attachAndSendCR race', function (t) {
	var WAIT_BEFORE_CLOSE = 10000;
	var WAIT_BEFORE_RELISTEN = 13000;
	var WAIT_BEFORE_END = 5000;

	var zk1 = mod_net.createServer();
	var zk2 = mod_net.createServer();
	var conns = [];

	zk1.listen(2181, function () {
		log.trace('zk server 1 - listening');
		setTimeout(function () {
			log.trace('zk server 1 - closing');
			zk1.close(function () {
				setTimeout(function () {
					zk1.listen(2181, function () {
						log.trace('zk server 1 ' +
						    '- listening');
						setTimeout(killTest,
						    WAIT_BEFORE_END);
					});
				}, WAIT_BEFORE_RELISTEN);
			});
		}, WAIT_BEFORE_CLOSE);
	});
	zk2.listen(2182, function () {
		log.trace('zk server 2 - listening');
	});

	zk1.on('connection', function (conn) {
		conns.push(conn);
	});
	zk2.on('connection', function (conn) {
		conns.push(conn);
	});

	var zkc = new mod_zkc.Client({
		log: log,
		servers: [ {
			address: '127.0.0.1',
			port: 2181
		}, {
			address: '127.0.0.1',
			port: 2182
		} ]
	});

	zkc.on('close', function () {
		t.end();
	});

	zkc.on('failed', function () {
		log.trace('received failed event');
	});

	function killTest() {
		zk1.close();
		zk2.close();
		zkc.close();
		conns.forEach(function (conn) {
			conn.destroy();
		});
	}
});

mod_tape.test('start awful zk server', function (t) {
	zk = mod_net.createServer();
	zk.on('connection', function (sock) {
		++connCount;
		sock.on('error', function (e) {
			log.error(e);
		});
		sock.write(pkt);
		if (pkt2) {
			setTimeout(function () {
				sock.write(pkt2);
			}, 100);
		}
	});
	zk.listen(2181, function () {
		t.end();
	});
});

mod_tape.test('connect failure: bad length (too big)', function (t) {
	pkt = new Buffer('4000', 'hex');
	pkt2 = new Buffer('4000', 'hex');

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

mod_tape.test('connect failure: bad length (zero)', function (t) {
	pkt = new Buffer('000000000102', 'hex');

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

mod_tape.test('connect failure: bad length (negative)', function (t) {
	pkt = new Buffer('fffffffe0102', 'hex');

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

mod_tape.test('argument assertions', function (t) {
	var zkc = new mod_zkc.Client({
		log: log,
		address: '127.0.0.1',
		port: 2181
	});

	zkc.on('connect', function (st) {
		t.fail();
	});

	zkc.on('close', function () {
		t.end();
	});

	t.throws(function () {
		zkc.list(5);
	});

	t.throws(function () {
		zkc.list('/foo');
	});

	zkc.close();
});

mod_tape.test('calling before ready (not connected)', function (t) {
	var zkc = new mod_zkc.Client({
		log: log,
		address: '127.0.0.1',
		port: 2181
	});

	zkc.on('connect', function (st) {
		t.fail();
	});

	zkc.on('close', function () {
		t.end();
	});

	zkc.list('/', function (err) {
		t.ok(err);
		t.strictEqual(err.code, 'CONNECTION_LOSS');
		zkc.close();
	});
});

mod_tape.test('start hanging zk server', function (t) {
	zk = mod_net.createServer();
	zk.on('connection', function (sock) {
		++connCount;
		sock.on('error', function (e) {
			log.error(e);
		});
		if (connCb)
			connCb();
	});
	zk.listen(2181, function () {
		t.end();
	});
});


mod_tape.test('calling before ready (handshaking)', function (t) {
	var zkc = new mod_zkc.Client({
		log: log,
		address: '127.0.0.1',
		port: 2181
	});

	zkc.on('connect', function (st) {
		t.fail();
	});

	zkc.on('close', function () {
		t.end();
	});

	connCb = function () {
		connCb = undefined;
		zkc.list('/', function (err) {
			t.ok(err);
			t.strictEqual(err.code, 'CONNECTION_LOSS');
			t.strictEqual(err.name, 'ZKNotConnectedError');
			zkc.close();
		});
	};
});

mod_tape.test('stop awful zk server', function (t) {
	zk.close(function () {
		zk = undefined;
		t.end();
	});
});

mod_tape.test('start fake zk server', function (t) {
	zk = mod_net.createServer();
	zk.on('connection', function (sock) {
		++connCount;
		var fsm = {};
		fsm.isInState = function (st) {
			return (st === 'handshaking');
		};
		fsm.zcf_xidMap = {};
		var enc = new mod_zkstreams.ZKEncodeStream({
			fsm: fsm,
			isServer: true
		});
		var dec = new mod_zkstreams.ZKDecodeStream({
			fsm: fsm,
			isServer: true
		});
		sock.pipe(dec);
		enc.pipe(sock);
		sock.on('error', function (e) {
			log.error(e);
		});

		dec.on('readable', function () {
			var d = dec.read();
			if (d === null)
				return;
			t.strictEqual(typeof (d), 'object');
			t.notStrictEqual(d, null);
			t.strictEqual(d.protocolVersion, 0);

			t.strictEqual(dec.read(), null);

			var r = {
				protocolVersion: 1,
				timeOut: d.timeOut,
				sessionId: mod_crypto.randomBytes(8),
				passwd: mod_crypto.randomBytes(8)
			};
			enc.write(r);
		});
	});
	zk.listen(2181, function () {
		t.end();
	});
});

mod_tape.test('handshake failure: bad version', function (t) {
	pkt = new Buffer('fffffffe0102', 'hex');

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

mod_tape.test('stop fake zk server', function (t) {
	zk.close(function () {
		zk = undefined;
		t.end();
	});
});
