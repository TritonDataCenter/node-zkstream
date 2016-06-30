/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
 */

const mod_tape = require('tape');

const mod_zkbuf = require('../lib/zk-buffer');
const mod_zkstreams = require('../lib/zk-streams');

/* An example packet capture of "zkCli ls /" */
var CAPTURE1 = [
	{ send: 'AAAALQAAAAAAAAAAAAAAAAAAdTAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAA' +
	    'AAAAAA==' },
	{ recv: 'AAAAJQAAAAAAAHUwAVWjqFbbAAAAAAAQh19uvwgo25o9B6hUkSvqKQA=' },
	{ send: 'AAAADgAAAAEAAAAIAAAAAS8A' },
	{ recv: 'AAAAKAAAAAEAAAAAAAAFFwAAAAAAAAACAAAACXpvb2tlZXBlcgAAAANmb28=' }
];
var DECODED1 = [
	{
		lastZxidSeen: new Buffer([0, 0, 0, 0, 0, 0, 0, 0]),
		passwd: new Buffer([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		    0, 0, 0, 0, 0]),
		protocolVersion: 0,
		sessionId: new Buffer([0, 0, 0, 0, 0, 0, 0, 0]),
		timeOut: 30000
	},
	{
		passwd: new Buffer('h19uvwgo25o9B6hUkSvqKQ==', 'base64'),
		protocolVersion: 0,
		sessionId: new Buffer('AVWjqFbbAAA=', 'base64'),
		timeOut: 30000
	},
	{
		xid: 1,
		opcode: 'GET_CHILDREN',
		path: '/',
		watch: false
	},
	{
		xid: 1,
		opcode: 'GET_CHILDREN',
		err: 'OK',
		zxid: new Buffer([0, 0, 0, 0, 0, 0, 5, 0x17]),
		children: ['zookeeper', 'foo']
	}
];

mod_tape.test('decode capture #1', function (t) {
	var xidMap = {};
	for (var i = 0; i < CAPTURE1.length; ++i) {
		var data = CAPTURE1[i].send || CAPTURE1[i].recv;
		var buf = new Buffer(data, 'base64');
		var zkb = new mod_zkbuf.ZKBuffer({ buffer: buf });
		var len = zkb.readInt();
		t.strictEqual(len, zkb.remainder().length);
		var pkt;
		if (i === 0) {
			pkt = zkb.readConnectRequest();
		} else if (i === 1) {
			pkt = zkb.readConnectResponse();
		} else if (CAPTURE1[i].send) {
			pkt = zkb.readRequest();
			xidMap[pkt.xid] = pkt.opcode;
		} else if (CAPTURE1[i].recv) {
			pkt = zkb.readResponse(xidMap);
		} else {
			t.fail('Invalid capture');
		}
		t.deepEqual(pkt, DECODED1[i]);
	}
	t.end();
});
