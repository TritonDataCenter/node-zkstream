/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
 */

module.exports = {
	ZKDecodeStream: ZKDecodeStream,
	ZKEncodeStream: ZKEncodeStream
};

const mod_assert = require('assert-plus');
const ZKBuffer = require('./zk-buffer').ZKBuffer;
const mod_consts = require('./zk-consts');
const mod_util = require('util');
const mod_stream = require('stream');
const mod_errors = require('./errors');
const mod_jsbn = require('jsbn');
const mod_events = require('events');

const MAX_PACKET = 8*1024*1024;

function ZKDecodeStream(opts) {
	mod_assert.object(opts, 'options');
	mod_assert.object(opts.fsm, 'options.fsm');
	this.zd_fsm = opts.fsm;
	this.zd_used = 0;
	this.zd_buffer = new Buffer(1024);
	opts.readableObjectMode = true;
	opts.writableObjectMode = false;
	mod_stream.Transform.call(this, opts);
}
mod_util.inherits(ZKDecodeStream, mod_stream.Transform);

ZKDecodeStream.prototype._transform = function (chunk, enc, cb) {
	var self = this;
	var pkt;
	mod_assert.buffer(chunk);
	while (this.zd_buffer.length < this.zd_used + chunk.length)
		this.expandBuffer();
	chunk.copy(this.zd_buffer, this.zd_used);
	this.zd_used += chunk.length;
	while (this.zd_used > 4) {
		var len = this.zd_buffer.readInt32BE(0);
		if (len < 0 || len > MAX_PACKET) {
			this.emit('error', new mod_errors.ZKProtocolError(
			    'BAD_LENGTH', 'Invalid ZK packet length'));
			return;
		}
		if (this.zd_used >= 4 + len) {
			pkt = new Buffer(len);
			this.zd_buffer.copy(pkt, 0, 4, 4 + len);
			this.zd_buffer.copy(this.zd_buffer,
			    0, 4 + len, this.zd_used);
			this.zd_used -= (4 + len);
			handlePacket(pkt);
		} else {
			break;
		}
	}
	cb();
	function handlePacket(pktBuf) {
		var zkb = new ZKBuffer({buffer: pktBuf});
		if (self.zd_fsm.getState() === 'connecting') {
			try {
				pkt = zkb.readConnectResponse();
			} catch (e) {
				self.emit('error',
				    new mod_errors.ZKProtocolError('BAD_DECODE',
				    'Failed to decode ConnectResponse: ' +
				    e.name + ': ' + e.message));
				return;
			}
			self.push(pkt);
		} else {
			try {
				pkt = zkb.readResponse(self.zd_fsm.zs_xidMap);
			} catch (e) {
				self.emit('error',
				    new mod_errors.ZKProtocolError('BAD_DECODE',
				    'Failed to decode Response: ' +
				    e.name + ': ' + e.message));
				return;
			}
			console.log(" < %j", pkt);
			self.push(pkt);
		}
	}
};

ZKDecodeStream.prototype.expandBuffer = function () {
	var newBuf = new Buffer(this.zd_buffer.size * 2);
	this.zd_buffer.copy(newBuf);
	this.zd_buffer = newBuf;
};


function ZKEncodeStream(opts) {
	mod_assert.object(opts, 'options');
	mod_assert.object(opts.fsm, 'options.fsm');
	this.ze_fsm = opts.fsm;
	opts.readableObjectMode = false;
	opts.writableObjectMode = true;
	mod_stream.Transform.call(this, opts);
}
mod_util.inherits(ZKEncodeStream, mod_stream.Transform);

ZKEncodeStream.prototype._transform = function (pkt, enc, cb) {
	mod_assert.object(pkt);
	var zkb = new ZKBuffer({});

	console.log(" > %j", pkt);

	if (this.ze_fsm.getState() === 'connecting') {
		zkb.writeLengthPrefixed(function (sub) {
			sub.writeConnectRequest(pkt);
		});
		this.push(zkb.toBuffer());
		cb();
	} else {
		mod_assert.number(pkt.xid);
		mod_assert.string(pkt.opcode);
		zkb.writeLengthPrefixed(function (sub) {
			sub.writeRequest(pkt);
		});
		this.push(zkb.toBuffer());
		this.ze_fsm.zs_xidMap[pkt.xid] = pkt.opcode;
		cb();
	}
};
