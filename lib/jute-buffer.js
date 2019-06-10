/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
 */

module.exports = { JuteBuffer: JuteBuffer };

const mod_assert = require('assert-plus');
const mod_jsbn = require('jsbn');

function JuteBuffer(opts) {
	mod_assert.object(opts, 'options');
	if (opts.buffer !== undefined)
		mod_assert.buffer(opts.buffer, 'options.buffer');
	this.jb_size = opts.buffer ? opts.buffer.length : 1024;
	this.jb_buffer = opts.buffer || (new Buffer(this.jb_size));
	this.jb_offset = 0;
}

JuteBuffer.prototype.toBuffer = function () {
	return (this.jb_buffer.slice(0, this.jb_offset));
};

JuteBuffer.prototype.atEnd = function () {
	return (this.jb_offset >= this.jb_buffer.length);
};

JuteBuffer.prototype.remainder = function () {
	return (this.jb_buffer.slice(this.jb_offset));
};

JuteBuffer.prototype.skip = function (n) {
	this.jb_offset += n;
};

JuteBuffer.prototype.expand = function () {
	this.jb_size *= 2;
	var buf = new Buffer(this.jb_size);
	this.jb_buffer.copy(buf, 0);
	this.jb_buffer = buf;
};

JuteBuffer.prototype.readByte = function () {
	var v = this.jb_buffer.readInt8(this.jb_offset++);
	return (v);
};

JuteBuffer.prototype.readBool = function () {
	mod_assert.ok(this.jb_offset < this.jb_size);
	var v = this.jb_buffer[this.jb_offset++];
	mod_assert.ok(v === 0 || v === 1);
	return (v === 1);
};

JuteBuffer.prototype.readInt = function () {
	var v = this.jb_buffer.readInt32BE(this.jb_offset);
	this.jb_offset += 4;
	return (v);
};

JuteBuffer.prototype.readLongBuffer = function () {
	mod_assert.ok(this.jb_offset + 8 <= this.jb_size);
	var v = this.jb_buffer.slice(this.jb_offset, this.jb_offset + 8);
	this.jb_offset += 8;
	return (v);
};

JuteBuffer.prototype.readLongBN = function () {
	return (new mod_jsbn.BigInteger(this.readLongBuffer()));
};

JuteBuffer.prototype.readLongDate = function () {
	return (new LongDate(this.readLongBuffer()));
};

function LongDate(buffer) {
	mod_assert.buffer(buffer, 'buffer');
	this.ld_buffer = buffer;
}
LongDate.prototype.toDate = function () {
	var n = this.ld_buffer.readUInt32BE(4);
	n += 4294967296 * this.ld_buffer.readUInt32BE(0);
	var d = new Date();
	d.setTime(n);
	return (d);
};
LongDate.prototype.toBuffer = function () {
	return (this.ld_buffer.slice());
};
LongDate.prototype.toString = function () {
	return (this.toDate().toString());
};

JuteBuffer.prototype.readBuffer = function () {
	var len = this.readInt();
	if (len < 0)
		len = 0;
	mod_assert.ok(this.jb_offset + len <= this.jb_size);
	var buf = this.jb_buffer.slice(this.jb_offset, this.jb_offset + len);
	this.jb_offset += len;
	return (buf);
};

JuteBuffer.prototype.readUString = function () {
	return (this.readBuffer().toString('utf-8'));
};

JuteBuffer.prototype.writeBool = function (v, assertName) {
	mod_assert.bool(v, assertName || 'value');
	this.writeByte(v ? 1 : 0);
};

JuteBuffer.prototype.writeByte = function (v, assertName) {
	mod_assert.number(v, assertName || 'value');
	while (this.jb_offset + 1 > this.jb_size)
		this.expand();
	this.jb_buffer.writeInt8(v, this.jb_offset++);
};

JuteBuffer.prototype.writeBuffer = function (v, assertName) {
	mod_assert.buffer(v, assertName || 'value');
	while (this.jb_offset + v.length + 4 > this.jb_size)
		this.expand();
	if (v.length === 0) {
		this.writeInt(-1);
		return;
	}
	this.writeInt(v.length);
	v.copy(this.jb_buffer, this.jb_offset);
	this.jb_offset += v.length;
};

JuteBuffer.prototype.writeUString = function (v, assertName) {
	mod_assert.string(v, assertName || 'value');
	this.writeBuffer(new Buffer(v, 'utf-8'));
};

JuteBuffer.prototype.writeInt = function (v, assertName) {
	mod_assert.number(v, assertName || 'value');
	while (this.jb_offset + 4 > this.jb_size)
		this.expand();
	this.jb_buffer.writeInt32BE(v, this.jb_offset);
	this.jb_offset += 4;
};

JuteBuffer.prototype.writeLong = function (v) {
	if (Buffer.isBuffer(v)) {
		mod_assert.ok(v.length <= 8);
	} else if (v instanceof LongDate) {
		v = v.toBuffer();
		mod_assert.buffer(v);
		mod_assert.ok(v.length <= 8);
	} else {
		v = new Buffer(v.toByteArray());
		mod_assert.ok(v.length <= 8);
	}
	while (this.jb_offset + 8 > this.jb_size)
		this.expand();
	this.jb_buffer.fill(0, this.jb_offset, this.jb_offset + 8);
	v.copy(this.jb_buffer, this.jb_offset + (8 - v.length));
	this.jb_offset += 8;
};

JuteBuffer.prototype.readLengthPrefixed = function (cb) {
	mod_assert.func(cb, 'callback');
	var len = this._buffer.readUInt32BE(this.jb_offset);
	this.jb_offset += 4;
	mod_assert.ok(this.jb_offset + len <= this.jb_size);

	var child = Object.create(this);
	child.jb_size = this.jb_offset + len;
	var ret = cb(child);
	this.jb_offset += len;

	return (ret);
};

JuteBuffer.prototype.writeLengthPrefixed = function (cb) {
	var lenOffset = this.jb_offset;
	this.jb_offset += 4;
	var ret = cb(this);
	var len = this.jb_offset - lenOffset - 4;
	this.jb_buffer.writeUInt32BE(len, lenOffset);

	return (ret);
};
