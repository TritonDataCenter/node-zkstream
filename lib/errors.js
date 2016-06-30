/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
 */

module.exports = {
	ZKProtocolError: ZKProtocolError,
	ZKError: ZKError
};

const mod_assert = require('assert-plus');
const mod_util = require('util');

function ZKProtocolError(code, msg) {
	if (Error.captureStackTrace)
		Error.captureStackTrace(this, ZKProtocolError);
	this.code = code;
	this.name = 'ZKProtocolError';
	this.message = code + ': ' + msg;
}
mod_util.inherits(ZKProtocolError, Error);

function ZKError(code, msg) {
	if (Error.captureStackTrace)
		Error.captureStackTrace(this, ZKError);
	this.code = code;
	this.name = 'ZKError';
	this.message = code + ': ' + msg;
}
mod_util.inherits(ZKError, Error);
