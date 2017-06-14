/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
 */

const mod_client = require('./client-fsm');
const mod_errors = require('./errors');
const mod_assert = require('assert-plus');
const mod_net = require('net');

module.exports = {
	connect: connect,
	Client: mod_client.Client,
	ZKError: mod_errors.ZKError,
	ZKProtocolError: mod_errors.ZKProtocolError,
	ZKPingTimeoutError: mod_errors.ZKPingTimeoutError
};

function connect(options, cb) {
	mod_assert.object(options, 'options');
	mod_assert.optionalFunc(cb, 'callback');

	var zkc = new mod_client.Client(options);
	if (cb) {
		function onStateChanged(st) {
			if (st === 'connected') {
				zkc.removeListener('stateChanged',
				    onStateChanged);
				zkc.removeListener('error', onError);
				cb(null, zkc);
			}
		}
		function onError(err) {
			cb(err);
		}
		zkc.on('stateChanged', onStateChanged);
		zkc.on('error', onError);
	}
	zkc.connect();

	return (zkc);
}
