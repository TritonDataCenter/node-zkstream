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
	Client: mod_client.ClientFSM,
	ZKError: mod_errors.ZKError,
	ZKProtocolError: mod_errors.ZKProtocolError
};

function connect(options, cb) {
	mod_assert.object(options, 'options');
	mod_assert.optionalFunc(cb, 'callback');

	mod_assert.string(options.host, 'options.host');
	mod_assert.optionalNumber(options.port, 'options.port');
	if (options.port === undefined)
		options.port = 2181;

	var sock = mod_net.connect(options);
	var zkc = new mod_client.ClientFSM(options);
	if (cb)
		zkc.onState('connected', cb);
	zkc.attach(sock);

	return (zkc);
}
