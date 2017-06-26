/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
 */

const mod_client = require('./client');
const mod_errors = require('./errors');
const mod_assert = require('assert-plus');
const mod_net = require('net');

/*
 * Overview
 * ========
 *
 * +----------------------------+
 * |           ZKClient         |
 * +----------------------------+
 *         |                |
 * +---------------+  +---------+
 * |ZKConnectionFSM|--|ZKSession|
 * +---------------+  +---------+
 *            |
 *      +-------------------+
 *      |ZK(En|De)codeStream|
 *      +-------------------+
 *               |
 *           +--------+
 *           |ZKBuffer|
 *           +--------+
 *               |
 *          +----------+
 *          |JuteBuffer|
 *          +----------+
 *
 * The ZKClient (in client.js, exported as just "Client") is the public-facing
 * API of the library.
 *
 * The client sets up a cueball ConnectionSet which constructs ZKConnectionFSMs.
 * It also manages a ZKSession instance which tracks our virtual session with
 * ZK (which we can re-attach to from some other server once established).
 *
 * ZKConnectionFSM informs the active ZKSession instance directly when a new
 * connection has been established (and ZKSession is responsible for deciding
 * how to proceed with re-attaching to the old session or creating a new one).
 *
 * The ZKConnectionFSM communicates with ZK using the ZKEncodeStream and
 * ZKDecodeStream, which manage the ZK framing format.
 *
 * The actual packet payloads are decoded and encoded using the code in
 * ZKBuffer (with utilities provided by JuteBuffer).
 */

module.exports = {
	Client: mod_client.Client,
	ZKError: mod_errors.ZKError,
	ZKProtocolError: mod_errors.ZKProtocolError,
	ZKPingTimeoutError: mod_errors.ZKPingTimeoutError
};
