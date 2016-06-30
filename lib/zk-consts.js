/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
 */

module.exports = {};

const mod_util = require('util');

module.exports.PERM_MASKS = {
	READ: (1<<0),
	WRITE: (1<<1),
	CREATE: (1<<2),
	DELETE: (1<<3),
	ADMIN: (1<<4)
};

module.exports.CREATE_FLAGS = {
	EPHEMERAL: (1<<0),
	SEQUENTIAL: (1<<1)
};

var ERR_CODES = (module.exports.ERR_CODES = {
	OK : 0,
	SYSTEM_ERROR : -1,
	RUNTIME_INCONSISTENCY : -2,
	DATA_INCONSISTENCY : -3,
	CONNECTION_LOSS : -4,
	MARSHALLING_ERROR : -5,
	UNIMPLEMENTED : -6,
	OPERATION_TIMEOUT : -7,
	BAD_ARGUMENTS : -8,
	API_ERROR : -100,
	NO_NODE : -101,
	NO_AUTH : -102,
	BAD_VERSION : -103,
	NO_CHILDREN_FOR_EPHEMERALS : -108,
	NODE_EXISTS : -110,
	NOT_EMPTY : -111,
	SESSION_EXPIRED : -112,
	INVALID_CALLBACK : -113,
	INVALID_ACL : -114,
	AUTH_FAILED : -115
});
var ERR_LOOKUP = (module.exports.ERR_LOOKUP = {});
Object.keys(ERR_CODES).forEach(function (k) {
	ERR_LOOKUP[ERR_CODES[k]] = k;
});

var ERR_TEXT = (module.exports.ERR_TEXT = {
	SYSTEM_ERROR : 'An unknown system error occurred on the ZooKeeper ' +
	    'server',
	RUNTIME_INCONSISTENCY : 'A runtime inconsistency was found, and the ' +
	    'request aborted for safety',
	DATA_INCONSISTENCY : 'A data inconsistency was found, and the ' +
	    'request aborted for safety',
	CONNECTION_LOSS : 'Connection to the ZooKeeper server has been lost',
	MARSHALLING_ERROR : 'Error while marshalling or unmarshalling data',
	UNIMPLEMENTED : 'ZooKeeper request unimplemented',
	OPERATION_TIMEOUT : 'ZooKeeper operation timed out',
	BAD_ARGUMENTS : 'Bad arguments to ZooKeeper request',
	API_ERROR : '',
	NO_NODE : 'The specified ZooKeeper path does not exist',
	NO_AUTH : 'Request requires authentication and your ZooKeeper ' +
	    'connection is anonymous',
	BAD_VERSION : 'A specific version of an object was named in the ' +
	    'request, but this was not the latest version on the server.' +
	    ' The object may have been changed by another client.',
	NO_CHILDREN_FOR_EPHEMERALS : 'Ephemeral nodes cannot have children',
	NODE_EXISTS : 'The specified ZooKeeper path already exists, and ' +
	    'the requested operation requires creating a new node',
	NOT_EMPTY : 'The specified ZooKeeper node has children and thus ' +
	    'cannot be destroyed',
	SESSION_EXPIRED : 'ZooKeeper session expired',
	INVALID_CALLBACK : '',
	INVALID_ACL : 'The given ZooKeeper ACL was found to be invalid on ' +
	    'the server side',
	AUTH_FAILED : 'ZooKeeper authentication failed'
});

var OP_CODES = (module.exports.OP_CODES = {
	NOTIFICATION : 0,
	CREATE : 1,
	DELETE : 2,
	EXISTS : 3,
	GET_DATA : 4,
	SET_DATA : 5,
	GET_ACL : 6,
	SET_ACL : 7,
	GET_CHILDREN : 8,
	SYNC : 9,
	PING : 11,
	GET_CHILDREN2 : 12,
	CHECK : 13,
	MULTI : 14,
	AUTH : 100,
	SET_WATCHES : 101,
	SASL : 102,
	CREATE_SESSION : -10,
	CLOSE_SESSION : -11,
	ERROR : -1
});
var OP_CODE_LOOKUP = (module.exports.OP_CODE_LOOKUP = {});
Object.keys(OP_CODES).forEach(function (k) {
	OP_CODE_LOOKUP[OP_CODES[k]] = k;
});

var NOTIFICATION_TYPE = (module.exports.NOTIFICATION_TYPE = {
	CREATED : 1,
	DELETED : 2,
	DATA_CHANGED : 3,
	CHILDREN_CHANGED : 4
});
var NOTIFICATION_TYPE_LOOKUP = (module.exports.NOTIFICATION_TYPE_LOOKUP = {});
Object.keys(NOTIFICATION_TYPE).forEach(function (k) {
	NOTIFICATION_TYPE_LOOKUP[NOTIFICATION_TYPE[k]] = k;
});

var STATE = (module.exports.STATE = {
	DISCONNECTED : 0,
	SYNC_CONNECTED : 3,
	AUTH_FAILED : 4,
	CONNECTED_READ_ONLY : 5,
	SASL_AUTHENTICATED : 6,
	EXPIRED : -122
});
var STATE_LOOKUP = (module.exports.STATE_LOOKUP = {});
Object.keys(STATE).forEach(function (k) {
	STATE_LOOKUP[STATE[k]] = k;
});

module.exports.XID_NOTIFICATION = -1;
module.exports.XID_PING = -2;
module.exports.XID_AUTHENTICATION = -4;
module.exports.XID_SET_WATCHES = -8;
