/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2016, Joyent, Inc.
 */

module.exports = { ZKServer: ZKServer };

const mod_fsm = require('mooremachine');
const mod_cproc = require('child_process');
const mod_fs = require('fs');
const mod_util = require('util');
const mod_assert = require('assert-plus');
const mod_ps = require('ps-node');
const mod_events = require('events');

function ZKServer(opts) {
	this.zk_cmds = ['zkServer.sh', 'zkServer'];
	this.zk_opts = opts;
	this.zk_tokill = [];
	if (opts && opts.command)
		this.zk_cmds.unshift(opts.command);
	mod_fsm.FSM.call(this, 'starting');
}
mod_util.inherits(ZKServer, mod_fsm.FSM);

ZKServer.prototype.cli = function () {
	mod_assert.strictEqual(this.getState(), 'running');

	var args = Array.prototype.slice.apply(arguments);
	var cb = args.pop();
	mod_assert.func(cb, 'callback');
	var cmd = this.zk_cmd.replace('zkServer', 'zkCli');

	var kid = mod_cproc.spawn(cmd, args);

	var output = '';
	kid.stdout.on('data', function (d) {
		output += d.toString('ascii');
	});
	var errout = '';
	kid.stderr.on('data', function (d) {
		errout += d.toString('ascii');
	});
	kid.on('close', function (code) {
		if (code === 0) {
			output = output.split('\n');
			if (/^Connecting to /.test(output[0]))
				output.shift();
			if (output[0] === '')
				output.shift();
			if (/^WATCHER::/.test(output[0]))
				output.shift();
			if (output[0] === '')
				output.shift();
			if (/^WatchedEvent.*type:None/.test(output[0]))
				output.shift();
			if (output[0] === '')
				output.shift();
			cb(null, output.join('\n'));

		} else {
			errout = errout.split('\n');
			var excs = errout.filter(function (l) {
				return (/^Exception in/.test(l));
			});
			if (excs.length > 0) {
				cb(new Error(excs[0]));
				return;
			}
			errout = errout.filter(function (l) {
				return (!/^Connecting to /.test(l));
			});
			cb(new Error(errout.join(' ')));
		}
	});
};

ZKServer.prototype.state_starting = function (on) {
	this.zk_cmd = this.zk_cmds.shift();
	this.gotoState('spawning');
};

ZKServer.prototype.state_spawning = function (on) {
	var self = this;
	this.zk_kid = mod_cproc.spawn(this.zk_cmd, ['start-foreground']);
	on(this.zk_kid, 'error', function (err) {
		if (err.code === 'ENOENT') {
			self.gotoState('starting');
		} else {
			self.zk_lastError = err;
			self.gotoState('error');
		}
	});
	var output = '';
	this.zk_kid.stderr.on('data', function (data) {
		console.error('zk: %j', data.toString('ascii'));
	});
	this.zk_kid.stdout.on('data', function (data) {
		console.error('zk: %j', data.toString('ascii'));
	});
	on(this.zk_kid.stderr, 'data', function (data) {
		output += data.toString('ascii');
		var lines = output.split('\n');
		lines = lines.map(function (l) {
			return (/^Using config: [^ \t]+$/.test(l));
		});
		if (lines.length > 0)
			self.gotoState('findkids');
	});
	on(this.zk_kid, 'close', function (code) {
		self.zk_lastError = new Error('Exited with status ' + code);
		self.gotoState('error');
	});
};

ZKServer.prototype.state_findkids = function (on) {
	var self = this;
	var req = new mod_events.EventEmitter();

	mod_ps.lookup({ ppid: self.zk_kid.pid },
	    function (err, res) {
		if (err)
			req.emit('error', err);
		else
			req.emit('result', res);
	});

	on(req, 'error', function (err) {
		self.zk_lastError = err;
		self.gotoState('error');
	});

	on(req, 'result', function (res) {
		self.zk_tokill = res.map(function (ps) {
			return (parseInt(ps.pid, 10));
		});
		self.zk_tokill.push(self.zk_kid.pid);
		self.gotoState('testing');
	});
};

ZKServer.prototype.state_testing = function (on, once, timeout) {
	var self = this;
	var cmd = this.zk_cmd.replace('zkServer', 'zkCli');

	timeout(10000, function () {
		self.zk_lastError = new Error('Timeout');
		self.gotoState('error');
	});

	timeout(1000, function () {
		var kid = mod_cproc.spawn(cmd, ['ls', '/']);
		on(kid, 'close', function (code) {
			if (code === 0) {
				self.gotoState('running');
			} else {
				self.zk_lastError = new Error(
				    'Testing command exited with status ' +
				    code);
				self.gotoState('error');
			}
		});
	});

	on(self.zk_kid, 'close', function (code) {
		self.zk_lastError = new Error(
		    'Exited with status ' + code);
		self.gotoState('error');
	});
};

ZKServer.prototype.state_running = function (on) {
	var self = this;
	on(this.zk_kid, 'close', function (code) {
		self.zk_lastError = new Error('Exited with status ' + code);
		self.gotoState('error');
	});
	on(this, 'stopAsserted', function () {
		self.gotoState('stopping');
	});
	on(process, 'exit', function () {
		self.gotoState('stopping');
	});
};

ZKServer.prototype.stop = function () {
	mod_assert.strictEqual(this.getState(), 'running');
	this.emit('stopAsserted');
};

ZKServer.prototype.state_stopping = function (on) {
	var self = this;
	on(this.zk_kid, 'close', function (code) {
		self.gotoState('stopped');
	});
	this.zk_tokill.forEach(function (pid) {
		console.error('zk: killing %d', pid);
		mod_cproc.spawnSync('kill', [pid]);
	});
};

ZKServer.prototype.state_stopped = function () {
	delete (this.zk_kid);
};

ZKServer.prototype.state_error = function () {
	delete (this.zk_kid);
	this.emit('error', this.zk_lastError);
};
