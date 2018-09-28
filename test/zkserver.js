/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2018, Joyent, Inc.
 */

module.exports = { ZKServer: ZKServer };

const mod_fsm = require('mooremachine');
const mod_cproc = require('child_process');
const mod_fs = require('fs');
const mod_util = require('util');
const mod_assert = require('assert-plus');
const mod_ps = require('ps-node');
const mod_events = require('events');
const mod_uuid = require('node-uuid');

function ZKServer(opts) {
	var self = this;
	this.zk_cmds = ['zkServer.sh', 'zkServer',
	    '/usr/share/zookeeper/bin/zkServer.sh',
	    '/usr/local/bin/zkServer.sh',
	    '/opt/local/sbin/zkServer.sh'];
	this.zk_opts = opts || {};
	this.zk_tokill = [];
	var uuid = (this.zk_uuid = mod_uuid.v4());
	this.zk_tmpdir = '/tmp/' + uuid;
	this.zk_servers = this.zk_opts.servers;
	if (this.zk_servers === undefined) {
		this.zk_servers = {
			'1': {
				'clientPort': 2181,
				'quorumPort': 2888,
				'electionPort': 3888
			}
		};
	}
	this.zk_serverId = this.zk_opts.serverId || '1';
	this.zk_server = this.zk_servers[this.zk_serverId];
	this.zk_config = this.zk_tmpdir + '/zoo.cfg';
	mod_fs.mkdirSync(this.zk_tmpdir);
	mod_fs.mkdirSync(this.zk_tmpdir + '/data');
	var config = [];
	config.push('tickTime=2000');
	config.push('initLimit=10');
	config.push('syncLimit=5');
	config.push('dataDir=' + this.zk_tmpdir + '/data');
	config.push('clientPort=' + this.zk_server.clientPort);
	Object.keys(this.zk_servers).forEach(function (sid) {
		config.push('server.' + sid + '=localhost:' +
		    self.zk_servers[sid].quorumPort + ':' +
		    self.zk_servers[sid].electionPort);
	});
	mod_fs.writeFileSync(this.zk_config, config.join('\n') + '\n');
	mod_fs.writeFileSync(this.zk_tmpdir + '/data/myid', this.zk_serverId);
	mod_fs.writeFileSync(this.zk_tmpdir + '/log4j.properties',
	    'log4j.rootCategory=INFO,console\n' +
	    'log4j.rootLogger=INFO,console\n' +
	    'log4j.appender.console = org.apache.log4j.ConsoleAppender\n' +
	    'log4j.appender.console.Target = System.out\n' +
	    'log4j.appender.console.layout = org.apache.log4j.PatternLayout\n' +
	    'log4j.appender.console.layout.ConversionPattern = ' +
	        '%d{yyyy-MM-dd HH:mm:ss}|%p|%c{1}|%L|%m%n\n');
	if (opts && opts.command)
		this.zk_cmds.unshift(opts.command);
	mod_fsm.FSM.call(this, 'starting');
}
mod_util.inherits(ZKServer, mod_fsm.FSM);

ZKServer.prototype.cli = function () {
	if (!(this.isInState('running') || this.isInState('testing'))) {
		throw (new Error('Invalid state for cli()'));
	}

	var opts = {};
	opts.cwd = this.zk_tmpdir;
	opts.env = {};
	opts.env.HOME = process.env.HOME;
	opts.env.USER = process.env.USER;
	opts.env.LOGNAME = process.env.LOGNAME;
	opts.env.PATH = process.env.PATH;
	opts.env.ZOOCFGDIR = this.zk_tmpdir;
	opts.env.ZOO_LOG_DIR = this.zk_tmpdir;
	opts.env.ZOO_LOG4J_PROP = 'ERROR,console';
	opts.env.JVMFLAGS = '-Dzookeeper.log.dir=' + this.zk_tmpdir;

	var args = Array.prototype.slice.apply(arguments);
	var cb = args.pop();
	mod_assert.func(cb, 'callback');
	for (var i = 0; i < args.length; ++i) {
		if (/[ \t]/.test(args[i])) {
			/* JSSTYLED */
			args[i] = '"' + args[i].replace(/"/g, '\\"') + '"';
		}
	}
	args = args.join(' ');
	var cmd = this.zk_cmd.replace('zkServer', 'zkCli');

	var kid = mod_cproc.spawn(
	     cmd, ['-server', 'localhost:' + this.zk_server.clientPort], opts);

	kid.stdin.write(args + '\n');
	kid.stdin.end();

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
			var shifted = true;
			while (output.length > 0 && shifted) {
				shifted = false;
				var parts = output[0].split('|');
				if (parts.length === 5 &&
				    /^[A-Z]+$/.test(parts[1])) {
					output.shift();
					shifted = true;
				}
				if (/^\[zk: localhost.*\] /.test(output[0])) {
					output.shift();
					shifted = true;
				}
				if (/^Welcome to ZooKeeper!/.test(output[0]) ||
				    /^JLine support/.test(output[0]) ||
				    /^Connecting to /.test(output[0]) ||
				    /^WATCHER::/.test(output[0]) ||
				    /^WatchedEvent.*type:None/.test(
				    output[0]) || output[0] === '') {
					output.shift();
					shifted = true;
				}
			}
			while (output[output.length - 1] === '')
				output.pop();
			while (/^\[zk: localhost.*\] /.test(
			    output[output.length - 1])) {
				output.pop();
			}
			cb(null, output.join('\n') + '\n');

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

ZKServer.prototype.state_starting = function (S) {
	this.zk_cmd = this.zk_cmds.shift();
	S.gotoState('spawning');
};

ZKServer.prototype.state_spawning = function (S) {
	var self = this;

	var opts = {};
	opts.cwd = this.zk_tmpdir;
	opts.env = {};
	opts.env.HOME = process.env.HOME;
	opts.env.USER = process.env.USER;
	opts.env.LOGNAME = process.env.LOGNAME;
	opts.env.PATH = process.env.PATH;
	opts.env.ZOOCFGDIR = this.zk_tmpdir;
	opts.env.ZOO_LOG_DIR = this.zk_tmpdir;
	opts.env.ZOO_LOG4J_PROP = 'INFO,console';
	opts.env.JVMFLAGS = '-Dzookeeper.log.dir=' + this.zk_tmpdir;

	this.zk_kid = mod_cproc.spawn(this.zk_cmd, ['start-foreground',
	    this.zk_config], opts);
	S.on(this.zk_kid, 'error', function (err) {
		if (err.code === 'ENOENT') {
			S.gotoState('starting');
		} else {
			self.zk_lastError = err;
			S.gotoState('error');
		}
	});
	var logs = '';
	this.zk_kid.stderr.on('data', function (data) {
		console.error('zk: %j', data.toString('ascii'));
	});
	this.zk_kid.stdout.on('data', function (data) {
		var str = data.toString('ascii');
		logs += str;
		var lines = logs.split('\n');
		if (str.charAt(str.length - 1) === '\n') {
			logs = '';
		} else {
			logs = lines[lines.length - 1];
		}
		lines.forEach(function (l) {
			var parts = l.split('|');
			if (parts.length === 5 && /^[A-Z]+$/.test(parts[1])) {
				self.emit('zkLogLine', parts[0], parts[1],
				    parts[2], parts[3], parts[4]);
			}
		});
	});
	S.on(this, 'zkLogLine', function (date, level, klass, line, msg) {
		var sc = Object.keys(self.zk_servers).length;
		if (sc > 1 &&
		    level === 'INFO' && klass === 'QuorumPeer' &&
		    /^(LEADING|FOLLOWING)/.test(msg)) {
			S.gotoState('findkids');
		}
		if (sc === 1 &&
		    level === 'INFO' && klass === 'NIOServerCnxnFactory' &&
		    /^binding to port/.test(msg)) {
			S.gotoState('findkids');
		}
	});
	S.on(this.zk_kid, 'close', function (code) {
		self.zk_lastError = new Error('Exited with status ' + code);
		S.gotoState('error');
	});
};

ZKServer.prototype.state_findkids = function (S) {
	var self = this;
	var req = new mod_events.EventEmitter();

	mod_ps.lookup({ ppid: self.zk_kid.pid },
	    function (err, res) {
		if (err)
			req.emit('error', err);
		else
			req.emit('result', res);
	});

	S.on(req, 'error', function (err) {
		self.zk_lastError = err;
		S.gotoState('error');
	});

	S.on(req, 'result', function (res) {
		if (res.length < 1) {
			S.gotoState('findkids');
			return;
		}
		self.zk_tokill = res.map(function (ps) {
			return (parseInt(ps.pid, 10));
		});
		self.zk_tokill.push(self.zk_kid.pid);
		S.gotoState('testing');
	});
};

ZKServer.prototype.state_testing = function (S) {
	var self = this;

	this.zk_lastError = undefined;

	S.timeout(10000, function () {
		if (self.zk_lastError === undefined)
			self.zk_lastError = new Error('Timeout');
		S.gotoState('error');
	});

	S.interval(1000, function () {
		self.cli('ls', '/', S.callback(function (err) {
			if (err) {
				self.zk_lastError = err;
				return;
			}
			S.gotoState('running');
		}));
	});

	S.on(self.zk_kid, 'close', function (code) {
		self.zk_lastError = new Error(
		    'Exited with status ' + code);
		S.gotoState('error');
	});
};

ZKServer.prototype.state_running = function (S) {
	var self = this;
	S.on(this.zk_kid, 'close', function (code) {
		self.zk_lastError = new Error('Exited with status ' + code);
		S.gotoState('error');
	});
	S.on(this, 'stopAsserted', function () {
		S.gotoState('stopping');
	});
	S.on(process, 'exit', function () {
		S.gotoState('stopping');
	});
};

ZKServer.prototype.stop = function () {
	mod_assert.strictEqual(this.getState(), 'running');
	this.emit('stopAsserted');
};

ZKServer.prototype.state_stopping = function (S) {
	S.on(this.zk_kid, 'close', function (code) {
		S.gotoState('stopped');
	});
	this.zk_tokill.forEach(function (pid) {
		console.error('zk: killing %d', pid);
		mod_cproc.spawnSync('kill', [pid]);
	});
};

ZKServer.prototype.state_stopped = function () {
	mod_cproc.spawnSync('rm', ['-fr', this.zk_tmpdir]);
	delete (this.zk_kid);
};

ZKServer.prototype.state_error = function () {
	delete (this.zk_kid);
	this.emit('error', this.zk_lastError);
};
