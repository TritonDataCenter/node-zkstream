/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2019, Joyent, Inc.
 */

var mod_assert = require('assert-plus');

module.exports = {
	wait: wait
};

function wait(t, name, maxTimeout, cond, cb) {
	mod_assert.object(t, 'test object');
	mod_assert.string(name, 'name');
	mod_assert.number(maxTimeout, 'maxTimeout');
	mod_assert.func(cond, 'condition');
	mod_assert.func(cb, 'cb');
	var time = 0;
	function check() {
		var res = cond();
		if (res) {
			t.ok(res, name);
			cb();
			return;
		}
		time += 100;
		if (time > maxTimeout) {
			t.fail('timed out waiting for condition: ' + name);
			t.end();
			return;
		}
		setTimeout(check, 100);
	}
	setTimeout(check, 100);
}
