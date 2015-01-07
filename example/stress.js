var assert = require('assert');
var async = require('async');
var _ = require('underscore');

var mq = require('../');

var config = {
	host: 'localhost',
	port: 27017,
	db: 'job'
};

mq.configure(config);

async.waterfall([
	function (next) {
		async.forEach(['orca', 'wolf'], function (chanName, asyncCb) {
			var chan = mq(chanName);
			var docs = _.range(300).map(function (n) {
				return { chan: chanName, n: n };
			});
			async.forEachSeries(docs, function (doc, feCb) {
				chan.publish('alpha', doc);
				setTimeout(function () {
					feCb()
				}, 5);
			}, asyncCb);
		}, next);
	},
	function (next) {
		var wolf = mq('wolf');
		wolf._q.forEach(function (msg, i) {
			assert.equal(msg.message.n, i);
		});
		next();
	}
], function (err) {
	if (err) {
		console.error('[-] Error');
		throw err;
	}
	var res = Array.prototype.slice.call(arguments, 1);
	console.log('[-] Terminated. Receiced %d result', res.length);
	res.forEach(function (r, i) {
		console.log(i + ':', r);
	});
	mq.close();
});
