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
		mq('Chan').consume('Alpha', next);
	},
	function (msg, next) {
		mq('Chan').publish('Alpha', msg, next);
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
