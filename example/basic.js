var async = require('async');
var _ = require('underscore');

var mq = require('../');

var config = {
	url: 'mongodb://localhost:27017',
	db: 'jobs'
};

mq.configure(config);

async.waterfall([
	function (next) {
	// 	mq('chan').publish('Alpha', 'teub', next);
	// },
	// function (next) {
		mq('chan').consume('Alpha', console.log);
		// mq('chan').publish('Alpha', msg, next);
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
