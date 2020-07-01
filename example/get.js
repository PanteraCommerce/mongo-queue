var assert = require('assert');
var async = require('async');
var _ = require('underscore');

var mq = require('../');

var config = {
	url: 'mongodb://localhost:27017',
	db: 'mocha_job'
};

mq.configure(config);

async.waterfall([
	function (next) {
		// mq('orca').get('delta', next);
		// mq('orca').restore('bravo', next);
	}
], callback);

function callback(err) {
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
}
