var assert = require('assert');
var async = require('async');
var _ = require('underscore');

var mq = require('../');

var config = {
	host: 'localhost',
	port: 27017,
	db: 'mocha_job'
};

mq.configure(config);

var cache = {};
var col = mq('orca');
var qn = 'delta';
cache.doc = { lorem: 'ipsum' }
async.waterfall([
	col.restore.bind(col, qn),
	col.consume.bind(col, qn),
	function (res, next) {
		console.warn(res);
		// should.not.exist(res);
		col.publish(qn, cache.doc, next);
	},
	col.consume.bind(col, qn),
	function (res, next) {
		console.warn(res);
		// res.should.eql(cache.doc);
		col.consume(qn, next);
	},
	function (res, next) {
		console.warn(res);
		// should.not.exist(res);
		col.restore(qn, next);
	},
	col.consume.bind(col, qn),
	function (res, next) {
		console.warn(res);
		// res.should.eql(cache.doc);
		col.consume(qn, next);
	},
	function (res, next) {
	console.warn(res);
		// should.not.exist(res);
		next();
	}
], callback);
return;
async.waterfall([
	function (next) {
		mq('orca').consume('delta', next);
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