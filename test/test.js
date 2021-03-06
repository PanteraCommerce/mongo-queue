var util = require('util');
var should = require('should');
var async = require('async');
var _ = require('underscore');
var MongoClient = require('mongodb').MongoClient;

var mq = require('../');

var separator = '##';
var config = {
	url: 'mongodb://localhost:27017',
	db: 'mocha_job',
	sep: separator
};

var chan = {
	lion: 'lion',
	bear: 'bear',
	orca: 'orca',
	wolf: 'wolf'
};

var p = {
	shiva: 'shiva',
	vishnu: 'vishnu'
};

var q = {
	alpha: 'alpha',
	bravo: 'bravo',
	charlie: 'charlie',
	delta: 'delta'
};

describe('Basic tests (1 channel, 1 queue, no prefix)', function () {
	var cache = {};

	it('Drop db', function (done) {
		async.waterfall([
      function (next) {
        MongoClient.connect(config.url, { useUnifiedTopology: true }, next);
      },
      function (client, next) {
        client.db(config.db).dropDatabase(next);
      }
    ], done);
  });

  it('configure mongo-queue', function (done) {
		mq.configure(config, done);
	});

	it('consume() should return null when queue does not exists yet', function (done) {
		mq(chan.lion).consume(q.alpha, function (err, res) {
			should.not.exist(err);
			should.not.exist(res);
			done();
		});
	});
	it('publish() should post a message', function (done) {
		cache.msg1 = { color: 'black', n: 0 };
		mq(chan.lion).publish(q.alpha, cache.msg1, done);
	});

	it('consume() should return the lately published message', function (done) {
		mq(chan.lion).consume(q.alpha, function (err, res) {
			should.not.exist(err);
			res.should.eql(cache.msg1);
			done();
		});
	});

	it('queue should now be empty and consume() should return null', function (done) {
		mq(chan.lion).consume(q.alpha, function (err, res) {
			should.not.exist(err);
			should.not.exist(res);
			done();
		});
	});

	it('posting X messages on the same queue (series)', function (done) {
		cache.msg2 = _.range(10).map(function (n) {
			return { color: 'black', n: n };
		});
		async.forEachSeries(cache.msg2, function (msg, asyncCb) {
			mq(chan.lion).publish(q.alpha, msg, asyncCb);
		}, done);
	});

	it('Consumin X + 1 messages on the same queue (series)', function (done) {
		async.forEachSeries(cache.msg2.concat(null), function (msg, asyncCb) {
			mq(chan.lion).consume(q.alpha, function (err, res) {
				should.not.exist(err);
				if (msg) {
					res.should.eql(msg);
				}
				else {
					should.not.exist(res);
				}
				asyncCb();
			});
		}, done);
	});
});

describe('Intermadiate tests with prefix (1 channel, 4 queue)', function () {
	var cache = {};

	it(util.format('publish() on <%s> and <%s%s%s>', q.alpha, p.shiva, separator, q.alpha), function (done) {
		cache.alpha = { type: 'simple', n: 0 };
		cache.shivaAlpha = { type: 'composite', n: 1 };
		async.parallel([
			function (asyncCb) {
				mq(chan.lion).publish(q.alpha, cache.alpha, asyncCb);
			},
			function (asyncCb) {
				mq(chan.lion).publish(p.shiva, q.alpha, cache.shivaAlpha, asyncCb);
			}
		], done);
	});

	it(util.format('consume() on <%s> and <%s%s%s>', q.alpha, p.shiva, separator, q.alpha), function (done) {
		async.parallel([
			function (asyncCb) {
				mq(chan.lion).consume(q.alpha, asyncCb);
			},
			function (asyncCb) {
				mq(chan.lion).consume(p.shiva, q.alpha, asyncCb);
			}
		], function (err, res) {
			should.not.exist(err);
			res[0].should.eql(cache.alpha);
			res[1].should.eql(cache.shivaAlpha);
			done();
		});
	});

	it(util.format('Queues <%s> and <%s%s%s> should be empty', q.alpha, p.shiva, separator, q.alpha), function (done) {
		async.parallel([
			function (asyncCb) {
				mq(chan.lion).consume(q.alpha, asyncCb);
			},
			function (asyncCb) {
				mq(chan.lion).consume(p.shiva, q.alpha, asyncCb);
			}
		], function (err, res) {
			should.not.exist(err);
			should.not.exist(res[0]);
			should.not.exist(res[1]);
			done();
		});
	});

	it('Should be able to record prefix object...', function (done) {
		cache.vishnu = mq(chan.lion).prefix(p.vishnu);
		cache.msg1 = { chan: chan.lion, prefix: [p.vishnu], queue: q.alpha };
		cache.vishnu.publish(q.alpha, cache.msg1, function (err) {
			should.not.exist(err);
			done();
		});
	});

	it('...and order reuse it', function (done) {
		cache.vishnu.consume(q.alpha, function (err, res) {
			should.not.exist(err);
			res.should.eql(cache.msg1);
			done();
		});
	});

	it('A prefix object should be able to return another prefix...', function (done) {
		cache.shivaVishnu = cache.vishnu.prefix(p.shiva);
		cache.msg2 = { chan: chan.lion, prefix: [p.shiva, p.vishnu], queue: q.alpha };
		cache.shivaVishnu.publish(q.alpha, cache.msg2, function (err) {
			should.not.exist(err);
			done();
		});
	});

	it('...and use it the same way', function (done) {
		cache.shivaVishnu.consume(q.alpha, function (err, res) {
			should.not.exist(err);
			res.should.eql(cache.msg2);
			done();
		});
	});
});

describe('Intermadiate tests with channels', function () {
	var cache = {};

	it('Different queues may have the same name if the belong to different channels', function (done) {
		cache.bearBravo = { chan: chan.bear, prefix: [], queue: q.bravo };
		cache.orcaBravo = { chan: chan.orca, prefix: [], queue: q.bravo };
		async.parallel([
			function (asyncCb) {
				mq(chan.bear).publish(q.bravo, cache.bearBravo, asyncCb);
			},
			function (asyncCb) {
				mq(chan.orca).publish(q.bravo, cache.orcaBravo, asyncCb);
			}
		], done);
	});

	it('...so we can get document from 2 different queues', function (done) {
		async.parallel([
			function (asyncCb) {
				mq(chan.bear).consume(q.bravo, asyncCb);
			},
			function (asyncCb) {
				mq(chan.orca).consume(q.bravo, asyncCb);
			}
		], function (err, res) {
			should.not.exist(err);
			res[0].should.eql(cache.bearBravo);
			res[1].should.eql(cache.orcaBravo);
			done();
		});
	});

	it('We should be able to do the same thing with queues whose name is prefixed', function (done) {
		cache.bearShivaCharlie = { chan: chan.bear, prefix: [p.shiva], queue: q.charlie };
		cache.orcaShivaCharlie = { chan: chan.orca, prefix: [p.shiva], queue: q.charlie };
		async.parallel([
			function (asyncCb) {
				mq(chan.bear).publish(p.shiva, q.charlie, cache.bearShivaCharlie, asyncCb);
			},
			function (asyncCb) {
				mq(chan.orca).publish(p.shiva, q.charlie, cache.orcaShivaCharlie, asyncCb);
			}
		], done);
	});

	it('...and get the documents the same way', function (done) {
		async.parallel([
			function (asyncCb) {
				mq(chan.bear).consume(p.shiva, q.charlie, asyncCb);
			},
			function (asyncCb) {
				mq(chan.orca).consume(p.shiva, q.charlie, asyncCb);
			}
		], function (err, res) {
			should.not.exist(err);
			res[0].should.eql(cache.bearShivaCharlie);
			res[1].should.eql(cache.orcaShivaCharlie);
			done();
		});
	});
});

describe('Stress tests', function () {
	var cache = {};

	it('Stress publish()', function (done) {
		cache.stress = {};
		async.forEach([chan.orca, chan.wolf], function (chanName, asyncCb) {
			var chan = mq(chanName);
			var docs = cache.stress[chanName] = _.range(100).map(function (n) {
				return { chan: chanName, n: n };
			});
			async.forEachSeries(docs, function (doc, feCb) {
				chan.publish(q.alpha, doc, feCb);
			}, asyncCb);
		}, done);
	});

	it('Check order (serial consume() calls)', function (done) {
		async.forEach(_.keys(cache.stress), function (chanName, feCb) {
			var chan = mq(chanName);
			async.forEachSeries(cache.stress[chanName], function (doc, feCb) {
				chan.consume(q.alpha, function (err, res) {
          should.not.exist(err);
					res.should.eql(doc);
					feCb();
				});
			}, feCb);
		}, done);
	});

	it('[Hard] stress publish()', function (done) {
		cache.stress = {};
		async.forEach([chan.orca, chan.wolf], function (chanName, asyncCb) {
			var chan = mq(chanName);
			var docs = cache.stress[chanName] = _.range(1000).map(function (n) {
				return { chan: chanName, n: n };
			});
			async.forEachSeries(docs, function (doc, feCb) {
				chan.publish(q.alpha, doc);
				setImmediate(feCb);
			}, asyncCb);
		}, done);
	});

	it('[Hard] Check order (serial consume() calls)', function (done) {
		async.forEach(_.keys(cache.stress), function (chanName, feCb) {
			var chan = mq(chanName);
			async.forEachSeries(cache.stress[chanName], function (doc, feCb) {
				chan.consume(q.alpha, function (err, res) {
					should.not.exist(err);
					res.should.eql(doc);
					feCb();
				});
			}, feCb);
		}, done);
	});
});

describe('get()', function () {
	var cache = {};
	var col = null;
	var qn = q.charlie;

	it('publish a message', function (done) {
		col = mq(chan.wolf);
		cache.doc = { lorem: 'ipsum', dolor :'sit amet', consectetur: 'adipiscing elit' };
		col.publish(qn, cache.doc, done);
	});

	it('should be able to get it without consuming it', function (done) {
		col.get(qn, function (err, res) {
			should.not.exist(err);
			res.should.eql(cache.doc);
			done();
		});
	});

	it('should be able to consume it', function (done) {
		col.consume(qn, function (err, res) {
			should.not.exist(err);
			res.should.eql(cache.doc);
			done();
		});
	});

	it('should be unable to get this message anymore', function (done) {
		col.get(qn, function (err, res) {
			should.not.exist(err);
			should.not.exist(res);
			done();
		});
	});
});

// describe('restore() tests', function () {
// 	var cache = {};

// 	it('#1', function (done) {
// 		var col = mq(chan.orca);
// 		var qn = q.delta;
// 		cache.doc = { lorem: 'ipsum' }
// 		async.waterfall([
// 			col.restore.bind(col, qn),
// 			col.consume.bind(col, qn),
// 			function (res, next) {
// 				// console.warn(res);
// 				should.not.exist(res);
// 				col.publish(qn, cache.doc, next);
// 			},
// 			col.consume.bind(col, qn),
// 			function (res, next) {
// 				// console.warn(res);
// 				res.should.eql(cache.doc);
// 				col.consume(qn, next);
// 			},
// 			function (res, next) {
// 				// console.warn(res);
// 				should.not.exist(res);
// 				col.restore(qn, next);
// 			},
// 			col.consume.bind(col, qn),
// 			function (res, next) {
// 				// console.warn(res);
// 				res.should.eql(cache.doc);
// 				col.consume(qn, next);
// 			},
// 			function (res, next) {
// 			// console.warn(res);
// 				should.not.exist(res);
// 				next();
// 			}
// 		], done);
// 	});
// });
