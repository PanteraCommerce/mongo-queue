var util = require('util');
var EventEmitter = require('events').EventEmitter;
var mongodb = require('mongodb');
var MongoClient = require('mongodb').MongoClient;
var async = require('async');
var _ = require('underscore');

var noop = function () {};

// Shared static data
var _shr = {
	sep: '##',
	local: 'local'
};

// Prefix ----------------------------------------------------------------------

function Prefix(name, channel) {
	this.name = name;
	this._channel = channel;
}

Prefix.prototype.prefix = function () {
	var prefix = Array.prototype.slice.call(arguments).join(_shr.sep);
	return new Prefix([prefix, this.name].join(_shr.sep), this._channel);
};
Prefix.prototype.$ = Prefix.prototype.prefix;

// ['publish', 'consume', 'get', 'restore'].forEach(function (method) {
['publish', 'consume', 'get'].forEach(function (method) {
	Prefix.prototype[method] = function () {
		var args = Array.prototype.slice.call(arguments);
		args.unshift(this.name);
		Channel.prototype[method].apply(this._channel, args);
		return this;
	};
});

// Channel ---------------------------------------------------------------------

function Channel(name, db) {
	var self = this;
	EventEmitter.prototype.constructor.call(this);
	this.name = name;
	this.ready = false;
	this._db = db;
	this._collection = null;
	this._connectCalled = false;
	this._q = async.queue(function (task, callback) {
		self._publish(task.queueName, task.message, callback);
	}, 1);
	this.setMaxListeners(500);
}

util.inherits(Channel, EventEmitter);

Channel.set = function (key, value) {
	_shr[key] = value;
	return Channel;
};

Channel.prototype._connect = function(db) {
	var self = this;

	if (this._connectCalled === true)
		return;
	this._connectCalled = true;
	this._db = db;

	async.waterfall([
		function (next) {
			self._db.collection(self.name, next);
		},
		function (col, next) {
			self._collection = col;
			self._collection.ensureIndex({ name: 1 }, { unique: true, dropDups: true }, next);
		}
	], function (err) {
		if (err) {
			self._connectCalled = false;
			self.ready = false;
			self.emit('error', err);
		}
		else {
			self.ready = true;
			self.emit('ready');
		}
	});
};

Channel.prototype._onReady = function (fn) {
	if (this.ready === true) {
		return fn();
	}
	this.once('ready', fn);
};

Channel.prototype.prefix = function () {
	var prefix = Array.prototype.slice.call(arguments).join(_shr.sep);
	return new Prefix(prefix, this);
};
Channel.prototype.$ = Channel.prototype.prefix;

Channel.prototype._publish = function (queueName, message, callback) {
	var self = this;

	this._onReady(function () {
		// For mongo@2.0
		// self._collection.updateOne({ name: queueName }, { $push: { queue: message } }, { safe: true, upsert: true }, function (err) {
		self._collection.update({ name: queueName }, { $push: { queue: message } }, { safe: true, upsert: true }, function (err) {
			callback(err);
		});
	});
	return this;
};

// publish([..., ] queueName, message [, callback]);
Channel.prototype.publish = function (queueName, message, callback) {
	var self = this;
	var args = Array.prototype.slice.call(arguments);

	callback = typeof args[args.length - 1] === 'function' ? args.pop() : noop;
	var task = {
		message: args.pop(),
		queueName: args.join(_shr.sep)
	};
	this._q.push(task, callback);
	return this;
};

Channel.prototype.consume = function (queueName, callback) {
	var self = this;
	var args = Array.prototype.slice.call(arguments);

	this._onReady(function () {
		callback = typeof args[args.length - 1] === 'function' ? args.pop() : noop;
		queueName = args.join(_shr.sep);
		self._collection.findAndModify({ name: queueName }, [], { $pop: { queue: -1 } }, {
			fields:	{ queue: { $slice: 1 }, _id: 0 },
		}, function (err, doc, res) {
			if (err)
				return callback(err);
			// doc = doc.value; // For mongodb@2.0
			callback(null, doc ? doc.queue[0] || null : null);
		});
	});
	return this;
};

Channel.prototype.get = function(queueName, callback) {
	var self = this;
	var args = Array.prototype.slice.call(arguments);

	this._onReady(function () {
		callback = typeof args[args.length - 1] === 'function' ? args.pop() : noop;
		queueName = args.join(_shr.sep);
		self._collection.find({ name: queueName }, { queue: { $slice: 1 }, _id: 0 }).toArray(function (err, doc) {
			if (err) {
				return callback(err);
			}
			doc = doc[0];
			callback(null, doc ? doc.queue[0] || null : null);
		});
	});
};

/*
db.oplog.$main.find({ op: 'u', ns: 'job.orca', 'o.$set.queue': { $exists: 1 }  }).sort({ ts: -1 })
*/
// Channel.prototype.restore = function(queueName, callback) {
// 	var self = this;
// 	var args = Array.prototype.slice.call(arguments);

// 	callback = typeof args[args.length - 1] === 'function' ? args.pop() : noop;
// 	queueName = args.join(_shr.sep);
// 	var sc = this._db.serverConfig;
// 	var db = new mongodb.Db(_shr.local, new mongodb.Server(sc.host, sc.port, sc.options), { safe: true });
// 	var _id = null;
// 	var _restore = null;
// 	async.waterfall([
// 		this._onReady.bind(this),
// 		function (next) {
// 			self._collection.find({ name: queueName }, { _restore: 1, _id: 1 }).toArray(next);
// 		},
// 		function (q, next) {
// 			if (q.length === 0) {
// 				return callback();
// 			}
// 			_id = q[0]._id;
// 			_restore = q[0]._restore;
// 			db.open(next);
// 		},
// 		function (db, next) {
// 			db.collection('oplog.$main', next);
// 		},
// 		function (col, next) {
// 			col.find({ 'o2._id': _id, op: 'u', ns: [self._db.databaseName, self.name].join('.'), 'o.$set': { $exists: true } })
// 			.limit(500)
// 			.sort({ ts: -1 })
// 			.toArray(next);
// 		},
// 		function (res, next) {
// 			if (res.length === 0) {
// 				db.close();
// 				return callback();
// 			}
// 			var set = _.map(res, function (h) {
// 				return _.chain(h.o.$set).clone().extend({ ts: h.ts }).value();
// 			});
// 			var q = _(set).groupBy(function (doc) {
// 				return Array.isArray(doc.queue) ? 'consume' : 'publish';
// 			});
// 			var last = null;
// 			var ts = null;
// 			if (q.consume[0].queue.length > 0 && q.consume[1] && q.consume[1].queue.length === 0) {
// 				last = _.find(q.publish, function (h) {
// 					return h['queue.0'] != null || (h.queue != null && h.queue.length > 0);
// 				});
// 				ts = last.ts;
// 				if (last) {
// 					last = last['queue.0'] || last.queue[0];
// 				}
// 			}
// 			else if (q.consume[0].queue.length > 0 && q.consume[1] && q.consume[1].queue.length > 0) {
// 				ts = q.consume[1].ts;
// 				last = q.consume[1].queue[0];
// 			}
// 			else if (q.consume[0].queue.length === 0) {
// 				var i = 0;
// 				while (set[i].queue == null) { ++i; }
// 				last = _.find(set.slice(i), function (h) {
// 					return h['queue.0'] != null || (h.queue != null && h.queue.length > 0);
// 				});
// 				if (last) {
// 					ts = last.ts;
// 					last = last['queue.0'] || last.queue[0];
// 				}
// 			}
// 			db.close();
// 			if (last == null) {
// 				return callback();
// 			}
// 			if (_restore != null && _restore.high_ === ts.high_ && _restore.low_ === ts.low_) {
// 				return callback();
// 			}
// 			next(null , last, ts);
// 		},
// 		function (last, ts, next) {
// 			self._db.eval(function (col, queueName, last, ts) {
// 				var chan = db[col];
// 				var doc = chan.findOne({ name: queueName });
// 				if (doc) {
// 					doc._restore = ts;
// 					doc.queue.unshift(last);
// 					chan.save(doc);
// 				}
// 				return doc;
// 			}, [self.name, queueName, last, ts], next);
// 		},
// 		function (res, next) {
// 			next();
// 		}
// 	], callback);
// 	return this;
// };

// Channel.prototype.consume = function (queueName, callback) {
// 	var self = this;
// 	var args = Array.prototype.slice.call(arguments);

// 	this._onReady(function () {
// 		callback = typeof args[args.length - 1] === 'function' ? args.pop() : noop;
// 		queueName = args.join(_shr.sep);
// 		self._db.eval(function (col, queueName) {
// 			var chan = db[col];
// 			var doc = chan.findAndModify({ query: { name: queueName }, update: { $pop: { queue: -1 } }, fields: { queue: { $slice: 1 }, _id: 1 } });
// 			if (doc) {
// 				chan.update({ _id: doc._id }, { $set: { last: doc.queue[0] } });
// 				delete doc._id;
// 			}
// 			return doc ? doc.queue[0] || null : null;
// 		}, [self.name, queueName], { nolock: true }, callback);
// 	});
// 	return this;
// };

// Channel.prototype.restore = function (queueName, callback) {
// 	var self = this;
// 	var args = Array.prototype.slice.call(arguments);

// 	this._onReady(function () {
// 		callback = typeof args[args.length - 1] === 'function' ? args.pop() : noop;
// 		queueName = args.join(_shr.sep);
// 		self._db.eval(function (col, queueName) {
// 			var chan = db[col];
// 			var doc = chan.findAndModify({ query: { name: queueName }, update: { $set: { last: null } }, fields: { last: 1, _id: 1 } });
// 			if (doc) {
// 				chan.update({ _id: doc._id }, { $set: { last: doc.queue[0] } });
// 				delete doc._id;
// 			}
// 			return doc;
// 		}, [self.name, queueName], callback);
// 	});
// };

module.exports = Channel;