var mongodb = require('mongodb');
var async = require('async');
var _ = require('underscore');

var Channel = require('./Channel');

var config = {
	host: 'localhost',
	port: 27017,
	user: '',
	password: '',
	options: {
		auto_reconnect: true,
		native_parser : false
	},
	db: 'jobs',
	local: 'local',
	sep: '##'
};

var mq = (function () {
	_db = null;
	_chan = {};
	_openned = false;

	function mq(chanName) {
		if (!(_db instanceof mongodb.Db)) {
			mq.configure(config);
		}
		if (!(_chan[chanName] instanceof Channel)) {
			_chan[chanName] = new Channel(chanName, _db);
			_chan[chanName]._connect();
		}
		return _chan[chanName];
	}

	mq.configure = function (cnf) {
		if (_db instanceof mongodb.Db && _openned === true) {
			throw new Error('configure method cannot be called multiple times');
		}
		_.extend(config, cnf);
		Channel.set('sep', config.sep);
		Channel.set('local', config.local);
		var server = new mongodb.Server(config.host, config.port, { auto_reconnect: true, poolSize: 10 });
		var client = new mongodb.MongoClient(server, { w: 1, native_parser: true });
		_openned = true;
		client.open(function (err, client) {
			_db = mongoClient.db(database);
			if (err)
				return _db.emit('ready', err);
			db.open(function(err, db) {
				_db = db;
				if (!config.user) return _db.emit('ready', err);
				_db.authenticate(config.user, config.password, function (err, connected) {
					if (err || !connected)
						throw new Error('Cannot connect to '+config.db+' database, bad user/password.');
					else
						_db.emit('ready', err);
				});
			});
		});
		return mq;
	};

	mq.close = function (callback) {
		_db.close(callback);
		_openned = false;
		return mq;
	};

	return mq;
}).call({});


module.exports = mq;
