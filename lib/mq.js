var MongoClient = require('mongodb').MongoClient;
var path = require('path');
var async = require('async');
var _ = require('underscore');

var Channel = require('./Channel');

var config = {
	url: 'mongodb://localhost:27017',
	dbName: 'jobs',
	replicaSet: '',
	options: {
		db: {
			w: 1,
			native_parser : false,
		},
		server: {
			poolSize: 5, //default
		}
	},
	local: 'local',
	sep: '##',
};

var mq = (function () {
	_db = null;
	_chan = {};
	_openned = false;

	function mq(chanName) {
		if (!_db)
			mq.configure(config);
		if (!(_chan[chanName] instanceof Channel)) {
			_chan[chanName] = new Channel(chanName);
			if (_db)
				_chan[chanName]._connect(_db);
		}
		return _chan[chanName];
	}

	mq.configure = function (cnf) {
		if (_openned === true)
			return;
		_openned = true;
		_.extend(config, cnf);
		Channel.set('sep', config.sep);
		Channel.set('local', config.local);
		var URI = (_.last(config.url) !== '/' ? config.url + '/' : config.url) + config.dbName + (config.replicaSet ? '?replicaSet=' + config.replicaSet : '');
		MongoClient.connect(URI, config.options, function (err, db) {
			if (err) {
				throw new Error('Cannot connect to '+URI);
				return;
			}
			_db = db;
			for (var chanName in _chan) {
				_chan[chanName]._connect(_db);
			}
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