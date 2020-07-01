var MongoClient = require('mongodb').MongoClient;
var path = require('path');
var async = require('async');
var _ = require('underscore');

var Channel = require('./Channel');

var config = {
	url: 'mongodb://localhost:27017',
	db: 'jobs',
	options: {
    useUnifiedTopology: true,
    w: 1,
    // native_parser: false,
    // poolSize: 5, // default
	},
	local: 'local',
	sep: '##',
};

var mq = (function () {
  _client = null;
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

	mq.configure = function (cnf, callback) {
		if (_openned === true)
			return;
		_openned = true;
		_.extend(config, cnf);
		Channel.set('sep', config.sep);
		Channel.set('local', config.local);
		async.waterfall([
			function (next) {
				MongoClient.connect(config.url, config.options, next);
			},
			function (client, next) {
        _client = client;
        _db = client.db(config.db);
        next(null)
			}
		], function (err) {
			if (err) {
				throw new Error('Cannot connect to ' + config.url);
			}
			for (var chanName in _chan) {
				_chan[chanName]._connect(_db);
      }
      callback && callback(err)
		})
		return mq;
	};

	mq.close = function (callback) {
		_client.close(callback);
		_openned = false;
		return mq;
	};

	return mq;
}).call({});


module.exports = mq;
