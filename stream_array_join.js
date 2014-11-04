console.time("exchange");

var mysql = require('mysql');
var arango = require('arango');
var stream = require('stream');
var util = require('util');

var stream;

function EncodeJSON(options) {
	if (!options) options = {};
	options.objectMode = true;
	
	if (!(this instanceof EncodeJSON)) {
		return new EncodeJSON(options);
	}
	
	stream.Transform.call(this, options);
	// set properties here, this.foo = ...
}
util.inherits(EncodeJSON, stream.Transform);


EncodeJSON.prototype._transform = function(obj, enc, cb) {
	var self = this;
	
	if (!obj) {
		this.push(null);
		cb();
		return;
	}
	
	// process obj here
	obj._key = "" + obj.findex;

	this.push(JSON.stringify(obj, function(key, value) {
			// ignore "findex" key and keys with value null, undefined or whitespace only
			if (value == null || key === "findex" || (typeof value === "string" && !value.trim())) {
				return undefined;
			} else {
				return value;
			}
		}
	));
	cb();
};

function quit() {
	// WARNING: some documents do not import!
	console.timeEnd("exchange");
	process.exit();
}
var docs = [];

function importArango(json, finish) {
	// buffer size makes very little difference in speed,
	// 31s @100, 29s @2000,
	// memory consumption is a bit higher with larger buffer
	if (docs.length < 10000 && !finish) {
		docs.push(json);
	} else {
		if (!finish) stream.pause();
		db.import.importJSONData(
			"fa_stammdaten_stream",
			docs.join("\n"),
			{
				createCollection: true,
				waitForSync: false
			},
			function (err, res) {
				if (err) {
					console.log(err);
				}
				docs = [];
				if (!finish) {
					stream.resume();
				} else {
					quit();
				}
			}
		);
	}
}


var records = 0;
var encoder = new EncodeJSON();

encoder
  .on('readable', function () {
	importArango(encoder.read(), false);
	records++;
  })
  .on('finish', function () {
	// write rest
    importArango("", true);
//  })
//  .on('finish', function() {
//	console.log("Records:", records);
  });



/*
var doc = [];
function processRow(row, connection) {
	if (docs.length < 1000) {
	
		// Duplicate primary key (and remove original pair later),
		// casting to string is required (_key is a string in ArangoDB!)
		row._key = "" + row.findex;
		
		docs.push(row);
	} else {
	    connection.pause();
		db.import.importJSONData(
			"fa_stammdaten",
			JSON.stringify(docs, function(key, value) {
				// ignore "findex" key and keys with value null, undefined or whitespace only
				if (value == null || key === "findex" || (typeof value === "string" && !value.trim())) {
					return undefined;
				} else {
					return value;
				}
			}),
			{
				createCollection: true,
				waitForSync: false
			},
			function(err, ret) {
				connection.resume();
				docs = [];
			}
		);
	}
}
*/

var connection = mysql.createConnection({
	host: 'localhost',
	user: 'root',
	password: ''
});

var db = arango.Connection("http://localhost:8529/amtub");

console.log("Connecting to MySQL");
connection.connect();

var query = connection.query('SELECT * FROM _import_mysql.fa_stammdaten');

stream = query.stream();
stream.pipe(encoder);

/*
	.on('error', function(err) {
		console.log(err);
	})
	.on('fields', function(fields) {
		//console.log(fields);
	})
	.on('result', function(row) {
		i++;
		if (i % 1000 == 0) console.log(i);
		
		processRow(row, connection);
		
	})
	.on('end', function() {
		console.log("end");
		console.timeEnd("exchange");
		process.exit();
	});
*/