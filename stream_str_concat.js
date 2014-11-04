console.time("exchange");

var mysql = require('mysql');
var arango = require('arango');
var stream = require('stream');
var util = require('util');

var stream;

function EncodeJSON(options) {
	if (!(this instanceof EncodeJSON)) {
		return new EncodeJSON(options);
	}
	
	if (!options) options = {};
	options.objectMode = true;
	stream.Transform.call(this, options);
	// set properties here, this.foo = ...
}
util.inherits(EncodeJSON, stream.Transform);


EncodeJSON.prototype._transform = function(obj, enc, cb) {
	var self = this;
	
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
	console.timeEnd("exchange");
	process.exit();
}
var str = "";

function importArango(json, finish) {
	if (docs.length < 1000 && !finish) {
		str += json + "\n";
	} else {
		stream.pause();
		// NOTE: collection and docs appear after import is finished,
		// seems to be related to string concatenation...
		db.import.importJSONData(
			"fa_stammdaten_stream",
			str,
			{
				createCollection: true,
				waitForSync: false
			},
			function (err, res) {
				if (err) {
					console.log(err);
				}
				str = "";
				stream.resume();
				if (finish) quit();
			}
		);
	}
}


var encoder = new EncodeJSON();

encoder
  .on('readable', function () {
	importArango(encoder.read(), false);
  })
  .on('finish', function () {
	// write rest
    importArango(encoder.read(), true);
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