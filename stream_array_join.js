console.time("exchange");

var mysql = require('mysql');
var arango = require('arango');
var stream = require('stream');
var util = require('util');


// Transform stream

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


var docs = [];

function importArango(json, cb) {
	
	if (typeof json != "undefined")
		docs.push(json);
		
	// buffer size makes very little difference in speed,
	// 31s @100, 29s @2000,
	// memory consumption is a bit higher with larger buffer
	if (docs.length >= 10000 || typeof json == "undefined") {
		stream.pause();
		db.import.importJSONData(
			"fa_stammdaten_stream_array_join",
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
				stream.resume();
				if (typeof cb == "function") cb();
			}
		);
	}
}


var encoder = new EncodeJSON();

encoder
	.on('readable', function () {
		importArango(encoder.read());
	})
	.on('end', function () {
		// write rest
		importArango(undefined, function() {
			console.timeEnd("exchange");
			process.exit();
		});
	});


var connection = mysql.createConnection({
	host: 'localhost',
	user: 'root',
	password: ''
});

var db = arango.Connection("http://localhost:8529/amtub");

console.log("Connecting to MySQL");
connection.connect();

var query = connection.query('SELECT * FROM _import_mysql.fa_stammdaten');

var stream = query.stream();
stream.pipe(encoder);

