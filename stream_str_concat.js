console.time("exchange");

var mysql = require('mysql');
var arango = require('arango');
var stream = require('stream');
var util = require('util');

// Transform stream

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


var str = "";
var c = 0;

function importArango(json, cb) {
	if (json) {
		str += json + "\n";
		c++;
	}
		
	//if (c >= 1000 || !json) {
	// buffer approx. 5 MB
	if (str.length > 5000000 || !json) {
		c = 0;
		stream.pause();
		db.import.importJSONData(
			"fa_stammdaten_stream_str_concat",
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
				cb();
			}
		);
	}
}

function noop(){}

var encoder = new EncodeJSON();

encoder
	.on('readable', function() {
		importArango(encoder.read(), noop);
	})
	.on('end', function() {
		// write rest
		importArango("", function() {
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
