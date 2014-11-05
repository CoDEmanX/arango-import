console.time("exchange");

var mysql = require('mysql');
var arango = require('arango');
var stream = require('stream');
var util = require('util');
var child_process = require('child_process');


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
	
	// process obj here (e.g. make primary key to arango _key)
	obj._key = "" + obj.findex;

	this.push(JSON.stringify(obj, function(key, value) {
			if (value == null || // ignore NULL'd values
                key === "findex" || // ignore a certain key
                (typeof value === "string" && !value.trim())) // ignore if whitespace only
            {
				return undefined;
			} else {
				return value;
			}
		}
	));
    // important: arangoimp expects line break after document,
    // or it will assume an array otherwise (and import thus fail).
    // No performance difference whether push() is called twice or
    // line break is appended (json+"\n") in a single call
    this.push("\n");
	cb();
};


var connection = mysql.createConnection({
	host: 'localhost',
	user: 'root',
	password: ''
});

var db = arango.Connection("http://localhost:8529/amtub");

console.log("Connecting to MySQL");
connection.connect();

var query = connection.query('SELECT * FROM _import_mysql.fa_stammdaten');

var args = [
    "--server.database", "amtub",
    "--collection", "fa_stammdaten_stream_arangoimp",
    "--create-collection", "true",
    "--progress", "true", // doesn't print anything when streaming file
    "--type", "json",
    "--file", "-"
]

// set path to arangodb binaries here
process.chdir("D:/Webserver/arangodb/bin");

var arangoimp = child_process.spawn("arangoimp", args, {stdio: ['pipe', process.stdout, process.stdout]});
arangoimp
    .on('exit', function(code, signal) {
        console.log("\nexit", code, signal);
        console.timeEnd("exchange");
        process.exit();
    });

var fs = require('fs');

var encoder = new EncodeJSON();
    
var query_stream = query.stream();
query_stream.pipe(encoder).pipe(arangoimp.stdin);

