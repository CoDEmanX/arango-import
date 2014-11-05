console.time("exchange");

var mysql = require('mysql');
var arango = require('arango');
var child_process = require('child_process');
var json_transform = require('./json_transform_stream');

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

var encoder = new json_transform.stream();

var query_stream = query.stream();
query_stream.pipe(encoder).pipe(arangoimp.stdin);

