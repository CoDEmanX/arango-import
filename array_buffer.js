console.time("exchange");

var mysql = require('mysql');
var arango = require('arango');

var docs = [];

function processRow(row, connection /*resume*/) {
	if (docs.length < 1000 && row !== false) {
	
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
				docs = [];
				connection.resume();
				if (row === false) quit();
			}
		);
	}
}

function quit() {
	console.timeEnd("exchange");
	process.exit();
}

var connection = mysql.createConnection({
	host: 'localhost',
	user: 'root',
	password: ''
});

var db = arango.Connection("http://localhost:8529/amtub");

console.log("Connecting to MySQL");
connection.connect();

var query = connection.query('SELECT * FROM _import_mysql.fa_stammdaten');
var i = 0;

query
	.on('error', function(err) {
		console.log(err);
	})
	.on('fields', function(fields) {
		//console.log(fields);
	})
	.on('result', function(row) {
		i++;
		if (i % 1000 == 0) console.log(i);
		
		/*
		connection.pause();
		
		processRow(row, function() {
			connection.resume();
		});*/
		
		processRow(row, connection);
		
	})
	.on('end', function() {
		processRow(false, connection);
	});
