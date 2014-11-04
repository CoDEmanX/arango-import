console.time("exchange");

var mysql = require('mysql');
var arango = require('arango');

var docs = [];

function processRow(row, connection, quit) {
	
	if (row instanceof Object) {
		// Duplicate primary key (and remove original pair later),
		// casting to string is required (_key is a string in ArangoDB!)
		row._key = "" + row.findex;
		docs.push(row);
	}
	
	if (docs.length >= 1000 || row === undefined) {
	    connection.pause();
		db.import.importJSONData(
			"fa_stammdaten_array_buffer",
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
				if (typeof quit == "function") quit();
			}
		);
	}
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
		console.log("Fields:", Object.keys(fields).length);
	})
	.on('result', function(row) {
		if (++i % 1000 == 0) console.log(i);

		processRow(row, connection);
	})
	.on('end', function() {
		processRow(undefined, connection, function(){query.emit('finish')});
	})
	.on('finish', function() {
		console.timeEnd("exchange");
		process.exit();
	});
