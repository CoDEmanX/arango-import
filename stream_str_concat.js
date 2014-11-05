console.time("exchange");

var mysql = require('mysql');
var arango = require('arango');
var json_transform = require('./json_transform_stream');

var str = "";
var c = 0;

function importArango(json, cb) {
	if (json) {
		str += json;
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

var encoder = new json_transform.stream();

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
