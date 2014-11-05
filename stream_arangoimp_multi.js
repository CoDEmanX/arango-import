console.time("exchange");

var mysql = require('mysql');
var arango = require('arango');
var child_process = require('child_process');
var async = require("async");
var json_transform = require('./json_transform_stream');

var source = "_import_mysql.fa_stammdaten";
var num_jobs = 4;

var mysql_credentials = {
    host: 'localhost',
    user: 'root',
    password: ''
}

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


function getRecordCount(callback) {
    var offsets = [];
    var row_count;
    var count;
    var connection = mysql.createConnection(mysql_credentials);
    //connection.connect(...);
    connection.query('SELECT COUNT(*) AS count FROM ' + source, function(err, rows, fields) {
        if (err) {
            console.log("Error", err);
            process.exit();
        }
        row_count = rows[0].count;
        console.log("Total rows:", row_count);
        count = row_count / num_jobs | 0;
        for (var o = 0; o < num_jobs; o++) {
            // select all remaining rows by using the (almost) highest number
            // http://stackoverflow.com/questions/255517/mysql-offset-infinite-rows
            offsets.push("" + (count * o) + ',' + ((o == num_jobs - 1) ? Math.pow(2,63) : count));
        }
        connection.end();
        callback(offsets);
    });
}

function startMigration(offsets) {
    async.each(offsets,
        function(offset, callback){
            console.log("Migrating", offset);
            migrate(offset, callback);
        },
        function(err){
            if (err) console.log("Error:", err);
            else console.log("\nAll done.");
            console.timeEnd("exchange");
            process.exit();
        }
    );
}

function migrate(offset, callback) {

    //console.log("Connecting to ArangoDB");
    var db = arango.Connection("http://localhost:8529/amtub");

    //console.log("Connecting to MySQL");
    var connection = mysql.createConnection(mysql_credentials);
    //connection.connect(...);
    
    // if multiple processes pipe to main process, console log will be messed up, thus ignoring
    var arangoimp = child_process.spawn("arangoimp", args, {stdio: ['pipe', 'ignore', 'ignore']});
    arangoimp
        .on('exit', function(code, signal) {
            console.log("\nexit", code, signal);
            connection.end(); // close MySQL connection
            callback();
        });

    var encoder = new json_transform.stream();
    var query = connection.query('SELECT * FROM ' + source + ' LIMIT ' + offset);
    var query_stream = query.stream();
    query_stream.pipe(encoder).pipe(arangoimp.stdin);
}

// Run
getRecordCount(startMigration);
