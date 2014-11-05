console.time("migration");

var mysql = require('mysql');
var child_process = require('child_process');
var os = require('os');

var options = {
    mySqlSource: "_import_mysql.fa_stammdaten",
    mySqlConnect: {
        host: 'localhost',
        user: 'root',
        password: ''
    },
    arangoConnect: "http://localhost:8529/amtub",
    arangoImp: "D:/Webserver/arangodb/bin/arangoimp",
    arangoArgs: [
        "--server.database", "amtub",
        "--collection", "fa_stammdaten_stream_arangoimp",
        "--create-collection", "true",
        "--progress", "true", // doesn't print anything when streaming file
        "--type", "json",
        "--file", "-" // read from stdin
    ],
    numJobs: os.cpus().length
}


function getRecordOffsets(callback) {
    var offsets = [];
    var connection = mysql.createConnection(options.mySqlConnect);
    //connection.connect(...);
    connection.query('SELECT COUNT(*) AS count FROM ' + options.mySqlSource, function(err, rows, fields) {
        if (err) {
            console.log("Error", err);
            process.exit();
        }
        var rowCount = rows[0].count;
        var batchSize = rowCount / options.numJobs | 0;
        
        console.log("Total rows:", rowCount);
        
        for (var j = 0; j < options.numJobs; j++) {
            offsets.push({
                offset: batchSize * j,
                limit: batchSize
            });
        }
        
        // select all remaining rows by using the (almost) highest number
        // http://stackoverflow.com/questions/255517/mysql-offset-infinite-rows
        offsets[offsets.length-1].limit = Math.pow(2,63);
        
        connection.end();
        callback(offsets);
    });
}

function startJobs(offsets) {
    var running = 0;
    for (var j = 0; j < options.numJobs; j++) {;
        var child = child_process.fork(__dirname + "/worker");
        child.on('message', function(msg) {
            if (msg.type == 'done') {
                console.log(msg.num + ':', msg.type + ', exit code', msg.code)
                running--;
                if (running <= 0) {
                    console.log("\nAll done.");
                    console.timeEnd("migration");
                    process.exit();
                }
            } else {
                console.log(msg.num + ':', msg.type);
            }
        });
        child.send({
            num: j,
            type: 'start',
            offset: offsets[j].offset,
            limit: offsets[j].limit,
            options: options
        });
        running++;
    }
}

getRecordOffsets(startJobs);
