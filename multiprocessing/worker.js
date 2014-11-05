var mysql = require('mysql');
var arango = require('arango');
var child_process = require('child_process');
var json_transform = require('../json_transform_stream');

process.on('message', function(msg) {
    switch (msg.type) {
        case 'start':
            migrate(msg.offset, msg.limit, msg.options, function(code) {
                process.send({
                    num: msg.num,
                    type: 'done',
                    code: code
                })
            });
            process.send({
                num: msg.num,
                type: 'started'
            });
            break;
        // ...
    }
});

function migrate(offset, limit, options, callback) {
    //console.log("Connecting to ArangoDB");
    var db = arango.Connection(options.arangoConnect);

    //console.log("Connecting to MySQL");
    var connection = mysql.createConnection(options.mySqlConnect);
    //connection.connect(...);
    
    var arangoimp = child_process.spawn(
        options.arangoImp,
        options.arangoArgs,
        {stdio: ['pipe', 'ignore', 'ignore']}
    );
    arangoimp.on('exit', function(code, signal) {
        connection.end(); // close MySQL connection
        callback(code);
    });

    var encoder = new json_transform.stream();
    var query = connection.query('SELECT * FROM ' + options.mySqlSource + ' LIMIT ' + offset + ',' + limit);
    var query_stream = query.stream();
    query_stream.pipe(encoder).pipe(arangoimp.stdin);
}
