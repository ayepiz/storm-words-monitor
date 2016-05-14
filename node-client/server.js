var app = require('express')();
var http = require('http').Server(app);
var io = require('socket.io')(http);
var path = require('path');
var mongoClient = require('mongodb').MongoClient;
var mongodbUri = "mongodb://10.26.69.75/Storm_Words_Monitor";

app.get('/', function(req, res) {
	res.sendFile(path.join(__dirname,'public/index.html'));
});

io.on('connection', function(socket) {
	
	socket.on('monitorTopic', function(newTopic) {
		
		if(socket.topic !== undefined) {
			socket.leave(socket.topic);
			//console.log('Leaves: ' + socket.topic);
		}
		
		socket.join(newTopic);
		//console.log('Joins: ' + socket.topic);
		socket.topic = newTopic;
		
	});
});

http.listen(8081, function() {
	console.log('listening on *:8081');
});

mongoClient.connect(mongodbUri, function(err, db) {

    db.collection("Ticker", function(err, collection) {

            if(err) {
                    console.log({err:err});
                    return err;
            }

            console.log("== open tailable cursor");
            
            var stream = collection.find({ }, {tailable:true, awaitdata: true, sortValue: {$natural: 1}, numberOfRetries: Number.MAX_VALUE, tailableRetryInterval: 200}).stream();

            stream.on('data', function(data) {
            	//console.log(data.topic + ': ' + JSON.stringify(data));
            	io.sockets.in(data.topic).emit('newDataPoint',data);
            });
    });
});
