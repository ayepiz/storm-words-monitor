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

/*
var connect = require('connect');
var mongo = require("mongodb");
//var mubsub = require('mubsub');
var ticketSrv;

var mongodbUri = "mongodb://10.26.69.75/Storm_Words_Monitor";

function ticker(req, res, next) {
	console.log(req);
	return next();
	if(req.query.phrase === undefined) {
		return next();	
	}
	
    mongo.MongoClient.connect(mongodbUri, function(err, db) {

        db.collection("Ticker", function(err, collection) {

                if(err) {
                        console.log({err:err});
                        return err;
                }

                console.log("== open tailable cursor");
                
                var phrase = req.query.phrase;

                var stream = collection.find({ "phrase" : phrase }, {tailable:true, awaitdata: true, sortValue: {$natural: 1}, numberOfRetries: Number.MAX_VALUE, tailableRetryInterval: 200}).stream();

                stream.on('data', function(doc) {
                	res.json(doc); 
                });
        });
    });
    
	 //send headers for event-stream connection
	 res.writeHead(200, {
		 'Content-Type': 'text/event-stream',
		 'Cache-Control': 'no-cache',
		 'Connection': 'keep-alive'
	 });
	 
	 res.json = function(obj) { res.write("data:"+JSON.stringify(obj)+"\n\n"); };
	 
	 res.json({});
	 
}

ticketSrv = connect();
ticketSrv.use("/ticker",ticker);
ticketSrv.listen(8081);
*/

/*
http.createServer(function handler(req, res) {
    res.writeHead(200, {'Content-Type': 'text/plain'});
    res.end('Hello World\n');
    
    mongo.MongoClient.connect(mongodbUri, function(err, db) {

        db.collection("Ticker", function(err, collection) {

                if(err) {
                        console.log({err:err});
                        return err;
                }

                console.log("== open tailable cursor");

                var stream = collection.find({}, {tailable:true, awaitdata: true, sortValue: {$natural: 1}, numberOfRetries: Number.MAX_VALUE, tailableRetryInterval: 200}).stream();

                stream.on('data', function(doc) {
                        //console.log(doc);
                	 res.writeHead(200, {
                		 'Content-Type': 'text/event-stream',
                		 'Cache-Control': 'no-cache',
                		 'Connection': 'keep-alive'
                		 });
                	 
                	 res.json = doc;
                });
        });
    });
    
}).listen(1337, '127.0.0.1');
console.log('Server running at http://127.0.0.1:1337/');
*/
/*
function ticker(req, res) {
	
	var client = mubsub(mongodbUri);
	var channel = client.channel('Ticker');
	
	channel.on('error', console.error);
	client.on('error', console.error);
	
	 //send headers for event-stream connection
	 res.writeHead(200, {
		 'Content-Type': 'text/event-stream',
		 'Cache-Control': 'no-cache',
		 'Connection': 'keep-alive'
	 });
	 
	 res.json = function(obj) { res.write("data:"+obj+"\n\n"); };
	 
	 res.json(JSON.stringify({}));
	 
	 req.on("close", function() {
		 client.close();
	 });
	
	channel.subscribe('message',function(msg) {
		res.json(msg); 
	});
}
*/
