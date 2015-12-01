package com.neoris.storm.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.CreateCollectionOptions;
import org.bson.Document;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * Created by jesus.yepiz on 11/30/2015.
 */
public class MongoWriterBolt  extends BaseRichBolt {
    OutputCollector _collector;
    private MongoDatabase mongoDB;
    private MongoCollection<Document> mongoCollection;
    private final String mongoHost;
    private final int mongoPort;
    private final String mongoDbName;
    private final String mongoQueueName;
    //private final DateFormat dateFormat;

    public MongoWriterBolt(String mongoHost, int mongoPort, String mongoDbName, String mongoQueueName) {
        this.mongoHost = mongoHost;
        this.mongoPort = mongoPort;
        this.mongoDbName = mongoDbName;
        this.mongoQueueName = mongoQueueName;
        //this.dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.S");
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        this.mongoDB = new MongoClient(this.mongoHost, this.mongoPort).getDatabase(this.mongoDbName);
        if(collectionExists(this.mongoQueueName)) {
            this.mongoDB.getCollection(mongoQueueName).drop();
        }
        CreateCollectionOptions options = new CreateCollectionOptions();
        options.capped(true);
        options.maxDocuments(1000);
        options.sizeInBytes(302400); //2000 tweets more or less
        this.mongoDB.createCollection(this.mongoQueueName, options);
        this.mongoCollection = this.mongoDB.getCollection(this.mongoQueueName);
    }

    @Override
    public void execute(Tuple tuple) {
        Document doc = new Document();
        doc.append("phrase",tuple.getValue(0));
        doc.append("time",tuple.getValue(1));
        doc.append("count",tuple.getValue(2));
        doc.append("dateTime",tuple.getValue(3));
        this.mongoCollection.insertOne(doc);
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    private boolean collectionExists(final String collectionName) {
        MongoIterable<String> collectionNames = this.mongoDB.listCollectionNames();
        for (final String name : collectionNames) {
            if (name.equalsIgnoreCase(collectionName)) {
                return true;
            }
        }
        return false;
    }
}