package com.neoris.stream.endpoint;

import java.io.IOException;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.CreateCollectionOptions;

import org.bson.Document;

/**
 * Created by jesus.yepiz on 11/28/2015.
 */
public class MongoEndPoint implements EndPoint {

    private final MongoDatabase mongoDB;
    private final MongoCollection mongoQueue;

    /*private final String mongoHost;
    private final int mongoPort;
    private final String mongoDbName;
    private final String mongoQueueName;*/

    public MongoEndPoint(String mongoHost, int mongoPort, String mongoDbName, String mongoQueueName) throws IOException {

        /*this.mongoHost = mongoHost;
        this.mongoPort = mongoPort;
        this.mongoDbName = mongoDbName;
        this.mongoQueueName = mongoQueueName;*/

        this.mongoDB = new MongoClient(mongoHost, mongoPort).getDatabase(mongoDbName);
        if(collectionExists(mongoQueueName)) {
            this.mongoDB.getCollection(mongoQueueName).drop();
        }
        CreateCollectionOptions options = new CreateCollectionOptions();
        options.capped(true);
        options.maxDocuments(1000);
        options.sizeInBytes(302400); //2000 tweets more or less
        this.mongoDB.createCollection(mongoQueueName, options);
        this.mongoQueue = this.mongoDB.getCollection(mongoQueueName);
    }

    @Override
    public void write(Object[] row) {
        Document doc = new Document();
        doc.append("dateTime",row[0]);
        doc.append("text",row[1]);
        this.mongoQueue.insertOne(doc);
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
