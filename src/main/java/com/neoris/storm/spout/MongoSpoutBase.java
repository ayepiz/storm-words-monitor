package com.neoris.storm.spout;

/**
 * Created by jesus.yepiz on 11/28/2015.
 * The base of this code is taken from https://github.com/stormprocessor/storm-mongo.git
 * It was converted to use mongodb driver 3.0
 */

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.utils.Utils;

import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.CursorType;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.MongoClient;

import org.bson.Document;

/**
 * A Spout which consumes documents from a Mongodb tailable cursor.
 * <p>
 * Subclasses should simply override two methods:
 * <ul>
 * <li>{@link #declareOutputFields(OutputFieldsDeclarer) declareOutputFields}
 * <li>{@link #dbObjectToStormTuple(Document) dbObjectToStormTuple}, which turns
 * a Mongo document into a Storm tuple matching the declared output fields.
 * </ul>
 * <p>
 * * <p>
 * <b>WARNING:</b> You can only use tailable cursors on capped collections.
 *
 * @author Dan Beaulieu <danjacob.beaulieu@gmail.com>
 */
public abstract class MongoSpoutBase extends BaseRichSpout {

    private SpoutOutputCollector collector;

    private LinkedBlockingQueue<Document> queue;
    private final AtomicBoolean opened = new AtomicBoolean(false);

    private MongoDatabase mongoDB;

    private final String mongoHost;
    private final int mongoPort;
    private final String mongoDbName;
    private final String mongoCollectionName;


    public MongoSpoutBase(String mongoHost, int mongoPort, String mongoDbName, String mongoCollectionName) {

        this.mongoHost = mongoHost;
        this.mongoPort = mongoPort;
        this.mongoDbName = mongoDbName;
        this.mongoCollectionName = mongoCollectionName;
    }

    class TailableCursorThread extends Thread {

        LinkedBlockingQueue<Document> queue;
        String mongoCollectionName;
        MongoDatabase mongoDB;

        public TailableCursorThread(LinkedBlockingQueue<Document> queue, MongoDatabase mongoDB, String mongoCollectionName) {

            this.queue = queue;
            this.mongoDB = mongoDB;
            this.mongoCollectionName = mongoCollectionName;
        }

        public void run() {

            while (opened.get()) {
                try {
                    // create the cursor
                    final MongoCursor<Document> cursor = mongoDB.getCollection(mongoCollectionName)
                            .find()
                            .sort(new Document("$natural",1))
                            .cursorType(CursorType.TailableAwait)
                            .iterator();
                    try {
                        while (opened.get() && cursor.hasNext()) {
                            final Document doc = cursor.next();

                            if (doc == null) break;

                            queue.put(doc);
                        }
                    } finally {
                        try {
                            if (cursor != null) cursor.close();
                        } catch (final Throwable t) {
                        }
                    }

                    Utils.sleep(500);
                } catch (final MongoCursorNotFoundException cnf) {
                    // rethrow only if something went wrong while we expect the cursor to be open.
                    if (opened.get()) {
                        throw cnf;
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

        this.collector = collector;
        this.queue = new LinkedBlockingQueue<Document>(1000);
        try {
            this.mongoDB = new MongoClient(this.mongoHost, this.mongoPort).getDatabase(this.mongoDbName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        TailableCursorThread listener = new TailableCursorThread(this.queue, this.mongoDB, this.mongoCollectionName);
        this.opened.set(true);
        listener.start();
    }

    @Override
    public void close() {
        this.opened.set(false);
    }

    @Override
    public void nextTuple() {

        Document dbo = this.queue.poll();
        if (dbo == null) {
            Utils.sleep(50);
        } else {
            this.collector.emit(dbObjectToStormTuple(dbo));
        }
    }

    @Override
    public void ack(Object msgId) {
        // TODO Auto-generated method stub
    }

    @Override
    public void fail(Object msgId) {
        // TODO Auto-generated method stub
    }

    public abstract List<Object> dbObjectToStormTuple(Document message);

}