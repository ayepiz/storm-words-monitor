package com.neoris.storm.spout;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.List;
import org.bson.Document;
/**
 * Created by jesus.yepiz on 11/29/2015.
 */
public class MongoSpout extends MongoSpoutBase {
	private static final long serialVersionUID = 2000000L;

    public MongoSpout(String mongoHost, int mongoPort, String mongoDbName, String mongoCollectionName) {
        super(mongoHost, mongoPort, mongoDbName, mongoCollectionName);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("text"));
    }

    @Override
    public List<Object> dbObjectToStormTuple(Document document) {
        return new Values(document.getString("text"));
    }
}
