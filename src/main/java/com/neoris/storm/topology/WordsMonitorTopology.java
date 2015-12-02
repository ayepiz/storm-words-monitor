package com.neoris.storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.neoris.storm.bolt.MongoWriterBolt;
import com.neoris.storm.bolt.RollingCountBolt;
import com.neoris.storm.bolt.ExtractTopicsBolt;
import com.neoris.storm.spout.MongoSpout;
import com.neoris.stream.Twitter;
import twitter4j.TwitterException;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by jesus.yepiz on 11/29/2015.
 */
public class WordsMonitorTopology {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.load(WordsMonitorTopology.class.getClassLoader().getResourceAsStream("storm-words-monitor.properties"));

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("TextEmitter", new MongoSpout(props.getProperty("mongoHost"),Integer.parseInt(props.getProperty("mongoPort")),
                props.getProperty("mongoDBName"), props.getProperty("mongoTwitterQueueCollection")), 1);

        builder.setBolt("TopicsExtracter", new ExtractTopicsBolt(props.getProperty("topics").split(";")),1).shuffleGrouping("TextEmitter");

        builder.setBolt("TopicsCounter", new RollingCountBolt(600,5), 2).fieldsGrouping("TopicsExtracter", new Fields("topic"));

        builder.setBolt("MongoStorer", new MongoWriterBolt(props.getProperty("mongoHost"),Integer.parseInt(props.getProperty("mongoPort")),
                props.getProperty("mongoDBName"), props.getProperty("mongoTickerCollection")), 1).globalGrouping("TopicsCounter");

        String topologyName = "TopicsMonitorTopology";
        String runAs = "local";

        if(args != null && args.length > 0) {
        	topologyName = args[0];
        	if(args[1] != null)
        		runAs = args[1];
        }

        if(runAs == "remote") {
        	submitRemotely(builder, topologyName);
        } else {
        	submitLocally(builder, topologyName);
        }
    }
    
    private static void submitRemotely(TopologyBuilder builder, String name) throws Exception {
        Config conf = new Config();
        conf.setDebug(false);
        
        StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
    }
    
    private static void submitLocally(TopologyBuilder builder, String name) {
        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(name, conf, builder.createTopology());

        Runnable writer = new Runnable() {

            @Override
            public void run() {
                try {
                    Twitter.main(new String[0]);
                } catch(IOException ex) {

                } catch(TwitterException ex) {

                }
            }
        };

        new Thread(writer).start();
        Utils.sleep(120000);
        cluster.killTopology(name);
        cluster.shutdown();    	
    }

}
