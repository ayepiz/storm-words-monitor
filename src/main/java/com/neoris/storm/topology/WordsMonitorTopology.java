package com.neoris.storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.neoris.storm.bolt.MongoWriterBolt;
import com.neoris.storm.bolt.RollingCountBolt;
import com.neoris.storm.bolt.ExtractPhrasesBolt;
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

        builder.setBolt("PhrasesExtracter", new ExtractPhrasesBolt(props.getProperty("phrases").split(";")),1).shuffleGrouping("TextEmitter");

        builder.setBolt("PhrasesCounter", new RollingCountBolt(60,10), 2).fieldsGrouping("PhrasesExtracter", new Fields("phrase"));

        builder.setBolt("MongoStorer", new MongoWriterBolt(props.getProperty("mongoHost"),Integer.parseInt(props.getProperty("mongoPort")),
                props.getProperty("mongoDBName"), props.getProperty("mongoTickerCollection")), 1).globalGrouping("PhrasesCounter");

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());

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
        Utils.sleep(60000);
        cluster.killTopology("test");
        cluster.shutdown();
    }

}
