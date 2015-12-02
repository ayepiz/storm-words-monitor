package com.neoris.stream;

import com.neoris.stream.endpoint.MongoEndPoint;
import twitter4j.*;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by jesus.yepiz on 11/28/2015.
 */
public class Twitter {

    public static void main( String[] args ) throws TwitterException, IOException
    {
        Properties props = new Properties();
        props.load(Twitter.class.getClassLoader().getResourceAsStream("storm-words-monitor.properties"));

        MongoEndPoint queue = new MongoEndPoint(props.getProperty("mongoHost"),Integer.parseInt(props.getProperty("mongoPort")),
                props.getProperty("mongoDBName"), props.getProperty("mongoTwitterQueueCollection"));

        StatusListener listener = new StatusListener() {
            public void onStatus(Status status) {
                //System.out.println(status.getUser().getName() + " : " + status.getText());
                queue.write(new Object[] { status.getCreatedAt(), status.getText()});
            }
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
            public void onException(Exception ex) {
                ex.printStackTrace();
                queue.closeCnn();
            }
            public void onScrubGeo(long l, long l1) {}
            public void onStallWarning(StallWarning arg0) {}
        };

        TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(listener);

        //twitterStream.sample();
        FilterQuery tweetFilterQuery = new FilterQuery();
        tweetFilterQuery.track(props.getProperty("topics").split(";")); // OR on keywords
        tweetFilterQuery.language(new String[]{"en","es"});
        twitterStream.filter(tweetFilterQuery);
    }
}
