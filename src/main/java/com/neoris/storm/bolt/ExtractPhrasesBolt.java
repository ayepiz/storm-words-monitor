package com.neoris.storm.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by jesus.yepiz on 11/29/2015.
 */
public class ExtractPhrasesBolt  extends BaseRichBolt {
    OutputCollector _collector;
    private String[] phrases;

    public ExtractPhrasesBolt(String[] phrases) {
        this.phrases = phrases;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String text = tuple.getString(0);
        for (String phrase : this.phrases) {
            if(text.toLowerCase().contains(phrase.toLowerCase())) {
                _collector.emit(tuple, new Values(phrase));
            }
            //System.out.println(item);
        }
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("phrase"));
    }
}