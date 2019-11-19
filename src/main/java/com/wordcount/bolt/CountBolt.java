package com.wordcount.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class CountBolt extends BaseBasicBolt {

    private static Logger loger = LoggerFactory.getLogger(CountBolt.class);

    private HashMap<String, Integer> map = new HashMap<>();
    private int count;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String word = tuple.getStringByField("word");

        loger.info("count bolt 发出消息===>" + word + "==>1");
        // 输出
        basicOutputCollector.emit(new Values(word, 1));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word", "count"));
    }
}
