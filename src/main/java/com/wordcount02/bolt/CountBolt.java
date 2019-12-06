package com.wordcount02.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * @author maxwell
 * @Description: 计数bolt
 * @date 2019/12/6 15:21
 */
public class CountBolt extends BaseRichBolt {

    private OutputCollector collector;
    private Map<String, Integer> map = new HashMap<>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");

        if(map.containsKey(word)){
            map.put(word, map.get(word)+1);
        }else {
            map.put(word, 1);
        }

        this.collector.emit(new Values(word , map.get(word)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //
        outputFieldsDeclarer.declare(new Fields("word","count"));
    }
}
