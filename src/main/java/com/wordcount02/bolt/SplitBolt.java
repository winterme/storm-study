package com.wordcount02.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author maxwell
 * @Description: 消息切割
 * @date 2019/12/6 15:13
 */
public class SplitBolt extends BaseRichBolt {

    private Logger logger = LoggerFactory.getLogger(SplitBolt.class);

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String words = tuple.getStringByField("words");
        Pattern pattern = Pattern.compile("\\S+");
        Matcher matcher = pattern.matcher(words);

        while (matcher.find()){
            String s = matcher.group();
            logger.info("split bolt 发送消息成功！===>" + s );
            // 发送单词
            this.collector.emit(new Values(s));
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // 流转
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
