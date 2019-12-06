package com.wordcount02.spout;

import com.util.MyTimeUtil;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 消息发送
 */
public class WordSpout extends BaseRichSpout {

    private Logger logger = LoggerFactory.getLogger(WordSpout.class);

    private SpoutOutputCollector collector;

    private int index = 0;

    private static ArrayList<String> data = new ArrayList<>();

    static {
        data.add("my name is zhangzq");
        data.add("you name is licm");
        data.add("name phtyon");
        data.add("name licm");
        data.add("name love you");

    }


    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        System.out.println(Thread.currentThread().getName()+"==>WordSpout被实例化了====>");
    }

    @Override
    public void nextTuple() {

        this.collector.emit(new Values(data.get(index)));
        logger.info("消息发送成功!==>"+ data.get(index));
        index++;
        if(index>=data.size()){
            index = 0;
            MyTimeUtil.sleep(30000);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // 消息流转
        outputFieldsDeclarer.declare(new Fields("words"));
    }

}
