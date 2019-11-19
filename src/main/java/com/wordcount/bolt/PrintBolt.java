package com.wordcount.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PrintBolt extends BaseBasicBolt {

    private static Logger loger = LoggerFactory.getLogger(PrintBolt.class);

    private Map<String , Integer> map = new ConcurrentHashMap<>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        // 获取数据
        String word = tuple.getStringByField("word");
        int count = tuple.getIntegerByField("count");

        loger.info("print bolt 收到消息==>"+word+"===>"+count);

        if(map.containsKey(word)){
            map.put(word, map.get(word)+count);
        }else {
            map.put(word, count);
        }

        loger.info("word ："+word +"\t出现次数：" + map.get(word) );

        loger.info("================================================================");

        for (String s : map.keySet()) {
            System.out.println("word：" +s +"\t showcount："+ map.get(s));
        }
        loger.info("================================================================");

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
