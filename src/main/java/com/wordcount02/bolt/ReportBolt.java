package com.wordcount02.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author maxwell
 * @Description: 报数
 * @date 2019/12/6 15:26
 */
public class ReportBolt extends BaseRichBolt {

    private static Logger loger = LoggerFactory.getLogger(ReportBolt.class);

    private OutputCollector collector;
    private Map<String, Integer> map = new HashMap<>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        System.out.println(Thread.currentThread().getName()+"==>ReportBolt被实例化了");
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Integer count = tuple.getIntegerByField("count");

        map.put(word, count);

        /*if(map.containsKey(word)){
            map.put(word, map.get(word) + count );
        }else{
            map.put(word, count);
        }*/

        loger.info("================================================================");

        for (String s : map.keySet()) {
            System.out.println(Thread.currentThread().getName()+ "ReportBolt 输出 ==> word：" +s +"\t showcount："+ map.get(s));
        }
        loger.info("================================================================");

        try {
            String path = "D://s//"+this;

            File file = new File(path);
            if(!file.exists()){
                file.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
