package com.zzq.bolt;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PrintBolt extends BaseBasicBolt {

    private static Logger logger = LoggerFactory.getLogger(PrintBolt.class);

    private static final long serialVersionUID = 1L;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        //获取上一个组件所声明的Field
        String line = tuple.getStringByField("line");
        logger.info("获取到数据==>" + line );

        // 传递给下一个 bolt
        basicOutputCollector.emit(new Values(line));
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // 进行申明传递给下一个的字段
        outputFieldsDeclarer.declare(new Fields("line"));
    }

}
