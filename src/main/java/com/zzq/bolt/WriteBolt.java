package com.zzq.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class WriteBolt extends BaseBasicBolt {

    private static Logger logger = LoggerFactory.getLogger(WriteBolt.class);

    private String outpath = "/storm-study-out.txt";

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String line = tuple.getStringByField("line");
        try{
            BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(
                            new FileOutputStream(
                                    new File(outpath), true
                            )
                    )
            );

            writer.append(line);
            writer.newLine();
            writer.flush();
            logger.info("写出成功！======>" + line);
        }catch (Exception e){}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
