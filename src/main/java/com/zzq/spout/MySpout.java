package com.zzq.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;

public class MySpout extends BaseRichSpout {

    private static final long serialVersionUID = 1L;
    private SpoutOutputCollector collector;

    private String filePath = "/storm-zzq.txt";

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        // 对 spout 进行初始化
        this.collector = spoutOutputCollector;
    }

    /**
     * <B>方法名称：</B>轮询tuple<BR>
     * <B>概要说明：</B><BR>
     */
    @Override
    public void nextTuple() {
        // 发送数据
        try{
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(filePath))));
            String line ;
            while ((line = reader.readLine()) !=null){
                this.collector.emit(new Values(line));
            }
        }catch (Exception e){}
        finally {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // 进行申明传递给下一个的字段
        outputFieldsDeclarer.declare(new Fields("line"));
    }
}
