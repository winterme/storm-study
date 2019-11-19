package com.wordcount.spout;

import org.apache.logging.log4j.core.util.FileUtils;
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

public class WordSpout extends BaseRichSpout {

    private String filePath = "D:\\storm-data";

    private SpoutOutputCollector collector;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        // 初始化
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        try{
            File listfile = new File(filePath);
            for (File file : listfile.listFiles()) {
                if(file.getName().endsWith(".txt")){
                    synchronized (this){
                        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));

                        String line ;
                        while ( (line = reader.readLine()) != null ){
                            this.collector.emit(new Values(line));
                        }

                        reader.close();
                        // 读取完了之后标记一下，改个名字
                        file.renameTo(new File(file.getAbsolutePath()+"."+System.currentTimeMillis()));
                    }
                }
            }
        }catch (Exception e){

        }finally {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }

    public static void main(String[] args) throws Exception {
        File file = new File("D:\\storm-data\\x1.txt");

        file.renameTo(new File(file.getAbsolutePath()+"."+System.currentTimeMillis()));

    }

}
