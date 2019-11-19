package com.wordcount.topology;

import com.wordcount.bolt.CountBolt;
import com.wordcount.bolt.PrintBolt;
import com.wordcount.spout.WordSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountTopology {

    public static void main(String[] args) {
        /*// 1 创建拓扑
        TopologyBuilder builder = new TopologyBuilder();

        // 2 指定设置
        builder.setSpout("wordSpout", new WordSpout());
        builder.setBolt("countBolt", new CountBolt()).shuffleGrouping("wordSpout")
                .fieldsGrouping("wordSpout", new Fields("word"));
        builder.setBolt("printBolt", new PrintBolt()).shuffleGrouping("countBolt")
                .fieldsGrouping("countBolt", new Fields("threadId" , "count"));

        // 3.创建配置信息
        Config conf = new Config();
        conf.setNumWorkers(2);

        // 4 本地模式启动
        LocalCluster localCluster = new LocalCluster();
        // 提交任务
        localCluster.submitTopology("st1", conf, builder.createTopology());*/

        //
        Config cfg = new Config();
        cfg.setNumWorkers(2);
        cfg.setDebug(true);


        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new WordSpout());
        builder.setBolt("print-bolt", new CountBolt()).shuffleGrouping("spout");
        builder.setBolt("write-bolt", new PrintBolt()).shuffleGrouping("print-bolt");


        // 本地模式
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("top1", cfg, builder.createTopology());
        try {
            Thread.sleep(10000000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        cluster.killTopology("top1");
        cluster.shutdown();


    }

}
