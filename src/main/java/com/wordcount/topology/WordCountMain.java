package com.wordcount.topology;

import com.wordcount.bolt.CountBolt;
import com.wordcount.bolt.PrintBolt;
import com.wordcount.spout.WordSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class WordCountMain {
    public static void main(String[] args) {
        // 创建 拓扑图
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("wordSpout" , new WordSpout());
        builder.setBolt("print-bolt", new CountBolt()).shuffleGrouping("wordSpout");
        builder.setBolt("write-bolt", new PrintBolt()).shuffleGrouping("print-bolt");

        // 配置工作线程
        Config cfg = new Config();
        cfg.setNumWorkers(2);
        cfg.setDebug(true);

        // 本地模式
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("top1", cfg, builder.createTopology());

    }
}
