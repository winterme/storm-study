package com.zzq.topology;


import com.zzq.bolt.PrintBolt;
import com.zzq.bolt.WriteBolt;
import com.zzq.spout.MySpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class PWTopology1 {

    public static void main(String[] args) {
        //
        Config cfg = new Config();
        cfg.setNumWorkers(1);
        cfg.setDebug(true);


        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new MySpout());
        builder.setBolt("print-bolt", new PrintBolt()).shuffleGrouping("spout");
        builder.setBolt("write-bolt", new WriteBolt()).shuffleGrouping("print-bolt");


        /*// 本地模式
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("top1", cfg, builder.createTopology());*/
        try {

            // 集群模式
            StormSubmitter.submitTopology("zhanzgqtop" , cfg , builder.createTopology());

            Thread.sleep(10000000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
