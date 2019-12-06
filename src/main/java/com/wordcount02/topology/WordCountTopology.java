package com.wordcount02.topology;

import com.util.MyTimeUtil;
import com.wordcount02.bolt.CountBolt;
import com.wordcount02.bolt.ReportBolt;
import com.wordcount02.bolt.SplitBolt;
import com.wordcount02.spout.WordSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * @author maxwell
 * @Description: TODO
 * @date 2019/12/6 15:31
 */
public class WordCountTopology {

    public static void main(String[] args) {
        // 创建拓扑图
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        WordSpout wordSpout = new WordSpout();
        SplitBolt splitBolt = new SplitBolt();
        CountBolt countBolt = new CountBolt();
        ReportBolt reportBolt = new ReportBolt();


        topologyBuilder.setSpout("wordSpout", wordSpout , 1);
        topologyBuilder.setBolt("splitBolt", splitBolt , 4).shuffleGrouping("wordSpout");
        topologyBuilder.setBolt("countBolt", countBolt , 4).fieldsGrouping("splitBolt", new Fields("word"));
        topologyBuilder.setBolt("reportBolt", reportBolt, 4).globalGrouping("countBolt");


        // 配置工作线程
        Config cfg = new Config();
        cfg.setNumWorkers(2);
        cfg.setDebug(true);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("top1", cfg, topologyBuilder.createTopology());

        MyTimeUtil.sleep(10*1000);
        System.out.println("==================game over==============================");
    }

}
