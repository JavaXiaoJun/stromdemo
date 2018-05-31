package com.study.storm.windowrolling;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;

import java.util.concurrent.TimeUnit;

public class WindowRollingTest {
	
	 public static void main(String[] args) throws Exception {
		        TopologyBuilder builder = new TopologyBuilder();
		        //设置Spout
		        builder.setSpout("spout", new RandomSentenceSpout(), 1);
		    
		        //每1秒统计最近5秒的数据 .2个rollingcountbolt并发执行
	            builder.setBolt("rollingcountbolt", new RollingCountBolt()
		        .withWindow(new Duration(5, TimeUnit.SECONDS), new Duration(1, TimeUnit.SECONDS)),1).shuffleGrouping("spout");
		        
	             builder.setBolt("countwordbolt", new CountResultBolt()
	            .withWindow(new Count(1), new Count(1)),1).shuffleGrouping("rollingcountbolt");
	            //每收到2条tuple就统计最近两条统的数据（因为我们RollingCountBolt的并行度为2）
		
		        Config conf = new Config();
		        conf.setNumWorkers(1);
		        conf.setMessageTimeoutSecs(60 * 6);
		        LocalCluster cluster = new LocalCluster();
		        cluster.submitTopology("word-count", conf, builder.createTopology());
		   }

}
