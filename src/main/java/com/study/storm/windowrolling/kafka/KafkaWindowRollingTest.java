package com.study.storm.windowrolling.kafka;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;




public class KafkaWindowRollingTest {
	
    private static final String KAFKA_SPOUT_ID = "spout";
	/**
	 * KafkaSpout默认从上次运行停止时的位置开始继续消费。
	 * 因为KafkaSpout默认每2秒钟会提交一次kafka的offset位置到zk上。
	 * 如果要每次运行都从头开始消费可以通过配置实现。
	 */
	 public static void main(String[] args) throws Exception {
		    String zks = "10.16.46.174:2181";
	        String topic = "leostormtest";
	        String zkRoot = "/kafka-stormtest-1";
	        BrokerHosts brokerHosts = new ZkHosts(zks);
	        SpoutConfig spoutConf = new SpoutConfig(brokerHosts,topic,zkRoot,KAFKA_SPOUT_ID);
	        //此处可以自定义Scheme以满足自己的需求
	        spoutConf.scheme = new SchemeAsMultiScheme(new MessageScheme());
	        spoutConf.zkServers = Arrays.asList(new String[] { "10.16.46.191" });
	        spoutConf.zkPort = 2181;
	       

	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout(KAFKA_SPOUT_ID, new KafkaSpout(spoutConf));
	        builder.setBolt("rollingcountbolt", new KafkaRollingCountBolt()
	        		.withWindow(new Duration(5, TimeUnit.MINUTES), new Duration(1, TimeUnit.MINUTES)),1).shuffleGrouping(KAFKA_SPOUT_ID);//.setNumTasks(100);

	        Config conf = new Config();
		    conf.setNumWorkers(1);
		    conf.setDebug(true);
		   //Worker的超时时间（默认30s），单位为秒，超时后，Storm认为当前worker进程死掉，会重新分配其运行着的task任务
		    conf.setMessageTimeoutSecs(60 * 6);
		    LocalCluster cluster = new LocalCluster();
		    cluster.submitTopology("word-count", conf, builder.createTopology());
		    //StormSubmitter.submitTopology("word-count", conf, builder.createTopology());
	    }

}
