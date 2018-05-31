package com.study.storm.utils;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.List;
import java.util.Properties;

public class ConsumerUtils  {
	private static ConsumerConnector consumer;

	static {
		//创建一个consumer对象
		Properties props = new Properties();
		props.put("zookeeper.connect","10.16.46.191:2181");//zookeeper集群地址
		//props.put("auto.offset.reset", "smallest"); //如果要读旧数据，必须要加
		props.put("group.id", "0");//消费者群组id,不指定使用默认
		props.put("zookeeper.session.timeout.ms", "30000");//服务器响应延时
		consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
	}


	public static List<KafkaStream<byte[],byte[]>> getMessage(String topic) {
	    Whitelist whitelist = new Whitelist(topic);
		List<KafkaStream<byte[],byte[]>> partitions = consumer.createMessageStreamsByFilter(whitelist);
		return partitions;
	}
	

}