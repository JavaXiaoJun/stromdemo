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
		//����һ��consumer����
		Properties props = new Properties();
		props.put("zookeeper.connect","10.16.46.191:2181");//zookeeper��Ⱥ��ַ
		//props.put("auto.offset.reset", "smallest"); //���Ҫ�������ݣ�����Ҫ��
		props.put("group.id", "0");//������Ⱥ��id,��ָ��ʹ��Ĭ��
		props.put("zookeeper.session.timeout.ms", "30000");//��������Ӧ��ʱ
		consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
	}


	public static List<KafkaStream<byte[],byte[]>> getMessage(String topic) {
	    Whitelist whitelist = new Whitelist(topic);
		List<KafkaStream<byte[],byte[]>> partitions = consumer.createMessageStreamsByFilter(whitelist);
		return partitions;
	}
	

}