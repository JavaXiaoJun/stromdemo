package com.study.storm.windowrolling.kafka;

import com.study.storm.utils.ConsumerUtils;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;

/**
 *接入kafka消息获取实时数据流。
 */
public class KafkaSpout extends BaseRichSpout {

	
	private static final long serialVersionUID = 1999652681174101065L;
	
	SpoutOutputCollector collector; 
        
    @Override
	public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
    	this.collector = collector;
	}
    
    @Override
	public void nextTuple() {
		List<KafkaStream<byte[],byte[]>> partitions = ConsumerUtils.getMessage("leostormtest");
		if(partitions.size() == 0){
			System.out.println("消息为空!");
		}
		for(KafkaStream<byte[], byte[]> partition : partitions) {
			ConsumerIterator<byte[],byte[]> iterator = partition.iterator();
			while(iterator.hasNext()) {
				MessageAndMetadata<byte[],byte[]> next = iterator.next();
				System.out.println("从kafka获取未消费的消息:"+ new String(next.message()) +" offset:"+ next.offset()
						+ "partiton:" + next.partition());
				collector.emit(new Values(next.key(),next.message()));
			}
		}

	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		  declarer.declare(new Fields("intsmaze"));
	}

}
