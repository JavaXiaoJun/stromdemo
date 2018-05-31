package com.study.storm.windowrolling;

import com.study.storm.utils.CommonUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.List;
import java.util.Map;

/**
 *定义一个spout，用于产生数据。固定间隔1s向bolt发送一次数据
 *实际中会接入kafka消息获取实时数据流。
 */
public class RandomSentenceSpout extends BaseRichSpout {

	
	private static final long serialVersionUID = 1999652681174101065L;
	
	SpoutOutputCollector collector; 
        
    @Override
	public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
    	this.collector = collector;
	}
    
    @Override
	public void nextTuple() {
    	      List<Integer> spout= CommonUtils.readfile("D:\\spout.txt");
    	      for(Integer value:spout){
    	    	  collector.emit(new Values(value));
        	      //睡眠一段时间产生一个数据 ,模拟流式数据，实际中会接入kafka消（读取指定topic的消息）。
                  Utils.sleep(500); 
    	      }
    	      
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		  declarer.declare(new Fields("intsmaze"));
	}

}
