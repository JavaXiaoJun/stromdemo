package com.study.storm.windowrolling;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 
 * RollingCount是多个bolt并行计算，在RollingCountBolt中无法对多个bolt的结果进行合并，
 * 故增加一个bolt类，只统计每个RollingCount节点发送给它的数值（完成数据的merge展示）。
 *
 */
public class CountResultBolt extends BaseWindowedBolt{

	
	private static final long serialVersionUID = 8809272676320076864L;
	
	private OutputCollector collector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
    }
	
	@Override
	public void execute(TupleWindow inputWindow) {	
		 List<Integer> allList = new ArrayList<Integer>();
         for(Tuple tuple: inputWindow.get()) {
        	 List<Integer> massageList=(List<Integer>)tuple.getValueByField("massagelist");
        	 allList.addAll(massageList);
           
         }
         //最终数据会发送到指定位置（新的topic）
         if(allList.size() > 0){
        	 System.out.println("当前窗口需要发送的消息数据为:"+allList);
         }
         
         
	}

}
