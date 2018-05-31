package com.study.storm.windowrolling;

import com.study.storm.utils.CommonUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * 
 * 继承一个新的类名为BaseWindowedBolt来获得窗口计数的功能。
 * execute方法的参数类型为TupleWindow,TupleWindow参数里面装载了一个窗口长度类的tuple数据。
 * 通过对TupleWindow遍历，我们可以计算这一个窗口内tuple数的平均值或总和等指标
 *
 */
public class RollingCountBolt extends BaseWindowedBolt{
	
	
	private static final long serialVersionUID = 1826359067444332583L;
	
	private OutputCollector collector;
	private int taskid; 
	//窗口内未重复数据集合
	private  Map<String, String> norepeatMap ;
	
	 @Override
     public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.taskid = context.getThisTaskId();
        this.collector = collector;
        norepeatMap = new HashMap<String, String>();
     }
	

	@Override
	public void execute(TupleWindow inputWindow) {
		
		//定义窗口内需要发送的数据集合
		List<String> massageList = new LinkedList<>();
		
	    //获取相对上一次窗口新增和过期的tuples
	    List<Tuple> expired = inputWindow.getExpired();
	    List<Tuple> add = inputWindow.getNew();
	    List<Tuple> now = inputWindow.get();
	    
	    norepeatMap.clear();
	    
	    //处理当前窗口内的数据
	    for(Tuple tuple: CommonUtils.receiveDefectList(now, add)) {
	    	String value=tuple.getValueByField("intsmaze").toString();
	    	if(StringUtils.isNotEmpty(value)){
		       norepeatMap.put(value,value);	    	
	    	}
	    	
	    }
	    
	    //添加相对上一次窗口新增数据
	    for(Tuple tuple: add) {
	    	String value=tuple.getValueByField("intsmaze").toString();
	    	if(StringUtils.isNotEmpty(value)){
	    		if(!norepeatMap.containsKey(value)){
		    		norepeatMap.put(value,value);
		    		System.out.println("新增可用(new)数据: "+value+",发送消息。");
		    		massageList.add(value);
		    	}else{
		    		System.out.println("新增重复(repeat)数据: "+value+",删除。");
		    	}
	    	}
	    	
	    }
	    
	    //移除相对上一次窗口过期的数据
	    for(Tuple tuple: expired) {
            String value=tuple.getValueByField("intsmaze").toString();
            if(StringUtils.isNotEmpty(value)){  
	               norepeatMap.remove(value);
	               System.out.println("移除过期数据: "+value);
            }
   
        }
	    
	    
	    
        System.out.println("blot"+taskid+"内的数据: "+norepeatMap); 
        collector.emit(new Values(massageList));
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	       declarer.declare(new Fields("massagelist"));
	}
	

}
