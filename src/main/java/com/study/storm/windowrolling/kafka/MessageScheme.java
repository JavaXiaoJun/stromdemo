package com.study.storm.windowrolling.kafka;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class  MessageScheme implements Scheme{
	@Override
	public List<Object> deserialize(ByteBuffer ser) {
		String result;
		 if (ser.hasArray()) {
	            int base = ser.arrayOffset();
	            result =  new String(ser.array(), base + ser.position(), ser.remaining());
	        } else {
	        	result =  new String(Utils.toByteArray(ser), StandardCharsets.UTF_8);
	        }
		 return new Values(result);
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("intsmaze");
	}

}
