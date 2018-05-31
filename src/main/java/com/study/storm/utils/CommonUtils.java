package com.study.storm.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.tuple.Tuple;

import java.io.*;
import java.util.*;

public class CommonUtils {
	
	/**
	 * 读取指定文件的内容
	 * @param path
	 * @return
	 */
	public static List<Integer> readfile(String path){
		  List<Integer> arrlist = new ArrayList<>();
		  InputStreamReader read =null;
		  FileInputStream fis= null;
	      try {
	          File file=new File(path);
	          if(file.isFile() && file.exists()){
				  fis =  new FileInputStream(file);
				  read = new InputStreamReader(fis,"GBK");
	              BufferedReader bufferedReader = new BufferedReader(read);
	              String lineTxt = null;
	              while((lineTxt = bufferedReader.readLine()) != null){
	                  arrlist.add(Integer.parseInt(lineTxt));
	              }
	              read.close();
	          }else{
	              System.out.println("file not found");
	          }
	      } catch (Exception e) {
	          e.printStackTrace();
	      }finally {
			  if (fis !=null){
				  try {
					  fis.close();
				  } catch (IOException e) {
					  e.printStackTrace();
				  }
			  }
			  if( read != null){
				  try {
					  read.close();
				  } catch (IOException e) {
					  e.printStackTrace();
				  }
			  }
		  }
		  return arrlist;
	}
	 
	
	  /**
     * 获取两个ArrayList的差集
     * @param firstArrayList 第一个ArrayList
     * @param secondArrayList 第二个ArrayList
     * @return resultList 差集ArrayList
     */
    public static List<Tuple> receiveDefectList(List<Tuple> firstArrayList, List<Tuple> secondArrayList) {
        List<Tuple> resultList = new ArrayList<Tuple>();
        LinkedList<Tuple> result = new LinkedList<Tuple>(firstArrayList);
        HashSet<Tuple> othHash = new HashSet<Tuple>(secondArrayList);
        Iterator<Tuple> iter = result.iterator();
        while(iter.hasNext()){  
            if(othHash.contains(iter.next())){  
                iter.remove();            
            }     
        }  
        resultList = new ArrayList<Tuple>(result);
        return resultList;
    }

    /**
     * 记录本次窗口数据相关日志
     * @param now
     * @param add
     * @param expired
     */
	public static void keepLogs(List<Tuple> now, List<Tuple> add, List<Tuple> expired) {
		List<String> nowList = new LinkedList<>();
		List<String> addList = new LinkedList<>();
		List<String> expiredList = new LinkedList<>();
		for(Tuple t:now){
			String value  = t.getValueByField("intsmaze").toString();
			if(StringUtils.isNotEmpty(value)){
			  nowList.add(value);
			}
		}
		
		for(Tuple t:add){
			String value  = t.getValueByField("intsmaze").toString();
			if(StringUtils.isNotEmpty(value)){
				addList.add(value);
			}
		}
		
		for(Tuple t:expired){
			String value  = t.getValueByField("intsmaze").toString();
			if(StringUtils.isNotEmpty(value)){
				expiredList.add(value);
			}
		}
		
		System.out.println("本次窗口相关数据如下: all"  + nowList +"    add:" + addList + "   expired" + expiredList);
	}

}
