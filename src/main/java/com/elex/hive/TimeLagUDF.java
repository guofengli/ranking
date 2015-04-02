package com.elex.hive;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.hive.ql.exec.UDF;


public class TimeLagUDF extends UDF{
	public static HashMap<String, Long> timeLagMap =  getTimeLag();
	public String evaluate(String nation, String time){
		String []periods = {"midnight","midnight","weehours","weehours","weehours","morning","morning","morning","forenoon","forenoon","forenoon","noon","noon","noon",
				"afternoon","afternoon","afternoon","afternoon","evening","evening","evening","evening","evening","midnight"};
		if(time == null || "".equals(time) || "undefined".equals(time)){
			return "null";
		}
		nation = nation.toLowerCase();
		if(nation == null || "".equals(nation) || "undefined".equals(nation)){
			nation = "cn";
		}
//		HashMap<String, Long> timeLagMap = getTimeLag();
		SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = null;
		try {
			date = inputFormat.parse(time);
			long t = date.getTime();
			if(timeLagMap.get(nation) != null){
				date = new Date(t + timeLagMap.get(nation));
			}
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		int hour = date.getHours();
		return periods[hour];
	}
	public static HashMap<String, Long> getTimeLag(){
		String f = TimeLagUDF.class.getClassLoader().getResource("timelag").getPath();
		File file = new File(f);
        BufferedReader reader = null;
        HashMap<String,Long> timeLagMap = new HashMap<String,Long>();
        try {  
            reader = new BufferedReader(new FileReader(file));  
            String tempString = null;  
            while ((tempString = reader.readLine()) != null) {
            	StringTokenizer timeLags = new StringTokenizer(tempString.toLowerCase(), "\t");
            	String nation = timeLags.nextToken();
            	timeLags.nextToken();
            	String timeLagStr = timeLags.nextToken();
            	float timeLag = Float.parseFloat(timeLagStr);
            	long time = 0;
            	int mark = timeLag >= 0.0f? 1:-1;
            	time += timeLag * 60 * 60 * 1000 * mark;
            	timeLagMap.put(nation, time);
            }  
            reader.close();  
        } catch (IOException e) {  
            e.printStackTrace();  
        } finally {  
            if (reader != null) {  
                try {  
                    reader.close();  
                } catch (IOException e1) {  
                }  
            }  
        }  
		return timeLagMap;
	}
	
//	public static void main(String []args){
//		TimeLagUDF test = new TimeLagUDF();
//		System.out.println(test.evaluate("",""));
//		String []periods = {"midnight","midnight","weehours","weehours","weehours","morning","morning","morning","forenoon","forenoon","forenoon","noon","noon","noon","afternoon","afternoon","afternoon","afternoon","evening",
//				"evening","evening","evening","evening","midnight"};
//		for(int i = 0; i < periods.length; i++){
//			System.out.println(i + " : " + periods[i]);
//		}
//	}
}
