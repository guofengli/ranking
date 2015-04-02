package com.elex.hive;

import java.util.StringTokenizer;

import org.apache.hadoop.hive.ql.exec.UDF;

public class SplitCampIDUDF extends UDF{
	public String evaluate(String str){
		StringTokenizer campIDAndAdIDs = new StringTokenizer(str, ":");
		StringTokenizer campID = new StringTokenizer(campIDAndAdIDs.nextToken(), ",");
		StringTokenizer adID = new StringTokenizer(campIDAndAdIDs.nextToken(), ",");
		StringBuffer strTMP = new StringBuffer();
		while(campID.hasMoreTokens() && adID.hasMoreTokens()){
			strTMP.append(campID.nextToken() + "\t" + adID.nextToken());
			if(campID.hasMoreTokens()) {
				strTMP.append(":");
			}
		}
		return strTMP.toString();   
  }  
}
