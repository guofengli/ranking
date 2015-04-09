package com.elex.util;

import java.io.IOException;
import java.util.Properties;

import com.elex.hive.AresMerge;

public class RankWeight {
	static String cpm;
	static String cpc;
	static String cpa;
	static String other;;
	static{
		Properties prop = new Properties();
		try {
			prop.load(RankWeight.class.getClassLoader().getResourceAsStream("weightconf.properties"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		cpm = prop.getProperty("cpm");
		cpc = prop.getProperty("cpc");
		cpa = prop.getProperty("cpa");
		other = prop.getProperty("other");
	}
	public static float getWeight(String confirmedSum, String confirmedDollar, String otherSum, String otherDollar, String reqidSum, String clickidSum) throws IOException{
		float cpmWeight = 0.0f, cpcWeight = 0.0f, cpaWeight = 0.0f, otherWeight = 0.0f; 
		if((confirmedDollar != null) && (!"".equals(confirmedDollar)) && (!"\\N".equals(confirmedDollar))){
			if((reqidSum != null) && (!"".equals(reqidSum)) && (!"\\N".equals(reqidSum))){
				float tmpNum = Float.parseFloat(reqidSum);
				if(tmpNum != 0.0f)
					cpmWeight = Float.parseFloat(confirmedDollar)/tmpNum;
			}
			if((clickidSum != null) && (!"".equals(clickidSum)) && (!"\\N".equals(clickidSum))){
				float tmpNum = Float.parseFloat(clickidSum);
				if(tmpNum != 0.0f)
					cpcWeight = Float.parseFloat(confirmedDollar)/tmpNum;
			}
			if((confirmedSum != null) && (!"".equals(confirmedSum)) && (!"\\N".equals(confirmedSum))){
				float tmpNum = Float.parseFloat(confirmedSum);
				if(tmpNum != 0.0f)
					cpaWeight = Float.parseFloat(confirmedDollar)/tmpNum;
			}
			if((otherDollar != null) && (!"".equals(otherDollar)) && (!"\\N".equals(otherDollar))){
				if((otherSum != null) && (!"".equals(otherSum)) && (!"\\N".equals(otherSum))){
					float tmpNum = Float.parseFloat(otherSum);
					if(tmpNum != 0.0f)
						otherWeight = Float.parseFloat(otherDollar)/Float.parseFloat(otherSum);
				}
			}
			return cpmWeight * Float.parseFloat(cpm) + cpcWeight * Float.parseFloat(cpc) + cpaWeight * Float.parseFloat(cpa) + otherWeight * Float.parseFloat(other);
		} else{
			
			return 0.0f;
		}
	}
}
