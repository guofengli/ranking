package com.elex.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Ranking {
	public static class RankMap extends Mapper<LongWritable, Text, Text, Text>{
//		String []features = {"campid","nation","size", "language","site","slot","browser","time","confirmedSum","confirmedDollar","otherSum","otherDollar","maxDelay","reqidSum","clickidSum"};
//		String []features = {"nation","size", "language","site","slot","browser","time"};
		String []features = new String[7];
		String []prefixs = {"nation_","size_","language_","site_","slot_","browser_","time_"};
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
				StringTokenizer featureTMP = new StringTokenizer(value.toString(), "\t");		
				String campid = featureTMP.nextToken();
				features[0] = featureTMP.nextToken();
				features[1] = featureTMP.nextToken();
				features[2] = featureTMP.nextToken();
				features[3] = featureTMP.nextToken();
				features[4] = featureTMP.nextToken();
				features[5] = featureTMP.nextToken();
				features[6] = featureTMP.nextToken();
				String confirmedSum = featureTMP.nextToken();
				String confirmedDollar = featureTMP.nextToken();
				String otherSum = featureTMP.nextToken();
				String otherDollar = featureTMP.nextToken();
				String maxDelay = featureTMP.nextToken();
				String reqidSum = featureTMP.nextToken();
				String clickidSum = featureTMP.nextToken();
				int []num = new int[8]; num[0] = 1;
				for( int i = 1 ; i < num.length; i++ ){
					num[i] = 0;
				}
				while( num[7] == 0 ){
					StringBuffer strTMP = new StringBuffer();
					for(int i = 0; i < 7; i++){
						if(num[i] == 1)
							strTMP.append( prefixs[i] + features[i] + "\t" );
					}
					int mark = 1, i = 0;
					while( mark == 1 ){
						num[i] += mark;
						if( num[i] > 1 ){
							num[i++] = 0; mark = 1;
						}else
							mark = 0;
					}
					Text outputKey = new Text();
					outputKey.set(strTMP.toString());
					Text outputValue = new Text();
					float weight = getWeight(confirmedSum, confirmedDollar, otherSum, otherDollar, reqidSum, clickidSum);
					outputValue.set(campid + "\t" + weight);
					context.write(outputKey, outputValue);
				}
		}
		public float getWeight(String confirmedSum, String confirmedDollar, String otherSum, String otherDollar, String reqidSum, String clickidSum){
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
			}
			if((otherDollar != null) && (!"".equals(otherDollar)) && (!"\\N".equals(otherDollar))){
				if((otherSum != null) && (!"".equals(otherSum)) && (!"\\N".equals(otherSum))){
					float tmpNum = Float.parseFloat(otherSum);
					if(tmpNum != 0.0f)
						otherWeight = Float.parseFloat(otherDollar)/Float.parseFloat(otherSum);
				}
			}
			return cpmWeight * 0.1f + cpcWeight * 0.20f + cpaWeight * 0.5f + otherWeight * 0.20f;
		}
		
		
	}
	public static class RankReduce extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			HashMap<String, Float>  weightMap = new HashMap<String, Float>();
			for(Text value:values){
				StringTokenizer tmp = new StringTokenizer(value.toString(), "\t");
				String campID = tmp.nextToken();
				String weightStr = tmp.nextToken();
				float weight = Float.parseFloat(weightStr);
				if(weightMap.get(campID) == null){
					weightMap.put(campID, weight);
				} else{
					weightMap.put(campID, (weightMap.get(campID) + weight));
				}
			}
			List<Map.Entry<String, Float>> list = new ArrayList<Map.Entry<String, Float>>(weightMap.entrySet());
			Collections.sort(list, new Comparator<Map.Entry<String, Float>>(){
				public int compare(Entry<String, Float> o1,
						Entry<String, Float> o2) {
					return Float.compare(o2.getValue(), o1.getValue());
				}	
			});
			Text outNull = new Text();
			context.write(key, outNull);
			for(int i = 0, len = list.size(); i < len; i++){
				Map.Entry<String, Float> map = list.get(i);
				Text outputKey = new Text();
				Text outputValue = new Text();
				outputKey.set(map.getKey());
				outputValue.set(map.getValue().toString());
				context.write(outputKey, outputValue);
			}
		}
	}
	
	public static void main(String []args) throws IOException, ClassNotFoundException, InterruptedException{
		String inputPath = "/user/hive/warehouse/ares_merge/000000_0";
		String outputPath = "/tmp/output";
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"ranking");
		job.setJarByClass(Ranking.class);
		job.setMapperClass(RankMap.class);
		job.setReducerClass(RankReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
