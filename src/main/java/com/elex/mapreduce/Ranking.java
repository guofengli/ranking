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

import com.elex.util.ComFeature;
import com.elex.util.RankWeight;

public class Ranking {
	public static class RankMap extends Mapper<LongWritable, Text, Text, Text>{
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
				for(String strTMP:ComFeature.getFeatures(features)){
					Text outputKey = new Text();
					outputKey.set(strTMP);
					Text outputValue = new Text();
					float weight = RankWeight.getWeight(confirmedSum, confirmedDollar, otherSum, otherDollar, reqidSum, clickidSum);
					outputValue.set(campid + "\t" + weight);
					context.write(outputKey, outputValue);
				}
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
			StringBuffer outputStr = new StringBuffer();
			Text outputValue = new Text();
			for(int i = 0, len = list.size(); i < len; i++){
				Map.Entry<String, Float> map = list.get(i);
				outputStr.append(map.getKey() + ":" + map.getValue().toString() + "\t");
			}
			outputValue.set(outputStr.toString());
			context.write(key, outputValue);
		}
	}
	
	public static void main(String []args) throws IOException, ClassNotFoundException, InterruptedException{
		String inputPath = "/user/hive/warehouse/ares_merge/000000_0";
		String outputPath = "/tmp/outputjson";
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
