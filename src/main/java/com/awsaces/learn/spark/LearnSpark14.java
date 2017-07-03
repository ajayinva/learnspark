/**
 * 
 */
package com.awsaces.learn.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
/**
 * @author aagarwal
 *
 */
public class LearnSpark14 {
	
	private static String appName = "Learn Spark App";
	private static String sparkMaster = "local[2]";
	
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(sparkMaster);
		System.setProperty("hadoop.home.dir", "c:\\spark\\winutils\\");			
		JavaSparkContext spContext = new JavaSparkContext(conf);
		 
		JavaRDD<String> allData = spContext.textFile("data/auto-data.csv");
		String header = allData.first();
		JavaRDD<String> dataWithoutHeader = allData.filter(str -> !str.equals(header));
		
		JavaRDD<Integer> mpgRdd = dataWithoutHeader.map(new Function<String,Integer>(){
			public Integer call(String str){
				String[] array = str.split(",");
				return new Integer(array[9]);
			}
		});		
		Integer totalMpg = mpgRdd.reduce((x,y)->x+y);
		System.out.println("Total Mpg = " + totalMpg);
		spContext.close();
	}
}
