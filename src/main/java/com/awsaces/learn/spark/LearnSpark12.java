/**
 * 
 */
package com.awsaces.learn.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
/**
 * @author aagarwal
 *
 */
public class LearnSpark12 {
	
	private static String appName = "Learn Spark App";
	private static String sparkMaster = "local[2]";
	
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(sparkMaster);
		System.setProperty("hadoop.home.dir", "c:\\spark\\winutils\\");			
		JavaSparkContext spContext = new JavaSparkContext(conf);		
		JavaRDD<String> allData = spContext.textFile("data/auto-data.csv");							
		String shortestString = allData.reduce(new ShortestString());
		System.out.println("Shortest String = " + shortestString);
		spContext.close();
	}
}
class ShortestString implements Function2<String,String,String>{
	public String call(String x, String y){
		return (x.length() < y.length() ? x : y);
	}
}
