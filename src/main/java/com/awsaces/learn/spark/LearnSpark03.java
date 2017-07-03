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
public class LearnSpark03 {
	
	private static String appName = "Learn Spark App";
	private static String sparkMaster = "local[2]";
	
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(sparkMaster);
		System.setProperty("hadoop.home.dir", "c:\\spark\\winutils\\");			
		JavaSparkContext spContext = new JavaSparkContext(conf);		
		JavaRDD<String> allData = spContext.textFile("data/auto-data.csv");		
		System.out.println("Total Records "+ allData.count());
		JavaRDD<String> percentageSeperatedData =  allData.map(new Function<String, String>(){
			@Override
			public String call(String str) throws Exception {
				return str.replace(',', '%');
			}
		});		
		for(String str:percentageSeperatedData.take(10)){
			System.out.println(str);
		}	
		spContext.close();
	}
}
