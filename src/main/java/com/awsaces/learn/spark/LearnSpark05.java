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
public class LearnSpark05 {
	
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
		String header = allData.first();		
		/*JavaRDD<String> noHeaderData =  allData.filter(
			new Function<String, Boolean>(){
				@Override
				public Boolean call(String str) throws Exception {
					return !str.equals(header);
				}
		});		*/				
		JavaRDD<String> noHeaderData =  allData.filter(str -> !str.equals(header));	
		for(String str:noHeaderData.take(10)){
			System.out.println(str);
		}	
		spContext.close();
	}
}
