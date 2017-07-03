/**
 * 
 */
package com.awsaces.learn.spark;

import java.util.Arrays;

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
public class LearnSpark08 {
	
	private static String appName = "Learn Spark App";
	private static String sparkMaster = "local[2]";
	
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(sparkMaster);
		System.setProperty("hadoop.home.dir", "c:\\spark\\winutils\\");			
		JavaSparkContext spContext = new JavaSparkContext(conf);		
		JavaRDD<String> allData = spContext.textFile("data/auto-data.csv");							
		JavaRDD<String> toyotaRdd =  allData.filter(str -> str.contains("toyota"));					
		JavaRDD<String> cleanedData = toyotaRdd.map(new ToyotaDataCleaner());
		for(String str:cleanedData.take(32)){
			System.out.println(str);
		}
		spContext.close();
	}
}
class ToyotaDataCleaner implements Function<String,String> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 9108043011253973507L;

	public String call(String toyotaStr){
		String[] wordArray = toyotaStr.split(",");
		wordArray[3] = wordArray[3].equals("two") ? "2" : "4";
		wordArray[6] = wordArray[6].equals("two") ? "2" : "4";
		return Arrays.asList(wordArray).toString();
	}
}
