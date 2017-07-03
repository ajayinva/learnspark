/**
 * 
 */
package com.awsaces.learn.spark;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
/**
 * @author aagarwal
 *
 */
public class LearnSpark07 {
	
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
		JavaRDD<String> totalWordsRdd = toyotaRdd.flatMap( new FlatMapFunction<String,String>(){
			public Iterator<String> call(String toyotaStr){
				String[] wordArray = toyotaStr.split(",");
				return Arrays.asList(wordArray).iterator();
			}
		}).distinct();		
		System.out.println("Total Words "+ totalWordsRdd.count());		
		for(String str:totalWordsRdd.take(385)){
			System.out.println(str);
		}
		spContext.close();
	}
}
