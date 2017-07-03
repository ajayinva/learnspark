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
/**
 * @author aagarwal
 *
 */
public class LearnSpark09 {
	
	private static String appName = "Learn Spark App";
	private static String sparkMaster = "local[2]";
	
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(sparkMaster);
		System.setProperty("hadoop.home.dir", "c:\\spark\\winutils\\");			
		JavaSparkContext spContext = new JavaSparkContext(conf);		
		
		JavaRDD<String> wordList1 = spContext.parallelize(Arrays.asList("hello", "war", "peace", "world"));
		JavaRDD<String> wordList2 = spContext.parallelize(Arrays.asList("hello", "war", "peace", "India"));
		
		JavaRDD<String> unionList = wordList1.union(wordList2);
		for(String str:unionList.take(32)){
			System.out.println(str);
		}
		System.out.println("--------------------------------------------------------");
		JavaRDD<String> intersectionList = wordList1.intersection(wordList2);
		for(String str:intersectionList.take(32)){
			System.out.println(str);
		}
		System.out.println("--------------------------------------------------------");
		JavaRDD<String> minusList = wordList1.subtract(wordList2);
		for(String str:minusList.take(32)){
			System.out.println(str);
		}
		spContext.close();
	}
}
