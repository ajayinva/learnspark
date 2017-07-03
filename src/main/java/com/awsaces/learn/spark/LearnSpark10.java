/**
 * 
 */
package com.awsaces.learn.spark;

import java.util.Arrays;
import java.util.List;

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
public class LearnSpark10 {
	
	private static String appName = "Learn Spark App";
	private static String sparkMaster = "local[2]";
	
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(sparkMaster);
		System.setProperty("hadoop.home.dir", "c:\\spark\\winutils\\");			
		JavaSparkContext spContext = new JavaSparkContext(conf);		
		List<Integer> data = Arrays.asList(3,6,3,4,8);
		JavaRDD<Integer> collData = spContext.parallelize(data);
		
		//Integer sum = collData.reduce((x,y)->x+y);
		
		/*Integer sum = collData.reduce(new Function2<Integer,Integer,Integer>(){
			public Integer call(Integer x, Integer y){
				return x+y;				
			}
		});*/
		
		Integer sum = collData.reduce(new SumCalculator());
		
		System.out.println("Total = " + sum);
		spContext.close();
	}
}
class SumCalculator implements Function2<Integer,Integer,Integer> {
	public Integer call(Integer x, Integer y){
		return x+y;				
	}
}