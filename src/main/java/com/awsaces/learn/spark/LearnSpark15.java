/**
 * 
 */
package com.awsaces.learn.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
/**
 * @author aagarwal
 *
 */
public class LearnSpark15 {
	
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
		JavaPairRDD<String, Integer[]> keyValuePairRdd = 
			dataWithoutHeader.mapToPair(
				new PairFunction<String,String,Integer[]>(){
					public Tuple2<String, Integer[]> call(String str) throws Exception {
						String[] array = str.split(",");
						String key = array[0];
						Integer[] value = new Integer[2]; 
						value[0] = Integer.valueOf(array[7]);
						value[1] = 1;
						return new Tuple2<String, Integer[]>(key, value);
					}
				}
			);
		JavaPairRDD<String, Integer[]> brandTotalHpRdd = 
			keyValuePairRdd.reduceByKey(
				new Function2<Integer[],Integer[],Integer[]>(){
					public Integer[] call(Integer[] value1, Integer[] value2) throws Exception {
						Integer[] result = new Integer[2];
						result[0] = value1[0]+value2[0];
						result[1] = value1[1]+value2[1];
						return result;
					}
				}
			);		
		JavaPairRDD<String, Integer> avgHpRdd =  brandTotalHpRdd.mapValues(x-> x[0]/x[1]);		
		for (Tuple2<String, Integer> kvList : avgHpRdd.take(100)) {
			System.out.println(kvList);
		}
		spContext.close();
	}
}
