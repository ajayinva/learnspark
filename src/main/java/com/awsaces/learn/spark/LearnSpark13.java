/**
 * 
 */
package com.awsaces.learn.spark;

import org.apache.commons.lang.math.NumberUtils;
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
public class LearnSpark13 {
	
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
		String totalMpg = dataWithoutHeader.reduce(new TotalMpg());
		System.out.println("Total Mpg = " + totalMpg);
		spContext.close();
	}
}
class TotalMpg implements Function2<String,String,String>{	
	public String call(String x, String y){
		Integer first = 0;
		Integer second = 0;
		if(!NumberUtils.isNumber(x)){
			String[] array = x.split(",");
			first = new Integer(array[9]);
		} else{
			first = new Integer(x);
		}
		if(!NumberUtils.isNumber(y)){
			String[] array = y.split(",");
			second = new Integer(array[9]);
		} else{
			second = new Integer(y);
		}		
		return first+second+"";
	}
}
