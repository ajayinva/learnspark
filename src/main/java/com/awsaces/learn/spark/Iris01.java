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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * @author aagarwal
 *
 */
public class Iris01 {
	
	private static String appName = "Iris01";
	private static String sparkMaster = "local[*]";
	
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(sparkMaster);
		System.setProperty("hadoop.home.dir", "c:\\spark\\winutils\\");			
		JavaSparkContext spContext = new JavaSparkContext(conf);				
		JavaRDD<String> initDataRdd = spContext.textFile("data/iris.csv");		
		String header = initDataRdd.first();
		JavaRDD<String> initDataWithoutHeader = initDataRdd.filter(x-> !x.equals(header));	
		JavaPairRDD<String, Double> sepalLengthRdd = initDataWithoutHeader.mapToPair(new PairFunction<String, String, Double>(){			
			public Tuple2<String,Double> call(String line){
				String[] array = line.split(",");
				String key = array[4];
				Double sepalLength = Double.parseDouble(array[0]);
				return new Tuple2<String, Double>(key, sepalLength);
			}
		});
		JavaPairRDD<String, Double> minLengthRdd = sepalLengthRdd.reduceByKey(new Function2<Double, Double, Double>(){
			public Double call(Double value1, Double value2){				
				return Math.min(value1, value2);
			}
		});
		/*JavaPairRDD<String, Double> averageLengthRdd = totalLengthByKey.mapValues(new Function<Double[], Double>(){
			public Double call(Double[] value){
				return value[0]/value[1];
			}
		});*/
		print(minLengthRdd,5);
		spContext.close();
	}
	/**
	 * 
	 * @param rdd
	 * @param count
	 */
	private static void print(JavaPairRDD<String, Double> rdd, int count){
		for(Tuple2<String,Double> s : rdd.take(count)){
			System.out.println("Key:"+s._1+" Value:"+s._2);
		}
	}
}
