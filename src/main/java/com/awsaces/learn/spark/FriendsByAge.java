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
public class FriendsByAge {
	
	private static String appName = "FriendsByAge";
	private static String sparkMaster = "local[*]";
	
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(sparkMaster);
		System.setProperty("hadoop.home.dir", "c:\\spark\\winutils\\");			
		JavaSparkContext spContext = new JavaSparkContext(conf);				
		JavaRDD<String> initDataRdd = spContext.textFile("data/fakefriends.csv");				
		JavaPairRDD<Integer,Integer[]> ageFriendsKeyPairRdd = initDataRdd.mapToPair(new PairFunction<String, Integer, Integer[]> (){						
			public Tuple2<Integer, Integer[]> call(String line) throws Exception{
				String[] array = line.split(",");
				return new Tuple2<Integer, Integer[]>(Integer.parseInt(array[2]), new Integer[] {Integer.parseInt(array[3]), 1});
			}
		});
		JavaPairRDD<Integer,Integer[]> totalFriendsByAge = ageFriendsKeyPairRdd.reduceByKey(new Function2<Integer[],Integer[],Integer[]>(){
			public Integer[] call(Integer[] value1, Integer[] value2) throws Exception {
				Integer[] array = new Integer[2];
				array[0] = value1[0]+ value2[0];
				array[1] = value1[1]+ value2[1];
				return array;
			}			
		});		
		JavaPairRDD<Integer,Double> friendsByAge =  totalFriendsByAge.mapValues(new Function<Integer[],Double>(){
			public Double call(Integer[] value) throws Exception {
				return value[0]/new Double(value[1]);
			}
		}).sortByKey();
		print(friendsByAge,100);
		spContext.close();
	}
	/**
	 * 
	 * @param rdd
	 * @param count
	 */
	private static void print(JavaPairRDD<Integer,Double> rdd, int count){
		for(Tuple2<Integer,Double> s : rdd.take(count)){
			System.out.println("Key:"+s._1+" Value:"+s._2);
		}
	}
}
