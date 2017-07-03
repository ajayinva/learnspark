/**
 * 
 */
package com.awsaces.learn.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * @author aagarwal
 *
 */
public class WordCounter {
	
	private static String appName = "WordCounter";
	private static String sparkMaster = "local[*]";
	
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(sparkMaster);
		System.setProperty("hadoop.home.dir", "c:\\spark\\winutils\\");			
		JavaSparkContext spContext = new JavaSparkContext(conf);				
		JavaRDD<String> initDataRdd = spContext.textFile("data/book.txt");						
		JavaRDD<String> wordList = initDataRdd.flatMap(new FlatMapFunction<String,String>(){
			public Iterator<String> call (String line){
				String[] array = line.split("\\W+");
				return Arrays.asList(array).iterator();
			}
		});
		JavaPairRDD<String,Integer> wordListPairRdd = wordList.mapToPair(new PairFunction<String,String,Integer>(){
			public Tuple2<String,Integer> call(String word){
				return new Tuple2<String,Integer> (word.toLowerCase(),1);
			}
		});
		JavaPairRDD<String,Integer> wordCount =  wordListPairRdd.reduceByKey((x,y)->x+y);		
		JavaPairRDD<Integer,String> popularWordKey = wordCount.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>(){
			public Tuple2<Integer,String> call(Tuple2<String,Integer> wordCountPair){
				return new Tuple2<Integer,String>(wordCountPair._2, wordCountPair._1);
			}
		}).sortByKey(false);
		print(popularWordKey,10000);
		spContext.close();
	}
	/**
	 * 
	 * @param rdd
	 * @param count
	 */
	private static void print(JavaPairRDD<Integer,String> popularWordKey, int count){
		for(Tuple2<Integer,String> s : popularWordKey.take(count)){
			System.out.println(s._2+":"+s._1);
		}
	}
}
