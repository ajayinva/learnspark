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
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * @author aagarwal
 *
 */
public class MovieRatingHistogram {
	
	private static String appName = "MovieRatingHistogram";
	private static String sparkMaster = "local[*]";
	
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(sparkMaster);
		System.setProperty("hadoop.home.dir", "c:\\spark\\winutils\\");			
		JavaSparkContext spContext = new JavaSparkContext(conf);				
		JavaRDD<String> initDataRdd = spContext.textFile("data/ml-latest-small/ratings.csv");		
		String header = initDataRdd.first();
		JavaRDD<String> initDataWithoutHeader = initDataRdd.filter(x-> !x.equals(header));	
		
		JavaRDD<String> ratings = initDataWithoutHeader.map(x->x.split(",")[2]);
		System.out.println(ratings.countByValue());
		
		/*JavaPairRDD<String,Integer> ratingPairRdd = initDataWithoutHeader.mapToPair(new PairFunction<String, String, Integer> (){			
			private static final long serialVersionUID = 6344234842953164341L;
			public Tuple2<String, Integer> call(String line) throws Exception{
				String[] array = line.split(",");
				return new Tuple2<String, Integer>(array[2],1);
			}
		});
		JavaPairRDD<String,Integer> histogramRdd = ratingPairRdd.reduceByKey((x,y)->x+y).sortByKey();		
		print(histogramRdd,10);*/
		spContext.close();
	}
	/**
	 * 
	 * @param rdd
	 * @param count
	 */
	private static void print(JavaPairRDD<String,Integer> rdd, int count){
		for(Tuple2<String,Integer> s : rdd.take(count)){
			System.out.println("Key:"+s._1+" Value:"+s._2);
		}
	}
}
