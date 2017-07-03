/**
 * 
 */
package com.awsaces.learn.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
/**
 * @author aagarwal
 *
 */
public class LearnSpark16 {
	
	private static String appName = "Learn Spark App";
	private static String master = "local[2]";
	private static String tempDir = "file:///c:/temp/spark-warehouse";
	
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		SparkSession spark = SparkSession
								  .builder()
								  .appName(appName)
								  .master(master)
								  .config("spark.sql.warehouse.dir", tempDir)
								  .getOrCreate();
		Dataset<Row> df = spark.read().json("data/customerData.json");
		df.createOrReplaceTempView("people");
		Dataset<Row> sqlDF = spark.sql("SELECT AGE, DEPTID FROM people");
		//df.show();
		//df.groupBy(col("deptid")).count().orderBy(col("count")).show();
		sqlDF.show();
	}
}
