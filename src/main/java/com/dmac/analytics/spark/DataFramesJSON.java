package com.dmac.analytics.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class DataFramesJSON {

	public static void main(String args[]) {
		
		SparkConf sparkConfig = new SparkConf()
									.setAppName("DataFrames")
									.setMaster("local[5]");


		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);

		SQLContext sc = new SQLContext(javaSparkContext);
		DataFrame df = sc.read().json("file:///Users/tester/ac/ycy.json");
		df.printSchema();
	}
}
