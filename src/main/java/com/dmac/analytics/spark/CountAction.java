package com.dmac.analytics.spark;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class CountAction {

	public static void main(String[] args) {
		SparkConf sparkConfig = new SparkConf()
				.setAppName("UNDataRead")
				.setMaster("local[8]");

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);
		
		JavaRDD<String> rdd = javaSparkContext.textFile("file:///Users/apple/undata1.csv");
		
		
		long totalCount = rdd.count();
		System.out.println(totalCount);
		
		javaSparkContext.close();

	}

}
