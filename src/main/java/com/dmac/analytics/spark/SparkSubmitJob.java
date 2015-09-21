package com.dmac.analytics.spark;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkSubmitJob {

	
	public static void main(String[] args) {

		SparkConf sparkConfig = new SparkConf();

		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);

		javaSparkContext.textFile("file:///Users/apple/titanic3.csv")
						//.foreach(System.out::println);
						.foreach(z -> System.out.println(z));
		
		javaSparkContext.close();
		javaSparkContext.stop();
	
	}
}
