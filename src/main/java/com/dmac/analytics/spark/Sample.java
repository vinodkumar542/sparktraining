package com.dmac.analytics.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;



public class Sample {
	
	
	
	public static void main(String[] args) {

		SparkConf sparkConfig = new SparkConf()
						.setAppName("ReadLogFile")
						.setMaster("local[8]");
						//.setMaster("spark://Apples-MacBook-Pro.local:7077");
						

		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);

		javaSparkContext.textFile("file:///Users/apple/titanic3.csv")
						.sample(false, 0.1)
						.foreach(param -> System.out.println(param));
		
		javaSparkContext.close();
		javaSparkContext.stop();
	
	}
}
