package com.dmac.analytics.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;



public class SparkReadFile {
	
	public static void main(String[] args) {
		
		
		SparkConf sparkConfig = new SparkConf()
						.setAppName("ReadLogFile")
						.setMaster("local[8]");
						//.setMaster("spark://Apples-MacBook-Pro.local:7077");
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);

		javaSparkContext.textFile("file:///E:/ac/spark/code/sparktraining/data/titanic3.csv")
						//.foreach(System.out::println);
						.foreach(z -> System.out.println(z));
		
		javaSparkContext.close();
		javaSparkContext.stop();
	
	}
}
