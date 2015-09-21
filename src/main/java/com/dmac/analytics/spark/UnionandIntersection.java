package com.dmac.analytics.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;



public class UnionandIntersection {
	
	
	
	public static void main(String[] args) {

		SparkConf sparkConfig = new SparkConf()
						.setAppName("ReadLogFile")
						.setMaster("local[8]");
						//.setMaster("spark://Apples-MacBook-Pro.local:7077");
						

		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);

		JavaRDD<String> indiaRDD = javaSparkContext.textFile("file:///Users/apple/undata1.csv")
						.filter(param -> param.split(",")[0].toString().equals("\"India\""));
		
		JavaRDD<String> hungaryRDD = javaSparkContext.textFile("file:///Users/apple/undata1.csv")
				.filter(param -> param.split(",")[0].toString().equals("\"Hungary\""));
		
		//indiaRDD.union(hungaryRDD).foreach(param -> System.out.println(param));
		
		
		JavaRDD<String> indiaRDDCode = javaSparkContext.textFile("file:///Users/apple/undata1.csv")
				.map(param -> param.split(",")[2].toString());

		JavaRDD<String> hungaryRDDCode = javaSparkContext.textFile("file:///Users/apple/undata1.csv")
				.map(param -> param.split(",")[2].toString());

		indiaRDDCode.intersection(hungaryRDDCode).foreach(param -> System.out.println(param));

		
		javaSparkContext.close();
		javaSparkContext.stop();
	
	}
}
