package com.dmac.analytics.spark;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SparkPerformanceTuning {

	public static void main(String[] args) {
		
		
		SparkConf sparkConfig = new SparkConf()
				.setAppName("UNDataRead")
				.setMaster("local[8]");

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);

		JavaRDD<String> rdd = javaSparkContext.textFile("file:///Users/apple/undata1.csv");
		
		
		// Repartition it after filtering
		/*
		rdd.filter(param -> param.split(",")[0].equals("\"India\""))
		.repartition(2)
		.foreach(System.out::println);*/
		
		
		Map<Integer, String> hugeHashMap = new HashMap<>();
		hugeHashMap.put(1, "Turing");
		
		
		//Don’t pass in large amounts of data into parallel functions, 
		//Instead 
		//use broadcast or parallelize to a RDD.
		/*
		javaSparkContext.parallelizePairs(list)
		javaSparkContext.parallelize(list) */
		
		rdd.map(new Function<String, String>() {

			@Override
			public String call(String arg0) throws Exception {
				System.out.println(hugeHashMap.get(1));
				return "";
			}
			
		}).count();
		
		

	}

}
