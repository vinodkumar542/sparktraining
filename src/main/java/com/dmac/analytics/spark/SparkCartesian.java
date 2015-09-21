package com.dmac.analytics.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SparkCartesian {

	public static void main(String[] args) {
	
		

		SparkConf sparkConfig = new SparkConf()
				.setAppName("ReadCSVFile")
				.setMaster("local[8]");
				

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);
		
		JavaRDD<String> rdd = javaSparkContext.textFile("file:///Users/apple/undata1.csv");
		
		
		JavaRDD<String> countryRDD = rdd.map(new Function<String, String>() {
			
			@Override
			public String call(String input) throws Exception {
				
				String[] splitColumns = input.split(",");
		
				return splitColumns[0];
				
			}
		});

		JavaRDD<String> commodityRDD = rdd.map(new Function<String, String>() {
			
			@Override
			public String call(String input) throws Exception {
				
				String[] splitColumns = input.split(",");
		
				return splitColumns[2];
				
			}
		});
		
		
		JavaPairRDD<String, String> countryAndCommodity = countryRDD.cartesian(commodityRDD);
		countryAndCommodity.foreach(param -> System.out.println(param._1 + " " + param._2));
		
	}

}
