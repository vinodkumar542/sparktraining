package com.dmac.analytics.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SparkRepartitionAndCoalesce {

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

		JavaRDD<String> repartitionedRDD = countryRDD.repartition(100);
		JavaRDD<String> coalesceRDD = countryRDD.coalesce(115);
		
		repartitionedRDD.foreach(param -> System.out.println(param));
		
	}

}
