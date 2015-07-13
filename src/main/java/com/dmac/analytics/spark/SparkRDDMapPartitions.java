package com.dmac.analytics.spark;

import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Consumer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.FlatMapFunction;

public class SparkRDDMapPartitions {
	public static void main(String args[]) {

		SparkConf sparkConfig = new SparkConf()
						.setAppName("ReadCSVFile")
						.setMaster("local[5]");
						
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);

		
		// 5 is the number of partitions
		JavaRDD<String> rdd = javaSparkContext.textFile("file:///Users/tester/ac/entitlement_view.csv", 5);
	
		
		JavaRDD<Integer> partitionRDD = rdd.mapPartitions(new FlatMapFunction<Iterator<String>, Integer>() {

			@Override
			public Iterable<Integer> call(Iterator<String> input) throws Exception {
				
			Integer integer = new Integer(0);
			
				 input.forEachRemaining(new Consumer<String>() {

					@Override
					public void accept(String input) {
						
						String[] splitColumns = input.split(",");
						//integer = Integer.parseInt(splitColumns[0]);
					}
				});
				
				return Arrays.asList(integer);
			}
		});
		
		javaSparkContext.close();
	}
}

