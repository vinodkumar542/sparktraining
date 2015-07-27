package com.dmac.analytics.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

public class SparkRDDFlatMapTransformation {

	public static void main(String args[]) {

		SparkConf sparkConfig = new SparkConf()
						.setAppName("ReadDataFromArray")
						.setMaster("local[5]");
						
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);

		System.out.println(Runtime.getRuntime().availableProcessors());
		
		List<LatLong> locationList = new ListSource().retrieveList();
	
		
		// RDD can be calculated from an existing collection.
		JavaRDD<LatLong> locRDD = javaSparkContext.parallelize(locationList);

		
		JavaRDD<String> flatMappedRDD = locRDD.flatMap(new FlatMapFunction<LatLong, String>() {

			@Override
			public Iterable<String> call(LatLong latLong) throws Exception {
				
				return Arrays.asList(latLong.getId());
			}
			
		});

		flatMappedRDD.foreach(out -> System.out.println("ID = " + out));
		
		/*
		locRDD.flatMap(out -> Arrays.asList(out.getId()))
			  .foreach(out -> System.out.println("ID = " + out));
		*/		
		
		
		javaSparkContext.close();
	}
}
