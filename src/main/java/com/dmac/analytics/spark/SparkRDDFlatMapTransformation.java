package com.dmac.analytics.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class SparkRDDFlatMapTransformation {

	public static void main(String args[]) {

		SparkConf sparkConfig = new SparkConf()
						.setAppName("ReadDataFromArray")
						.setMaster("local[5]");
						
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);

		
		List<LatLong> locationList = new ListSource().retrieveList();
	
		
		// RDD can be calculated from an existing collection.
		JavaRDD<LatLong> locRDD = javaSparkContext.parallelize(locationList);

		/*
		JavaRDD<String> flatMappedRDD = locRDD.flatMap(new FlatMapFunction<LatLong, String>() {

			@Override
			public Iterable<String> call(LatLong latLong) throws Exception {
				
				return Arrays.asList(latLong.getId(), latLong.getLatitude(), latLong.getName());
			}
			
		});
		
		flatMappedRDD.collect().forEach(z -> System.out.println("Item = " + z));*/
		
		locRDD.flatMap(z -> Arrays.asList(z.getId()))
			  .foreach(z -> System.out.println("Dojo = " + z));;
				
		
		javaSparkContext.close();
	}
}
