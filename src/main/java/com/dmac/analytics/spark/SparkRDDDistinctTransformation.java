package com.dmac.analytics.spark;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import com.dmac.analytics.spark.LatLong;

public class SparkRDDDistinctTransformation {

	public static void main(String[] args) {
		SparkConf sparkConfig = new SparkConf()
		.setAppName("ReadLogFile")
		.setMaster("local[5]");

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);

		// Retrieve the source
		List<String> locationList = new ListSource().retrieveListOfStrings();
				
		
				
		// RDD can be calculated from an existing collection.
		JavaRDD<String> locRDD = javaSparkContext.parallelize(locationList);
		
		JavaRDD<String> distinctRDD = locRDD.distinct();
		distinctRDD.foreach((outParam) -> System.out.println(outParam));
	}
}
