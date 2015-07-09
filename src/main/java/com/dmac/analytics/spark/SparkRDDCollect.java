package com.dmac.analytics.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

public class SparkRDDCollect {

	public static void main(String args[]) {

		// local - runs the spark locally
		// local[5] - runs the spark locally with 5 threads
		// Application name identifies the application on the cluster manager UI
		SparkConf sparkConfig = new SparkConf()
						.setAppName("ReadLogFile")
						.setMaster("local[5]");
						
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);

		JavaRDD<String> rdd = javaSparkContext.textFile("file:///Users/tester/ac/entitlement_view.csv");
		
		
		// Caching is nothing but the StorageLevel.MEMORY_ONLY()
		//rdd.cache();
		
		//JavaRDD<String> persistedRDD = rdd.persist(StorageLevel.MEMORY_ONLY());
		JavaRDD<String> persistedRDD = rdd.persist(StorageLevel.DISK_ONLY());
		persistedRDD.collect();
		persistedRDD.unpersist();
		
		//javaSparkContext.textFile("file:///Users/tester/input.txt").cache().collect();
		
		// To shutdown the spark
		javaSparkContext.stop();
		javaSparkContext.close(); // close inturn calls the stop method.
	}
}
