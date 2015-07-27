package com.dmac.analytics.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

public class SparkRDDCheckPointing {

	public static void main(String[] args) {
		SparkConf sparkConfig = new SparkConf()
		.setAppName("ReadLogFile")
		.setMaster("local[8]");

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);
		javaSparkContext.setCheckpointDir("/Users/tester/whitetiger");
		
		JavaRDD<String> locRDD = javaSparkContext.textFile("file:///Users/tester/ac/entitlement_view.csv");
		
		
		JavaRDD<String> locRDDUnion = locRDD.union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD)	  
		.union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD)	  
		.union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD)	  
		.union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD)	  
		.union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD)	  
		.union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD)	  
		.union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD)	  
		.union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD)	  
		.union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD)	  
		.union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD)	  
		.union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD)	  
		.union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD)	  
		.union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD)	  
		.union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD)	  
		.union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD)	  
		.union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD)	  
		.union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD)	  
		.union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD)	  
		.union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD)	  
		.union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD).union(locRDD);
		
		locRDDUnion.persist(StorageLevel.DISK_ONLY());
		locRDDUnion.checkpoint();
		
		locRDDUnion.foreach((z) -> System.out.println(z));
	}
}
