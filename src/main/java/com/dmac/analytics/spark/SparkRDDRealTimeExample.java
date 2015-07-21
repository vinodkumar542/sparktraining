package com.dmac.analytics.spark;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkRDDRealTimeExample {
	public static void main(String args[]) {

		SparkConf sparkConfig = new SparkConf()
						.setAppName("ReadCSVFile")
						.setMaster("local[5]");
						
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);

		ListSource ls = new ListSource();
		List<LatLong> latLong = ls.retrieveList();
		// 5 is the number of partitions
		javaSparkContext.parallelize(latLong).partitions().forEach(z -> System.out.println(z.index()));
	
		javaSparkContext.close();
	}
}

