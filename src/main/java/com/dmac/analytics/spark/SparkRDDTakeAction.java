package com.dmac.analytics.spark;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkRDDTakeAction {

public static void main(String[] args) {

	
		
		SparkConf sparkConfig = new SparkConf()
									.setAppName("ReadLogFile")
									.setMaster("local[5]");

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);
		
		JavaRDD<String> rdd = javaSparkContext.textFile("file:///Users/tester/ac/entitlement_view.csv");
		
		// The take action returns in the first five rows of the RDD on which it is operated.
		List<String> firstFiveRows = rdd.take(5);
		firstFiveRows.forEach(z -> System.out.println(z));
		
		javaSparkContext.close();
		
	}
}
