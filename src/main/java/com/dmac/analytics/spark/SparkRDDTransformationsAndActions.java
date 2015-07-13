package com.dmac.analytics.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkRDDTransformationsAndActions {

	public static void main(String[] args) {

		
		SparkConf sparkConfig = new SparkConf()
									.setAppName("ReadLogFile")
									.setMaster("local[5]");

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);
		
		JavaRDD<String> rdd = javaSparkContext.textFile("file:///Users/tester/ac/entitlement_view.csv");
		
		// Only when you say the action, spark starts reading from the file
		//rdd.collect();
		rdd.foreach(z -> System.out.println(z));
		
		javaSparkContext.close();
		
	}
}
