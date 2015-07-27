package com.dmac.analytics.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class BroadCast {

	public static void main(String args[]) {

		SparkConf sparkConfig = new SparkConf()
						.setAppName("ReadDataFromArray")
						.setMaster("local[5]");
						
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);
		
		javaSparkContext.setCheckpointDir("/Users/tester/whitetiger");
		javaSparkContext.checkpointFile("/Users/tester/whitetiger");
		
		Broadcast<LatLong> bc = javaSparkContext.broadcast(new LatLong("1", "2", "3", "4"));
		bc.getValue();
	
		 
		System.out.println(bc.getValue().getId());

	}
}
