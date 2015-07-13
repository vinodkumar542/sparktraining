package com.dmac.analytics.spark;

import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class SparkAccumulatorReckoner {

	public static void main(String args[]) {

		SparkConf sparkConfig = new SparkConf()
						.setAppName("ReadDataFromArray")
						.setMaster("local[5]");
						
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);
		
		
		List<LatLong> locationList = new ListSource().retrieveList();
		
		
		// RDD can be calculated from an existing collection.
		JavaRDD<LatLong> locRDD = javaSparkContext.parallelize(locationList);
		
		
		Accumulator<Integer> accumulator = javaSparkContext.accumulator(55);
		
		 Broadcast<LatLong> bc = javaSparkContext.broadcast(new LatLong("", "", "", ""));
		 
		 bc.getValue();
		 
		//accumulator.setValue(new Integer(22));
		accumulator.add(new Integer(33));
		
		
		System.out.println(accumulator.value());
		javaSparkContext.close();
	}
}
