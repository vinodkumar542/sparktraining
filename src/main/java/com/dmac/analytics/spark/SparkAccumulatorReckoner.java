package com.dmac.analytics.spark;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkAccumulatorReckoner {

	public static void main(String args[]) {

		SparkConf sparkConfig = new SparkConf()
						.setAppName("ReadDataFromArray")
						.setMaster("local[5]");
						
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);
		
		Accumulator<Integer> accumulator = javaSparkContext.accumulator(10, "adder_accumulator");
		
		for (;;) {
			
			
			/*
			Broadcast<LatLong> bc = javaSparkContext.broadcast(new LatLong("", "", "", ""));
			 bc.getValue();
			*/ 
			//accumulator.setValue(new Integer(22));
			accumulator.add(new Integer(1));
			
			try {
				Thread.sleep(3333);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			System.out.println(accumulator.value());
			
		}
		//System.out.println(accumulator.value());
		//javaSparkContext.close();
	}
}
