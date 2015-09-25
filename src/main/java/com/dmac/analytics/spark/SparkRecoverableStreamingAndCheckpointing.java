package com.dmac.analytics.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;

public class SparkRecoverableStreamingAndCheckpointing {

	public static void main(String[] args) {
		
		JavaStreamingContextFactory factory = new JavaStreamingContextFactory() {

			@Override
			public JavaStreamingContext create() {
				return createContext();
			}
			
		};
		
		JavaStreamingContext ssc = JavaStreamingContext.getOrCreate("", factory);
		ssc.start();
		ssc.awaitTermination();

	}

	private static JavaStreamingContext createContext() {
		
		SparkConf sparkConfig = new SparkConf()
				.setAppName("ReadUNData")
				.setMaster("local[8]");
		
		 JavaStreamingContext ssc = new JavaStreamingContext(sparkConfig, Durations.seconds(1));
		 ssc.checkpoint("/Users/apple/checkpointdir");
		 
		 JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 5555);
		 
		 lines.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterable<String> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return null;
			}
			 
		 });
		 
		 return ssc;
	}
}
