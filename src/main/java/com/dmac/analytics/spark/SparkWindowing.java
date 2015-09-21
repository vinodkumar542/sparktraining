package com.dmac.analytics.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkWindowing {

	public static void main(String[] args) {
	
		
		SparkConf sparkConfig = new SparkConf()
				.setAppName("SparkStreaming")
				.setMaster("local[5]");

		JavaStreamingContext jsc = new JavaStreamingContext(sparkConfig, Durations.seconds(5));

		JavaReceiverInputDStream<String> streamOfLines = jsc.socketTextStream("localhost", 5555);

		streamOfLines.reduceByWindow(new Function2<String,String,String>() {

			@Override
			public String call(String arg0, String arg1) throws Exception {
				return arg1.concat(arg0);
			}
		}, Durations.seconds(15), Durations.seconds(5)).foreach(new Function<JavaRDD<String>, Void>() {
			
			@Override
			public Void call(JavaRDD<String> arg0) throws Exception {
				arg0.foreach(param -> System.out.println("\n\n\n\n\n\n\n\n\nOUTPUT - " + param));
				return null;
			}
		});
		jsc.start();
		jsc.awaitTermination();
	
	}

}
