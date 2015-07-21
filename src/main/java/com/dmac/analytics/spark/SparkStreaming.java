package com.dmac.analytics.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class SparkStreaming {

	public static void main(String args[]) {
		
		SparkConf sparkConfig = new SparkConf()
										.setAppName("SparkStreaming")
										.setMaster("local[5]");
		
		JavaStreamingContext jsc = new JavaStreamingContext(sparkConfig, Durations.seconds(1));
		JavaReceiverInputDStream<String> streamOfLines = jsc.socketTextStream("localhost", 5555);
		
		JavaDStream<String> dStream = streamOfLines.flatMap(z -> Arrays.asList(z.split(" ")));
		
		 JavaPairDStream<String, String> pairDStream = dStream.mapToPair(new PairFunction<String, String, String>() {

			@Override
			public Tuple2<String, String> call(String t) throws Exception {
				System.out.println(t);
				return new Tuple2<String, String>(t, t);
			}
		});
		 
		 pairDStream.print();
		jsc.start();
		jsc.awaitTermination();
		
	}
}
