package com.dmac.analytics.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkStreamingWithCustomReceiver {

	public static void main(String[] args) {
		SparkConf sparkConfig = new SparkConf()
				.setAppName("SparkStreaming")
				.setMaster("local[5]");

		JavaStreamingContext jsc = new JavaStreamingContext(sparkConfig, Durations.seconds(10));

		JavaReceiverInputDStream<String> streamOfLines = jsc.receiverStream(new MyOwnCustomReceiver(StorageLevel.MEMORY_ONLY()));

	}

}
