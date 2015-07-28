package com.dmac.analytics.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

public class SparkKafkaStreaming {

	
	public static void main(String args[]) {

		SparkConf sparkConfig = new SparkConf()
						.setAppName("KafkaStreaming")
						.setMaster("local[5]");
						
		
		JavaStreamingContext jsc = new JavaStreamingContext(sparkConfig, Durations.seconds(5));
		Map<String, Integer> params = new HashMap();
		params.put("Lohith_Topic", 1);
		
		JavaPairReceiverInputDStream<String, String> receive = KafkaUtils.createStream(jsc, "localhost:2181", "GROUP-ID",  params);
		receive.print();
		jsc.start();
		jsc.awaitTermination();
	}
}
