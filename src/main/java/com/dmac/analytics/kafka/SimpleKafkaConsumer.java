package com.dmac.analytics.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class SimpleKafkaConsumer {

	
	public static void main(String[] args) {
		
		 	Properties properties = new Properties();
	        properties.put("zookeeper.connect","localhost:2181");
	        properties.put("group.id","test-group");
	        ConsumerConfig consumerConfig = new ConsumerConfig(properties);
	        
	        Map<String, Integer> topicCountMap = new HashMap();
	        topicCountMap.put("Lohith_Topic", new Integer(1));
	        
	        
	        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
	        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
	        KafkaStream<byte[], byte[]> stream =  consumerMap.get("Lohith_Topic").get(0);
	        
	        ConsumerIterator<byte[], byte[]> it = stream.iterator();
	        while(it.hasNext()) {
	        	 System.out.println(new String(it.next().message()));
	        }
	
	}
}
