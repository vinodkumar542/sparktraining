package com.dmac.analytics.kafka;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class SimpleKafkaProducer {

	public static void main(String[] args) {
		
		
		Properties props = new Properties();
		
		props.put("zk.connect", "127.0.0.1:2181");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", "broker1:9092,broker2:9092");
		props.put("request.required.acks", "1");
		
		ProducerConfig config = new ProducerConfig(props);
				
		Producer<String, String> producer = new Producer<String, String>(config);
		
		producer.send(new KeyedMessage<String, String>("test", "BUSINESS-MESSAGE"));
		producer.close();
	}
}
