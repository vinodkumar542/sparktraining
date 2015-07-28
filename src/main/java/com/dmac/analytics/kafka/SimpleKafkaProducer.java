package com.dmac.analytics.kafka;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class SimpleKafkaProducer {

	public static void main(String[] args) {
		
		
		Properties props = new Properties();
		
		props.put("zk.connect", "localhost:2181");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", "localhost:9092");
		
		ProducerConfig config = new ProducerConfig(props);
				
		Producer<String, String> producer = new Producer<String, String>(config);
		
		producer.send(new KeyedMessage<String, String>("Lohith_Topic", "BUSINESS-MESSAGE77777777"));
		producer.close();
	}
}
