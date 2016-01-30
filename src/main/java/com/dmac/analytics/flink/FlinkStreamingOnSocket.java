package com.dmac.analytics.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkStreamingOnSocket {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment see =  StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<String> text = see.socketTextStream("localhost", 3333);
		
		text.print();
		
		see.execute();
	}

}
