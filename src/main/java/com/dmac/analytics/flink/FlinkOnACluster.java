package com.dmac.analytics.flink;

import org.apache.flink.api.java.ExecutionEnvironment;

public class FlinkOnACluster {

	public static void main(String[] args) {
		
		ExecutionEnvironment env = ExecutionEnvironment
		        .createRemoteEnvironment("flink-master", 6123, "/home/user/udfs.jar");

		
	}

}
