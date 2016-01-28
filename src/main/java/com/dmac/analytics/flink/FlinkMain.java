package com.dmac.analytics.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class FlinkMain {

	public static void main(String[] args) {
		 ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		 
		 DataSet<String> data = env.readTextFile("file:///path/to/file");
	}

}
