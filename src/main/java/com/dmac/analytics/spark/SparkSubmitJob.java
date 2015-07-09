package com.dmac.analytics.spark;

import java.io.IOException;

public class SparkSubmitJob {

	
	private SparkSubmitJob() {}
	
	
	public static void submitSparkJob(String jobName) {
		
		try {
			Process process = Runtime.getRuntime().exec(jobName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
