package com.dmac.analytics.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;



public class SparkRunningInWindows {
	
	
	
	public static void main(String[] args) {	
		
		/*
		
		Failed to locate the winutils binary in the hadoop binary path
		java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.
			at org.apache.hadoop.util.Shell.getQualifiedBinPath(Shell.java:278)
			
		*/
		
		//  Put winutils.exe located in the config folder into D:\winutils\bin
		//  Add the line System.setProperty("hadoop.home.dir", "D:\\winutils\\")
		//  The above exception occurs only for the Windows Environment
		
		System.setProperty("hadoop.home.dir", "D:\\winutils\\");
		
		
		SparkConf sparkConfig = new SparkConf()
						.setAppName("ReadLogFile")
						.setMaster("local[8]");
						//.setMaster("spark://Apples-MacBook-Pro.local:7077");				

		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);

		javaSparkContext.textFile("file:///E:/ac/spark/code/sparktraining/data/titanic3.csv")
						//.foreach(System.out::println);
						.foreach(z -> System.out.println(z));
		
		javaSparkContext.close();
		javaSparkContext.stop();
	
	}
}
