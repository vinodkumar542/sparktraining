package com.dmac.analytics.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class NaiveBayes {

	
	public static void main(String args[]) {

		// local - runs the spark locally
		// local[5] - runs the spark locally with 5 threads
		// Application name identifies the application on the cluster manager UI
		SparkConf sparkConfig = new SparkConf()
						.setAppName("DecisionTrees")
						.setMaster("local[5]");
		
		
		JavaSparkContext sc = new JavaSparkContext(sparkConfig);
		
		//MLUtils.de
		
		//DecisionTree.trainClassifier(arg0, arg1, arg2, arg3, arg4, arg5)
		
	}
	
}
