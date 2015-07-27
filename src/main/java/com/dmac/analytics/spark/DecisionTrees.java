package com.dmac.analytics.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.util.MLUtils;

public class DecisionTrees {

	public static void main(String args[]) {

		// local - runs the spark locally
		// local[5] - runs the spark locally with 5 threads
		// Application name identifies the application on the cluster manager UI
		SparkConf sparkConfig = new SparkConf()
						.setAppName("DecisionTrees")
						.setMaster("local[5]");
		
		
		JavaSparkContext sc = new JavaSparkContext(sparkConfig);
		JavaRDD<String> inputData = sc.textFile("/Users/tester/golf.csv");
		
		inputData.foreach(z -> System.out.println(z));
		//MLUtils.de
		
		JavaRDD<LabeledPoint> source  = null;
		String impurity = "gini";
		int maxDepth = 5;
		int maxBins = 32;
		
		
		DecisionTreeModel dtm = DecisionTree.trainClassifier(source, 2, null, impurity, maxDepth, maxBins);
		
	}
	
}
