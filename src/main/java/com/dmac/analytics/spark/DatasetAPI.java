package com.dmac.analytics.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SQLContext;

public class DatasetAPI {

	public static void main(String[] args) {
		
		List<String> data = Arrays.asList("Spark", "is", "doing", "awesome");
		
		
		SparkConf sparkConfig = new SparkConf()
									.setAppName("DatasetAPI")	
									.setMaster("local[8]");
		
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);
		
		SQLContext sqlContext = new SQLContext(javaSparkContext);
		
		Dataset<String> dataset = sqlContext.createDataset(data, Encoders.STRING());
		dataset.foreach(eachElement -> System.out.println(eachElement));
		

	}

}
