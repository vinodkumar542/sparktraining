package com.dmac.analytics.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SparkPipeTransformation {

	public static void main(String[] args) {
	
		
		List<String> input = new ArrayList<String>();
		input.add("spark"); input.add("input");
		
		SparkConf sparkConfig = new SparkConf()
				.setAppName("ReadCSVFile")
				.setMaster("local[8]");
				

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);
		
		//JavaRDD<String> rdd = javaSparkContext.textFile("file:///Users/apple/undata1.csv");
		//rdd.pipe("ls -l").foreach(param -> System.out.println(param));
		
		JavaRDD<String> rdd = javaSparkContext.parallelize(input);
		rdd.pipe("execute.sh").foreach(param -> System.out.println(param));
		
		
		//  while read LINE; do
		//   echo ${LINE}    
		//   done
		
	}

}
