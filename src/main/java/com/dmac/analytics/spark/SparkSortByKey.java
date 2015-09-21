package com.dmac.analytics.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkSortByKey {

	public static void main(String[] args) {
		
		
		SparkConf sparkConfig = new SparkConf()
				.setAppName("WordCountSparkExample")
				.setMaster("local[8]");

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);

		JavaRDD<String> textRDD = javaSparkContext.textFile("file:///Users/apple/simple_text_file.txt");
		
		textRDD.flatMap(param -> Arrays.asList(param.split(" ")))
				.mapToPair(param -> new Tuple2<String, Integer>(param, 1))
				.sortByKey()
				.foreach((words) -> System.out.println(words._1 + " " + words._2)); 

	}

}
