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

public class WordCountUsingSpark {

	public static void main(String[] args) {
		
		
		SparkConf sparkConfig = new SparkConf()
				.setAppName("WordCountSparkExample")
				.setMaster("local[8]");

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);

		JavaRDD<String> textRDD = javaSparkContext.textFile("file:///Users/tester/ac/entitlement_view.txt");

		
		JavaRDD<String> wordsFlattenedRDD = textRDD.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterable<String> call(String input) throws Exception {
				return Arrays.asList(input);
			}
			
		});
		
		JavaPairRDD<String, Integer>  pairWordMapperRDD = wordsFlattenedRDD.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String inputWord) throws Exception {
				
				return new Tuple2<String, Integer>(inputWord, 1);
			}
			
		} );
		
		JavaPairRDD<String, Integer> countedWords = pairWordMapperRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return new Integer(arg0 + arg1);
			}
		});
		
		countedWords.foreach((words) -> System.out.println(words._1 + words._2));
		
		
		
		
		
		
		
		textRDD.flatMap(param -> Arrays.asList(param))
				.mapToPair(param -> new Tuple2<String, Integer>(param, 1))
				.reduceByKey((integerParam1, integerParam2) -> new Integer(integerParam1 + integerParam2))
				.foreach((words) -> System.out.println(words._1 + words._2));

	}

}
