package com.dmac.analytics.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class SparkInternals {

	public static void main(String[] args) {
		
		SparkConf sparkConfig = new SparkConf()
				.setAppName("ReadFile")
				.setMaster("local[8]");

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);

		JavaRDD<String> textRDD = javaSparkContext.textFile("file:///Users/apple/simple_text_file.txt");

		
		textRDD.flatMap(param -> Arrays.asList(param.split(" ")))
		.mapToPair(param -> new Tuple2<String, Integer>(param, param.length()))
		.groupByKey()
		.map(new Function<Tuple2<String,Iterable<Integer>>, Tuple2<String,Iterable<Integer>>>() {

			@Override
			public Tuple2<String,Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> freq) throws Exception {
				int counter = 0;
				while(freq._2.iterator().hasNext()) {
					counter++;
				}
				
				if (counter>=2)
					return freq;
				else 
					return null;
			}
		})
		.count();
	}

}
