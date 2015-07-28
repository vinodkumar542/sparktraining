package com.dmac.analytics.spark;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class PairRDDOperations {

	
	public static void main(String args[])
	{
			SparkConf sparkConfig = new SparkConf()
											.setAppName("PairRDDOperations")
											.setMaster("local[8]");
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);

		
		ListSource source = new ListSource();
		List<String> listOfNames = source.retrieveListOfStrings();
		
		JavaPairRDD<String, Integer>  reduceByKeyRDD = javaSparkContext.parallelize(listOfNames)
																	   .mapToPair(param -> new Tuple2<>(String.valueOf(param.charAt(0)), new Integer(1)))
																	   .reduceByKey((param1, param2) -> new Integer(param1.intValue() + param2.intValue()));
																	
		
		Map<String, Object> countByKeyRDD = javaSparkContext.parallelize(listOfNames)
															.mapToPair(param -> new Tuple2<>(String.valueOf(param.charAt(0)), new Integer(1)))
															.countByKey();
		
		
		JavaPairRDD<String, Iterable<Integer>> groupByKey = javaSparkContext.parallelize(listOfNames)
																			.mapToPair(param -> new Tuple2<>(String.valueOf(param.charAt(0)), new Integer(1)))
																			.groupByKey();
		
		JavaPairRDD<String, Integer>  sortByKeyRDD = javaSparkContext.parallelize(listOfNames)
																	 .mapToPair(param -> new Tuple2<>(String.valueOf(param.charAt(0)), new Integer(1)))
																	 .sortByKey();
		
		//reduceByKeyRDD.foreach(param -> System.out.println(param._1 + " **** " + param._2));
		
		countByKeyRDD.forEach((param1, param2) -> System.out.println(param1 + " ### " + ((Long)param2).intValue()));
		
		//reduceByKeyRDD.foreach(param -> System.out.println(param._1 + " **** " + param._2));
		
		//reduceByKeyRDD.foreach(param -> System.out.println(param._1 + " **** " + param._2));
		
		//reduceByKeyRDD.foreach(param -> System.out.println(param._1 + " **** " + param._2));
	}
		
}
