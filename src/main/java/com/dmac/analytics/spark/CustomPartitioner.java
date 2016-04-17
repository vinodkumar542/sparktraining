package com.dmac.analytics.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class CustomPartitioner {

	public static void main(String[] args) {
		
		
		SparkConf sparkConfig = new SparkConf()
									.setAppName("ReadDataFromArray")
									.setMaster("local[1]");
									

			JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);
			
			
			List<String> locationList = new ArrayList<String>();
			locationList.add("14567834");
			locationList.add("14567856");
			locationList.add("19349785");
			locationList.add("19349785");
			locationList.add("15345345");
			locationList.add("15467567");
			locationList.add("19849848");
			locationList.add("27848784");
			locationList.add("25454959");
			locationList.add("28577575");
			
			
			// RDD can be calculated from an existing collection.
			JavaRDD<String> locRDD = javaSparkContext.parallelize(locationList);
					
			JavaPairRDD<String, Integer> locationPairRDD = locRDD.mapToPair(new PairFunction<String, String, Integer>() {
			
					@Override
					public Tuple2<String, Integer> call(String input) throws Exception {
						
						return new Tuple2<>(input, new Integer(input.length()));
					}
			
			})
					.partitionBy(new UIDPartitioner());
				
					
			/*
			locationPairRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<String,Integer>>, Iterator<Integer>>() {

				@Override
				public Iterator<Integer> call(Integer v1, Iterator<Tuple2<String, Integer>> v2) throws Exception {
					// TODO Auto-generated method stub
					return null;
				}
				
			}); */

			
			

	}

}


class UIDPartitioner extends Partitioner {

	@Override
	public int getPartition(Object input) {
		String inputDataSet = (String) input;
		
		if (inputDataSet.startsWith("1"))
			return 0;
		else 
			return 1;
	}

	@Override
	public int numPartitions() {
		return 2;
	}
	
}