package com.dmac.analytics.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class CustomPartitioner extends Partitioner {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6735035793561121618L;

	@Override
	public int getPartition(Object inputObject) {
		Integer input = (Integer)inputObject;
		return 1;
	}

	@Override
	public int numPartitions() {
		return 1;
	}

	
	public static void main(String[] args) {
		
		
		SparkConf sparkConfig = new SparkConf()
									.setAppName("ReadDataFromArray")
									.setMaster("local[1]");
									

			JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);
			
			
			List<String> locationList = new ArrayList<String>();
			locationList.add("A");
			locationList.add("B");
			locationList.add("CCC");
			locationList.add("DDDD");
			
			// RDD can be calculated from an existing collection.
			JavaRDD<String> locRDD = javaSparkContext.parallelize(locationList);
			
			JavaPairRDD<Integer, String> pairRDD = locRDD.mapToPair(new PairFunction<String, Integer, String>() {
			
					@Override
					public Tuple2<Integer, String> call(String input) throws Exception {
						
						return new Tuple2<>(new Integer(input.length()), input);
					}
			
			});
			pairRDD.partitionBy(new CustomPartitioner())
				   .foreach(z -> System.out.println(z._1 +" "+ z._2));
				   
				   

	}
}
