package com.dmac.analytics.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class CustomPartitioner extends Partitioner {

	

	
	public static void main(String[] args) {
		
		
		SparkConf sparkConfig = new SparkConf()
									.setAppName("ReadDataFromArray")
									.setMaster("local[1]");
									

			JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);
			
			
			List<String> locationList = new ArrayList<String>();
			locationList.add("A");
			//locationList.add("B");
			locationList.add("CCC");
			locationList.add("DDDD");
			locationList.add("RRRRRRRRRR");
			
			// RDD can be calculated from an existing collection.
			JavaRDD<String> locRDD = javaSparkContext.parallelize(locationList);
			
			 locRDD.mapToPair(new PairFunction<String, String, Integer>() {
			
					@Override
					public Tuple2<String, Integer> call(String input) throws Exception {
						
						return new Tuple2<>(input, new Integer(input.length()));
					}
			
			})
					.partitionBy(new CustomPartitioner())
					//.partitions().forEach(z -> System.out.println(z.hashCode()));
					.foreach(z -> System.out.println(z.hashCode() + " " + z._1 +" "+ z._2));
				   


	}


	@Override
	public int getPartition(Object arg0) {
		System.out.println("getPartition");
		String partitionNum = (String) arg0;
		System.out.println("getPartition" + partitionNum);
		return partitionNum.length();
	}

	@Override
	public int numPartitions() {
		System.out.println("numPartitions");
		return 11;
	}	
}
