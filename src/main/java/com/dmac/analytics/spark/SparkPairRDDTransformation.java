package com.dmac.analytics.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class SparkPairRDDTransformation {

	public static void main(String args[]) {

		SparkConf sparkConfig = new SparkConf()
						.setAppName("ReadDataFromArray")
						.setMaster("local[5]");
						
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);
		
		
		List<LatLong> locationList = new ListSource().retrieveList();
		
		
		// RDD can be calculated from an existing collection.
		JavaRDD<LatLong> locRDD = javaSparkContext.parallelize(locationList);
		
		JavaPairRDD<Integer, LatLong> pairRDD = locRDD.mapToPair(new PairFunction<LatLong, Integer, LatLong>() {

			@Override
			public Tuple2<Integer, LatLong> call(LatLong latLong) throws Exception {
				return new Tuple2<>(new Integer(latLong.getId()), latLong);
			}
			
		});
		
		JavaPairRDD<Integer, String> flatMapValuesRDD = pairRDD.flatMapValues(new Function<LatLong, Iterable<String>>() {

			@Override
			public Iterable<String> call(LatLong latLong) throws Exception {
				
				return Arrays.asList(latLong.getLatitude(), latLong.getLongitude(), latLong.getName());
			}
			
		});
		
		
		//TODO
		JavaPairRDD<Integer, LatLong> pairRDDB_Key = locRDD.keyBy(new Function<LatLong, Integer>() {

			@Override
			public Integer call(LatLong v1) throws Exception {
				// TODO Auto-generated method stub
				return null;
			}
			
		});
		
		
		pairRDD.foreach((z) -> System.out.println(z._1.intValue() + " - " + z._2.getId()));
		
		javaSparkContext.close();
	}
}
