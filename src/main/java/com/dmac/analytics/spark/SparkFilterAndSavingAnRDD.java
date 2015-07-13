package com.dmac.analytics.spark;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SparkFilterAndSavingAnRDD {

	public static void main(String args[]) {

		SparkConf sparkConfig = new SparkConf()
						.setAppName("ReadDataFromArray")
						.setMaster("local[5]");						
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);

		// Retrieve the source
		List<LatLong> locationList = new ListSource().retrieveList();
		
		
		// RDD can be calculated from an existing collection.
		JavaRDD<LatLong> locRDD = javaSparkContext.parallelize(locationList);
		
		
		//Filter Operation
		JavaRDD<LatLong> evenRDD = locRDD.filter(new Function<LatLong, Boolean>() {
			
			@Override
			public Boolean call(LatLong latLong) throws Exception {
				int id = Integer.parseInt(latLong.getId());
				
				if (id % 2 == 0)
					return Boolean.TRUE;
				else 
					return Boolean.FALSE;
			}
		});
		
		
		//Map operation
		/*
		JavaRDD<String> latitudeRDD = evenRDD.map(new Function<LatLong, String>() {

			@Override
			public String call(LatLong latLong) throws Exception {
				return latLong.getLatitude();
			}
		});*/
			
		//Map operation
		JavaRDD<String> latitudeRDD = evenRDD.map(z -> z.getLatitude());
		
		latitudeRDD.foreach(z -> System.out.println(z));
		//latitudeRDD.saveAsTextFile("/Users/tester/savedFile");
		
		javaSparkContext.close();
	}
}
