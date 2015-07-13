package com.dmac.analytics.spark;

import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.FlatMapFunction;

public class SparkBasicPartitions {
	public static void main(String args[]) {

		SparkConf sparkConfig = new SparkConf()
						.setAppName("ReadCSVFile")
						.setMaster("local[5]");
						
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);

		// 5 is the number of partitions
		JavaRDD<String> rdd = javaSparkContext.textFile("file:///Users/tester/ac/entitlement_view.csv", 5);
		

		JavaRDD<LicenseCountDataObject> lcRDD = rdd.map(new Function<String, LicenseCountDataObject>() {
									
														@Override
														public LicenseCountDataObject call(String input) throws Exception {
															String[] splitColumns = input.split(",");
															LicenseCountDataObject lcObject = new LicenseCountDataObject();
															lcObject.setId(splitColumns[0]);
															lcObject.setLicenseCount(splitColumns[3]);
															return lcObject;
														}
													});
		

		//lcRDD.foreach(z -> System.out.println(z.getId()));
		lcRDD.partitions().forEach(z -> System.out.println(z.hashCode()));
		System.out.println("Number of partitions size = " + lcRDD.partitions().size());
		
		
		
		JavaRDD<String> partitionRDD = lcRDD.mapPartitions(new FlatMapFunction<Iterator<LicenseCountDataObject>, String>() {

			@Override
			public Iterable<String> call(Iterator<LicenseCountDataObject> t)
					throws Exception {
				// TODO Auto-generated method stub
				return null;
			}
		});
		
		
		javaSparkContext.close();
	}
}

