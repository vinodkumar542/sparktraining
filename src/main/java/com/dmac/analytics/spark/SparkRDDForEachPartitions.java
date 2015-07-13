package com.dmac.analytics.spark;

import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

public class SparkRDDForEachPartitions {
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
		
		
		lcRDD.foreachPartition(new VoidFunction<Iterator<LicenseCountDataObject>>() {
			
			@Override
			public void call(Iterator<LicenseCountDataObject> t) throws Exception {
				// TODO Auto-generated method stub
				
			}
		});
		
		javaSparkContext.close();
	}
}

