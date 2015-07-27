package com.dmac.analytics.spark;

import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import com.dmac.analytics.spark.LicenseCountDataObject;

public class SparkRDDMap {

	public static void main(String args[]) {

		SparkConf sparkConfig = new SparkConf()
						.set("spark.local.dir", "/Users/tester/whitetiger")
						.setAppName("ReadCSVFile")
						.setMaster("local[8]");
						
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);

		// 5 is the number of partitions
		JavaRDD<String> rdd = javaSparkContext.textFile("file:///Users/tester/ac/entitlement_view.csv");
		

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
		

		lcRDD.foreach(z -> System.out.println(z.getId()));

		
		
		javaSparkContext.close();
		javaSparkContext.stop();
		
	}
}


