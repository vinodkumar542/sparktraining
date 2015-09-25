package com.dmac.analytics.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SparkEventLogging {

	public static void main(String[] args) {
		
		SparkConf sparkConfig = new SparkConf()
				.set("spark.eventLog.enabled", "true")
				.set("spark.eventLog.dir", "file:///Users/apple/spark-events")  // Default is /tmp/spark-events
				.setAppName("ReadUNData")
				.setMaster("local[8]");
				

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);
		
		
		
		JavaRDD<String> rdd = javaSparkContext.textFile("file:///Users/apple/undata1.csv");
		
		
		rdd.map(new Function<String, UNDataBean>() {
									
														@Override
														public UNDataBean call(String input) throws Exception {
															
															String[] splitColumns = input.split(",");
		
															UNDataBean undata = new UNDataBean();
															undata.setCountry(splitColumns[0]);
															undata.setCommodityCode(splitColumns[2]);
															undata.setCommodity(splitColumns[3]);
															return undata;
														}
													}).foreach(param -> System.out.println(param.getCommodity()));

	}

}
