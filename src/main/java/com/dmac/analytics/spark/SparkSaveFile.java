package com.dmac.analytics.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SparkSaveFile {

	public static void main(String[] args) {
		
		SparkConf sparkConfig = new SparkConf()
				//.set("spark.local.dir", "/Users/apple")
				.setAppName("ReadCSVFile")
				.setMaster("local[8]");
				

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);
		
		
		/*
		JavaRDD<String> rdd = javaSparkContext.textFile("file:///Users/apple/undata1.csv");
		
		
		JavaRDD<UNDataBean> lcRDD = rdd.map(new Function<String, UNDataBean>() {
									
														@Override
														public UNDataBean call(String input) throws Exception {
															
															String[] splitColumns = input.split(",");
		
															UNDataBean undata = new UNDataBean();
															undata.setCountry(splitColumns[0]);
															undata.setCommodityCode(splitColumns[2]);
															undata.setCommodity(splitColumns[3]);
															return undata;
														}
													});
		
		
		JavaRDD<String> countryNamesRDD = rdd.map(new Function<String, String>() {
			
			@Override
			public String call(String input) throws Exception {
				
				String[] splitColumns = input.split(",");
		
				
				return splitColumns[0];
			}
		});*/
		
		
		//lcRDD.saveAsTextFile("/Users/apple/saver002");
		//lcRDD.saveAsObjectFile("/Users/apple/saverzz");
		
		
		JavaRDD<UNDataBean> lcRDD = javaSparkContext.objectFile("/Users/apple/saverzz");
		lcRDD.foreach(param -> System.out.println(param.getCountry()));
		
		javaSparkContext.close();
		javaSparkContext.stop();

	}

}
