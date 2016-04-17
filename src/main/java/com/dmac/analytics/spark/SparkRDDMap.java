package com.dmac.analytics.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SparkRDDMap {

	public static void main(String args[]) {

		SparkConf sparkConfig = new SparkConf()
						//.set("spark.local.dir", "/Users/apple")
						.setAppName("ReadCSVFile")
						.setMaster("local[8]");
						
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);

		// 5 is the number of partitions
		JavaRDD<String> rdd = javaSparkContext.textFile("file:///C:/ac/spark/code/sparktraining/data/undata1.csv");
		

		/**
		 * 1,2,3 all are same
		 * 
		 */
		// 1
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
		
		// 2
		JavaRDD<UNDataBean> undataBeanconverterRDD = rdd.map(new UNBeanConverterFunction());
			
		// 3
		JavaRDD<UNDataBean> undataBeanRDD = rdd.map((each) -> {			
			
				String[] splitColumns = each.split(",");
	
				UNDataBean undata = new UNDataBean();
				undata.setCountry(splitColumns[0]);
				undata.setCommodityCode(splitColumns[2]);
				undata.setCommodity(splitColumns[3]);
				return undata;	
		});

		
		undataBeanRDD.collect();
		
		
		JavaRDD<String> countryNamesRDD = rdd.map(new Function<String, String>() {
			
			@Override
			public String call(String input) throws Exception {
				
				String[] splitColumns = input.split(",");

				
				return splitColumns[0];
			}
		});

		//lcRDD.foreach(param -> System.out.println(param.getCountry()));

		//lcRDD.saveAsTextFile("/Users/apple/saver002");
		//lcRDD.saveAsObjectFile("/Users/apple/saverzz");
		
		//countryNamesRDD.saveAsTextFile("/Users/apple/countryNamesSaver");
		
		javaSparkContext.close();
		javaSparkContext.stop();
		
	}
}

class UNBeanConverterFunction implements Function<String, UNDataBean> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	

	@Override
	public UNDataBean call(String input) throws Exception {
		
		String[] splitColumns = input.split(",");

		UNDataBean undata = new UNDataBean();
		undata.setCountry(splitColumns[0]);
		undata.setCommodityCode(splitColumns[2]);
		undata.setCommodity(splitColumns[3]);
		return undata;
	}
	
}

