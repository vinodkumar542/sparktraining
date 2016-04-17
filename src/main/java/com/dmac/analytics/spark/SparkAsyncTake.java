package com.dmac.analytics.spark;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class SparkAsyncTake {

	public static void main(String args[]) {

		SparkConf sparkConfig = new SparkConf()
						//.set("spark.local.dir", "/Users/apple")
						.setAppName("ReadCSVFile")
						.setMaster("local[8]");
						
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);

		// 5 is the number of partitions
		JavaRDD<String> rdd = javaSparkContext.textFile("file:///C:/ac/spark/code/sparktraining/data/undata1.csv");
		

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
				
		//treeAggregate
		//treeReduce
		lcRDD.fold(new UNDataBean(), new Function2<UNDataBean, UNDataBean, UNDataBean>() {
			
			@Override
			public UNDataBean call(UNDataBean v1, UNDataBean v2) throws Exception {
				
				return null;
			}
		});
		
		JavaFutureAction<List<UNDataBean>> futureTake = lcRDD.takeAsync(10);
		
		try {
			System.out.println(futureTake.get().size());
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		System.out.println(futureTake.isDone());
		
		
		javaSparkContext.close();
		javaSparkContext.stop();
		
	}
}


