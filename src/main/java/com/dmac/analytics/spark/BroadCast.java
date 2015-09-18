package com.dmac.analytics.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

public class BroadCast {

	public static void main(String args[]) {

		SparkConf sparkConfig = new SparkConf()
						.setAppName("ReadDataFromArray")
						.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
						.registerKryoClasses(new Class[]{TitanicBean.class})
						.setMaster("local[5]");
						
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);
		
		
		
		Broadcast<TitanicBean> bc = javaSparkContext.broadcast(new TitanicBean("1", "1", "William"));
		
		long totalCount = javaSparkContext.textFile("file:///Users/koteshwar/titanic3.csv")
				.map(new Function<String, String>() {

					@Override
					public String call(String input) throws Exception {
						
						String[] splittedInput = input.split(",");
						
						TitanicBean titanicBean = bc.getValue();
						
						System.out.println(titanicBean.getName());
						return splittedInput[1];
					}
					
				}).count();
		
		javaSparkContext.close();

	}
}
