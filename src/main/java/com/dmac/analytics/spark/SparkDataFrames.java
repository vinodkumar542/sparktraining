package com.dmac.analytics.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

public class SparkDataFrames {

	
	
	public static void main(String args[])
	{
			SparkConf sparkConfig = new SparkConf()
			.setAppName("ReadLogFile")
			.setMaster("local[5]");
			
			//.setMaster("spark://SCHMAC-TESTER-4.local:7077");
			//.setMaster("spark://52.24.58.38:7077");
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);
		
		SQLContext sqlContext = new SQLContext(javaSparkContext);
		sqlContext.read().json("");
		
		javaSparkContext.textFile("file:///Users/tester/ac/entitlement_view.txt")
			.collect()
			.forEach(z -> System.out.println(z));
		
		javaSparkContext.close();
		javaSparkContext.stop();

	}
}
