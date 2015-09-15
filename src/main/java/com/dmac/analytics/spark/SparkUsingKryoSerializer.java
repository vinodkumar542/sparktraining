package com.dmac.analytics.spark;

import java.lang.reflect.Array;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class SparkUsingKryoSerializer {

	public static void main(String[] args) {
		SparkConf sparkConfig = new SparkConf()
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.registerKryoClasses(new Class[]{ListSource.class})
				.setAppName("ReadFile")
				.setMaster("local[8]");
				

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);

		JavaRDD<String> rdd = javaSparkContext.textFile("file:///Users/koteshwar/titanic3.csv");

		rdd.foreach(lines -> System.out.println(lines));

		javaSparkContext.close();
	
	}

}
