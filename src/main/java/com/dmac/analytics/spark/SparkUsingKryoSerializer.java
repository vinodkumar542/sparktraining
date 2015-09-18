package com.dmac.analytics.spark;

import java.util.StringTokenizer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


public class SparkUsingKryoSerializer {

	public static void main(String[] args) {
		SparkConf sparkConfig = new SparkConf()
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.registerKryoClasses(new Class[]{TitanicBean.class})
				.setAppName("ReadFile")
				.setMaster("local[8]");
				

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);

		JavaRDD<String> rdd = javaSparkContext.textFile("file:///Users/koteshwar/titanic3.csv");

		JavaRDD<TitanicBean> titanicRDD = rdd.map(new Function<String, TitanicBean>() {

			@Override
			public TitanicBean call(String inputLine) throws Exception {
				TitanicBean tBean = new TitanicBean();
				
				String[] iLine = inputLine.split(",");
				tBean.setName(iLine[2]);
				
				
				return tBean;
			}
		});
		
		titanicRDD.foreach(lines -> System.out.println(lines.getName()));

		javaSparkContext.close();
	
	}

}
