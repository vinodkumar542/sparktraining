package com.dmac.analytics.spark;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class SparkMySQL {

	public static void main(String[] args) {
		SparkConf sparkConfig = new SparkConf()
		.setAppName("ReadLogFile")
		.setMaster("local[8]");


		JavaSparkContext javaSparkContext =  new JavaSparkContext(sparkConfig);

		SQLContext sqlContext = new SQLContext(javaSparkContext);
		
		
		Map<String, String> options = new HashMap<String, String>();
		options.put("url", "jdbc:mysql://127.0.0.1:3306/slz_core");
		options.put("user", "slz02");
		options.put("password", "slz02@123");
		options.put("dbtable", "regions");
		
		
		DataFrame slzDF = sqlContext.read().format("jdbc").options(options).load();
		slzDF.select("name").show();
	}

}
