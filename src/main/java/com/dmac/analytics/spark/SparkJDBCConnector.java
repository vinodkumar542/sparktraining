package com.dmac.analytics.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.JdbcRDD;

public class SparkJDBCConnector {

	public static void main(String[] args) {
		SparkConf sparkConfig = new SparkConf()
				.setAppName("ReadCSVFile")
				.setMaster("local[8]");
				

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);

		//JdbcRDD<String> jdbcData = new JdbcRDD<>(javaSparkContext, getConnection, sql, 1, 3, 2, mapRow, evidence$1)
	}

}
