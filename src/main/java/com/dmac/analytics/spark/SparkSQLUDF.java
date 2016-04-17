package com.dmac.analytics.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class SparkSQLUDF {

	public static void main(String[] args) {
		
		SparkConf sparkConfig = new SparkConf()
										.setAppName("SparkSQLUDF")
										.setMaster("local[8]");

		/**
		 * borrowerConverter is the User Defined Function (UDF)
		 */
		UDF1 borrowerConverter = new UDF1<String, String>() {
			
			@Override
			public String call(String input) throws Exception {
				
				if (input == null)
					return "_EMPTY_";
				else if (input.equals("MINISTRY OF FINANCE"))
					return "FINANCE";
				else 
					return input;
			}
			
		};
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);
		
		SQLContext sqlContext = new SQLContext(javaSparkContext);
		DataFrame df = sqlContext.read().json("file:///C:/ac/spark/code/sparktraining/data/world_bank.json");
		df.registerTempTable("WORLD_BANK");
		
		sqlContext.udf().register("borrowerConverter", borrowerConverter, DataTypes.StringType);
		
		DataFrame borrowerCountry = sqlContext.sql("select borrowerConverter(borrower), country_namecode from WORLD_BANK");
		borrowerCountry.show();
		
	
		javaSparkContext.close();
		javaSparkContext.stop();


	}

}
