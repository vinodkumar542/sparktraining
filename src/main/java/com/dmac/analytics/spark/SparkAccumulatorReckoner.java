package com.dmac.analytics.spark;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SparkAccumulatorReckoner {

	public static void main(String args[]) {

		SparkConf sparkConfig = new SparkConf()
						.setAppName("TitanicSurvival")
						.setMaster("local[8]");
						
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);
		
		Accumulator<Integer> survivedAccumulator = javaSparkContext.accumulator(0, "survived_accumulator");
		Accumulator<Integer> deadAccumulator = javaSparkContext.accumulator(0, "dead_accumulator");
		
		long totalCount = javaSparkContext.textFile("file:///Users/koteshwar/titanic3.csv")
						.map(new Function<String, String>() {

							@Override
							public String call(String input) throws Exception {
								
								System.out.println(survivedAccumulator.value());
								String[] splittedInput = input.split(",");
								
								if (splittedInput[1].equals("1"))
									survivedAccumulator.add(new Integer(1));
								else
									deadAccumulator.add(new Integer(1));
								
								return splittedInput[1];
							}
							
						}).count();

		System.out.println("Survived People - " + survivedAccumulator.value());

		System.out.println("People Dead - " + deadAccumulator.value());
		System.out.println("Total Count - " + totalCount);
		
		javaSparkContext.close();
	}
}
