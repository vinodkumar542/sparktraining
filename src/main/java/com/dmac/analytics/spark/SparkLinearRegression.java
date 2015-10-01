package com.dmac.analytics.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;

import scala.Tuple2;

public class SparkLinearRegression {

	public static void main(String[] args) {
		
		
		// local - runs the spark locally
				// local[5] - runs the spark locally with 5 threads
				// Application name identifies the application on the cluster manager UI
		SparkConf sparkConfig = new SparkConf()
								.setAppName("LinearRegression")
								.setMaster("local[5]");
		
		JavaSparkContext sc = new JavaSparkContext(sparkConfig);
		
		String path = "/Users/tester/Desktop/gaga.csv";
		
		JavaRDD<String> data = sc.textFile(path);
		
		JavaRDD<LabeledPoint> parsedData = data.map(new Function<String, LabeledPoint>() {
			public LabeledPoint call(String line) {
                List<String> featureList = Arrays.asList(line.trim().split(",")); 
                
                String placement = featureList.get(0).toLowerCase();
                String prominence = featureList.get(1).toLowerCase();
                String pricing = featureList.get(2).toLowerCase();
                String eyeLevel = featureList.get(3).toLowerCase();
                String customerPurchase = featureList.get(4).toLowerCase();
                
                double placementQualifier = 0.0;
                double prominenceQualifer = 0.0;
                double pricingQualifer = 0.0;
                double eyeLevelQualifier = 0.0;
                double custPurchaseQualifier = 0.0;
                
                if (placement.equals("end_rack"))
                	placementQualifier = 1;
                else if (placement.equals("cd_spec"))
                	placementQualifier = 2;
                else if(placement.equals("std_rack"))
                	placementQualifier = 3;
                
                prominenceQualifer = Double.parseDouble(prominence);   
                pricingQualifer = Double.parseDouble(pricing);
                
                if(eyeLevel.equals("true"))
                	eyeLevelQualifier = 1;
                else 
                	eyeLevelQualifier = 0;
                
                if(customerPurchase.equals("yes"))
                	custPurchaseQualifier = 1;
                else
                	custPurchaseQualifier = 0;
                
                return new LabeledPoint(custPurchaseQualifier, Vectors.dense(prominenceQualifer,pricingQualifer,eyeLevelQualifier,placementQualifier)); 
			}
		});
		
		 int numIterations = 10;
		final LinearRegressionModel model = 
			      LinearRegressionWithSGD.train(JavaRDD.toRDD(parsedData), numIterations);
		

//		model.predict(testData);
		
/*		 JavaRDD<Tuple2<Double, Double>> valuesAndPreds = parsedData.map(
			      new Function<LabeledPoint, Tuple2<Double, Double>>() {
			        public Tuple2<Double, Double> call(LabeledPoint point) {
			          double prediction = model.predict(point.features());
			          return new Tuple2<Double, Double>(prediction, point.label());
			        }
			      }
			    );
		 
		   double MSE = new JavaDoubleRDD(valuesAndPreds.map(
				      new Function<Tuple2<Double, Double>, Object>() {
				        public Object call(Tuple2<Double, Double> pair) {
				          return Math.pow(pair._1() - pair._2(), 2.0);
				        }
				      }
				    ).rdd()).mean();*/

//		   System.out.println( MSE);
	}

}
