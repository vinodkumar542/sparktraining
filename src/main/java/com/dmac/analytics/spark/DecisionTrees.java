package com.dmac.analytics.spark;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.util.MLUtils;

public class DecisionTrees {

	public static void main(String args[]) {

		// local - runs the spark locally
		// local[5] - runs the spark locally with 5 threads
		// Application name identifies the application on the cluster manager UI
		SparkConf sparkConfig = new SparkConf()
						.setAppName("DecisionTrees")
						.setMaster("local[5]");
		
		
		JavaSparkContext sc = new JavaSparkContext(sparkConfig);
		JavaRDD<LabeledPoint> data = sc.textFile("file:///Users/tester/Desktop/gaga.csv").map(new Function<String, LabeledPoint>() {

			@Override
			public LabeledPoint call(String v1) throws Exception {
                List<String> featureList = Arrays.asList(v1.trim().split(",")); 
                
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
		
	/*	String datapath = "/Users/tester/Desktop/gaga.csv";
		JavaRDD<LabeledPoint> data = MLUtils.loadLabeledData(sc.sc(),datapath).toJavaRDD(); */
		
		JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.7, 0.3});
		JavaRDD<LabeledPoint> trainingData = splits[0];
		JavaRDD<LabeledPoint> testData = splits[1];
		
		Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
		
		
		data.foreach(z -> System.out.println(z));
		//MLUtils.de
		
//		JavaRDD<LabeledPoint> source  = null;
		String impurity = "gini";
		int maxDepth = 15;
		int maxBins = 32;
		
		
		DecisionTreeModel dtm = DecisionTree.trainClassifier(trainingData, 2, categoricalFeaturesInfo, impurity, maxDepth, maxBins);
		
		JavaPairRDD<Double, Double> predictionAndLabel =
				  testData.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
				    @Override
				    public Tuple2<Double, Double> call(LabeledPoint p) {
				      return new Tuple2<Double, Double>(dtm.predict(p.features()), p.label());
				    }
				  });
		
		Double testErr =
				  1.0 * predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
				    @Override
				    public Boolean call(Tuple2<Double, Double> pl) {
				      return !pl._1().equals(pl._2());
				    }
				  }).count() / testData.count();
				System.out.println("Test Error: " + testErr);
				System.out.println("Learned classification tree model:\n" + dtm.toDebugString());

		dtm.save(sc.sc(), "myModelPath");
		DecisionTreeModel sameModel = DecisionTreeModel.load(sc.sc(), "myModelPath");
		//dtm.predict(features);
		
	}
	
}
