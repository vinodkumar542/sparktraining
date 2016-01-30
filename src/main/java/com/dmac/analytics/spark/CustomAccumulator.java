package com.dmac.analytics.spark;

import java.io.Serializable;

import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class CustomAccumulator {

	public static void main(String[] args) {
					
			SparkConf sparkConfig = new SparkConf()
					.setAppName("TitanicSurvival")
					.setMaster("local[8]");
					
			
			JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);
			
			Accumulator<UIDBean> successAccumulator = javaSparkContext.accumulator(new UIDBean(), "success_uid_accumulator", new UIDAccumulator());
			Accumulator<UIDBean> failureAccumulator = javaSparkContext.accumulator(new UIDBean(), "failure_uid_accumulator", new UIDAccumulator());
			
			javaSparkContext.textFile("file:///D:/ac/data/legacy_uid.csv")
					.map(new Function<String, String>() {
			
						@Override
						public String call(String input) throws Exception {
						
							UIDBean uidBean = new UIDBean();
							uidBean.setUid(input);
							
							if (input.startsWith("28336"))
								successAccumulator.add(uidBean);
							else
								failureAccumulator.add(uidBean);
							
							return input;
						}
						
					}).count();
			
			System.out.println("Successful Aadhaars - " + successAccumulator.value().getUid());
			
			javaSparkContext.close();

	}

}

class UIDAccumulator implements AccumulatorParam<UIDBean> {

	@Override
	public UIDBean addInPlace(UIDBean first, UIDBean second) {
		
		UIDBean bean = new UIDBean();
		bean.setUid(first.getUid() + " - " + second.getUid());
		
		return bean;
	}

	@Override
	public UIDBean zero(UIDBean arg0) {
		return new UIDBean();
	}

	@Override
	public UIDBean addAccumulator(UIDBean first, UIDBean second) {
		
		UIDBean bean = new UIDBean();
		bean.setUid(first.getUid() + " - " + second.getUid());
		
		return bean;
	}
	
}


class UIDBean implements Serializable {
	
	private String uid = "";

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}
	
	
}
