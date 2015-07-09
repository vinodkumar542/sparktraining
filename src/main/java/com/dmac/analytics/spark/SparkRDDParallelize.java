package com.dmac.analytics.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;

public class SparkRDDParallelize {

	public static void main(String args[]) {

		SparkConf sparkConfig = new SparkConf()
						.setAppName("ReadDataFromArray")
						.setMaster("local[5]");
						
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);

		
		List<LatLong> locationList = new ListSource().retrieveList();
		
		JavaRDD<LatLong> locRDD = javaSparkContext.parallelize(locationList);
		
		locRDD.collect().forEach(z -> System.out.println(z.getId()));
		javaSparkContext.close();
	}
}


class ListSource {
	
	public List<LatLong> retrieveList() {
		List<LatLong> listSource = new ArrayList<>();
		
		listSource.add(new LatLong("1", "82°30′N", "62°20′W", "Nunavut"));
		listSource.add(new LatLong("2", "82°30′N", "62°20′W", "Nunavut"));
		listSource.add(new LatLong("3", "82°30′N", "62°20′W", "Nunavut"));
		listSource.add(new LatLong("4", "82°30′N", "62°20′W", "Nunavut"));
		listSource.add(new LatLong("5", "82°30′N", "62°20′W", "Nunavut"));
		listSource.add(new LatLong("6", "82°30′N", "62°20′W", "Nunavut"));
		listSource.add(new LatLong("7", "82°30′N", "62°20′W", "Nunavut"));
		listSource.add(new LatLong("8", "82°30′N", "62°20′W", "Nunavut"));
		listSource.add(new LatLong("9", "71°18′N", "156°46′W", "Alaska"));
		listSource.add(new LatLong("10", "82°30′N", "62°20′W", "Nunavut"));
		listSource.add(new LatLong("11", "82°30′N", "62°20′W", "Nunavut"));
		return listSource;
	}
	 
}


class LatLong implements Serializable{
	
	
	private String id = "";
	private String latitude = "";
	
	private String longitude = "";
	
	private String name = "";

	public LatLong(String _id, String _latitude, String _longitude, String _name) {
		this.id = _id;
		this.latitude = _latitude;
		this.longitude = _longitude;
		this.name = _name;
	}
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getLatitude() {
		return latitude;
	}

	public void setLatitude(String latitude) {
		this.latitude = latitude;
	}

	public String getLongitude() {
		return longitude;
	}

	public void setLongitude(String longitude) {
		this.longitude = longitude;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	
}
