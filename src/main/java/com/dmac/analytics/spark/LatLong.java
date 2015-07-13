package com.dmac.analytics.spark;

import java.io.Serializable;

public class LatLong implements Serializable{
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 922482783473635209L;
	
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