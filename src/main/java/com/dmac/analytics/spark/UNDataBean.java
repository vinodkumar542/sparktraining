package com.dmac.analytics.spark;

import java.io.Serializable;

public class UNDataBean implements Serializable {

	private String country 			= "";
	
	private String commodityCode 	= "";
	
	private String commodity 		= "";

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getCommodityCode() {
		return commodityCode;
	}

	public void setCommodityCode(String commodityCode) {
		this.commodityCode = commodityCode;
	}

	public String getCommodity() {
		return commodity;
	}

	public void setCommodity(String commodity) {
		this.commodity = commodity;
	}
	
	
}
