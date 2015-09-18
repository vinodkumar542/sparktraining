package com.dmac.analytics.spark;

import java.io.Serializable;

public class TitanicBean {

	
	private String className 		= 	"";
	
	private String survived	 		= 	"";
	
	private String name		 		= 	"";
	
	private String sex		 		= 	"";
	
	private String age			 	= 	"";
	
	private String sibsp			=	"";

	private String parch			=	"";
	
	private String ticket			=	"";
	
	private String fare				=	"";
	
	private String cabin			=	"";
	
	private String embarked			=	"";
	
	private String boat				=	"";
	
	private String body				=	"";
	
	private String destination		=	"";

	public TitanicBean() {
		super();
	}
	
	public TitanicBean(String _className, String _survived, String _name) {
		this.className = _className;
		this.survived = _survived;
		this.name = _name;
	}
	
	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public String getSurvived() {
		return survived;
	}

	public void setSurvived(String survived) {
		this.survived = survived;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSex() {
		return sex;
	}

	public void setSex(String sex) {
		this.sex = sex;
	}

	public String getAge() {
		return age;
	}

	public void setAge(String age) {
		this.age = age;
	}

	public String getSibsp() {
		return sibsp;
	}

	public void setSibsp(String sibsp) {
		this.sibsp = sibsp;
	}

	public String getParch() {
		return parch;
	}

	public void setParch(String parch) {
		this.parch = parch;
	}

	public String getTicket() {
		return ticket;
	}

	public void setTicket(String ticket) {
		this.ticket = ticket;
	}

	public String getFare() {
		return fare;
	}

	public void setFare(String fare) {
		this.fare = fare;
	}

	public String getCabin() {
		return cabin;
	}

	public void setCabin(String cabin) {
		this.cabin = cabin;
	}

	public String getEmbarked() {
		return embarked;
	}

	public void setEmbarked(String embarked) {
		this.embarked = embarked;
	}

	public String getBoat() {
		return boat;
	}

	public void setBoat(String boat) {
		this.boat = boat;
	}

	public String getBody() {
		return body;
	}

	public void setBody(String body) {
		this.body = body;
	}

	public String getDestination() {
		return destination;
	}

	public void setDestination(String destination) {
		this.destination = destination;
	}
	
	
	
}
