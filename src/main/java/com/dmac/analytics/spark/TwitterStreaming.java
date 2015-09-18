package com.dmac.analytics.spark;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.Status;

public class TwitterStreaming {

	public static void main(String[] args) {
		    System.setProperty("twitter4j.oauth.consumerKey", "icZ1EX1WWeFJ925oO0ffMW0ks");
		    System.setProperty("twitter4j.oauth.consumerSecret", "1Ma8L69gWUIWICd9a6Mg6rU30OiofcFC6lp5Zq49xGaqsxZ9SI");
		    System.setProperty("twitter4j.oauth.accessToken", "3604186573-C5fVrJ7EDyuPcsEaMmiW5kB2KaIr5pqIUTKwmkj");
		    System.setProperty("twitter4j.oauth.accessTokenSecret", "rT4HPzSexcBnoxDErfMFKkzb7kQ9pqNIcwXzZkjgJWw8x");
		
		
		SparkConf config = new SparkConf()
								.setAppName("ReadTwitter")
								.setMaster("local[8]");

		JavaStreamingContext jsc = new JavaStreamingContext(config, Durations.seconds(10));
		
		JavaDStream<Status> tweets =  TwitterUtils.createStream(jsc);

		JavaDStream<String> statuses = tweets.map(
			      new Function<Status, String>() {
			        public String call(Status status) { 
			        	System.out.println("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n");
			        	System.out.println("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n");
			        	
			        	
			        	String tweets = status.getText();
			        	if (tweets.equals("LOHITHAAA"))
			        		return status.getText();
			        
			        	System.out.println(status.getText());
			        	return ""; 
			        	
			        }
			      }
			    );
	    
		statuses.print();

		jsc.start();
		jsc.awaitTermination();

	}

}
