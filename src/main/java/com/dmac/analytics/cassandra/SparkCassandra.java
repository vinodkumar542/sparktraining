package com.dmac.analytics.cassandra;

import org.apache.spark.SparkConf;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;

public class SparkCassandra {

	public static void main(String[] args) {
		
		 SparkConf conf = new SparkConf();
		 conf.setAppName("SparkCassandra");
		 conf.setMaster("local[8]");
		 conf.set("spark.cassandra.connection.host", "localhost");
						        
	        
		 CassandraConnector connector = CassandraConnector.apply(conf);
		 Session session = connector.openSession();
		 session.execute("CREATE KEYSPACE uidai WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}");
		 session.execute("CREATE TABLE uidai.resident_detail (id INT PRIMARY KEY, name TEXT)");	 
		 session.close();
	}

}
