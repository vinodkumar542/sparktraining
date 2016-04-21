package com.dmac.analytics.cassandra;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.SomeColumns;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;


public class SparkCassandra {

	public static void main(String[] args) {
		
		 SparkConf conf = new SparkConf();
		 conf.setAppName("SparkCassandra");
		 conf.setMaster("local[8]");
		 conf.set("spark.cassandra.connection.host", "localhost");
						        
	        
		 CassandraConnector connector = CassandraConnector.apply(conf);
		 Session session = connector.openSession();
		 //session.execute("CREATE KEYSPACE uidai WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}");
		 //session.execute("CREATE TABLE uidai.resident_detail (id INT PRIMARY KEY, name TEXT)");
		 
		 session.execute("insert into uidai.otp (uid, otp) VALUES (17777536, '777')");
		 
		 
		 // ---------------------------------------------------------------------------------------------------------------------------------------
		 
		 
		 
		 JavaSparkContext javaSparkContext =  new JavaSparkContext(conf);
			
		 CassandraTableScanJavaRDD<CassandraRow> tableScan = javaFunctions(javaSparkContext).cassandraTable("uidai", "otp");
		 tableScan.map(new Function<CassandraRow, String>() {

			@Override
			public String call(CassandraRow cassandraRow) throws Exception {
				return cassandraRow.toString();
			}
		});
		 

		 // scala way - tableScan.select(SomeColumns("", ""));
		 session.close();
	}

}






