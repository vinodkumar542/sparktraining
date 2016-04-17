package com.dmac.analytics.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import scala.Function1;
import scala.Function2;
import scala.Option;
import scala.PartialFunction;
import scala.Tuple2;
import scala.Tuple3;
import scala.Predef.$less$colon$less;
import scala.collection.GenIterable;
import scala.collection.GenSeq;
import scala.collection.GenTraversable;
import scala.collection.GenTraversableOnce;
import scala.collection.Iterable;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.collection.Traversable;
import scala.collection.TraversableOnce;
import scala.collection.TraversableView;
import scala.collection.generic.CanBuildFrom;
import scala.collection.generic.FilterMonadic;
import scala.collection.generic.GenericCompanion;
import scala.collection.immutable.IndexedSeq;
import scala.collection.immutable.List;
import scala.collection.immutable.Map;
import scala.collection.immutable.Range;
import scala.collection.immutable.Set;
import scala.collection.immutable.Stream;
import scala.collection.immutable.Vector;
import scala.collection.mutable.Buffer;
import scala.collection.mutable.Builder;
import scala.collection.mutable.StringBuilder;
import scala.collection.parallel.Combiner;
import scala.collection.parallel.ParIterable;
import scala.math.Numeric;
import scala.math.Ordering;
import scala.reflect.ClassTag;
import scala.runtime.BoxedUnit;
import scala.runtime.Nothing$;

public class SparkDataFrames {

	
	
	public static void main(String args[])
	{
		
		
		SparkConf sparkConfig = new SparkConf()
											.setAppName("SparkDataFrames")
											.setMaster("local[5]");
			
			//.setMaster("spark://SCHMAC-TESTER-4.local:7077");
			//.setMaster("spark://52.24.58.38:7077");
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);
		
		SQLContext sqlContext = new SQLContext(javaSparkContext);
		DataFrame df = sqlContext.read().json("file:///Users/koteshwar/world_bank.json");

		//df.printSchema();
		df.show();
		
		//df.select("borrower").show();
		//df.select("borrower", "country_namecode").show();
		//df.select(df.col("borrower"), df.col("country_namecode")).show();
		//df.select(df.col("borrower"), df.col("grantamt").plus(1)).show();
		//df.select(df.col("borrower"), df.col("grantamt").gt(15000000)).show();
		
		//df.groupBy("countryshortname").count().show();
		
		
		//DataFrame dataFrame = sqlContext.sql("");
		
		javaSparkContext.close();
		javaSparkContext.stop();

	}
}
