/**
 * Created by Aravindh Chinnasamy
 */

import org.apache.spark.{SparkContext, SparkConf}

object ReadCSVFile {

  def main(args : Array[String]) : Unit = {

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("ReadCSV")

    val sparkContext = new SparkContext(conf)
    val csvFile = sparkContext.textFile("file:///C:/ac/spark/code/sparktraining/data/titanic3.csv")
    csvFile.foreach(println)
    sparkContext.stop()
  }
}
