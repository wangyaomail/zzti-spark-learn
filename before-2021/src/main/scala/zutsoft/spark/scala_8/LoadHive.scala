package zutsoft.spark.scala_8

import java.io._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.log4j.{ Level, Logger }

object LoadHive {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    var localProjectPath = new File("").getAbsolutePath();
    System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
    val sc = new SparkContext("local", "LoadJsonWithSparkSQL", System.getenv("SPARK_HOME"))
    val sparkSession = SparkSession.builder.master("local")
      .appName("spark session example")
      .enableHiveSupport()
      .getOrCreate()
  }
}
