package zutsoft.spark.scala_8

import java.io._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.log4j.{ Level, Logger }

object sql1 {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    var localProjectPath = new File("").getAbsolutePath();
    System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
    val sc = new SparkContext("local", "LoadJsonWithSparkSQL", System.getenv("SPARK_HOME"))
    val sparkSession = SparkSession.builder.master("local")
      .appName("spark session example")
      .getOrCreate()
    val df = sparkSession.read.json(localProjectPath + "/input/sql/people.json")
    df.show()
    df.createOrReplaceTempView("people")
    val sqlDF = sparkSession.sql("SELECT * FROM people")
    sqlDF.show()
  }
}
