package zutsoft.spark.scala_8

import java.io._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.log4j.{ Level, Logger }

object LoadJson2 {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    var localProjectPath = new File("").getAbsolutePath();
    System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
    val sc = new SparkContext("local", "LoadJsonWithSparkSQL", System.getenv("SPARK_HOME"))

    val sparkSession = SparkSession.builder.master("local")
      .appName("LoadWithSparkSession")
      .getOrCreate()
    println("读取csv:")
    val session1 = sparkSession.read
      .schema("a STRING, b STRING, c STRING, d STRING, e STRING, f STRING, g STRING")
      .option("header", "true")
      .csv(localProjectPath + "/input/students.csv")
    session1.show()
    println("")
    println("读取json:")
    val session2 = sparkSession.read
      .json(localProjectPath + "/input/testweet.json")
    session2.printSchema()

  }
}
