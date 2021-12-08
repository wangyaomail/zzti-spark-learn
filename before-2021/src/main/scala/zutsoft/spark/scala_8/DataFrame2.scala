package zutsoft.spark.scala_8

import java.io._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.functions._

object DataFrame2 {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    var localProjectPath = new File("").getAbsolutePath();
    System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
    val sc = new SparkContext("local", "LoadJsonWithSparkSQL", System.getenv("SPARK_HOME"))
    val sparkSession = SparkSession.builder.master("local")
      .appName("spark session example")
      //.enableHiveSupport()
      .getOrCreate()

    println("协方差")
    val df = sparkSession.range(0, 10)
      .select(
        new Column("id"),
        rand(seed = 10).alias("a"),
        randn(seed = 27).alias("b"))
    println(df.stat.cov("a", "b"))
    println(df.stat.cov("a", "a"))
    
    println("相关性")
    val df2 = sparkSession.range(0, 10)
      .select(
        new Column("id"),
        rand(seed = 10).alias("a"),
        randn(seed = 27).alias("b"))
    println(df2.stat.corr("a", "b"))
    println(df2.stat.corr("a", "a"))
    
    println("函数")
    df.select(
       new Column("a"),
       degrees("a"),
       (pow(cos("a"), 2) + pow(sin("a"), 2)).alias("cos^2 + sin^2"))
     .show()
  }
}
