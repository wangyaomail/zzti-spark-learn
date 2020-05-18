package zutsoft.spark.scala_8

import java.io._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.functions._

object DataFrame1 {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    var localProjectPath = new File("").getAbsolutePath();
    System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
    val sc = new SparkContext("local", "LoadJsonWithSparkSQL", System.getenv("SPARK_HOME"))
    val sparkSession = SparkSession.builder.master("local")
      .appName("spark session example")
      //.enableHiveSupport()
      .getOrCreate()

    println("range(0, 10)")
    val df = sparkSession.range(0, 10)
    df.show()

    println("生成随机数")
    df.select(new Column("id"), rand(seed = 10).alias("uniform"), randn(seed = 27).alias("normal")).show()
    df.select(rand().alias("a"), randn().alias("b")).show()

    println("describe")
    sparkSession.range(0, 10).select(new Column("id"), rand(seed = 10).alias("uniform"), randn(seed = 27).alias("normal")).describe().show()
  }
}
