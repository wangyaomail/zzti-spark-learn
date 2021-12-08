package zutsoft.spark.scala_4
// 任务八：（topk）求出生日最大的5个同学的名字

import java.io._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{ Level, Logger }

object job8 {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    var localProjectPath = new File("").getAbsolutePath();
    System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
    val sc = new SparkContext("local", "Average", System.getenv("SPARK_HOME"))
    val input = sc.textFile(localProjectPath + "/input/students.data").map(line => line.trim().split("\t")).filter(_.length == 8)
    input.map(x=>(x(0), x(4).substring(5, 10).replaceAll("-", "")))
    .sortBy(_._2, false).take(5).foreach(println(_))

  }
}