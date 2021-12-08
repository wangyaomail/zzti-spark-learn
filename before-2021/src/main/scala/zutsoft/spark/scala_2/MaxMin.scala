package zutsoft.spark.scala_2

import java.io._
import org.apache.spark._
import org.apache.spark.rdd.RDD

object MaxMin {
  def main(args: Array[String]) {
    var localProjectPath = new File("").getAbsolutePath();
    System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
    val sc = new SparkContext("local", "MaxMin", System.getenv("SPARK_HOME"))
    val input = sc.textFile(localProjectPath + "/input/rand_numbers.data")
      .flatMap(line => line.trim().split("\t").map(tok => tok.toInt))
    val (max, min) = (input.max, input.min)
    println("max:", max)
    println("min:", min)
  }
}