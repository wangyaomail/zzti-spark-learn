package zutsoft.spark.scala_2

import java.io._
import org.apache.spark._
import org.apache.spark.rdd.RDD

object MaxMin {
  def main(args: Array[String]) {
    var localProjectPath = new File("").getAbsolutePath();
    System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
    val sc = new SparkContext("local", "MaxMin", System.getenv("SPARK_HOME"))
    val input = sc.textFile(localProjectPath + "/input/rand_numbers.data").flatMap(line => line.trim().split("\t").map(tok => tok.toInt))
    val (max, min) = (input.max, input.min)
    println("max:", max)
    println("min:", min)
  }

  def computeAvg(input: RDD[Int]) = {
    input.aggregate((0, 0))(
      (x, y) => (x._1 + y, x._2 + 1),
      (x, y) => (x._1 + y._1, x._2 + y._2))
  }
}