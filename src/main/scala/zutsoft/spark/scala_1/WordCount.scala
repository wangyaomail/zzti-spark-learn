package zutsoft.spark.scala_1

import java.io._

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object WordCount {
  def main(args: Array[String]) {
    var localProjectPath = new File("").getAbsolutePath();
    System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
    val sc = new SparkContext("local", "WordCount", System.getenv("SPARK_HOME"))
    val input = sc.textFile(localProjectPath + "/input")
    val words = input.flatMap(line => line.trim().split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey { (x, y) => x + y }
    counts.saveAsTextFile(localProjectPath + "/output/wordcount")
  }
}
