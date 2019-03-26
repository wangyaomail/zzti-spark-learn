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
    val input = args.length match { 
      case x: Int if x > 1 => sc.textFile(args(1))
      case _ => sc.parallelize(List("I have an army", "We have a Hulk"))
    }
    val words = input.flatMap(line => line.split(" "))
    args.length match {
      case x: Int if x > 2 => {
        val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
        counts.saveAsTextFile(args(2))
      }
      case _ => {
        val wc = words.countByValue()
        println(wc.mkString(","))
      }
    }
  }
}
