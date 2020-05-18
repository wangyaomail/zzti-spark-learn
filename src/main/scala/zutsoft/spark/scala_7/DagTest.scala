package zutsoft.spark.scala_7

import java.io._
import java.util.Random

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD

object DagTest {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    var localProjectPath = new File("").getAbsolutePath();
    System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
    val sc = new SparkContext("local", "Average", System.getenv("SPARK_HOME"))

    val data = sc.parallelize(List("a c", "a b", "b c", "b d", "c d"), 2)
    val wordcount = data.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).map(x => (x._2, x._1)).reduceByKey(_ + _)
    val data2 = sc.parallelize(List("a c", "a b", "b c", "b d", "c d"), 2)
    val wordcount2 = data2.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).map(x => (x._2, x._1)).reduceByKey(_ + _)
    wordcount.join(wordcount2).collect()
  }
}