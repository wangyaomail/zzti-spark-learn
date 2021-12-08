package zutsoft.spark.scala_6

import java.io._
import java.util.Random

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD

object GroupByTest {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    var localProjectPath = new File("").getAbsolutePath();
    System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
    val sc = new SparkContext("local", "Average", System.getenv("SPARK_HOME"))

    val pairs1 = sc.parallelize(0 until 100, 100).flatMap { p =>
      val ranGen = new Random
      var arr1 = new Array[(Int, Array[Byte])](10000)
      for (i <- 0 until 10000) {
        val byteArr = new Array[Byte](1000)
        ranGen.nextBytes(byteArr)
        arr1(i) = (ranGen.nextInt(100), byteArr)
      }
      arr1
    }.cache
    println(pairs1.count)
    val groups1 = pairs1.groupByKey(5)
    println(groups1.toDebugString)
    println(groups1.count)
    println(groups1.toDebugString)
    sc.stop()
  }
}