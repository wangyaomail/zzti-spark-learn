package zutsoft.spark.scala_3

import java.io._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.{ Level, Logger }

/**
 * mapValues
 * combineByKey
 */
object MapValuesTest {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    var localProjectPath = new File("").getAbsolutePath();
    System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
    val sc = new SparkContext("local", "WordCount", System.getenv("SPARK_HOME"))
    // mapValues
    println("mapValues")
    val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "pander", "eagle"), 2)
    val b = a.map(x => (x.length, x))
    b.mapValues("x" + _ + "x").collect().foreach { println }
    // combineByKey
    println("\n\ncombineByKey")
    val c0 = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
    val c1 = sc.parallelize(List(1,1,2,2,2,1,2,2,2), 3)
    val d = c1.zip(c0)
    val e = d.combineByKey(List(_), 
                           (x: List[String], y: String) => y :: x, 
                           (x: List[String], y: List[String]) => x ::: y)
    e.collect.foreach { println }
    // reduceByKey
    println("\n\nreduceByKeys")
    val f = a.map(x => (x.length, x))
    f.reduceByKey(_ + _).collect.foreach { println }
    // partitionBy
    println("\n\npartitionBy")
    val g = a.map(x => (x.length, x))
    g.partitionBy(new org.apache.spark.HashPartitioner(3)).collect.foreach { println }
    // cogroup
    println("\n\ncogroup")
    val h = sc.parallelize(List(1, 2, 1, 3), 1)
    val i = h.map((_, "b"))
    val j = h.map((_, "c"))
    i.cogroup(j).collect.foreach { println }
    // join
    println("\n\njoin")
    val k = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
    val l = k.keyBy(_.length)
    val m = sc.parallelize(List("dog","cat","gnu","salmon","rabbituu","turkey","wolf","bear","bee"), 3)
    val n = m.keyBy(_.length)
    l.join(n).collect.foreach { println }
  }
}
