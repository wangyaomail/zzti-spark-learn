package zutsoft.spark.scala_3

import java.io._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.{ Level, Logger }

/**
 * foreach
 */
object ForeachTest {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    var localProjectPath = new File("").getAbsolutePath();
    System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
    val sc = new SparkContext("local", "WordCount", System.getenv("SPARK_HOME"))
    // foreach
    println("foreach")
    val a = sc.parallelize(List("cat", "dog", "tiger", "lion", "gnu", "crocodile", "ant", "whale", "dolphin", "spider"), 3)
    a.foreach(x => println(x + "s are yummy"))
//    // saveAsTestFile
//    println("\n\nsaveAsTestFile")
//    val b = sc.parallelize(1 to 10000, 3)
//    b.saveAsTextFile("mydata_saveAsTestFile")
//    // saveAsObjectFile
//    println("\n\nsaveAsObjectFile")
//    val c = sc.parallelize(1 to 100, 3)
//    c.saveAsObjectFile("objFile")
//    val d = sc.objectFile[Int]("objFile")
//    d.collect.foreach { x => print(x + ",") }
    // collectAsMap
    println("\n\ncollectAsMap")
    val e = sc.parallelize(List(1, 2, 1, 3), 1)
    val f = e.zip(e)
    f.collect.foreach { x => print(x + ",") }
    // reduceByKeyLocally
    println("\n\nreduceByKeyLocally")
    val g = sc.parallelize(List("dog", "cat", "owl", "gnu", "ant","dear","bear","fish"), 2)
    val h = g.map(x => (x.length, x))
    h.reduceByKey(_ + _).foreach { x => print(x + ",") }
  }
}










