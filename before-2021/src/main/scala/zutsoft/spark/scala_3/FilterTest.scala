package zutsoft.spark.scala_3

import java.io._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.{ Level, Logger }

/**
 *
 */
object FilterTest {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    var localProjectPath = new File("").getAbsolutePath();
    System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
    val sc = new SparkContext("local", "WordCount", System.getenv("SPARK_HOME"))
    val a = sc.parallelize(1 to 10, 3)
    val b = a.filter(_ % 2 == 0)
    // filter
    println("filter:")
    a.collect.foreach { x => print(x + ",") }
    println("")
    b.collect.foreach { x => print(x + ",") }
    // distinct
    println("\n\ndistinct:")
    val c = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu", "Rat"), 2)
    c.distinct.collect.foreach { x => print(x + ",") }
    // subtract
    println("\n\nsubtract:")
    val d = sc.parallelize(1 to 9, 3)
    val e = sc.parallelize(1 to 3, 3)
    val f = d.subtract(e)
    f.collect.foreach { x => print(x + ",") }
    // sample
    println("\n\nsample:")
    val g = sc.parallelize(1 to 10000, 3)
    g.sample(false, 0.01, 8765).foreach { x => print(x + ",") }
    println(g.count)
    //println(g.sample(false, 0.01, 8765).count)
    // takeSample
    println("\n\ntakeSample:")
    val h = sc.parallelize(1 to 1000, 3)
    h.takeSample(true, 10, 1).foreach { x => print(x + ",") }
    // cache
    println("\n\ncache:")
    val i = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu", "Rat"), 2)
    println(i.getStorageLevel)
    println(i.cache)
    println(i.getStorageLevel)
  }
}
