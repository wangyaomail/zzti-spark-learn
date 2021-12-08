package zutsoft.spark.scala_3

import java.io._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.{ Level, Logger }

/**
 * lookup
 */
object LookupTest {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    var localProjectPath = new File("").getAbsolutePath();
    System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
    val sc = new SparkContext("local", "WordCount", System.getenv("SPARK_HOME"))
    // lookup
    println("lookup")
    val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
    val b = a.map(x => (x.length, x))
    b.lookup(5).foreach(x => println(x + "s are yummy"))
    // count
    println("\n\ncount")
    val c = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog"), 2)
    println(c.count)
    // top
    println("\n\ntop")
    val d = sc.parallelize(Array(6, 9, 4, 7, 5, 8), 2)
    d.takeOrdered(2).foreach(println)
    // first 
    println("\n\nfirst："+d.first)
    // reduce
    println("\n\nreduce")
    val e = sc.parallelize(1 to 100, 3)
    println(e.reduce(_ + _))
    // fold
    println("\n\nfold")
    val f = sc.parallelize(List(1, 2, 3), 1)
    println(f.fold(-1)(_ + _))
    // aggregate
    println("\n\naggregate")
    val g = sc.parallelize(List(1, 2, 3, 4, 5, 6), 2)
    def myfunc(index: Int, iter: Iterator[(Int)]): Iterator[String] = {
      iter.toList.map(x => "[partID:" + index + ", val: " + x + "]").iterator
    }
    g.mapPartitionsWithIndex(myfunc).collect.foreach(x => println(x + "s are yummy"))
    println(g.aggregate(0)(math.max(_, _), _ + _))
  }
}










