package zutsoft.spark.scala_3

import java.io._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.{ Level, Logger }

/**
 * map
 * flatMap
 * nmapPartitions
 * glom
 */
object MapTest {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    var localProjectPath = new File("").getAbsolutePath();
    System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
    val sc = new SparkContext("local", "WordCount", System.getenv("SPARK_HOME"))
    val linesA = sc.parallelize(
      List(List(List("V0", "V1"), List("V2", "V3")), List(List("U0", "U1"), List("U2", "U3"))),2)
    println("map:")
    linesA.map(x => x+"@").foreach { x => print(x + ",") }
    println("\n\nflatMap:")
    linesA.flatMap(x => x).flatMap(x=>x).foreach { x => print(x + ",") }
    println("\n\nmapPartitions:")
    linesA.mapPartitions(x => x).foreach { x => print(x + ",") }

    val linesB = sc.parallelize(List("V1", "V987652", "V3", "V4", "V5", "V6"), 2)
    println("\n\nmap:")
    linesB.map(x => x).foreach { x => println(x.mkString(",")) }
    println("\n\nglom:")
    linesB.glom().foreach { x => println(x.mkString(",")) }
  }
}
