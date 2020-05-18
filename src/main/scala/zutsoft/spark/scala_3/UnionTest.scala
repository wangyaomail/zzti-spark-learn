package zutsoft.spark.scala_3

import java.io._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.{ Level, Logger }

/**
 * union
 * cartesian
 */
object UnionTest {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    var localProjectPath = new File("").getAbsolutePath();
    System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
    val sc = new SparkContext("local", "WordCount", System.getenv("SPARK_HOME"))
    val a = sc.parallelize(1 to 3, 1)
    val b = sc.parallelize(5 to 7, 1)
    println("union:")
    (a ++ b).distinct.collect().foreach { x=>print(x+",") }
    println("\n\ncartesian:")
    a.cartesian(b).collect().foreach { x=>print(x+",") }
  }
}
