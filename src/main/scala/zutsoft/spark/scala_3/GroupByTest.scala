package zutsoft.spark.scala_3

import java.io._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.{ Level, Logger }

/**
 * GroupBy
 */
object GroupByTest {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    var localProjectPath = new File("").getAbsolutePath();
    System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
    val sc = new SparkContext("local", "WordCount", System.getenv("SPARK_HOME"))
    val a = sc.parallelize(1 to 9, 3)
    println("GroupBy:")
    a.groupBy(x => { if (x % 2 == 0) "even" else "odd" }).collect.foreach { x=>println(x+",") }
  }
}
