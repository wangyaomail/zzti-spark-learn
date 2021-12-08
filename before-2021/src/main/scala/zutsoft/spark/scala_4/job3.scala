package zutsoft.spark.scala_4
// （去重）求学生中出现的所有姓氏
import java.io._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{ Level, Logger }

object job3 {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    var localProjectPath = new File("").getAbsolutePath();
    System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
    val sc = new SparkContext("local", "Average", System.getenv("SPARK_HOME"))
    val input = sc.textFile(localProjectPath + "/input/students.data").map(line => line.trim().split("\t")).filter(_.length == 8)
    val dates = input
    .map(_(0).substring(0, 1))
    .distinct.foreach(print(_))
  }
}