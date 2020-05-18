package zutsoft.spark.scala_4
// 任务五：（分布）求学生的年龄分布
import java.io._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{ Level, Logger }

object job5 {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    var localProjectPath = new File("").getAbsolutePath();
    System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
    val sc = new SparkContext("local", "Average", System.getenv("SPARK_HOME"))
    val input = sc.textFile(localProjectPath + "/input/students.data").map(line => line.trim().split("\t")).filter(_.length == 8)
    input.map(2019 - _(4).substring(0, 4).toInt)
    .map(x => (x, 1))
    .groupBy(x => x._1)
    .mapValues(_.map(_._2).sum).foreach(println(_))

  }
}