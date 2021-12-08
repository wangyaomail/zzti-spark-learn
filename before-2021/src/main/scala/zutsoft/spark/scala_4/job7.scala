package zutsoft.spark.scala_4
// 任务七：（索引）索引出相同生日下同学的姓名链表
import java.io._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{ Level, Logger }

object job7 {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    var localProjectPath = new File("").getAbsolutePath();
    System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
    val sc = new SparkContext("local", "Average", System.getenv("SPARK_HOME"))
    val input = sc.textFile(localProjectPath + "/input/students.data").map(line => line.trim().split("\t")).filter(_.length == 8)
    input.map(x => (x(4).substring(5, 10), x(0)))
    .groupByKey().foreach(println)

  }
}