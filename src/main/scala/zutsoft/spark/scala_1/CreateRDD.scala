package zutsoft.spark.scala_1

import java.io._
import org.apache.spark._
import org.apache.spark.SparkContext._

object CreateRDD {
  def main(args: Array[String]) {
    var localProjectPath = new File("").getAbsolutePath();
    System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
    val sc = new SparkContext("local", "WordCount", System.getenv("SPARK_HOME"))
    // 从内存创建
    val linesA = sc.parallelize(List("pandas", "i like pandas"))
    println(linesA)
    // 从文件创建
    val linesB = sc.textFile("/path/to/README.md")
    println(linesB)
  }
}
