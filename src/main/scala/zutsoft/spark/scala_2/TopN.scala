package zutsoft.spark.scala_2

import java.io._
import org.apache.spark._
import org.apache.spark.rdd.RDD

object TopN {
  def main(args: Array[String]) {
    var localProjectPath = new File("").getAbsolutePath();
    System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
    val sc = new SparkContext("local", "TopN", System.getenv("SPARK_HOME"))
    val N = 10;
    val input = sc.textFile(localProjectPath + "/input/rand_numbers.data").flatMap(line => line.trim().split("\t").map(tok => tok.toInt))
    val result = input.sortBy(k => k, true)
//    println("升序：")
//    result.take(N).foreach { println }
//    val result2 = input.sortBy(k => k, false)
//    println("降序：")
//    result2.take(N).foreach { println }
    
    println(result.take(N))
  }

}
