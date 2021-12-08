package zutsoft.spark.scala_9

import java.io._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.log4j.{ Level, Logger }

object streaming9 {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    var localProjectPath = new File("").getAbsolutePath();
    System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
    val sc = new SparkContext("local", "streaming", System.getenv("SPARK_HOME"))
    val ssc = new StreamingContext(sc, Seconds(1))
    val input = ssc.socketTextStream("localhost", 7777).map(line => line.trim().split(" "))

    val request_page = input.map(x => x(2) + ":" + x(3))
    request_page.saveAsTextFiles(localProjectPath + "/output/logs", "txt")

    ssc.start()
    ssc.awaitTerminationOrTimeout(10000l)
    ssc.stop()
  }
}
