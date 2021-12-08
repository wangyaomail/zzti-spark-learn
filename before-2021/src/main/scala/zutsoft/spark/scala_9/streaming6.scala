package zutsoft.spark.scala_9

import java.io._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.log4j.{ Level, Logger }

object streaming6 {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    var localProjectPath = new File("").getAbsolutePath();
    System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
    val sc = new SparkContext("local", "streaming", System.getenv("SPARK_HOME"))
    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.checkpoint("tmp")
    val input = ssc.socketTextStream("localhost", 7777).map(line => line.trim().split(" "))

    val ipDStream = input.map(x => (x(1), 1)).reduceByKeyAndWindow(
      { (x, y) => x + y }, // 加上新进入窗口的批次中的元素
      { (x, y) => x - y }, // 移除离开窗口的老批次中的元素
      Seconds(2), // 窗口时长
      Seconds(1)) // 滑动步长
    ipDStream.print()
    ssc.start()
    ssc.awaitTerminationOrTimeout(5000l)
    ssc.stop()
  }
}
