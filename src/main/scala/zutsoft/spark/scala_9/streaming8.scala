package zutsoft.spark.scala_9

import java.io._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.log4j.{ Level, Logger }

object streaming8 {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    var localProjectPath = new File("").getAbsolutePath();
    System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
    val sc = new SparkContext("local", "streaming", System.getenv("SPARK_HOME"))
    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.checkpoint(localProjectPath + "/output/checkpoint")
    val input = ssc.socketTextStream("localhost", 7777).map(line => line.trim().split(" "))

    def updateRunningSum(values: Seq[String], state: Option[String]) = {
      Some(state.getOrElse("") + values)
    }
    val responseCodeDStream = input.map(x => (x(3), "正"))
    val responseCodeCountDStream = responseCodeDStream.updateStateByKey(updateRunningSum _)
    responseCodeCountDStream.print()
    ssc.start()
    ssc.awaitTerminationOrTimeout(20000l)
    ssc.stop()
  }
}
