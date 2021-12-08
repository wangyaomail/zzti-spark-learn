import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.File

object L17O1{
  def main(args: Array[String]): Unit = {
    val map = Map("Spark" -> 1, "Hadoop" -> 2, "Scala" -> 3)
    print((map - "Hadoop"))
  }
}

object L17o2{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.INFO)
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")

    val conf = new SparkConf().setAppName("a").setMaster("local")
    val stream = new StreamingContext(conf, Seconds(1))
    //val data = stream.socketTextStream("localhost",7777)
    val data = stream.socketTextStream("192.168.17.150",8888)
    //data.map(_.split(":").toList).print()
    data.print()
    stream.start()
    //stream.awaitTerminationOrTimeout()
    stream.awaitTermination()
  }
}