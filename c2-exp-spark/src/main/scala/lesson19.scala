import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.File

object l19T1{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    val conf = new SparkConf().setAppName("a").setMaster("local[*]")
    val stream = new StreamingContext(conf, Seconds(1))
    // guangbo
    val gb = stream.sparkContext.broadcast(Seq("a","c"))
    val acc = stream.sparkContext.longAccumulator("acc")
    //stream.sparkContext.register(acc)
    val data = stream.socketTextStream("192.168.17.150",8888)
    data.foreachRDD(rdd=>{
      println("____________")
      var gb_value = rdd.filter(x=>gb.value.contains(x))
      gb_value.foreach(x=>acc.add(1))
      println(acc.value)
    })


    stream.start()
    stream.awaitTermination()
  }
}