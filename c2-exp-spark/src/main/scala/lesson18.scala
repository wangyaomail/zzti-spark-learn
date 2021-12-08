import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.{BufferedReader, File, InputStreamReader}
import java.net.Socket

object l18T1{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")

    val conf = new SparkConf().setAppName("a").setMaster("local")
    val stream = new StreamingContext(conf, Seconds(1))
    val data = stream.socketTextStream("192.168.17.150",8888)
    data//.filter(_.contains("error"))
      .print()
    stream.start()
    stream.awaitTerminationOrTimeout(5000l);
    //stream.awaitTermination()
  }
}

object l18T2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")

    val conf = new SparkConf().setAppName("a").setMaster("local")
    val stream = new StreamingContext(conf, Seconds(1))
    val data = stream.textFileStream("hdfs://192.168.17.150:9000/stream/in")
    data.print()
    stream.start()
    //stream.awaitTerminationOrTimeout(10000l);
    stream.awaitTermination()
  }
}

object l18T3{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    val conf = new SparkConf().setAppName("a").setMaster("local")
    val stream = new StreamingContext(conf, Seconds(1))

    import scala.collection.mutable
    val rddQueue = mutable.Queue[RDD[Int]]()
    val data = stream.queueStream(rddQueue)
    data.print()


    stream.start()

    for(i <- 1 to 1000){
      val r = stream.sparkContext.makeRDD(1 to 100)
      r.collect()
      rddQueue.enqueue(r)
      Thread.sleep(3000)
    }

    stream.awaitTermination()


  }
}

class UUUUUUU (host:String,port:Int)
  extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  //启动的时候调用
  override def onStart(): Unit = {
    println("启动了")

    //创建一个socket
    val socket = new Socket(host, port)
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))
    //创建一个变量去读取socket的输入流的数据
    var line = reader.readLine()
    while (!isStopped() && line != null) {
      //如果接收到了数据，就使用父类的中store方法进行保存
      store(line)
      //继续读取下一行数据
      line = reader.readLine()
    }

  }

  //停止的时候调用
  override def onStop(): Unit = {
    println("停止了")
  }

}
object SparkStreamCustomReceiver extends App{
  //配置对象
  val conf = new SparkConf().setAppName("stream").setMaster("local[3]")
  //创建StreamingContext
  val ssc = new StreamingContext(conf,Seconds(5))
  //从Socket接收数据
  val lineDStream = ssc.receiverStream(new UUUUUUU("192.168.17.150",8888))
  //统计词频
  //val result = lineDStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
  lineDStream.print()
  //启动
  ssc.start()
  ssc.awaitTermination()
}

object l18T4 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    val conf = new SparkConf().setAppName("a").setMaster("local[*]")
    val stream = new StreamingContext(conf, Seconds(1))
    val data = stream.receiverStream(new UUUUUUU("192.168.17.150",8888))
    data.print()
    stream.start()
    stream.awaitTermination()
  }
}

object l18T5 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    val conf = new SparkConf().setAppName("a").setMaster("local[*]")
    val stream = new StreamingContext(conf, Seconds(2))
    stream.sparkContext.setCheckpointDir("D:\\tmp\\test")
    val data = stream.socketTextStream("192.168.17.150",8888)
    val data2 = data.flatMap(_.split(" ")).map((_,1))
    data2.print()
    val state = data2.updateStateByKey[Int]((values:Seq[Int],state:Option[Int])=>{
      // values为当前批次单词频数
      // state为之前批次单词频数
      val currentCount = values.foldLeft(0)(_+_)
      //getOrElse如果有值则获取值，如果没有则使用默认值
      val stateCount = state.getOrElse(0)
      //返回新的状态
      Some(currentCount+stateCount)
    })
    state.print()


    stream.start()
    stream.awaitTermination()
  }
}
import org.apache.spark.rdd._

object l18T6 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    val conf = new SparkConf().setAppName("a").setMaster("local[*]")
    val stream = new StreamingContext(conf, Seconds(2))
    val rddlocal = stream.sparkContext.makeRDD(1 to 26)
    val rddlocal2 = rddlocal.map(x=>(x,"hello"))
    val data = stream.socketTextStream("192.168.17.150",8888)
    val result = data.filter(x=>x!="").map(x=>(x.toInt,1))
    result.transform(rdd=>{
      val result = rdd.leftOuterJoin(rddlocal2)
      result
//      r.zipPartitions(rddlocal, x=>{
//        x.
//      })
    }).print()

    stream.start()
    stream.awaitTermination()
  }

}

object l18T7 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    val conf = new SparkConf().setAppName("a").setMaster("local[*]")
    val stream = new StreamingContext(conf, Seconds(1))
    val data = stream.socketTextStream("192.168.17.150",8888)
    val words = data.flatMap(_.split(" ")).map((_,1))
    words.foreachRDD(x=>x.groupBy(_._1).foreach(println))
    val window = words.reduceByKeyAndWindow(
      (a:Int,b:Int)=>a+b,//reduce函数：将值进行叠加
      Seconds(4),//窗口的时间间隔，大小为3
      Seconds(2)//窗口滑动的时间间隔，步长为2
    )
    //window.foreachRDD(println)

    stream.start()
    stream.awaitTermination()
  }
}

