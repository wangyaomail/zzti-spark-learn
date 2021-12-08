import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.File

object l20T1{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    //val conf = new SparkConf().setAppName("a").setMaster("local[*]")
    val spark = SparkSession.builder().master("local[*]")
      .appName("a").getOrCreate()
    import spark.implicits._
    val stream = spark.readStream.format("socket")
      .option("host","192.168.17.150")
      .option("port",8888)
      .load()
    val df = stream.flatMap(x=>{
//      val ret = x.getAs[String]("value").split(" ")
//        .map(y=>(y,1))
      val ret = x.getString(0).split(" ")
              .map(y=>(y,1))
      ret.foreach(print)
      ret
    }).toDF("word","number")
    df.createTempView("words")
    df.printSchema()
    val df2 = spark.sql("select word from words")

    val result = df2
      .writeStream
      .trigger(Trigger.ProcessingTime(2))
      .outputMode("append")
      .format("console")
      .start()
    result.awaitTermination()

  }
}
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

object l20T2{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("haha")
      .getOrCreate()
    val dataFrame = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 0.1, -1.0)),
      (1, Vectors.dense(2.0, 1.0, 1.0)),
      (2, Vectors.dense(3.0, 10.1, 3.0))
    )).toDF("id", "features")

    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    //计算汇总量，生成MinMaxScalerModel
    val scalerModel = scaler.fit(dataFrame)

    // rescale each feature to range [min, max].
    val scaledData = scalerModel.transform(dataFrame)
    //输出到控制台
    scaledData.select("features", "scaledFeatures").show()
  }

}