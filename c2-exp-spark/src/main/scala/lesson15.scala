import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import java.io.File

object sql1{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    val ss = SparkSession
      .builder()
      .appName("sql1")
      .master("local[*]").getOrCreate()
    //读取数据
    //val dataFrame = ss.read.json("file:///C:\\Users\\d\\Desktop\\spark-maven-2\\src\\main\\resources\\people.json")
    val dataFrame = ss.read.option("header","true").csv("file:///C:\\Users\\d\\Desktop\\spark-maven-2\\input\\students.csv")
    //展示数据
    dataFrame.show()
    //关闭连接
    ss.stop()

  }
}

object sql2{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")

    val ss = SparkSession.builder().appName("sql2").master("local").getOrCreate()
    //val data = ss.read.json("file:///C:\\Users\\wy\\Desktop\\spark-maven\\input\\people.json")
    //data.show()
    val data = ss.read.option("header","true").csv("file:///C:\\Users\\wy\\Desktop\\spark-maven\\input\\students.csv")

    //data.createTempView("students")
    //ss.sql("select phone from students").show()
    //
    //data.show()
    //data.printSchema()
    //data.select($"age"+1).show()
  }
}

