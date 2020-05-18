package zutsoft.spark.scala_8

import java.io._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.functions._

object job2 {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    var localProjectPath = new File("").getAbsolutePath();
    System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
    val sc = new SparkContext("local", "job", System.getenv("SPARK_HOME"))

    val sparkSession = SparkSession.builder.master("local")
      .appName("job").getOrCreate()
    val df = sparkSession.read
      //姓名,学号,班级,性别,生日,手机号,住址
      .schema("name STRING, no STRING, cls STRING, gender STRING, birthday STRING, phone STRING, loc STRING")
      .option("header", "true")
      .csv(localProjectPath + "/input/students.csv")
    df.show()
    // 出生年月最大最小值
    println(df.sort(df("birthday")).first)
    println(df.sort(df("birthday").desc).first)

    // sql
    df.createOrReplaceTempView("students")
    val sqlDF = sparkSession.sql("select max(birthday),min(birthday) from students")
    sqlDF.show()
  }
}
