import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.io.File

object Exam2_1_a {
  def main(args: Array[String]) {
    print(addInt()+addInt(1,2)+addInt(b=0));
  }
  def addInt(a: Int = 2, b: Int = 3): Int = {
    var sum: Int = 0
    sum = a + b
    return sum
  }
}
object Exam2_1_b {
  def main(args: Array[String]) {
    printStrings("x", "y", "z");
  }
  def printStrings(args: String*) = {
    var i: Int = 1;
    for (arg <- args) {
      print("[" + i + "]=" + arg+",");
      i = i + 1;
    }
  }
}
object Exam2_1_c {
  def main(args: Array[String]) {
    println(f1(5))
  }
  def f1(i: Int): Int = {
    def f2(i: Int, acc: Int): Int = {
      if (i <= 1)
        acc
      else
        f2(i - 1, i + acc)
    }
    f2(i, 1)
  }
}
object Exam2_1_d {
  def main(args: Array[String]) {
    def joint(m: String)(n: String): String = n + "-" +m
    val timesTwo = joint("x") _
    println(timesTwo("y"))
  }
}
object Exam2_1_e {
  def main(args: Array[String]) {
    val num = Set(3, 6, 9, 20, 30, 40)
    println(num.min + "," + num.max+","+num.size)
  }
}

//张祥德,RB17101,RB171,男,1997-02-10,11122223333,河南省郑州市1号,88
//张成刚,RB17102,RB171,女,1996-10-01,18837110115,河南省洛阳市2号,86
//卢伟兴,RB17103,RB171,男,1998-08-02,19999228822,河南省开封市3号,95
//杨飞龙,RB17104,RB171,男,1996-08-09,13322554455,河南省安阳市4号,91
//姜松林,RB17201,RB172,女,1997-01-03,13688552244,河南省鹤壁市1号,75
//高飞,RB17202,RB172,男,1996-08-27,13522114455,河南省新乡市2号,68
//何桦,RB17203,RB172,女,1997-12-20,13566998855,河南省焦作市3号,84
//高天阳,RB17204,RB172,男,1999-11-08,13688446622,河南省濮阳市4号,77
//周存富,RB17301,RB173,男,1996-05-28,13699552658,河南省许昌市1号,93
//罗鹏,RB17302,RB173,男,1998-03-02,13365298741,河南省漯河市2号,85
//宋立昌,RB17401,RB174,男,1995-05-28,13596325874,河南省南阳市3号,81
//杨国胜,RB17402,RB174,男,1996-03-02,13256987456,河南省信阳市4号,91
//徐子文,RB17403,RB174,男,1998-05-28,13523654789,河南省周口市5号,85
//马彦,RB17404,RB174,女,1997-03-02,13526845962,河南省郑州市6号,73

object Exam2_2_a{
  def main(args: Array[String]): Unit = {
    val source = scala.io.Source
      .fromFile("input/students.csv", "UTF-8")
      .getLines().toArray
    source
      .map(_.trim().split(","))
      .filter(_.length == 8)
      .map(x => (x(0), x(1)))
      .take(3)
      .foreach(print(_))
  }
}

object Exam2_2_b{
  def main(args: Array[String]): Unit = {
    val source = scala.io.Source
      .fromFile("input/students.csv", "UTF-8")
      .getLines().toArray
    val d = source
      .map(_.trim().split(","))
      .filter(_.length == 8)
      .map(_(1).replaceAll("RB", ""))
    println(d.max+","+d.min)
  }
}

object Exam2_2_c{
  def main(args: Array[String]): Unit = {
val source = scala.io.Source
  .fromFile("input/students.csv", "UTF-8")
  .getLines().toArray
val locate = source.map(_.trim().split(","))
  .filter(_.length == 8)
  .map(_(4).substring(0, 4))
  .distinct
  .foreach(print(_))

  }
}

object Exam2_2_d{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    val ss = SparkSession.builder().appName("exam").master("local[*]").getOrCreate()
    val result = ss.sparkContext
      .textFile("input/students.csv")
      .map(line => line.trim()
        .split(","))
      .filter(_.length == 8)
      .map(x=>x(2)+":"+x(5)+",")
      .take(3)
      .foreach(print)
  }
}

object Exam2_2_e{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    val ss = SparkSession.builder().appName("exam").master("local[*]").getOrCreate()
    val input = ss.sparkContext
      .textFile("input/students.csv")
      .map(line => line.trim()
        .split(","))
      .filter(_.length == 8)
    input.map(x=>(x(0), x(4).substring(5, 10).replaceAll("-", "")))
      .sortBy(_._2, false)
      .foreach(print(_))
  }
}

object Exam3_1{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    val ss = SparkSession.builder().appName("exam").master("local[*]").getOrCreate()

    val df = ss.read.option("header",true).csv("file:///C:\\Users\\d\\Desktop\\spark-maven-2\\input\\students.csv")
    df.createTempView("students")
    df.show()
    df.groupBy("gender")
      .count()
      .show()
    df.orderBy(desc("score"))
      .limit(1)
      .show()
    val rdd = df.rdd
      .map(x=>x.toSeq.toList)
    rdd.foreach(println)
    df.rdd
      .map(x=>x.toSeq.toList)
      .map(x=>x(0))
      .foreach(println)
    // 名字为3个字的同学数量
    val job1 = rdd
      .filter(x=>x(0).toString.length==3)
      .filter(x=>x(3).toString.equals("男"))
      .count()
    print(job1)
    // 174学生姓名链表
    rdd.map(x => (x(2), x(0)))
      .groupByKey()
      .filter(x=>x._1.equals("RB174"))
      .foreach(println)
    // 返回90分以上的同学占所有同学数量的比例
    val score90 = rdd.map(x => x(7).toString.toInt)
      .filter(x=> x.toInt>=90)
      .count()
    println(score90.toFloat/rdd.count())
  }
}
