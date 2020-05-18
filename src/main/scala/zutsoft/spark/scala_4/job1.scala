package zutsoft.spark.scala_4
// 统计男生和女生的总人数
import java.io._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{ Level, Logger }

object job1 {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    var localProjectPath = new File("").getAbsolutePath();
    System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
    val sc = new SparkContext("local", "Average", System.getenv("SPARK_HOME"))
    val input = sc.textFile(localProjectPath + "/input/students.data").map(line => line.trim().split("\t")).filter(_.length == 8)
    input.map(x => (x(3), 1))
    .groupBy(x => x._1)
    .mapValues(_.map(_._2).sum)
    //.foreach(println)
  }
}

/** 提供参考的模拟students.data:
张祥德	RB17101	RB171	男	1997-02-10	11122223333	河南省郑州市1号	88
冯成刚	RB17102	RB171	女	1996-10-01	18837110115	河南省洛阳市2号	86
卢伟兴	RB17103	RB171	男	1998-08-02	19999228822	河南省开封市3号	95
杨飞龙	RB17104	RB171	男	1996-08-09	13322554455	河南省安阳市4号	91
姜松林	RB17201	RB172	女	1997-01-03	13688552244	河南省鹤壁市1号	75
高飞	RB17202	RB172	男	1996-08-27	13522114455	河南省新乡市2号	68
何桦	RB17203	RB172	女	1997-12-20	13566998855	河南省焦作市3号	84
高天阳	RB17204	RB172	男	1999-11-08	13688446622	河南省濮阳市4号	77
周存富	RB17301	RB173	男	1996-05-28	13699552658	河南省许昌市1号	93
罗鹏	RB17302	RB173	男	1998-03-02	13365298741	河南省漯河市2号	85
宋立昌	RB17401	RB174	男	1995-05-28	13596325874	河南省南阳市3号	81
杨国胜	RB17402	RB174	男	1996-03-02	13256987456	河南省信阳市4号	91
徐子文	RB17403	RB174	男	1998-05-28	13523654789	河南省周口市5号	85
马彦	RB17404	RB174	女	1997-03-02	13526845962	河南省郑州市6号	73
*/

