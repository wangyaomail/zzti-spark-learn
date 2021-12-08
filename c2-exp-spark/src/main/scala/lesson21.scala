import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.io.File
import scala.util.matching.Regex

//    // 首先将数据导入为普通rdd
//    val rddSrc = ss.sparkContext.textFile("file:///E:\\data\\nlp_chinese_corpus\\webtext2019zh\\web_text_zh_test.json")
//    // 随机采样3条，
//    val rddSize = rddSrc.count()
//    rddSrc.sample(withReplacement = true,fraction = 3.0/rddSize).foreach(println)
//    print(rddSize)


object L21T1{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    val ss = SparkSession.builder().appName("nlp").master("local").getOrCreate()
    // 以DataFrame读入测试
    val rddDF = ss.read.json("file:///E:\\data\\nlp_chinese_corpus\\webtext2019zh\\web_text_zh_test.json")
    // 看一下一共有多少数据
    val rddSize = rddDF.count()
    println(rddSize)
    // 随机采样3条看一下
    rddDF.sample(withReplacement = true,fraction = 3.0/rddSize).show()
    // 将该数据保存为spark的临时表
    rddDF.createTempView("test_corpus")
    // 仅输出answer_id和title两个字段的前10条数据
    val writeData = rddDF.select(col = "answer_id","title").limit(10)
    writeData.show()
    // 数据保存本地
    writeData.write.json("file:///E:\\data\\nlp_chinese_corpus\\webtext2019zh\\web_text_zh_test_output")
    ss.close()
  }
}

object L21T2{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    val ss = SparkSession.builder().appName("nlp").master("local").getOrCreate()
    // 以DataFrame读入测试
    val rddDF = ss.read.json("file:///E:\\data\\nlp_chinese_corpus\\webtext2019zh\\web_text_zh_test.json")
    // 将该数据保存为spark的临时表
    rddDF.createTempView("test_corpus")
    // 输出表结构
    rddDF.printSchema()
    // 使用sql的方式输出answer_id和title两个字段的前10条数据
    ss.sql("select answer_id,title from test_corpus limit 10").show()
    // 使用sql的方式输出answer_id最大的前10条
    ss.sql("select answer_id,title from test_corpus order by answer_id desc limit 10").show()
    // 输出热点问题（回答多的话题）
    rddDF.groupBy("qid")
      .agg(count("answer_id").as("answer_count"),first("title").as("title"))
      .orderBy(desc("answer_count"))
      .limit(10)
      .show()
    ss.sql("select qid,count(answer_id) as answer_count, first(title) as title from test_corpus group by qid order by answer_count desc limit 10").show()
    ss.close()
  }
}

object L21T3{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    val ss = SparkSession.builder().appName("nlp").master("local").getOrCreate()
    // 以DataFrame读入测试
    val rddDF = ss.read.json("file:///E:\\data\\nlp_chinese_corpus\\webtext2019zh\\web_text_zh_test.json")
    // 将该数据保存为spark的临时表
    rddDF.createTempView("test_corpus")
    // 输出高频标签
    rddDF.select(explode(split(column("answerer_tags"), " ")).as("tags"))
      .filter("length(tags)>0")
      .groupBy("tags")
      .agg(count("tags").as("tags_count"))
      .orderBy(desc("tags_count"))
      .limit(10)
      .show()
    // 作业1：将该语句还原成sql语句
    ss.close()
  }
}

object L21T4{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    val ss = SparkSession.builder().appName("nlp").master("local").getOrCreate()
    // 以DataFrame读入测试
    val rddDF = ss.read.json("file:///E:\\data\\nlp_chinese_corpus\\webtext2019zh\\web_text_zh_test.json")
    // 将该数据保存为spark的临时表
    rddDF.createTempView("test_corpus")
    // 输出高赞用户
    ss.sql("select answerer_tags, sum(star) as star from test_corpus group by answerer_tags order by star desc limit 10").show()
    // 作业2：将该语句还原成sql语句
    ss.close()
  }
}