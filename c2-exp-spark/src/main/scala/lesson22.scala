import com.alibaba.fastjson.{JSON, JSONObject}
import org.ansj.splitWord.analysis.BaseAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Encoders
import org.apache.spark.rdd.EmptyRDD
import scala.collection.JavaConverters._
import java.io.File

object L22T1{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    val ss = SparkSession.builder().appName("nlp").master("local[*]").getOrCreate()
    // 如何使用外部库？先尝试普通的导入数据分词
    val rddSrc = ss.sparkContext.textFile("file:///E:\\data\\nlp_chinese_corpus\\webtext2019zh\\web_text_zh_test.json")
    // 因为scala的json被废弃，所以使用第三方的json库
    val answers = rddSrc.map(line=>JSON.parseObject(line))
      .map(_.getString("content"))
      .filter(_.length>0)
    // 输出前10条
    answers.take(10).foreach(println)
    // 使用第三方库分词
    val answerToks = answers.flatMap(BaseAnalysis.parse(_).getTerms().asScala.map(_.getName))
      .groupBy(x=>x)
      .map(x=>(x._1,x._2.size))
      .filter(_._1.size>1)
      .sortBy(_._2,ascending=false)
    // 输出前10条
    answerToks.take(10).foreach(println)
    // 构建到DataFrame
    ss.createDataFrame(answerToks.filter(_._1.length>0).map(x=>Row(x._1,x._2)),
      StructType(Array(StructField("word",StringType),StructField("word_count",IntegerType))))
      .show()
    ss.close()
  }
}

object L22T2{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    val ss = SparkSession.builder().appName("nlp").master("local[*]").getOrCreate()
    // 使用DataFrame完成同样的事情，且不要长度为1的
    val dfSrc = ss.read.json("file:///E:\\data\\nlp_chinese_corpus\\webtext2019zh\\web_text_zh_test.json")
    dfSrc.createTempView("corpus")
    dfSrc.select("content")
      .flatMap(row=>BaseAnalysis.parse(row.getString(0)).getTerms().asScala.map(_.getName).toList)(Encoders.STRING)
      .toDF("words")
      .filter("length(words)>1")
      .groupBy("words")
      .agg(count("words").as("words_count"))
      .orderBy(desc("words_count"))
      .limit(10)
      .show()
    ss.close()
  }
}

object L22T3{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    val ss = SparkSession.builder().appName("nlp").master("local[*]").getOrCreate()
    // 读入两个词典
    val posDict = ss.sparkContext.textFile("file:///E:\\dict\\正面词.dict").filter(_.length>0).map(x=>(x,1))
    val negDict = ss.sparkContext.textFile("file:///E:\\dict\\负面词.dict").filter(_.length>0).map(x=>(x,-1))
    val fullDict = posDict.union(negDict) // 求并集
    println("正面词",posDict.take(10).toList)
    println("负面词",negDict.take(10).toList)
    // 读入数据
    val rddJoin = ss.sparkContext.textFile("file:///E:\\data\\nlp_chinese_corpus\\webtext2019zh\\web_text_zh_test.json")
      .map(line=>JSON.parseObject(line))
      .map(x=>(x.getString("title"),x.getString("content")))
      .filter(_._2.length>0)
      .flatMap(x=>BaseAnalysis.parse(x._2).getTerms().asScala.map(_.getName).map(a=>(a,x._1))) // word - qid
      .join(fullDict) // 不要用outerjoin
      .groupBy(_._2)
    val result = rddJoin.map(x=>(x._1._1,x._2.map(_._2._2).fold(0)(_+_)))
      .sortBy(_._2, ascending=false)
    result.take(10)
      .foreach(println)
    println(result.count())
  }
}

object L22T4{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    val ss = SparkSession.builder().appName("nlp").master("local[*]").getOrCreate()
    // 构造词典表结构
    val dictScheme = StructType(Array(StructField("word",StringType),StructField("score",IntegerType)))
    // 读入两个词典，插入这个表
    val posDict = ss.sparkContext.textFile("file:///E:\\dict\\正面词.dict").filter(_.length>0).map(x=>Row(x,1))
    val negDict = ss.sparkContext.textFile("file:///E:\\dict\\负面词.dict").filter(_.length>0).map(x=>Row(x,-1))
    val fullDictDF = ss.createDataFrame(posDict,dictScheme).union(ss.createDataFrame(negDict, dictScheme))
    fullDictDF.createTempView("dict")
    fullDictDF.show()
    // 读入数据
    val corpus = ss.read.json("file:///E:\\data\\nlp_chinese_corpus\\webtext2019zh\\web_text_zh_test.json")
    corpus.select("title","content")
      .flatMap(row=>BaseAnalysis.parse(row.getString(1)).getTerms().asScala.map(_.getName).map(x=>(x,row.getString(0))))(Encoders.tuple(Encoders.STRING, Encoders.STRING))
      .toDF("word","title")
      .createTempView("toks")
    ss.sql("select toks.title as title, sum(dict.score) as score from dict join toks on dict.word=toks.word " +
      "group by title order by score desc limit 10").show()
  }
}

