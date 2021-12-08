import com.alibaba.fastjson.{JSON, JSONObject}
import org.ansj.splitWord.analysis.BaseAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.param.IntParam
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Encoders
import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.ml.classification.{DecisionTreeClassifier, LogisticRegression, NaiveBayes}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import scala.collection.JavaConverters._
import java.io.File
import scala.util.matching.Regex


// 根据情感分析的结果，将数据集变为qid, title,content, toks, score的形式，sort后保存到本地
object L23T1{
  // 自定义聚合函数进行分词和语段拼接
  class ContentTok extends UserDefinedAggregateFunction{
    override def inputSchema: StructType = { // 输入格式
      new StructType().add("content",StringType)
    }
    override def bufferSchema: StructType = { // 输出格式
      new StructType().add("content_tok",StringType)
    }
    override def dataType: DataType = StringType
    override def deterministic: Boolean = true
    override def initialize(buffer: MutableAggregationBuffer): Unit = { // 缓冲区初始化
      buffer(0)=""
    }
    val pattern = new Regex("[\\u4E00-\\u9FA5]+")
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = { // 更新代码
      val words = BaseAnalysis.parse(input.getString(0)).getTerms() // 处理分词
        .asScala
        .map(_.getName)
        .mkString(",") //以逗号隔开
      buffer(0) =buffer(0)+","+words
    }
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0)=buffer1.getString(0)+","+buffer2.getString(0)
    }
    override def evaluate(buffer: Row): Any = { // 最终返回的，去除多个逗号
      buffer.getString(0).split(",")
        .filter(_.length>1) // 过滤掉长度短的
        .filter(pattern.findFirstIn(_).size>0) // 过滤掉汉字以外的
        .mkString(",")
    }
  }
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    val ss = SparkSession.builder().appName("nlp").master("local[*]").getOrCreate()
    // 注册agg函数
    ss.udf.register("content_tok",new ContentTok)
    // 读入数据
    val corpus = ss.read.json("file:///E:\\data\\nlp_chinese_corpus\\webtext2019zh\\web_text_zh_test.json")
    corpus.createTempView("corpus")
    val titleContent = ss.sql("select qid, first(title) as title, concat_ws(';;', collect_list(content)) as content_all, " +
      "content_tok(content) as content_tok from corpus group by qid")
    titleContent.show()
    titleContent.coalesce(1).write.mode("overwrite")
      .json("file:///E:\\data\\nlp_chinese_corpus\\webtext2019zh\\1_qid_title_content_toks")
    // 将词典表也保存到本地
    val dictScheme = StructType(Array(StructField("word",StringType),StructField("score",IntegerType)))
    val posDict = ss.sparkContext.textFile("file:///E:\\dict\\正面词.dict").filter(_.length>0).map(x=>Row(x,1))
    val negDict = ss.sparkContext.textFile("file:///E:\\dict\\负面词.dict").filter(_.length>0).map(x=>Row(x,-1))
    val fullDictDF = ss.createDataFrame(posDict,dictScheme).union(ss.createDataFrame(negDict, dictScheme))
    fullDictDF.coalesce(1).write.mode("overwrite")
      .json("file:///E:\\data\\nlp_chinese_corpus\\webtext2019zh\\2_pos_neg_words")
    ss.close()
  }
}

object L23T2{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    val ss = SparkSession.builder().appName("nlp").master("local[*]").getOrCreate()
    // 加载词典表
    val pos_neg_words = ss.read.json("file:///E:\\data\\nlp_chinese_corpus\\webtext2019zh\\2_pos_neg_words")
    pos_neg_words.createTempView("pos_neg_words")
    pos_neg_words.show()
    // 加载上一步的content
    val title_content = ss.read.json("file:///E:\\data\\nlp_chinese_corpus\\webtext2019zh\\1_qid_title_content_toks")
    title_content.createTempView("title_content")
    title_content.show()
    // 列转行
    val word_qid = ss.sql("select word,qid from title_content lateral view explode(split(content_tok, ','))  as word ")
    word_qid.createTempView("word_qid")
    word_qid.show()
    // 计算reg_score和cls_score
    val sql1 =
      """
        |select t3.qid, t3.title, t3.content_all, t3.content_tok,
        |       t4.score as reg_score, cast((t4.score+abs(t4.score))/(2*t4.score+0.1) as int) as cls_score
        | from title_content as t3 join
        | ( select
        |    t2.qid as t2qid, sum(t1.score) as score
        |  from pos_neg_words as t1
        |  join word_qid as t2
        |  on t1.word=t2.word
        |  group by t2qid
        | ) as t4
        | on t3.qid=t4.t2qid
        | order by t3.qid desc
      """.stripMargin
    print(sql1)
    val qid_score = ss.sql(sql1)
    qid_score.show()
    qid_score.coalesce(1).write.mode("overwrite")
      .json("file:///E:\\data\\nlp_chinese_corpus\\webtext2019zh\\3_reg_cls")
    ss.close()
  }
}

object L23T3Pre{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    val ss = SparkSession.builder().appName("nlp").master("local[*]").getOrCreate()
    // 加载数据
    val data = ss.read.format("libsvm").load("input/sample_libsvm_data.txt")
    data.show()
    ss.close()
  }
}

object L23T3{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    val ss = SparkSession.builder().appName("nlp").master("local[*]").getOrCreate()
    // 加载数据
    val reg_cls = ss.read.json("file:///E:\\data\\nlp_chinese_corpus\\webtext2019zh\\3_reg_cls")
    reg_cls.createTempView("reg_cls")
    // 注意，如果要用spark ml库训练，结果需要以label标记，label以01为准，特征需要以features标记
    val sql1 =
      """
        |select qid, title, cls_score, reg_score,
        | split(content_all,';;') as content_arr,
        | split(content_tok,',') as tok_arr
        | from reg_cls
        |""".stripMargin
    print(sql1)
    val tf_pre=  ss.sql(sql1)
    tf_pre.show()
    // 使用词频构建特征
    val tf = new HashingTF()
      .setNumFeatures(3000)
      .setInputCol("tok_arr")
      .setOutputCol("features")
    val features_label = tf.transform(tf_pre)
    features_label.show()
    // feature不能以json的形式保存，读取后会因tinyint的格式出问题
    features_label.coalesce(1).write.mode("overwrite").format("parquet")
      .save("file:///E:\\data\\nlp_chinese_corpus\\webtext2019zh\\4_features_label")
    ss.close()
  }
}

object L23T4{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    val ss = SparkSession.builder().appName("nlp").master("local[*]").getOrCreate()
    // 加载数据
    val features_label = ss.read.format("parquet")
      .load("file:///E:\\data\\nlp_chinese_corpus\\webtext2019zh\\4_features_label")
    features_label.show()
    // 数据集划分
    val Array(train_data, test_data) = features_label.randomSplit(Array(0.7, 0.3))
    // 使用朴素贝叶斯训练
    val model_nb = new NaiveBayes()
      .setFeaturesCol("features")
      .setLabelCol("cls_score")
      .fit(train_data)
    // 计算测训练集准确率和测试集准确率
    val train_result_nb = model_nb.transform(train_data)
    val test_result_nb = model_nb.transform(test_data)
    // 验证准确率
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("cls_score")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    println("nb训练集准确率="+evaluator.evaluate(train_result_nb))
    println("nb测试集准确率="+evaluator.evaluate(test_result_nb))
    // 使用逻辑回归模型
    val model_lr = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("cls_score")
      .fit(train_data)
    // 计算测训练集准确率和测试集准确率
    val train_result_lr = model_nb.transform(train_data)
    val test_result_lr = model_nb.transform(test_data)
    // 验证准确率
    println("lr训练集准确率="+evaluator.evaluate(train_result_lr))
    println("lr测试集准确率="+evaluator.evaluate(test_result_lr))
    // 使用决策树
    val model_tree = new DecisionTreeClassifier()
      .setFeaturesCol("features")
      .setLabelCol("cls_score")
      .setMaxDepth(5)
      .setMinInstancesPerNode(1)
      .setMinInfoGain(0.0)
      .fit(train_data)
    // 计算测训练集准确率和测试集准确率
    val train_result_tree = model_tree.transform(train_data)
    val test_result_tree = model_tree.transform(test_data)
    // 验证准确率
    println("tree训练集准确率="+evaluator.evaluate(train_result_tree))
    println("tree测试集准确率="+evaluator.evaluate(test_result_tree))
    ss.close()
  }
}