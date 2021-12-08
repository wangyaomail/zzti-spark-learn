import com.alibaba.fastjson.{JSON, JSONObject}
import org.ansj.splitWord.analysis.BaseAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{CountVectorizer, HashingTF}
import org.apache.spark.ml.param.IntParam
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Encoders
import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.ml.classification.{DecisionTreeClassifier, LogisticRegression, NaiveBayes}
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.regression.{DecisionTreeRegressor, LinearRegression}

import scala.collection.JavaConverters._
import java.io.File
import scala.util.matching.Regex

object L24T1{
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

// 分类
object L24T2{
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

// 回归
object L24T3{
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
    // 使用线性回归训练
    val model_linear = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("reg_score")
      .setRegParam(0.0) // 正则项
      .setMaxIter(100) // 训练轮数
      .setTol(1E-3) // 错误率
      .fit(train_data)
    // 计算测训练集和测试集
    val train_result_linear = model_linear.transform(train_data)
    val test_result_linear = model_linear.transform(test_data)
    // 验证误差
    val evaluator = new RegressionEvaluator()
      .setLabelCol("reg_score")
      .setPredictionCol("prediction")
      .setMetricName("mse") // 均方误差
    println("linear训练集误差="+evaluator.evaluate(train_result_linear))
    println("linear测试集误差="+evaluator.evaluate(test_result_linear))
    // 使用决策树回归
    val model_tree = new DecisionTreeRegressor()
      .setFeaturesCol("features")
      .setLabelCol("cls_score")
      .setMaxDepth(5)
      .setMinInstancesPerNode(1)
      .setMinInfoGain(0.0)
      .fit(train_data)
    // 计算测训练集和测试集
    val train_result_tree = model_tree.transform(train_data)
    val test_result_tree = model_tree.transform(test_data)
    // 验证误差
    println("tree训练集误差="+evaluator.evaluate(train_result_tree))
    println("tree测试集误差="+evaluator.evaluate(test_result_tree))
    ss.close()
  }
}

// 计算主题词
object L24T4{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    val ss = SparkSession.builder().appName("nlp").master("local[*]").getOrCreate()
    // 加载数据
    val features_label = ss.read.format("parquet")
      .load("file:///E:\\data\\nlp_chinese_corpus\\webtext2019zh\\4_features_label")
    features_label.show()
    // 对tok_arr向量化
    val model_vec = new CountVectorizer()
      .setInputCol("tok_arr")
      .setOutputCol("tok_vec")
      .setVocabSize(3000)
    val fit_vec = model_vec.fit(features_label)
    println(fit_vec.vocabulary)
    val trans_vec = fit_vec.transform(features_label)
    trans_vec.show()
    // LDA 模型
    val model_lda = new LDA()
      .setFeaturesCol("tok_vec")
      .setK(20)
      .setMaxIter(20)
      .fit(trans_vec)
    // 输出结果
    val result_lda = model_lda.transform(trans_vec)
    result_lda.select("title","topicDistribution").show(false)
    // 显示所有话题
    val vec_2_str = udf { (termIndices: Seq[Int]) => termIndices.map(idx => fit_vec.vocabulary(idx)) }
    val topics = model_lda.describeTopics(maxTermsPerTopic = 5)
      .withColumn("terms", vec_2_str(col("termIndices")))
    topics.show()
    ss.close()
  }
}

