package zutsoft.spark.scala_5

import java.io._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD

object email {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    var localProjectPath = new File("").getAbsolutePath();
    System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
    val sc = new SparkContext("local", "Average", System.getenv("SPARK_HOME"))
    val spam = sc.textFile(localProjectPath+"\\input\\simple\\1.data")
    val normal = sc.textFile(localProjectPath+"\\input\\simple\\2.data")
    // 创建一个HashingTF实例来把邮件文本映射为包含10000个特征的向量
    val tf = new HashingTF(numFeatures = 3)
    // 各邮件都被切分为单词，每个单词被映射为一个特征
    val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
    val normalFeatures = normal.map(email => tf.transform(email.split(" ")))
    // 创建LabeledPoint数据集分别存放阳性（垃圾邮件）和阴性（正常邮件）的例子
    val positiveExamples = spamFeatures.map(features => LabeledPoint(1, features))
    val negativeExamples = normalFeatures.map(features => LabeledPoint(0, features))
    val trainingData = positiveExamples.union(negativeExamples)
    trainingData.cache() // 因为逻辑回归是迭代算法，所以缓存训练数据RDD
    // 使用SGD算法运行逻辑回归
    val model = new LogisticRegressionWithSGD().run(trainingData)
    // 以阳性（垃圾邮件）和阴性（正常邮件）的例子分别进行测试
    val posTest = tf.transform(
      "1 3 2 1 3 2".split(" "))
    val negTest = tf.transform(
      "4 1 1 1 4 1".split(" "))
    println("Prediction for positive test example: " + model.predict(posTest))
    println("Prediction for negative test example: " + model.predict(negTest))
  }
}