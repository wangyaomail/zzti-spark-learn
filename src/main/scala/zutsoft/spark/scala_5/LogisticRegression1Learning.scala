/**
 * @author xubo
 *         ref:Spark MlLib机器学习实战
 *         more code:https://github.com/xubo245/SparkLearning
 *         more blog:http://blog.csdn.net/xubo245
 */
package org.apache.spark.mllib.regression

import java.io._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{ SparkConf, SparkContext }

/**
 * Created by xubo on 2016/5/23.
 * 一元逻辑回归
 */
object LogisticRegression1Learning {
  def main(args: Array[String]) {
    var localProjectPath = new File("").getAbsolutePath();
    System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
    val sc = new SparkContext("local", "Average", System.getenv("SPARK_HOME"))

    val data = sc.textFile(localProjectPath+"\\input\\simple\\lr1.data") //获取数据集路径
    val parsedData = data.map { line => //开始对数据集处理
      val parts = line.split('|') //根据逗号进行分区
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }.cache() //转化数据格式
    parsedData.foreach(println)
    val model = LogisticRegressionWithSGD.train(parsedData, 50) //建立模型
    val target = Vectors.dense(-1) //创建测试值
    val resulet = model.predict(target) //根据模型计算结果
    println("model.weights:")
    println(model.weights)
    println(resulet) //打印结果
    println(model.predict(Vectors.dense(10)))
    sc.stop
  }
}
