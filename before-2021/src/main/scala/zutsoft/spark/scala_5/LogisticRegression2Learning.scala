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
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{ SparkConf, SparkContext }

/**
 * Created by xubo on 2016/5/23.
 * 多元逻辑回归
 */
object LogisticRegression2Learning {
  def main(args: Array[String]) {
    var localProjectPath = new File("").getAbsolutePath();
    System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
    val sc = new SparkContext("local", "Average", System.getenv("SPARK_HOME"))

    val data = MLUtils.loadLibSVMFile(sc, localProjectPath + "\\input\\simple\\lr2.data") //读取数据文件
    val model = LogisticRegressionWithSGD.train(data, 50) //训练数据模型
    println(model.weights.size) //打印θ值
    println(model.weights) //打印θ值个数
    println(model.weights.toArray.filter(_ != 0).size) //打印θ中不为0的数
    data.take(10).foreach(println)
    val vc = data.take(1).map(_.features)
    //    println(model.predict())
    sc.stop
  }
}
