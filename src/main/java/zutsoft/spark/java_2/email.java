package zutsoft.spark.java_2;

import java.io.File;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;

public class email {
    public static void main(String[] args) {
        String localProjectPath = new File("").getAbsolutePath();
        System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
        SparkConf sparkConf = new SparkConf().setAppName("wordcount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> spam = sc.textFile(localProjectPath + "\\input\\email\\num_neg.data");
        JavaRDD<String> normal = sc.textFile(localProjectPath + "\\input\\email\\num_pos.data");
        // 创建一个HashingTF实例来把邮件文本映射为包含10000个特征的向量
        final HashingTF tf = new HashingTF(10);
        // 创建LabeledPoint数据集分别存放阳性（垃圾邮件）和阴性（正常邮件）的例子
        JavaRDD<LabeledPoint> posExamples = spam.map(new Function<String, LabeledPoint>() {
            public LabeledPoint call(String email) {
                return new LabeledPoint(1, tf.transform(Arrays.asList(email.split(" "))));
            }
        });
        JavaRDD<LabeledPoint> negExamples = normal.map(new Function<String, LabeledPoint>() {
            public LabeledPoint call(String email) {
                return new LabeledPoint(0, tf.transform(Arrays.asList(email.split(" "))));
            }
        });
        JavaRDD<LabeledPoint> trainData = posExamples.union(negExamples);
        trainData.cache(); // 因为逻辑回归是迭代算法，所以缓存训练数据RDD
        // 使用SGD算法运行逻辑回归
        LogisticRegressionModel model = new LogisticRegressionWithSGD().run(trainData.rdd());
        System.out.println(model.toString());
        // 以阳性（垃圾邮件）和阴性（正常邮件）的例子分别进行测试
        Vector posTest = tf.transform(Arrays.asList("a a b".split(" ")));
        Vector negTest = tf.transform(Arrays.asList("c d".split(" ")));
        System.out.println("Prediction for positive example: " + model.predict(posTest));
        System.out.println("Prediction for negative example: " + model.predict(negTest));
        predict(model, tf, "a a b c d");
        predict(model, tf, "a a b c c");
        predict(model, tf, "a b c c d");
        predict(model, tf, "a a b b c c d d");
        predict(model, tf, "a a b b c d d");
        predict(model, tf, "a a b c c d d");
        System.out.println(trainData.count());
    }

    private static void predict(LogisticRegressionModel model,
                                HashingTF tf,
                                String str) {
        System.out.println(model.predict(tf.transform(Arrays.asList(str.split(" ")))) + ": " + str);
    }
}
