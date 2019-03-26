package cn.edu.zut.soft.spark.java;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings({ "resource", "serial" })
public class WordCount {
    public static void main(String[] args) throws Exception {
        String localProjectPath = new File("").getAbsolutePath();
        System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
        SparkConf sparkConf = new SparkConf().setAppName("wordcount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = sc.textFile(localProjectPath + "\\input\\test.data");
        JavaPairRDD<String, Integer> counts = rdd.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String x) {
                return Arrays.asList(x.split(" ")).iterator();
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String x) {
                return new Tuple2<String, Integer>(x, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer x,
                                Integer y) {
                return x + y;
            }
        });

        counts.saveAsTextFile(localProjectPath + "\\output\\wordcount");
    }
}
