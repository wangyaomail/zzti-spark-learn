package zutsoft.spark.java_3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;

import java.io.File;
import java.util.Arrays;

public class AssociationRulesTest {
    public static void main(String[] args) {
        String localProjectPath = new File("").getAbsolutePath();
        System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
        SparkConf sparkConf = new SparkConf().setAppName("JavaAssociationRulesExample")
                                             .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<FPGrowth.FreqItemset<String>> freqItemsets = null;
        freqItemsets = sc.parallelize(Arrays.asList(new FPGrowth.FreqItemset<>(new String[] { "a" },
                                                                               15L),
                                                    new FPGrowth.FreqItemset<>(new String[] { "b" },
                                                                               35L),
                                                    new FPGrowth.FreqItemset<>(new String[] { "a",
                                                            "b" }, 12L)));

        AssociationRules rules = new AssociationRules().setMinConfidence(0.8);
        JavaRDD<AssociationRules.Rule<String>> results = rules.run(freqItemsets);

        for (AssociationRules.Rule<String> rule : results.collect()) {
            System.out.println(rule.javaAntecedent() + " => " + rule.javaConsequent() + ", "
                    + rule.confidence());
        }

        sc.stop();
    }
}
