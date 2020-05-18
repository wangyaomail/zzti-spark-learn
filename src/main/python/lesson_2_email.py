from  loadenv import SparkEnv
(se, sc) = SparkEnv("lesson_2").postInit()

from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.classification import LogisticRegressionWithSGD

spam = sc.textFile(se.projLoc() + r"\input\email\num_neg.data")  # a:b:c:d=1:2:3:4
normal = sc.textFile(se.projLoc() + r"\input\email\num_pos.data")  # a:b:c:d=4:3:2:1
# 创建一个HashingTF实例来把邮件文本映射为包含10000个特征的向量
tf = HashingTF(numFeatures=20)
# 各邮件都被切分为单词，每个单词被映射为一个特征
spamFeatures = spam.map(lambda email: tf.transform(email.split(" ")))
normalFeatures = normal.map(lambda email: tf.transform(email.split(" ")))
# 创建LabeledPoint数据集分别存放阳性（垃圾邮件）和阴性（正常邮件）的例子
positiveExamples = spamFeatures.map(lambda features: LabeledPoint(1, features))
negativeExamples = normalFeatures.map(lambda features: LabeledPoint(0, features))
trainingData = positiveExamples.union(negativeExamples)
trainingData.cache()  # 因为逻辑回归是迭代算法，所以缓存训练数据RDD
# 使用SGD算法运行逻辑回归
model = LogisticRegressionWithSGD.train(trainingData, iterations=200)
# 以阳性（垃圾邮件）和阴性（正常邮件）的例子分别进行测试。首先使用
# 一样的HashingTF特征来得到特征向量，然后对该向量应用得到的模型
posTest = tf.transform("a a b b c c d d".split(" "))
negTest = tf.transform("c d".split(" "))
print ("Prediction for positive test example: %g" % model.predict(posTest))
print ("Prediction for negative test example: %g" % model.predict(negTest))


def output(inputStr):
    test = tf.transform(inputStr.split(" "))
    print (str(model.predict(test)) + ": " + inputStr)

output("a a a a b b b c c d")
output("a b b c c c d d d d")
output("a b d")
output("a c d")
output("a a b b c d d")
output("a a b c c d d")
output("a a a b b b c c d d d")
output("a a a b b c c c d d d")

print(model.weights)
