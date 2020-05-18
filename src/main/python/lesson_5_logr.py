from  loadenv import SparkEnv
(se, sc) = SparkEnv("lesson_4").postInit()

from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.classification import LogisticRegressionWithSGD, LogisticRegressionWithLBFGS, LogisticRegressionModel
from pyspark.mllib.linalg import SparseVector

model = None

def output(inputStr):
    print (str(model.predict(inputStr)) + ": " + str(inputStr))

data = [ LabeledPoint(0.0, [0.0, 1.0]), LabeledPoint(1.0, [1.0, 0.0]), ]
model = LogisticRegressionWithSGD.train(sc.parallelize(data), iterations=10)
output([1.0, 0.0])
output([0.0, 1.0])
model.clearThreshold()
output([0.0, 1.0])

# # 稀疏矩阵
# sparse_data = [
#      LabeledPoint(0.0, SparseVector(2, {0: 0.0})),
#      LabeledPoint(1.0, SparseVector(2, {1: 1.0})),
#      LabeledPoint(0.0, SparseVector(2, {0: 1.0})),
#      LabeledPoint(1.0, SparseVector(2, {1: 2.0}))
# ]
# model = LogisticRegressionWithSGD.train(sc.parallelize(sparse_data), iterations=10)
# 
# output([1.0, 0.0])
# output([0.0, 1.0])
# output(SparseVector(2, {1: 1.0}))
# output(SparseVector(2, {0: 1.0}))
# 
# # 持久化
# import tempfile
# path = tempfile.mkdtemp()
# model.save(sc, path)
# model = LogisticRegressionModel.load(sc, path)
# output([0.0, 1.0])
# output(SparseVector(2, {0: 1.0}))
# 
# # 多分类
# multi_class_data = [
#     LabeledPoint(0.0, [0.0, 1.0, 0.0]),
#     LabeledPoint(1.0, [1.0, 0.0, 0.0]),
#     LabeledPoint(2.0, [0.0, 0.0, 1.0])
# ]
# data = sc.parallelize(multi_class_data)
# model = LogisticRegressionWithLBFGS.train(data, iterations=10, numClasses=3)
# output([0.0, 0.5, 0.0])
# output([0.8, 0.0, 0.0])
# output([0.0, 0.0, 0.3])

