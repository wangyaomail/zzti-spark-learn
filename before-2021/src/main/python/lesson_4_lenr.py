from  loadenv import SparkEnv
(se, sc) = SparkEnv("lesson_4").postInit()

from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.regression import LinearRegressionWithSGD, RidgeRegressionWithSGD
from pyspark.mllib.linalg import SparseVector

model = None


def output(x, y):
    print (str(abs(model.predict(x) - y)) + ": x=" + str(x) + ", y=" + str(y))


data = [
    LabeledPoint(0.0, [0.0]),
    LabeledPoint(1.0, [1.0]),
    LabeledPoint(3.0, [2.0]),
    LabeledPoint(2.0, [3.0])
]
model = LinearRegressionWithSGD.train(sc.parallelize(data),
                                      iterations=10000,
                                      initialWeights=[1.0])

output([0.0], 0)
output([1.0], 1)
output(SparseVector(1, {0: 1.0}), 1)
print(model.weights)
# 
# data = [
#     LabeledPoint(0.0, SparseVector(1, {0: 0.0})),
#     LabeledPoint(1.0, SparseVector(1, {0: 1.0})),
#     LabeledPoint(3.0, SparseVector(1, {0: 2.0})),
#     LabeledPoint(2.0, SparseVector(1, {0: 3.0}))
# ]
# model = LinearRegressionWithSGD.train(sc.parallelize(data),
#                                       iterations=10,
#                                       initialWeights=[1.0])
# output([0.0], 0)
# output(SparseVector(1, {0: 1.0}), 1)
# 
# model = LinearRegressionWithSGD.train(sc.parallelize(data),
#                                       iterations=10,
#                                       step=1.0,
#                                       regParam=0.01,
#                                       miniBatchFraction=1.0,
#                                       initialWeights=[1.0],
#                                       intercept=True,
#                                       validateData=True)
# output([0.0], 0)
# output(SparseVector(1, {0: 1.0}), 1)


