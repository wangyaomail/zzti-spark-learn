from  loadenv import SparkEnv
(se, sc, ss) = SparkEnv("lesson_a_1").postInit()

from pyspark.ml.clustering import GaussianMixture
from pyspark.ml.linalg import Vectors

data = [(Vectors.dense([-0.1, -0.05 ]),),
        (Vectors.dense([-0.01, -0.1]),),
        (Vectors.dense([0.9, 0.8]),),
        (Vectors.dense([0.75, 0.935]),),
        (Vectors.dense([-0.83, -0.68]),),
        (Vectors.dense([-0.91, -0.76]),)]

df = ss.createDataFrame(data, ["features"])
gm = GaussianMixture(k=3, tol=0.0001, maxIter=10, seed=10)
model = gm.fit(df)
summary = model.summary

print(summary.clusterSizes)
print(model.weights)
