from  loadenv import SparkEnv
(se, sc, ss) = SparkEnv("lesson_9_1").postInit()

from pyspark.ml.linalg import Vectors
from pyspark.ml.clustering import KMeans

data = [(Vectors.dense([0.0, 0.0]),), (Vectors.dense([1.0, 1.0]),), (Vectors.dense([9.0, 8.0]),), (Vectors.dense([8.0, 9.0]),)]
df = ss.createDataFrame(data, ["features"])
kmeans = KMeans(k=1, seed=1)
model = kmeans.fit(df)
print(model.clusterCenters())
print(model.computeCost(df))

