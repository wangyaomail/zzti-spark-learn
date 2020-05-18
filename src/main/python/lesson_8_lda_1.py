from  loadenv import SparkEnv
(se, sc, ss) = SparkEnv("lesson_8_1").postInit()

from pyspark.ml.linalg import Vectors
from pyspark.ml.clustering import LDA

df = ss.createDataFrame([[1, Vectors.dense([0.0, 1.0])], [2, Vectors.dense([1.0, 0.0])], ], ["id", "features"])

lda = LDA(k=2, seed=1, optimizer="em")
model = lda.fit(df)
model.describeTopics().show()
