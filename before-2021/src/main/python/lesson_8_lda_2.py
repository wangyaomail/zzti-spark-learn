from  loadenv import SparkEnv
(se, sc, ss) = SparkEnv("lesson_8_2").postInit()

from pyspark.ml.linalg import Vectors, SparseVector
from pyspark.ml.clustering import LDA

# Loads data.
dataset = ss.read.format("libsvm").load(se.projLoc()+r"\input\clustering\sample_lda_data.data")

# Trains a LDA model.
lda = LDA(k=10, maxIter=10)
model = lda.fit(dataset)

ll = model.logLikelihood(dataset)
lp = model.logPerplexity(dataset)
print("给定推断主题的所提供文档的下限: " + str(ll))
print("给定推断主题的所提供文档的复杂程度的上限: " + str(lp))

# Describe topics.
topics = model.describeTopics(3)
print("描述主题的最高级:")
topics.show(truncate=False)

# Shows the result
transformed = model.transform(dataset)
transformed.show(truncate=False)