from  loadenv import SparkEnv
(se, sc, ss) = SparkEnv("lesson_7").postInit()

from pyspark.ml.feature import Word2Vec

sent = ("a b " * 100 + "a c " * 10).split(" ")
doc = ss.createDataFrame([(sent,), (sent,)], ["sentence"])
word2Vec = Word2Vec(vectorSize=3, inputCol="sentence", outputCol="model")
model = word2Vec.fit(doc)
model.getVectors().show(100, False)

from pyspark.sql.functions import format_number as fmt
model.findSynonyms("a", 2).select("word", fmt("similarity", 3).alias("similarity")).show(100, False)






