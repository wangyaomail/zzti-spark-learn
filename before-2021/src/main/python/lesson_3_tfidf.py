from  loadenv import SparkEnv
(se, sc) = SparkEnv("lesson_3").postInit()

from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF

docs = sc.textFile(se.projLoc()+r"\input\simple\1.data")
docToks = docs.map(lambda line: line.split(" "))
hashingTF = HashingTF(10)
tf = hashingTF.transform(docs).cache()
idf = IDF(minDocFreq=2).fit(tf)
tfidf = idf.transform(tf)

print(tf.collect())
print(tfidf.collect())

