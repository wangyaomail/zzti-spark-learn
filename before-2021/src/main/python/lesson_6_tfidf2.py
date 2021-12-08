from  loadenv import SparkEnv
(se, sc, ss) = SparkEnv("lesson_6").postInit()

from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF

from pyspark.ml.feature import HashingTF, IDF, Tokenizer

sentenceData = ss.createDataFrame([
    (0, "hi this is spark"),
    (1, "using spark mllib to fit"),
    (2, "tfidf is smart and usefull in spark")
], ["label", "sentence"])
 
tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
wordsData = tokenizer.transform(sentenceData)
 
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=10)
featurizedData = hashingTF.transform(wordsData)
 
idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)
 
rescaledData.select("label", "features").show()
