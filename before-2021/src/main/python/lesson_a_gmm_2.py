from  loadenv import SparkEnv
(se, sc, ss) = SparkEnv("lesson_a_2").postInit()

from pyspark.ml.clustering import GaussianMixture

# loads data
dataset = ss.read.format("libsvm").load(se.projLoc()+r"\input\clustering\sample_kmeans_data.data")

gmm = GaussianMixture().setK(2).setSeed(538009335)
model = gmm.fit(dataset)

print("Gaussians shown as a DataFrame: ")
model.gaussiansDF.show(truncate=False)

