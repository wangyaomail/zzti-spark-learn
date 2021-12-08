from  loadenv import SparkEnv
(se, sc, ss) = SparkEnv("lesson_9_2").postInit()

from pyspark.ml.clustering import KMeans
#from pyspark.ml.evaluation import ClusteringEvaluator

dataset = ss.read.format("libsvm").load(se.projLoc()+r"\input\clustering\sample_kmeans_data.data")

# Trains a k-means model.
kmeans = KMeans().setK(2).setSeed(1)
model = kmeans.fit(dataset)

# Make predictions
predictions = model.transform(dataset)

# # Evaluate clustering by computing Silhouette score
# evaluator = ClusteringEvaluator()
# 
# silhouette = evaluator.evaluate(predictions)
# print("Silhouette with squared euclidean distance = " + str(silhouette))

# Shows the result.
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)