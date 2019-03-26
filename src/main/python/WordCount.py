import sys
import os
from os.path import *
os.environ['HADOOP_HOME'] = dirname(dirname(dirname(dirname(os.path.abspath(__file__))))) + r'/hadoopdir'
os.environ['SPARK_HOME'] = r"D:\assistlibs\hadoop\spark-2.2.3-bin-hadoop2.6"
sys.path.append(r"D:\assistlibs\hadoop\spark-2.2.3-bin-hadoop2.6\python")

from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext("local", "WordCount")
    lines = sc.parallelize(["I have an army", "We have a Hulk"])
    result = lines.flatMap(lambda x: x.split(" ")).countByValue()
    for key, value in result.items():
        print ("%s %i" % (key, value))
