import sys
import os
from os.path import *


def pl():
    return dirname(dirname(dirname(dirname(os.path.abspath(__file__)))))

class SparkEnv:

    def __init__(self, name):
        os.environ['HADOOP_HOME'] = dirname(dirname(dirname(dirname(os.path.abspath(__file__))))) + r'/hadoopdir'
        os.environ['SPARK_HOME'] = r"D:\assistlibs\hadoop\spark-2.2.3-bin-hadoop2.6"
        sys.path.append(r"D:\assistlibs\hadoop\spark-2.2.3-bin-hadoop2.6\python")
        from pyspark import SparkContext
        self.sc = SparkContext("local", name)
        self.sc.setLogLevel("WARN")
        from pyspark.sql import SparkSession
        self.ss = SparkSession.builder.appName(name).getOrCreate()

    def postInit(self):
        return (self, self.sc, self.ss)

    def projLoc(self):
        return dirname(dirname(dirname(dirname(os.path.abspath(__file__)))))
