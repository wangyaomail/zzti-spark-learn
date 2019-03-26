import sys
import os
from os.path import *


class SparkEnv:

    def __init__(self, name):
        os.environ['HADOOP_HOME'] = dirname(dirname(dirname(dirname(os.path.abspath(__file__))))) + r'/hadoopdir'
        os.environ['SPARK_HOME'] = r"D:\assistlibs\hadoop\spark-2.2.3-bin-hadoop2.6"
        sys.path.append(r"D:\assistlibs\hadoop\spark-2.2.3-bin-hadoop2.6\python")
        from pyspark import SparkContext
        self.sc = SparkContext("local", name)
        self.sc.setLogLevel("INFO")

    def postInit(self):
        return (self, self.sc)

    def projLoc(self):
        return dirname(dirname(dirname(dirname(os.path.abspath(__file__)))))
