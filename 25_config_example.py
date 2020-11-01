from pyspark import SparkConf, SparkContext
import configparser as cp
import sys

props = cp.RawConfigParser()
'''Note here path is from src likewise edit config need to have working path till pyspark3s'''
props.read("src/main/python/resources/application.properties")
'''argsv is input param passed while calling file'''
env = sys.argv[1]
conf = SparkConf()\
       .setAppName("PySpark App")\
       .setMaster(props.get(env ,'executionMode'))
sc = SparkContext(conf=conf)

words = sc.parallelize (
   ["scala",
   "java",
   "hadoop",
   "spark",
   "akka",
   "spark vs hadoop",
   "pyspark",
   "pyspark and spark"]
)
counts = words.count()
print("Number of elements in RDD -> %i" % (counts))