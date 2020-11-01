from pyspark.sql import SparkSession

appName = "Spark - Setting Log Level"
master = "local[2]"

# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()

'''For changing log level log4j.properties and change line -> log4j.rootCategory=INFO'''
spark.sparkContext.setLogLevel("ERROR")