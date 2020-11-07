from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('structured_streaming').getOrCreate()
import pyspark.sql.functions as F
from pyspark.sql.types import *
df_1=spark.createDataFrame([("XN203",'FB',300,30),("XN201",'Twitter',10,19),("XN202",'Insta',500,45)],["user_id","app","time_in_secs","age"]).write.csv("csv_folder",mode='append')
schema=StructType().add("user_id","string").add("app","string").add("time_in_secs", "integer").add("age", "integer")
data=spark.readStream.option("sep", ",").schema(schema).csv("csv_folder")
data.printSchema()
data.createOrReplaceTempView("mystream")
app_count = spark.sql("select app,count(app) from mystream group by app")

query = (
  app_count \
    .writeStream \
    .format("console") \
    .outputMode("complete") \
    .start() \
    .awaitTermination())

print(query.status)