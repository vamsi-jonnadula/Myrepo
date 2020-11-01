'''Row objects internally represent arrays of bytes. The byte array interface is never shown
to users because we only use column expressions to manipulate them.'''
from pyspark.sql import Row
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType
spark = SparkSession. \
  builder. \
  appName('Row Example'). \
  getOrCreate()

myRow = Row("Hello", None, 1, False)

print(myRow[0])
print(myRow[2])

'''Row with schema'''
myManualSchema = StructType([
StructField("some", StringType(), True),
StructField("col", StringType(), True),
StructField("names", LongType(), False)
])
myRow = Row("Hello", None, 1)
myDf = spark.createDataFrame([myRow], myManualSchema)
myDf.show()