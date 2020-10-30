from pyspark.sql.session import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.functions import flatten
from pyspark.sql.functions import array_contains
from pyspark.sql.functions import arrays_zip, col

spark = SparkSession. \
    builder. \
    appName('Column Operation Example'). \
    getOrCreate()

df = spark.read.format("json").load("/home/bose/PycharmProjects/pyspark3/datasources/2015-summary.json")
df.createOrReplaceTempView("dfTable")

'''Python way'''
df.select("DEST_COUNTRY_NAME").show(2)

'''SQL Way'''
dfsql = spark.sql("SELECT DEST_COUNTRY_NAME FROM dfTable LIMIT 2")
dfsql.show()

'''Python Way'''
df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)

'''We use col and column both as they are same'''
from pyspark.sql.functions import expr, col, column

df.select(
    expr("DEST_COUNTRY_NAME"),
    col("DEST_COUNTRY_NAME"),
    column("DEST_COUNTRY_NAME")) \
    .show(2)

'''col and normal column'''
df.select(col("DEST_COUNTRY_NAME"), "DEST_COUNTRY_NAME").show(2)

'''expr with AS keyword'''
df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)

'''again rename with expr and .alias'''
df.select(expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME")) \
    .show(2)

'''selectExpr with condition check-Python'''
df.selectExpr(
    "*",  # all original columns
    "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry") \
    .show(2)

'''Same as above with SQL'''
spark.sql("SELECT *, (DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry FROM dfTable LIMIT 2").show()

'''Aggregate avg and count'''
df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)

'''Aggregation sql'''
spark.sql("SELECT avg(count), count(distinct(DEST_COUNTRY_NAME)) FROM dfTable LIMIT 2").show()

'''literal i.e constant column'''
from pyspark.sql.functions import lit

df.select(expr("*"), lit(1).alias("One")).show(2)

'''withColumn -adding column,one is column and second is expression'''
df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME")) \
    .show(2)

'''renaming column- withColumnRenamed'''
print(df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns)

'''We will try long column name'''
dfWithLongColName = df.withColumn(
    "This Long Column-Name",
    expr("ORIGIN_COUNTRY_NAME"))

'''Below without backtick long column name fails as here we are referencing long column'''
dfWithLongColName.selectExpr(
    "`This Long Column-Name`",
    "`This Long Column-Name` as `new col`") \
    .show(2)

'''We are referring Long keyword but with backtick no error removing same fails'''
print(dfWithLongColName.select(expr("`This Long Column-Name`")).columns)

'''Spark is case insensitive,incase you want to make it case sensitive use below'''
'''set spark.sql.caseSensitive true'''

'''Removing a column'''
print(df.drop("ORIGIN_COUNTRY_NAME").columns)

'''casting (conversion to other type)'''
df.withColumn("count2", col("count").cast("long"))

df.printSchema()

'''Filter in spark'''
df.filter(col("count") < 2).show(2)
df.where("count < 2").show(2)

'''where clause'''
df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") != "Croatia") \
    .show(2)

'''Getting unique row'''
df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()

from pyspark.sql import Row

schema = df.schema
newRows = [
    Row("New Country", "Other Country", 5),
    Row("New Country 2", "Other Country 3", 1)
]
parallelizedRows = spark.sparkContext.parallelize(newRows)
newDF = spark.createDataFrame(parallelizedRows, schema)

'''Union between newRows and df'''
df.union(newDF) \
    .where("count = 1") \
    .where(col("ORIGIN_COUNTRY_NAME") != "United States") \
    .show()

'''sort by column count'''
df.sort("count").show(5)
'''orderBy count and DEST_COUNTRY_NAME'''
df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)

'''Limit '''
df.limit(5).show()

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/home/bose/PycharmProjects/pyspark3/datasources//2010-12-01.csv")
df.printSchema()
df.createOrReplaceTempView("dfTable")

# in Python- where with not equal
from pyspark.sql.functions import col

df.where(col("InvoiceNo") != 536365) \
    .select("InvoiceNo", "Description") \
    .show(5, False)

from pyspark.sql.functions import instr

'''filter condition defined priceFilter'''
priceFilter = col("UnitPrice") > 600
'''descripFilter filter defined '''
descripFilter = instr(df.Description, "POSTAGE") >= 1
'''below we apply the above conditions
   Stockcode is in DOT along with 2 filters'''
df.where(df.StockCode.isin("DOT")).where(priceFilter | descripFilter).show()

'''adding filter condition'''
from pyspark.sql.functions import instr

'''Below is DOTCodeFilter'''
DOTCodeFilter = col("StockCode") == "DOT"
'''Below is priceFilter'''
priceFilter = col("UnitPrice") > 600
'''Below is descripFilter'''
descripFilter = instr(col("Description"), "POSTAGE") >= 1
'''Now we apply the filters and create a new column isExpensive'''
df.withColumn("isExpensive", DOTCodeFilter & (priceFilter | descripFilter)) \
    .where("isExpensive") \
    .select("unitPrice", "isExpensive").show(5)

'''Working with numbers'''
from pyspark.sql.functions import expr, pow

'''we create a column from other columns Quantity and Unit Price'''
fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
'''Next we print CustomerId and realQuantity on which fabricatedQuantity was applied'''
df.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(3)

'''same as above with selectExpr'''
df.selectExpr(
    "CustomerId",
    "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity").show(3)

'''To see data overview'''
df.describe().show()

'''Increasing row number incrementally'''
from pyspark.sql.functions import monotonically_increasing_id

df.select(monotonically_increasing_id()).show(3)

'''String Operations'''
from pyspark.sql.functions import initcap

'''initcap applied to description'''
df.select(initcap(col("Description"))).show(3)

from pyspark.sql.functions import lower, upper

'''upper and lower'''
df.select(col("Description"),
          lower(col("Description")),
          upper(lower(col("Description")))).show(2)

'''regexp_replace '''
from pyspark.sql.functions import regexp_replace

regex_string = "BLACK|WHITE|RED|GREEN|BLUE"
df.select(
    regexp_replace(col("Description"), regex_string, "COLOR").alias("color_clean"),
    col("Description")).show(2)

# translate used to replace single character with another
from pyspark.sql.functions import translate

df.select(translate(col("Description"), "LEET", "1337"), col("Description")) \
    .show(2)

'''regular expression '''
from pyspark.sql.functions import regexp_extract

extract_str = "(BLACK|WHITE|RED|GREEN|BLUE)"
df.select(
    regexp_extract(col("Description"), extract_str, 1).alias("color_clean"),
    col("Description")).show(2)

'''instring searching for a string'''
from pyspark.sql.functions import instr

containsBlack = instr(col("Description"), "BLACK") >= 1
containsWhite = instr(col("Description"), "WHITE") >= 1
'''new field hasSimpleColor which contains BLACK or WHITE'''
df.withColumn("hasSimpleColor", containsBlack | containsWhite) \
    .where("hasSimpleColor") \
    .select("Description").show(3, False)

from pyspark.sql.functions import expr, locate

simpleColors = ["black", "white", "red", "green", "blue"]
def color_locator(column, color_string):
    return locate(color_string.upper(), column) \
        .cast("boolean") \
        .alias("is_" + color_string)
selectedColumns = [color_locator(df.Description, c) for c in simpleColors]
selectedColumns.append(expr("*"))  # has to a be Column type
df.select(*selectedColumns).where(expr("is_white OR is_red")) \
    .select("Description").show(10, False)

'''------Timestamp Operations--------'''
from pyspark.sql.functions import current_date, current_timestamp
dateDF = spark.range(10)\
  .withColumn("today", current_date())\
  .withColumn("now", current_timestamp())
dateDF.createOrReplaceTempView("dateTable")
dateDF.printSchema()

'''Subtracting adding days to a date'''
from pyspark.sql.functions import date_add, date_sub
dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)

'''Finding diff of date add 7 days with todays create column and subtract todays date'''
from pyspark.sql.functions import datediff, months_between, to_date
dateDF.withColumn("week_ago", date_sub(col("today"), 7))\
  .select(datediff(col("week_ago"), col("today"))).show(1)

'''Month difference between days'''
dateDF.select(
   to_date(lit("2016-01-01")).alias("start"),
   to_date(lit("2017-05-22")).alias("end"))\
 .select(months_between(col("start"), col("end"))).show(1)

'''date in string literal format converted to date again'''
from pyspark.sql.functions import to_date, lit
spark.range(5).withColumn("date", lit("2017-01-01"))\
 .select(to_date(col("date"))).show(1)

'''Below it prints wrongly null and 11dec2017 whereas it was 12nov2017'''
dateDF.select(to_date(lit("2016-20-12")),to_date(lit("2017-12-11"))).show(1)

'''Next w provide the dateformat'''
from pyspark.sql.functions import to_date
dateFormat = "yyyy-dd-MM"
cleanDateDF = spark.range(1).select(
  to_date(lit("2017-12-11"), dateFormat).alias("date"),
  to_date(lit("2017-20-12"), dateFormat).alias("date2"))
cleanDateDF.createOrReplaceTempView("dateTable2")

'''now data printed properly'''
spark.sql("select * from dateTable2").show(1)

'''timestamp printing of date'''
from pyspark.sql.functions import to_timestamp
cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()

'''filter date using literal type'''
cleanDateDF.filter(col("date2") > lit("2017-12-12")).show()

'''coalesce - select the first non-null value from a set of columns'''
from pyspark.sql.functions import coalesce
df.select(coalesce(col("Description"), col("CustomerId"))).show(3)

'''all null is shown as- All Null values become this string                     '''
df.na.fill("All Null values become this string")

'''filling null in StockCode and InvoiceNo column with 5'''
df.na.fill("all", subset=["StockCode", "InvoiceNo"])

'''replacing column value "" with "UNKNOWN"'''
df.na.replace([""], ["UNKNOWN"], "Description")

'''explode and split'''
from pyspark.sql.functions import split, explode
df.withColumn("splitted", split(col("Description"), " "))\
 .withColumn("exploded", explode(col("splitted")))\
 .select("Description", "InvoiceNo", "exploded").show(2)

'''UDF Operation'''
'''Spark will serialize the function on the driver and transfer it over the network to all executor processes.'''
'''If the function is written in Python, something quite different happens. Spark starts a Python
process on the worker, serializes all of the data to a format that Python can understand
(remember, it was in the JVM earlier), executes the function row by row on that data in the
Python process, and then finally returns the results of the row operations to the JVM and Spark.'''
udfExampleDF = spark.range(5).toDF("num")
def power3(double_value):
  return double_value ** 3
print(power3(2.0))

'''------Aggregation-----'''
df_agg = spark.read.format("csv")\
.option("header", "true")\
.option("inferSchema", "true")\
.load("/home/bose/PycharmProjects/pyspark3/datasources/online-retail-dataset.csv")\
.coalesce(5)

df_agg.cache()

df_agg.createOrReplaceTempView("dfTable")

print('Count of df_agg:')
print(df_agg.count())

'''Count of column StockCode'''
print('Count of column StockCode')
from pyspark.sql.functions import count
df.select(count("StockCode")).show()

'''countDistinct column'''
print('Distinct count of column')
from pyspark.sql.functions import countDistinct
df.select(countDistinct("StockCode")).show()

'''Get first and last value'''
print('Get first and last value from column')
from pyspark.sql.functions import first, last
df.select(first("StockCode"), last("StockCode")).show()

'''min and max'''
print('Get min and max of column')
from pyspark.sql.functions import min, max
df.select(min("Quantity"), max("Quantity")).show()

'''sum of column'''
print('sum of column value:')
from pyspark.sql.functions import sum
df.select(sum("Quantity")).show()

'''sumDistinct '''
print('sumDistinct:')
from pyspark.sql.functions import sumDistinct
df.select(sumDistinct("Quantity")).show()

'''Complex aggregation'''
print('Complex Aggregation:')
from pyspark.sql.functions import sum, count, avg, expr
df.select(
count("Quantity").alias("total_transactions"),
sum("Quantity").alias("total_purchases"),
avg("Quantity").alias("avg_purchases"),
expr("mean(Quantity)").alias("mean_purchases"))\
.selectExpr(
"total_purchases/total_transactions",
"avg_purchases",
"mean_purchases").show()

'''Stddev'''
print('stddev')
from pyspark.sql.functions import var_pop, stddev_pop
from pyspark.sql.functions import var_samp, stddev_samp
df.select(var_pop("Quantity"), var_samp("Quantity"),
stddev_pop("Quantity"), stddev_samp("Quantity")).show()

'''skewness, kurtosis '''
print('skewness  and kurtosis')
from pyspark.sql.functions import skewness, kurtosis
df.select(skewness("Quantity"), kurtosis("Quantity")).show()

'''corr and covariance '''
from pyspark.sql.functions import corr, covar_pop, covar_samp
df.select(corr("InvoiceNo", "Quantity"), covar_samp("InvoiceNo", "Quantity"),
covar_pop("InvoiceNo", "Quantity")).show()

'''complex type in aggregation '''
print('Complex Type In Aggregation-Country Column')
from pyspark.sql.functions import collect_set, collect_list
df.agg(collect_set("Country"), collect_list("Country")).show()

'''Invoice count group by '''
print('Count for each Invoice')
df.groupBy("InvoiceNo", "CustomerId").count().show()

'''Using agg function we can pass count as expression'''
print('agg groupBy')
from pyspark.sql.functions import count
df.groupBy("InvoiceNo").agg(
count("Quantity").alias("quan"),
expr("count(Quantity)")).show()

'''group with agg'''
print('agg with group')
df.groupBy("InvoiceNo").agg(expr("avg(Quantity)"),expr("stddev_pop(Quantity)"))\
.show()

'''windowing function starting'''
print('windowing function...')
from pyspark.sql.functions import col, to_date
dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))

dfWithDate.show(2)

from pyspark.sql.window import Window
from pyspark.sql.functions import desc
'''Define a date column'''
from pyspark.sql.functions import col, to_date
dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "yyyy-dd-MM"))
dfWithDate.createOrReplaceTempView("dfWithDate")

print(dfWithDate)

from pyspark.sql.window import Window
from pyspark.sql.functions import desc
'''Defining window condition'''
windowSpec = Window\
.partitionBy("CustomerId", "InvoiceDate")\
.orderBy(desc("Quantity"))\
.rowsBetween(Window.unboundedPreceding, Window.currentRow)

'''maximum purchase quantity over all time expression'''
print('maximum purchase quantity over all time expression')
from pyspark.sql.functions import max
maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)

'''purchase rank expression'''
print('purchase rank')
from pyspark.sql.functions import dense_rank, rank
purchaseDenseRank = dense_rank().over(windowSpec)
purchaseRank = rank().over(windowSpec)


'''Now applying all expression to our dataframe'''
from pyspark.sql.functions import col
dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")\
.select(
col("CustomerId"),
col("InvoiceDate"),
col("Quantity"),
purchaseRank.alias("quantityRank"),
purchaseDenseRank.alias("quantityDenseRank"),
maxPurchaseQuantity.alias("maxPurchaseQuantity")).show()

'''------------Join----------'''
person = spark.createDataFrame([
(0, "Bill Chambers", 0, [100]),
(1, "Matei Zaharia", 1, [500, 250, 100]),
(2, "Michael Armbrust", 1, [250, 100])])\
.toDF("id", "name", "graduate_program", "spark_status")
graduateProgram = spark.createDataFrame([
(0, "Masters", "School of Information", "UC Berkeley"),
(2, "Masters", "EECS", "UC Berkeley"),
(1, "Ph.D.", "EECS", "UC Berkeley")])\
.toDF("id", "degree", "department", "school")
sparkStatus = spark.createDataFrame([
(500, "Vice President"),
(250, "PMC Member"),
(100, "Contributor")])\
.toDF("id", "status")

person_null_test = spark.createDataFrame([
(0, "Bill", 0, [100]),
(1, "Matei", 1, [500, 250, 100]),
(2, "Michael", 1, [250, 100]),
(3, "", 1, [250, 100])])\
.toDF("id", "name", "graduate_program", "spark_status")

print('Null safety..')
from pyspark.sql import functions as F
'''person_null_test.select("*").where(F.col("name").("Michael")).show()'''

print('Inner Join:')
joinExpression = person["graduate_program"] == graduateProgram['id']
person.join(graduateProgram, joinExpression).show()

'''Inner Join can be done by passing join type as below'''
joinType = "inner"
person.join(graduateProgram, joinExpression, joinType).show()

'''Outer Join'''
print('Outer Join..')
joinType = "outer"
person.join(graduateProgram, joinExpression, joinType).show()

'''Left Outer Join'''
print('Left Outer Join..')
joinType = "left_outer"
graduateProgram.join(person, joinExpression, joinType).show()

'''Right Outer Join'''
print('Right Outer Join..')
joinType = "right_outer"
graduateProgram.join(person, joinExpression, joinType).show()

'''Left Semi Join'''
print('Left Semi Join..')
joinType = "left_semi"
graduateProgram.join(person, joinExpression, joinType).show()

'''Complex Join'''
print("complex join:")
from pyspark.sql.functions import expr
person.withColumnRenamed("id", "personId")\
.join(sparkStatus, expr("array_contains(spark_status, id)")).show()

'''Communication Strategies In Join
Spark approaches cluster communication in two different ways during joins. It either incurs a
shuffle join, which results in an all-to-all communication or a broadcast join.
1.Shuffle Join: In a shuffle join, every node talks to every other node and they share data according to which
node has a certain key or set of keys (on which you are joining). These joins are expensive
because the network can become congested with traffic, especially if your data is not partitioned
well
2.Broadcast Join: When the table is small enough to fit into the memory of a single worker node, with some
breathing room of course, we can optimize our join. Although we can use a big table–to–big
table communication strategy, it can often be more efficient to use a broadcast join.
We perform it only once at the beginning and then let each individual worker node perform the
work without having to wait or communicate with any other worker node
At the beginning of this join will be a large communication, just like in the previous type of join.
However, immediately after that first, there will be no further communication between nodes.
This means that joins will be performed on every single node individually, making CPU the
biggest bottleneck.
3.Sort Merge Join: https://www.linkedin.com/pulse/spark-sql-3-common-joins-explained-ram-ghadiyaram/'''

'''------------DataSource----------------'''
'''Spark’s read modes
permissive->Sets all fields to null when it encounters a corrupted record and places all corrupted records
in a string column called _corrupt_record
dropMalformed-> Drops the row that contains malformed records
failFast->Fails immediately upon encountering malformed records'''

'''Spark’s save modes
   
append -> Appends the output files to the list of files that already exist at that location
overwrite -> Will completely overwrite any data that already exists there
errorIfExists -> Throws an error and fails the write if data or files already exist at the specified location
ignore -> If data or files exist at the location, do nothing with the current DataFrame

The default is errorIfExists
'''


'''ORC:
    Faster response time
    Parallel processing of row collections(ORC stores collections of rows in one file and within the collection the row data is stored in a columnar format)
    It skips whole block if it doesnt match query
    Predicate Pushdown
    Supports both compressed and uncompressed storage
'''
'''Format difference:-

AVRO vs PARQUET

    1.AVRO is a row-based storage format whereas PARQUET is a columnar based storage format.
    2.PARQUET is much better for analytical querying i.e. reads and querying are much more efficient than writing.
    3.Write operations in AVRO are better than in PARQUET.
    4.AVRO is much matured than PARQUET when it comes to schema evolution. PARQUET only supports schema append whereas AVRO supports a much-featured schema evolution i.e. adding or modifying columns.
    5.PARQUET is ideal for querying a subset of columns in a multi-column table. AVRO is ideal in case of ETL operations where we need to query all the columns.

ORC vs PARQUET
    1.PARQUET is more capable of storing nested data.
    2.ORC is more capable of Predicate Pushdown.
    3.ORC supports ACID properties.
    4.ORC is more compression efficient.
'''

'''Integration Hive with Spark:
You configure Hive by placing your hive-site.xml, core-site.xml, and hdfs-site.xml files in conf/ of
Spark Installation.'''

'''Creating Hive External Tables:
CREATE EXTERNAL TABLE hive_flights (
DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/data/flight-data-hive/' 

---Inserting into specific partition---
INSERT INTO partitioned_flights
PARTITION (DEST_COUNTRY_NAME="UNITED STATES")
SELECT count, ORIGIN_COUNTRY_NAME FROM flights
WHERE DEST_COUNTRY_NAME='UNITED STATES' LIMIT 12

---To view partitions---
SHOW PARTITIONS partitioned_flights

---Refreshing Table Metadata---
---REFRESH TABLE refreshes all cached entries (essentially, files) associated with the table.---
REFRESH table partitioned_flights

---REPAIR TABLE, which refreshes the partitions maintained in the catalog for that given table.---
MSCK REPAIR TABLE partitioned_flights

---Caching Tables---
CACHE TABLE flights
'''

'''---------------------DATASET---------------------'''
'''
We already worked with DataFrames,which are Datasets of type Row, and are available across Spark’s different 
languages. Datasets are a strictly Java Virtual Machine (JVM) language feature that work only with Scala and 
Java.'''


'''
When you use the DataFrame API, you do not create strings or
integers, but Spark manipulates the data for you by manipulating the Row object. In fact, if you
use Scala or Java, all “DataFrames” are actually Datasets of type Row. To efficiently support
domain-specific objects, a special concept called an “Encoder” is required. The encoder maps the
domain-specific type T to Spark’s internal type system.

For example, given a class Person with two fields, name (string) and age (int), an encoder
directs Spark to generate code at runtime to serialize the Person object into a binary structure.
When using DataFrames or the “standard” Structured APIs, this binary structure will be a Row.
When we want to create our own domain-specific objects, we specify a case class in Scala or a
JavaBean in Java. Spark will allow us to manipulate this object (in place of a Row) in a
distributed manner.

When to Use Datasets:-
1.When the operation(s) you would like to perform cannot be expressed using DataFrame
manipulations
2.When you want or need type-safety, and you’re willing to accept the cost of performance to achieve it

When to use RDD:-
1.You need some functionality that you cannot find in the higher-level APIs; for example,
if you need very tight control over physical data placement across the cluster.
2.You need to maintain some legacy codebase written using RDDs.
3.You need to do some custom shared variable manipulation.
'''

'''RDD Types:
1.the “generic” RDD type or a key-value RDD that provides additional
2.functions, such as aggregating by key.
'''

'''
Each RDD is characterized by five main properties:
1.A list of partitions
2.A function for computing each split
3.A list of dependencies on other RDDs
Optionally, a Partitioner for key-value RDDs (e.g., to say that the RDD is hash-
partitioned)
Optionally, a list of preferred locations on which to compute each split (e.g., block
locations for a Hadoop Distributed File System [HDFS] file)

'''

'''
As python doesnt have Datasets hence spark.range(10).rdd gives type as Row

--To operate on the data need to convert to RDD of type Row
spark.range(10).toDF("id").rdd.map(lambda row: row[0])
'''
'''
To create an RDD from a collection, you will need to use the parallelize method on a
SparkContext (within a SparkSession).
myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"\
.split(" ")
words = spark.sparkContext.parallelize(myCollection, 2)
We can set above RDD name as below:
words.setName("myWords")
words.name() # myWords

---distinct---
A distinct method call on an RDD removes duplicates from the RDD:
words.distinct().count()

---filter---
def startsWithS(individual):
return individual.startswith("S")

words.filter(lambda word: startsWithS(word)).collect()

---map---
words2 = words.map(lambda word: (word, word[0], word.startswith("S")))
words2.filter(lambda record: record[2]).take(5)

This returns a tuple of “Spark,” “S,” and “true,” as well as “Simple,” “S,” and “True.”

---flatMap---
flatMap provides a simple extension of the map function we just looked at. Sometimes, each
current row should return multiple rows, instead.

words.flatMap(lambda word: list(word)).take(5)
This yields S, P, A, R, K.


--mapPartitions--
--below runs once per partition--
words.mapPartitions(lambda part: [1]).sum() # 2

----glom----
spark.sparkContext.parallelize(["Hello", "World"], 2).glom().collect()
# [['Hello'], ['World']]

'''
myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"\
.split(" ")
words = spark.sparkContext.parallelize(myCollection, 2)
print(words.take(2))

'''Execution Modes
You have three modes to choose from:
  Cluster mode
  Client mode
  Local mode

The spark.sql.shuffle.partitions default value is 200, which means that when there is a shuffle
performed during execution, it outputs 200 shuffle partitions by default.

A good rule of thumb is that the number of partitions should be larger than the number ofexecutors on your 
cluster, potentially by multiple factors depending on the workload.

Stages in Spark consist of tasks. Each task corresponds to a combination of blocks of data and a
set of transformations that will run on a single executor. If there is one big partition in our
dataset, we will have one task. If there are 1,000 little partitions, we will have 1,000 tasks that
can be executed in parallel.

Some popular cluster-level monitoring tools include Ganglia and Prometheus.
'''
