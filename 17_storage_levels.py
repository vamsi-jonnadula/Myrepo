from pyspark.sql.session import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as f
import pandas as pd
spark = SparkSession. \
  builder. \
  appName('Storage Level Example'). \
  getOrCreate()

data1 = {'PassengerId': {0: 1, 1: 2, 2: 3, 3: 4, 4: 5},
         'Name': {0: 'Owen', 1: 'Florence', 2: 'Laina', 3: 'Lily', 4: 'William'},
         'Sex': {0: 'male', 1: 'female', 2: 'female', 3: 'female', 4: 'male'},
         'Survived': {0: 0, 1: 1, 2: 1, 3: 1, 4: 0}}

data2 = {'PassengerId': {0: 1, 1: 2, 2: 3, 3: 4, 4: 5},
         'Age': {0: 22, 1: 38, 2: 26, 3: 35, 4: 35},
         'Fare': {0: 7.3, 1: 71.3, 2: 7.9, 3: 53.1, 4: 8.0},
         'Pclass': {0: 3, 1: 1, 2: 3, 3: 1, 4: 3}}

df1_pd = pd.DataFrame(data1, columns=data1.keys())
df2_pd = pd.DataFrame(data2, columns=data2.keys())

'''Converting pandas df to spark dataframe'''
df1 = spark.createDataFrame(df1_pd)
df2 = spark.createDataFrame(df2_pd)

df1.show()

df2.show()

'''Selecting only few fields'''
cols1 = ['PassengerId', 'Name']
df1.select(cols1).show()

'''Sorting based on Fare column'''
df2.sort('Fare', ascending=False).show()

'''Join based on PassengerId'''
dfj1=df1.join(df2, ['PassengerId'])

dfj1.show()

print(dfj1.explain())

'''Checks if cached'''
print(df1.storageLevel.useMemory)

'''Caching - cache() method by default stores the data in-memory (MEMORY_ONLY) '''
print(df1.cache())

'''Again check if df is cached'''
print(df1.is_cached)

'''Storage level-There are 4 caching levels that can be fine-tuned with persist()'''
print(df1.storageLevel)

'''Persist- Persist() in Apache Spark by default takes the storage level as MEMORY_AND_DISK t'''
'''Using persist(), will initially start storing the data in JVM memory and when the data requires additional 
storage to accommodate, it pushes some excess data in the partition to disk and reads back the data from disk 
when it is required. Since it involves some I/O operation, persist() is considerably slower than cache(). 

Storage levels in persist can be:-
            MEMORY_ONLY,
            MEMORY_AND_DISK,
            MEMORY_ONLY_SER,
            MEMORY_AND_DISK_SER,
            DISK_ONLY,
            MEMORY_ONLY_2,
            MEMORY_AND_DISK_2,
            DISK_ONLY_2
            MEMORY_ONLY_SER_2,
            MEMORY_AND_DISK_SER_2

Note: _2 in level means data is persisted and each is replicated into two cluster node
      _SER means serialized in either memory or disk.Serialization saves space but uses cpu memory
'''
df1.persist()

'''Storage level after persist'''
print(df1.storageLevel)

'''Chaining too many union() cause performance problem or even out of memory errors. 
checkpoint() truncates the execution plan and saves the checkpointed dataframe to a temporary location on the 
disk.
Jacek Laskowski recommends caching before checkpointing so Spark doesn’t have to read in the dataframe from
disk after it’s checkpointed.'''

'''Checkpoint'''
sc = spark.sparkContext
'''Checkpoint folder is defined'''
sc.setCheckpointDir("/home/bose/PycharmProjects/pyspark3/checkpointdir")

'''Without checkpoint explain plan'''
df = df1.join(df1, ['PassengerId'])
df.join(df1, ['PassengerId']).explain()

'''With checkpoint explain plan'''
df = df1.join(df1, ['PassengerId']).checkpoint()
df.join(df1, ['PassengerId']).explain()

