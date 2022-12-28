import pyspark
from pyspark.sql import functions as F, SparkSession

# from
# Understanding What is SparkSession in Spark

'''
Since Spark 2.0, SparkSession is an entry point to PySpark to work with RDD,DataFrame,Dataset.
Many diffrent spark context API are now under one umbrella of SparkSession like SQLContext,HiveContext,SparkContext, HiveContext e.t.c.,

'''

# Creating SparkSession using builder method

spark = SparkSession.builder.master("local[2]").appName(
    "test_SparkSession").getOrCreate()
# print(spark)


# Create new SparkSession in the same PySpark Application
'''
spark2 = SparkSession.newSession
print(spark2)
'''

# It gets the already created SparkSession
'''
spark3 = SparkSession.builder.getOrCreate()
print(spark3)
'''

# Implementing some pyspark config properties to the spark Session
'''
spark4 = SparkSession.builder.master("local[2]").config(
    "Spark.conf.set.config_property_name", "config_value").appName("spark-config").getOrCreate()
'''

# Setting some Spark config properties
# spark.conf.set("spark.task.reaper.enabled", "False")

# In order to use Hive with spark, we need to enable Hive as shown below
'''enableHiveSupport connects to Hive Catalog, with this implemented we can read/write from/to Hive Metastore,
It does not mean that the query are executed in Hive QL, but Spark SQL only.'''

'''
spark5 = SparkSession.builder.master("local[2]").enableHiveSupport().appName(
    "Incorporate-Hive").getOrCreate()
'''

# Create PySpark DataFrame without schema

df1 = spark.createDataFrame([("Sushil", "", "Tiwari"), (
                            "Sumit", "", "Tiwari"), ("Saurabh", "", "Tiwari")], ["F_Name", "M_Name", "L_Name"])

df1.show(truncate=False)

# Working with Spark SQL on dataframe/dataset

df1.createOrReplaceTempView("bro_data")

query_1 = spark.sql("select * from bro_data where L_Name like '%wa%'")
query_1.show()

# Creating HIve Table and querying on it, saveAsTable() creates Hive Managed table
'''
spark.table("bro_data").write.saveAsTable("sample_hive_table")
hive_df = spark.sql("select * from sampl_hive_table")

hive_df.show(truncate=False)
'''

# Working with Catalog in Spark
print(spark.catalog.listDatabases())
print(spark.catalog.listTables())
