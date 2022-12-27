import pyspark
from pyspark.sql import functions as F, SparkSession

# from
# Understanding What is SparkSession in Spark

'''
Since Spark 2.0, SparkSession is an entry point to PySpark to work with RDD,DataFrame,Dataset.
Many diffrent spark context API are now under one umbrella of SparkSession like SQLContext,HiveContext,SparkContext, HiveContext e.t.c.,

'''

# Creating SparkSession using builder method

spark = SparkSession.builder.master("local[1]").appName(
    "test_SparkSession").getOrCreate()

print(spark)
# Create new SparkSession in the same PySpark Application
spark2 = SparkSession.newSession
print(spark2)

spark3 = SparkSession.builder.getOrCreate()
print(spark3)
