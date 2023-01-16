# Pyspark column class

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Starting a sparksession to facilitate the pyspark snippet
spark = SparkSession.builder.appName("Test_firm").getOrCreate()

# lit is used to create a column with a literal/constant value
'''
_col_df = lit("sushil_firm")
print(_col_df)
'''


# Creating a sample dataframe for example in further below examples
schema_df = StructType([StructField('fname', StringType(), False),
                       StructField('Age', IntegerType(), False)])

# print(schema_df)
df_data = [('Sushil', 28), ('Sonal', 27)]
df_1 = spark.createDataFrame(data=df_data, schema=schema_df)
'''
# Below are different example of accessing a dataframe Columns in multiple ways

# 1 Column name is case-sensitive in Pyspark
# df_1.select(df_1.Age).show()

# 2 Accessing column names having special characters with backtick
# df_1.select(df_1['`Name.fname`']).show()

# 3 Accessing with col keyword
# df_1.select(col('Age'), col('`Name.fname`')).show()

'''

# Creating a sample dataframe to perform arithmetic operation on PySpark Dataframe columns
'''
data_2 = [(200, 15, 5), (87872, 897, 123)]

df_2 = spark.createDataFrame(data_2, schema=['val1', 'val2', 'val3'])
df_2.show()

df_2.select(df_2.val1 + df_2.val3).alias('sum_1_3').show()
df_2.select(df_2['val1'] > df_2['val3']).alias('is_col_1_large').show()
df_2.select(df_2['val1'] == df_2['val3']).alias('is_both_col_eq').show()
'''

'''
# Sorting dataframe columns in asc/desc order
df_1.sort(df_1.Age.asc()).show()
df_1.sort(df_1['Age'].desc()).show()

# Checking individual column datatype
print(df_1.select(col("Age")).dtypes)
'''

# Casting the dataype of a dataframe column
# |
# -----cast & astype can be used interchangiably to convert the dataype of dataframe columns, both are same
'''
df_2 = df_1.select(
    df_1.columns[0], df_1[df_1.columns[1]].astype('string').alias('type'))
df_2 = df_1.select(
    df_1.columns[0], df_1[df_1.columns[1]].cast('string').alias('type'))
df_2.printSchema()
print(df_2.dtypes)
'''

# # Checking for the null values in column of a Dataframe
'''
df_1.filter(df_1.Age.isNull()).show()
df_1.filter(df_1.Age.isNotNull()).show()
# Check the status of all the column and returns boolean values in result
df_1.select(df_1.Age.isNull()).show()
df_1.select(df_1.Age.isNotNull()).show()
'''
'''
# Fetching values from the dataframe on basis of like expression
df_1.filter(df_1.fname.like('%s%')).show()

# Fetching substring from a particular dataframe column
df_1.select(df_1.fname.substr(1, 2).name('substr_col')).show()
'''

'''
# Use of when and otherwise in pyspark dataframe
df_1.select(df_1.columns[0], when(df_1.fname.like('%s%'), 'Found'
                                  ).otherwise('Not Found').name('col_check')).show()

# Checking for a certain value in dataframe column

df_1.show()

df_1.filter(df_1.Age.isin(['28'])).show()
df_1.filter(~df_1.Age.isin(['28'])).show()
'''
