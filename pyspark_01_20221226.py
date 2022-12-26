from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# from pyspark.sql.types import DataType

# --------------------------Note for Github----------------------------

# Explaining Functionality of StructType & StructField
'''
StructType is collection of StructField which is used to define complex schema for DataFrames. The StructField
defines the Column Name, Column DataType, Column Nullable Boolean value.

However, PySpark infers schema from the data (i.e., inferSchema=True), but in the production environment in organization we should not prefer the inferSchema=True,
we should explicitly define the schema of the dataframe.
StructType([StructField('Column Name',StringType(),True)])
'''

# Initializing Spark Session
spark = SparkSession.builder.appName('gov_auto_data').getOrCreate()

# Plain Structure StructType, Not Nested
'''
data_1 = [('MotorCycle', "Cruiser", '220', '78236273')]

schema_1 = StructType([StructField('Category', StringType(), False), StructField('Vehicle Type', StringType(
), True), StructField('Registered_2016_17', StringType(), False), StructField('Total_Registered', StringType(), False)])

final_df = spark.createDataFrame(data=data_1, schema=schema_1)

final_df.printSchema()
final_df.show()
'''

# Complex Nested StructType Example

schema_2 = StructType([
    StructField("name", StructType([
        StructField("first_name", StringType(), False),
        StructField("Middle_Name", StringType(), True),
        StructField("Last_Name", StringType(), True)
    ])),
    StructField("place", StringType(), False),
    StructField("Relationship_Status", StringType(), False)
])

print(schema_2)
data_2 = [(('Sushil', "", "Tiwari"), 'Channasandra', 'Single'),
          (("Sonal", "", "."), "HSR Layout", "Married")]

final_df2 = spark.createDataFrame(schema=schema_2, data=data_2)

final_df2.printSchema()
final_df2.show()
