# from pyspark.sql.functions import contains
import json
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType

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

# print(schema_2)
data_2 = [(('Sushil', "", "Tiwari"), 'Channasandra', 'Single'),
          (("Sonal", "", "."), "HSR Layout", "Married")]

final_df2 = spark.createDataFrame(schema=schema_2, data=data_2)

final_df2.printSchema()
final_df2.show()


# Using struct we can restructure the schema of the dataframe.
'''
final_df3 = final_df2.withColumn("OtherInfo", F.struct(
    F.col("place"), F.col("Relationship_Status"))).drop("place", "Relationship_Status")
final_df3.printSchema()
final_df3.show(truncate=False)
'''

# print(final_df2.schema.json())
'''Output
{"fields":[{"metadata":{},"name":"name","nullable":true,"type":{"fields":[{"metadata":{},"name":"first_name","nullable":false,"type":"string"},{"metadata":{},"name":"Middle_Name","nullable":true,"type":"string"},{"metadata":{},"name":"Last_Name","nullable":true,"type":"string"}],"type":"struct"}},{"metadata":{},"name":"place","nullable":false,"type":"string"},{"metadata":{},"name":"Relationship_Status","nullable":false,"type":"string"}],"type":"struct"}
'''
# print(final_df2.schema.simpleString())
'''
Output
truct<name:struct<first_name:string,Middle_Name:string,Last_Name:string>,place:string,Relationship_Status:string>
'''

# Now Creating a Dataframe using the json File
'''
schema_of_json = StructType.fromJson(json.loads(final_df2.schema.json()))
data_of_json = [(("Sumit", " ", "Tiwari"), "Greater Noida", "Single")]
print(schema_of_json)
final_df4 = spark.createDataFrame(data_of_json, schema_of_json)
final_df4.show(truncate=False)
'''

# Checking if a column name exist in a DataFrame
final_df2.show()
col_name = "place"

if col_name in final_df2.schema.fieldNames():
    print("Column {} Found in the DataFrame".format(col_name))

# Creating one more DataFrame from complex schema structure
schema_3 = StructType([StructField("Vehicle_Type",
                                   StructType([StructField("Two Wheeler", StringType(), False), StructField("Four Wheeler", StringType(), False), StructField(
                                       "Ten Wheeler", StringType(), False)])),
                       StructField("Brand_Name",
                                   StringType(), False),
                       StructField("Customized_Status",
                                   StringType(), False)
                       ])

data_3 = ([(("", "2 Wheeler", ""), "Royal Enfield", "Yes")])

tempo_df = spark.createDataFrame(data_3, schema_3)

print(tempo_df.schema)
tempo_df.show(truncate=False)
