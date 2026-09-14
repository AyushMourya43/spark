import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = (
    SparkSession.builder #spark sesssion created 
    .appName("Hospital Patient Records")
    .master("local[*]")
    .getOrCreate()
) 

print("Spark session started!")

# 2. Read patients.csv without using schema inference. Display the DataFrame and its schema.

df1 = (
    spark.read
    .option("header", True)
    .csv("input/patients.csv")
)

print("CSV without schema inference:")

df1.show()

df1.printSchema()

# 3. Read the same CSV using inferSchema=True. Display the DataFrame and its schema.

df2 = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("input/patients.csv")
)

print("CSV with schema inference:")

df2.show()

df2.printSchema() 

# 4. Compare the schemas obtained in Questions 2 and 3. What differences do you observe?
# i oberved that when schema inference is not used, all columns are read as strings. When schema
# inference is used, the columns are read with their appropriate data types means the last value 
# data type of the coulms ( jo column m last value hogi wahi type degaa last means jo bhi last data type hogaa woh )

# 5. Create an explicit StructType schema for all six columns in the dataset.

schema = StructType([
    StructField("patient_id", IntegerType(), True),  #nullable = True means that the column can contain null values
    StructField("patient_name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("is_admitted", BooleanType(), True),
    StructField("department", StringType(), True)
])

# 6. Read the CSV using your explicit schema and display the data and schema.

df3 = (
    spark.read
    .option("header", True)
    .schema(schema)
    .csv("input/patients.csv")
)

print("CSV with explicit schema:")

df3.show()

df3.printSchema()

# 7. Explain why a hospital data-processing system might prefer an explicit schema instead of 
# relying completely on schema inference.

# InferSchema → Spark decides datatype.

# Explicit Schema → We decide datatype.

# 8. Create a patients.json file containing at least 5 records from the CSV dataset.

# 9. Read the JSON file using Spark and display its data and schema

# Q9: Read JSON file

df_json = (
    spark.read
    .option("multiLine", True)
    .json("input/patients.json")
)       # spark.read.json() → JSON file ko read karke DataFrame banata hai.

print("JSON Data:")

df_json.show()

print("JSON Schema:")

df_json.printSchema()