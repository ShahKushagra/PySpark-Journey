from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructField, StructType, IntegerType, StringType

#Start Session
spark = SparkSession.builder.appName("Lesson3_Schema").getOrCreate()

# 1. Define the Schema (Expert Level)
# StructField(Name, DataType, CanBeNull?)
schema = StructType([
    StructField("Product", StringType(), True),
    StructField("Price", IntegerType(), True),
    StructField("Category", StringType(), True)
])

data = [
    ("Laptop", 1000, "Electronics"),
    ("Desk", 200, "Furniture"),
    ("Lamp", 40, "Furniture")
]

df = spark.createDataFrame(data, schema=schema)

# 2. Add a new column 'Discounted_Price' (Price * 0.8)
# withColumn(New_Name, Logic)
df_final = df.withColumn("Discounted_Price", col("Price") * 0.8).withColumn("Tax", col("Discounted_Price") * 0.1)

print("Schema Overview:")
df_final.printSchema()

print("Data with Discounts and Tax:")
df_final.show()



