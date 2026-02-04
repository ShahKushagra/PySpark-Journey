#1 Import the necessary library
from pyspark.sql import SparkSession

# Initialize the session
spark = SparkSession.builder\
        .appName("MyFirstStep")\
        .getOrCreate()

print("Spark is ready!")

data = [("Laptop", 1200), ("Mouse", 25), ("Keyboard", 50)]

colums = ["Product", "Price"]

df = spark.createDataFrame(data, schema=colums)

df.show()
