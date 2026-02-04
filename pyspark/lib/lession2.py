from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# Start your session
spark = SparkSession.builder.appName("Lesson2_Transform").getOrCreate()

# Sample Data: Product, Price, and Store Location
data = [
    ("Laptop", 1200, "Online"),
    ("Mouse", 25, "In-Store"),
    ("Keyboard", 50, "Online"),
    ("Monitor", 300, "In-Store")
]

columns = ("Product", "Price", "Location")

df = spark.createDataFrame(data, schema= columns)


# --- TRANSFORMATION 1: Select only Product and Price ---
# We ignore "Location" for now
#product_prices = df.select("Product", "Price")

# --- TRANSFORMATION 2: Filter for expensive items ---
# We only want items that cost more than $100
#expensive_items = product_prices.filter(col("Price")>100)

# --- TRANSFORMATION 3: Filter for online items ---
# We only want items that are available online
online_items = df.filter(col("Location") == "Online")

# --- ACTION: Show the final result ---
#print("Expensive Products:")
#expensive_items.show()

print("Online Products:")
online_items.show()


