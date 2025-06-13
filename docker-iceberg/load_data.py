from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load Data") \
    .getOrCreate()

# Path to CSV file - using direct path inside container
file_path = "/data/home-credit-default-risk-dataset/daily/20250612/application_train_20250612.csv"

# Load the CSV file
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(file_path)

# Add timestamp columns
df = df.withColumn("DATA_TIMESTAMP", lit("20250612")) \
    .withColumn("LOAD_DATE", lit(datetime.now()))

# Show a sample of the data
print("Sample data:")
df.show(5)

# Count rows
print(f"Total rows: {df.count()}")

# Insert into Iceberg table
print("Loading data into Iceberg table...")
df.write \
    .format("iceberg") \
    .mode("append") \
    .save("home_credit.application_train")

print("Data loaded successfully")

# Count rows in the destination table
count = spark.sql("SELECT COUNT(*) FROM home_credit.application_train WHERE DATA_TIMESTAMP = '20250612'").collect()[0][0]
print(f"Rows in home_credit.application_train: {count}")

# Close the Spark session
spark.stop()
