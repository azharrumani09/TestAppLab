# Databricks notebook source
# DBTITLE 1,Product
# Replace with your actual file path in DBFS or mounted volume
file_path_product = "dbfs:/Volumes/workspace/default/test/product_reference 2.csv"

# Read CSV file into a DataFrame
df_product = spark.read.format("csv") \
    .option("header", "true")\
    .option("inferSchema", "true") \
    .load(file_path_product)

# Display the DataFrame
df_product.show()


# COMMAND ----------

# DBTITLE 1,Sales
# Replace with your actual file path in DBFS or mounted volume
file_path_sales = "dbfs:/Volumes/workspace/default/test/sales_data 2.csv"

# Read CSV file into a DataFrame
df_sales = spark.read.format("csv") \
    .option("header", "true")\
    .option("inferSchema", "true") \
    .load(file_path_sales)

# Display the DataFrame
df_sales.display()


# COMMAND ----------

# DBTITLE 1,API
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql import Row
import requests

# Create Spark session
spark = SparkSession.builder.getOrCreate()

# Define base currencies and fallback exchange rates to USD
base_currencies = ["EUR", "USD", "GBP"]
fallback_rates = {
    "EUR": 1.1,
    "USD": 1.0,
    "GBP": 1.3
}

# Prepare lists for exchange rates and error logs
all_rates = []
error_logs = []

# Function to fetch live exchange rate from an API
def fetch_live_rate(base_currency):
    # url = f"https://api.exchangerate.host/latest?base={base_currency}&symbols=USD"
    url = f"https://api.exchangerate-api.com/v4/latest/{base_currency}"
    response = requests.get(url, timeout=5)
    response.raise_for_status()
    data = response.json()
    return data["rates"]["USD"], data["date"]

# Fetch exchange rates with fallback and error logging
for base in base_currencies:
    timestamp = datetime.now()
    try:
        rate, date = fetch_live_rate(base)
    except Exception as e:
        rate = fallback_rates[base]
        date = "fallback"
        error_type = type(e).__name__
        error_message = str(e)

        # Log error details
        error_logs.append(Row(
            BaseCurrency=base,
            ErrorType=error_type,
            ErrorMessage=error_message,
            Timestamp=timestamp
        ))

    # Log exchange rate
    all_rates.append(Row(
        BaseCurrency=base,
        Date=date,
        TargetCurrency="USD",
        ExchangeRate=rate
    ))

# Define schemas
schema_rates = StructType([
    StructField("BaseCurrency", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("TargetCurrency", StringType(), True),
    StructField("ExchangeRate", DoubleType(), True)
])

schema_errors = StructType([
    StructField("BaseCurrency", StringType(), True),
    StructField("ErrorType", StringType(), True),
    StructField("ErrorMessage", StringType(), True),
    StructField("Timestamp", TimestampType(), True)
])

# Create DataFrames
df_usd_rates = spark.createDataFrame(all_rates, schema_rates)
df_error_logs = spark.createDataFrame(error_logs, schema_errors)

# Display results
df_usd_rates.show()
df_error_logs.show()


# COMMAND ----------

# DBTITLE 1,Data Cleanup and Transformation
from pyspark.sql.functions import col, round, row_number, to_date ,regexp_replace, expr, regexp_extract, when
from pyspark.sql.window import Window

# Replace null SaleAmount with 0
df_sales = df_sales.fillna({"SaleAmount": 0})

# Join with product reference data to get ProductName and ProductCategory
df_sales_lookup = df_sales.join(
    df_product,
    on="ProductID",
    how="left"
).withColumnRenamed("Category", "ProductCategory")\
.withColumn("OrderDate",regexp_replace(col("OrderDate"), "/", "-"))\
.withColumn(
    "OrderDate",
    when(
        regexp_extract(col("OrderDate"), r"^\d{2}-\d{2}-\d{4}$", 0) != "",
        to_date(col("OrderDate"), "dd-MM-yyyy")
    ).otherwise(None)
)\
.select("OrderID","ProductID","SaleAmount","OrderDate","Region","CustomerID","Discount","Currency","ProductName","ProductCategory")

# Join sales with exchange rates
df_sales_converted = df_sales_lookup.join(
    df_usd_rates,
    col("Currency") == col("BaseCurrency"),
    how="left"
).withColumn(
    "SaleAmountUSD",
    round(col("SaleAmount") * col("ExchangeRate"), 2)
).select("OrderID","ProductID","SaleAmount","OrderDate","Region","CustomerID","Discount","Currency","ProductName","ProductCategory","BaseCurrency","ExchangeRate","SaleAmountUSD")


# # Define columns to check for duplicates
dedup_columns = ["OrderID","ProductID","SaleAmount","OrderDate","Region","CustomerID","Discount","Currency","ProductName","ProductCategory","BaseCurrency","ExchangeRate","SaleAmountUSD"]

# Identify duplicates
window_spec = Window.partitionBy(*dedup_columns).orderBy("OrderID")
df_with_row_num = df_sales_converted.withColumn("row_num", row_number().over(window_spec))

df_sales_rejected = df_with_row_num.filter((col("row_num") > 1) | (col("OrderDate").isNull())).drop("row_num")

df_sales_rejected.display()

# Drop duplicates from main DataFrame
df_sales_cleaned = df_sales_converted.dropDuplicates(dedup_columns).filter(col("OrderDate").isNotNull())

# Display cleaned data
display(df_sales_cleaned.select(*dedup_columns))


# COMMAND ----------

# DBTITLE 1,Create Schema
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS main.sales_data;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Write sales_cleaned data

df_sales_cleaned.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("main.sales_data.sales_cleaned")


# COMMAND ----------

# DBTITLE 1,Write sales_rejected data
df_sales_rejected.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("main.sales_data.sales_rejected")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM main.sales_data.sales_cleaned
# MAGIC UNION ALL
# MAGIC SELECT * FROM main.sales_data.sales_rejected
# MAGIC
# MAGIC