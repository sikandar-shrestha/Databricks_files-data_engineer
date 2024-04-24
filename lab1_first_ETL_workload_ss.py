# Databricks notebook source
# Import functions
from pyspark.sql.functions import col, current_timestamp

# Define variables used in code below
file_path = "/databricks-datasets/structured-streaming/events"
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
table_name = f"{username}_etl_quickstart"
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"

# Clear out data from previous demo execution
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
dbutils.fs.rm(checkpoint_path, True)

# Configure Auto Loader to ingest JSON data to a Delta table
df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").option("cloudFiles.schemaLocation", checkpoint_path).load(file_path)
df = df.select("*", col("_rescued_data").alias("source_file"), current_timestamp().alias("processing_time"))
df.writeStream.option("checkpointLocation", checkpoint_path).trigger(once=True).toTable(table_name)

# COMMAND ----------

df = spark.read.table(table_name)

# COMMAND ----------

display(df)
