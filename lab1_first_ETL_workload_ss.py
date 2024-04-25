# Databricks notebook source
# This Python code snippet utilizes PySpark, a Spark Python API, to perform ETL (Extract, Transform, Load) operations. It specifically demonstrates how to configure Auto Loader for streaming data ingestion from JSON files into a Delta table on Databricks. Here's a step-by-step explanation:

# Import Functions: It imports essential functions from pyspark.sql.functions. The col function is used to refer to a DataFrame column, current_timestamp returns the current timestamp, which can be useful for adding a processing timestamp to ingested data.

# Define Variables:

# file_path: Specifies the location of the JSON files to be read.
# username: Extracts and formats the current Databricks user's name to ensure it contains only alphanumeric characters and underscores. This is useful for creating user-specific paths or table names that are SQL-compliant.
# table_name: Creates a unique table name for the Delta table using the formatted username.
# checkpoint_path: Specifies a path for storing checkpoint information to manage state between restarts of the stream.
# Cleanup: Drops the Delta table if it exists from previous runs and deletes the checkpoint directory to ensure a clean start. This is useful for demonstration or development purposes but should be used with caution in production.

# Configure Auto Loader for JSON Data Ingestion:

# Initiates a read stream using spark.readStream with the format set as "cloudFiles", indicating the use of Auto Loader for scalable and efficient data loading.
# Specifies the format of the files to ingest ("json") and a path to store the schema of the ingested data (checkpoint_path).
# Loads the data from file_path.
# Transforms the data stream by adding two columns: source_file, which contains the path of the source file each record was ingested from, and processing_time, which marks the time each record was processed.
# Configures the write stream with the checkpoint location (to manage state) and triggers the ingestion immediately with availableNow=True, which is a special trigger that processes all available data before stopping.
# Writes the data to a Delta table (table_name) created specifically for this demonstration.
# This code efficiently demonstrates the setup for a structured streaming ETL pipeline in Databricks using Auto Loader for JSON data, handling schema evolution, checkpointing for failure recovery, and adding metadata to ingested records.





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

'''
# Configure Auto Loader to ingest JSON data to a Delta table
df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").option("cloudFiles.schemaLocation", checkpoint_path).load(file_path)
df = df.select("*", col("_rescued_data").alias("source_file"), current_timestamp().alias("processing_time"))
df.writeStream.option("checkpointLocation", checkpoint_path).trigger(once=True).toTable(table_name)
'''

# Configure Auto Loader to ingest JSON data to a Delta table
(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .load(file_path)
  .select("*", col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time"))
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .toTable(table_name))

# COMMAND ----------

# The line of Python code you've provided is for reading a table into a DataFrame using the Apache Spark framework. Let's break it down:

# spark: This refers to an instance of a SparkSession, which is the entry point to programming Spark with the Dataset and DataFrame API. A SparkSession is created when you start Spark in your application.

# .read: This is an attribute of the SparkSession that returns a DataFrameReader object. The DataFrameReader is used for reading data into a DataFrame.

# .table(table_name): This method of the DataFrameReader object allows you to read a table by specifying its name. The table_name variable should contain the string name of the table you want to read. This table must already be accessible within the Spark cluster's environment, potentially stored in a database or a file system that Spark can access.

# In essence, this single line of code initializes the process of loading a specified table from your storage system into a DataFrame in Spark, making it ready for data manipulation, analysis, or processing tasks.






df = spark.read.table(table_name)

# COMMAND ----------

display(df)
