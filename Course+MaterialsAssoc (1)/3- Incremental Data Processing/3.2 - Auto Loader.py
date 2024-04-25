# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# Based on the given command, it appears you're trying to execute a script or tool named Copy-Datasets that is located in a parent directory relative to your current directory. Here's a breakdown of the command's components:

#../: This part specifies a path relative to your current working directory. The .. represents the parent directory, so this path points to something located one directory up from where you are now.
#Includes/: This directory name implies that Copy-Datasets resides within an Includes directory that is found in the parent directory of your current location.

#Copy-Datasets: This is presumably the name of a script, program, or tool you're attempting to run. This could be a script file for copying datasets as the name suggests.

#In summary, this command is trying to execute a script or application named Copy-Datasets that is located in an "Includes" folder in the parent directory of your current working directory. The exact behavior and functionality of Copy-Datasets would depend on how it is implemented, which isn't specified in the provided code snippet.

%run ../Includes/Copy-Datasets

# COMMAND ----------

# The code starts by using the dbutils.fs.ls function to list all the files located in the directory specified by the dataset_bookstore variable, with the subdirectory orders-raw.

# The ls function returns a list of file metadata, including the file path, name, size, and other details.

# Finally, the display function is used to visualize the list of files in a tabular format.

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

# This code reads streaming data from a cloud storage location in parquet format using Spark Structured Streaming. It specifies the cloud storage format as "cloudFiles" and provides the schema location for the data. The data is loaded from the specified dataset and path ("dataset_bookstore/orders-raw").

# The code then writes the streaming data to a table named "orders_updates" using a streaming writeStream operation. It also specifies the checkpoint location for fault tolerance and resuming from failures as "dbfs:/mnt/demo/orders_checkpoint".


(spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/orders_checkpoint")
        .load(f"{dataset_bookstore}/orders-raw")
      .writeStream
        .option("checkpointLocation", "dbfs:/mnt/demo/orders_checkpoint")
        .table("orders_updates")
)

# COMMAND ----------

# This SQL command retrieves all columns and all records from the table named orders_updates in a database. When executed, it will display the entire contents of orders_updates without any filtering or sorting, showing every entry as it exists in the table.

%sql
SELECT * FROM orders_updates

# COMMAND ----------

#The given SQL code selects the count of all rows from the "orders_updates" table.

%sql
SELECT count(*) FROM orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC #### Land new data

# COMMAND ----------

load_new_data()

# COMMAND ----------

# This code uses the dbutils.fs.ls() function to list all the files in the directory specified in the dataset_bookstore variable. The resulting list of files is then displayed using the display() function.

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

#The code is performing a SQL query on a table called orders_updates. It is selecting the count of all the rows in the table and returning the result

%sql
SELECT count(*) FROM orders_updates

# COMMAND ----------

# This SQL code is used to describe the structure of a table named "orders_updates" in a database. The DESCRIBE statement is used to retrieve information about the columns and data types present in a table. By referencing the "orders_updates" table, this code will show the structure of that specific table, including the column names, data types, and any constraints or indexes that may be applied to the columns.


%sql
DESCRIBE HISTORY orders_updates

# COMMAND ----------

#The SQL code drops the table called "orders_updates". This means that the table, along with all its data and structure, will be permanently deleted from the database.

%sql
DROP TABLE orders_updates

# COMMAND ----------

#The given code is using the dbutils.fs.rm() function to remove a directory orders_checkpoint located at dbfs:/mnt/demo/. The second argument True is used to recursively remove the directory along with its contents.

dbutils.fs.rm("dbfs:/mnt/demo/orders_checkpoint", True)

# COMMAND ----------

# In this line of Python code, you are utilizing the Apache Spark structured streaming feature with PySpark (Python API for Spark).

# spark: This is likely a reference to a SparkSession object, which is the entry point to programming Spark with the Dataset and DataFrame API. In the context of structured streaming, it's used to initialize and manage streaming queries.

# .streams: This attribute of the SparkSession gives you access to the StreamingQueryManager. The StreamingQueryManager is a component that allows you to manage all the streaming queries in your Spark application.

# .active: This property of the StreamingQueryManager returns a list of currently active streaming queries. A streaming query represents a continuous computation or processing on streaming data. An active streaming query is one that is running and has not been stopped.

# So, the code snippet you've provided retrieves a list of all active (currently running) streaming queries within your Spark streaming application, allowing for management or monitoring tasks related to these queries.

active_queries = spark.streams.active

# COMMAND ----------

active_queries

# COMMAND ----------

# The provided Python script is designed to work with Apache Spark, specifically targeting Spark Structured Streaming queries. The script iterates through all actively running streaming queries in a Spark session and stops each one. Here's a breakdown of the code:

# active_queries = spark.streams.active: This line fetches a list of all currently active streaming queries in the Spark session. In Spark Structured Streaming, a 'query' represents a continuous computation started using the DataStreamWriter.start() method. The spark object is typically a SparkSession instance, which is the entry point to programming Spark with the Dataset and DataFrame API. The streams attribute provides a handle on managing streaming queries, and the active attribute returns a list of currently active (running) streaming queries.

# for query in active_queries:: This starts a loop over each active streaming query that was found.

# query.stop(): Inside the loop, this line calls the stop() method on each active query object. This method stops the execution of the streaming query, effectively halting any data processing and streaming that the query was performing.

# In summary, the script is used to gracefully shut down all active Spark Structured Streaming queries in a session, typically used for cleanup or resetting the streaming environment for a fresh batch of computations. This can be particularly useful in development or testing environments, or when needing to stop all streaming jobs due to an error or to reallocate resources.

active_queries = spark.streams.active
for query in active_queries:
    query.stop()


# COMMAND ----------


