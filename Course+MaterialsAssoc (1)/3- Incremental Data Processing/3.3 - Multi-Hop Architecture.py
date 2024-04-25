# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# The command you provided, ../Includes/Copy-Datasets, appears to execute a script or a program named Copy-Datasets located in the directory one level up from the current directory (as indicated by ../), inside a folder named Includes.

# Breaking it down:

# ../ - This part of the path indicates that you should move up one directory level from the current working directory. For example, if you are currently in /home/user/CurrentDir and you execute this command, it will look for the Includes directory in /home/user/.

# Includes/ - This specifies the directory named Includes, which should exist in the directory reached after following the ../ part. This is where the command expects to find the Copy-Datasets script or program.

# Copy-Datasets - This is the name of the script or binary that you are intending to run. The naming suggests its purpose is to copy datasets, though without more context or seeing the contents of Copy-Datasets, it's not clear what exactly it copies, from where to where, or how it operates.

# To run such a command successfully, a few conditions must be met:

# The path ../Includes/Copy-Datasets must correctly point to a runnable file.
# The file at ../Includes/Copy-Datasets must have execute permissions. In a Unix-like environment, you might need to set this permission with a command like chmod +x ../Includes/Copy-Datasets if it's not already executable.
# If Copy-Datasets is a script, the first line (shebang) must correctly indicate the interpreter. For example, for a Bash script, it would start with #!/bin/bash.
# Without more specific details about what Copy-Datasets does or its contents, this is a general explanation of the command's structure and implication.

%run ../Includes/Copy-Datasets

# COMMAND ----------

# This snippet of Python code is executing within a context where dbutils is likely a utility object provided by Databricks, a unified data analytics platform. The code performs the following actions:

# dbutils.fs.ls(f"{dataset_bookstore}/orders-raw"): This line lists all the files and directories located at the path formed by combining the value of dataset_bookstore with the string /orders-raw. The dbutils.fs.ls function is used to interact with the Databricks file system (DBFS), allowing you to list contents of the specified directory path. The resultant list can include both files and subdirectories found at that location.

# display(files): This line is specific to environments like Databricks notebooks, where display() is a built-in function to visually render the output. Here, it is used to display the list of files and directories returned from the dbutils.fs.ls command. The output will be shown in a formatted and potentially interactive manner within the notebook interface, making it easier to explore the listed items.

# The purpose of this code is to query and display the contents of a specific directory (orders-raw) in the dataset stored in DBFS, helping users to quickly ascertain the files or subdirectories present at that location.

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

# The code reads a stream of data from a cloud storage system in the Parquet format. It specifies the location of the schema file and loads the data from a directory with the name specified in the dataset_bookstore variable, followed by "/orders-raw". The loaded data is then assigned the temporary view name "orders_raw_temp".


(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/checkpoints/orders_raw")
    .load(f"{dataset_bookstore}/orders-raw")
    .createOrReplaceTempView("orders_raw_temp"))

# COMMAND ----------

#The code is a list comprehension that iterates over the attributes of the spark.readStream object. It filters out the attributes that start with an underscore and returns a list of the non-private attributes of the readStream object.

[i for i in dir(spark.readStream) if not str(i).startswith('_')]

# COMMAND ----------

# The SQL code snippet you've provided creates or replaces a temporary view named orders_tmp. This temporary view is based on querying the table orders_raw_temp and adding two additional columns to its existing structure. Here's a concise breakdown of its components:

# CREATE OR REPLACE TEMPORARY VIEW orders_tmp: This statement initiates the creation (or replacement if it already exists) of a temporary view named orders_tmp. A temporary view is a virtual table that is not physically stored in the database but can be used in queries during the session in which it was created.

# SELECT *, current_timestamp() arrival_time, input_file_name() source_file: This part of the statement selects all columns from the orders_raw_temp table (denoted by *) and adds two more columns:

# current_timestamp() arrival_time: A column named arrival_time that captures the current timestamp each time the query is executed. This represents the time when the data is being queried or processed.
# input_file_name() source_file: A column named source_file that stores the file name where each row of data originated, obtained by the function input_file_name(). This is particularly useful in scenarios where data is being loaded from multiple files, and tracking the source of each row is required.
# FROM orders_raw_temp: Specifies the orders_raw_temp table as the source from which to select data and add the two new columns.

# In summary, the code creates a temporary view that augments the data in orders_raw_temp by capturing the current timestamp and the source file name for each record. This enhanced view can be very useful for data processing tasks that require temporal information and data lineage tracking.

%sql
CREATE OR REPLACE TEMPORARY VIEW orders_tmp AS (
  SELECT *, current_timestamp() arrival_time, input_file_name() source_file
  FROM orders_raw_temp
)

# COMMAND ----------

#This code is selecting all columns (*) from the table called "orders_tmp". It is retrieving all the data from the "orders_tmp" table.


%sql
SELECT * FROM orders_tmp

# COMMAND ----------

# This code uses Apache Spark to read a temporary table called "orders_tmp" and writes it as a Delta Lake stream. It sets the output mode to append, meaning that any new data flowing into the stream will be added to the existing data. It also specifies a checkpoint location where Spark will store its metadata for fault tolerance. Finally, it declares a destination table name as "orders_bronze" where the data will be written.

(spark.table("orders_tmp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_bronze")
      .outputMode("append")
      .table("orders_bronze"))

# COMMAND ----------

#This SQL code performs a simple operation on a database. It counts the total number of rows in the table named orders_bronze. The SELECT count(*) statement is used to find the total number of records in this specific table. When executed, this query will return a single number representing the total row count in the orders_bronze table.

%sql
SELECT count(*) FROM orders_bronze

# COMMAND ----------

load_new_data()

# COMMAND ----------

# The code loads a JSON file from a bookstore dataset into a Spark dataframe. It then registers the dataframe as a temporary view with the name "customers_lookup".


(spark.read
      .format("json")
      .load(f"{dataset_bookstore}/customers-json")
      .createOrReplaceTempView("customers_lookup"))

# COMMAND ----------

#This SQL code retrieves all rows and columns from a database table named customers_lookup. The SELECT * syntax is used to select all columns without specifying them individually. This query will return the complete dataset contained within the customers_lookup table, including all customer records and details present in each column of the table.

%sql
SELECT * FROM customers_lookup

# COMMAND ----------

# The code is reading a streaming table called "orders_bronze" and creating a temporary view called "orders_bronze_tmp" for further processing.

(spark.readStream
  .table("orders_bronze")
  .createOrReplaceTempView("orders_bronze_tmp"))

# COMMAND ----------

# This SQL code snippet creates or replaces a temporary view named orders_enriched_tmp. Here's a concise breakdown of what the code does:

# Create or Replace: The CREATE OR REPLACE TEMPORARY VIEW clause means that if a temporary view with the same name (orders_enriched_tmp) already exists, it will be replaced with this new definition. If it doesn't exist, a new temporary view will be created.

# Selecting and Joining Data:

# The SELECT statement lists the columns to be included in the temporary view, which are order_id, quantity, a customer ID (o.customer_id), customer's first name (c.profile:first_name as f_name), customer's last name (c.profile:last_name as l_name), a timestamp conversion of order_timestamp, and books.
# It's important to note the syntax used for accessing nested fields in the c.profile column, indicating c (which is an alias for the customers_lookup table) contains a complex data structure (profile) from which first_name and last_name are being accessed and then renamed as f_name and l_name, respectively.
# The cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) function converts order_timestamp from a Unix timestamp into a more human-readable timestamp format.
# FROM and JOIN Clause:

# The data is being fetched from the orders_bronze_tmp o table/view and is being joined with the customers_lookup c table/view using an INNER JOIN.
# The join condition is o.customer_id = c.customer_id, ensuring data is merged based on matching customer IDs between the orders_bronze_tmp and customers_lookup tables/views.
# Filtering: The query includes a WHERE clause that filters the rows to only include those where the quantity is greater than 0.

# Usage: The purpose of creating this temporary view is to enrich order data (from orders_bronze_tmp) by combining it with corresponding customer data (from customers_lookup), filtering by quantity, and presenting it in an accessible format. This enriched view could then be used for various analyses or reporting tasks that require information about orders, their timestamps, quantities, and associated customer details within the scope of the session or transaction it was created in.




%sql
CREATE OR REPLACE TEMPORARY VIEW orders_enriched_tmp AS (
  SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
         cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, books
  FROM orders_bronze_tmp o
  INNER JOIN customers_lookup c
  ON o.customer_id = c.customer_id
  WHERE quantity > 0)

# COMMAND ----------

# This code snippet is using Apache Spark's structured streaming to write a dataframe to a Delta table.

# The code starts by reading a Delta table called "orders_enriched_tmp" using spark.table().

# Then it uses the writeStream method to specify that the data should be written as a streaming output.

# The format("delta") method sets the output format to Delta format.

# The option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_silver") method specifies the location where the checkpoints for the streaming job will be stored. In this case, it is set to the "orders_silver" folder in the DBFS (Databricks File System).

# The outputMode("append") method sets the mode for writing the streaming output. In this case, it appends new records to the existing Delta table.

# Finally, the table("orders_silver") method specifies the name of the Delta table where the streaming data will be written to.



(spark.table("orders_enriched_tmp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_silver")
      .outputMode("append")
      .table("orders_silver"))

# COMMAND ----------

#The SQL code provided is a straightforward query to select and return all columns and rows from a database table named orders_silver. This code will fetch the complete dataset contained in orders_silver without any filtration or sorting.

%sql
SELECT * FROM orders_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM orders_silver

# COMMAND ----------

load_new_data()

# COMMAND ----------

# This code reads streaming data from a table called "orders_silver", then creates a temporary view called "orders_silver_tmp" using that streaming data.

(spark.readStream
  .table("orders_silver")
  .createOrReplaceTempView("orders_silver_tmp"))

# COMMAND ----------

# The code creates a temporary view called "daily_customer_books_tmp". The view selects the customer_id, first name (f_name), last name (l_name), order date (truncated to day precision), and the sum of the quantity of books ordered for each customer on each day. The data is obtained from a table called "orders_silver_tmp". The results are grouped by customer_id, f_name, l_name, and order date.

%sql
CREATE OR REPLACE TEMP VIEW daily_customer_books_tmp AS (
  SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
  FROM orders_silver_tmp
  GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
  )

# COMMAND ----------

# The provided Python code snippet is designed to work with Apache Spark, specifically using the PySpark API. It demonstrates how to set up a streaming write from a table called "daily_customer_books_tmp" to a Delta Lake table named "daily_customer_books". Here's the breakdown of what each part of the code does:

# spark.table("daily_customer_books_tmp"): This line loads the table named "daily_customer_books_tmp" into a DataFrame. It uses the SparkSession (spark) to read data from a table.

# writeStream: This method is called on the DataFrame obtained from the previous step, signifying that the operation will be a streaming write as opposed to a batch write. Streaming writes handle data continuously as it comes in, rather than in large, single batches.

# .format("delta"): Specifies the format of the output data source. In this case, it's Delta Lake, a storage layer that brings ACID transactions to Apache Spark and big data workloads.

# .outputMode("complete"): Sets the output mode for the streaming query. The "complete" mode means that the entire updated Result Table will be written to the external storage. This is often used for aggregations where the result table is expected to have aggregated summaries and needs to be written entirely.

# .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/daily_customer_books"): Checkpointing is a critical feature for fault tolerance in Spark Streaming. The "checkpointLocation" specifies where Spark will save checkpoint data, which includes information necessary to resume the stream from where it left off in case of a failure. Here, the checkpoint data is stored in DBFS (Databricks File System) at the specified path.

# .trigger(availableNow=True): Determines when the OutputSink should trigger the writing of the data. Setting availableNow=True triggers a one-time micro-batch to process all available data and then stop.

# .table("daily_customer_books"): Specifies the output table where the stream should write its results. This is the Delta table into which the transformed stream from "daily_customer_books_tmp" will be written.

# In summary, this script reads data from the table "daily_customer_books_tmp", processes it as a continuous stream, and writes the resulting data into a Delta Lake table named "daily_customer_books" in a complete output mode, with checkpointing enabled for fault tolerance, and triggers a one-time batch to process all available data immediately.





(spark.table("daily_customer_books_tmp")
      .writeStream
      .format("delta")
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/daily_customer_books")
      .trigger(availableNow=True)
      .table("daily_customer_books"))

# COMMAND ----------

#The SQL code you provided is a query that retrieves all the columns and rows from a database table named daily_customer_books. This will return the entire contents of that table without applying any filters or conditions.

%sql
SELECT * FROM daily_customer_books

# COMMAND ----------

load_new_data()

# COMMAND ----------

# Your code snippet is designed to work within a context where Spark Structured Streaming is being used. Apache Spark is a powerful open-source engine designed for big data processing and analytics, and Structured Streaming is an API built on top of Spark SQL that enables scalable and fault-tolerant stream processing of live data streams. Here's a step-by-step explanation of what your code does:

# Loop through active streams:

# for s in spark.streams.active: This line loops over all the active streaming queries in your Spark session. The spark object presumably refers to an instance of a Spark session, and spark.streams.active provides a list of all active streams within that session. Each item s in this list represents an active streaming query.
# Print the stream ID:

# print("Stopping stream: " + s.id) For each active streaming query s, this line prints a message to the standard output indicating that the stream is going to be stopped. s.id is a unique identifier for the streaming query, helping to distinguish each stream.
# Stop the stream:

# s.stop() This line stops the active streaming query s. Stopping a stream means that Spark will cease to process new data for this stream, and it will be marked as inactive.
# Wait for the stream to terminate:

# s.awaitTermination() After instructing the stream to stop, this line ensures that the code waits for the stream to fully terminate before moving on. This is important to make sure that all resources are properly released and that the stream has completely stopped processing any remaining data.
# Overall, the purpose of your code is to iterate over all currently active Spark Structured Streaming queries, stop each one gracefully, and ensure it's fully terminated before proceeding. This might be particularly useful in scenarios where you need to programmatically halt streaming processes, perhaps for maintenance, upgrading, or shutting down resources in an organized manner.




for s in spark.streams.active:
    print("Stopping stream: " + s.id)
    s.stop()
    s.awaitTermination()

# COMMAND ----------


