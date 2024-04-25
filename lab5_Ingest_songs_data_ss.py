# Databricks notebook source
# MAGIC %md
# MAGIC ## 5.1 Ingestion

# COMMAND ----------

# This Python script uses Apache Spark to read and process streaming data from files in CSV format, and then write the processed data to a table. Here's a concise explanation of what each part of the code does:

# Importing Required Libraries:

# The script starts by importing necessary data types (DoubleType, IntegerType,StringType, StructType, StructField) from the pyspark.sql.types module, which are used to define the schema of the data.
# Defining Variables:

# file_path: Specifies the path to the dataset.
# table_name: The name of the table where the processed data will be saved.
# checkpoint_path: Path for saving checkpoint data to manage streaming state.
# Defining the Schema:

# A StructType schema is defined, detailing the structure of the data to be processed. This includes various fields like artist_id, artist_lat, artist_long, duration, etc., with corresponding data types (StringType, DoubleType, IntegerType).
# Reading Streaming Data:

# The script reads streaming data in CSV format from the specified file path (file_path), using the defined schema. It specifies the format of source files as "cloudFiles", and the delimiter in the CSV files as a tab character (\t).
# Writing the Stream to a Table:

# Finally, the script writes the streamed data into a table named table_name with a checkpointing mechanism. Checkpointing (saved in checkpoint_path) helps in managing and maintaining the state of the stream, allowing for fault tolerance and the ability to restart the stream from the last processed point in case of a failure.
# It uses a trigger setting (once=True) to indicate that this batch job should execute once and then stop.
# In summary, the script is set up to perform a one-time streaming read from a specified file path using Spark, with a predefined schema, and then write the output to a table, while maintaining fault tolerance through checkpointing.



from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType, StructField
 
# Define variables used in the code below
file_path = "/databricks-datasets/songs/data-001/"
table_name = "raw_song_data"
checkpoint_path = "/tmp/pipeline_get_started/_checkpoint/song_data"
 
schema = StructType(
  [
    StructField("artist_id", StringType(), True),
    StructField("artist_lat", DoubleType(), True),
    StructField("artist_long", DoubleType(), True),
    StructField("artist_location", StringType(), True),
    StructField("artist_name", StringType(), True),
    StructField("duration", DoubleType(), True),
    StructField("end_of_fade_in", DoubleType(), True),
    StructField("key", IntegerType(), True),
    StructField("key_confidence", DoubleType(), True),
    StructField("loudness", DoubleType(), True),
    StructField("release", StringType(), True),
    StructField("song_hotnes", DoubleType(), True),
    StructField("song_id", StringType(), True),
    StructField("start_of_fade_out", DoubleType(), True),
    StructField("tempo", DoubleType(), True),
    StructField("time_signature", DoubleType(), True),
    StructField("time_signature_confidence", DoubleType(), True),
    StructField("title", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("partial_sequence", IntegerType(), True)
  ]
)
 
(spark.readStream
  .format("cloudFiles")
  .schema(schema)
  .option("cloudFiles.format", "csv")
  .option("sep","\t")
  .load(file_path)
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(once=True)
  .toTable(table_name)
)

# COMMAND ----------

# This code is using PySpark to display the contents of a table named table_name. The spark.table() function is used to access the table, and the display() function is used to show the table's contents.


display(spark.table(table_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Preparation

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE
# MAGIC   prepared_song_data (
# MAGIC     artist_id STRING,
# MAGIC     artist_name STRING,
# MAGIC     duration DOUBLE,
# MAGIC     release STRING,
# MAGIC     tempo DOUBLE,
# MAGIC     time_signature DOUBLE,
# MAGIC     title STRING,
# MAGIC     year DOUBLE,
# MAGIC     processed_time TIMESTAMP
# MAGIC   );
# MAGIC  
# MAGIC INSERT INTO
# MAGIC   prepared_song_data
# MAGIC SELECT
# MAGIC   artist_id,
# MAGIC   artist_name,
# MAGIC   duration,
# MAGIC   release,
# MAGIC   tempo,
# MAGIC   time_signature,
# MAGIC   title,
# MAGIC   year,
# MAGIC   current_timestamp()
# MAGIC FROM
# MAGIC   raw_song_data
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from prepared_song_data;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 Query

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --Question 1 :-- Which artists released the most songs each year?
# MAGIC SELECT
# MAGIC   artist_name,
# MAGIC   count(artist_name)
# MAGIC AS
# MAGIC   num_songs,
# MAGIC   year
# MAGIC FROM
# MAGIC   prepared_song_data
# MAGIC WHERE
# MAGIC   year > 0
# MAGIC GROUP BY
# MAGIC   artist_name,
# MAGIC   year
# MAGIC ORDER BY
# MAGIC   num_songs DESC,
# MAGIC   year DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Question 2:-- Find songs for your DJ list.
# MAGIC  SELECT
# MAGIC    artist_name,
# MAGIC    title,
# MAGIC    tempo
# MAGIC  FROM
# MAGIC    prepared_song_data
# MAGIC  WHERE
# MAGIC    time_signature = 4
# MAGIC    AND
# MAGIC    tempo between 100 and 140;
