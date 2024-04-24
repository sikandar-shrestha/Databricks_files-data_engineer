# Databricks notebook source
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
# MAGIC
# MAGIC -- Which artists released the most songs each year?
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
# MAGIC -- Find songs for your DJ list
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

# COMMAND ----------



# COMMAND ----------


