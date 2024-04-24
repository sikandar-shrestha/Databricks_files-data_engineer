# Databricks notebook source
# MAGIC %md
# MAGIC ## Create a table

# COMMAND ----------

# Load the data from its source.
df = spark.read.load("/databricks-datasets/learning-spark-v2/people/people-10m.delta")

# Write the data to a table.
table_name = "people_10m_ss"
df.write.saveAsTable(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## describe detail

# COMMAND ----------

display(spark.sql('DESCRIBE DETAIL people_10m_ss'))

# COMMAND ----------

from delta.tables import DeltaTable

# Create table in the metastore
DeltaTable.createIfNotExists(spark) \
  .tableName("default.people10m_ss") \
  .addColumn("id", "INT") \
  .addColumn("firstName", "STRING") \
  .addColumn("middleName", "STRING") \
  .addColumn("lastName", "STRING", comment = "surname") \
  .addColumn("gender", "STRING") \
  .addColumn("birthDate", "TIMESTAMP") \
  .addColumn("ssn", "STRING") \
  .addColumn("salary", "INT") \
  .execute()

# Create or replace table with path and add properties
DeltaTable.createOrReplace(spark) \
  .addColumn("id", "INT") \
  .addColumn("firstName", "STRING") \
  .addColumn("middleName", "STRING") \
  .addColumn("lastName", "STRING", comment = "surname") \
  .addColumn("gender", "STRING") \
  .addColumn("birthDate", "TIMESTAMP") \
  .addColumn("ssn", "STRING") \
  .addColumn("salary", "INT") \
  .property("description", "table with people data") \
  .location("/tmp/delta/people10m_ss") \
  .execute()

# COMMAND ----------

people_df = spark.read.table(table_name)

display(people_df)



# COMMAND ----------

df.write.mode("append").saveAsTable("people10m_ss")

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("people10m_ss")

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, '/tmp/delta/people-10m')

# Declare the predicate by using a SQL-formatted string.
deltaTable.update(
  condition = "gender = 'F'",
  set = { "gender": "'Female'" }
)

# Declare the predicate by using Spark SQL functions.
deltaTable.update(
  condition = col('gender') == 'M',
  set = { 'gender': lit('Male') }
)

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, '/tmp/delta/people-10m')

# Declare the predicate by using a SQL-formatted string.
deltaTable.delete("birthDate < '1955-01-01'")

# Declare the predicate by using Spark SQL functions.
deltaTable.delete(col('birthDate') < '1960-01-01')

# COMMAND ----------

df2 = spark.read.format('delta').option('versionAsOf', 0).table("people_10m_ss")

display(df2)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE people_10m_ss

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE people_10m_ss
# MAGIC ZORDER BY (gender)

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM people_10m

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM people_10m_ss

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


