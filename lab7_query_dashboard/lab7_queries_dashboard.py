# Databricks notebook source
# MAGIC %md 
# MAGIC ## Query for taxi pickups by hours

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC date_format(tpep_pickup_datetime, "HH") AS `Pickup Hour`,
# MAGIC count(*) AS `Number of Rides`
# MAGIC FROM
# MAGIC samples.nyctaxi.trips
# MAGIC GROUP BY 1
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query for daily fare trends

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   T.weekday,
# MAGIC   CASE
# MAGIC     WHEN T.weekday = 1 THEN 'Sunday'
# MAGIC     WHEN T.weekday = 2 THEN 'Monday'
# MAGIC     WHEN T.weekday = 3 THEN 'Tuesday'
# MAGIC     WHEN T.weekday = 4 THEN 'Wednesday'
# MAGIC     WHEN T.weekday = 5 THEN 'Thursday'
# MAGIC     WHEN T.weekday = 6 THEN 'Friday'
# MAGIC     WHEN T.weekday = 7 THEN 'Saturday'
# MAGIC     ELSE 'N/A'
# MAGIC   END AS day_of_week,
# MAGIC   T.fare_amount,
# MAGIC   T.trip_distance
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       dayofweek(tpep_pickup_datetime) as weekday,
# MAGIC       *
# MAGIC     FROM
# MAGIC       `samples`.`nyctaxi`.`trips`
# MAGIC   ) T
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Add a pickup zip code parameter to Query for taxi pickups by hours

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC date_format(tpep_pickup_datetime, "HH") AS `Pickup Hour`,
# MAGIC count(*) AS `Number of Rides`
# MAGIC FROM
# MAGIC samples.nyctaxi.trips
# MAGIC WHERE
# MAGIC   pickup_zip IN ({{ pickupzip }})
# MAGIC GROUP BY 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   T.weekday,
# MAGIC   CASE
# MAGIC     WHEN T.weekday = 1 THEN 'Sunday'
# MAGIC     WHEN T.weekday = 2 THEN 'Monday'
# MAGIC     WHEN T.weekday = 3 THEN 'Tuesday'
# MAGIC     WHEN T.weekday = 4 THEN 'Wednesday'
# MAGIC     WHEN T.weekday = 5 THEN 'Thursday'
# MAGIC     WHEN T.weekday = 6 THEN 'Friday'
# MAGIC     WHEN T.weekday = 7 THEN 'Saturday'
# MAGIC     ELSE 'N/A'
# MAGIC   END AS day_of_week,
# MAGIC   T.fare_amount,
# MAGIC   T.trip_distance
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       dayofweek(tpep_pickup_datetime) as weekday,
# MAGIC       *
# MAGIC     FROM
# MAGIC       `samples`.`nyctaxi`.`trips`
# MAGIC   ) T
# MAGIC   WHERE
# MAGIC      pickup_zip IN ({{ pickupzip }})

# COMMAND ----------


