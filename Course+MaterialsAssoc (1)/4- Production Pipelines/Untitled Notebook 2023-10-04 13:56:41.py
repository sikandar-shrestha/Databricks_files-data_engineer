# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC SELECT collect_set(col) FROM VALUES (1), (2), (NULL), (1) AS tab(col);
# MAGIC       

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT collect_set(col1) FILTER(WHERE col2 = 10)
# MAGIC     FROM VALUES (1, 10), (2, 10), (NULL, 10), (1, 10), (3, 12) AS tab(col1, col2);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT flatten(array(array(1, 2), array(3, 4)));

# COMMAND ----------


