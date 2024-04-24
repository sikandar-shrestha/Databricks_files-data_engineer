# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

(spark.readStream
      .table("books") 
      .createOrReplaceTempView("books_streaming_tmp_vw")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books_streaming_tmp_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT author, count(book_id) AS total_books
# MAGIC FROM books_streaming_tmp_vw
# MAGIC GROUP BY author

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sorting is not supported with streaming
# MAGIC  SELECT * 
# MAGIC  FROM books_streaming_tmp_vw
# MAGIC  ORDER BY author

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Streaming TEMP view
# MAGIC CREATE OR REPLACE TEMP VIEW author_counts_tmp_vw AS (
# MAGIC   SELECT author, count(book_id) AS total_books
# MAGIC   FROM books_streaming_tmp_vw
# MAGIC   GROUP BY author
# MAGIC )

# COMMAND ----------

display(streamingDF)

# COMMAND ----------

# dbutils.fs.rm("dbfs:/mnt/demo/author_counts_checkpoint", True) 

# COMMAND ----------

# Notice the attributes
(spark.table("author_counts_tmp_vw")                               
      .writeStream  
      .trigger(processingTime='4 seconds')
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
      .table("author_counts")
)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from author_counts
# MAGIC order by 2 desc

# COMMAND ----------

books_df = spark.sql("""
                     select book_id
                            ,title
                            ,author
                            ,category
                            ,price
                        from(       
                        select book_id
                            ,title
                            ,author
                            ,category
                            ,price
                            , row_number()over( partition by book_id order by book_id) xx_check
                        from books )
                        where xx_check = 1
                        and book_id < 'B19'""")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values ("B19", "Introduction to Modeling and Simulation", "Mark W. Spong", "Computer Science", 25),
# MAGIC         ("B20", "Robot Modeling and Control", "Mark W. Spong", "Computer Science", 30),
# MAGIC         ("B21", "Turing's Vision: The Birth of Computer Science", "Chris Bernhardt", "Computer Science", 35)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values ("B16", "Hands-On Deep Learning Algorithms with Python", "Sudharsan Ravichandiran", "Computer Science", 25),
# MAGIC         ("B17", "Neural Network Methods in Natural Language Processing", "Yoav Goldberg", "Computer Science", 30),
# MAGIC         ("B18", "Understanding digital signal processing", "Richard Lyons", "Computer Science", 35)

# COMMAND ----------

(spark.table("author_counts_tmp_vw")                               
      .writeStream           
      .trigger(availableNow=True)
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
      .table("author_counts")
      .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM author_counts
# MAGIC order by 2 desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from books

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from books
# MAGIC where book_id > 'B15'
# MAGIC  order by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from books
# MAGIC where book_id > 'B15'
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop view books_streaming_tmp_vw
# MAGIC select * from  books_streaming_tmp_vw

# COMMAND ----------


