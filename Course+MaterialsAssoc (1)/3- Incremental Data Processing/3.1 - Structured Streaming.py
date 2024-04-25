# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# The code reads the data from the "books" table using Spark's readStream() function.
# It then creates a temporary view named "books_streaming_tmp_vw" using the createOrReplaceTempView() function.
# This temporary view allows us to perform SQL queries on the streaming data.
# The code will wait for new records to stream in and keep the view updated with the latest data.
# If any new records arrive, they will be added to the view and can be queried using SQL.



(spark.readStream
      .table("books") 
      .createOrReplaceTempView("books_streaming_tmp_vw")
)

# COMMAND ----------

# taking data from the books table and reading it into temporary view. 
# this needs to be interupted, it will keep streaming, its waiting for a new record


%sql
SELECT * FROM books_streaming_tmp_vw

# COMMAND ----------

#The code is selecting the "author" column and counting the number of occurrences of each "book_id" in the "books_streaming_tmp_vw" table. It then groups the results by the "author" column. This code allows us to determine the total number of books written by each author.

%sql
SELECT author, count(book_id) AS total_books
FROM books_streaming_tmp_vw
GROUP BY author

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sorting is not supported with streaming
# MAGIC  SELECT * 
# MAGIC  FROM books_streaming_tmp_vw
# MAGIC  ORDER BY author

# COMMAND ----------

#This code creates a temporary view called "author_counts_tmp_vw" that calculates the total number of books written by each author. It achieves this by selecting the "author" column and counting the number of distinct "book_id" values for each author from another temporary view called "books_streaming_tmp_vw". The result is grouped by the "author" column.

%sql
-- Streaming TEMP view
CREATE OR REPLACE TEMP VIEW author_counts_tmp_vw AS (
  SELECT author, count(book_id) AS total_books
  FROM books_streaming_tmp_vw
  GROUP BY author
)

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

# The SQL code is selecting all columns from the table "author_counts" and ordering the result set by the second column (in descending order).


%sql
select * 
from author_counts
order by 2 desc

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

#The .writeStream method starts the streaming process.

#The .trigger(availableNow=True) sets the trigger mode to "available now", meaning the query will be triggered as soon as possible for available data.

#The .outputMode("complete") specifies that the entire updated result table should be written to the output sink.

#The .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint") sets the checkpoint location to "dbfs:/mnt/demo/author_counts_checkpoint" for fault-tolerance and data recovery.

#The .table("author_counts") is the output sink where the final result will be written.

#Finally, .awaitTermination() waits for the streaming query to terminate before exiting the program.

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

# The SQL code retrieves all rows from the "books" table where the "book_id" column is greater than 'B15'. It then orders the results in ascending order based on the first column.

%sql
select * 
from books
where book_id > 'B15'
 order by 1

# COMMAND ----------

# The code is deleting rows from the "books" table where the book_id is greater than 'B15'. This will result in the removal of any books with an ID higher than 'B15' from the table.

%sql
delete from books
where book_id > 'B15'
 

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop view books_streaming_tmp_vw
# MAGIC select * from  books_streaming_tmp_vw

# COMMAND ----------


