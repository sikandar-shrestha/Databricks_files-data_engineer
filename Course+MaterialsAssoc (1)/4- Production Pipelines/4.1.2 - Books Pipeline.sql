-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Bronze Layer Tables

-- COMMAND ----------

-- This SQL command is creating or refreshing a streaming live table named books_bronze. Here's a concise summary of its components:

-- CREATE OR REFRESH STREAMING LIVE TABLE books_bronze: This clause creates a new streaming live table named books_bronze if it doesn't already exist, or refreshes it if it does. Streaming live tables are designed for real-time data processing, enabling continuous updates and transformations as new data arrives.

-- COMMENT "The raw books data, ingested from CDC feed": This part adds a description to the table, indicating that it contains raw books data ingested from a Change Data Capture (CDC) feed. Comments are useful for documentation and maintaining clarity about the table's purpose.

-- The as select * from code line value suggests a parameterized path to the directory containing the data files, allowing for flexibility and potentially different environments or datasets.

-- Overall, this command sets up a foundational streaming live table receiving real-time JSON data from a specified cloud storage path, aimed at processing raw books data ingested from a CDC feed.





CREATE OR REFRESH STREAMING LIVE TABLE books_bronze
COMMENT "The raw books data, ingested from CDC feed"
AS SELECT * FROM cloud_files("${datasets_path}/books-cdc", "json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Silver Layer Tables

-- COMMAND ----------

-- The code creates a streaming live table called "books_silver". It then applies changes to this table from a streaming source table called "books_bronze". The table keys are defined as "book_id".

-- The APPLY clause is used to process the changes from the source table into the destination table. In this case, it includes a DELETE condition where rows with "row_status" column value equal to "DELETE" will be deleted from the target table.

-- The rows in the source table are ordered by "row_time" column using the SEQUENCE BY clause.

-- Finally, all columns from the source table except "row_status" and "row_time" are selected and inserted into the target table.


CREATE OR REFRESH STREAMING LIVE TABLE books_silver;

APPLY CHANGES INTO LIVE.books_silver
  FROM STREAM(LIVE.books_bronze)
  KEYS (book_id)
  APPLY AS DELETE WHEN row_status = "DELETE"
  SEQUENCE BY row_time
  COLUMNS * EXCEPT (row_status, row_time)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Gold Layer Tables

-- COMMAND ----------

-- This SQL code is designed to create a live table named author_counts_state. This table is intended to provide a dynamic view of the number of books written by each author, alongside the timestamp indicating when the information was last updated. Here's a breakdown of its components:

-- CREATE LIVE TABLE author_counts_state: This command creates a new live table named author_counts_state. Live tables are a feature in some modern database systems (such as Delta Live Tables in Databricks) that enable real-time data processing and updating. This contrasts with traditional static tables, where updates require manual intervention or batch processing.

-- COMMENT "Number of books per author": This is a descriptive comment for the table, indicating its purpose or content. It doesn't affect the functionality but makes the database more understandable and maintainable.

-- AS SELECT author, count(*) as books_count, current_timestamp() updated_time: This part of the statement defines the structure and content of the author_counts_state table by executing a SELECT query to fill it. Specifically:

-- author: This column will list the authors.
-- count(*) as books_count: This operation counts the number of rows (i.e., books) for each author, labeling the result as books_count. It effectively shows how many books each author has written.
-- current_timestamp() updated_time: This records the timestamp at which each row was last updated, providing a reference for when the data was last refreshed.
-- FROM LIVE.books_silver: This specifies the source data for the query, which is another live table named books_silver. This table presumably contains detailed records of books, including their authors.

-- GROUP BY author: This part of the statement aggregates the data from books_silver by the author column, ensuring that the count is calculated separately for each author.

-- In summary, this SQL code establishes a live table that keeps an up-to-date count of books written by each author, along with a timestamp of the last update. This live table can be particularly useful for tracking author productivity or catalog size in real-time within a dynamic database or data lakehouse environment.



CREATE LIVE TABLE author_counts_state
  COMMENT "Number of books per author"
AS SELECT author, count(*) as books_count, current_timestamp() updated_time
  FROM LIVE.books_silver
  GROUP BY author

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DLT Views

-- COMMAND ----------

-- This SQL code snippet is creating a Live View named books_sales. A Live View is a virtual table that provides real-time results of a SELECT query. The data reflects changes in the underlying tables immediately, making it highly useful for streaming or live datasets.

-- Here's a breakdown of the code:

-- Live View Creation: CREATE LIVE VIEW books_sales initiates the creation of a live view named books_sales. This live view will update in real-time as the underlying data changes.

-- Selection of Data:

-- The SELECT b.title, o.quantity part specifies the columns to be included in the live view. It selects the title of books from the LIVE.books_silver table and the quantity of orders from a derived table o.
-- FROM and Inner Subquery:

-- The FROM clause is specifying the source of the data. It includes a subquery that selects all columns from LIVE.orders_cleaned and uses the explode function on the books column to transform each item in the books array into a separate row, referred to as book.
-- Inner Join:

-- The INNER JOIN part combines rows from two datasets based on a related column between them. In this case, it's joining the derived table containing orders (aliased as o) with the LIVE.books_silver table on the condition that the book_id from both tables matches (o.book.book_id = b.book_id). This ensures that only orders containing books listed in the LIVE.books_silver table are included.
-- Purpose:

-- The main purpose of this code is to create a real-time view that showcases the sales data for books. It ties together the quantity of each book sold (from orders) with the book's title (from a master list of books), presenting this data in a live, up-to-date fashion.
-- By leveraging the capabilities of live views and the power of SQL operations like inner joins and the explode function, this code efficiently assembles a dynamic and continuously updated dataset showcasing book sales.



CREATE LIVE VIEW books_sales
  AS SELECT b.title, o.quantity
    FROM (
      SELECT *, explode(books) AS book 
      FROM LIVE.orders_cleaned) o
    INNER JOIN LIVE.books_silver b
    ON o.book.book_id = b.book_id;
