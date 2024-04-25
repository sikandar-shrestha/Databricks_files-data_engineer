# Databricks notebook source


# This SQL code is querying data using a special function collect_set to gather unique, non-null elements from a specified column into a collection, specifically a set. Understanding the components:

# SELECT collect_set(col) tells the database to apply the function collect_set on the column named col. The collect_set function takes all unique values from col, excluding duplicates and null values, and aggregates them into a set.

# FROM VALUES (1), (2), (NULL), (1) AS tab(col) is constructing an inline table on-the-fly with a single column col. This part uses the VALUES clause to create a table named tab with the specified values in col. The values provided in this example are 1, 2, a NULL, and 1 again.

# This code produces a collection (or set) containing unique, non-null values from the given list, which in this case would be {1, 2}:

# The 1 and 2 are unique and non-null, so they make it into the set.
# The NULL is excluded because collect_set ignores null values.
# The second instance of 1 does not contribute to the set because it's a duplicate and collect_set only collects unique values.
# Therefore, the result of this query would typically be represented as a collection or array with elements [1, 2], depending on the specific SQL database's way of displaying sets or arrays. Different SQL databases might have varied syntax or functions for achieving this, and the exact behavior can depend on the particular SQL dialect being used (e.g., PostgreSQL, MySQL, Hive, etc.).



%sql

SELECT collect_set(col) FROM VALUES (1), (2), (NULL), (1) AS tab(col);
      

# COMMAND ----------



# This SQL query selects a distinct set of values from the column "col1" where the condition on "col2" being equal to 10 is satisfied. It uses collect_set in combination with a FILTER clause to achieve this outcome. The query is executed on a sequence of defined values for demonstration purposes.

# Here's a step-by-step explanation:

# FROM Clause with VALUES: The query creates a temporary table (tab) using the VALUES keyword with rows defined as (1, 10), (2, 10), (NULL, 10), (1, 10), (3, 12). Each row includes two values corresponding to col1 and col2. So, you have a table structure where col1 contains (1, 2, NULL, 1, 3) and col2 contains (10, 10, 10, 10, 12).

# SELECT with collect_set and FILTER: The SELECT statement is employing the collect_set function which aggregates values from col1 into a set, meaning it collects unique, non-null values. This is paired with a FILTER clause specifying the condition that this collection should only include rows where col2 = 10.

# Result: Given the condition and the original data, the query filters out the last row where col2 is 12, and then it collects unique, non-null values of col1 where col2 is 10. Since collect_set only includes unique values and ignores NULL, and considering duplicates are also ignored, the final result is a set of unique non-null values from col1 where col2 is 10: {1, 2}.

# Remember that the behavior of collecting sets and how NULL values are handled might slightly vary by SQL dialects, but the general principle remains consistent. In this case, NULL values are ignored, and duplicates are not included in the resulting set.



%sql

SELECT collect_set(col1) FILTER(WHERE col2 = 10)
    FROM VALUES (1, 10), (2, 10), (NULL, 10), (1, 10), (3, 12) AS tab(col1, col2);

# COMMAND ----------


# The SQL code selects the function flatten, which is used to transform a nested array into a single-level array. In this code, it takes an input array that contains two inner arrays: [1, 2] and [3, 4]. The function combines these inner arrays into one result: [1, 2, 3, 4]. The result of the flatten function is then returned as the output of the query.


%sql
SELECT flatten(array(array(1, 2), array(3, 4)));

# COMMAND ----------


