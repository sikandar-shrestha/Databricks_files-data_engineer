# Databricks notebook source
# The provided code is likely used to copy datasets from one location to another. It includes a script or function named "Copy-Datasets" which is being imported or included from a separate file or directory.

# The purpose of this code is to facilitate the copying of datasets from one location to another, which could be useful for tasks such as backing up data or transferring datasets between different environments.


%run ../Includes/Copy-Datasets

# COMMAND ----------

# The code is calling a function called load_new_json_data(). This function is likely responsible for loading new data from a JSON file or API into the program for further processing or analysis.


load_new_json_data()
