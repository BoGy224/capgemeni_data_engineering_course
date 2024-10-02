# -*- coding: utf-8 -*-
"""
Created on Mon Sep 30 11:48:07 2024

@author: Влад
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("restoreSilverTable").getOrCreate()

silver_table_path = 's3://<backet>/delta/nyc_airbnb/silver'
version = 5

# Query a specific version of the table
df_previous = spark.read.format("delta").option("versionAsOf", version).load(silver_table_path)

# Restore the previous version if necessary
df_previous.write.format("delta").mode("overwrite").save(silver_table_path)