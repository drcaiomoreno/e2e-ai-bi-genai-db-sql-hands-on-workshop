# Databricks notebook source
# MAGIC %md
# MAGIC ### Airbnb Madrid Listing Analytics using Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC This notebook will:
# MAGIC 1. Create an Unity Catalog, Schema, Database and Volume;
# MAGIC 2. Download the Airbnb Dataset from the Internet; 
# MAGIC 3. Load the CSV file content to a Spark Dataframe using Spark;
# MAGIC 4. Write the Spark Dataframe to a Delta Table;
# MAGIC 5. Query the new Delta Table;
# MAGIC
# MAGIC This notebook was created by Caio Moreno (caio.moreno@databricks.com)
# MAGIC
# MAGIC Original data from:https://insideairbnb.com/get-the-data/

# COMMAND ----------

# MAGIC %md
# MAGIC **Important: Previous of running this notebook, you will have to create a Unity Catalog, Schema and a Volume**

# COMMAND ----------

# MAGIC %md
# MAGIC Lear more about Volume:<BR>
# MAGIC https://docs.databricks.com/en/connect/unity-catalog/volumes.html

# COMMAND ----------

# MAGIC %md
# MAGIC You can create the Catalog, Schema, Table and Volume using the Databricks UI or using the code below:

# COMMAND ----------

# MAGIC %md
# MAGIC **Catalogue name:** caio_moreno_airbnb_demo<BR>
# MAGIC **Schema:** airbnb<BR>
# MAGIC **Table:** airbnb_listing_detailed<BR><BR>
# MAGIC
# MAGIC **Volume path:** /Volumes/caio_moreno_airbnb_demo/airbnb/airbnb/<BR>

# COMMAND ----------

# MAGIC %md
# MAGIC **Show existing Unity Catalog Schema (need to uncomment the SQL Code)** 

# COMMAND ----------

# MAGIC %sql
# MAGIC --SHOW CATALOGS;

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Catalog**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS caio_moreno_airbnb_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Schema**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS caio_moreno_airbnb_demo.lisbon;

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Volume**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS  caio_moreno_airbnb_demo.lisbon.rawfiles;

# COMMAND ----------

# MAGIC %md
# MAGIC **Show Volume**

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES IN caio_moreno_airbnb_demo.lisbon;

# COMMAND ----------

# MAGIC %md
# MAGIC **List all files inside the Volume: /Volumes/caio_moreno_airbnb_demo/airbnb/airbnb**

# COMMAND ----------

# MAGIC %sh 
# MAGIC ls -lh /Volumes/caio_moreno_airbnb_demo/lisbon/rawfiles

# COMMAND ----------

# MAGIC %md
# MAGIC **Create a new folder called raw csv files**

# COMMAND ----------

# MAGIC %sh 
# MAGIC mkdir /Volumes/caio_moreno_airbnb_demo/lisbon/rawfiles/raw-csv-files/

# COMMAND ----------

# MAGIC %md
# MAGIC Create a folder called Listing inside the Volume

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir /Volumes/caio_moreno_airbnb_demo/lisbon/rawfiles/raw-csv-files/listing

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /Volumes/caio_moreno_airbnb_demo/lisbon/rawfiles/raw-csv-files/listing
# MAGIC wget https://data.insideairbnb.com/portugal/lisbon/lisbon/2025-03-08/data/listings.csv.gz

# COMMAND ----------

# MAGIC %sh
# MAGIC # Command to remove files 
# MAGIC #rm -rf /Volumes/cm_airbnb_mad_demo/airbnb_mad/airbnb_mad/listings.csv.gz
# MAGIC #rm -rf /Volumes/cm_airbnb_mad_demo/airbnb_mad/airbnb_mad/listings.csv.gz.1
# MAGIC #rm -rf /Volumes/cm_airbnb_mad_demo/airbnb_mad/airbnb_mad/listings.csv

# COMMAND ----------

# MAGIC %md
# MAGIC List files in the folder

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /Volumes/caio_moreno_airbnb_demo/lisbon/rawfiles/raw-csv-files/listing
# MAGIC ls -lh

# COMMAND ----------

# MAGIC %md
# MAGIC Unzip the file

# COMMAND ----------

# MAGIC %sh
# MAGIC gzip -d /Volumes/caio_moreno_airbnb_demo/lisbon/rawfiles/raw-csv-files/listing/listings.csv.gz

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /Volumes/caio_moreno_airbnb_demo/lisbon/rawfiles/raw-csv-files/listing
# MAGIC ls -lh

# COMMAND ----------

# MAGIC %sh
# MAGIC #tail -1000f /Volumes/caio_moreno_airbnb_demo/lisbon/rawfiles/raw-csv-files/listing/listings.csv

# COMMAND ----------

# MAGIC %md
# MAGIC Load Listing.csv from Databricks Unity Catalog Volume.

# COMMAND ----------

air_bnb_lisbon_df = spark.read.csv("/Volumes/caio_moreno_airbnb_demo/lisbon/rawfiles/raw-csv-files/listing/listings.csv", header=True, inferSchema=True,multiLine="true", escape='"')
display(air_bnb_lisbon_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Print Schema

# COMMAND ----------

air_bnb_lisbon_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Fix ValueError: could not convert string to float: '$67.00'

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

air_bnb_lisbon_df_curated = air_bnb_lisbon_df.withColumn("price_curated", regexp_replace(col("price"), "[^\d.]", "").cast("float"))

# COMMAND ----------

air_bnb_lisbon_df.printSchema()

# COMMAND ----------

air_bnb_lisbon_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Clean Dataset

# COMMAND ----------

from pyspark.sql.functions import col

# Remove null values in the price_curated column
air_bnb_lisbon_df_curated = air_bnb_lisbon_df_curated.filter(col("price_curated").isNotNull())

# COMMAND ----------

air_bnb_lisbon_df_curated.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Write DataFrame to a Delta Table

# COMMAND ----------

air_bnb_lisbon_df.write.format("delta").mode("overwrite").saveAsTable("caio_moreno_airbnb_demo.lisbon.airbnb_lisbon_listing_detailed");

# COMMAND ----------

air_bnb_lisbon_df_curated.write.format("delta").mode("overwrite").saveAsTable("caio_moreno_airbnb_demo.lisbon.airbnb_lisbon_listing_silver");

# COMMAND ----------

silver_lisbon_df_sql = spark.sql("SELECT * FROM caio_moreno_airbnb_demo.lisbon.airbnb_lisbon_listing_silver")
display(silver_lisbon_df_sql)

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM caio_moreno_airbnb_demo.lisbon.airbnb_lisbon_listing_silver 

# COMMAND ----------

# MAGIC %sql SELECT * FROM caio_moreno_airbnb_demo.lisbon.airbnb_lisbon_listing_silver

# COMMAND ----------

caio_moreno_airbnb_demo.lisbon.cmairbnblisbonprice

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM caio_moreno_airbnb_demo.lisbon.airbnb_lisbon_listing_detailed

# COMMAND ----------

print("Congrats!")

# COMMAND ----------

# MAGIC %md
# MAGIC You can now, create a Genie AI/BI Application to query your data using Natural Language. <BR><BR>See this demo:<BR>https://github.com/drcaiomoreno/e2e-ai-bi-genai-db-sql-hands-on-workshop

# COMMAND ----------

# MAGIC %md
# MAGIC - Show me the top super hosts by highest number of reviews
# MAGIC - How many superhosts are there in each neighborhood?
# MAGIC - I only have 120 euros, where can I stay?
# MAGIC - Show me the top 10 super hosts by number of reviews
# MAGIC - Show me all the properties where Fran Y Marta are the super host
# MAGIC - Give me some ideas of questions that I could ask based in the data?
# MAGIC - Number of listings per property type.
# MAGIC - Average review scores by room type.
# MAGIC - Listings availability trends over the next 30, 60, 90, and 365 days.
# MAGIC - Distribution of listings based on the number of bedrooms or accommodates.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
