# Databricks notebook source
# MAGIC %md
# MAGIC ### Airbnb Madrid Listing Analytics using Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC This notebook will:
# MAGIC 1. Download the Dataset from the Internet 
# MAGIC 2. Load the CSV file content to a Spark Dataframe using Spark
# MAGIC 3. Write the Spark Dataframe to a Delta Table
# MAGIC 4. Query the new Delta Table
# MAGIC
# MAGIC This notebook was created by Caio Moreno (caio.moreno@databricks.com)
# MAGIC
# MAGIC Original data from:https://insideairbnb.com/get-the-data/

# COMMAND ----------

# MAGIC %md
# MAGIC **Important: Previous of running this notebook, you will have to create a Unity Catalog, Schema and a Volume**

# COMMAND ----------

# MAGIC %md
# MAGIC Volume:<BR>
# MAGIC https://docs.databricks.com/en/connect/unity-catalog/volumes.html

# COMMAND ----------

# MAGIC %md
# MAGIC **Catalogue name:** cm_airbnb_mad_demo<BR>
# MAGIC **Schema:** airbnb_mad<BR>
# MAGIC **Table:** airbnb_mad_listing_detailed<BR><BR>
# MAGIC
# MAGIC **Volume path:** /Volumes/cm_airbnb_mad_demo/airbnb_mad/airbnb_mad/<BR>

# COMMAND ----------

# MAGIC %md
# MAGIC List all files inside the Volume

# COMMAND ----------

# MAGIC %sh 
# MAGIC ls -lh /Volumes/cm_airbnb_mad_demo/airbnb_mad/airbnb_mad/

# COMMAND ----------

# MAGIC %md
# MAGIC Create a folder called Listing inside the Volume

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir /Volumes/cm_airbnb_mad_demo/airbnb_mad/airbnb_mad/listing

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /Volumes/cm_airbnb_mad_demo/airbnb_mad/airbnb_mad/listing
# MAGIC wget https://data.insideairbnb.com/spain/comunidad-de-madrid/madrid/2024-03-22/data/listings.csv.gz 

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
# MAGIC cd /Volumes/cm_airbnb_mad_demo/airbnb_mad/airbnb_mad/listing
# MAGIC ls -lh

# COMMAND ----------

# MAGIC %md
# MAGIC Unzip the file

# COMMAND ----------

# MAGIC %sh
# MAGIC gzip -d /Volumes/cm_airbnb_mad_demo/airbnb_mad/airbnb_mad/listing/listings.csv.gz

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /Volumes/cm_airbnb_mad_demo/airbnb_mad/airbnb_mad/listing
# MAGIC ls -lh

# COMMAND ----------

# MAGIC %sh
# MAGIC #tail -1000f /Volumes/cm_airbnb_mad_demo/airbnb_mad/airbnb_mad/listing/listings.csv

# COMMAND ----------

# MAGIC %md
# MAGIC Load Listing.csv from Databricks Unity Catalog Volume.

# COMMAND ----------

air_bnb_mad_df = spark.read.csv("/Volumes/cm_airbnb_mad_demo/airbnb_mad/airbnb_mad/listing/listings.csv", header=True, inferSchema=True,multiLine="true", escape='"')
display(air_bnb_mad_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Write a Delta Table

# COMMAND ----------

air_bnb_mad_df.write.format("delta").mode("overwrite").saveAsTable("cm_airbnb_mad_demo.airbnb_mad.airbnb_mad_listing_detailed")

# COMMAND ----------

df = spark.sql("SELECT * FROM cm_airbnb_mad_demo.airbnb_mad.airbnb_mad_listing_detailed")
display(df)
