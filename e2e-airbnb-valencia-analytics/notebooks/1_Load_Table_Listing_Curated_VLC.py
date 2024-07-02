# Databricks notebook source
df = spark.sql("SELECT * FROM cm_airbnb_mad_demo.airbnb_vlc.airbnb_vlc_listing_detailed")
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Fix ValueError: could not convert string to float: '$67.00'

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

df = df.withColumn("price", regexp_replace(col("price"), "[^\d.]", "").cast("float"))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("cm_airbnb_mad_demo.airbnb_vlc.airbnb_vlc_listing_detailed_curated")

# COMMAND ----------

df_curated = spark.sql("SELECT * FROM cm_airbnb_mad_demo.airbnb_vlc.airbnb_vlc_listing_detailed_curated")
display(df_curated)
