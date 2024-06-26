# Databricks notebook source
# MAGIC %pip install "mlflow-skinny[databricks]>=2.4.1"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# cm_airbnb_mad_demo.airbnb_mad.airbnb_mad_listing_curated_ml_model
import mlflow
catalog = "cm_airbnb_mad_demo"
schema = "airbnb_mad"
model_name = "airbnb_mad_listing_curated_ml_model-best-model01"
mlflow.set_registry_uri("databricks-uc")
mlflow.register_model("runs:/aa6c5b8f32d94b9eace5363f99e0e585/model", f"{catalog}.{schema}.{model_name}")
