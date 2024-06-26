# Databricks notebook source
# MAGIC %md
# MAGIC Notebook to show how to predict using Curl and Python 

# COMMAND ----------

# Use this to test the UI 

{
  "dataframe_split": {
    "index": [0, 1, 2, 3, 4],
    "columns": ["latitude", "longitude", "bathrooms", "bedrooms", "beds"],
    "data": [
      [40.40985, -3.65516, 1.0, 3.0, 4.0],
      [40.40933, -3.70886, 1.5, 1.0, 2.0],
      [40.40510698553972, -3.7469288502624454, 2.0, 4.0, 4.0],
      [40.42243, -3.66541, 2.5, 2.0, 0.0],
      [40.42640366686258, -3.679466842738444, 1.0, 1.0, 1.0]
    ]
  }
}


# COMMAND ----------

{
  "dataframe_split": {
    "columns": [
      "latitude",
      "longitude",
      "bathrooms",
      "bedrooms",
      "beds"
    ],
    "data": [
      [
        40.40985,
        -3.65516,
        1,
        3,
        4
      ],
      [
        40.40933,
        -3.70886,
        1.5,
        1,
        2
      ],
      [
        40.40510698553972,
        -3.7469288502624454,
        2,
        4,
        4
      ],
      [
        40.42243,
        -3.66541,
        2.5,
        2,
        0
      ],
      [
        40.42640366686258,
        -3.679466842738444,
        1,
        1,
        1
      ]
    ]
  }
}

# COMMAND ----------

import mlflow
import pandas as pd

# Define the input data
ml_data = {
  "dataframe_split": {
    "index": [0, 1, 2, 3, 4],
    "columns": ["latitude", "longitude", "bathrooms", "bedrooms", "beds"],
    "data": [
      [40.40985, -3.65516, 1.0, 3.0, 4.0],
      [40.40933, -3.70886, 1.5, 1.0, 2.0],
      [40.40510698553972, -3.7469288502624454, 2.0, 4.0, 4.0],
      [40.42243, -3.66541, 2.5, 2.0, 0.0],
      [40.42640366686258, -3.679466842738444, 1.0, 1.0, 1.0]
    ]
  }
}


# COMMAND ----------

ml_data

# COMMAND ----------

DATABRICKS_OAUTH_TOKEN = "PLACE_HERE_YOUR_OAUTH_TOKEN"

# COMMAND ----------

import os

os.environ["DATABRICKS_OAUTH_TOKEN"] = DATABRICKS_OAUTH_TOKEN
#print(os.environ.get("DATABRICKS_OAUTH_TOKEN"))

# COMMAND ----------

token = os.environ.get("DATABRICKS_OAUTH_TOKEN")

# COMMAND ----------

#print(token)

# COMMAND ----------

#print(os.environ.get("DATABRICKS_OAUTH_TOKEN"))

# COMMAND ----------

# MAGIC %md
# MAGIC Calling the ML Endpoint using CURL

# COMMAND ----------

# MAGIC %sh
# MAGIC curl \
# MAGIC   -H "Authorization: Bearer $DATABRICKS_OAUTH_TOKEN" \
# MAGIC   -X POST \
# MAGIC   -H "Content-Type: application/json" \
# MAGIC   -d@input_example.json \
# MAGIC   https://3f07bdea10534c01859bda4b07c98e35.serving.cloud.databricks.com/1444828305810485/serving-endpoints/airbnb_mad_listing_curated_ml_model-best-model01/invocations

# COMMAND ----------

# MAGIC %md
# MAGIC Batch Prediction using the Online Endpoint

# COMMAND ----------

import os
import requests
import numpy as np
import pandas as pd
import json

def create_tf_serving_json(data):
    if isinstance(data, pd.DataFrame):
        return {'dataframe_split': data.to_dict(orient='split')}
    elif isinstance(data, dict):
        return {'inputs': {name: data[name].tolist() for name in data.keys()}}
    else:
        return {'inputs': data.tolist()}

def score_model(dataset):
    url = 'https://3f07bdea10534c01859bda4b07c98e35.serving.cloud.databricks.com/1444828305810485/serving-endpoints/airbnb_mad_listing_curated_ml_model-best-model01/invocations'
    headers = {'Authorization': f'Bearer {os.environ.get("DATABRICKS_OAUTH_TOKEN")}', 'Content-Type': 'application/json'}
    #print(os.environ.get("DATABRICKS_OAUTH_TOKEN"))
    print(dataset)

    ds_dict = create_tf_serving_json(dataset)
    data_json = json.dumps(ds_dict, allow_nan=True)
    response = requests.request(method='POST', headers=headers, url=url, data=data_json)
    if response.status_code != 200:
        raise Exception(f'Request failed with status {response.status_code}, {response.text}')
    return response.json()

# Example usage:
ml_data = pd.DataFrame({
    "latitude": [40.40985, 40.40933, 40.405107, 40.42243, 40.426404],
    "longitude": [-3.65516, -3.70886, -3.746929, -3.66541, -3.679467],
    "bathrooms": [1.0, 1.5, 2.0, 2.5, 1.0],
    "bedrooms": [3.0, 1.0, 4.0, 2.0, 1.0],
    "beds": [4.0, 2.0, 4.0, 0.0, 1.0]
})

# Call the function
result = score_model(ml_data)
print(result)


# COMMAND ----------

# MAGIC %md
# MAGIC Single Prediction

# COMMAND ----------

import os
import requests
import numpy as np
import pandas as pd
import json

def create_tf_serving_json(data):
    if isinstance(data, pd.DataFrame):
        return {'dataframe_split': data.to_dict(orient='split')}
    elif isinstance(data, dict):
        return {'inputs': {name: data[name].tolist() for name in data.keys()}}
    else:
        return {'inputs': data.tolist()}

def score_model(dataset):
    url = 'https://3f07bdea10534c01859bda4b07c98e35.serving.cloud.databricks.com/1444828305810485/serving-endpoints/airbnb_mad_listing_curated_ml_model-best-model01/invocations'
    headers = {'Authorization': f'Bearer {os.environ.get("DATABRICKS_OAUTH_TOKEN")}', 'Content-Type': 'application/json'}
    #print(os.environ.get("DATABRICKS_OAUTH_TOKEN"))
    print(dataset)

    ds_dict = create_tf_serving_json(dataset)
    data_json = json.dumps(ds_dict, allow_nan=True)
    response = requests.request(method='POST', headers=headers, url=url, data=data_json)
    if response.status_code != 200:
        raise Exception(f'Request failed with status {response.status_code}, {response.text}')
    return response.json()

# Example usage:
ml_data = pd.DataFrame({
    "latitude": [40.40985],
    "longitude": [-3.65516],
    "bathrooms": [1.0],
    "bedrooms": [3.0],
    "beds": [4.0]
})

# Call the function
result = score_model(ml_data)
print(result)

