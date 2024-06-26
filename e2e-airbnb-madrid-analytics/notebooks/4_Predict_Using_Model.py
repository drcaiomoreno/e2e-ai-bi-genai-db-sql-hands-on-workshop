# Databricks notebook source
#airbnb_mad_listing_curated_ml_model-best-model01

# COMMAND ----------

import mlflow
import pandas as pd

# Define the input data
data = {"columns": ["latitude", "longitude", "bathrooms", "bedrooms", "beds"], "data": [[40.40985, -3.65516, 1.0, 3.0, 4.0], [40.40933, -3.70886, 1.5, 1.0, 2.0], [40.40510698553972, -3.7469288502624454, 2.0, 4.0, 4.0], [40.42243, -3.66541, 2.5, 2.0, 0.0], [40.42640366686258, -3.679466842738444, 1.0, 1.0, 1.0]]}

# COMMAND ----------

# MAGIC %md
# MAGIC Predict on a Pandas DataFrame:

# COMMAND ----------

import mlflow
import pandas as pd

# Define the input data
data = {"columns": ["latitude", "longitude", "bathrooms", "bedrooms", "beds"], "data": [[40.40985, -3.65516, 1.0, 3.0, 4.0], [40.40933, -3.70886, 1.5, 1.0, 2.0], [40.40510698553972, -3.7469288502624454, 2.0, 4.0, 4.0], [40.42243, -3.66541, 2.5, 2.0, 0.0], [40.42640366686258, -3.679466842738444, 1.0, 1.0, 1.0]]}

# Load the model
logged_model = 'runs:/aa6c5b8f32d94b9eace5363f99e0e585/model'
loaded_model = mlflow.pyfunc.load_model(logged_model)

# Create a DataFrame from the input data
input_df = pd.DataFrame(data["data"], columns=data["columns"])

# Predict using the loaded model
prediction = loaded_model.predict(input_df)

# Display the prediction
prediction

# COMMAND ----------

# MAGIC %md
# MAGIC Try different values

# COMMAND ----------

import mlflow
import pandas as pd

# Define the input data
data = {"columns": ["latitude", "longitude", "bathrooms", "bedrooms", "beds"], "data": [[40.40985, -3.65516, 1.0, 3.0, 4.0], [40.40933, -3.70886, 2.5, 3.0, 1.0], [40.40510698553972, -3.7469288502624454, 1.0, 2.0, 5.0], [40.42243, -3.66541, 2.5, 2.0, 0.0], [40.42640366686258, -3.679466842738444, 4.0, 3.0, 1.0]]}

# Load the model
logged_model = 'runs:/aa6c5b8f32d94b9eace5363f99e0e585/model'
loaded_model = mlflow.pyfunc.load_model(logged_model)

# Create a DataFrame from the input data
input_df = pd.DataFrame(data["data"], columns=data["columns"])

# Predict using the loaded model
prediction = loaded_model.predict(input_df)

# Display the prediction
prediction
