import snowflake.snowpark
import json
import os
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
import pickle

state_dict = json.loads(os.environ['STATE_DICT'].replace("\'","\""))

snowpark_session = snowflake.snowpark.Session.builder.configs(state_dict["connection_parameters"]).create()

df = snowpark_session.table(state_dict['feature_table_name']).to_pandas()

X = df[['DROPOFF_LOCATION_ID','PICKUP_LOCATION_ID']]
y = df[['TRIP_DURATION']]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42)

lr = LinearRegression().fit(X_train, y_train)
print(lr.score(X_train, y_train))

state_dict['model_params']=pickle.dumps(lr)

return_json = {"return_value":f"{state_dict}"}

with open('./airflow/xcom/return.json', 'w') as jrf:
    json.dump(return_json, jrf)