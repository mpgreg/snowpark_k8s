from snowflake.snowpark import Session
import json
import os
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from joblib import dump
from uuid import uuid1

state_dict = json.loads(os.environ['STATE_DICT'].replace("\'","\""))

new_model_id = uuid1().hex
new_model_file = new_model_id+'.joblib'

snowpark_session = Session.builder.configs(state_dict["snowflake_connection_parameters"]).create()

df = snowpark_session.table(state_dict['feature_table_name'])\
                     .select('PICKUP_LOCATION_ID','DROPOFF_LOCATION_ID','TRIP_DURATION')\
                     .to_pandas()

X = df[['PICKUP_LOCATION_ID','DROPOFF_LOCATION_ID']]
y = df[['TRIP_DURATION']]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42)

lr = LinearRegression().fit(X_train, y_train)

dump(lr, new_model_file) 

put_result = snowpark_session.file.put(local_file_name=new_model_file, 
                                        stage_location=state_dict['project_stage_name'], 
                                        auto_compress=False, 
                                        source_compression='NONE', 
                                        overwrite=False)

new_model_instance = int(max(state_dict['models'])) + 1
state_dict['models'][new_model_instance]={'model_id': new_model_id, 'model_file': new_model_file}

with open('./airflow/xcom/return.json', 'w') as jrf:
    json.dump(state_dict, jrf)
