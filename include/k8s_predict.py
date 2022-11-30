from snowflake.snowpark import Session
import sys, os, json
from joblib import load
from sklearn.linear_model import LinearRegression
import pandas as pd

state_dict = json.loads(os.environ['STATE_DICT'].replace("\'","\""))
snowflake_password = os.environ['snowflake_password']
model_instance = os.environ['MODEL_INSTANCE']

connection_parameters = state_dict['snowflake_connection_parameters'].copy()
connection_parameters.update({'password': snowflake_password})
snowpark_session = Session.builder.configs(connection_parameters).create()

df = snowpark_session.table(state_dict['feature_table_name']).to_pandas()

if model_instance == 'latest':
    model_instance = int(max(state_dict['models']))
else:
    model_instance = model_instance

model_id = state_dict['models'][str(model_instance)]['model_id']
model_file = state_dict['models'][str(model_instance)]['model_file']
staged_model_file = '@'+state_dict['project_stage_name']+'/'+model_file

snowpark_session.file.get(stage_location=staged_model_file, 
                          target_directory='/tmp')

pred_table_name = 'pred_'+model_id

lr = load('/tmp/'+model_file)

df['PREDICTED_DURATION']=lr.predict(df[['PICKUP_LOCATION_ID','DROPOFF_LOCATION_ID']]).astype(int)

snowpark_session.write_pandas(df, table_name=pred_table_name.upper(), overwrite=True).collect()

state_dict['pred_table_name'] = pred_table_name.upper()

with open('./airflow/xcom/return.json', 'w') as jrf:
    json.dump(state_dict, jrf)

snowpark_session.close()