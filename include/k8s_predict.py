from snowflake.snowpark import Session
import sys, os, json
from joblib import load
from sklearn.linear_model import LinearRegression
import pandas as pd

state_dict = json.loads(os.environ['STATE_DICT'].replace("\'","\""))
model_instance = os.environ['MODEL_INSTANCE']

snowpark_session = Session.builder.configs(state_dict['snowflake_connection_parameters']).create()

df = snowpark_session.table(state_dict['feature_table_name'])\
                     .select('PICKUP_LOCATION_ID','DROPOFF_LOCATION_ID')\
                     .to_pandas()

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

df.columns = ['PICKUP_LOCATION_ID', 'DROPOFF_LOCATION_ID']

lr = load('/tmp/'+model_file)

df['PICKUP_LOCATION_ID'] = df['PICKUP_LOCATION_ID'].astype('str')
df['DROPOFF_LOCATION_ID'] = df['DROPOFF_LOCATION_ID'].astype('str')
df['predicted_duration']=lr.predict(df[['PICKUP_LOCATION_ID','DROPOFF_LOCATION_ID']]).astype(int)

snowpark_session.write_pandas(df, table_name=pred_table_name, overwrite=True).collect()

state_dict['pred_table_name'] = pred_table_name

return_json = {"return_value":f"{state_dict}"}

with open('./airflow/xcom/return.json', 'w') as jrf:
    json.dump(return_json, jrf)
