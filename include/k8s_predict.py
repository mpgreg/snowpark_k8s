import json
import os
from sklearn.linear_model import LinearRegression
import pickle
from datetime import datetime

state_dict = json.loads(os.environ['STATE_DICT'].replace("\'","\""))

lrp = pickle.loads(state_dict['model_params'])

pickup_datetime = datetime.strptime('2019-01-05 06:36:51', "%Y-%m-%d %H:%M:%S")
pickup_location_id = '234'
#dropoff_datetime = datetime.strptime('2019-01-05 06:50:42', "%Y-%m-%d %H:%M:%S")

return_json = {"return_value":f"{state_dict}"}

with open('./airflow/xcom/return.json', 'w') as jrf:
    json.dump(return_json, jrf)