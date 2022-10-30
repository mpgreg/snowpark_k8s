import snowflake.snowpark
import json
import os
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
import pickle

state_dict = json.loads(os.environ['STATE_DICT'].replace("\'","\""))

# with open('/tmp/connection.json') as sdf:
#     state_dict = json.load(sdf)    
# state_dict['feature_table_name']='taxi_data'
# state_dict['load_table_name']='raw'

snowpark_session = snowflake.snowpark.Session.builder.configs(state_dict["connection_parameters"]).create()

df = snowpark_session.table(state_dict['feature_table_name']).to_pandas()
#df['TRIP_DURATION'] = (df.DROPOFF_DATETIME - df.PICKUP_DATETIME).dt.seconds

X = df[['PICKUP_LOCATION_ID','DROPOFF_LOCATION_ID']]
y = df[['TRIP_DURATION']]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42)

lr = LinearRegression().fit(X_train, y_train)
print(lr.score(X_train, y_train))
#y_pred = lr.predict(X_test)
#lr.score(X_test, y_test)
#np.mean(y_pred - y_test)

state_dict['model_params']=pickle.dumps(lr)

return_json = {"return_value":f"{state_dict}"}

with open('./airflow/xcom/return.json', 'w') as jrf:
    json.dump(return_json, jrf)