@task.external_python(task_id="sproc_train", python='/home/astro/.pyenv/versions/snowpark_env/bin/python')
    def train_model(state_dict:dict):
        from snowflake.snowpark import Session
		snowpark_session = Session.builder.configs(state_dict['connection_parameters']).create()		
		snowpark_session.add_packages(["snowflake-snowpark-python","scikit-learn"])

        @sproc() #session=snowpark_session, state_dict:dict)
        def train(snowpark_session: snowflake.snowpark.Session, state_dict:dict):
			import json
			import os
			import pandas as pd
			from sklearn.linear_model import LinearRegression
			from sklearn.model_selection import train_test_split
			import pickle

			df = snowpark_session.table(state_dict['feature_table_name']).to_pandas()
			X = df[['DROPOFF_LOCATION_ID','PICKUP_LOCATION_ID']]
			y = df[['TRIP_DURATION']]

			X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42)

			lr = LinearRegression().fit(X_train, y_train)
			#print(lr.score(X_train, y_train))

			state_dict['model_params']=pickle.dumps(lr)
			
        	return train(snowpark_session, state_dict)

    sproc_result = calculate_pi_sproc(10000)