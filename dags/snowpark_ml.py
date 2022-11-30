from datetime import datetime, timedelta
from os import stat
from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.configuration import conf

# Build the Docker image in snowpark_k8s/include/Dockerfile.  

docker_image_uri = 'mpgregor/snowpark_task:buildx-latest'

# If using docker-desktop k8s service, enable kubernetes and copy the kube config file 
# to <astro-project-dir>/include/.kube/config and set in_cluster=False below.
# If using Astronomer managed airflow set in_cluster=True to use the managed k8s instance

in_cluster=False

if in_cluster:
	k8s_namespace = conf.get("kubernetes", "NAMESPACE")
	cluster_context = None
	config_file = None
else: 
	k8s_namespace = 'default'
	cluster_context = 'docker-desktop'
	config_file = '/usr/local/airflow/include/.kube/config'

default_args={"snowflake_conn_id": 'snowflake_default',
			  "retries": 2}

# Since we are using ExternalPythonOperator and KubernetesPodOperators it is necessary to serialize data passing into/out of tasks
# We will use a state dictionary to pass state more easily between tasks
@dag(dag_id='snowpark_ml_dag', default_args=default_args, schedule_interval=None, start_date=datetime(2022, 10, 27), tags=['snowpark_ml'])
def snowpark_ml_dag():
	import urllib
	from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

	snowflake_hook = SnowflakeHook(conn_id=default_args['snowflake_conn_id'])
	snowflake_connection = snowflake_hook.get_conn()

	state_dict = {
		'files_to_stage': ['include/yellow_tripdata_sample_2019-01.csv', 'include/yellow_tripdata_sample_2019-02.csv'],
		'files_to_load': ['yellow_tripdata_sample_2019-01.csv', 'yellow_tripdata_sample_2019-02.csv'],
		'load_table_name': 'raw',
		'feature_table_name': 'taxi_data',
		'project_stage_name': 'snowpark_ml_stage',
		'models': {0: {}},
		'snowflake_connection_parameters': {'user': snowflake_connection.user, 
											'account': snowflake_connection.account, 
											'region': snowflake_connection.region,
											'role': snowflake_connection.role,
											'database': snowflake_connection.database,
											'schema': snowflake_connection.schema, 		
											'password': urllib.parse.urlparse(snowflake_hook.get_uri()).password,						   
											'warehouse': snowflake_connection.warehouse
											}
	}
	
	@task(task_id='test_connection', default_args=default_args)
	def _test_snowflake_connection(state_dict:dict):
		from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
		print(SnowflakeHook(conn_id=default_args['snowflake_conn_id']).test_connection())
		return state_dict

	create_stage = SnowflakeOperator(
        task_id="create_stage",
        sql="CREATE OR REPLACE STAGE "+state_dict['project_stage_name']
    )

	@task.external_python(task_id="extract_data", python='/home/astro/miniconda3/envs/snowpark_env/bin/python')
	def _extract_to_stage(state_dict:dict):
		from snowflake.snowpark import Session
				
		snowpark_session = Session.builder.configs(state_dict['snowflake_connection_parameters']).create()

		for csv_file_name in state_dict['files_to_stage']:
			put_result = snowpark_session.file.put(local_file_name=csv_file_name, 
												   stage_location=state_dict['project_stage_name'], 
												   auto_compress=False, 
												   source_compression='GZIP',
												   overwrite=True)
			print(put_result)

		return state_dict
	
	@task.external_python(task_id="load_data", python='/home/astro/miniconda3/envs/snowpark_env/bin/python')
	def _load_to_raw(state_dict:dict):
		from snowflake.snowpark import functions as F
		from snowflake.snowpark import types as T
		from snowflake.snowpark import Session

		snowpark_session = Session.builder.configs(state_dict['snowflake_connection_parameters']).create()
	
		csv_file_format_options = {"FIELD_OPTIONALLY_ENCLOSED_BY": "'\"'", "skip_header": 1}
		#2019-01-05 06:36:51
		taxi_schema = T.StructType([T.StructField("vendor_id", T.StringType()),
									T.StructField("pickup_datetime", T.TimestampType()), 
									T.StructField("dropoff_datetime", T.TimestampType()), 
									T.StructField("passenger_count", T.IntegerType()),
									T.StructField("trip_distance", T.FloatType()), 
									T.StructField("rate_code_id", T.StringType()),
									T.StructField("store_and_fwd_flag", T.StringType()),
									T.StructField("pickup_location_id", T.StringType()),
									T.StructField("dropoff_location_id", T.StringType()), 
									T.StructField("payment_type", T.StringType()),
									T.StructField("fare_amount", T.FloatType()),
									T.StructField("extra", T.FloatType()),
									T.StructField("mta_tax", T.FloatType()), 
									T.StructField("tip_amount", T.FloatType()),
									T.StructField("tolls_amount", T.FloatType()),
									T.StructField("improvement_surcharge", T.FloatType()),
									T.StructField("total_amount", T.FloatType()),
									T.StructField("congestion_surcharge", T.FloatType())])

		loaddf = snowpark_session.read.option("SKIP_HEADER", 1)\
							.option("FIELD_OPTIONALLY_ENCLOSED_BY", "\042")\
							.option("COMPRESSION", "GZIP")\
							.option("NULL_IF", "\\\\N")\
							.option("NULL_IF", "NULL")\
							.option("OVERWRITE", "TRUE")\
							.schema(taxi_schema)\
							.csv('@'+state_dict['project_stage_name'])\
							.copy_into_table(state_dict['load_table_name'], 
											files=state_dict['files_to_load'],
											format_type_options=csv_file_format_options)                     
		return state_dict

	@task.external_python(task_id="transform_data", python='/home/astro/miniconda3/envs/snowpark_env/bin/python')
	def _transform_to_features(state_dict:dict):		
		from snowflake.snowpark import Session
		from snowflake.snowpark import functions as F

		snowpark_session = Session.builder.configs(state_dict['snowflake_connection_parameters']).create()
		
		taxidf = snowpark_session.table(state_dict['load_table_name'])
		
		taxidf = taxidf.withColumn('TRIP_DURATION', F.datediff('seconds', F.col('PICKUP_DATETIME'), F.col('DROPOFF_DATETIME')))

		taxidf.write.mode('overwrite').save_as_table(state_dict['feature_table_name'])
		return state_dict

	@task.external_python(task_id="train_sproc", python='/home/astro/miniconda3/envs/snowpark_env/bin/python')
	def _train_sproc(state_dict):
		from snowflake.snowpark import Session
		from snowflake.snowpark.functions import sproc

		snowpark_session = Session.builder.configs(state_dict['snowflake_connection_parameters']).create()
		snowpark_session.add_packages('snowflake-snowpark-python', 'scikit-learn')

		@sproc(session=snowpark_session)
		def train_model(snowpark_session: Session, feature_table_name: str, model_stage_location:str) -> str:
			import pandas as pd
			from sklearn.linear_model import LinearRegression
			from sklearn.model_selection import train_test_split
			from joblib import dump
			from uuid import uuid1

			new_model_id = uuid1().hex
			new_model_file = '/tmp/'+new_model_id+'.joblib'

			df = snowpark_session.table(feature_table_name).to_pandas()
			X = df[['PICKUP_LOCATION_ID','DROPOFF_LOCATION_ID']]
			y = df[['TRIP_DURATION']]

			X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42)

			lr = LinearRegression().fit(X_train, y_train)

			dump(lr, new_model_file) 

			put_result = snowpark_session.file.put(local_file_name=new_model_file, 
												   stage_location=model_stage_location, 
												   auto_compress=False, 
												   source_compression='NONE', 
												   overwrite=False)

			return new_model_id #pickle.dumps(lr) #, lr.score(X_train, y_train)

		new_model_id = train_model(snowpark_session, state_dict['feature_table_name'], state_dict['project_stage_name'])

		new_model_instance = int(max(state_dict['models'])) + 1
		state_dict['models'][new_model_instance]={'model_id': new_model_id, 'model_file': new_model_id+'.joblib'}

		return state_dict


	@task.external_python(task_id="predict_udf", default_args=default_args, python='/home/astro/miniconda3/envs/snowpark_env/bin/python')
	def _predict_udf(state_dict, model_instance):
		from snowflake.snowpark import Session
		from snowflake.snowpark.types import PandasSeries, PandasDataFrame
		from snowflake.snowpark.functions import udf, lit

		snowpark_session = Session.builder.configs(state_dict['snowflake_connection_parameters']).create()

		if model_instance == 'latest':
			model_instance = int(max(state_dict['models']))
		else:
			model_instance = model_instance

		model_id = state_dict['models'][str(model_instance)]['model_id']
		model_file = state_dict['models'][str(model_instance)]['model_file']
		staged_model_file = '@'+state_dict['project_stage_name']+'/'+model_file

		snowpark_session.add_import(staged_model_file)

		pred_table_name = 'pred_'+model_id

		@udf(session=snowpark_session, packages=['pandas', 'scikit-learn', 'joblib'])
		def predict(df: PandasDataFrame[str, str, str]) -> PandasSeries[int]:
			import sys, os
			from joblib import load
			from sklearn.linear_model import LinearRegression
			import pandas as pd
			df.columns = ['PICKUP_LOCATION_ID', 'DROPOFF_LOCATION_ID', 'MODEL_FILE']
			import_dir = sys._xoptions.get("snowflake_import_directory")
			lr = load(os.path.join(import_dir, df['MODEL_FILE'][0]))
			df['PICKUP_LOCATION_ID'] = df['PICKUP_LOCATION_ID'].astype('str')
			df['DROPOFF_LOCATION_ID'] = df['DROPOFF_LOCATION_ID'].astype('str')
			return lr.predict(df[['PICKUP_LOCATION_ID','DROPOFF_LOCATION_ID']]).astype(int)

		snowpark_session.table(state_dict['feature_table_name'])\
						.with_column('predicted_duration', predict('PICKUP_LOCATION_ID', 'DROPOFF_LOCATION_ID', lit(model_file)))\
						.write.mode('overwrite').save_as_table(pred_table_name)		

		state_dict['pred_table_name'] = pred_table_name

		return state_dict

	train_k8s = KubernetesPodOperator(
		namespace=k8s_namespace, 
		image=docker_image_uri, 
		cmds=["python", "/tmp/k8s_train.py"], 
		name="snowpark-task-pod",
		task_id="train_k8s",
		in_cluster=in_cluster, 
		cluster_context=cluster_context,
		config_file=config_file,
		env_vars={"STATE_DICT": """{{ ti.xcom_pull(task_ids='transform_data', key='return_value') }}"""},
		is_delete_operator_pod=True,
		get_logs=True,
		do_xcom_push=True)

	predict_k8s = KubernetesPodOperator(
		namespace=k8s_namespace, 
		image=docker_image_uri, 
		cmds=["python", "/tmp/k8s_predict.py"], 
		name="snowpark-task-pod",
		task_id="predict_k8s",
		in_cluster=in_cluster, 
		cluster_context=cluster_context,
		config_file=config_file,
		env_vars={"STATE_DICT": """{{ ti.xcom_pull(task_ids='train_k8s', key='return_value') }}""",
				  "MODEL_INSTANCE": "latest"},
		is_delete_operator_pod=True,
		get_logs=True,
		do_xcom_push=True)
	
	test_conn = _test_snowflake_connection(state_dict)
	extract = _extract_to_stage(state_dict)
	load = _load_to_raw(extract)
	transform = _transform_to_features(load)
	train_sproc = _train_sproc(transform)
	predict_udf = _predict_udf(state_dict=train_sproc, model_instance='latest')

	test_conn >> create_stage >> extract >> load >> transform >> train_sproc >> predict_udf
	transform >> train_k8s >> predict_k8s

snowpark_ml_dag = snowpark_ml_dag()