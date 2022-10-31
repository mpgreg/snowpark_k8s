from datetime import datetime, timedelta
from xml.etree.ElementPath import prepare_predicate
from airflow.decorators import dag, task
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
#from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

SNOWFLAKE_CONN_ID = "snowflake_default"
default_args={"conn_id": SNOWFLAKE_CONN_ID, "snowflake_conn_id": SNOWFLAKE_CONN_ID}

@dag(dag_id='snowpark_k8s_dag', default_args=default_args, schedule_interval=None, start_date=datetime(2022, 10, 27))
def snowpark_k8s_dag():
	from airflow.hooks import base
	import json
	snowflake_connection = base.BaseHook.get_connection(conn_id=SNOWFLAKE_CONN_ID)
	snowflake_connection_extra = json.loads(snowflake_connection.get_extra())

	state_dict = {
		'files_to_stage': ['include/yellow_tripdata_sample_2019-01.csv', 'include/yellow_tripdata_sample_2019-02.csv'],
		'files_to_load': ['yellow_tripdata_sample_2019-01.csv', 'yellow_tripdata_sample_2019-02.csv'],
		'load_table_name': 'raw',
		'feature_table_name': 'taxi_data',
		'load_stage_name': 'my_load_stage',
		'connection_parameters': {'user': snowflake_connection.login, 
								  'account': snowflake_connection.host, 
								  'role': snowflake_connection_extra['extra__snowflake__role'], 
								  'database': snowflake_connection_extra['extra__snowflake__database'], 
								  'schema': snowflake_connection.schema, 
								  'password': snowflake_connection.password,
								  'warehouse': snowflake_connection_extra['extra__snowflake__warehouse']}
	}
	
	@task(task_id='test_connection')
	def _test_snowflake_connection(state_dict:dict):
		from airflow.hooks import base
		print(base.BaseHook.get_connection(conn_id=SNOWFLAKE_CONN_ID).test_connection())
		return state_dict

	create_stage = SnowflakeOperator(
        task_id="create_stage",
        sql="CREATE STAGE IF NOT EXISTS "+state_dict['load_stage_name']
    )

	@task.external_python(task_id="extract_data", python='/home/astro/.pyenv/versions/snowpark_env/bin/python')
	def _extract_to_stage(state_dict:dict):
		from snowflake.snowpark import Session
		snowpark_session = Session.builder.configs(state_dict['connection_parameters']).create()

		for csv_file_name in state_dict['files_to_stage']:
			put_result = snowpark_session.file.put(local_file_name=csv_file_name, 
												   stage_location=state_dict['load_stage_name'], 
												   source_compression='GZIP', 
												   overwrite=True)
			print(put_result)

		return state_dict

	@task.external_python(task_id="load_data", python='/home/astro/.pyenv/versions/snowpark_env/bin/python')
	def _load_to_raw(state_dict:dict):
		from snowflake.snowpark import functions as F
		from snowflake.snowpark import types as T
		from snowflake.snowpark import Session

		snowpark_session = Session.builder.configs(state_dict['connection_parameters']).create()
	
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
							.schema(taxi_schema)\
							.csv('@'+state_dict['load_stage_name'])\
							.copy_into_table(state_dict['load_table_name'], 
											files=state_dict['files_to_load'],
											format_type_options=csv_file_format_options)                     
		return state_dict

	@task.external_python(task_id="transform_data", python='/home/astro/.pyenv/versions/snowpark_env/bin/python')
	def _transform_to_features(state_dict:dict):		
		from snowflake.snowpark import Session
		from snowflake.snowpark import functions as F

		snowpark_session = Session.builder.configs(state_dict['connection_parameters']).create()
		
		taxidf = snowpark_session.table(state_dict['load_table_name'])
		
		taxidf.withColumn('TRIP_DURATION', F.datediff('seconds', F.col('PICKUP_DATETIME'), F.col('DROPOFF_DATETIME'))) \
			  .write.mode('overwrite').save_as_table(state_dict['feature_table_name'])
		return state_dict

	train_model = KubernetesPodOperator(namespace='default', 
									   image='snowpark_task:local', 
									   cmds=["python", "/tmp/k8s_train.py"], 
									   name="snowpark-task-pod",
									   task_id="train_model",
									   in_cluster=False, 
									   cluster_context="docker-desktop", 
									   config_file='/usr/local/airflow/include/.kube/config',
									   is_delete_operator_pod=True,
									   get_logs=True,
									   do_xcom_push=True,
									   env_vars={"STATE_DICT": """{{ ti.xcom_pull(task_ids='transform_data',
                                                 key='return_value') }}"""})
	
	# predict = KubernetesPodOperator(namespace='default', 
	# 								image='snowpark_task:local', 
	# 								cmds=["python", "/tmp/k8s_predict.py"], 
	# 								name="snowpark-task-pod",
	# 								task_id="predict",
	# 								in_cluster=False, 
	# 								cluster_context="docker-desktop", 
	# 								config_file='/usr/local/airflow/include/.kube/config',
	# 								is_delete_operator_pod=True,
	# 								get_logs=True,
	# 								do_xcom_push=True,
	# 								env_vars={"STATE_DICT": """{{ ti.xcom_pull(task_ids='train_model', 
	# 															  key='return_value') }}"""})

	test_conn = _test_snowflake_connection(state_dict)
	extract = _extract_to_stage(state_dict)
	load = _load_to_raw(extract)
	transform = _transform_to_features(load)
	
	test_conn >> create_stage >> extract >> load >> transform >> train_model

snowpark_k8s_dag = snowpark_k8s_dag()