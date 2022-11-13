
from datetime import datetime, timedelta
from os import stat
from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.configuration import conf

#Set to false for local testing in docker-desktop k8s service
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

@dag(dag_id='snowpark_ml_k8s_dag', default_args=default_args, schedule_interval=None, start_date=datetime(2022, 10, 27), tags=['snowpark_ml'])
def snowpark_ml_k8s_dag():
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

	train = KubernetesPodOperator(
		namespace=k8s_namespace, 
		image='mpgregor/snowpark_task:latest', 
		cmds=["python", "/tmp/k8s_train.py"], 
		name="snowpark-task-pod",
		task_id="train",
		in_cluster=in_cluster, 
		cluster_context=cluster_context,
		config_file=config_file,
		env_vars={"STATE_DICT": """{{ ti.xcom_pull(task_ids='transform_data', key='return_value') }}"""},
		is_delete_operator_pod=True,
		get_logs=True,
		do_xcom_push=True)

	predict = KubernetesPodOperator(
		namespace=k8s_namespace, 
		image='mpgregor/snowpark_task:latest', 
		cmds=["python", "/tmp/k8s_predict.py"], 
		name="snowpark-task-pod",
		task_id="predict",
		in_cluster=in_cluster, 
		cluster_context=cluster_context,
		config_file=config_file,
		env_vars={"STATE_DICT": """{{ ti.xcom_pull(task_ids='train', key='return_value') }}""",
				  "MODEL_INSTANCE": "latest"},
		is_delete_operator_pod=True,
		get_logs=True,
		do_xcom_push=True) 
	
	test_conn = _test_snowflake_connection(state_dict)
	extract = _extract_to_stage(state_dict)
	load = _load_to_raw(extract)
	transform = _transform_to_features(load)
	
	test_conn >> create_stage >> extract >> load >> transform >> train >> predict

snowpark_ml_k8s_dag = snowpark_ml_k8s_dag()