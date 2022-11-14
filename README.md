This DAG demonstrates how to build a machine learning workflow using Snowpark and compares the use of Snowpark stored procedurs (SPROCs) and user-defined functions (UDFs) with kubernetes pods.

benefits Snowpark

benefits k8s

why external pod operators

setup

copy .kube/config

setup connection in UI or airflow_settings.yaml

build snowpark_task 
docker build . -t snowpark_task:latest
docker tag snowpark_task:latest mpgregor/snowpark_task:latest 
docker push mpgregor/snowpark_task:latest    