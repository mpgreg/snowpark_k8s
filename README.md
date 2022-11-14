---
title: "Run machine learning workflows in Snowpark via ExternalPythonOperator and/or KubernetesPodOperator"
sidebar_label: "ExternalPythonOperator, KubernetesPodOperator"
description: "Learn how to run a machine learning workflow using Snowpark dataframe API, stored procedures and user-defined functions as well as Kubernetes pods in Apache Airflow."
id: 
---

[Snowpark](https://docs.snowflake.com/en/developer-guide/snowpark/python/index.html) is an extensibility framework for the [Snowflake](https://www.snowflake.com/) managed data warehouse.  Snowpark allows users to interact with Snowflake's SQL-based compute frameworks using non-SQL languages (currently Java/Scala and Python) and includes a dataframe API as well as a backend runtime to run  scala/python functions as Store Procedures (sproc) or User-defined Functions (udf) on Snowflake-provided compute.

This guide demonstrates how to use Apache Airflow to orchestrate a machine learning pipeline leveraging Snowflake storage and compute via Snowpark Python including how to:  
  
- Use the Apache Airflow Snowflake provider for common tasks in SQL
- Build python-based DAGs using the Snowpark dataframe API
- Extend basic provider functionality with the ExternalPythonOperator to workaround python major version dependencies
- Comparison of equivalent tasks using the KubernetesPodOperator for tasks that may not work well in Snowpark SPROCs and UDFs.

## Prerequisites  
  
- Astro [CLI](https://docs.astronomer.io/astro/cli/get-started)
- Docker Desktop (with Kubernetes enabled)
- Git
- Snowflake account (or a [trial account](https://signup.snowflake.com/))

## Airflow Snowflake Provider  
  
The Snowflake provider for Apache Airflow includes several functions for interfacing with Snowflake including a Hook and a Connection type.  For this demo it is necessary to instantiate a Airflow connection called 'snowflake_default' using either the Airflow UI or the airflow_settings.yaml file.  The "test_connection" task shows an example of how to use the Snowflake provider hook.

## ExternalPythonOperator for Snowpark  
  
As of this demo's creation Snowpark Python is supported only on Python 3.8.  While Apache Airflow is supported on Python 3.8 it will often be the case that managed Airflow services, such as Astronomer, will use the most up-to-date versions of Apache Airflow and Python.  As explained in the [ExternalPythonOperator tutorial](https://github.com/astronomer/docs/blob/pythonvirtualenv-tutorial/learn/external-python-operator.md) it is possible to run Snowpark Python despite major-version differences.  In this demo we show an example of using [miniconda](https://docs.conda.io/en/latest/miniconda.html) to create the external python instance for the ExternalPythonOperator.

## Comparison: Snowpark or Kubernetes  
  
Snowpark makes it possible to run (mostly) arbitrary compute "close" to your data.  While this proximity may allow for more performant data access (theoretically at cloud storage speed) it also has the benefit of reinforcing security and governance by avoiding the need to read data out of Snowflake.  Additionally Snowpark compute reduces the need to find (or maintain) external compute frameworks like Kubernetes or a serverless framework like AWS Lambda and also has UDF constructs to enable parallelized processing for those "embarassingly parallel" tasks like machine learning inference.

However, Snowpark is a new development framework and somethings run contradictory to generally accepted development practices which are often built around Docker, Kubernetes, and CI/CD practices with things like Github actions.  Some find the developer experience in Snowpark to be sub-optimal for things like interactive testing, debugging, etc.  Additionally the potential for dependency management issues must be considered as the Snowflake currated runtime (in conjunction with Anaconda) may lack needed libraries and/or versions.  Lastly, Snowpark currently only supports Java/Scala and Python and may be slow to release new languages (Julia?).

## KubernetesPodOperator with Snowpark  
  
For the above reasons developers may choose to continue using things like Kubernetes for certain tasks. The KubernetesPodOperator allows users to build tasks in practically any language, with simplified dependency management, and a consistent developer experience.  Using the KubernetesPodOperator often requires additional overhead of building and maintaining compute infrastructure.  This demo uses the Astronomer CLI and local development runtime along with Docker Desktop (with local Kubernetes enabled) to simplify local development and testing.  Users of Astronomer's managed Airflow services can easily push this DAG to run in Astronomer-managed Kubernetes without the need to build or maintain infrastructure.

## Setup  
  
1. Git clone this repository
2. [Enable Kubernetes in Docker Desktop](https://docs.docker.com/desktop/kubernetes/)
3. Copy kubernetes config:  
    ```sh
    mkdir ./snowpark_k8s/include/.kube
    cp ~/.kube/config ./snowpark_k8s/include/.kube
    ```
4. Edit airflow_settings.yaml and create a new connection called **'snowflake_default'** with your Snowflake account details.  Example:  
    ```text
    connections:
        - conn_id: snowflake_default
        conn_type: Snowflake
        conn_host: <account_id>.<region_name>
        conn_schema: <schema_name>
        conn_login: <user_login_name>
        conn_password: <password>
        conn_port:
        conn_extra: {"extra__snowflake__account": "<account_id>", 
                    "extra__snowflake__warehouse": "<warehouse_name>", 
                    "extra__snowflake__database": "<database_name>", 
                    "extra__snowflake__region": "<region_name>", 
                    "extra__snowflake__role": "<user_role_name>", 
                    "extra__snowflake__insecure_mode": false}
    ```  
    The airflow_settings.yaml file listed in .gitignore so this should not be copied to git.  Alternatively you can enter this information in the Airflow UI after starting the Astronomer dev runtime.  
    ```sh
    cd snowpark_k8s
    astro dev start
    ```  
6. Connect to the Local Airflow instance UI at (http://localhost:8080) and login with **Admin/Admin**  
    If you did not add the Snowflake connection to airflow_settings.yaml add it now in the Admin -> Connections menu.  
7. To run the Kubernetes-based tasks locally you will need to build the Docker image:  
    ```sh
    cd snowpark_k8s/include
    docker build . -t snowpark_task:0.1.0
    ```
8. If you will be using Astronomer managed Airflow you will need to push the Docker image to an accessible repo:
    ```sh
    docker tag snowpark_task:0.1.0 <your_repo>/snowpark_task:0.1.0 
    docker push <your_repo>/snowpark_task:0.1.0
    ```  
    Edit the DAG \(snowpark_k8s/dags/snowpark_ml.py\) and update the `docker_repo` variable at the top.