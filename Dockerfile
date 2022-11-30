FROM quay.io/astronomer/astro-runtime:6.0.4

#Python for Snowpark via ExternalPythonOperator
RUN arch=$(arch | sed s/aarch64/aarch64/ | sed s/x86_64/x86_64/) && \
    wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-${arch}.sh -O ~/miniconda.sh && \
    /bin/bash ~/miniconda.sh -b && \
    ~/miniconda3/bin/conda init bash

RUN ~/miniconda3/bin/conda create -qyn snowpark_env --override-channels -c https://repo.anaconda.com/pkgs/snowflake python=3.8 snowflake-snowpark-python pandas    
    
ENV PATH=~/miniconda3/bin:$PATH

ENV AIRFLOW_CONN_SNOWFLAKE_DEFAULT='{\
    "conn_type": "Snowflake",\
    "login": "user_name", \
    "schema": "schema_name", \
    "password": "password", \
    "extra": "{\"account\": \"account_name\", \"region\": \"region_name\", \"role\": \"role_name\", \"warehouse\": \"warehouse_name\", \"database\": \"database_name\"}"\
    }'