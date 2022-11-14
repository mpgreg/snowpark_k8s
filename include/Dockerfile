FROM --platform=amd64 python:3.8.10

COPY k8s_train.py /tmp
COPY k8s_predict.py /tmp

RUN pip install 'snowflake-snowpark-python[pandas]' &&\
    pip install scikit-learn && \
    mkdir -p airflow/xcom && \
    echo "" > airflow/xcom/return.json