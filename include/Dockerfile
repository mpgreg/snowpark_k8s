#docker buildx build --push --platform linux/arm64/v8,linux/amd64 --tag mpgregor/snowpark_task:buildx-latest .

FROM python:3.8.10 

COPY k8s_train.py /tmp
COPY k8s_predict.py /tmp

RUN pip install 'snowflake-snowpark-python[pandas]' &&\
    pip install scikit-learn && \
    mkdir -p airflow/xcom && \
    echo "" > airflow/xcom/return.json