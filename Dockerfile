FROM quay.io/astronomer/astro-runtime:6.0.3

ENV PYENV_ROOT="/home/astro/.pyenv" 
ENV PATH=${PYENV_ROOT}/bin:${PATH}
#COPY apache_airflow_providers_snowflake-3.3.0-py3-none-any.whl /tmp
#RUN pip install --no-cache-dir /tmp/apache_airflow_providers_snowflake-3.3.0-py3-none-any.whl

RUN curl https://pyenv.run | bash  && \
    eval "$(pyenv init -)" && \
    pyenv install 3.8.14 && \
    pyenv virtualenv 3.8.14 snowpark_env && \
    pyenv activate snowpark_env && \
    pip install --no-cache-dir --upgrade pip && \ 
    pip install --no-cache-dir 'snowflake-snowpark-python[pandas]'