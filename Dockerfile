FROM apache/airflow:3.2.2

USER airflow

COPY --chown=airflow:root requirements.txt /opt/airflow/requirements.txt

RUN pip install --no-cache-dir \
    -r /opt/airflow/requirements.txt

# dags/ and data/ are mounted by docker-compose during local development.
