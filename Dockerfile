FROM sindb/de-pg-cr-af:latest
RUN sed -i 's/executor = SequentialExecutor/executor = LocalExecutor/' /opt/airflow/airflow.cfg
