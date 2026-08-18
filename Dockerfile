FROM congtvjits/airflow-dbt-3.0.2:v1.2
# kafka-python: cob_gate nhánh B (lag = kafka_end_offset − spark_committed_offset)
RUN pip install --no-cache-dir "kafka-python>=2.0.2"
COPY --chown=airflow:root . /opt/airflow/dags/repo/
