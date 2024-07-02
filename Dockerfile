FROM apache/airflow:2.9.2

COPY config/. /opt/airflow/config/.
COPY requirements.txt /opt/airflow/.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /opt/airflow/requirements.txt
ENV PYTHONPATH=/opt/airflow