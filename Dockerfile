FROM apache/airflow:2.2.3
COPY temp/.  /opt/airflow/config/.
COPY requirements.txt /opt/airflow/.
RUN /home/airflow/.local/bin/pip3.7 install -r /opt/airflow/requirements.txt
RUN /home/airflow/.local/bin/pip3.7 install pyOpenSSL --upgrade
ENV PYTHONPATH=/opt/airflow