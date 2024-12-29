FROM apache/airflow:2.10.4-python3.11

COPY ./requirements.txt /opt/airflow

RUN pip install "apache-airflow==2.10.4" --requirement requirements.txt
