FROM apache/airflow:2.10.2-python3.11

COPY requirements.txt /requirements.txt

USER airflow

ENV AIRFLOW_VERSION=2.10.2
ENV PYTHON_VERSION=3.11
ENV PATH="/home/airflow/.local/bin:${PATH}"

RUN pip install --no-cache-dir \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
  -r /requirements.txt
