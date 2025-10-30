FROM apache/airflow:2.6.2
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user -r /requirements.txt