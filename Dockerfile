#imagem oficial do Airflow
FROM apache/airflow:2.10.2

#arquivo de requisitos para dentro da imagem
COPY requirements.txt .

#bibliotecas extras (DuckDB, S3, etc)
RUN pip install --no-cache-dir -r requirements.txt