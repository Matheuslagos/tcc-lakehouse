from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import requests
import json
import duckdb

# --- CONFIGURAÇÕES ---
# Conexão Airflow
CONEXAO_MINIO = "minio_conn"

# Buckets e Arquivos
BUCKET_BRONZE = "bronze"
ARQUIVO_RAW = "cotacao_cripto.json"

BUCKET_SILVER = "silver"
# O nome na silver é dinâmico, então não definimos fixo aqui

BUCKET_GOLD = "gold"
ARQUIVO_FINAL = "historico_unificado.parquet"

# Configurações DuckDB / MinIO
MINIO_ENDPOINT = "minio-datalake:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
URL_API = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd"

# --- 1. BRONZE (EXTRACT & LOAD) ---
def ingestao_bronze():
    print(f"1. Baixando dados: {URL_API}")
    resposta = requests.get(URL_API)
    dados = resposta.json()
    
    print("2. Salvando na Bronze...")
    hook = S3Hook(aws_conn_id=CONEXAO_MINIO)
    hook.load_string(
        string_data=json.dumps(dados, indent=4),
        key=ARQUIVO_RAW,
        bucket_name=BUCKET_BRONZE,
        replace=True
    )

# --- 2. SILVER (TRANSFORM & APPEND) ---
def transformacao_silver():
    print("3. DuckDB: Processando Silver...")
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_region='us-east-1'; SET s3_endpoint='{MINIO_ENDPOINT}';
        SET s3_access_key_id='{MINIO_ACCESS_KEY}'; SET s3_secret_access_key='{MINIO_SECRET_KEY}';
        SET s3_use_ssl=false; SET s3_url_style='path';
    """)
    
    # Gera nome único com timestamp (ex: cotacao_20251207_1530.parquet)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    arquivo_silver = f"cotacao_{timestamp}.parquet"
    
    path_origem = f"s3://{BUCKET_BRONZE}/{ARQUIVO_RAW}"
    path_destino = f"s3://{BUCKET_SILVER}/{arquivo_silver}"
    
    print(f"4. Lendo {path_origem} e gerando partição {arquivo_silver}")
    
    df = con.execute(f"""
        SELECT 'bitcoin' as moeda, bitcoin.usd as preco_usd, 
               current_date as data_extracao, now() as data_hora_processamento
        FROM read_json_auto('{path_origem}')
        UNION ALL
        SELECT 'ethereum' as moeda, ethereum.usd as preco_usd, 
               current_date as data_extracao, now() as data_hora_processamento
        FROM read_json_auto('{path_origem}')
    """).df()
    
    con.execute(f"COPY (SELECT * FROM df) TO '{path_destino}' (FORMAT PARQUET);")

# --- 3. GOLD (CONSOLIDATION) ---
def consolidacao_gold():
    print("5. DuckDB: Iniciando Consolidação Gold...")
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_region='us-east-1'; SET s3_endpoint='{MINIO_ENDPOINT}';
        SET s3_access_key_id='{MINIO_ACCESS_KEY}'; SET s3_secret_access_key='{MINIO_SECRET_KEY}';
        SET s3_use_ssl=false; SET s3_url_style='path';
    """)
    
    # O asterisco (*) pega TODOS os arquivos da Silver
    path_origem_todos = f"s3://{BUCKET_SILVER}/*.parquet"
    path_destino_gold = f"s3://{BUCKET_GOLD}/{ARQUIVO_FINAL}"
    
    print(f"6. Lendo TUDO de {path_origem_todos} e consolidando em {path_destino_gold}")
    
    # Lemos tudo, ordenamos por data e salvamos um único arquivo otimizado
    con.execute(f"""
        COPY (
            SELECT * FROM read_parquet('{path_origem_todos}')
            ORDER BY data_hora_processamento DESC
        ) TO '{path_destino_gold}' (FORMAT PARQUET);
    """)
    print("Sucesso: Arquivo Gold atualizado.")

# --- DAG ---
with DAG(
    dag_id="elt",
    start_date=datetime(2023, 1, 1),
    schedule_interval="*/10 * * * *",
    catchup=False,
    tags=["TCC", "Gold Layer"]
) as dag:

    t1 = PythonOperator(task_id="ingestao_bronze", python_callable=ingestao_bronze)
    t2 = PythonOperator(task_id="transformacao_silver", python_callable=transformacao_silver)
    t3 = PythonOperator(task_id="consolidacao_gold", python_callable=consolidacao_gold)

    # Fluxo Linear: Bronze -> Silver -> Gold
    t1 >> t2 >> t3