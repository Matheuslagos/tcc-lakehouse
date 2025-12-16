from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import duckdb
import boto3

# --- CONFIGURAÇÕES ---
MINIO_ENDPOINT = "http://minio-datalake:9000" # Atenção ao http://
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"

def calcular_metricas_lakehouse():
    print(f"{'CAMADA':<10} | {'TIPO':<10} | {'REGISTROS':<10} | {'TAMANHO (MB)':<10}")
    print("-" * 60)

    # 1. Configura cliente S3 (Boto3) para calcular TAMANHO
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        verify=False
    )

    # 2. Configura DuckDB para contar LINHAS
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_region='us-east-1'; 
        SET s3_endpoint='minio-datalake:9000';
        SET s3_access_key_id='{MINIO_ACCESS_KEY}'; 
        SET s3_secret_access_key='{MINIO_SECRET_KEY}';
        SET s3_use_ssl=false; 
        SET s3_url_style='path';
    """)

    camadas = [
        {'nome': 'bronze', 'formato': 'json', 'ext': 'json'},
        {'nome': 'silver', 'formato': 'parquet', 'ext': 'parquet'},
        {'nome': 'gold',   'formato': 'parquet', 'ext': 'parquet'}
    ]

    for camada in camadas:
        bucket = camada['nome']
        extensao = camada['ext']
        
        # --- A. Calcular Tamanho (Boto3) ---
        try:
            response = s3_client.list_objects_v2(Bucket=bucket)
            tamanho_bytes = 0
            if 'Contents' in response:
                # Soma o tamanho de todos os arquivos do bucket
                tamanho_bytes = sum(item['Size'] for item in response['Contents'])
            
            tamanho_mb = tamanho_bytes / (1024 * 1024)
        except Exception as e:
            print(f"Erro ao ler tamanho do bucket {bucket}: {e}")
            tamanho_mb = 0.0

        # --- B. Contar Registros (DuckDB) ---
        try:
            path_s3 = f"s3://{bucket}/*.{extensao}"
            
            if camada['formato'] == 'json':
                # ignore_errors=true ajuda se tiver algum json malformado
                query = f"SELECT COUNT(*) FROM read_json_auto('{path_s3}', ignore_errors=true)"
            else:
                query = f"SELECT COUNT(*) FROM read_parquet('{path_s3}')"
            
            # Se o bucket estiver vazio, o DuckDB pode lançar erro, tratamos aqui
            if tamanho_bytes > 0:
                qtd_registros = con.execute(query).fetchone()[0]
            else:
                qtd_registros = 0
                
        except Exception as e:
            # Comum acontecer se não houver arquivos (File not found)
            qtd_registros = 0

        # Imprime na linha formatada
        print(f"{bucket.capitalize():<10} | {camada['formato'].upper():<10} | {qtd_registros:<10} | {tamanho_mb:.2f}")

# --- DEFINIÇÃO DA DAG ---
with DAG(
    dag_id="auditoria_lakehouse",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None, # Roda apenas manualmente
    catchup=False,
    tags=["Metricas", "TCC"]
) as dag:

    task_metricas = PythonOperator(
        task_id="calcular_metricas",
        python_callable=calcular_metricas_lakehouse
    )

    task_metricas