from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import requests
import json
import duckdb

# --- CONFIGURAÇÕES ---
CONEXAO_MINIO = "minio_conn"
BUCKET_BRONZE = "bronze"
BUCKET_SILVER = "silver"
BUCKET_GOLD = "gold"

ARQUIVO_CAMBIO = "cambio_raw.json"
ARQUIVO_INDICADORES = "indicadores_raw.json"
ARQUIVO_FINAL = "economia_unificada.parquet"

MINIO_ENDPOINT = "minio-datalake:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"

# Códigos das Séries (IDs fixos do BCB)
COD_DOLAR = 1
COD_EURO = 21619
COD_IPCA = 433
COD_SELIC = 432

# --- HELPER 1: Gerador de URL por Data (Pula o limite de 20 itens) ---
def gerar_url(codigo_serie, anos_historico):
    """
    Calcula data inicial e final para pegar grandes volumes de dados.
    CORREÇÃO: Adicionado 'bcdata.sgs.' na URL.
    """
    data_fim = datetime.now()
    data_inicio = data_fim - timedelta(days=anos_historico * 365)
    
    fmt = "%d/%m/%Y"
    str_inicio = data_inicio.strftime(fmt)
    str_fim = data_fim.strftime(fmt)
    
    # URL Corrigida com o prefixo 'bcdata.sgs.'
    return f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_serie}/dados?formato=json&dataInicial={str_inicio}&dataFinal={str_fim}"

# --- HELPER 2: Salvar no MinIO ---
def salvar_no_minio(dados_dict, nome_arquivo):
    hook = S3Hook(aws_conn_id=CONEXAO_MINIO)
    hook.load_string(
        string_data=json.dumps(dados_dict, indent=4),
        key=nome_arquivo,
        bucket_name=BUCKET_BRONZE,
        replace=True
    )

# --- HELPER 3: Validar API ---
def pegar_dados_validos(url, nome_indicador):
    print(f"Baixando {nome_indicador} via URL: {url}")
    response = requests.get(url)
    response.raise_for_status()
    dados = response.json()
    
    if not isinstance(dados, list):
        # Se vier erro dentro do JSON (mesmo com status 200)
        raise ValueError(f" Erro na API do BCB para {nome_indicador}: {dados}")
        
    return dados

# --- 1.A TAREFA: Ingestão de Câmbio ---
def ingestao_cambio():
    # Pegando 1 ano de histórico
    url_dolar = gerar_url(COD_DOLAR, anos_historico=1)
    url_euro = gerar_url(COD_EURO, anos_historico=1)
    
    dolar = pegar_dados_validos(url_dolar, "Dolar")
    euro = pegar_dados_validos(url_euro, "Euro")
    
    payload = { "dolar": dolar, "euro": euro }
    salvar_no_minio(payload, ARQUIVO_CAMBIO)
    print(f">>> Sucesso! Baixados {len(dolar)} registros de Dólar.")

# --- 1.B TAREFA: Ingestão de Indicadores ---
def ingestao_indicadores():
    # Pegando 4 anos de histórico para ver a curva
    url_ipca = gerar_url(COD_IPCA, anos_historico=4)
    url_selic = gerar_url(COD_SELIC, anos_historico=4)
    
    ipca = pegar_dados_validos(url_ipca, "IPCA")
    selic = pegar_dados_validos(url_selic, "Selic")
    
    payload = { "ipca": ipca, "selic": selic }
    salvar_no_minio(payload, ARQUIVO_INDICADORES)
    print(f">>> Sucesso! Baixados {len(selic)} registros de Selic.")

# --- 2. TAREFA: Transformação Silver ---
def transformacao_silver():
    print(">>> DuckDB: Processando Silver...")
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_region='us-east-1'; SET s3_endpoint='{MINIO_ENDPOINT}';
        SET s3_access_key_id='{MINIO_ACCESS_KEY}'; SET s3_secret_access_key='{MINIO_SECRET_KEY}';
        SET s3_use_ssl=false; SET s3_url_style='path';
    """)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    arquivo_silver = f"economia_{timestamp}.parquet"
    path_destino = f"s3://{BUCKET_SILVER}/{arquivo_silver}"
    path_cambio = f"s3://{BUCKET_BRONZE}/{ARQUIVO_CAMBIO}"
    path_indicadores = f"s3://{BUCKET_BRONZE}/{ARQUIVO_INDICADORES}"
    
    # CAST EXPLÍCITO (Mantido pois funciona perfeitamente com listas grandes)
    df = con.execute(f"""
        SELECT 'dolar' as ativo, 'moeda' as categoria, 
               strptime(obj.data, '%d/%m/%Y') as data_ref, 
               CAST(obj.valor AS DOUBLE) as valor, now() as data_processamento
        FROM read_json_auto('{path_cambio}') as source, 
             UNNEST(source.dolar::STRUCT(data VARCHAR, valor VARCHAR)[]) as t(obj)
        UNION ALL
        SELECT 'euro' as ativo, 'moeda' as categoria, 
               strptime(obj.data, '%d/%m/%Y') as data_ref, 
               CAST(obj.valor AS DOUBLE) as valor, now() as data_processamento
        FROM read_json_auto('{path_cambio}') as source, 
             UNNEST(source.euro::STRUCT(data VARCHAR, valor VARCHAR)[]) as t(obj)
        UNION ALL
        SELECT 'ipca' as ativo, 'indicador' as categoria, 
               strptime(obj.data, '%d/%m/%Y') as data_ref, 
               CAST(obj.valor AS DOUBLE) as valor, now() as data_processamento
        FROM read_json_auto('{path_indicadores}') as source, 
             UNNEST(source.ipca::STRUCT(data VARCHAR, valor VARCHAR)[]) as t(obj)
        UNION ALL
        SELECT 'selic' as ativo, 'indicador' as categoria, 
               strptime(obj.data, '%d/%m/%Y') as data_ref, 
               CAST(obj.valor AS DOUBLE) as valor, now() as data_processamento
        FROM read_json_auto('{path_indicadores}') as source, 
             UNNEST(source.selic::STRUCT(data VARCHAR, valor VARCHAR)[]) as t(obj)
    """).df()
    
    con.execute(f"COPY (SELECT * FROM df) TO '{path_destino}' (FORMAT PARQUET);")

# --- 3. TAREFA: Gold ---
def consolidacao_gold():
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_region='us-east-1'; SET s3_endpoint='{MINIO_ENDPOINT}';
        SET s3_access_key_id='{MINIO_ACCESS_KEY}'; SET s3_secret_access_key='{MINIO_SECRET_KEY}';
        SET s3_use_ssl=false; SET s3_url_style='path';
    """)
    
    # Filtra apenas economia_*.parquet
    path_origem = f"s3://{BUCKET_SILVER}/economia_*.parquet"
    path_destino = f"s3://{BUCKET_GOLD}/{ARQUIVO_FINAL}"
    
    con.execute(f"""
        COPY (
            SELECT DISTINCT * FROM read_parquet('{path_origem}')
            ORDER BY data_ref DESC
        ) TO '{path_destino}' (FORMAT PARQUET);
    """)

# --- DAG ---
with DAG(
    dag_id="elt_economia_bcb",
    start_date=datetime(2023, 1, 1),
    schedule_interval="0 9 * * 1-5",
    catchup=False,
    tags=["BCB", "Economia", "BigData"]
) as dag:

    t_cambio = PythonOperator(task_id="ingestao_cambio", python_callable=ingestao_cambio)
    t_indicadores = PythonOperator(task_id="ingestao_indicadores", python_callable=ingestao_indicadores)
    t_silver = PythonOperator(task_id="transformacao_silver", python_callable=transformacao_silver)
    t_gold = PythonOperator(task_id="consolidacao_gold", python_callable=consolidacao_gold)

    [t_cambio, t_indicadores] >> t_silver >> t_gold