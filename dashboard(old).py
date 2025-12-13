import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import os

# --- 1. Configuração da Página ---
st.set_page_config(
    page_title="Crypto Lakehouse",
    page_icon="🪙",
    layout="wide"
)

st.title("🪙 Monitoramento de Criptomoedas")
st.markdown("""
Esta aplicação consome dados da **Camada Gold** do Data Lake (MinIO).
O pipeline **Airflow** extrai, transforma e consolida os dados a cada 10 minutos.
""")

# --- 2. Configurações de Conexão (MinIO) ---
# variáveis de ambiente ou valores padrão para Docker
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio-datalake:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
BUCKET_GOLD = "gold"
ARQUIVO_FINAL = "historico_unificado.parquet"

# --- 3. Função de Carga de Dados (DuckDB) ---
# TTL=2 significa que o cache expira em 2 segundos.
# Isso garante que sempre que você der Refresh (R), verá dados novos.
@st.cache_data(ttl=2)
def carregar_dados():
    # Conecta no DuckDB em memória
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")
    
    # Configura credenciais para ler do MinIO
    # O .replace é para garantir que o endpoint fique sem 'http://' para o DuckDB
    endpoint_limpo = MINIO_ENDPOINT.replace("http://", "")
    
    con.execute(f"""
        SET s3_region='us-east-1';
        SET s3_endpoint='{endpoint_limpo}';
        SET s3_access_key_id='{MINIO_ACCESS_KEY}';
        SET s3_secret_access_key='{MINIO_SECRET_KEY}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    
    # Lê o arquivo consolidado da camada GOLD
    print("Lendo dados do Data Lake...")
    query = f"SELECT * FROM read_parquet('s3://{BUCKET_GOLD}/{ARQUIVO_FINAL}')"
    
    # Traz para Pandas e ordena por data
    df = con.execute(query).df().sort_values(by='data_hora_processamento')
    return df

# --- 4. Interface Visual ---
try:
    # Tenta carregar os dados
    df = carregar_dados()

    # --- Seção de KPIs (Indicadores) ---
    st.subheader("📌 Cotação Mais Recente")
    col1, col2, col3 = st.columns(3)
    
    # Pega o último registro de cada moeda
    ultimo_dado = df.sort_values(by='data_hora_processamento', ascending=False)
    btc_price = ultimo_dado[ultimo_dado['moeda'] == 'bitcoin']['preco_usd'].iloc[0]
    eth_price = ultimo_dado[ultimo_dado['moeda'] == 'ethereum']['preco_usd'].iloc[0]
    dt_atualizacao = ultimo_dado['data_hora_processamento'].iloc[0]

    col1.metric("Bitcoin (BTC)", f"${btc_price:,.2f}")
    col2.metric("Ethereum (ETH)", f"${eth_price:,.2f}")
    col3.metric("Última Atualização", dt_atualizacao.strftime('%H:%M:%S'))

    st.markdown("---")

    # --- Seção de Gráficos (Separados por Moeda) ---
    st.subheader("📈 Tendência de Preços (Séries Temporais)")

    # Pega lista única de moedas (Bitcoin, Ethereum)
    moedas = df['moeda'].unique()

    # Cria colunas dinâmicas para os gráficos ficarem lado a lado
    cols = st.columns(len(moedas))

    for i, moeda in enumerate(moedas):
        with cols[i]:
            # Filtra apenas os dados daquela moeda
            df_moeda = df[df['moeda'] == moeda]
            
            # Cria o gráfico
            fig = px.line(
                df_moeda, 
                x='data_hora_processamento', 
                y='preco_usd', 
                markers=True,
                title=f"Evolução: {moeda.capitalize()}",
                template="plotly_dark",
                # Personaliza a tooltip (caixinha que aparece ao passar o mouse)
                hover_data={"data_hora_processamento": "|%H:%M:%S"}
            )
            
            fig.update_yaxes(autorange=True, title="Preço (USD)")
            fig.update_xaxes(title="Horário")
            
            st.plotly_chart(fig, use_container_width=True)

    # --- Seção de Dados Brutos ---
    with st.expander("🔍 Ver Dados Brutos (Camada Gold)"):
        st.dataframe(df, use_container_width=True)

except Exception as e:
    # Caso o arquivo ainda não exista (pipeline não rodou nenhuma vez)
    st.warning("⚠️ Aguardando dados... O Pipeline Airflow ainda está gerando o arquivo Gold.")
    st.info(f"Detalhe técnico: {e}")
    
    if st.button("Tentar recarregar agora"):
        st.rerun()