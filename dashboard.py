import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import os

# --- 1. Configuração da Página ---
st.set_page_config(
    page_title="Monitor Econômico Brasil",
    page_icon="🇧🇷",
    layout="wide"
)

st.title("🇧🇷 Monitor Econômico (Dados Oficiais BCB)")
st.markdown("Dashboard analítico consumindo dados da **API do Banco Central** processados via Data Lakehouse.")

# --- 2. Configurações de Conexão (MinIO) ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio-datalake:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
BUCKET_GOLD = "gold"
# Note que agora apontamos para o NOVO arquivo gerado pela nova DAG
ARQUIVO_FINAL = "economia_unificada.parquet"

# --- 3. Função de Carga de Dados ---
@st.cache_data(ttl=5) # Cache curto para pegar atualizações rápido
def carregar_dados():
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")
    
    endpoint_limpo = MINIO_ENDPOINT.replace("http://", "")
    
    con.execute(f"""
        SET s3_region='us-east-1';
        SET s3_endpoint='{endpoint_limpo}';
        SET s3_access_key_id='{MINIO_ACCESS_KEY}';
        SET s3_secret_access_key='{MINIO_SECRET_KEY}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    
    try:
        # Lê o arquivo unificado
        df = con.execute(f"SELECT * FROM read_parquet('s3://{BUCKET_GOLD}/{ARQUIVO_FINAL}')").df()
        # Garante que a data seja datetime
        df['data_ref'] = pd.to_datetime(df['data_ref'])
        return df.sort_values(by='data_ref')
    except Exception as e:
        return None

# --- 4. Interface Visual ---
# --- 4. Interface Visual ---
df = carregar_dados()

if df is None or df.empty:
    st.warning("⚠️ Aguardando dados... Verifique se o Pipeline Airflow rodou com sucesso.")
    if st.button("Tentar recarregar"):
        st.rerun()
else:
    # --- LÓGICA CORRIGIDA DOS KPIS ---
    # Em vez de pegar a data máxima global, pegamos a máxima DE CADA ATIVO.
    
    def pegar_ultimo_valor(nome_ativo):
        try:
            # Filtra apenas o ativo desejado
            df_ativo = df[df['ativo'] == nome_ativo]
            # Ordena do mais recente para o mais antigo e pega o primeiro
            valor = df_ativo.sort_values(by='data_ref', ascending=False)['valor'].iloc[0]
            return valor
        except IndexError:
            return 0.0

    dolar_hoje = pegar_ultimo_valor('dolar')
    euro_hoje = pegar_ultimo_valor('euro')
    selic_hoje = pegar_ultimo_valor('selic')
    ipca_hoje = pegar_ultimo_valor('ipca')

    # --- LINHA DE DESTAQUES (KPIs) ---
    col1, col2, col3, col4 = st.columns(4)
    
    # Adicionei uma lógica de cor: se for 0.0, fica cinza (erro), senão fica normal
    col1.metric("💵 Dólar (PTAX)", f"R$ {dolar_hoje:.4f}")
    col2.metric("💶 Euro", f"R$ {euro_hoje:.4f}")
    col3.metric("📉 IPCA (12m)", f"{ipca_hoje:.2f}%")
    col4.metric("🏦 Selic Meta", f"{selic_hoje:.2f}%")

    st.divider()

    # --- ABAS PARA GRÁFICOS ---
    tab1, tab2 = st.tabs(["📊 Câmbio (Moedas)", "🏗️ Macroeconomia"])

    with tab1:
        st.subheader("Evolução Dólar vs Euro (Último Ano)")
        df_moedas = df[df['categoria'] == 'moeda']
        
        fig_cambio = px.line(
            df_moedas, 
            x='data_ref', 
            y='valor', 
            color='ativo',
            title="Histórico de Cotações",
            color_discrete_map={"dolar": "#00CC96", "euro": "#636EFA"}
        )
        fig_cambio.update_yaxes(title="Valor (R$)")
        st.plotly_chart(fig_cambio, use_container_width=True)

    with tab2:
        st.subheader("Inflação vs Juros (Ciclos Econômicos)")
        df_macro = df[df['categoria'] == 'indicador']
        
        # Selic é linha, IPCA é barra (Visual melhor para macro)
        # Como o Plotly Express é simples, vamos fazer duas linhas ou barras agrupadas
        fig_macro = px.line(
            df_macro,
            x='data_ref',
            y='valor',
            color='ativo',
            title="Selic (Juros) vs IPCA (Inflação)",
            color_discrete_map={"ipca": "#EF553B", "selic": "#AB63FA"}
        )
        fig_macro.update_yaxes(title="Taxa (%)")
        st.plotly_chart(fig_macro, use_container_width=True)

    with st.expander("Ver Tabela de Dados Brutos"):
        st.dataframe(df.sort_values(by=['data_ref', 'ativo'], ascending=False), use_container_width=True)