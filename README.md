# Lakehouse: Monitor Econômico do Brasil (End-to-End)

Este projeto implementa um pipeline completo de Engenharia de Dados (**ELT**), utilizando uma arquitetura **Data Lakehouse** baseada em Containers. 

O objetivo é ingerir dados oficiais do **Banco Central do Brasil (BCB)**, processar séries históricas de Câmbio e Indicadores Macroeconômicos (IPCA, Selic) e disponibilizar um dashboard analítico para tomada de decisão.

##  Arquitetura do Projeto

O projeto segue a **Medallion Architecture** (Bronze, Silver, Gold):

1.  **Ingestão (Bronze):** O **Apache Airflow** consulta a API de Dados Abertos do BCB (SGS), baixa séries históricas longas (Dólar, Euro, Selic, IPCA) e salva os arquivos JSON brutos no **MinIO** (Data Lake).
2.  **Transformação (Silver):** O **DuckDB** lê os JSONs complexos, utiliza `UNNEST` para explodir as listas aninhadas, aplica tipagem forte (Casting) e salva o histórico particionado em **Parquet**.
3.  **Consolidação (Gold):** O **DuckDB** unifica os arquivos históricos em uma tabela analítica otimizada (`economia_unificada.parquet`).
4.  **Visualização:** O **Streamlit** consome a camada Gold para gerar gráficos de tendência (Inflação vs Juros) e KPIs de Câmbio.

##  Tech Stack

* **Orquestração:** Apache Airflow 2.10
* **Fonte de Dados:** API Banco Central do Brasil (SGS)
* **Storage (Data Lake):** MinIO (S3 Compatible)
* **Processamento:** DuckDB (In-memory SQL OLAP)
* **Visualização:** Streamlit + Plotly
* **Infraestrutura:** Docker & Docker Compose

##  Como Rodar

### Pré-requisitos
* Docker e Docker Compose instalados.

### Passo a Passo

1.  Clone o repositório:
    ```bash
    git clone [https://github.com/Matheuslagos/tcc-lakehouse.git](https://github.com/Matheuslagos/tcc-lakehouse.git)
    cd tcc-lakehouse
    ```

2.  Suba o ambiente (Build & Run):
    ```bash
    docker compose up --build -d
    ```

3.  Acesse os serviços:

| Serviço | URL | Credenciais (User/Pass) |
|---|---|---|
| **Airflow** | `http://localhost:8080` | `admin` / `admin` |
| **MinIO** | `http://localhost:9001` | `admin` / `password123` |
| **Dashboard** | `http://localhost:8501` | *(Acesso Livre)* |

4.  No Airflow, ative a DAG **`elt_economia_bcb`**.
    * *Nota:* Ela está configurada para rodar diariamente (dias úteis), mas você pode executar manualmente (Trigger) para carga inicial.

##  Estrutura de Pastas

```text
├── dags/                  
│   └── elt_valores_economicos.py  # Pipeline ELT (Extração BCB -> Silver -> Gold)
├── dashboard.py           # Aplicação Streamlit (KPIs e Gráficos Macro)
├── docker-compose.yaml    # Definição da Infraestrutura
├── Dockerfile             # Imagem Customizada do Airflow
├── Dockerfile.streamlit   # Imagem Customizada do Dashboard
└── requirements.txt       # Dependências Python
