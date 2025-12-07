# 🪙 Crypto Lakehouse: Arquitetura de Dados Moderna (End-to-End)

Este projeto implementa um pipeline completo de Engenharia de Dados (**ELT**), utilizando uma arquitetura **Data Lakehouse** baseada em Containers. O objetivo é ingerir dados de criptomoedas em tempo real, processá-los e disponibilizar visualizações analíticas.

## 🏗️ Arquitetura do Projeto

O projeto segue a **Medallion Architecture** (Bronze, Silver, Gold):

1.  **Ingestão (Bronze):** O **Apache Airflow** extrai dados da API CoinGecko e salva o JSON bruto no **MinIO** (Data Lake).
2.  **Transformação (Silver):** O **DuckDB** lê o JSON, normaliza os dados, adiciona metadados e salva histórico particionado em **Parquet**.
3.  **Consolidação (Gold):** O **DuckDB** unifica os arquivos históricos em uma tabela única otimizada para leitura.
4.  **Visualização:** O **Streamlit** consome a camada Gold para gerar dashboards interativos em tempo real.

## 🛠️ Tech Stack

* **Orquestração:** Apache Airflow 2.10
* **Storage (Data Lake):** MinIO (S3 Compatible)
* **Processamento:** DuckDB (In-memory SQL OLAP)
* **Visualização:** Streamlit + Plotly
* **Infraestrutura:** Docker & Docker Compose

## 🚀 Como Rodar

### Pré-requisitos
* Docker e Docker Compose instalados.

### Passo a Passo

1.  Clone o repositório:
    ```bash
    git clone [https://github.com/SEU_USUARIO/SEU_REPO.git](https://github.com/SEU_USUARIO/SEU_REPO.git)
    cd SEU_REPO
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

4.  No Airflow, ative a DAG `elt`. Ela rodará a cada 10 minutos.

## 📊 Estrutura de Pastas

```text
├── dags/                  # Pipelines do Airflow (Python)
├── dashboard.py           # Aplicação Streamlit
├── docker-compose.yaml    # Definição da Infraestrutura
├── Dockerfile             # Imagem Customizada do Airflow
├── Dockerfile.streamlit   # Imagem Customizada do Dashboard
└── requirements.txt       # Dependências Python
```

