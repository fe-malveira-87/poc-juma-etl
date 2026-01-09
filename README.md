# 🚀 ETL - Integração CISSPoder para BigQuery

Pipeline de dados desenvolvido em Python para extrair dados da API **CISSPoder** e carregar no **Google BigQuery** (Camada RAW). O projeto utiliza processamento paralelo, gerenciamento de dependências com **uv** e práticas de segurança para credenciais.

## 📋 Funcionalidades

* **Arquitetura Unificada:** Ponto único de entrada via `main.py`.
* **Paralelismo:** Execução simultânea de múltiplos ETLs usando `ProcessPoolExecutor`.
* **Segurança:** Credenciais gerenciadas via variáveis de ambiente (`.env`) e arquivos ignorados pelo Git.
* **Idempotência:** Garante consistência (DELETE + INSERT) para cargas incrementais.
* **Gestão de Token:** Sistema de cache de autenticação com TTL.
* **Tipos de Carga:**
    * *Cadastrais:* Carga Full (Write Truncate).
    * *Transacionais:* Carga Histórica + Refresh Recente (Write Append com limpeza de range).

## 🛠️ Pré-requisitos

* **Python:** 3.10+
* **Gerenciador:** [uv](https://github.com/astral-sh/uv) (Astral)
* **GCP:** Service Account com permissão de `BigQuery Data Editor`.

## ⚙️ Configuração e Segurança

Este projeto não armazena credenciais no código. Antes de rodar, configure o ambiente:

### 1. Variáveis de Ambiente (.env)
Crie um arquivo `.env` na raiz do projeto seguindo este modelo:

API_BASE_URL_AUTH=[https://cliente.dataciss.com.br:4665/cisspoder-auth/oauth/token](https://jumacim.dataciss.com.br:4665/cisspoder-auth/oauth/token)
API_BASE_URL_SERVICE=[https://cliente.dataciss.com.br:4665/cisspoder-service/](https://jumacim.dataciss.com.br:4665/cisspoder-service/)
API_USERNAME=seu_usuario
API_PASSWORD=sua_senha
API_CLIENT_ID=cisspoder-oauth
API_CLIENT_SECRET=seu_secret
API_GRANT_TYPE=password

GCP_PROJECT_ID=seu-projeto-gcp
GCP_DATASET_ID=RAW_JUMA
GOOGLE_APPLICATION_CREDENTIALS=credentials/service_account.json
