# config.py

import os
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Carrega as variáveis definidas no arquivo .env
load_dotenv()

# ====================================================
# 1. DATAS FIXAS PARA CARGA HISTÓRICA
# ====================================================
# Ajuste conforme necessário. 
# Nota: START_DATE_HISTORICAL define o início da carga histórica para tabelas transacionais.
START_DATE_HISTORICAL = datetime(2050, 1, 1) 
END_DATE_HISTORICAL = datetime(2050, 1, 1) 
DAYS_FOR_RECENT_REFRESH = 10 # Reprocessamento dos últimos N dias para Transacionais

# ====================================================
# 2. CONFIGURAÇÕES GCP
# ====================================================
GCP_CONFIG = {
    "PROJECT_ID": os.getenv("GCP_PROJECT_ID"), 
    "DATASET_ID": os.getenv("GCP_DATASET_ID"),
}

# Lógica segura para carregar a Service Account
path_to_sa = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

if path_to_sa and os.path.exists(path_to_sa):
    try:
        with open(path_to_sa, "r") as f:
            SERVICE_ACCOUNT_JSON = json.load(f)
    except Exception as e:
        raise ValueError(f"Erro ao ler o arquivo de credenciais JSON em '{path_to_sa}': {e}")
else:
    # Se a variável não estiver definida ou o arquivo não existir, gera erro explicativo
    if not path_to_sa:
        raise ValueError("A variável de ambiente 'GOOGLE_APPLICATION_CREDENTIALS' não está definida no .env")
    else:
        raise FileNotFoundError(f"Arquivo de credenciais não encontrado no caminho: {path_to_sa}")

# ====================================================
# 3. CONFIGURAÇÕES DA API CISSPoder
# ====================================================
API_CONFIG = {
    "BASE_URL_AUTH": os.getenv("API_BASE_URL_AUTH"),
    "BASE_URL_SERVICE": os.getenv("API_BASE_URL_SERVICE"),
    "USERNAME": os.getenv("API_USERNAME"),
    "PASSWORD": os.getenv("API_PASSWORD"),
    "CLIENT_ID": os.getenv("API_CLIENT_ID"),
    "CLIENT_SECRET": os.getenv("API_CLIENT_SECRET"),
    "GRANT_TYPE": os.getenv("API_GRANT_TYPE")
}

# ====================================================
# 4. MAPA DE SERVIÇOS (api_name em minúsculas)
# ====================================================
SERVICE_MAP = {
    # Cadastrais (FULL LOAD - TRUNCATE)
    "CAD_LOJAS": {"api_name": "cad_lojas", "filter_field": None, "load_mode": "WRITE_TRUNCATE"},
    "CAD_PESSOAS": {"api_name": "cad_pessoas", "filter_field": None, "load_mode": "WRITE_TRUNCATE"},
    "CAD_PRODUTOS": {"api_name": "cad_produtos", "filter_field": None, "load_mode": "WRITE_TRUNCATE"},
    "PRODUTOS_SALDO_ESTOQUE_EMPRESA": {"api_name": "produtos_saldo_estoque_empresa", "filter_field": None, "load_mode": "WRITE_TRUNCATE"},
    
    # Transacionais (INCREMENTAL-HISTÓRICO & N-DAY REFRESH)
    "DOCUMENTOS_FISCAIS_ENTRADA": {"api_name": "documentos_fiscais_entrada", "filter_field": "dtmovimento", "load_mode": "WRITE_APPEND"},
    "DOCUMENTOS_FISCAIS_SAIDA": {"api_name": "documentos_fiscais_saida", "filter_field": "dtmovimento", "load_mode": "WRITE_APPEND"},
    "ITENS_DOCUMENTOS_FISCAIS_ENTRADA": {"api_name": "itens_documentos_fiscais_entrada", "filter_field": "dtmovimento", "load_mode": "WRITE_APPEND"},
    "ITENS_DOCUMENTOS_FISCAIS_SAIDA": {"api_name": "itens_documentos_fiscais_saida", "filter_field": "dtmovimento", "load_mode": "WRITE_APPEND"},
    "RECEBIMENTOS_DOCUMENTOS_FISCAIS_SAIDA": {"api_name": "recebimentos_documentos_fiscais_saida", "filter_field": "dtmovimento", "load_mode": "WRITE_APPEND"},
    "PAGAMENTOS_DOCUMENTOS_FISCAIS_ENTRADA": {"api_name": "pagamentos_documentos_fiscais_entrada", "filter_field": "dtmovimento", "load_mode": "WRITE_APPEND"},
}

# Colunas de data para normalização do Pandas
DATE_COLUMNS = [
    'DTALTERACAO', 'DTNASCIMENTO', 'DTCADASTRO', 'DTEMISSAO', 'DTMOVIMENTO', 
    'DTRECEBIMENTO', 'DTPAGAMENTO', 'DTVENCIMENTO', 'DTINICIOTABELA', 'DTFIMTABELA'
]

# ====================================================
# 5. FUNÇÕES AUXILIARES (Preservadas)
# ====================================================

def get_monthly_ranges(start_date, end_date):
    """ Retorna uma lista de tuplas (start, end) para carregar dados mês a mês. """
    ranges = []
    current = start_date
    
    while current <= end_date:
        if current.month == 12:
            next_month = datetime(current.year + 1, 1, 1)
        else:
            next_month = datetime(current.year, current.month + 1, 1)
            
        last_day_of_month = next_month - timedelta(days=1)
        batch_end = min(last_day_of_month, end_date)
        
        ranges.append((current, batch_end))
        current = next_month
        
        if current > end_date:
            break
            
    return ranges

def get_daily_ranges(start_date: datetime, end_date: datetime) -> list[tuple[datetime, datetime]]:
    """ 
    Retorna uma lista de tuplas (start, end) para carregar dados dia a dia. 
    """
    ranges = []
    current_day = start_date
    
    while current_day <= end_date:
        batch_end = current_day
        ranges.append((current_day, batch_end))
        current_day += timedelta(days=1)
        
    return ranges

def get_custom_day_ranges(start_date: datetime, end_date: datetime, days_in_batch: int = 10) -> list[tuple[datetime, datetime]]:
    """ 
    Retorna uma lista de tuplas (start, end) para carregar dados em lotes de 'days_in_batch' dias.
    """
    ranges = []
    current_start = start_date
    
    while current_start <= end_date:
        calculated_end = current_start + timedelta(days=days_in_batch - 1)
        batch_end = min(calculated_end, end_date)
        
        ranges.append((current_start, batch_end))
        current_start = batch_end + timedelta(days=1)
        
    return ranges