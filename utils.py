# utils.py

import requests
import pandas as pd
import json
import logging
import sys
import os 
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
from config import (
    API_CONFIG, GCP_CONFIG, SERVICE_ACCOUNT_JSON, DATE_COLUMNS, 
    get_monthly_ranges, get_daily_ranges, START_DATE_HISTORICAL, END_DATE_HISTORICAL, DAYS_FOR_RECENT_REFRESH
)

# ----------------------------------------------------
# CACHE DE TOKEN E CONFIGURAÇÃO DE TIMEOUT
# ----------------------------------------------------
TOKEN_CACHE = {
    'access_token': None,
    'expires_at': datetime.min, # Inicializa com data mínima para forçar a primeira geração
}
TOKEN_LIFESPAN_MINUTES = 10 # Tempo de vida (TTL) do token em minutos.
# ----------------------------------------------------

# ----------------------------------------------------
# CONFIGURAÇÃO DE LOG DINÂMICO (Por Serviço)
# ----------------------------------------------------
LOG_DIR = 'logs' # Define um diretório para logs
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
    
LOG_LEVEL = logging.INFO
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(name)s - %(message)s'


def setup_service_logger(service_name):
    """ 
    Cria ou recupera um logger configurado para salvar em um arquivo 
    específico ('logs/etl_<SERVICE_NAME>.log') e também na saída do console.
    """
    logger_name = f'ETL_{service_name}'
    logger = logging.getLogger(logger_name) 
    logger.setLevel(LOG_LEVEL)

    # Previne que handlers sejam adicionados múltiplas vezes se a função for chamada novamente
    if logger.handlers:
        return logger
    
    # Handler para arquivo
    log_file_name = f"{LOG_DIR}/etl_{service_name}.log"
    file_handler = logging.FileHandler(log_file_name, mode='a', encoding='utf-8')
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(file_handler)

    # Handler para console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(console_handler)

    logger.info(f"Log de serviço '{service_name}' configurado. Saída também sendo gravada em: {log_file_name}")
    return logger

# Funções de Log de conveniência
def log_info(logger_obj, message):
    logger_obj.info(message)
def log_warning(logger_obj, message):
    logger_obj.warning(message)
def log_error(logger_obj, message):
    logger_obj.error(message)

# ----------------------------------------------------
# FUNÇÃO DE AUTENTICAÇÃO API (AGORA COM CACHE)
# ----------------------------------------------------
def get_auth_token(logger_obj):
    """ 
    Realiza a requisição POST para obter o Token de Autenticação, 
    utilizando cache com TTL de 10 minutos. 
    """
    global TOKEN_CACHE # Indica que você usará a variável global
    
    # 1. Checa o Cache
    # Se o token existir E a data/hora atual for menor que a data de expiração, usa o token em cache.
    if TOKEN_CACHE['access_token'] and datetime.now() < TOKEN_CACHE['expires_at']:
        log_info(logger_obj, "Token de Autenticação reutilizado do cache (ainda válido).")
        return TOKEN_CACHE['access_token']
        
    log_info(logger_obj, "Token de Autenticação expirado ou não existe. Iniciando nova requisição...")

    auth_data = {
        "client_id": API_CONFIG['CLIENT_ID'],
        "grant_type": API_CONFIG['GRANT_TYPE'],
        "client_secret": API_CONFIG['CLIENT_SECRET'],
        "username": API_CONFIG['USERNAME'],
        "password": API_CONFIG['PASSWORD']
    }
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    
    try:
        response = requests.post(API_CONFIG['BASE_URL_AUTH'], data=auth_data, headers=headers)
        response.raise_for_status()
        token_info = response.json()
        access_token = token_info.get("access_token")
        
        if access_token:
            # 2. Atualiza o Cache (com 10 minutos de validade)
            TOKEN_CACHE['access_token'] = access_token
            TOKEN_CACHE['expires_at'] = datetime.now() + timedelta(minutes=TOKEN_LIFESPAN_MINUTES)
            
            log_info(logger_obj, f"Novo Token de Autenticação obtido e válido até: {TOKEN_CACHE['expires_at'].strftime('%Y-%m-%d %H:%M:%S')}")
            return access_token
        else:
            log_error(logger_obj, f"Erro ao obter token. Resposta da API: {token_info}")
            return None 
            
    except requests.exceptions.RequestException as e:
        log_error(logger_obj, f"Erro na requisição de autenticação: {e}")
        return None

# ----------------------------------------------------
# FUNÇÃO DE EXTRAÇÃO PAGINADA
# ----------------------------------------------------
def extract_service_data(logger_obj, access_token, service_name_api, date_filter_field, start_date=None, end_date=None):
    """ Extrai dados de um serviço, aplicando filtro de data se fornecido. """
    all_records = []
    page = 1
    has_next = True
    
    # Montagem da URL, removendo barra dupla
    service_url = API_CONFIG['BASE_URL_SERVICE'].rstrip('/') + '/' + service_name_api

    # Payload Corrigido: Removida a ordenação por 'idempresa' que causava erro 500
    payload = {"clausulas": []} 

    if date_filter_field and start_date and end_date:
        # Nota: As datas devem ser objetos datetime para a formatação correta.
        if isinstance(start_date, datetime):
            data_inicio_str = f"{start_date.strftime('%Y-%m-%d')} 00:00:00.000000"
        else:
            data_inicio_str = f"{start_date.strftime('%Y-%m-%d')} 00:00:00.000000"
            
        if isinstance(end_date, datetime):
            data_fim_str = f"{end_date.strftime('%Y-%m-%d')} 23:59:59.999999"
        else:
            data_fim_str = f"{end_date.strftime('%Y-%m-%d')} 23:59:59.999999"

        payload["clausulas"].append({
            "campo": date_filter_field,
            "valor": [data_inicio_str, data_fim_str],
            "operador": "BETWEEN"
        })
        log_info(logger_obj, f"Filtro Aplicado: {date_filter_field} BETWEEN {data_inicio_str} e {data_fim_str}")
    else:
        log_info(logger_obj, "Nenhum filtro de data aplicado (Carga Completa/Cadastral).")
        
    headers = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'}
    
    log_info(logger_obj, f"Iniciando extração do serviço: {service_name_api}")
    
    while has_next:
        payload["page"] = page
        
        try:
            # Requisita com POST
            response = requests.post(service_url, headers=headers, data=json.dumps(payload), timeout=5400)
            response.raise_for_status()
            
            data = response.json()
            # Trata 'registros' ou 'data', dependendo da resposta da API
            records = data.get("registros", data.get("data", [])) 
            has_next = data.get("hasNext", False)
            
            if not records and page == 1:
                log_warning(logger_obj, f"Serviço {service_name_api} respondeu com 0 registros.")
                break
            elif not records:
                break
                
            all_records.extend(records)
            log_info(logger_obj, f"Página {page} extraída. Registros nesta página: {len(records)}. Total: {len(all_records)}")
            page += 1
            
        except requests.exceptions.RequestException as e:
            log_error(logger_obj, f"Erro na requisição do serviço {service_name_api} na página {page}: {e}")
            log_error(logger_obj, f"Resposta HTTP: {response.text}" if 'response' in locals() else "Sem resposta HTTP.")
            break
            
    return all_records

# ----------------------------------------------------
# FUNÇÕES GOOGLE BIGQUERY
# ----------------------------------------------------

def get_bigquery_client(logger_obj):
    """ Inicializa e retorna o cliente BigQuery. """
    try:
        credentials = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_JSON)
        client = bigquery.Client(credentials=credentials, project=GCP_CONFIG['PROJECT_ID'])
        return client
    except Exception as e:
        log_error(logger_obj, f"Erro ao inicializar cliente BigQuery: {e}")
        return None

def delete_bigquery_range(logger_obj, table_name, filter_field, start_date, end_date):
    """ Executa um DELETE no BigQuery para um determinado intervalo de data. """
    client = get_bigquery_client(logger_obj)
    if not client:
        return
        
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    full_table_name = f"{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_ID']}.{table_name}"
    
    # IMPORTANTE: Colunas de filtro no BQ devem estar em minúsculo
    
    query = f"""
        DELETE FROM `{full_table_name}`
        WHERE DATE(LOWER({filter_field})) BETWEEN DATE('{start_date_str}') AND DATE('{end_date_str}')
    """
    
    log_warning(logger_obj, f"Iniciando DELETE em {table_name} para o range {start_date_str} a {end_date_str} (Campo: {filter_field.lower()})...")
    
    try:
        query_job = client.query(query)
        query_job.result() 
        log_info(logger_obj, f"DELETE concluído com sucesso. Linhas modificadas: {query_job.num_dml_affected_rows}")
        
    except Exception as e:
        log_error(logger_obj, f"Erro ao executar DELETE no BigQuery: {e}")

def load_to_bigquery(logger_obj, df, table_name, load_mode):
    """ 
    Carrega o DataFrame para o BigQuery. 
    """
    if df.empty:
        log_info(logger_obj, f"DataFrame para {table_name} está vazio. Nenhuma linha carregada.")
        return

    client = get_bigquery_client(logger_obj)
    if not client:
        return

    full_table_id = f"{GCP_CONFIG['DATASET_ID']}.{table_name}"
    
    log_info(logger_obj, "Normalizando colunas de data...")
    for col in DATE_COLUMNS:
        if col.lower() in df.columns:
            # Converte a coluna para string, formatando a data, para evitar problemas de timezone/type
            df[col.lower()] = pd.to_datetime(df[col.lower()], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

    # Garante que todas as colunas estejam em lowercase para match com o schema do BigQuery
    df.columns = [col.lower() for col in df.columns]

    job_config = bigquery.LoadJobConfig(
        write_disposition=load_mode, # WRITE_TRUNCATE ou WRITE_APPEND
    )

    log_info(logger_obj, f"Iniciando job de carga para BigQuery ({load_mode})...")

    try:
        # Carrega o DataFrame
        job = client.load_table_from_dataframe(
            df, full_table_id, job_config=job_config
        )
        job.result() # Espera o job terminar

        log_info(logger_obj, f"Carga para BigQuery concluído. Linhas carregadas: {job.output_rows}")
        
    except Exception as e:
        log_error(logger_obj, f"ERRO FATAL ao carregar para o BigQuery {full_table_id}: {e}")


# ----------------------------------------------------
# FUNÇÃO PRINCIPAL DE EXECUÇÃO ETL (COM CACHE DE TOKEN)
# ----------------------------------------------------

def run_etl_service(table_name, config, historical_ranges=None):
    """ Função unificada para executar o ETL de um serviço. """

    # Inicializa o logger específico para este serviço
    logger_obj = setup_service_logger(table_name)
    
    log_info(logger_obj, f"\n================================================================================")
    log_info(logger_obj, f"INICIANDO CARGA PARA: {table_name} (API: {config['api_name']})")
    log_info(logger_obj, f"================================================================================")

    # O token será obtido e cacheado automaticamente pela função get_auth_token
    filter_field = config.get('filter_field')
    load_mode = config.get('load_mode', 'WRITE_APPEND')

    # A. Carga Completa (Cadastrais)
    if historical_ranges is None:
        log_info(logger_obj, f"Modo: Carga Completa ({load_mode})")
        df_list = []
        
        # --- Obtém o token (ou pega do cache) para a Carga Full ---
        token = get_auth_token(logger_obj)
        if not token:
            log_error(logger_obj, f"Não foi possível obter o token. ETL {table_name} abortado.")
            return
        # --------------------------------------------------------
        
        # Extrai todos os dados (sem filtro de data)
        records = extract_service_data(
            logger_obj, token, config['api_name'], date_filter_field=None, start_date=None, end_date=None
        )
        
        if records:
            df_list.append(pd.DataFrame(records))
            
        final_df = pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()
        load_to_bigquery(logger_obj, final_df, table_name, load_mode)
        
    # B. Carga Histórica e Refresh (Transacionais)
    else:
        # FASE 1: Carga Histórica 
        
        # Opcional: Adapta o log para ser mais preciso (Diário ou Mensal)
        is_daily_load = (historical_ranges[0][1] - historical_ranges[0][0]).days == 0
        range_label = "Diária" if is_daily_load else "Mensal"
        
        log_info(logger_obj, "\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        log_info(logger_obj, f"INICIANDO FASE: CARGA HISTÓRICA ({range_label}) | Período: {historical_ranges[0][0].strftime('%d/%m/%Y')} a {historical_ranges[-1][1].strftime('%d/%m/%Y')}")
        log_info(logger_obj, "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        
        # REMOVIDO: A lógica de 'is_first_batch' não é mais necessária,
        # pois vamos deletar lote a lote.

        for i, (start_date, end_date) in enumerate(historical_ranges):
            
            # Adapta o log para mostrar 'Dia' ou 'Mês'
            period_label = "Dia" if is_daily_load else "Mês"
            date_format = '%Y-%m-%d' if is_daily_load else '%Y-%m'
            log_info(logger_obj, f"Processando {period_label}: {start_date.strftime(date_format)}")
            
            # --- OBTÉM TOKEN (PODE SER DO CACHE) PARA O LOTE ---
            token = get_auth_token(logger_obj)
            if not token:
                log_error(logger_obj, f"Não foi possível obter o token. Carregamento do lote {start_date.strftime(date_format)} abortado.")
                continue # Pula para a próxima iteração/lote
            # ----------------------------------------------------

            # ++++++++++++++++++++++++++++++++++++++++++++++++++++++
            # INÍCIO DA MODIFICAÇÃO: Deleta o intervalo ANTES da extração/carga
            # Isso garante a idempotência (não duplicar dados) se o script for re-executado.
            # ++++++++++++++++++++++++++++++++++++++++++++++++++++++
            if filter_field:
                log_info(logger_obj, f"Garantindo a limpeza do intervalo (DELETE) antes da carga...")
                delete_bigquery_range(
                    logger_obj, table_name, filter_field, start_date, end_date
                )
            else:
                log_error(logger_obj, f"Não é possível deletar o range para {table_name} pois 'filter_field' não está definido. Pulando delete.")
            # ++++++++++++++++++++++++++++++++++++++++++++++++++++++
            # FIM DA MODIFICAÇÃO
            # ++++++++++++++++++++++++++++++++++++++++++++++++++++++

            records = extract_service_data(
                logger_obj, token, config['api_name'], filter_field, start_date, end_date
            )
            
            if records:
                df_batch = pd.DataFrame(records)
                
                # O modo de carga agora é SEMPRE 'WRITE_APPEND',
                # pois o 'DELETE' foi feito antes.
                batch_load_mode = 'WRITE_APPEND'
                
                log_info(logger_obj, f"Carregando {len(df_batch)} linhas com modo: {batch_load_mode}...")

                # Carrega o lote imediatamente
                load_to_bigquery(logger_obj, df_batch, table_name, batch_load_mode)
                
            else:
                log_warning(logger_obj, f"Nenhum registro para o {period_label}: {start_date.strftime(date_format)}. Prosseguindo.")
        
        # FASE 2: Refresh dos últimos N dias (DELETE + APPEND)
        log_info(logger_obj, "\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        log_info(logger_obj, "INICIANDO FASE: REFRESH DOS ÚLTIMOS N DIAS")
        log_info(logger_obj, "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        
        # ++++++++++++++++++++++++++++++++++++++++++++++++++++++
        # INÍCIO DA MODIFICAÇÃO: Adiciona verificação para pular FASE 2
        # ++++++++++++++++++++++++++++++++++++++++++++++++++++++
        if DAYS_FOR_RECENT_REFRESH <= 0:
            log_info(logger_obj, f"Refresh de N dias está desabilitado (DAYS_FOR_RECENT_REFRESH = {DAYS_FOR_RECENT_REFRESH}). Pulando FASE 2.")
        # ++++++++++++++++++++++++++++++++++++++++++++++++++++++
        else:
            # Calcula o intervalo de refresh
            # Se N=1, pega só hoje (N-1 = 0 dias atrás).
            # Se N=3, pega hoje, ontem, anteontem (N-1 = 2 dias atrás).
            start_date_refresh = datetime.now().date() - timedelta(days=DAYS_FOR_RECENT_REFRESH - 1)
            end_date_refresh = datetime.now().date()

            log_info(logger_obj, f"Intervalo de Refresh: {start_date_refresh.strftime('%d/%m/%Y')} a {end_date_refresh.strftime('%d/%m/%Y')}")

            # --- OBTÉM TOKEN (PODE SER DO CACHE) PARA O REFRESH ---
            token = get_auth_token(logger_obj)
            if not token:
                log_error(logger_obj, f"Não foi possível obter o token para Refresh. Refresh {table_name} abortado.")
                log_info(logger_obj, f"*** ETL {table_name} CONCLUÍDO COM ERRO NO REFRESH ***")
                return
            # ----------------------------------------------------

            # 1. DELETA o intervalo no BigQuery (Lógica original, que está correta)
            delete_bigquery_range(
                logger_obj, table_name, filter_field, start_date_refresh, end_date_refresh
            )
            
            # 2. Extrai os dados do intervalo novamente
            start_dt_api = datetime.combine(start_date_refresh, datetime.min.time())
            end_dt_api = datetime.combine(end_date_refresh, datetime.max.time())
            
            records_refresh = extract_service_data(
                logger_obj, token, config['api_name'], filter_field, 
                start_date=start_dt_api, end_date=end_dt_api
            )
            
            df_refresh = pd.DataFrame(records_refresh) if records_refresh else pd.DataFrame()
            
            # 3. Carrega os dados atualizados com APPEND
            load_to_bigquery(logger_obj, df_refresh, table_name, 'WRITE_APPEND')

    log_info(logger_obj, f"*** ETL {table_name} CONCLUÍDO ***")