# utils.py

import json
import logging
import os
from datetime import datetime, timedelta

import pandas as pd
import requests
from google.cloud import bigquery
from google.oauth2 import service_account

from config import (
    API_CONFIG,
    DATE_COLUMNS,
    DAYS_FOR_RECENT_REFRESH,
    GCP_CONFIG,
    SERVICE_ACCOUNT_JSON,
)

# ----------------------------------------------------
# CACHE DE TOKEN E CONFIGURAÇÃO DE TIMEOUT
# ----------------------------------------------------
TOKEN_CACHE = {
    "access_token": None,
    "expires_at": datetime.min,  # Inicializa com data mínima para forçar a primeira geração
}
TOKEN_LIFESPAN_MINUTES = 10  # Tempo de vida (TTL) do token em minutos.
# ----------------------------------------------------

# ----------------------------------------------------
# CONFIGURAÇÃO DE LOG DINÂMICO (Por Serviço)
# ----------------------------------------------------
LOG_DIR = "logs"  # Define um diretório para logs
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

LOG_LEVEL = logging.INFO
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"


def setup_service_logger(service_name):
    """
    Cria ou recupera um logger configurado para salvar em um arquivo
    específico ('logs/etl_<SERVICE_NAME>.log').
    """
    logger_name = f"ETL_{service_name}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(LOG_LEVEL)

    # Previne que handlers sejam adicionados múltiplas vezes se a função for chamada novamente
    if logger.handlers:
        return logger

    # Handler para arquivo
    log_file_name = f"{LOG_DIR}/etl_{service_name}.log"
    file_handler = logging.FileHandler(log_file_name, mode="a", encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(file_handler)

    # --- ALTERAÇÃO PARA DASHBOARD: Console Handler COMENTADO ---
    # Motivo: O 'rich' vai controlar o terminal. Logs vão apenas para arquivo.
    # console_handler = logging.StreamHandler(sys.stdout)
    # console_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    # logger.addHandler(console_handler)
    # -----------------------------------------------------------

    logger.info(
        f"Log de serviço '{service_name}' configurado. Saída gravada em: {log_file_name}"
    )
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
    global TOKEN_CACHE  # Indica que você usará a variável global

    # 1. Checa o Cache
    if TOKEN_CACHE["access_token"] and datetime.now() < TOKEN_CACHE["expires_at"]:
        log_info(
            logger_obj, "Token de Autenticação reutilizado do cache (ainda válido)."
        )
        return TOKEN_CACHE["access_token"]

    log_info(
        logger_obj,
        "Token de Autenticação expirado ou não existe. Iniciando nova requisição...",
    )

    auth_data = {
        "client_id": API_CONFIG["CLIENT_ID"],
        "grant_type": API_CONFIG["GRANT_TYPE"],
        "client_secret": API_CONFIG["CLIENT_SECRET"],
        "username": API_CONFIG["USERNAME"],
        "password": API_CONFIG["PASSWORD"],
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    try:
        response = requests.post(
            API_CONFIG["BASE_URL_AUTH"], data=auth_data, headers=headers
        )
        response.raise_for_status()
        token_info = response.json()
        access_token = token_info.get("access_token")

        if access_token:
            # 2. Atualiza o Cache (com 10 minutos de validade)
            TOKEN_CACHE["access_token"] = access_token
            TOKEN_CACHE["expires_at"] = datetime.now() + timedelta(
                minutes=TOKEN_LIFESPAN_MINUTES
            )

            log_info(
                logger_obj,
                f"Novo Token de Autenticação obtido e válido até: {TOKEN_CACHE['expires_at'].strftime('%Y-%m-%d %H:%M:%S')}",
            )
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
def extract_service_data(
    logger_obj,
    access_token,
    service_name_api,
    date_filter_field,
    start_date=None,
    end_date=None,
):
    """Extrai dados de um serviço, aplicando filtro de data se fornecido."""
    all_records = []
    page = 1
    has_next = True

    service_url = API_CONFIG["BASE_URL_SERVICE"].rstrip("/") + "/" + service_name_api
    payload = {"clausulas": []}

    if date_filter_field and start_date and end_date:
        if isinstance(start_date, datetime):
            data_inicio_str = f"{start_date.strftime('%Y-%m-%d')} 00:00:00.000000"
        else:
            data_inicio_str = f"{start_date.strftime('%Y-%m-%d')} 00:00:00.000000"

        if isinstance(end_date, datetime):
            data_fim_str = f"{end_date.strftime('%Y-%m-%d')} 23:59:59.999999"
        else:
            data_fim_str = f"{end_date.strftime('%Y-%m-%d')} 23:59:59.999999"

        payload["clausulas"].append(
            {
                "campo": date_filter_field,
                "valor": [data_inicio_str, data_fim_str],
                "operador": "BETWEEN",
            }
        )
        log_info(
            logger_obj,
            f"Filtro Aplicado: {date_filter_field} BETWEEN {data_inicio_str} e {data_fim_str}",
        )
    else:
        log_info(
            logger_obj, "Nenhum filtro de data aplicado (Carga Completa/Cadastral)."
        )

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    log_info(logger_obj, f"Iniciando extração do serviço: {service_name_api}")

    while has_next:
        payload["page"] = page

        try:
            response = requests.post(
                service_url, headers=headers, data=json.dumps(payload), timeout=5400
            )
            response.raise_for_status()

            data = response.json()
            records = data.get("registros", data.get("data", []))
            has_next = data.get("hasNext", False)

            if not records and page == 1:
                log_warning(
                    logger_obj, f"Serviço {service_name_api} respondeu com 0 registros."
                )
                break
            elif not records:
                break

            all_records.extend(records)
            log_info(
                logger_obj,
                f"Página {page} extraída. Registros nesta página: {len(records)}. Total: {len(all_records)}",
            )
            page += 1

        except requests.exceptions.RequestException as e:
            log_error(
                logger_obj,
                f"Erro na requisição do serviço {service_name_api} na página {page}: {e}",
            )
            break

    return all_records


# ----------------------------------------------------
# FUNÇÕES GOOGLE BIGQUERY
# ----------------------------------------------------
def get_bigquery_client(logger_obj):
    try:
        credentials = service_account.Credentials.from_service_account_info(
            SERVICE_ACCOUNT_JSON
        )
        client = bigquery.Client(
            credentials=credentials, project=GCP_CONFIG["PROJECT_ID"]
        )
        return client
    except Exception as e:
        log_error(logger_obj, f"Erro ao inicializar cliente BigQuery: {e}")
        return None


def delete_bigquery_range(logger_obj, table_name, filter_field, start_date, end_date):
    client = get_bigquery_client(logger_obj)
    if not client:
        return

    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    full_table_name = (
        f"{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_ID']}.{table_name}"
    )

    query = f"""
        DELETE FROM `{full_table_name}`
        WHERE DATE(LOWER({filter_field})) BETWEEN DATE('{start_date_str}') AND DATE('{end_date_str}')
    """

    log_warning(
        logger_obj,
        f"Iniciando DELETE em {table_name} para o range {start_date_str} a {end_date_str} (Campo: {filter_field.lower()})...",
    )
    try:
        query_job = client.query(query)
        query_job.result()
        log_info(
            logger_obj,
            f"DELETE concluído com sucesso. Linhas modificadas: {query_job.num_dml_affected_rows}",
        )
    except Exception as e:
        log_error(logger_obj, f"Erro ao executar DELETE no BigQuery: {e}")


def load_to_bigquery(logger_obj, df, table_name, load_mode):
    if df.empty:
        log_info(
            logger_obj,
            f"DataFrame para {table_name} está vazio. Nenhuma linha carregada.",
        )
        return

    client = get_bigquery_client(logger_obj)
    if not client:
        return

    full_table_id = f"{GCP_CONFIG['DATASET_ID']}.{table_name}"

    log_info(logger_obj, "Normalizando colunas de data...")
    for col in DATE_COLUMNS:
        if col.lower() in df.columns:
            df[col.lower()] = pd.to_datetime(
                df[col.lower()], errors="coerce"
            ).dt.strftime("%Y-%m-%d %H:%M:%S")

    df.columns = [col.lower() for col in df.columns]

    job_config = bigquery.LoadJobConfig(write_disposition=load_mode)
    log_info(logger_obj, f"Iniciando job de carga para BigQuery ({load_mode})...")

    try:
        job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
        job.result()
        log_info(
            logger_obj,
            f"Carga para BigQuery concluído. Linhas carregadas: {job.output_rows}",
        )
    except Exception as e:
        log_error(
            logger_obj, f"ERRO FATAL ao carregar para o BigQuery {full_table_id}: {e}"
        )


# ----------------------------------------------------
# FUNÇÃO PRINCIPAL DE EXECUÇÃO ETL (COM CACHE DE TOKEN)
# ----------------------------------------------------
def run_etl_service(table_name, config, historical_ranges=None):
    logger_obj = setup_service_logger(table_name)

    log_info(
        logger_obj,
        "\n================================================================================",
    )
    log_info(
        logger_obj, f"INICIANDO CARGA PARA: {table_name} (API: {config['api_name']})"
    )
    log_info(
        logger_obj,
        "================================================================================",
    )

    filter_field = config.get("filter_field")
    load_mode = config.get("load_mode", "WRITE_APPEND")

    # A. Carga Completa (Cadastrais)
    if historical_ranges is None:
        log_info(logger_obj, f"Modo: Carga Completa ({load_mode})")
        token = get_auth_token(logger_obj)
        if not token:
            return

        records = extract_service_data(
            logger_obj, token, config["api_name"], None, None, None
        )
        if records:
            load_to_bigquery(logger_obj, pd.DataFrame(records), table_name, load_mode)

    # B. Carga Incremental (Histórico + Refresh)
    else:
        # FASE 1: Carga Histórica
        is_daily_load = (historical_ranges[0][1] - historical_ranges[0][0]).days == 0
        range_label = "Diária" if is_daily_load else "Mensal"

        log_info(logger_obj, f"--- INICIANDO FASE: CARGA HISTÓRICA ({range_label}) ---")

        for i, (start_date, end_date) in enumerate(historical_ranges):
            period_label = "Dia" if is_daily_load else "Mês"
            date_format = "%Y-%m-%d" if is_daily_load else "%Y-%m"
            log_info(
                logger_obj,
                f"Processando {period_label}: {start_date.strftime(date_format)}",
            )

            token = get_auth_token(logger_obj)
            if not token:
                continue

            # --- MUDANÇA DE ORDEM SOLICITADA ---
            # 1. Extrai primeiro
            records = extract_service_data(
                logger_obj,
                token,
                config["api_name"],
                filter_field,
                start_date,
                end_date,
            )

            # 2. Se houver registros, deleta e carrega
            if records:
                if filter_field:
                    delete_bigquery_range(
                        logger_obj, table_name, filter_field, start_date, end_date
                    )
                load_to_bigquery(
                    logger_obj, pd.DataFrame(records), table_name, "WRITE_APPEND"
                )
            else:
                log_warning(
                    logger_obj,
                    f"Nenhum registro para o {period_label}: {start_date.strftime(date_format)}. Prosseguindo.",
                )
            # -----------------------------------

        # FASE 2: Refresh dos últimos N dias
        log_info(logger_obj, "\n--- INICIANDO FASE: REFRESH DOS ÚLTIMOS N DIAS ---")

        if DAYS_FOR_RECENT_REFRESH <= 0:
            log_info(logger_obj, "Refresh desabilitado (0 dias).")
        else:
            # CORREÇÃO: Pega N dias completos para trás + hoje
            # Ex: Se N=7 e hoje é dia 19, pega de 12 a 19 (8 dias de margem segura)
            start_date_refresh = datetime.now().date() - timedelta(
                days=DAYS_FOR_RECENT_REFRESH
            )
            end_date_refresh = datetime.now().date()

            log_info(
                logger_obj,
                f"Intervalo de Refresh: {start_date_refresh} a {end_date_refresh}",
            )

            token = get_auth_token(logger_obj)
            if token:
                # Conversão para datetime completo
                s_dt = datetime.combine(start_date_refresh, datetime.min.time())
                e_dt = datetime.combine(end_date_refresh, datetime.max.time())

                # --- MUDANÇA DE ORDEM SOLICITADA ---
                # 1. Extrai primeiro
                records_refresh = extract_service_data(
                    logger_obj, token, config["api_name"], filter_field, s_dt, e_dt
                )

                # 2. Se houver registros, deleta e carrega
                if records_refresh:
                    delete_bigquery_range(
                        logger_obj,
                        table_name,
                        filter_field,
                        start_date_refresh,
                        end_date_refresh,
                    )
                    load_to_bigquery(
                        logger_obj,
                        pd.DataFrame(records_refresh),
                        table_name,
                        "WRITE_APPEND",
                    )
                # -----------------------------------

    log_info(logger_obj, f"*** ETL {table_name} CONCLUÍDO ***")
