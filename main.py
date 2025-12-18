import sys
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from config import SERVICE_MAP, START_DATE_HISTORICAL, END_DATE_HISTORICAL
from utils import run_etl_service

# Filtra apenas tabelas ativas/existentes no MAP
ALL_TABLES = list(SERVICE_MAP.keys())

def process_table(table_name):
    """Wrapper para ser chamado pelo Pool"""
    try:
        config = SERVICE_MAP[table_name]
        # Define range histórico apenas se necessário (lógica original)
        historical = None
        # Lógica simplificada baseada no seu código original:
        # Se for carga FULL/TRUNCATE, historical é None.
        # Se precisar de histórico customizado, passe aqui.
        # Para o exemplo, vou manter o padrão do seu script original:
        if config.get("load_mode") == "WRITE_APPEND": 
             # Nota: Sua lógica original de ranges estava dentro do run_etl_service 
             # ou passada no main. Aqui simplifiquei para chamar a função.
             # Se precisar passar ranges específicos, calcule aqui.
             pass

        # No seu código original, você passava historical_ranges=None para tudo no main,
        # mas dentro do utils.py ele parece tratar cargas históricas. 
        # Vou assumir a chamada padrão:
        run_etl_service(table_name, config, historical_ranges=None)
        return (table_name, True)
    except Exception as e:
        print(f"Erro em {table_name}: {e}")
        return (table_name, False)

def run_all_parallel(max_workers=4):
    print(f"🚀 Iniciando ETL para {len(ALL_TABLES)} tabelas com {max_workers} workers...")
    
    results = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_table = {executor.submit(process_table, table): table for table in ALL_TABLES}
        
        for future in as_completed(future_to_table):
            table, success = future.result()
            status = "✅ Sucesso" if success else "❌ Falha"
            print(f"{status}: {table}")
            results.append((table, success))

    print("\nResumo Final:")
    for table, success in results:
        print(f"{'OK' if success else 'ERRO'}: {table}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Juma Orchestrator")
    parser.add_argument("--table", type=str, help="Executa apenas uma tabela específica")
    parser.add_argument("--all", action="store_true", help="Executa todas as tabelas em paralelo")
    parser.add_argument("--workers", type=int, default=4, help="Número de processos paralelos")

    args = parser.parse_args()

    if args.table:
        table_upper = args.table.upper()
        if table_upper in SERVICE_MAP:
            print(f"Executando único ETL: {table_upper}")
            process_table(table_upper)
        else:
            print(f"Tabela {table_upper} não encontrada no config.py")
    elif args.all:
        run_all_parallel(max_workers=args.workers)
    else:
        parser.print_help()