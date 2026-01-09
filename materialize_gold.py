from google.cloud import bigquery
from google.oauth2 import service_account
from rich import box
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

from config import GCP_CONFIG, SERVICE_ACCOUNT_JSON

console = Console()

# Instancia o client BigQuery
try:
    credentials = service_account.Credentials.from_service_account_info(
        SERVICE_ACCOUNT_JSON
    )
    bq_client = bigquery.Client(
        credentials=credentials, project=GCP_CONFIG["PROJECT_ID"]
    )
except Exception as e:
    console.print(f"[bold red]Erro ao autenticar no BigQuery:[/bold red] {e}")
    bq_client = None

# Configuração das tabelas que serão materializadas
TABLES_TO_OPTIMIZE = {
    "VW_ITENS_SAIDA": {
        "partition_field": "DTMOVIMENTO",
        "cluster_fields": ["EMPRESA", "descrcomproduto", "descrsecao"],
    },
    "VW_NF_SAIDAS": {
        "partition_field": "DTMOVIMENTO",
        "cluster_fields": ["EMPRESA"],
    },
    "VW_ITENS_ENTRADA": {
        "partition_field": "DTMOVIMENTO",
        "cluster_fields": ["EMPRESA", "descrcomproduto", "descrsecao"],
    },
}


def materialize_specific_table(view_name):
    """
    Executa a materialização de UMA tabela específica.
    Função projetada para ser chamada individualmente pelo orquestrador (main.py).

    Args:
        view_name (str): Nome da View (ex: VW_ITENS_SAIDA)

    Returns:
        tuple: (sucesso: bool, mensagem: str)
    """
    if not bq_client:
        return False, "Cliente BigQuery não inicializado."

    if view_name not in TABLES_TO_OPTIMIZE:
        return False, f"Configuração para {view_name} não encontrada."

    config = TABLES_TO_OPTIMIZE[view_name]
    target_table = view_name.replace("VW_", "T_")
    table_id = f"{GCP_CONFIG['PROJECT_ID']}.GOLD_JUMA.{target_table}"

    try:
        # 1. DROP para garantir recriação limpa (partition/cluster specs)
        bq_client.query(f"DROP TABLE IF EXISTS `{table_id}`").result()

        # 2. CREATE TABLE ... AS SELECT ...
        sql = f"""
        CREATE TABLE `{table_id}`
        PARTITION BY {config["partition_field"]}
        CLUSTER BY {", ".join(config["cluster_fields"])}
        AS SELECT * FROM `{GCP_CONFIG["PROJECT_ID"]}.GOLD_JUMA.{view_name}`;
        """
        bq_client.query(sql).result()

        return True, f"Tabela {target_table} criada com sucesso."

    except Exception as e:
        return False, str(e)


def generate_gold_table(status_dict):
    """Gera a tabela visual de status da materialização (para o modo bateria)."""
    table = Table(box=box.ROUNDED)
    table.add_column("Tabela Gold", style="cyan")
    table.add_column("Status", justify="center")

    for table_name in sorted(status_dict.keys()):
        status = status_dict[table_name]
        if status == "pending":
            render_status = "[dim]Pendente[/dim]"
        elif status == "running":
            render_status = "[yellow]Processando...[/yellow]"
        elif status == "success":
            render_status = "[green]✔ Sucesso[/green]"
        else:
            render_status = "[red]✖ Erro[/red]"

        table.add_row(table_name, render_status)

    return Panel(table, title="Status Materialização Gold", border_style="blue")


def materialize_gold_tables():
    """
    Executa TODAS as materializações em sequência (Modo Bateria).
    Útil para execução manual ou rodar tudo de uma vez.
    """
    status_map = {name.replace("VW_", "T_"): "pending" for name in TABLES_TO_OPTIMIZE}

    console.print(
        Panel(
            "[bold magenta]💎 Materialização de Camada GOLD (Modo Bateria)[/bold magenta]"
        )
    )

    with Live(generate_gold_table(status_map), refresh_per_second=4) as live:
        for view_name in TABLES_TO_OPTIMIZE.keys():
            target_table = view_name.replace("VW_", "T_")

            # Atualiza UI
            status_map[target_table] = "running"
            live.update(generate_gold_table(status_map))

            # Chama a função individual que criamos acima
            success, msg = materialize_specific_table(view_name)

            # Atualiza UI final
            if success:
                status_map[target_table] = "success"
            else:
                status_map[target_table] = "error"
                console.print(f"[red]Erro em {target_table}: {msg}[/red]")

            live.update(generate_gold_table(status_map))

    console.print("[bold green]✨ Ciclo de Materialização Finalizado![/bold green]")


if __name__ == "__main__":
    # Permite testar este arquivo isoladamente
    materialize_gold_tables()
