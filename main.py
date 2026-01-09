import argparse
import os
from concurrent.futures import ProcessPoolExecutor, as_completed

from rich import box
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

from config import (
    END_DATE_HISTORICAL,
    SERVICE_MAP,
    START_DATE_HISTORICAL,
    get_daily_ranges,
    get_monthly_ranges,
)
from materialize_gold import TABLES_TO_OPTIMIZE, materialize_specific_table
from utils import run_etl_service

ALL_TABLES = list(SERVICE_MAP.keys())
console = Console()

# Mapeamento de Gatilhos: Define qual tabela RAW dispara qual materialização Gold
TRIGGER_MAP = {
    "ITENS_DOCUMENTOS_FISCAIS_SAIDA": "VW_ITENS_SAIDA",
    "DOCUMENTOS_FISCAIS_SAIDA": "VW_NF_SAIDAS",
    "ITENS_DOCUMENTOS_FISCAIS_ENTRADA": "VW_ITENS_ENTRADA",
}


def process_table(table_name):
    """Executa o processo de ETL para uma tabela específica (RAW)."""
    try:
        config = SERVICE_MAP[table_name]
        range_type = config.get("range_type")
        historical_ranges = None

        if range_type == "monthly":
            historical_ranges = get_monthly_ranges(
                START_DATE_HISTORICAL, END_DATE_HISTORICAL
            )
        elif range_type == "daily":
            historical_ranges = get_daily_ranges(
                START_DATE_HISTORICAL, END_DATE_HISTORICAL
            )

        run_etl_service(table_name, config, historical_ranges=historical_ranges)
        return (table_name, True, "Finalizado")
    except Exception as e:
        return (table_name, False, str(e))


def make_table_silver(status_dict):
    """Gera a tabela da CAMADA RAW."""
    table = Table(box=box.SIMPLE, show_header=True)
    table.add_column("Status", justify="left", width=20, no_wrap=True)
    table.add_column("Tabela RAW (API)", style="bold white")

    for name in ALL_TABLES:
        status = status_dict.get(name, "pending")
        if status == "pending":
            status_str = "[bold white]⚪ Aguardando...[/]"
        elif status == "running":
            status_str = "[bold blue]🔁 Executando...[/]"
        elif status == "success":
            status_str = "[bold green]🚀 Finalizado[/]"
        else:
            status_str = "[bold red]🚫 Erro[/]"
        table.add_row(status_str, name)
    return table


def make_table_gold(status_dict):
    """Gera a tabela da Camada Gold."""
    table = Table(box=box.SIMPLE, show_header=True)
    table.add_column("Status", justify="left", width=20, no_wrap=True)
    table.add_column("Tabela Gold (BQ)", style="bold white")

    for view_name in TABLES_TO_OPTIMIZE.keys():
        target_t = view_name.replace("VW_", "T_")
        status = status_dict.get(target_t, "pending")

        if status == "pending":
            status_str = "[bold white]⚪ Aguardando...[/]"
        elif status == "running":
            status_str = "[bold blue]🔁 Otimizando...[/]"
        elif status == "success":
            status_str = "[bold green]🚀 Finalizado[/]"
        else:
            status_str = "[bold red]🚫 Erro[/]"
        table.add_row(status_str, target_t)
    return table


def run_parallel_etl(workers):
    """Orquestrador principal com Dashboard em tempo real."""
    layout = Layout()
    layout.split_row(
        Layout(name="left", ratio=1),
        Layout(name="right", ratio=1),
    )

    silver_status = {name: "pending" for name in ALL_TABLES}
    gold_status = {v.replace("VW_", "T_"): "pending" for v in TABLES_TO_OPTIMIZE.keys()}

    console.print(
        Panel.fit(
            f"[bold green]ETL ORCHESTRATOR[/bold green] | "
            f"Workers: {workers} | Período: {START_DATE_HISTORICAL.date()} a {END_DATE_HISTORICAL.date()}",
            border_style="green",
        )
    )

    with Live(layout, refresh_per_second=4, screen=False):
        # Primeiro envio: Preenche o pool e atualiza a UI imediatamente
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {}
            tables_to_submit = ALL_TABLES.copy()

            # Submete todos, mas marca apenas os N primeiros como 'running' na UI inicialmente
            for i, name in enumerate(tables_to_submit):
                fut = executor.submit(process_table, name)
                futures[fut] = name
                if i < workers:
                    silver_status[name] = "running"

            # Atualização inicial da interface (antes de entrar no loop bloqueante)
            layout["left"].update(
                Panel(
                    make_table_silver(silver_status),
                    title="[bold cyan]📦 CAMADA RAW[/]",
                    border_style="cyan",
                )
            )
            layout["right"].update(
                Panel(
                    make_table_gold(gold_status),
                    title="[bold magenta]🚀 CAMADA GOLD[/]",
                    border_style="magenta",
                )
            )

            # Lista para controlar quem está na fila de espera para ser marcado como 'running'
            pending_queue = tables_to_submit[workers:]

            for future in as_completed(futures):
                res_name, success, msg = future.result()
                silver_status[res_name] = "success" if success else "error"

                # Se houver alguém na fila, move o próximo para 'running'
                if pending_queue:
                    next_table = pending_queue.pop(0)
                    silver_status[next_table] = "running"

                layout["left"].update(
                    Panel(
                        make_table_silver(silver_status),
                        title="[bold cyan]📦 CAMADA RAW[/]",
                        border_style="cyan",
                    )
                )

                # Lógica da Camada Gold (Trigger)
                if success and res_name in TRIGGER_MAP:
                    gold_view = TRIGGER_MAP[res_name]
                    target_t = gold_view.replace("VW_", "T_")

                    gold_status[target_t] = "running"
                    layout["right"].update(
                        Panel(
                            make_table_gold(gold_status),
                            title="[bold magenta]🚀 CAMADA GOLD[/]",
                            border_style="magenta",
                        )
                    )

                    mat_success, _ = materialize_specific_table(gold_view)

                    gold_status[target_t] = "success" if mat_success else "error"
                    layout["right"].update(
                        Panel(
                            make_table_gold(gold_status),
                            title="[bold magenta]🚀 CAMADA GOLD[/]",
                            border_style="magenta",
                        )
                    )

    console.print(
        "\n[bold green]✨ Ciclo completo finalizado com sucesso![/bold green]"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Orchestrator")
    parser.add_argument(
        "--table", type=str, help="Executa apenas uma tabela específica"
    )
    parser.add_argument(
        "--all", action="store_true", help="Executa todas as tabelas em paralelo"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=os.cpu_count(),
        help="Número de processos paralelos",
    )

    args = parser.parse_args()

    if args.table:
        table_upper = args.table.upper()
        if table_upper in SERVICE_MAP:
            console.print(f"[bold blue]Modo Unitário:[/bold blue] {table_upper}")
            res_name, success, msg = process_table(table_upper)
            if success:
                console.print("[green]✔ ETL RAW concluído.[/green]")
                if table_upper in TRIGGER_MAP:
                    gv = TRIGGER_MAP[table_upper]
                    materialize_specific_table(gv)
            else:
                console.print(f"[red]✖ Erro: {msg}[/red]")
        else:
            console.print(f"[red]Tabela {table_upper} não encontrada.[/red]")

    elif args.all:
        run_parallel_etl(args.workers)
    else:
        parser.print_help()
