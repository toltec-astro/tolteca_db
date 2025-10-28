"""Database management commands."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.table import Table

console = Console()

db_app = typer.Typer(
    name="db",
    help="Database management operations",
    no_args_is_help=True,
)


@db_app.command(name="init")
def init_database(
    db_url: Annotated[
        Optional[str],
        typer.Option("--url", help="Database URL (default: DuckDB in-memory)"),
    ] = None,
    create_registry: Annotated[
        bool,
        typer.Option("--registry/--no-registry", help="Populate registry tables"),
    ] = True,
) -> None:
    """
    Initialize database schema.
    
    Creates all tables and optionally populates registry tables
    (DataProdType, DataKind, Flag definitions).
    """
    from tolteca_db.db import get_engine, setup_database
    
    console.print("[bold blue]Initializing database...[/bold blue]")
    
    engine = get_engine(db_url)
    console.print(f"Database: {engine.url}")
    
    setup_database(engine, populate_registry=create_registry)
    
    console.print("[green]✓[/green] Database initialized successfully")
    if create_registry:
        console.print("  - Registry tables populated")


@db_app.command(name="info")
def database_info(
    db_url: Annotated[
        Optional[str],
        typer.Option("--url", help="Database URL"),
    ] = None,
) -> None:
    """
    Display database information and statistics.
    """
    from sqlalchemy import inspect, text
    from tolteca_db.db import get_engine
    from tolteca_db.models.orm import DataProd, DataProdAssoc, DataProdSource
    
    engine = get_engine(db_url)
    inspector = inspect(engine)
    
    console.print(f"[bold blue]Database Info:[/bold blue] {engine.url}")
    console.print(f"Dialect: {engine.dialect.name}")
    
    # List tables
    tables = inspector.get_table_names()
    console.print(f"\n[bold]Tables:[/bold] {len(tables)}")
    
    table = Table(title="Table Statistics")
    table.add_column("Table", style="cyan")
    table.add_column("Rows", style="magenta", justify="right")
    
    from sqlalchemy.orm import Session
    with Session(engine) as session:
        # Count rows in main tables
        counts = {
            "data_prod": session.query(DataProd).count(),
            "data_prod_source": session.query(DataProdSource).count(),
            "data_prod_assoc": session.query(DataProdAssoc).count(),
        }
        
        for table_name, count in counts.items():
            table.add_row(table_name, str(count))
    
    console.print(table)


@db_app.command(name="export")
def export_database(
    output_dir: Annotated[
        Path,
        typer.Argument(help="Output directory for Parquet files"),
    ],
    db_url: Annotated[
        Optional[str],
        typer.Option("--url", help="Database URL"),
    ] = None,
    tables: Annotated[
        Optional[str],
        typer.Option("--tables", help="Comma-separated table names (default: all)"),
    ] = None,
) -> None:
    """
    Export database tables to Parquet files.
    
    Useful for backup, analysis, or migration.
    """
    from tolteca_db.db import get_engine
    import duckdb
    
    engine = get_engine(db_url)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    console.print(f"[bold blue]Exporting to:[/bold blue] {output_dir}")
    
    # Connect with DuckDB for export
    conn = duckdb.connect()
    
    # Attach source database
    if "duckdb" in str(engine.url):
        db_path = str(engine.url).replace("duckdb:///", "")
        conn.execute(f"ATTACH '{db_path}' AS source_db")
    
    table_list = tables.split(",") if tables else ["data_prod", "data_prod_source", "data_prod_assoc"]
    
    for table_name in table_list:
        output_file = output_dir / f"{table_name}.parquet"
        try:
            conn.execute(f"COPY source_db.{table_name} TO '{output_file}' (FORMAT PARQUET)")
            console.print(f"[green]✓[/green] Exported {table_name} → {output_file.name}")
        except Exception as e:
            console.print(f"[red]✗[/red] Failed to export {table_name}: {e}")
    
    conn.close()


@db_app.command(name="vacuum")
def vacuum_database(
    db_url: Annotated[
        Optional[str],
        typer.Option("--url", help="Database URL"),
    ] = None,
    analyze: Annotated[
        bool,
        typer.Option("--analyze", help="Also run ANALYZE for statistics"),
    ] = True,
) -> None:
    """
    Optimize database (VACUUM and optionally ANALYZE).
    
    DuckDB: Reclaims space and updates statistics.
    """
    from tolteca_db.db import get_engine
    from sqlalchemy import text
    
    engine = get_engine(db_url)
    
    console.print("[bold blue]Optimizing database...[/bold blue]")
    
    with engine.connect() as conn:
        # DuckDB VACUUM
        conn.execute(text("VACUUM"))
        console.print("[green]✓[/green] VACUUM complete")
        
        if analyze:
            conn.execute(text("ANALYZE"))
            console.print("[green]✓[/green] ANALYZE complete")
        
        conn.commit()
