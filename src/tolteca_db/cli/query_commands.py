"""Query and export commands."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.table import Table

console = Console()

query_app = typer.Typer(
    name="query",
    help="Query and export operations",
    no_args_is_help=True,
)


@query_app.command(name="obs")
def query_observations(
    db_url: Annotated[
        Optional[str],
        typer.Option("--db", help="Database URL"),
    ] = None,
    obsnum: Annotated[
        Optional[int],
        typer.Option("--obsnum", help="Filter by observation number"),
    ] = None,
    master: Annotated[
        Optional[str],
        typer.Option("--master", help="Filter by master identifier"),
    ] = None,
    data_kind: Annotated[
        Optional[str],
        typer.Option("--kind", help="Filter by data kind (VnaSweep, TargetSweep, etc.)"),
    ] = None,
    limit: Annotated[
        int,
        typer.Option("--limit", "-n", help="Maximum results"),
    ] = 20,
) -> None:
    """
    Query raw observations.
    
    Display observations with optional filtering by obsnum, master, and data kind.
    """
    from tolteca_db.db import get_engine
    from sqlalchemy.orm import Session
    from tolteca_db.models.orm import DataProd
    from tolteca_db.constants import ToltecDataKind
    
    engine = get_engine(db_url)
    
    console.print("[bold blue]Querying observations...[/bold blue]")
    
    with Session(engine) as session:
        query = session.query(DataProd).filter(DataProd.data_prod_type_fk == 1)  # dp_raw_obs
        
        # Apply filters
        if obsnum is not None:
            # Filter by metadata JSON field
            query = query.filter(DataProd.meta["obsnum"].as_integer() == obsnum)
        
        if master is not None:
            query = query.filter(DataProd.meta["master"].as_string() == master)
        
        results = query.limit(limit).all()
        
        if not results:
            console.print("[yellow]No observations found[/yellow]")
            return
        
        # Display results
        table = Table(title=f"Raw Observations ({len(results)} results)")
        table.add_column("PK", style="cyan", max_width=20)
        table.add_column("ObsNum", style="magenta")
        table.add_column("SubObs", style="green")
        table.add_column("Scan", style="green")
        table.add_column("Master", style="blue")
        table.add_column("Roach", style="yellow")
        
        for obs in results:
            meta = obs.meta
            table.add_row(
                str(obs.pk)[:18] + "...",
                str(meta.get("obsnum", "N/A")),
                str(meta.get("subobsnum", "N/A")),
                str(meta.get("scannum", "N/A")),
                str(meta.get("master", "N/A")),
                str(meta.get("roachid", "N/A")),
            )
        
        console.print(table)


@query_app.command(name="groups")
def query_groups(
    db_url: Annotated[
        Optional[str],
        typer.Option("--db", help="Database URL"),
    ] = None,
    group_type: Annotated[
        Optional[str],
        typer.Option("--type", help="Filter by group type (cal_group, drivefit, focus_group)"),
    ] = None,
    obsnum: Annotated[
        Optional[int],
        typer.Option("--obsnum", help="Filter by observation number"),
    ] = None,
    limit: Annotated[
        int,
        typer.Option("--limit", "-n", help="Maximum results"),
    ] = 20,
    show_members: Annotated[
        bool,
        typer.Option("--members", help="Show group members"),
    ] = False,
) -> None:
    """
    Query association groups (cal_group, drivefit, focus_group).
    
    Display groups with optional member listing.
    """
    from tolteca_db.db import get_engine
    from sqlalchemy.orm import Session, joinedload
    from tolteca_db.models.orm import DataProd, DataProdAssoc
    
    engine = get_engine(db_url)
    
    console.print("[bold blue]Querying groups...[/bold blue]")
    
    with Session(engine) as session:
        # Groups have type_fk > 1 (not raw_obs)
        query = session.query(DataProd).filter(DataProd.data_prod_type_fk > 1)
        
        if group_type:
            # Map common names to type IDs
            type_map = {
                "cal_group": 2,
                "drivefit": 3,
                "focus_group": 4,
            }
            type_fk = type_map.get(group_type)
            if type_fk:
                query = query.filter(DataProd.data_prod_type_fk == type_fk)
        
        if obsnum is not None:
            query = query.filter(DataProd.meta["obsnum"].as_integer() == obsnum)
        
        if show_members:
            query = query.options(joinedload(DataProd.dst_assocs))
        
        results = query.limit(limit).all()
        
        if not results:
            console.print("[yellow]No groups found[/yellow]")
            return
        
        # Display groups
        for group in results:
            meta = group.meta
            
            console.print(f"\n[bold cyan]Group:[/bold cyan] {group.pk[:30]}...")
            console.print(f"  Type: {group.data_prod_type_fk}")
            console.print(f"  ObsNum: {meta.get('obsnum', 'N/A')}")
            console.print(f"  Master: {meta.get('master', 'N/A')}")
            
            if show_members and hasattr(group, 'dst_assocs'):
                console.print(f"  Members: {len(group.dst_assocs)}")
                
                if group.dst_assocs:
                    member_table = Table(show_header=True, box=None, padding=(0, 2))
                    member_table.add_column("Member PK", style="dim")
                    
                    for assoc in group.dst_assocs[:10]:  # Show first 10
                        member_table.add_row(f"  → {assoc.src_data_prod_fk[:30]}...")
                    
                    console.print(member_table)
                    
                    if len(group.dst_assocs) > 10:
                        console.print(f"  [dim]...(+{len(group.dst_assocs) - 10} more)[/dim]")


@query_app.command(name="export")
def export_query(
    output_file: Annotated[
        Path,
        typer.Argument(help="Output file (CSV or Parquet)"),
    ],
    table: Annotated[
        str,
        typer.Option("--table", "-t", help="Table to export (data_prod, data_prod_source, data_prod_assoc)"),
    ] = "data_prod",
    db_url: Annotated[
        Optional[str],
        typer.Option("--db", help="Database URL"),
    ] = None,
    filters: Annotated[
        Optional[str],
        typer.Option("--filter", help="SQL WHERE clause"),
    ] = None,
    limit: Annotated[
        Optional[int],
        typer.Option("--limit", "-n", help="Limit rows"),
    ] = None,
) -> None:
    """
    Export query results to CSV or Parquet.
    
    Supports SQL filtering and output format detection from extension.
    """
    from tolteca_db.db import get_engine
    import pandas as pd
    
    engine = get_engine(db_url)
    
    console.print(f"[bold blue]Exporting {table}...[/bold blue]")
    
    # Build query
    sql = f"SELECT * FROM {table}"
    if filters:
        sql += f" WHERE {filters}"
    if limit:
        sql += f" LIMIT {limit}"
    
    console.print(f"Query: {sql}")
    
    # Execute query and load into DataFrame
    df = pd.read_sql(sql, engine)
    
    console.print(f"Loaded {len(df)} rows")
    
    # Export based on file extension
    if output_file.suffix.lower() == ".csv":
        df.to_csv(output_file, index=False)
        console.print(f"[green]✓[/green] Exported to CSV: {output_file}")
    elif output_file.suffix.lower() == ".parquet":
        df.to_parquet(output_file, index=False)
        console.print(f"[green]✓[/green] Exported to Parquet: {output_file}")
    else:
        console.print(f"[red]Error:[/red] Unsupported format: {output_file.suffix}")
        console.print("Use .csv or .parquet extension")
        raise typer.Exit(code=1)


@query_app.command(name="stats")
def database_stats(
    db_url: Annotated[
        Optional[str],
        typer.Option("--db", help="Database URL"),
    ] = None,
) -> None:
    """
    Display comprehensive database statistics.
    
    Shows counts by type, data kinds, locations, and associations.
    """
    from tolteca_db.db import get_engine
    from sqlalchemy.orm import Session
    from sqlalchemy import func
    from tolteca_db.models.orm import DataProd, DataProdSource, DataProdAssoc, DataProdDataKind
    
    engine = get_engine(db_url)
    
    console.print("[bold blue]Database Statistics[/bold blue]\n")
    
    with Session(engine) as session:
        # Total counts
        total_products = session.query(DataProd).count()
        total_sources = session.query(DataProdSource).count()
        total_assocs = session.query(DataProdAssoc).count()
        
        console.print(f"Total DataProds: {total_products}")
        console.print(f"Total Sources: {total_sources}")
        console.print(f"Total Associations: {total_assocs}\n")
        
        # By product type
        type_counts = (
            session.query(
                DataProd.data_prod_type_fk,
                func.count(DataProd.pk)
            )
            .group_by(DataProd.data_prod_type_fk)
            .all()
        )
        
        if type_counts:
            table = Table(title="DataProds by Type")
            table.add_column("Type ID", style="cyan")
            table.add_column("Count", style="magenta", justify="right")
            
            type_names = {
                1: "dp_raw_obs",
                2: "dp_cal_group",
                3: "dp_drivefit",
                4: "dp_focus_group",
            }
            
            for type_fk, count in type_counts:
                type_name = type_names.get(type_fk, f"type_{type_fk}")
                table.add_row(f"{type_fk} ({type_name})", str(count))
            
            console.print(table)
        
        # Data kinds distribution
        kind_counts = (
            session.query(
                DataProdDataKind.data_kind_fk,
                func.count(DataProdDataKind.data_prod_fk)
            )
            .group_by(DataProdDataKind.data_kind_fk)
            .all()
        )
        
        if kind_counts:
            console.print()
            table = Table(title="Data Kind Distribution")
            table.add_column("Kind ID", style="cyan")
            table.add_column("Count", style="magenta", justify="right")
            
            for kind_fk, count in kind_counts:
                table.add_row(str(kind_fk), str(count))
            
            console.print(table)
