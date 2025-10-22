"""CLI commands for tolteca_db."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.table import Table

from tolteca_db.db import get_session
from tolteca_db.models.schemas import (
    DataProductCreate,
    DataProductFlagCreate,
    FlagDefinitionCreate,
)

console = Console()


def ingest_command(
    file_path: Annotated[Path, typer.Argument(help="Path to data file to ingest")],
    location: Annotated[
        str, typer.Option("--location", "-l", help="Location label for the file")
    ] = "LMT Data Archive",
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Parse and validate without ingesting")
    ] = False,
) -> None:
    """
    Ingest a data product file into the database.

    Parses the filename to extract metadata, validates the data product,
    and creates database records for the file and its storage location.
    """
    from tolteca_db.services.ingest import IngestService

    console.print(f"[bold blue]Ingesting file:[/bold blue] {file_path}")

    if not file_path.exists():
        console.print(f"[bold red]Error:[/bold red] File not found: {file_path}")
        raise typer.Exit(code=1)

    if dry_run:
        console.print("[yellow]Dry run mode - validation only[/yellow]")

    with get_session() as session:
        service = IngestService(session)
        try:
            result = service.ingest_file(file_path, location, dry_run=dry_run)

            if dry_run:
                console.print("[green]✓[/green] Validation successful")
                console.print(f"  Product type: {result['base_type']}")
                console.print(f"  Metadata: {result['metadata']}")
            else:
                console.print(
                    f"[green]✓[/green] Ingested: {result['product_pk'][:16]}..."
                )
                console.print(f"  Type: {result['base_type']}")
                console.print(f"  Location: {result['location']}")

        except ValueError as e:
            console.print(f"[bold red]Validation error:[/bold red] {e}")
            raise typer.Exit(code=1)


def query_command(
    base_type: Annotated[
        Optional[str],
        typer.Option("--type", "-t", help="Filter by product base type"),
    ] = None,
    location: Annotated[
        Optional[str], typer.Option("--location", "-l", help="Filter by location label")
    ] = None,
    limit: Annotated[int, typer.Option("--limit", "-n", help="Maximum results")] = 10,
    with_storage: Annotated[
        bool,
        typer.Option("--with-storage", help="Include storage location information"),
    ] = False,
) -> None:
    """
    Query data products from the database.

    Supports filtering by product type and location, with optional
    eager loading of storage information.
    """
    from tolteca_db.db.repositories import DataProductRepository

    console.print("[bold blue]Querying data products...[/bold blue]")

    with get_session() as session:
        repo = DataProductRepository(session)

        # Build filter conditions
        filters = {}
        if base_type:
            filters["base_type"] = base_type
        if location:
            filters["location_label"] = location

        # Execute query
        if with_storage:
            products = repo.get_with_storage(limit=limit, **filters)
        else:
            products = repo.get_all(limit=limit)

        if not products:
            console.print("[yellow]No products found[/yellow]")
            return

        # Display results in table
        table = Table(title=f"Data Products ({len(products)} results)")
        table.add_column("Product PK (truncated)", style="cyan")
        table.add_column("Base Type", style="magenta")
        table.add_column("Status", style="green")
        if with_storage:
            table.add_column("Storage Path", style="blue")

        for product in products:
            pk_short = product.product_pk[:16] + "..."
            row = [pk_short, product.base_type, product.status]
            if with_storage and hasattr(product, "storage") and product.storage:
                storage_path = (
                    product.storage[0].file_path if product.storage else "N/A"
                )
                row.append(str(storage_path))
            table.add_row(*row)

        console.print(table)


def flag_command(
    action: Annotated[
        str,
        typer.Argument(help="Action: 'create' to define new flag, 'set' to flag product"),
    ],
    name: Annotated[Optional[str], typer.Option("--name", help="Flag definition name")] = None,
    product_pk: Annotated[
        Optional[str], typer.Option("--product", help="Product PK to flag")
    ] = None,
    reason: Annotated[Optional[str], typer.Option("--reason", help="Flag reason")] = None,
) -> None:
    """
    Manage data product flags.

    Two actions:
    - create: Define a new flag type
    - set: Apply a flag to a data product
    """
    from tolteca_db.db.repositories import (
        DataProductFlagRepository,
        FlagDefinitionRepository,
    )

    if action not in ["create", "set"]:
        console.print("[bold red]Error:[/bold red] Action must be 'create' or 'set'")
        raise typer.Exit(code=1)

    with get_session() as session:
        if action == "create":
            if not name:
                console.print(
                    "[bold red]Error:[/bold red] --name required for create action"
                )
                raise typer.Exit(code=1)

            flag_def_data = FlagDefinitionCreate(
                flag_key=name,
                group_key=name.split("_")[0] if "_" in name else "QA",
                severity="WARN",
                description=reason or f"Flag: {name}",
            )

            repo = FlagDefinitionRepository(session)
            flag_def = repo.create_from_schema(flag_def_data)
            session.commit()

            console.print(f"[green]✓[/green] Created flag definition: {flag_def.flag_key}")

        elif action == "set":
            if not name or not product_pk:
                console.print(
                    "[bold red]Error:[/bold red] --name and --product required for set action"
                )
                raise typer.Exit(code=1)

            # Find flag definition
            flag_repo = FlagDefinitionRepository(session)
            flag_def = flag_repo.get_by_key(name)
            if not flag_def:
                console.print(f"[bold red]Error:[/bold red] Flag definition '{name}' not found")
                raise typer.Exit(code=1)

            # Create flag
            flag_data = DataProductFlagCreate(
                product_fk=product_pk,
                flag_key=name,
                asserted_by=reason or "CLI",
                details={},
            )

            flag_repo_dp = DataProductFlagRepository(session)
            flag = flag_repo_dp.create_from_schema(flag_data)
            session.commit()

            console.print(
                f"[green]✓[/green] Flagged product {product_pk[:16]}... with '{name}'"
            )
