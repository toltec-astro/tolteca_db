"""Console script for tolteca_db."""

from __future__ import annotations

import typer
from rich.console import Console

app = typer.Typer(
    name="tolteca_db",
    help="TolTEC Data Product Database CLI - Comprehensive data management interface",
    no_args_is_help=True,
)
console = Console()

# Import subcommand apps
from tolteca_db.cli.db_commands import db_app
from tolteca_db.cli.ingest_commands import ingest_app
from tolteca_db.cli.assoc_commands import assoc_app
from tolteca_db.cli.query_commands import query_app
from tolteca_db.cli.dash_commands import dash_app

# Register subcommands
app.add_typer(db_app, name="db", help="Database management operations")
app.add_typer(ingest_app, name="ingest", help="Data ingestion operations")
app.add_typer(assoc_app, name="assoc", help="Association generation operations")
app.add_typer(query_app, name="query", help="Query and export operations")
app.add_typer(dash_app, name="dash", help="Web interface for database browsing")


if __name__ == "__main__":
    app()
