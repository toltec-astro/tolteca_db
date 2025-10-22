"""Console script for tolteca_db."""

from __future__ import annotations

import typer
from rich.console import Console

from tolteca_db.cli.commands import flag_command, ingest_command, query_command

app = typer.Typer(
    name="tolteca_db",
    help="TolTEC Data Product Database CLI",
    no_args_is_help=True,
)
console = Console()


@app.command(name="ingest")
def ingest(*args, **kwargs):
    """Ingest a data product file into the database."""
    return ingest_command(*args, **kwargs)


@app.command(name="query")
def query(*args, **kwargs):
    """Query data products from the database."""
    return query_command(*args, **kwargs)


@app.command(name="flag")
def flag(*args, **kwargs):
    """Manage data product flags."""
    return flag_command(*args, **kwargs)


if __name__ == "__main__":
    app()
