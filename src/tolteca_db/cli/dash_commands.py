"""Dash web interface commands."""

from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console

dash_app = typer.Typer(
    name="dash",
    help="Web interface for database browsing",
    no_args_is_help=True,
)
console = Console()


@dash_app.command()
def serve(
    db_url: str = typer.Option(
        "duckdb:///tolteca.duckdb",
        "--db-url",
        "-d",
        help="Database URL to connect to",
    ),
    host: str = typer.Option(
        "127.0.0.1",
        "--host",
        "-h",
        help="Host to bind to",
    ),
    port: int = typer.Option(
        8050,
        "--port",
        "-p",
        help="Port to bind to",
    ),
    debug: bool = typer.Option(
        False,
        "--debug",
        help="Enable debug mode",
    ),
):
    """Start the Dash development server.
    
    Examples
    --------
    # Start with default database
    $ tolteca-db dash serve
    
    # Specify custom database
    $ tolteca-db dash serve --db-url duckdb:///path/to/db.duckdb
    
    # Enable debug mode
    $ tolteca-db dash serve --debug
    
    # Custom host and port
    $ tolteca-db dash serve --host 0.0.0.0 --port 9000
    """
    try:
        from tolteca_db.dash import create_app
    except ImportError as e:
        console.print(
            "[red]Error:[/red] Dash dependencies not installed. "
            "Install with: [cyan]uv pip install -e '.[dash]'[/cyan]"
        )
        raise typer.Exit(code=1) from e
    
    console.print(f"[green]Starting Dash server...[/green]")
    console.print(f"Database: [cyan]{db_url}[/cyan]")
    console.print(f"Server: [cyan]http://{host}:{port}[/cyan]")
    
    if debug:
        console.print("[yellow]Debug mode enabled[/yellow]")
    
    server = create_app(db_url=db_url, debug=debug)
    server.run(host=host, port=port, debug=debug)
