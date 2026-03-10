"""Dagster commands for running dev server and managing orchestration."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import typer
from dotenv import load_dotenv
from rich.console import Console

dagster_app = typer.Typer(
    name="dagster",
    help="Dagster orchestration commands",
    no_args_is_help=True,
)
console = Console()


@dagster_app.command("dev")
def dev(
    host: str = typer.Option("0.0.0.0", "--host", "-h", help="Host to bind to"),
    port: int = typer.Option(3000, "--port", "-p", help="Port to bind to"),
    env_file: Path = typer.Option(
        None, "--env-file", "-e", help="Path to .env file to load"
    ),
) -> None:
    """
    Run Dagster dev server with optional environment file loading.

    This command loads environment variables from an .env file (if provided)
    and starts the Dagster dev server. The environment variables are used
    for configuring database connections, file paths, and other settings.

    Example:
        tolteca_db dagster dev --env-file /path/to/dpv.env
    """
    console.print("[bold blue]Starting Dagster dev server...[/bold blue]")

    # Load environment file if provided
    if env_file:
        if not env_file.exists():
            console.print(f"[bold red]Error: Environment file not found: {env_file}[/bold red]")
            raise typer.Exit(1)

        console.print(f"[green]Loading environment from: {env_file}[/green]")
        # Use python-dotenv with interpolation for proper variable substitution
        # interpolate=True enables ${VAR} expansion within the .env file
        load_dotenv(env_file, override=True, interpolate=True)

    # Display configuration
    console.print("\n[bold]Configuration:[/bold]")
    console.print(f"  Host: {host}")
    console.print(f"  Port: {port}")
    if env_file:
        console.print(f"  Env File: {env_file}")

    # Display relevant environment variables
    _display_env_vars()

    # Create DAGSTER_HOME if needed
    dagster_home = os.getenv("DAGSTER_HOME", "dagster")
    Path(dagster_home).mkdir(parents=True, exist_ok=True)
    console.print(f"  Dagster Home: {dagster_home}\n")

    # Run dagster dev
    try:
        subprocess.run(
            ["dagster", "dev", "-h", host, "-p", str(port)],
            check=True,
        )
    except subprocess.CalledProcessError as e:
        console.print(f"[bold red]Error running Dagster: {e}[/bold red]")
        raise typer.Exit(1)
    except KeyboardInterrupt:
        console.print("\n[yellow]Dagster dev server stopped[/yellow]")
        raise typer.Exit(0)


def _display_env_vars() -> None:
    """Display relevant environment variables for Dagster."""
    relevant_vars = [
        "DAGSTER_HOME",
        "TOLTECA_DB_URL",
        "TOLTEC_DB_URL",
        "TOLTECA_WEB_TOLTEC_DB_URL",
        "TOLTECA_WEB_TOLTECA_DB_URL",
        "TOLTECA_WEB_DATA_LMT_ROOTPATH",
        "TOLTECA_WEB_DATA_PROD_OUTPUT_PATH",
    ]

    console.print("\n[bold]Environment Variables:[/bold]")
    for var in relevant_vars:
        value = os.getenv(var)
        if value:
            # Truncate long paths for display
            display_value = value
            if len(display_value) > 60:
                display_value = "..." + display_value[-57:]
            console.print(f"  {var}: {display_value}")
