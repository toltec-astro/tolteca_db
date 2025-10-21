"""Console script for tolteca_db."""

import typer
from rich.console import Console

app = typer.Typer()
console = Console()


@app.command()
def main():
    """Console script for tolteca_db."""
    console.print("This is tolteca_db CLI.")


if __name__ == "__main__":
    app()
