"""Association generation commands."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from rich.table import Table

console = Console()

assoc_app = typer.Typer(
    name="assoc",
    help="Association generation operations",
    no_args_is_help=True,
)


@assoc_app.command(name="generate")
def generate_associations(
    db_url: Annotated[
        Optional[str],
        typer.Option("--db", help="Database URL"),
    ] = None,
    n_observations: Annotated[
        Optional[int],
        typer.Option("--limit", "-n", help="Limit observations to process"),
    ] = None,
    incremental: Annotated[
        bool,
        typer.Option("--incremental/--full", help="Use incremental state tracking"),
    ] = True,
    state_backend: Annotated[
        str,
        typer.Option("--state", help="State backend: 'database' or 'filesystem'"),
    ] = "database",
    state_dir: Annotated[
        Optional[Path],
        typer.Option("--state-dir", help="Directory for filesystem state (if using filesystem backend)"),
    ] = None,
    commit: Annotated[
        bool,
        typer.Option("--commit/--dry-run", help="Commit changes to database"),
    ] = True,
) -> None:
    """
    Generate associations from raw observations.
    
    Supports both full rescan and incremental processing with state tracking.
    """
    from tolteca_db.db import get_engine
    from sqlalchemy.orm import Session
    from tolteca_db.associations import (
        AssociationGenerator,
        AssociationState,
        DatabaseBackend,
        FilesystemBackend,
    )
    
    engine = get_engine(db_url)
    
    console.print("[bold blue]Generating associations...[/bold blue]")
    console.print(f"Mode: {'Incremental' if incremental else 'Full rescan'}")
    console.print(f"State backend: {state_backend}")
    
    with Session(engine) as session:
        # Setup state if incremental
        state = None
        if incremental:
            if state_backend == "database":
                state = AssociationState(DatabaseBackend(session))
                console.print("Using database state backend")
            elif state_backend == "filesystem":
                if not state_dir:
                    state_dir = Path.cwd() / ".tolteca_db_state"
                state_dir.mkdir(parents=True, exist_ok=True)
                state = AssociationState(FilesystemBackend(state_dir))
                console.print(f"Using filesystem state: {state_dir}")
            else:
                console.print(f"[red]Error:[/red] Unknown state backend: {state_backend}")
                raise typer.Exit(code=1)
        
        # Create generator
        generator = AssociationGenerator(session, state=state)
        
        # Generate associations
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Processing...", total=None)
            
            stats = generator.generate_associations(
                n_observations=n_observations,
                incremental=incremental,
                commit=commit,
            )
            
            progress.update(task, completed=True)
        
        if commit:
            session.commit()
            console.print("[green]✓[/green] Changes committed")
        else:
            console.print("[yellow]Dry run - changes not committed[/yellow]")
    
    # Display statistics
    table = Table(title="Association Statistics")
    table.add_column("Metric", style="cyan")
    table.add_column("Count", style="magenta", justify="right")
    
    table.add_row("Observations Scanned", str(stats.observations_scanned))
    if incremental:
        table.add_row("Already Grouped", str(stats.observations_already_grouped))
        table.add_row("Observations Processed", str(stats.observations_processed))
    table.add_row("Groups Created", str(stats.groups_created))
    if incremental:
        table.add_row("Groups Updated", str(stats.groups_updated))
    table.add_row("Associations Created", str(stats.associations_created))
    
    console.print(table)
    
    # Performance note
    if incremental and stats.observations_already_grouped > 0:
        skip_pct = (stats.observations_already_grouped / stats.observations_scanned) * 100
        console.print(f"\n[green]Performance:[/green] Skipped {skip_pct:.1f}% of observations (already grouped)")


@assoc_app.command(name="stream")
def stream_associations(
    db_url: Annotated[
        Optional[str],
        typer.Option("--db", help="Database URL"),
    ] = None,
    batch_size: Annotated[
        int,
        typer.Option("--batch-size", help="Observations per batch"),
    ] = 20,
    commit_every: Annotated[
        int,
        typer.Option("--commit-every", help="Commit every N batches"),
    ] = 5,
    max_batches: Annotated[
        Optional[int],
        typer.Option("--max-batches", help="Stop after N batches (for testing)"),
    ] = None,
) -> None:
    """
    Stream processing of observations with automatic batching.
    
    Continuously processes new observations in batches with periodic commits.
    """
    from tolteca_db.db import get_engine
    from sqlalchemy.orm import Session
    from tolteca_db.associations import AssociationGenerator
    
    engine = get_engine(db_url)
    
    console.print("[bold blue]Streaming association generation...[/bold blue]")
    console.print(f"Batch size: {batch_size}, Commit every: {commit_every} batches")
    
    with Session(engine) as session:
        generator = AssociationGenerator(session)
        
        # Query observations in batches
        from tolteca_db.models.orm import DataProd
        
        def observation_iterator():
            offset = 0
            while True:
                batch = (
                    session.query(DataProd)
                    .filter(DataProd.data_prod_type_fk == 1)  # dp_raw_obs
                    .offset(offset)
                    .limit(batch_size)
                    .all()
                )
                
                if not batch:
                    break
                
                yield batch
                offset += batch_size
        
        batch_count = 0
        total_created = 0
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Processing batches...", total=max_batches)
            
            for stats in generator.generate_streaming(
                observation_iterator(),
                batch_size=batch_size,
                commit_every=commit_every,
            ):
                batch_count += 1
                total_created += stats.groups_created
                
                progress.update(
                    task,
                    advance=1,
                    description=f"Batch {batch_count}: {stats.groups_created} groups created",
                )
                
                if max_batches and batch_count >= max_batches:
                    break
        
        session.commit()
    
    console.print(f"[green]✓[/green] Processed {batch_count} batches")
    console.print(f"Total groups created: {total_created}")


@assoc_app.command(name="state")
def show_state(
    db_url: Annotated[
        Optional[str],
        typer.Option("--db", help="Database URL"),
    ] = None,
    state_backend: Annotated[
        str,
        typer.Option("--backend", help="State backend: 'database' or 'filesystem'"),
    ] = "database",
    state_dir: Annotated[
        Optional[Path],
        typer.Option("--state-dir", help="Directory for filesystem state"),
    ] = None,
) -> None:
    """
    Display current association state statistics.
    
    Shows number of grouped observations and existing groups.
    """
    from tolteca_db.db import get_engine
    from sqlalchemy.orm import Session
    from tolteca_db.associations import (
        AssociationState,
        DatabaseBackend,
        FilesystemBackend,
    )
    
    console.print("[bold blue]Association State[/bold blue]")
    console.print(f"Backend: {state_backend}")
    
    if state_backend == "database":
        engine = get_engine(db_url)
        with Session(engine) as session:
            state = AssociationState(DatabaseBackend(session))
            stats = state.stats()
    elif state_backend == "filesystem":
        if not state_dir:
            state_dir = Path.cwd() / ".tolteca_db_state"
        
        if not state_dir.exists():
            console.print(f"[yellow]Warning:[/yellow] State directory not found: {state_dir}")
            console.print("Run 'assoc generate --incremental --state filesystem' first")
            return
        
        state = AssociationState(FilesystemBackend(state_dir))
        stats = state.stats()
        console.print(f"State directory: {state_dir}")
    else:
        console.print(f"[red]Error:[/red] Unknown backend: {state_backend}")
        raise typer.Exit(code=1)
    
    # Display statistics
    table = Table(title="State Statistics")
    table.add_column("Metric", style="cyan")
    table.add_column("Count", style="magenta", justify="right")
    
    table.add_row("Grouped Observations", str(stats["n_grouped_observations"]))
    table.add_row("Registered Groups", str(stats["n_groups"]))
    
    console.print(table)


@assoc_app.command(name="reset-state")
def reset_state(
    state_backend: Annotated[
        str,
        typer.Option("--backend", help="State backend: 'filesystem' only"),
    ] = "filesystem",
    state_dir: Annotated[
        Optional[Path],
        typer.Option("--state-dir", help="Directory for filesystem state"),
    ] = None,
    confirm: Annotated[
        bool,
        typer.Option("--yes", "-y", help="Skip confirmation prompt"),
    ] = False,
) -> None:
    """
    Reset filesystem state (delete state files).
    
    Note: Database state is always live and cannot be reset without
    deleting actual DataProd entries.
    """
    if state_backend != "filesystem":
        console.print("[red]Error:[/red] Can only reset filesystem state")
        console.print("Database state is always live - delete DataProd entries to reset")
        raise typer.Exit(code=1)
    
    if not state_dir:
        state_dir = Path.cwd() / ".tolteca_db_state"
    
    if not state_dir.exists():
        console.print(f"[yellow]State directory doesn't exist:[/yellow] {state_dir}")
        return
    
    if not confirm:
        response = typer.confirm(f"Delete state directory: {state_dir}?")
        if not response:
            console.print("Cancelled")
            return
    
    import shutil
    shutil.rmtree(state_dir)
    
    console.print(f"[green]✓[/green] State directory removed: {state_dir}")
