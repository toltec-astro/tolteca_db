"""Data ingestion commands."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from rich.table import Table
from sqlalchemy.orm import Session

from tolteca_db.ingest import guess_info_from_file

console = Console()

ingest_app = typer.Typer(
    name="ingest",
    help="Data ingestion operations",
    no_args_is_help=True,
)


@ingest_app.command(name="file")
def ingest_file(
    file_path: Annotated[
        Path,
        typer.Argument(help="Path to TolTEC data file"),
    ],
    location: Annotated[
        str,
        typer.Option("--location", "-l", help="Location identifier"),
    ] = "LMT",
    master: Annotated[
        str,
        typer.Option("--master", help="Master network identifier"),
    ] = "toltec",
    nw_id: Annotated[
        int,
        typer.Option("--nw-id", help="Network ID"),
    ] = 0,
    db_url: Annotated[
        Optional[str],
        typer.Option("--db", help="Database URL"),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Parse only, don't write to DB"),
    ] = False,
) -> None:
    """
    Ingest a single TolTEC data file.
    
    Parses filename, creates DataProd and DataProdSource entries,
    links DataKind classifications.
    """
    from tolteca_db.db import get_engine
    from sqlalchemy.orm import Session
    from tolteca_db.ingest import DataIngestor, guess_info_from_file
    
    if not file_path.exists():
        console.print(f"[red]Error:[/red] File not found: {file_path}")
        raise typer.Exit(code=1)
    
    # Parse filename first
    console.print(f"[bold blue]Parsing:[/bold blue] {file_path.name}")
    
    try:
        parsed_info = guess_info_from_file(file_path)
        
        # Display parsed info
        table = Table(title="Parsed Metadata")
        table.add_column("Field", style="cyan")
        table.add_column("Value", style="magenta")
        
        table.add_row("Interface", parsed_info.interface)
        table.add_row("Roach ID", str(parsed_info.roach) if parsed_info.roach is not None else "N/A")
        table.add_row("ObsNum", str(parsed_info.obsnum))
        table.add_row("SubObsNum", str(parsed_info.subobsnum))
        table.add_row("ScanNum", str(parsed_info.scannum))
        table.add_row("Data Kind", parsed_info.data_kind.name if parsed_info.data_kind else "N/A")
        
        console.print(table)
        
        if dry_run:
            console.print("[yellow]Dry run - not writing to database[/yellow]")
            return
        
        # Ingest to database
        engine = get_engine(db_url)
        with Session(engine) as session:
            ingestor = DataIngestor(
                session=session,
                location_pk=location,
                master=master,
                nw_id=nw_id,
            )
            
            stats = ingestor.ingest_file(file_path)
            session.commit()
            
            console.print(f"[green]✓[/green] Ingested successfully")
            console.print(f"  DataProds created: {stats.data_prods_created}")
            console.print(f"  Sources created: {stats.sources_created}")
            
    except ValueError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(code=1)


@ingest_app.command(name="directory")
def ingest_directory(
    root_path: Annotated[
        Path,
        typer.Argument(help="Root directory to scan"),
    ],
    location: Annotated[
        str,
        typer.Option("--location", "-l", help="Location identifier"),
    ] = "LMT",
    master: Annotated[
        str,
        typer.Option("--master", help="Master network identifier"),
    ] = "toltec",
    nw_id: Annotated[
        int,
        typer.Option("--nw-id", help="Network ID"),
    ] = 0,
    pattern: Annotated[
        str,
        typer.Option("--pattern", "-p", help="File glob pattern"),
    ] = "*.nc",
    recursive: Annotated[
        bool,
        typer.Option("--recursive/--no-recursive", "-r", help="Scan subdirectories"),
    ] = True,
    skip_existing: Annotated[
        bool,
        typer.Option("--skip-existing", help="Skip files already in database"),
    ] = True,
    db_url: Annotated[
        Optional[str],
        typer.Option("--db", help="Database URL"),
    ] = None,
    commit_interval: Annotated[
        int,
        typer.Option("--commit-interval", help="Commit every N files"),
    ] = 100,
    with_associations: Annotated[
        bool,
        typer.Option("--with-associations", help="Generate associations after ingestion"),
    ] = False,
) -> None:
    """
    Ingest all TolTEC files in a directory.
    
    Scans directory recursively, parses filenames, and creates
    database entries for all matching files.
    
    Use --with-associations to automatically generate associations
    (CalGroup, DriveFit, FocusGroup) after ingestion completes.
    """
    from tolteca_db.db import get_engine
    from sqlalchemy.orm import Session
    from tolteca_db.ingest import DataIngestor
    
    if not root_path.exists():
        console.print(f"[red]Error:[/red] Directory not found: {root_path}")
        raise typer.Exit(code=1)
    
    console.print(f"[bold blue]Scanning:[/bold blue] {root_path}")
    console.print(f"Pattern: {pattern}, Recursive: {recursive}")
    
    engine = get_engine(db_url)
    
    with Session(engine) as session:
        ingestor = DataIngestor(
            session=session,
            location_pk=location,
            master=master,
            nw_id=nw_id,
        )
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Ingesting files...", total=None)
            
            stats = ingestor.ingest_directory(
                root_path=root_path,
                pattern=pattern,
                recursive=recursive,
                skip_existing=skip_existing,
                commit_interval=commit_interval,
            )
            
            progress.update(task, completed=True)
        
        session.commit()
    
    # Display results
    table = Table(title="Ingestion Statistics")
    table.add_column("Metric", style="cyan")
    table.add_column("Count", style="magenta", justify="right")
    
    table.add_row("Files Scanned", str(stats.files_scanned))
    table.add_row("Files Ingested", str(stats.files_ingested))
    table.add_row("Files Skipped", str(stats.files_skipped))
    table.add_row("Files Failed", str(stats.files_failed))
    table.add_row("DataProds Created", str(stats.data_prods_created))
    table.add_row("Sources Created", str(stats.sources_created))
    
    console.print(table)
    
    if stats.files_failed > 0:
        console.print("[yellow]Warning:[/yellow] Some files failed to ingest")
    
    # Generate associations if requested
    if with_associations:
        _generate_associations_after_ingest(engine, stats.data_prods_created)


@ingest_app.command(name="scan")
def scan_directory(
    root_path: Annotated[
        Path,
        typer.Argument(help="Directory to scan"),
    ],
    pattern: Annotated[
        str,
        typer.Option("--pattern", "-p", help="File glob pattern"),
    ] = "*.nc",
    recursive: Annotated[
        bool,
        typer.Option("--recursive/--no-recursive", "-r", help="Scan subdirectories"),
    ] = True,
    show_details: Annotated[
        bool,
        typer.Option("--details", help="Show parsed metadata for each file"),
    ] = False,
) -> None:
    """
    Scan directory and display parseable files (dry run).
    
    Shows which files can be parsed without writing to database.
    """
    from tolteca_db.ingest import FileScanner, guess_info_from_file
    
    if not root_path.exists():
        console.print(f"[red]Error:[/red] Directory not found: {root_path}")
        raise typer.Exit(code=1)
    
    console.print(f"[bold blue]Scanning:[/bold blue] {root_path}")
    
    scanner = FileScanner()
    files = list(scanner.scan_directory(root_path, pattern=pattern, recursive=recursive))
    
    console.print(f"\n[bold]Found {len(files)} files[/bold]")
    
    if show_details and files:
        table = Table(title="Parsed Files")
        table.add_column("Filename", style="cyan")
        table.add_column("ObsNum", style="magenta")
        table.add_column("Interface", style="green")
        table.add_column("Data Kind", style="blue")
        
        for file_path in files[:50]:  # Limit to 50 for display
            try:
                info = guess_info_from_file(file_path)
                table.add_row(
                    file_path.name,
                    str(info.obsnum),
                    info.interface,
                    info.data_kind.name if info.data_kind else "N/A",
                )
            except ValueError:
                table.add_row(file_path.name, "PARSE ERROR", "", "")
        
        console.print(table)
        
        if len(files) > 50:
            console.print(f"[yellow]...(showing first 50 of {len(files)} files)[/yellow]")


@ingest_app.command(name="from-toltec-db")
def ingest_from_toltec_db(
    toltec_db: Annotated[
        Path,
        typer.Argument(help="Path to toltec_db SQLite database"),
    ],
    data_root: Annotated[
        Path,
        typer.Option("--data-root", help="Root directory containing raw files"),
    ],
    target_url: Annotated[
        Optional[str],
        typer.Option("--db", help="Target database URL"),
    ] = None,
    location: Annotated[
        str,
        typer.Option("--location", help="Location label (e.g., LMT, local)"),
    ] = "LMT",
    location_root: Annotated[
        Optional[Path],
        typer.Option("--location-root", help="Location root path (for source_uri calculation, defaults to --data-root)"),
    ] = None,
    master: Annotated[
        str,
        typer.Option("--master", help="Master identifier (tcs/ics/clip)"),
    ] = "tcs",
    obstype_filter: Annotated[
        Optional[str],
        typer.Option("--obstype", help="Filter by obs type (Nominal/VNA/TARG/TUNE)"),
    ] = None,
    limit: Annotated[
        Optional[int],
        typer.Option("--limit", help="Limit number of files to ingest"),
    ] = None,
    skip_existing: Annotated[
        bool,
        typer.Option("--skip-existing/--no-skip-existing", help="Skip files already in tolteca_db"),
    ] = True,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Preview without ingesting"),
    ] = False,
    create_location: Annotated[
        bool,
        typer.Option("--create-location/--no-create-location", help="Create location if it doesn't exist"),
    ] = True,
    with_associations: Annotated[
        bool,
        typer.Option("--with-associations", help="Generate associations after ingestion"),
    ] = False,
) -> None:
    """
    Ingest files from toltec_db (real-time acquisition database).
    
    The toltec_db SQLite database is created during telescope observations
    and tracks all raw files as they're acquired. This command reads the
    toltec_db registry and ingests the corresponding files into tolteca_db.
    
    The --data-root specifies where to find the files on the current system.
    The --location-root (defaults to --data-root) specifies the root path 
    that will be stored in the Location.root_uri for computing relative paths.
    
    Use --with-associations to automatically generate associations
    (CalGroup, DriveFit, FocusGroup) after ingestion completes.
    
    Examples:
        # Ingest all files from toltec_db
        tolteca_db ingest from-toltec-db run/toltecdb_last_30days.sql \\
            --data-root /data_lmt/toltec/tcs
        
        # Dry-run to preview
        tolteca_db ingest from-toltec-db run/toltecdb_last_30days.sql \\
            --data-root /data_lmt/toltec/tcs --dry-run
        
        # With automatic association generation
        tolteca_db ingest from-toltec-db run/toltecdb_last_30days.sql \\
            --data-root /data_lmt/toltec/tcs --with-associations
        
        # Different location root for relative paths
        tolteca_db ingest from-toltec-db run/toltecdb_last_30days.sql \\
            --data-root /local/scratch/data \\
            --location-root /data_lmt/toltec/tcs \\
            --location local_scratch
    """
    import sqlite3
    from tolteca_db.db import get_engine, get_session
    from tolteca_db.ingest.ingest import DataIngestor
    from tolteca_db.ingest.file_scanner import guess_info_from_file
    from tolteca_db.models.orm import Location
    from sqlalchemy import select
    from rich.progress import Progress
    
    if not toltec_db.exists():
        console.print(f"[red]Error:[/red] toltec_db not found: {toltec_db}")
        raise typer.Exit(code=1)
    
    if not data_root.exists():
        console.print(f"[red]Error:[/red] data_root not found: {data_root}")
        raise typer.Exit(code=1)
    
    # Use absolute() to get absolute path without following symlinks,
    # then normalize to remove .. components
    import os
    toltec_db = Path(os.path.normpath(toltec_db.absolute()))
    data_root = Path(os.path.normpath(data_root.absolute()))
    
    # Use data_root as location_root if not specified
    if location_root is None:
        location_root = data_root
    else:
        location_root = Path(os.path.normpath(location_root.absolute()))
    
    console.print(f"[bold blue]Ingesting from toltec_db:[/bold blue] {toltec_db}")
    console.print(f"Data root: {data_root}")
    console.print(f"Location root: {location_root}")
    console.print(f"Location: {location}, Master: {master}\n")
    
    # Connect to toltec_db (source)
    toltec_conn = sqlite3.connect(str(toltec_db))
    toltec_conn.row_factory = sqlite3.Row
    cursor = toltec_conn.cursor()
    
    # Build query for toltec table
    query = """
        SELECT t.*, o.label as obstype_label
        FROM toltec t
        LEFT JOIN obstype o ON t.ObsType = o.id
        WHERE t.Valid = 1
    """
    
    params = []
    if obstype_filter:
        query += " AND o.label = ?"
        params.append(obstype_filter)
    
    if limit:
        query += f" LIMIT {limit}"
    
    # Execute query
    cursor.execute(query, params)
    rows = cursor.fetchall()
    
    console.print(f"Found {len(rows)} valid entries in toltec_db\n")
    
    if dry_run:
        # Preview mode
        table = Table(title="Preview (Dry Run)")
        table.add_column("ObsNum", style="magenta", justify="right")
        table.add_column("SubObs", style="blue", justify="right")
        table.add_column("Scan", style="blue", justify="right")
        table.add_column("ObsType", style="green")
        table.add_column("FileName", style="cyan", overflow="fold")
        
        for row in rows[:50]:  # Show first 50
            table.add_row(
                str(row['ObsNum']),
                str(row['SubObsNum']),
                str(row['ScanNum']),
                row['obstype_label'] or "?",
                row['FileName'],
            )
        
        console.print(table)
        if len(rows) > 50:
            console.print(f"\n[yellow]Note:[/yellow] Showing first 50 of {len(rows)} entries")
        console.print("\n[yellow]Dry run complete.[/yellow] Use --no-dry-run to ingest.")
        toltec_conn.close()
        return
    
    # Actual ingestion
    engine = get_engine(target_url)
    
    with Session(engine) as session:
        # Ensure location exists with correct root_uri
        stmt = select(Location).where(Location.label == location)
        loc = session.scalar(stmt)
        
        if loc is None:
            if not create_location:
                console.print(f"[red]Error:[/red] Location '{location}' not found and --no-create-location specified")
                toltec_conn.close()
                raise typer.Exit(code=1)
            
            # Create location
            console.print(f"Creating location '{location}' with root: file://{location_root}")
            loc = Location(
                label=location,
                location_type="filesystem",
                root_uri=f"file://{location_root}",
                priority=100,
            )
            session.add(loc)
            session.flush()
        else:
            # Verify root_uri matches
            expected_root = f"file://{location_root}"
            if loc.root_uri != expected_root:
                console.print(f"[yellow]Warning:[/yellow] Location root_uri mismatch:")
                console.print(f"  Expected: {expected_root}")
                console.print(f"  Actual:   {loc.root_uri}")
                console.print(f"  Files will be stored relative to: {loc.root_uri}")
        
        ingestor = DataIngestor(
            session=session,
            location_pk=location,
            master=master,
            nw_id=0,
        )
        
        ingested = 0
        skipped = 0
        failed = 0
        missing = 0
        
        with Progress() as progress:
            task = progress.add_task("[cyan]Ingesting files...", total=len(rows))
            
            for row in rows:
                # Construct file path from toltec_db FileName
                filename = row['FileName']
                
                # SQLite filenames: /data_lmt/toltec/tcs/toltec0/file.nc
                # data_root: ../run/data_lmt/toltec/tcs
                # We need to strip the common prefix and make relative to data_root
                
                # First, strip /data_lmt/ prefix from SQLite path
                if filename.startswith('/data_lmt/'):
                    filename_rel = filename[len('/data_lmt/'):]  # toltec/tcs/toltec0/file.nc
                elif filename.startswith('/data_lmt'):
                    filename_rel = filename[len('/data_lmt'):].lstrip('/')
                else:
                    filename_rel = filename
                
                # Now check if data_root contains a subpath that overlaps
                # data_root might be: /abs/path/data_lmt/toltec/tcs
                # filename_rel: toltec/tcs/toltec0/file.nc
                # Need to find the overlap and strip it from filename_rel
                
                # data_root is already resolved to absolute path
                data_root_parts = data_root.parts
                filename_parts = Path(filename_rel).parts
                
                # Find the longest suffix of data_root_parts that matches a prefix of filename_parts
                # Example: data_root ends with (..., 'toltec', 'tcs')
                #          filename starts with ('toltec', 'tcs', 'toltec0', ...)
                #          We want to strip ('toltec', 'tcs') from filename
                overlap = 0
                for suffix_len in range(1, min(len(data_root_parts), len(filename_parts)) + 1):
                    # Check if the last suffix_len parts of data_root match the first suffix_len parts of filename
                    if data_root_parts[-suffix_len:] == filename_parts[:suffix_len]:
                        overlap = suffix_len
                
                # Strip overlapping parts from filename
                if overlap > 0:
                    filename_rel = str(Path(*filename_parts[overlap:]))
                
                file_path = data_root / filename_rel
                
                try:
                    # Parse file info from filename
                    file_info = guess_info_from_file(file_path)
                    if file_info is None:
                        console.print(f"[yellow]Warning:[/yellow] Could not parse filename: {file_path.name}")
                        failed += 1
                        progress.update(task, advance=1)
                        continue
                    
                    # Get observation datetime from toltec_db Date and Time columns
                    obs_datetime = None
                    if row['Date'] and row['Time']:
                        from datetime import datetime
                        try:
                            # Combine Date (YYYY-MM-DD) and Time (HH:MM:SS)
                            obs_datetime = datetime.fromisoformat(f"{row['Date']} {row['Time']}")
                        except (ValueError, TypeError):
                            pass
                    
                    # Override file_info.obs_datetime with toltec_db value
                    file_info.obs_datetime = obs_datetime
                    
                    # Ingest file (logical entry created even if file missing)
                    ingestor.ingest_file(file_info)
                    
                    if file_path.exists():
                        ingested += 1
                    else:
                        missing += 1
                    
                    # Commit periodically
                    if (ingested + missing) % 100 == 0:
                        session.commit()
                    
                except Exception as e:
                    console.print(f"[red]Error ingesting {file_path.name}:[/red] {e}")
                    failed += 1
                
                progress.update(task, advance=1)
        
        # Final commit
        session.commit()
        
        # Summary
        console.print(f"\n[green]✓[/green] Ingestion complete:")
        console.print(f"  Ingested: {ingested}")
        console.print(f"  Logical (missing files): {missing}")
        console.print(f"  Skipped (existing): {skipped}")
        console.print(f"  Failed: {failed}")
        
    toltec_conn.close()
    
    # Generate associations if requested
    if with_associations and not dry_run:
        _generate_associations_after_ingest(engine, ingested + missing)


def _generate_associations_after_ingest(engine, n_ingested: int) -> None:
    """
    Helper to generate associations after ingestion.
    
    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine
    n_ingested : int
        Number of data products ingested (for context)
    """
    from sqlalchemy.orm import Session
    from tolteca_db.associations import (
        AssociationGenerator,
        AssociationState,
        DatabaseBackend,
    )
    
    console.print("\n[bold blue]Generating associations...[/bold blue]")
    console.print("Mode: Incremental")
    console.print("State backend: database")
    
    with Session(engine) as session:
        # Setup state for incremental processing (matching assoc generate behavior)
        state = AssociationState(DatabaseBackend(session))
        console.print("Using database state backend")
        
        # Create generator with state
        generator = AssociationGenerator(session, state=state)
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Processing...", total=None)
            
            # Generate associations for all raw observations
            stats = generator.generate_associations(
                n_observations=None,  # Process all
                incremental=True,
                commit=False,
            )
            
            progress.update(task, completed=True)
        
        session.commit()
        console.print("[green]✓[/green] Changes committed")
    
    # Display association statistics (matching assoc generate output format)
    table = Table(title="Association Statistics")
    table.add_column("Metric", style="cyan")
    table.add_column("Count", style="magenta", justify="right")
    
    table.add_row("Observations Scanned", str(stats.observations_scanned))
    table.add_row("Already Grouped", str(stats.observations_already_grouped))
    table.add_row("Observations Processed", str(stats.observations_processed))
    table.add_row("Groups Created", str(stats.groups_created))
    table.add_row("Groups Updated", str(stats.groups_updated))
    table.add_row("Associations Created", str(stats.associations_created))
    
    console.print(table)
    
    # Performance note (matching assoc generate behavior)
    if stats.observations_already_grouped > 0:
        skip_pct = (stats.observations_already_grouped / stats.observations_scanned) * 100
        console.print(f"\n[green]Performance:[/green] Skipped {skip_pct:.1f}% of observations (already grouped)")


@ingest_app.command(name="from-tel-csv")
def from_tel_csv(
    csv_path: Annotated[
        Path,
        typer.Argument(help="Path to LMT telescope metadata CSV"),
    ],
    location: Annotated[
        str,
        typer.Option("--location", "-l", help="Location label for tel metadata"),
    ] = "LMT",
    skip_existing: Annotated[
        bool,
        typer.Option("--skip-existing", help="Skip observations with existing tel sources"),
    ] = True,
    create_data_prods: Annotated[
        bool,
        typer.Option("--create-data-prods/--no-create-data-prods", help="Create DataProd entries if they don't exist"),
    ] = True,
    commit_batch_size: Annotated[
        int,
        typer.Option("--batch-size", help="Commit every N sources"),
    ] = 100,
    db_url: Annotated[
        Optional[str],
        typer.Option("--db", help="Database URL"),
    ] = None,
) -> None:
    """
    Ingest LMT telescope metadata from CSV file.
    
    CSV format: lmtmc_toltec_metadata.csv from LMT metadata database.
    CSV now includes FileName and Valid columns, providing complete quartet metadata:
    - ObsNum.SubObsNum.ScanNum identification
    - Filename for file path construction
    - Valid flag for data validation status
    - Tel-specific metadata (tau, az/el, M1 zernikes, M2 offsets, etc.)
    
    This unified structure (similar to toltec_db) allows:
    - Creating DataProd entries from tel metadata alone
    - Generating DataProdSource entries with file paths
    - Updating RawObsMeta with denormalized telescope fields
    
    File paths are constructed from FileName column (e.g., /data_lmt/tel/tel_*.nc)
    and stored as relative URIs (e.g., tel/tel_*.nc).
    
    Example:
        tolteca_db ingest from-tel-csv run/lmtmc_toltec_metadata.csv \\
            --location LMT \\
            --db "duckdb:///tolteca.duckdb"
    """
    from tolteca_db.db import get_engine
    from sqlalchemy.orm import Session
    from tolteca_db.models.orm import Location
    from tolteca_db.ingest.tel_ingestor import TelCSVIngestor
    from sqlalchemy import select
    
    if not csv_path.exists():
        console.print(f"[red]Error:[/red] CSV file not found: {csv_path}")
        raise typer.Exit(code=1)
    
    console.print(f"[bold blue]Ingesting Tel Metadata:[/bold blue] {csv_path}")
    
    engine = get_engine(db_url)
    
    with Session(engine) as session:
        # Get or verify location exists
        stmt = select(Location).where(Location.label == location)
        loc = session.execute(stmt).scalar_one_or_none()
        
        if loc is None:
            console.print(f"[red]Error:[/red] Location '{location}' not found. Create it first.")
            console.print("[yellow]Hint:[/yellow] Use 'tolteca_db db init' to create locations")
            raise typer.Exit(code=1)
        
        # Ingest CSV
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Processing tel metadata...", total=None)
            
            ingestor = TelCSVIngestor(
                session=session,
                location_pk=loc.pk,
                skip_existing=skip_existing,
                create_data_prods=create_data_prods,
                commit_batch_size=commit_batch_size,
            )
            
            stats = ingestor.ingest_csv(csv_path)
            
            progress.update(task, completed=True)
        
        console.print("[green]✓[/green] Tel metadata ingestion complete")
    
    # Display results
    table = Table(title="Tel Metadata Ingestion Statistics")
    table.add_column("Metric", style="cyan")
    table.add_column("Count", style="magenta", justify="right")
    
    table.add_row("Rows Scanned", str(stats.rows_scanned))
    table.add_row("Rows Ingested", str(stats.rows_ingested))
    table.add_row("Rows Skipped", str(stats.rows_skipped))
    table.add_row("DataProds Created", str(stats.data_prods_created))
    table.add_row("DataProds Updated", str(stats.data_prods_updated))
    table.add_row("Sources Created", str(stats.sources_created))
    
    console.print(table)
    
    if stats.rows_failed > 0:
        console.print("[yellow]Warning:[/yellow] Some rows failed to ingest")
    
    if stats.rows_skipped > 0 and skip_existing:
        console.print(f"[dim]Note:[/dim] Skipped {stats.rows_skipped} rows (no matching DataProd or already exists)")
