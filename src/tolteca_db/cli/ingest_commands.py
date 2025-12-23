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
        Optional[str],
        typer.Argument(help="Path to toltec_db SQLite file or database URL (e.g., mysql+pymysql://...)"),
    ] = None,
    toltec_db_url: Annotated[
        Optional[str],
        typer.Option("--toltec-db-url", help="toltec_db database URL (alternative to positional arg)"),
    ] = None,
    data_root: Annotated[
        Optional[Path],
        typer.Option("--data-root", help="Root directory containing LMT raw data tree (e.g., /data_lmt). If not provided, will look up from Location table."),
    ] = None,
    target_url: Annotated[
        Optional[str],
        typer.Option("--db", help="Target database URL"),
    ] = None,
    location: Annotated[
        str,
        typer.Option("--location", help="Location label (e.g., LMT, local)"),
    ] = "LMT",
    master: Annotated[
        str,
        typer.Option("--master", help="Master identifier (tcs/ics/clip)"),
    ] = "tcs",
    obstype_filter: Annotated[
        Optional[str],
        typer.Option("--obstype", help="Filter by obs type (Nominal/VNA/TARG/TUNE)"),
    ] = None,
    start_date: Annotated[
        Optional[str],
        typer.Option("--start-date", help="Start date (YYYY-MM-DD)"),
    ] = None,
    end_date: Annotated[
        Optional[str],
        typer.Option("--end-date", help="End date (YYYY-MM-DD)"),
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
    with_associations: Annotated[
        bool,
        typer.Option("--with-associations", help="Generate associations after ingestion"),
    ] = False,
) -> None:
    """
    Ingest files from toltec_db (real-time acquisition database).
    
    The --data-root specifies the root directory containing the LMT raw data tree
    (e.g., /data_lmt, ./data_lmt, ~/data_lmt). If not provided, it will be looked
    up from the Location table using the --location name.
    
    If both --data-root and --location are provided and the location doesn't exist,
    a new location entry will be created automatically.
    
    Use --with-associations to automatically generate associations
    (CalGroup, DriveFit, FocusGroup) after ingestion completes.
    
    Examples:
        # Ingest with location from database
        tolteca_db ingest from-toltec-db --toltec-db-url mysql+pymysql://...
        
        # Ingest with explicit data_root
        tolteca_db ingest from-toltec-db \\
            --toltec-db-url mysql+pymysql://... \\
            --data-root /data_lmt
        
        # Dry-run to preview
        tolteca_db ingest from-toltec-db \\
            --toltec-db-url mysql+pymysql://... \\
            --dry-run
        
        # With automatic association generation
        tolteca_db ingest from-toltec-db \\
            --toltec-db-url mysql+pymysql://... \\
            --with-associations
    """
    import sqlite3
    from tolteca_db.db import get_engine, get_session
    from tolteca_db.ingest.ingest import DataIngestor
    from tolteca_db.ingest.file_scanner import guess_info_from_file
    from tolteca_db.models.orm import Location
    from sqlalchemy import select, text, create_engine
    from rich.progress import Progress
    import os
    
    # Determine toltec_db source
    source_db_url = toltec_db_url or toltec_db
    if not source_db_url:
        console.print("[red]Error:[/red] Must provide toltec_db path/URL as argument or --toltec-db-url option")
        raise typer.Exit(code=1)
    
    # Check if it's a file path or database URL
    is_file = not source_db_url.startswith(('sqlite://', 'mysql://', 'mysql+pymysql://', 'postgresql://'))
    
    if is_file:
        # File path - convert to Path and check existence
        toltec_db_path = Path(source_db_url)
        if not toltec_db_path.exists():
            console.print(f"[red]Error:[/red] toltec_db not found: {toltec_db_path}")
            raise typer.Exit(code=1)
        # Use absolute() to get absolute path without following symlinks
        toltec_db_path = Path(os.path.normpath(toltec_db_path.absolute()))
        source_db_url = f"sqlite:///{toltec_db_path}"
    
    # Get target database engine
    engine = get_engine(target_url)
    
    # Resolve data_root and location
    with Session(engine) as session:
        # Look up location
        stmt = select(Location).where(Location.label == location)
        loc = session.scalar(stmt)
        
        if data_root is None:
            # No data_root provided - must get from location table
            if loc is None:
                console.print(f"[red]Error:[/red] Location '{location}' not found in database and no --data-root provided")
                console.print("[yellow]Hint:[/yellow] Either provide --data-root or create the location first with 'tolteca_db db init'")
                raise typer.Exit(code=1)
            
            # Extract data_root from location root_uri
            if loc.root_uri.startswith('file://'):
                data_root = Path(loc.root_uri.replace('file://', '', 1))
            else:
                data_root = Path(loc.root_uri)
            
            console.print(f"[green]Using data_root from Location '{location}':[/green] {data_root}")
        else:
            # data_root provided - expand and normalize (preserve symlinks)
            data_root = Path(os.path.expanduser(str(data_root)))
            data_root = Path(os.path.normpath(data_root.absolute()))
            
            if not data_root.exists():
                console.print(f"[red]Error:[/red] data_root not found: {data_root}")
                raise typer.Exit(code=1)
            
            # Check if location exists
            if loc is None:
                # Create new location
                console.print(f"[yellow]Creating new location '{location}' with data_root:[/yellow] {data_root}")
                loc = Location(
                    label=location,
                    location_type="filesystem",
                    root_uri=f"file://{data_root}",
                    priority=100,
                )
                session.add(loc)
                session.commit()
            else:
                # Verify root_uri matches
                expected_root = f"file://{data_root}"
                if loc.root_uri != expected_root:
                    console.print(f"[yellow]Warning:[/yellow] Location root_uri mismatch:")
                    console.print(f"  Expected: {expected_root}")
                    console.print(f"  Actual:   {loc.root_uri}")
                    console.print(f"  Using location's root_uri: {loc.root_uri}")
                    # Use location's root_uri
                    if loc.root_uri.startswith('file://'):
                        data_root = Path(loc.root_uri.replace('file://', '', 1))
                    else:
                        data_root = Path(loc.root_uri)
    
    console.print(f"\n[bold blue]Ingesting from toltec_db:[/bold blue] {source_db_url}")
    console.print(f"Data root: {data_root}")
    console.print(f"Location: {location}, Master: {master}\n")
    
    # Connect to toltec_db (source) using SQLAlchemy
    from sqlalchemy import create_engine
    toltec_engine = create_engine(source_db_url)
    
    with Session(toltec_engine) as toltec_session:
        # Build query for toltec table using database-agnostic SQL
        query_sql = """
            SELECT t.*, o.label as obstype_label,
                   LOWER(m.label) as master_label
            FROM toltec t
            LEFT JOIN obstype o ON t.ObsType = o.id
            LEFT JOIN master m ON t.Master = m.id
            WHERE t.Valid = 1
        """
        
        params = {}
        if obstype_filter:
            query_sql += " AND o.label = :obstype"
            params["obstype"] = obstype_filter
        
        if start_date:
            query_sql += " AND t.Date >= :start_date"
            params["start_date"] = start_date
        
        if end_date:
            query_sql += " AND t.Date <= :end_date"
            params["end_date"] = end_date
        
        if limit:
            query_sql += f" LIMIT {limit}"
        
        # Execute query
        result = toltec_session.execute(text(query_sql), params)
        rows = result.fetchall()
        
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
                    str(row.ObsNum),
                    str(row.SubObsNum),
                    str(row.ScanNum),
                    row.obstype_label or "?",
                    row.FileName,
                )
            
            console.print(table)
            if len(rows) > 50:
                console.print(f"\n[yellow]Note:[/yellow] Showing first 50 of {len(rows)} entries")
            console.print("\n[yellow]Dry run complete.[/yellow] Use --no-dry-run to ingest.")
            return
    
    # Actual ingestion
    import time
    timings = {
        'path_construct': 0,
        'parse_file': 0,
        'ingest_file': 0,
        'file_exists': 0,
        'commit': 0,
    }
    
    with Session(engine) as session:
        # Get location (already resolved above)
        stmt = select(Location).where(Location.label == location)
        loc = session.scalar(stmt)
        
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
                # Filenames from toltec_db: /data/toltec/ics/toltec0/file.nc or /data_lmt/toltec/clip/...
                # Goal: Extract relative path starting from 'toltec/', e.g., toltec/ics/toltec0/file.nc
                t0 = time.time()
                filename = row.FileName
                
                # Find 'toltec/' in the path and extract from there
                if '/toltec/' in filename:
                    toltec_idx = filename.index('/toltec/')
                    filename_rel = filename[toltec_idx + 1:]  # Remove leading slash, result: toltec/...
                elif filename.startswith('toltec/'):
                    filename_rel = filename
                else:
                    # Fallback: use filename as-is if 'toltec/' not found
                    console.print(f"[yellow]Warning:[/yellow] Could not find 'toltec/' in path: {filename}")
                    failed += 1
                    progress.update(task, advance=1)
                    continue
                
                # Construct full path: data_root + relative path
                file_path = data_root / filename_rel
                timings['path_construct'] += time.time() - t0
                
                try:
                    # Parse file info from filename
                    t0 = time.time()
                    file_info = guess_info_from_file(file_path)
                    timings['parse_file'] += time.time() - t0
                    
                    if file_info is None:
                        console.print(f"[yellow]Warning:[/yellow] Could not parse filename: {file_path.name}")
                        failed += 1
                        progress.update(task, advance=1)
                        continue
                    
                    # Get observation datetime from toltec_db Date and Time columns
                    obs_datetime = None
                    if row.Date and row.Time:
                        from datetime import datetime, timedelta
                        try:
                            # Handle both MySQL TIME (timedelta) and SQLite TEXT
                            if isinstance(row.Time, timedelta):
                                # MySQL returns TIME as timedelta
                                base_date = datetime.strptime(str(row.Date), "%Y-%m-%d")
                                obs_datetime = base_date + row.Time
                            else:
                                # SQLite returns TIME as TEXT string
                                obs_datetime = datetime.fromisoformat(f"{row.Date} {row.Time}")
                        except (ValueError, TypeError):
                            pass
                    
                    # Override file_info.obs_datetime with toltec_db value
                    file_info.obs_datetime = obs_datetime
                    
                    # Ingest file (logical entry created even if file missing)
                    t0 = time.time()
                    ingestor.ingest_file(file_info)
                    timings['ingest_file'] += time.time() - t0
                    
                    t0 = time.time()
                    if file_path.exists():
                        ingested += 1
                    else:
                        missing += 1
                    timings['file_exists'] += time.time() - t0
                    
                    # Commit periodically
                    if (ingested + missing) % 100 == 0:
                        t0 = time.time()
                        session.commit()
                        timings['commit'] += time.time() - t0
                    
                except Exception as e:
                    console.print(f"[red]Error ingesting {file_path.name}:[/red] {e}")
                    failed += 1
                
                progress.update(task, advance=1)
        
        # Final commit
        t0 = time.time()
        session.commit()
        timings['commit'] += time.time() - t0
        
        # Summary
        console.print(f"\n[green]✓[/green] Ingestion complete:")
        console.print(f"  Ingested: {ingested}")
        console.print(f"  Logical (missing files): {missing}")
        console.print(f"  Skipped (existing): {skipped}")
        console.print(f"  Failed: {failed}")
        
        # Timing breakdown
        total = sum(timings.values())
        console.print(f"\n[bold]Performance breakdown (CLI loop):[/bold]")
        for key, val in timings.items():
            pct = (val / total * 100) if total > 0 else 0
            console.print(f"  {key:20s}: {val:6.2f}s ({pct:5.1f}%)")
        console.print(f"  {'Total':20s}: {total:6.2f}s")
        
        # Ingestor internal timings
        if hasattr(ingestor, '_timings'):
            console.print(f"\n[bold]Performance breakdown (ingestor.ingest_file):[/bold]")
            ingestor_total = sum(ingestor._timings.values())
            for key, val in ingestor._timings.items():
                pct = (val / ingestor_total * 100) if ingestor_total > 0 else 0
                console.print(f"  {key:30s}: {val:6.2f}s ({pct:5.1f}%)")
            console.print(f"  {'Total':30s}: {ingestor_total:6.2f}s")
    
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
        Optional[Path],
        typer.Argument(help="Path to LMT telescope metadata CSV (or use --start-date for API mode)"),
    ] = None,
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
    start_date: Annotated[
        Optional[str],
        typer.Option("--start-date", "-s", help="Start date for LMTMC API query (YYYY-MM-DD)"),
    ] = None,
    end_date: Annotated[
        Optional[str],
        typer.Option("--end-date", "-e", help="End date for LMTMC API query (YYYY-MM-DD). Defaults to start_date."),
    ] = None,
    api_url: Annotated[
        Optional[str],
        typer.Option("--api-url", help="LMTMC API base URL (default: from LMTMC_API_BASE_URL env)"),
    ] = None,
    force_refresh: Annotated[
        bool,
        typer.Option("--force-refresh", help="Force API query (ignore cache)"),
    ] = False,
) -> None:
    """
    Ingest LMT telescope metadata from CSV file or LMTMC API.
    
    Two modes:
    
    1. File mode (provide CSV_PATH):
       tolteca_db ingest from-tel-csv /path/to/lmtmc.csv --location LMT
    
    2. API mode (provide --start-date):
       tolteca_db ingest from-tel-csv --start-date 2025-10-31 --end-date 2025-10-31
    
    API mode queries http://187.248.54.232/cgi-bin/lmtmc/mc_sql.cgi and caches
    the CSV response locally. Cached files are reused unless --force-refresh is specified.
    
    CSV format: lmtmc_toltec_metadata.csv from LMT metadata database.
    CSV includes FileName and Valid columns, providing complete quartet metadata:
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
    
    Examples:
        # File mode
        tolteca_db ingest from-tel-csv run/lmtmc_toltec_metadata.csv \\
            --location LMT \\
            --db "duckdb:///tolteca.duckdb"
        
        # API mode (single date)
        tolteca_db ingest from-tel-csv \\
            --start-date 2025-10-31 \\
            --location LMT
        
        # API mode (date range)
        tolteca_db ingest from-tel-csv \\
            --start-date 2025-10-01 \\
            --end-date 2025-10-31 \\
            --location LMT
    """
    from tolteca_db.db import get_engine
    from sqlalchemy.orm import Session
    from tolteca_db.models.orm import Location
    from tolteca_db.ingest.tel_ingestor import TelCSVIngestor
    from sqlalchemy import select
    
    # Determine mode: file or API
    if csv_path is None and start_date is None:
        console.print("[red]Error:[/red] Either CSV_PATH or --start-date must be provided")
        console.print("[yellow]Hint:[/yellow] Use 'tolteca_db ingest from-tel-csv --help' for usage")
        raise typer.Exit(code=1)
    
    if csv_path is not None and start_date is not None:
        console.print("[red]Error:[/red] Cannot use both CSV_PATH and --start-date")
        console.print("[yellow]Hint:[/yellow] Choose either file mode or API mode")
        raise typer.Exit(code=1)
    
    # API mode: Query LMTMC API
    if start_date is not None:
        from tolteca_db.ingest.lmtmc_api import query_lmtmc_csv, LMTMCAPIError
        
        # Default end_date to start_date
        if end_date is None:
            end_date = start_date
        
        console.print(f"[bold blue]Querying LMTMC API:[/bold blue] {start_date} to {end_date}")
        
        try:
            csv_path = query_lmtmc_csv(
                start_date=start_date,
                end_date=end_date,
                api_base_url=api_url,
                force_refresh=force_refresh,
            )
            console.print(f"[green]✓[/green] CSV cached at: {csv_path}")
        except LMTMCAPIError as e:
            console.print(f"[red]Error:[/red] API query failed: {e}")
            raise typer.Exit(code=1)
    
    # File mode validation
    if csv_path is not None and not csv_path.exists():
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
