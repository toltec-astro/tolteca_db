"""Helper functions for querying and processing toltec_db data.

These functions are adapted from the per-interface experiment but
simplified for the quartet-level architecture.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

__all__ = [
    "query_toltec_db_since",
    "query_quartet_status",
    "process_interface_data",
    "add_tel_csv_metadata",
]


def query_toltec_db_since(
    since_dt: datetime,
    session: Session,
    table_name: str = "toltec",
) -> list[dict]:
    """
    Query toltec_db for observations since a given timestamp.

    Parameters
    ----------
    since_dt : datetime
        Query for observations after this timestamp
    session : Session
        SQLAlchemy session for toltec_db
    table_name : str, optional
        Table name to query, by default "toltec"

    Returns
    -------
    list[dict]
        List of observation records with keys:
        - master, obsnum, subobsnum, scannum, roach_index
        - valid, filename, timestamp

    Examples
    --------
    >>> from datetime import datetime, timezone
    >>> since = datetime(2024, 1, 1, tzinfo=timezone.utc)
    >>> with toltec_db.get_session() as session:
    ...     obs = query_toltec_db_since(since, session)
    >>> len(obs)  # New observations since 2024-01-01
    """
    from sqlalchemy import text

    # Query all observations with timestamp >= since_dt
    # Note: Column names are case-sensitive (RoachIndex, not roach_index)
    # Use database-agnostic SQL: STR_TO_DATE() for MySQL, datetime() for SQLite
    # Both support CONCAT() for string concatenation
    query = text(
        f"""
        SELECT 
            LOWER(master.label) as master,
            toltec.ObsNum as obsnum,
            toltec.SubObsNum as subobsnum,
            toltec.ScanNum as scannum,
            toltec.RoachIndex as roach_index,
            toltec.Valid as valid,
            toltec.FileName as filename,
            toltec.Date as date,
            toltec.Time as time
        FROM {table_name} AS toltec
        JOIN master ON toltec.Master = master.id
        WHERE CONCAT(toltec.Date, ' ', toltec.Time) >= :since_dt
        ORDER BY toltec.id ASC
        """
    )

    result = session.execute(query, {"since_dt": since_dt.strftime("%Y-%m-%d %H:%M:%S")})
    rows = result.fetchall()

    # Convert to list of dicts
    observations = []
    for row in rows:
        # Combine date and time into timestamp
        # Handle both SQLite (TEXT) and MySQL (TIME/timedelta)
        from datetime import timedelta
        
        if isinstance(row.time, timedelta):
            # MySQL returns TIME as timedelta - combine with date
            base_date = datetime.strptime(str(row.date), "%Y-%m-%d")
            timestamp = base_date + row.time
        else:
            # SQLite returns TIME as TEXT string
            timestamp = datetime.fromisoformat(f"{row.date} {row.time}")

        observations.append(
            {
                "master": row.master,
                "obsnum": row.obsnum,
                "subobsnum": row.subobsnum,
                "scannum": row.scannum,
                "roach_index": row.roach_index,
                "valid": row.valid,
                "filename": row.filename,
                "timestamp": timestamp,
            }
        )

    return observations


def query_quartet_status(
    master: str,
    obsnum: int,
    subobsnum: int,
    scannum: int,
    session: Session,
    table_name: str = "toltec",
) -> list[dict]:
    """
    Query status of all interfaces in a quartet.

    Parameters
    ----------
    master : str
        Master instrument (e.g., "ics")
    obsnum : int
        Observation number
    subobsnum : int
        Sub-observation number
    scannum : int
        Scan number
    session : Session
        SQLAlchemy session for toltec_db
    table_name : str, optional
        Table name to query, by default "toltec"

    Returns
    -------
    list[dict]
        List of interface records with keys:
        - roach_index, valid, filename, timestamp

    Examples
    --------
    >>> with toltec_db.get_session() as session:
    ...     status = query_quartet_status("ics", 18846, 0, 0, session)
    >>> len(status)  # Should be 11 for enabled interfaces
    >>> status[0]["roach_index"]  # 0
    >>> status[0]["valid"]  # 1
    """
    from sqlalchemy import text

    query = text(
        f"""
        SELECT 
            toltec.RoachIndex as roach_index,
            toltec.Valid as valid,
            toltec.FileName as filename,
            toltec.Date as date,
            toltec.Time as time
        FROM {table_name} AS toltec
        JOIN master ON toltec.Master = master.id
        WHERE LOWER(master.label) = LOWER(:master)
            AND toltec.ObsNum = :obsnum
            AND toltec.SubObsNum = :subobsnum
            AND toltec.ScanNum = :scannum
        ORDER BY toltec.RoachIndex ASC
        """
    )

    result = session.execute(
        query,
        {
            "master": master,
            "obsnum": obsnum,
            "subobsnum": subobsnum,
            "scannum": scannum,
        },
    )
    rows = result.fetchall()

    # Convert to list of dicts
    interfaces = []
    for row in rows:
        # Combine date and time into timestamp
        # Handle both string and timedelta time types (MySQL returns timedelta)
        if isinstance(row.time, str):
            time_str = row.time
            # Handle times with missing leading zeros (e.g., "3:47:13" → "03:47:13")
            if len(time_str.split(':')[0]) == 1:
                time_str = f"0{time_str}"
            timestamp = datetime.fromisoformat(f"{row.date} {time_str}")
        else:
            # MySQL returns time as timedelta - combine with date
            from datetime import timedelta
            if isinstance(row.time, timedelta):
                timestamp = datetime.combine(row.date, datetime.min.time()) + row.time
            else:
                timestamp = datetime.fromisoformat(f"{row.date} {row.time}")

        interfaces.append(
            {
                "roach_index": row.roach_index,
                "valid": row.valid,
                "filename": row.filename,
                "timestamp": timestamp,
            }
        )

    return interfaces


def process_interface_data(
    master: str,
    obsnum: int,
    subobsnum: int,
    scannum: int,
    roach_index: int,
    session: Session,
    tolteca_db,
    location,
) -> dict:
    """
    Process data for a single interface using DataIngestor.

    This is the core processing logic that:
    1. Queries raw observation metadata from toltec_db
    2. Validates the interface (Valid flag must be 1)
    3. Uses DataIngestor to create DataProd + DataProdSource entries (idempotent)

    Uses the existing tested DataIngestor to ensure correct architecture:
    - ONE DataProd per quartet (shared across all interfaces)
    - ONE DataProdSource per interface file
    - Location tracking for multi-site storage

    Parameters
    ----------
    master : str
        Master instrument (e.g., "ics")
    obsnum : int
        Observation number
    subobsnum : int
        Sub-observation number
    scannum : int
        Scan number
    roach_index : int
        Interface index (0-12, excluding disabled interfaces)
    session : Session
        SQLAlchemy session for toltec_db (read-only)
    tolteca_db : ToltecaDBResource
        Resource for writing to tolteca_db
    location : LocationConfig
        Location configuration with data_root

    Returns
    -------
    dict
        Processing result with keys:
        - rows_processed: int (1 if created, 0 if skipped)
        - duration_seconds: float
        - status: str ("success")
        - data_prod_pk: str (DataProd primary key)
        - source_uri: str (DataProdSource URI)

    Raises
    ------
    RuntimeError
        If interface not found in toltec_db
    ValueError
        If interface Valid flag is not 1, or Location not found

    Examples
    --------
    >>> with toltec_db.get_session() as session:
    ...     result = process_interface_data(
    ...         "ics", 18846, 0, 0, 0, session, tolteca_db, location
    ...     )
    >>> result["rows_processed"]  # 1 (created) or 0 (existed)
    >>> result["data_prod_pk"]  # Auto-generated pk
    >>> result["source_uri"]  # "ics/toltec0/toltec0_18846_0_0.nc"
    >>> result["status"]  # "success"
    """
    import time
    from pathlib import Path
    from sqlalchemy import text
    from tolteca_db.ingest import DataIngestor, guess_info_from_file

    start_time = time.time()

    # Query observation metadata
    query = text(
        """
        SELECT 
            toltec.FileName as filename,
            toltec.Valid as valid,
            toltec.Date as date,
            toltec.Time as time
        FROM toltec
        JOIN master ON toltec.Master = master.id
        WHERE LOWER(master.label) = LOWER(:master)
            AND toltec.ObsNum = :obsnum
            AND toltec.SubObsNum = :subobsnum
            AND toltec.ScanNum = :scannum
            AND toltec.RoachIndex = :roach_index
        """
    )

    result = session.execute(
        query,
        {
            "master": master,
            "obsnum": obsnum,
            "subobsnum": subobsnum,
            "scannum": scannum,
            "roach_index": roach_index,
        },
    )
    row = result.fetchone()

    if not row:
        raise RuntimeError(
            f"Interface not found: {master}-{obsnum}-{subobsnum}-"
            f"{scannum}-toltec{roach_index}"
        )

    if row.valid != 1:
        raise ValueError(f"Interface not valid: {row.filename}")

    # Get data root from location config
    data_root = location.get_data_root()
    if data_root is None:
        raise ValueError("location.data_root is not configured")

    # Construct file path
    filename = row.filename
    # Strip /data_lmt prefix if present
    if filename.startswith("/data_lmt/"):
        filename = filename[len("/data_lmt/") :]
    elif filename.startswith("/data_lmt"):
        filename = filename[len("/data_lmt") :].lstrip("/")

    file_path = data_root / filename

    # Parse file info from filename
    file_info = guess_info_from_file(file_path)
    if file_info is None:
        raise ValueError(f"Could not parse filename: {file_path.name}")

    # Set observation datetime from toltec_db Date and Time columns
    if row.date and row.time:
        from datetime import datetime, timedelta
        import logging

        logger = logging.getLogger(__name__)
        try:
            # Handle both MySQL TIME (timedelta) and SQLite TEXT
            if isinstance(row.time, timedelta):
                # MySQL returns TIME as timedelta
                base_date = datetime.strptime(str(row.date), "%Y-%m-%d")
                file_info.obs_datetime = base_date + row.time
            else:
                # SQLite returns TIME as TEXT string
                file_info.obs_datetime = datetime.fromisoformat(f"{row.date} {row.time}")
        except (ValueError, TypeError) as e:
            # If parsing fails, log warning but continue
            logger.warning(
                f"Could not parse observation datetime from Date={row.date}, Time={row.time}: {e}"
            )

    # Use DataIngestor to create DataProd + DataProdSource
    # DuckDB: Retry on write-write conflicts (multi-process contention)
    max_retries = 3
    retry_delay = 0.5  # seconds

    for attempt in range(max_retries):
        try:
            with tolteca_db.get_session() as tdb_session:
                ingestor = DataIngestor(
                    session=tdb_session,
                    location_pk=location.location_pk,
                    master=master,
                    nw_id=roach_index,
                )

                data_prod, source = ingestor.ingest_file(
                    file_info,
                    skip_existing=True,
                    obs_goal=None,  # Could be retrieved from toltec_db if needed
                    source_name=None,
                )

                # Extract data BEFORE committing (ORM objects become detached after commit)
                if data_prod is not None:
                    data_prod_pk = str(data_prod.pk)
                    source_uri = source.source_uri if source else "unknown"
                    rows_processed = 1
                else:
                    data_prod_pk = None
                    source_uri = None
                    rows_processed = 0

                tdb_session.commit()
                break  # Success - exit retry loop

        except Exception as e:
            error_msg = str(e).lower()
            # DuckDB-specific error: "Conflicting lock is held" or "write-write conflict"
            is_lock_error = "lock" in error_msg or "conflict" in error_msg

            if is_lock_error and attempt < max_retries - 1:
                # Log and retry
                import logging

                logger = logging.getLogger(__name__)
                logger.warning(
                    f"DuckDB write conflict on attempt {attempt + 1}/{max_retries}, "
                    f"retrying in {retry_delay}s: {e}"
                )
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                # Not a lock error, or out of retries
                raise

    duration = time.time() - start_time

    # Return result
    if data_prod_pk is not None:
        return {
            "rows_processed": rows_processed,
            "duration_seconds": duration,
            "status": "success",
            "data_prod_pk": data_prod_pk,
            "source_uri": source_uri,
        }
    else:
        # Entry already exists - fetch existing DataProd
        from sqlalchemy import select
        from tolteca_db.models.orm import DataProd, DataProdType as DataProdTypeORM

        with tolteca_db.get_session() as tdb_session:
            stmt = select(DataProdTypeORM).where(DataProdTypeORM.label == "dp_raw_obs")
            dp_type = tdb_session.scalar(stmt)

            stmt = (
                select(DataProd)
                .where(DataProd.data_prod_type_fk == dp_type.pk)
                .where(DataProd.meta["master"].as_string() == master)
                .where(DataProd.meta["obsnum"].as_integer() == obsnum)
                .where(DataProd.meta["subobsnum"].as_integer() == subobsnum)
                .where(DataProd.meta["scannum"].as_integer() == scannum)
            )
            existing = tdb_session.scalar(stmt)

            if not existing:
                # This shouldn't happen - ingest_file returned None (skip_existing=True)
                # but query can't find the existing DataProd. This indicates a bug.
                raise RuntimeError(
                    f"DataProd not found for {master}-{obsnum}-{subobsnum}-{scannum}. "
                    f"ingest_file returned None (skip_existing) but query found nothing. "
                    f"This may indicate a master mismatch or database inconsistency."
                )

            # Calculate source_uri
            source_uri = ingestor._make_relative_uri(file_path)

            return {
                "rows_processed": 0,
                "duration_seconds": duration,
                "status": "success",
                "data_prod_pk": str(existing.pk),
                "source_uri": source_uri,
            }


def add_tel_csv_metadata(
    master: str,
    obsnum: int,
    subobsnum: int,
    scannum: int,
    data_prod_pk: int,
    tolteca_db,
    location,
) -> dict:
    """Add tel CSV metadata to existing DataProd using TelCSVIngestor.

    Reads test_lmtmc.csv and ingests matching tel metadata row.

    Uses unified CSV structure with FileName column to create file-based URIs
    (e.g., tel/tel_toltec_*.nc) instead of virtual URIs (tel://*).

    Parameters
    ----------
    master : str
        Master instrument (e.g., "tcs")
    obsnum : int
        Observation number
    subobsnum : int
        Sub-observation number
    scannum : int
        Scan number
    data_prod_pk : int
        DataProd primary key to add source to
    tolteca_db : ToltecaDBResource
        Resource for writing to tolteca_db
    location : LocationConfig
        Location configuration

    Returns
    -------
    dict
        Result with keys:
        - added: bool (True if added, False if already exists)
        - source_uri: str (file-based URI like tel/tel_toltec_*.nc)
        - status: str
    """
    import os
    from pathlib import Path
    from sqlalchemy import select
    from tolteca_db.models.orm import DataProdSource, DataProd, Location as LocationORM
    from tolteca_db.ingest.tel_ingestor import TelCSVIngestor
    from tolteca_db.ingest.lmtmc_api import query_lmtmc_csv, LMTMCAPIError

    # Determine CSV source: explicit path or API
    # LMTMC_CSV_PATH: If set, use this CSV file (for offline/test mode)
    # If not set, query LMTMC API using TOLTECA_SIMULATOR_DATE

    csv_path_env = os.getenv("LMTMC_CSV_PATH")

    if csv_path_env:
        # Use explicitly configured CSV file
        csv_path = Path(csv_path_env)
        if not csv_path.exists():
            return {
                "added": False,
                "source_uri": None,
                "status": f"csv_not_found: {csv_path}",
            }
    else:
        # Query API for CSV data
        # Get date from environment variable (set by simulator or config)
        obs_date = os.getenv("TOLTECA_SIMULATOR_DATE")
        if not obs_date:
            return {"added": False, "source_uri": None, "status": "no_obs_date"}

        try:
            # Query API for the observation date
            csv_path = query_lmtmc_csv(
                start_date=obs_date,
                end_date=obs_date,
                force_refresh=False,  # Use cache if available
            )
        except LMTMCAPIError as e:
            return {"added": False, "source_uri": None, "status": f"api_error: {e}"}

    with tolteca_db.get_session() as session:
        # Get location_fk
        stmt = select(LocationORM).where(LocationORM.label == location.location_pk)
        location_orm = session.scalar(stmt)

        if not location_orm:
            return {"added": False, "source_uri": None, "status": "location_not_found"}

        # Check if DataProd already has tel source
        # (tel sources have role="METADATA" and location matching tel location)
        stmt = (
            select(DataProdSource)
            .where(DataProdSource.data_prod_fk == data_prod_pk)
            .where(DataProdSource.role == "METADATA")
            .where(DataProdSource.location_fk == location_orm.pk)
        )
        existing = session.scalar(stmt)

        if existing:
            return {
                "added": False,
                "source_uri": existing.source_uri,
                "status": "already_exists",
            }

        # Use TelCSVIngestor to process CSV and create source
        # Set skip_existing=False to ensure we process this quartet
        # Set create_data_prods=False since DataProd already exists
        ingestor = TelCSVIngestor(
            session=session,
            location_pk=location_orm.pk,
            skip_existing=False,
            create_data_prods=False,
            commit_batch_size=1,
        )

        # Ingest the CSV - it will find matching row and update DataProd
        stats = ingestor.ingest_csv(csv_path)

        if stats.sources_created > 0:
            # Query the source that was just created to get its URI
            stmt = (
                select(DataProdSource)
                .where(DataProdSource.data_prod_fk == data_prod_pk)
                .where(DataProdSource.role == "METADATA")
                .where(DataProdSource.location_fk == location_orm.pk)
            )
            created_source = session.scalar(stmt)
            source_uri = created_source.source_uri if created_source else None
            return {"added": True, "source_uri": source_uri, "status": "success"}
        elif stats.rows_skipped > 0:
            # Row was skipped (already exists or filtered out)
            return {"added": False, "source_uri": None, "status": "already_exists"}
        else:
            # No matching row found in CSV
            return {
                "added": False,
                "source_uri": source_uri,
                "status": "csv_row_not_found",
            }
