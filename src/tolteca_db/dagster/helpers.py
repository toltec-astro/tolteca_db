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
    "add_tel_file_source",
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
        WHERE datetime(toltec.Date || ' ' || toltec.Time) >= datetime(:since_dt)
        ORDER BY toltec.id ASC
        """
    )

    result = session.execute(query, {"since_dt": since_dt})
    rows = result.fetchall()

    # Convert to list of dicts
    observations = []
    for row in rows:
        # Combine date and time into timestamp
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

            # Calculate source_uri
            source_uri = ingestor._make_relative_uri(file_path)

            return {
                "rows_processed": 0,
                "duration_seconds": duration,
                "status": "success",
                "data_prod_pk": str(existing.pk) if existing else "unknown",
                "source_uri": source_uri,
            }


def add_tel_file_source(
    master: str,
    obsnum: int,
    subobsnum: int,
    scannum: int,
    data_prod_pk: int,
    tolteca_db,
    location,
) -> dict:
    """Add tel file as an additional source to existing DataProd.

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
        Location configuration with data_root

    Returns
    -------
    dict
        Result with keys:
        - added: bool (True if added, False if already exists)
        - source_uri: str (tel file URI)
        - status: str ("success")
    """
    from pathlib import Path
    from sqlalchemy import select
    from tolteca_db.models.orm import DataProdSource, Location as LocationORM
    from tolteca_db.models.metadata import TelInterfaceMeta

    # Get data root from location config
    data_root = location.get_data_root()
    if data_root is None:
        return {"added": False, "source_uri": None, "status": "no_data_root"}

    # Construct tel file path
    # Format: tel_toltec_YYYY-MM-DD_OBSNUM_SUBOBS_SCAN.nc
    # The tel files are in data_lmt/tel/ directory
    # data_root from location config is: /path/to/data_lmt (TOLTECA_WEB_DATA_LMT_ROOTPATH)
    # We need: /path/to/data_lmt/tel
    tel_dir = Path(data_root) / "tel"
    
    # Search for tel file matching the quartet
    import glob
    pattern = f"tel_toltec_*_{obsnum:06d}_{subobsnum:02d}_{scannum:04d}.nc"
    tel_files = list(tel_dir.glob(pattern))
    
    if not tel_files:
        return {"added": False, "source_uri": None, "status": "tel_file_not_found"}
    
    tel_file_path = tel_files[0]  # Use first match
    
    # Create TelInterfaceMeta
    tel_meta = TelInterfaceMeta(
        obsnum=obsnum,
        subobsnum=subobsnum,
        scannum=scannum,
        master=master,
        interface="tel_toltec",
    )
    
    # Calculate relative URI from location root
    # Location root is at data_lmt level (from TOLTECA_WEB_DATA_LMT_ROOTPATH)
    # Tel file is at data_lmt/tel/tel_toltec_*.nc
    # So relative URI should be: tel/tel_toltec_*.nc
    location_root = Path(data_root)
    try:
        rel_path = tel_file_path.relative_to(location_root)
        source_uri = str(rel_path)
    except ValueError:
        # Path not relative to location_root, use absolute
        source_uri = str(tel_file_path)
    
    # Add DataProdSource
    with tolteca_db.get_session() as session:
        # Check if source already exists
        stmt = select(DataProdSource).where(DataProdSource.source_uri == source_uri)
        existing = session.scalar(stmt)
        
        if existing:
            return {"added": False, "source_uri": source_uri, "status": "already_exists"}
        
        # Get location_fk using location_pk (label) from config
        stmt = select(LocationORM).where(LocationORM.label == location.location_pk)
        location_orm = session.scalar(stmt)
        
        if not location_orm:
            return {"added": False, "source_uri": source_uri, "status": "location_not_found"}
        
        # Create new source
        tel_source = DataProdSource(
            source_uri=source_uri,
            data_prod_fk=data_prod_pk,
            location_fk=location_orm.pk,
            role="METADATA",
            meta=tel_meta,
            availability_state="ONLINE",
            size=tel_file_path.stat().st_size if tel_file_path.exists() else None,
        )
        
        session.add(tel_source)
        session.commit()
    
    return {"added": True, "source_uri": source_uri, "status": "success"}
