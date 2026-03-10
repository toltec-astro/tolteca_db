"""Helper functions for database queries and external system integration.

These are plain Python functions (not @op decorated) following Dagster 2025
best practices. They are called directly from assets and sensors.

Use @op only inside @graph_asset for complex multi-step processing.

References
----------
- Dagster Best Practices: https://docs.dagster.io/guides/build/ops
- "If you are just getting started with Dagster, we strongly recommend
   you use assets rather than ops to build your data pipelines."
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

__all__ = [
    "query_obs_timestamp",
    "query_toltec_db_observation",
    "query_toltec_db_since",
    "query_toltec_db_interface",
    "query_toltec_db_quartet_status",
    "query_toltec_db_active_quartets",
]


def query_obs_timestamp(
    master: str,
    obsnum: int,
    subobsnum: int,
    scannum: int,
    session: Session | None = None,
) -> datetime:
    """
    Query observation timestamp from toltec_db.

    This is a helper function (not @op) used inside sensors and assets.
    Error handling should be done at the caller level.

    Parameters
    ----------
    master : str
        Master identifier
    obsnum : int
        Observation number
    subobsnum : int
        Sub-observation number
    scannum : int
        Scan number
    session : Session | None
        SQLAlchemy session for toltec_db. If None, creates temporary session.

    Returns
    -------
    datetime
        Observation timestamp in UTC

    Examples
    --------
    >>> timestamp = query_obs_timestamp('toltec', 123456, 0, 1)
    >>> timestamp.isoformat()
    '2024-10-31T15:30:00+00:00'

    Notes
    -----
    TODO: Implement actual toltec_db query once connection is configured.
    Currently returns placeholder timestamp for development.
    """
    # TODO: Implement actual toltec_db query
    # Example query structure:
    # SELECT TIMESTAMP(toltec.Date, toltec.Time) as timestamp
    # FROM toltec
    # JOIN master ON toltec.Master = master.id
    # WHERE master.label = :master
    #   AND toltec.ObsNum = :obsnum
    #   AND toltec.SubObsNum = :subobsnum
    #   AND toltec.ScanNum = :scannum
    #   AND toltec.Valid > 0

    # Placeholder: return current time
    return datetime.now(timezone.utc)


def query_toltec_db_observation(
    master: str,
    obsnum: int,
    subobsnum: int,
    scannum: int,
    session: Session | None = None,
) -> dict:
    """
    Query full observation metadata from toltec_db.

    Helper function (not @op) for fetching observation details.

    Parameters
    ----------
    master : str
        Master identifier
    obsnum : int
        Observation number
    subobsnum : int
        Sub-observation number
    scannum : int
        Scan number
    session : Session | None
        SQLAlchemy session for toltec_db

    Returns
    -------
    dict
        Observation metadata with keys: master, obsnum, subobsnum, scannum,
        obs_type, obs_goal, source_name, timestamp, data_kind, etc.

    Examples
    --------
    >>> obs = query_toltec_db_observation('toltec', 123456, 0, 1)
    >>> obs['source_name']
    'NGC1068'

    Notes
    -----
    TODO: Implement actual toltec_db query once connection is configured.
    """
    # TODO: Implement actual toltec_db query
    # Return placeholder data
    return {
        "master": master,
        "obsnum": obsnum,
        "subobsnum": subobsnum,
        "scannum": scannum,
        "obs_type": "science",
        "obs_goal": "mapping",
        "source_name": "Unknown",
        "timestamp": datetime.now(timezone.utc),
        "data_kind": 0,  # ToltecDataKind flags
    }


def query_toltec_db_since(
    since_timestamp: datetime,
    session: Session | None = None,
) -> list[dict]:
    """
    Query toltec_db for observations since timestamp.

    This is a helper function (not @op) used by sensors to detect new
    observations. Returns observations in timeline order.

    Parameters
    ----------
    since_timestamp : datetime
        Query observations newer than this timestamp
    session : Session | None
        SQLAlchemy session for toltec_db (read-only)

    Returns
    -------
    list[dict]
        List of observations, each with keys: master, obsnum, subobsnum,
        scannum, obs_type, timestamp, etc. Ordered by timestamp ASC.

    Examples
    --------
    >>> from datetime import datetime, timedelta
    >>> week_ago = datetime.now() - timedelta(days=7)
    >>> new_obs = query_toltec_db_since(week_ago)
    >>> len(new_obs)
    42

    Notes
    -----
    Query returns observations in ascending timestamp order to preserve
    timeline sequence when creating 1D partitions.

    TODO: Implement actual toltec_db query with proper retry logic
    using tenacity decorator.

    Example SQL:
    ```sql
    SELECT
        master.label as master,
        toltec.ObsNum as obsnum,
        toltec.SubObsNum as subobsnum,
        toltec.ScanNum as scannum,
        obstype.label as obs_type,
        TIMESTAMP(toltec.Date, toltec.Time) as timestamp
    FROM toltec
    JOIN master ON toltec.Master = master.id
    JOIN obstype ON toltec.ObsType = obstype.id
    WHERE TIMESTAMP(toltec.Date, toltec.Time) > :since
        AND toltec.Valid > 0
    ORDER BY toltec.id ASC  -- Preserves timeline order
    ```
    """
    from sqlalchemy import text

    # Use provided session or raise error
    if session is None:
        raise ValueError("session parameter is required")

    # Format timestamp for SQL query
    # SQLite stores Date as TEXT (YYYY-MM-DD) and Time as TEXT (HH:MM:SS.ssssss)
    since_date = since_timestamp.date().isoformat()
    since_time = since_timestamp.time().isoformat()

    # Query toltec_db for observations since timestamp
    # Returns all interface entries (one row per RoachIndex)
    # Join with master table to get string label instead of numeric ID
    # Returns both Valid=0 (in progress) and Valid=1 (complete) records
    # LIMIT 100 for testing - remove for production
    # Note: master.id is the string identifier (ics, tcs, etc.) used as foreign key from toltec.Master
    query = text("""
        SELECT 
            LOWER(master.label) as master,
            toltec.ObsNum as obsnum,
            toltec.SubObsNum as subobsnum,
            toltec.ScanNum as scannum,
            toltec.RoachIndex as roach_index,
            toltec.Valid as valid,
            toltec.ObsType as obs_type,
            toltec.Date as date,
            toltec.Time as time,
            toltec.FileName as filename
        FROM toltec
        JOIN master ON toltec.Master = master.id
        WHERE (toltec.Date > :since_date)
           OR (toltec.Date = :since_date AND toltec.Time >= :since_time)
        ORDER BY toltec.Date DESC, toltec.Time DESC, toltec.ObsNum DESC, 
                 toltec.SubObsNum DESC, toltec.ScanNum DESC, toltec.RoachIndex DESC
        LIMIT 100
    """)

    # Debug logging
    import logging

    logger = logging.getLogger(__name__)
    logger.info(
        f"Executing query_toltec_db_since with since_date={since_date}, since_time={since_time}"
    )

    result = session.execute(
        query, {"since_date": since_date, "since_time": since_time}
    )

    logger.info(f"Query executed successfully, fetching results...")

    # Convert rows to list of dicts with proper datetime objects
    observations = []
    for row in result:
        # Combine Date and Time into a datetime object
        from datetime import datetime, timezone

        date_str = row.date if row.date else "2024-01-01"
        time_str = row.time if row.time else "00:00:00"
        timestamp = datetime.fromisoformat(f"{date_str} {time_str}").replace(
            tzinfo=timezone.utc
        )

        observations.append(
            {
                "master": row.master,
                "obsnum": row.obsnum,
                "subobsnum": row.subobsnum,
                "scannum": row.scannum,
                "roach_index": row.roach_index,
                "valid": row.valid,
                "obs_type": row.obs_type,
                "date": row.date,
                "time": row.time,
                "timestamp": timestamp,
                "filename": row.filename,
            }
        )

    logger.info(f"query_toltec_db_since returning {len(observations)} observations")
    if observations:
        logger.info(f"Sample observation: {observations[0]}")

    return observations


def ingest_interface_from_toltec_db(
    master: str,
    obsnum: int,
    subobsnum: int,
    scannum: int,
    roach_index: int,
    toltec_session: Session,
    tolteca_session: Session,
    data_root: Path,
    location_label: str = "LMT",
) -> dict:
    """
    Ingest single interface file using DataIngestor.

    Queries toltec_db for interface metadata, constructs file path,
    and uses DataIngestor to create DataProd + DataProdSource entries.

    Parameters
    ----------
    master : str
        Master identifier (tcs/ics/clip)
    obsnum : int
        Observation number
    subobsnum : int
        Sub-observation number
    scannum : int
        Scan number
    roach_index : int
        Interface roach index (0-12)
    toltec_session : Session
        SQLAlchemy session for toltec_db (read-only)
    tolteca_session : Session
        SQLAlchemy session for tolteca_db (read-write)
    data_root : Path
        Root directory for data files
    location_label : str, optional
        Location label, by default "LMT"

    Returns
    -------
    dict
        Ingestion result with keys:
        - data_prod_pk: str - DataProd primary key
        - source_uri: str - DataProdSource URI
        - created: bool - Whether new entries were created
        - skipped: bool - Whether already existed
        - filename: str - Filename from toltec_db

    Raises
    ------
    DagsterExecutionInterruptedError
        If interface Valid flag is 0
    ValueError
        If interface not found or Location not found
    """
    from dagster import DagsterExecutionInterruptedError
    from tolteca_db.ingest import DataIngestor, guess_info_from_file
    from pathlib import Path

    # Query toltec_db for interface metadata (includes validation check)
    interface_data = query_toltec_db_interface(
        master=master,
        obsnum=obsnum,
        subobsnum=subobsnum,
        scannum=scannum,
        roach_index=roach_index,
        session=toltec_session,
    )

    # Construct file path from filename
    filename = interface_data["filename"]
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

    # Initialize DataIngestor
    ingestor = DataIngestor(
        session=tolteca_session,
        location_pk=location_label,
        master=master,
        nw_id=roach_index,
    )

    # Ingest file (creates DataProd + DataProdSource)
    data_prod, source = ingestor.ingest_file(
        file_info,
        skip_existing=True,
        obs_goal=interface_data.get("obs_goal"),
        source_name=interface_data.get("source_name"),
    )

    # Commit transaction
    tolteca_session.commit()

    # Return result
    if data_prod is not None:
        return {
            "data_prod_pk": str(data_prod.pk),
            "source_uri": source.source_uri if source else "unknown",
            "created": True,
            "skipped": False,
            "filename": filename,
        }
    else:
        # Entry already exists - fetch existing DataProd
        from sqlalchemy import select
        from tolteca_db.models.orm import DataProd, DataProdType as DataProdTypeORM

        stmt = select(DataProdTypeORM).where(DataProdTypeORM.label == "dp_raw_obs")
        dp_type = tolteca_session.scalar(stmt)

        stmt = (
            select(DataProd)
            .where(DataProd.data_prod_type_fk == dp_type.pk)
            .where(DataProd.meta["master"].as_string() == master)
            .where(DataProd.meta["obsnum"].as_integer() == obsnum)
            .where(DataProd.meta["subobsnum"].as_integer() == subobsnum)
            .where(DataProd.meta["scannum"].as_integer() == scannum)
        )
        existing = tolteca_session.scalar(stmt)

        # Calculate source_uri
        source_uri = ingestor._make_relative_uri(file_path)

        return {
            "data_prod_pk": str(existing.pk) if existing else "unknown",
            "source_uri": source_uri,
            "created": False,
            "skipped": True,
            "filename": filename,
        }


def query_toltec_db_interface(
    master: str,
    obsnum: int,
    subobsnum: int,
    scannum: int,
    roach_index: int,
    session: Session | None = None,
) -> dict:
    """
    Query specific interface entry from toltec_db and validate.

    This helper function queries a single interface (RoachIndex) entry
    for a given observation quartet and checks the Valid flag.

    Parameters
    ----------
    master : str
        Master identifier
    obsnum : int
        Observation number
    subobsnum : int
        Sub-observation number
    scannum : int
        Scan number
    roach_index : int
        RoachIndex (0-12)
    session : Session | None
        SQLAlchemy session for toltec_db (read-only)

    Returns
    -------
    dict
        Interface entry with keys:
        - master: str
        - obsnum: int
        - subobsnum: int
        - scannum: int
        - roach_index: int
        - interface: str (e.g., "toltec5")
        - array_name: str (e.g., "a1100")
        - valid: bool (True if Valid=1)
        - filename: str
        - timestamp: datetime
        - obs_type: str

    Raises
    ------
    DagsterExecutionInterruptedError
        If interface Valid flag is 0 (data acquisition in progress).
        Dagster will automatically retry when sensor creates new run for Valid=1.
    ValueError
        If interface not found in database

    Examples
    --------
    >>> entry = query_toltec_db_interface('toltec', 1, 123456, 0, 0, 5, session=session)
    >>> entry['interface']
    'toltec5'
    >>> entry['array_name']
    'a1100'
    >>> entry['valid']
    True

    Notes
    -----
    This function implements the validation gate to prevent manual materialization
    of partitions corresponding to incomplete data (Valid=0).

    The sensor already filters for Valid=1 when creating automatic RunRequests,
    but users can manually materialize any partition. This check ensures that
    manual materialization also respects the Valid flag.

    Example SQL:
    ```sql
    SELECT
        master.label as master,
        toltec.ObsNum as obsnum,
        toltec.SubObsNum as subobsnum,
        toltec.ScanNum as scannum,
        toltec.RoachIndex as roach_index,
        toltec.FileName as filename,
        toltec.Valid as valid,
        toltec.ObsType as obs_type,
        toltec.Date as date,
        toltec.Time as time
    FROM toltec
    JOIN master ON toltec.Master = master.id
    WHERE UPPER(master.label) = UPPER(:master)
        AND toltec.ObsNum = :obsnum
        AND toltec.SubObsNum = :subobsnum
        AND toltec.ScanNum = :scannum
        AND toltec.RoachIndex = :roach_index
    ```
    """
    from dagster import DagsterExecutionInterruptedError
    from tolteca_db.dagster.partitions import get_array_name_for_interface
    from sqlalchemy import text

    # Require session parameter
    if session is None:
        raise ValueError("session parameter is required")

    # Query toltec_db for specific interface entry
    query = text("""
        SELECT 
            LOWER(master.label) as master,
            toltec.ObsNum as obsnum,
            toltec.SubObsNum as subobsnum,
            toltec.ScanNum as scannum,
            toltec.RoachIndex as roach_index,
            toltec.FileName as filename,
            toltec.Valid as valid,
            toltec.ObsType as obs_type,
            toltec.Date as date,
            toltec.Time as time
        FROM toltec
        JOIN master ON toltec.Master = master.id
        WHERE UPPER(master.label) = UPPER(:master)
            AND toltec.ObsNum = :obsnum
            AND toltec.SubObsNum = :subobsnum
            AND toltec.ScanNum = :scannum
            AND toltec.RoachIndex = :roach_index
    """)

    result = session.execute(
        query,
        {
            "master": master,
            "obsnum": obsnum,
            "subobsnum": subobsnum,
            "scannum": scannum,
            "roach_index": roach_index,
        },
    ).fetchone()

    if result is None:
        raise ValueError(
            f"Interface not found in toltec_db: "
            f"{master}-{obsnum}-{subobsnum}-{scannum} roach_index={roach_index}"
        )

    # Check Valid flag and raise DagsterExecutionInterruptedError if 0
    valid = result.valid
    if valid == 0:
        interface = f"toltec{roach_index}"
        raise DagsterExecutionInterruptedError(
            f"Interface {interface} ({master}-{obsnum}-{subobsnum}-{scannum}) "
            f"is not valid (Valid=0). Data acquisition in progress. "
            f"This partition will be automatically materialized by the sensor "
            f"when Valid=1."
        )

    # Build result dict
    interface = f"toltec{roach_index}"
    array_name = get_array_name_for_interface(interface)

    # Combine Date and Time into datetime
    date_str = result.date if result.date else "2024-01-01"
    time_str = result.time if result.time else "00:00:00"
    timestamp = datetime.fromisoformat(f"{date_str} {time_str}").replace(
        tzinfo=timezone.utc
    )

    return {
        "master": result.master,
        "obsnum": result.obsnum,
        "subobsnum": result.subobsnum,
        "scannum": result.scannum,
        "roach_index": result.roach_index,
        "interface": interface,
        "array_name": array_name,
        "valid": valid == 1,
        "filename": result.filename,
        "timestamp": timestamp,
        "obs_type": result.obs_type,
    }


def query_toltec_db_quartet_status(
    master: str,
    obsnum: int,
    subobsnum: int,
    scannum: int,
    session: Session | None = None,
) -> dict:
    """
    Query validation status for all interfaces of a quartet.

    This helper function queries all 13 interface entries for a given
    observation quartet and returns aggregated status information.

    Parameters
    ----------
    master : str
        Master identifier
    obsnum : int
        Observation number
    subobsnum : int
        Sub-observation number
    scannum : int
        Scan number
    session : Session | None
        SQLAlchemy session for toltec_db (read-only)

    Returns
    -------
    dict
        Aggregated status with keys:
        - interfaces: list[int] - RoachIndex values found in database
        - valid_interfaces: list[int] - RoachIndex where Valid=1
        - invalid_interfaces: list[int] - RoachIndex where Valid=0
        - missing_interfaces: list[int] - RoachIndex not found in database
        - valid_count: int - Number of valid interfaces
        - total_found: int - Number of interfaces found
        - first_valid_time: str | None - ISO timestamp when first interface became Valid=1
        - last_valid_time: str | None - ISO timestamp when last interface became Valid=1
        - time_since_last_valid: float - Seconds since last Valid=1 transition
        - new_quartet_detected: bool - True if a newer quartet exists (definitive completion)
        - first_created: datetime - Earliest entry timestamp
        - last_updated: datetime - Latest entry timestamp

    Examples
    --------
    >>> status = query_toltec_db_quartet_status('toltec', 1, 123456, 0, 0)
    >>> status['all_valid']
    True
    >>> status['valid_count']
    13
    >>> status['wait_time_seconds']
    45.2

    Notes
    -----
    TODO: Implement actual toltec_db query with retry logic.

    Example SQL:
    ```sql
    SELECT
        toltec.RoachIndex,
        toltec.Valid,
        TIMESTAMP(toltec.Date, toltec.Time) as timestamp
    FROM toltec
    JOIN master ON toltec.Master = master.id
    WHERE master.label = :master
        AND toltec.ObsNum = :obsnum
        AND toltec.SubObsNum = :subobsnum
        AND toltec.ScanNum = :scannum
    ORDER BY timestamp ASC
    ```
    """
    from sqlalchemy import text

    # Query all 13 interfaces for this quartet
    query = text("""
        SELECT 
            toltec.RoachIndex,
            toltec.Valid,
            toltec.Date,
            toltec.Time
        FROM toltec
        JOIN master ON toltec.Master = master.id
        WHERE UPPER(master.label) = UPPER(:master)
            AND toltec.ObsNum = :obsnum
            AND toltec.SubObsNum = :subobsnum
            AND toltec.ScanNum = :scannum
        ORDER BY toltec.RoachIndex ASC
    """)

    results = session.execute(
        query,
        {
            "master": master,
            "obsnum": obsnum,
            "subobsnum": subobsnum,
            "scannum": scannum,
        },
    ).fetchall()

    # Build interface status
    all_interfaces = list(range(13))
    found_interfaces = {}
    valid_interfaces = []
    invalid_interfaces = []
    valid_timestamps = []

    for row in results:
        roach_index = row.RoachIndex
        valid = row.Valid

        # Combine Date and Time into datetime
        date_str = row.Date if row.Date else "2024-01-01"
        time_str = row.Time if row.Time else "00:00:00"
        timestamp = datetime.fromisoformat(f"{date_str} {time_str}").replace(
            tzinfo=timezone.utc
        )

        found_interfaces[roach_index] = {
            "valid": valid,
            "timestamp": timestamp,
        }

        if valid == 1:
            valid_interfaces.append(roach_index)
            valid_timestamps.append(timestamp)
        else:
            invalid_interfaces.append(roach_index)

    missing_interfaces = [i for i in all_interfaces if i not in found_interfaces]

    # Calculate timing information
    now = datetime.now(timezone.utc)
    first_valid_time = min(valid_timestamps) if valid_timestamps else None
    last_valid_time = max(valid_timestamps) if valid_timestamps else None
    time_since_last_valid = (
        (now - last_valid_time).total_seconds() if last_valid_time else float("inf")
    )

    # Check if a newer quartet exists (definitive completion signal)
    # Query for any entry with higher quartet identifiers
    newer_quartet_query = text("""
        SELECT COUNT(*) as count
        FROM toltec
        JOIN master ON toltec.Master = master.id
        WHERE UPPER(master.label) = UPPER(:master)
            AND (
                (toltec.ObsNum = :obsnum AND toltec.SubObsNum = :subobsnum AND toltec.ScanNum > :scannum)
                OR (toltec.ObsNum = :obsnum AND toltec.SubObsNum > :subobsnum)
                OR (toltec.ObsNum > :obsnum)
            )
        LIMIT 1
    """)

    newer_result = session.execute(
        newer_quartet_query,
        {
            "master": master,
            "obsnum": obsnum,
            "subobsnum": subobsnum,
            "scannum": scannum,
        },
    ).fetchone()

    new_quartet_detected = newer_result.count > 0 if newer_result else False

    # Calculate first/last created times
    all_timestamps = [info["timestamp"] for info in found_interfaces.values()]
    first_created = min(all_timestamps) if all_timestamps else now
    last_updated = max(all_timestamps) if all_timestamps else now

    return {
        "interfaces": list(found_interfaces.keys()),
        "valid_interfaces": valid_interfaces,
        "invalid_interfaces": invalid_interfaces,
        "missing_interfaces": missing_interfaces,
        "valid_count": len(valid_interfaces),
        "total_found": len(found_interfaces),
        "first_valid_time": first_valid_time.isoformat() if first_valid_time else None,
        "last_valid_time": last_valid_time.isoformat() if last_valid_time else None,
        "time_since_last_valid": time_since_last_valid,
        "new_quartet_detected": new_quartet_detected,
        "first_created": first_created,
        "last_updated": last_updated,
    }


def query_toltec_db_active_quartets(
    session: Session | None = None,
    validation_timeout_seconds: float = 30.0,
) -> list[dict]:
    """
    Query all active quartets that may be ready for completion checking.

    Active quartets are those with at least one Valid=1 interface, meaning
    data acquisition has started. This function returns quartets that should
    be evaluated for completion based on timeout or validation status.

    Parameters
    ----------
    session : Session | None
        SQLAlchemy session for toltec_db (read-only)
    validation_timeout_seconds : float
        Timeout threshold in seconds (from validation resource config)

    Returns
    -------
    list[dict]
        List of active quartets, each with keys:
        - master: str
        - obsnum: int
        - subobsnum: int
        - scannum: int
        - valid_count: int - Number of valid interfaces
        - total_count: int - Total interfaces found
        - last_valid_time: datetime - Most recent Valid=1 timestamp
        - time_since_last_valid: float - Seconds since last Valid=1

    Examples
    --------
    >>> active = query_toltec_db_active_quartets(session, timeout=30.0)
    >>> len(active)
    5
    >>> active[0]['valid_count']
    10

    Notes
    -----
    This query finds quartets where:
    1. At least one interface has Valid=1 (started)
    2. Time since last Valid=1 is approaching or exceeding timeout
    3. Not all 13 interfaces are valid yet (incomplete)

    The sensor will create run requests for these quartets, and the
    quartet_complete asset will determine if they meet completion criteria.
    """
    from sqlalchemy import text

    if session is None:
        raise ValueError("session parameter is required")

    # Query for all distinct quartets with at least one Valid=1 interface
    # Group by quartet and calculate timing
    query = text("""
        SELECT 
            LOWER(master.label) as master,
            toltec.ObsNum as obsnum,
            toltec.SubObsNum as subobsnum,
            toltec.ScanNum as scannum,
            SUM(CASE WHEN toltec.Valid = 1 THEN 1 ELSE 0 END) as valid_count,
            COUNT(*) as total_count,
            MAX(
                CASE WHEN toltec.Valid = 1 
                THEN datetime(toltec.Date || ' ' || toltec.Time)
                ELSE NULL END
            ) as last_valid_time
        FROM toltec
        JOIN master ON toltec.Master = master.id
        GROUP BY master.label, toltec.ObsNum, toltec.SubObsNum, toltec.ScanNum
        HAVING valid_count > 0
        ORDER BY toltec.ObsNum DESC, toltec.SubObsNum DESC, toltec.ScanNum DESC
        LIMIT 100
    """)

    results = session.execute(query).fetchall()

    # Build result list with timing calculations
    now = datetime.now(timezone.utc)
    active_quartets = []

    for row in results:
        # Parse last_valid_time from string
        last_valid_time = None
        if row.last_valid_time:
            last_valid_time = datetime.fromisoformat(row.last_valid_time).replace(
                tzinfo=timezone.utc
            )

        time_since_last_valid = (
            (now - last_valid_time).total_seconds() if last_valid_time else float("inf")
        )

        # Only include quartets that are potentially ready for evaluation:
        # - Have at least one valid interface (already filtered in HAVING clause)
        # - Either not all valid yet, or close to timeout
        if row.valid_count < 13 or time_since_last_valid >= (
            validation_timeout_seconds * 0.8
        ):
            active_quartets.append(
                {
                    "master": row.master,
                    "obsnum": row.obsnum,
                    "subobsnum": row.subobsnum,
                    "scannum": row.scannum,
                    "valid_count": row.valid_count,
                    "total_count": row.total_count,
                    "last_valid_time": last_valid_time,
                    "time_since_last_valid": time_since_last_valid,
                }
            )

    return active_quartets
