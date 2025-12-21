"""Testing assets for simulating data acquisition.

Provides asset for testing quartet completion behavior with simulated
data acquisition (Valid=0 → Valid=1 transitions).
"""

# CRITICAL: Cannot use `from __future__ import annotations` with Dagster
# Dagster's runtime type validation requires actual type objects, not strings
# See: https://github.com/dagster-io/dagster/issues/28342

from dagster import AssetExecutionContext, Output, asset
from sqlalchemy import text
from sqlalchemy.orm import Session

__all__ = ["acquisition_simulator"]


@asset(
    automation_condition=None,  # Manual materialization or schedule-based
    required_resource_keys={"toltec_db", "simulator"},
)
def acquisition_simulator(context: AssetExecutionContext) -> Output[str]:
    """Simulate data acquisition by managing Valid flags in toltec table.

    Acts as a cron job with configurable integration time period.

    Logic per tick:
    1. Check if latest quartet has all Valid=1 OR db is empty
       → YES: Select next quartet from source db, insert all interfaces with Valid=0
       → NO: Mark all latest quartet interfaces as Valid=1

    Parameters
    ----------
    context : AssetExecutionContext
        Dagster execution context with resources:
        - toltec_db: Test database connection
        - simulator: Simulator configuration

    Returns
    -------
    Output[str]
        Summary of simulator action

    Examples
    --------
    Manual materialization:
    >>> # dagster asset materialize -a acquisition_simulator

    Or use schedule for periodic execution:
    >>> # See test_resources.py for schedule definition
    """
    simulator = context.resources.simulator
    toltec_db = context.resources.toltec_db

    if not simulator.enabled:
        return Output("Simulator disabled")

    # Resolve obsnum filter (from date_filter or explicit obsnum_filter)
    obsnum_filter = simulator.resolve_obsnum_filter(toltec_db.source_db_url)

    if obsnum_filter and simulator.date_filter:
        context.log.info(
            f"Resolved {len(obsnum_filter)} ObsNums from date filter: {simulator.date_filter}"
        )

    with toltec_db.get_session() as session:
        # Check if db is empty or latest quartet entries are all Valid=1
        latest_quartet = session.execute(
            text("""
            SELECT 
                ObsNum, SubObsNum, ScanNum, Master,
                COUNT(*) as interface_count,
                SUM(CASE WHEN Valid = 0 THEN 1 ELSE 0 END) as invalid_count
            FROM toltec
            GROUP BY ObsNum, SubObsNum, ScanNum, Master
            ORDER BY ObsNum DESC, SubObsNum DESC, ScanNum DESC
            LIMIT 1
        """)
        ).fetchone()

        if latest_quartet is None:
            # Database is empty - insert first quartet from source db
            context.log.info("Database empty - loading first quartet from source db")
            _insert_next_quartet_from_source_db(
                context,
                session,
                toltec_db.source_db_url,
                obsnum_filter,
                simulator.source_csv_path,
                simulator.test_csv_path,
            )
            return Output("Inserted first quartet with Valid=0")

        # Check if all interfaces for latest quartet are valid
        if latest_quartet.invalid_count == 0:
            # All valid - insert next quartet
            context.log.info(
                f"Latest quartet {latest_quartet.ObsNum}-{latest_quartet.SubObsNum}-"
                f"{latest_quartet.ScanNum} all Valid=1 - loading next quartet"
            )
            _insert_next_quartet_from_source_db(
                context,
                session,
                toltec_db.source_db_url,
                obsnum_filter,
                simulator.source_csv_path,
                simulator.test_csv_path,
            )
            return Output(
                f"Inserted next quartet after completing {latest_quartet.ObsNum}-"
                f"{latest_quartet.SubObsNum}-{latest_quartet.ScanNum}"
            )

        # Some interfaces still invalid - mark them all as valid
        context.log.info(
            f"Latest quartet {latest_quartet.ObsNum}-{latest_quartet.SubObsNum}-"
            f"{latest_quartet.ScanNum} has {latest_quartet.invalid_count}/"
            f"{latest_quartet.interface_count} invalid - marking all Valid=1"
        )

        result = session.execute(
            text("""
            UPDATE toltec
            SET Valid = 1
            WHERE ObsNum = :obsnum
              AND SubObsNum = :subobsnum
              AND ScanNum = :scannum
              AND Master = :master
              AND Valid = 0
        """),
            {
                "obsnum": latest_quartet.ObsNum,
                "subobsnum": latest_quartet.SubObsNum,
                "scannum": latest_quartet.ScanNum,
                "master": latest_quartet.Master,
            },
        )

        session.commit()

        updated_count = result.rowcount

        # Check if this completes the obsnum filter
        if obsnum_filter:
            # Get all distinct ObsNums that have been inserted
            inserted_obsnums = session.execute(
                text("SELECT DISTINCT ObsNum FROM toltec WHERE 1=1 ORDER BY ObsNum")
            ).fetchall()
            inserted_obsnum_list = [row[0] for row in inserted_obsnums]

            # Check which filtered ObsNums remain
            remaining_obsnums = [
                obs for obs in obsnum_filter if obs not in inserted_obsnum_list
            ]

            if not remaining_obsnums:
                context.log.info(
                    f"All filtered ObsNums {obsnum_filter} have been inserted and marked Valid=1 - simulator complete"
                )
                return Output(
                    f"Marked {updated_count} interfaces Valid=1 for quartet "
                    f"{latest_quartet.ObsNum}-{latest_quartet.SubObsNum}-"
                    f"{latest_quartet.ScanNum}. All filtered ObsNums complete."
                )

            context.log.info(
                f"Simulator filter: {len(inserted_obsnum_list)}/{len(obsnum_filter)} ObsNums processed. "
                f"Remaining: {remaining_obsnums}"
            )

        return Output(
            f"Marked {updated_count} interfaces Valid=1 for quartet "
            f"{latest_quartet.ObsNum}-{latest_quartet.SubObsNum}-"
            f"{latest_quartet.ScanNum}"
        )


def _insert_next_quartet_from_source_db(
    context: AssetExecutionContext,
    test_session: Session,
    source_db_url: str,
    obsnum_filter: list[int] | None = None,
    source_csv_path: str | None = None,
    test_csv_path: str | None = None,
) -> None:
    """Insert next quartet from source database into test database.

    Selects the next distinct quartet (ObsNum, SubObsNum, ScanNum, Master)
    from source database that hasn't been inserted yet, and inserts all its
    interface entries with Valid=0.

    Also updates test lmtmc CSV with corresponding metadata rows if CSV paths provided.

    Parameters
    ----------
    context : AssetExecutionContext
        Dagster execution context for logging
    test_session : Session
        SQLAlchemy session for test database
    source_db_url : str
        URL of source database to copy from
    obsnum_filter : list[int] | None
        Optional list of specific ObsNums to simulate
    source_csv_path : str | None
        Path to source lmtmc CSV (full dataset)
    test_csv_path : str | None
        Path to test lmtmc CSV (simulator output)
    """
    from sqlalchemy import create_engine

    # Get last inserted quartet from test db
    last_quartet = test_session.execute(
        text("""
        SELECT MAX(ObsNum) as obsnum, MAX(SubObsNum) as subobsnum, 
               MAX(ScanNum) as scannum
        FROM toltec
    """)
    ).fetchone()

    # Connect to source database and get next quartet
    source_engine = create_engine(source_db_url)

    with Session(source_engine) as source_session:
        # Apply obsnum filter if specified
        obsnum_filter_clause = ""
        if obsnum_filter:
            obsnum_list_str = ",".join(str(obs) for obs in obsnum_filter)
            obsnum_filter_clause = f" AND ObsNum IN ({obsnum_list_str})"

        # Get next distinct quartet from source db
        if last_quartet.obsnum is None:
            # Empty db - get first quartet
            next_quartet_query = text(f"""
                SELECT DISTINCT ObsNum, SubObsNum, ScanNum, Master
                FROM toltec
                WHERE 1=1{obsnum_filter_clause}
                ORDER BY ObsNum ASC, SubObsNum ASC, ScanNum ASC
                LIMIT 1
            """)
        else:
            # Get next quartet after the last one inserted
            next_quartet_query = text(f"""
                SELECT DISTINCT ObsNum, SubObsNum, ScanNum, Master
                FROM toltec
                WHERE ((ObsNum > :last_obsnum)
                   OR (ObsNum = :last_obsnum AND SubObsNum > :last_subobsnum)
                   OR (ObsNum = :last_obsnum AND SubObsNum = :last_subobsnum 
                       AND ScanNum > :last_scannum)){obsnum_filter_clause}
                ORDER BY ObsNum ASC, SubObsNum ASC, ScanNum ASC
                LIMIT 1
            """)

        if last_quartet.obsnum is None:
            next_quartet = source_session.execute(next_quartet_query).fetchone()
        else:
            next_quartet = source_session.execute(
                next_quartet_query,
                {
                    "last_obsnum": last_quartet.obsnum,
                    "last_subobsnum": last_quartet.subobsnum,
                    "last_scannum": last_quartet.scannum,
                },
            ).fetchone()

        if next_quartet is None:
            context.log.warning("No more quartets available in source database")
            return

        # Get all interface entries for this quartet from source
        interface_entries = source_session.execute(
            text("""
                SELECT * FROM toltec
                WHERE ObsNum = :obsnum
                  AND SubObsNum = :subobsnum
                  AND ScanNum = :scannum
                  AND Master = :master
            """),
            {
                "obsnum": next_quartet.ObsNum,
                "subobsnum": next_quartet.SubObsNum,
                "scannum": next_quartet.ScanNum,
                "master": next_quartet.Master,
            },
        ).fetchall()

        # Insert all interfaces into test db with Valid=0
        for entry in interface_entries:
            entry_dict = dict(entry._mapping)
            # Override Valid to 0 for simulation
            entry_dict["Valid"] = 0

            # Convert timedelta to string for SQLite compatibility (MySQL TIME → TEXT)
            import datetime

            for key, value in entry_dict.items():
                if isinstance(value, datetime.timedelta):
                    # Convert timedelta to HH:MM:SS string format
                    total_seconds = int(value.total_seconds())
                    hours = total_seconds // 3600
                    minutes = (total_seconds % 3600) // 60
                    seconds = total_seconds % 60
                    entry_dict[key] = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

            # Build insert statement dynamically
            columns = list(entry_dict.keys())
            column_list = ", ".join(columns)
            placeholders = ", ".join([f":{col}" for col in columns])

            insert_stmt = text(
                f"INSERT INTO toltec ({column_list}) VALUES ({placeholders})"
            )

            test_session.execute(insert_stmt, entry_dict)

        test_session.commit()

        context.log.info(
            f"Inserted quartet {next_quartet.ObsNum}-{next_quartet.SubObsNum}-"
            f"{next_quartet.ScanNum} with {len(interface_entries)} interfaces "
            f"(all Valid=0)"
        )

        # Update test CSV with tel metadata for this ObsNum
        if source_csv_path and test_csv_path:
            _update_test_csv_for_obsnum(
                context, source_csv_path, test_csv_path, next_quartet.ObsNum
            )

    source_engine.dispose()


def _update_test_csv_for_obsnum(
    context: AssetExecutionContext,
    source_csv_path: str,
    test_csv_path: str,
    obsnum: int,
) -> None:
    """Update test lmtmc CSV with rows for the given ObsNum.

    Reads source CSV, filters for matching ObsNum, and appends to test CSV.
    Creates test CSV with header if it doesn't exist.

    Parameters
    ----------
    context : AssetExecutionContext
        Dagster execution context for logging
    source_csv_path : str
        Path to source lmtmc CSV (full dataset)
    test_csv_path : str
        Path to test lmtmc CSV (simulator output)
    obsnum : int
        ObsNum to copy rows for
    """
    import csv
    from pathlib import Path

    source_path = Path(source_csv_path)
    test_path = Path(test_csv_path)

    if not source_path.exists():
        context.log.warning(f"Source CSV not found: {source_csv_path}")
        return

    # Read matching rows from source CSV
    matching_rows = []
    header = None

    with source_path.open("r") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames

        for row in reader:
            # Parse ObsNum.SubObsNum.ScanNum format (e.g., "18851.0.0")
            obsnum_str = row.get("ObsNum", "")
            if not obsnum_str:
                continue

            # Extract ObsNum from "XXXXX.Y.Z" format
            parts = obsnum_str.split(".")
            if not parts:
                continue

            try:
                row_obsnum = int(float(parts[0]))
                if row_obsnum == obsnum:
                    matching_rows.append(row)
            except (ValueError, IndexError):
                continue

    if not matching_rows:
        context.log.warning(f"No CSV rows found for ObsNum {obsnum}")
        return

    # Create test CSV with header if it doesn't exist
    file_exists = test_path.exists()

    with test_path.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)

        if not file_exists:
            writer.writeheader()
            context.log.info(f"Created test CSV: {test_csv_path}")

        for row in matching_rows:
            writer.writerow(row)

    context.log.info(f"Added {len(matching_rows)} CSV rows for ObsNum {obsnum}")
