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

    # TESTING: Only run 4 ticks to diagnose performance and overhead
    # Count how many ticks have occurred by counting distinct quartets
    with toltec_db.get_session() as check_session:
        tick_count = check_session.execute(
            text("""
            SELECT COUNT(DISTINCT ObsNum || '-' || SubObsNum || '-' || ScanNum || '-' || Master)
            FROM toltec
        """)
        ).scalar()

        if tick_count >= 4:
            context.log.info(
                f"Simulator tick limit reached ({tick_count}/4 quartets) - no-op"
            )
            return Output(f"Tick limit reached ({tick_count}/4 quartets) - no-op")

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
                context, session, toltec_db.source_db_url
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
                context, session, toltec_db.source_db_url
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
        return Output(
            f"Marked {updated_count} interfaces Valid=1 for quartet "
            f"{latest_quartet.ObsNum}-{latest_quartet.SubObsNum}-"
            f"{latest_quartet.ScanNum}"
        )


def _insert_next_quartet_from_source_db(
    context: AssetExecutionContext,
    test_session: Session,
    source_db_url: str,
) -> None:
    """Insert next quartet from source database into test database.

    Selects the next distinct quartet (ObsNum, SubObsNum, ScanNum, Master)
    from source database that hasn't been inserted yet, and inserts all its
    interface entries with Valid=0.

    Parameters
    ----------
    context : AssetExecutionContext
        Dagster execution context for logging
    test_session : Session
        SQLAlchemy session for test database
    source_db_url : str
        URL of source database to copy from
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
        # Get next distinct quartet from source db
        if last_quartet.obsnum is None:
            # Empty db - get first quartet
            next_quartet_query = text("""
                SELECT DISTINCT ObsNum, SubObsNum, ScanNum, Master
                FROM toltec
                ORDER BY ObsNum ASC, SubObsNum ASC, ScanNum ASC
                LIMIT 1
            """)
        else:
            # Get next quartet after the last one inserted
            next_quartet_query = text("""
                SELECT DISTINCT ObsNum, SubObsNum, ScanNum, Master
                FROM toltec
                WHERE (ObsNum > :last_obsnum)
                   OR (ObsNum = :last_obsnum AND SubObsNum > :last_subobsnum)
                   OR (ObsNum = :last_obsnum AND SubObsNum = :last_subobsnum 
                       AND ScanNum > :last_scannum)
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

    source_engine.dispose()
