"""Test resources for simulating ongoing data acquisition in Dagster.

This module provides resources and assets for testing quartet completion
behavior with simulated data acquisition (Invalid → Valid transitions).

Implementation Details
----------------------
1. Test database is created in-memory using SQLAlchemy reflection
2. Schema is reflected from real toltecdb (exact match)
3. Database starts empty
4. Simulator asset acts as a cron job with configurable period
5. Each simulator tick follows this logic:
   - If latest obs entries are all valid OR db is empty:
     → Select next quartet from real db, insert with valid=0
   - Else:
     → Mark all latest obs (valid=0) entries as valid=1

Usage
-----
Create test definitions and run with Dagster:

>>> from tolteca_db.dagster.test_resources import create_test_definitions
>>> test_defs = create_test_definitions(
...     real_db_url="sqlite:///../run/toltecdb_last_30days.sqlite",
...     integration_time_seconds=5.0
... )
>>> # Use test_defs in definitions.py or run via CLI
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

__all__ = [
    "TestToltecDBResource",
    "SimulatorConfig",
    "create_test_definitions",
]

from dagster import (
    AssetExecutionContext,
    AssetSelection,
    AutomationConditionSensorDefinition,
    ConfigurableResource,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    InitResourceContext,
    Output,
    RunRequest,
    ScheduleDefinition,
    asset,
    load_assets_from_modules,
    sensor,
)
from pydantic import Field
from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

# Module-level singleton for shared test database engine
# Each Dagster process (sensor, schedule, asset) creates its own Python interpreter,
# so we use a file-based database to share state across processes
_SHARED_TEST_DB_PATH = Path("./.dagster/test_toltecdb.sqlite")
_SHARED_TEST_ENGINE: Engine | None = None
_SCHEMA_INITIALIZED = False


class SimulatorConfig(ConfigurableResource):
    """Configuration for data acquisition simulator.

    Attributes
    ----------
    integration_time_seconds : float
        Simulated integration time (period of simulator cron)
    enabled : bool
        Whether simulator should run
    """

    integration_time_seconds: float = Field(
        default=5.0,
        description="Simulated integration time between ticks",
    )
    enabled: bool = Field(
        default=True,
        description="Whether simulator is enabled",
    )


class TestToltecDBResource(ConfigurableResource):
    """Test database resource with exact schema from real toltecdb.

    Uses module-level singleton pattern to share in-memory database across
    all asset/sensor/schedule executions during a Dagster session. The
    shared engine is created once and reused by all resource instances.

    Attributes
    ----------
    source_db_url : str
        URL of production database to reflect schema from
    """

    source_db_url: str = Field(
        default="sqlite:///../run/toltecdb_last_30days.sqlite",
        description="Production database URL for schema reflection",
    )

    def setup_for_execution(self, context: InitResourceContext) -> None:
        """Initialize shared test database engine (file-based for cross-process sharing).

        Database is shared across all Dagster processes using a file. The database
        is only cleared if it doesn't exist (fresh start) or if DAGSTER_CLEAR_TEST_DB
        environment variable is set.
        """
        global _SHARED_TEST_ENGINE, _SCHEMA_INITIALIZED

        # Use file-based database to share across Dagster processes (sensors, schedules, assets)
        if _SHARED_TEST_ENGINE is None:
            # Create database directory
            _SHARED_TEST_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

            # Check if database file exists (cross-process flag)
            db_existed = _SHARED_TEST_DB_PATH.exists()

            # Connect to file-based database (shared across all processes)
            _SHARED_TEST_ENGINE = create_engine(
                f"sqlite:///{_SHARED_TEST_DB_PATH}", echo=False
            )

            # Only initialize schema if database is new
            if not db_existed:
                self._reflect_and_create_schema(context, _SHARED_TEST_ENGINE)
                _SCHEMA_INITIALIZED = True
                context.log.info(
                    f"✓ Created fresh test database at {_SHARED_TEST_DB_PATH}"
                )
            else:
                context.log.info(
                    f"✓ Connected to existing test database at {_SHARED_TEST_DB_PATH}"
                )
        else:
            context.log.info("✓ Reusing test database connection in same process")

    def get_session(self) -> Session:
        """Create SQLAlchemy session for test toltec_db.

        Returns
        -------
        Session
            SQLAlchemy session for shared in-memory test database
        """
        global _SHARED_TEST_ENGINE

        if _SHARED_TEST_ENGINE is None:
            raise RuntimeError(
                "TestToltecDBResource not initialized. Call setup_for_execution first."
            )

        from sqlalchemy.orm import Session as SQLASession

        return SQLASession(_SHARED_TEST_ENGINE)

    def _reflect_and_create_schema(
        self, context: InitResourceContext, target_engine: Engine
    ) -> None:
        """Reflect schema from production database using SQLAlchemy metadata.

        Parameters
        ----------
        context : InitResourceContext
            Dagster resource initialization context
        target_engine : Engine
            Target engine to create reflected schema in (the shared test engine)
        """
        # Check source database exists
        if self.source_db_url.startswith("sqlite:///"):
            db_path = self.source_db_url.replace("sqlite:///", "")
            if not Path(db_path).exists():
                raise FileNotFoundError(f"Source database not found: {db_path}")

        # Connect to source database
        source_engine = create_engine(self.source_db_url)

        # Reflect all tables from source
        metadata = MetaData()
        metadata.reflect(bind=source_engine)

        context.log.info(
            f"Reflected {len(metadata.tables)} tables from source database"
        )

        # Create all tables in test database (empty, schema only)
        metadata.create_all(bind=target_engine)

        # Populate reference tables (master, obstype) from source database
        # These are required for foreign key relationships in toltec table
        self._populate_reference_tables(context, source_engine, target_engine)

        source_engine.dispose()

    def _populate_reference_tables(
        self,
        context: InitResourceContext,
        source_engine: Engine,
        target_engine: Engine,
    ) -> None:
        """Copy reference table data from source to test database.

        Copies all rows from master and obstype tables, which are required
        for foreign key relationships in the toltec table.

        Parameters
        ----------
        context : InitResourceContext
            Dagster resource initialization context
        source_engine : Engine
            Source database engine
        target_engine : Engine
            Target test database engine
        """
        from sqlalchemy.orm import Session as SQLASession

        reference_tables = ["master", "obstype"]

        with (
            SQLASession(source_engine) as source_session,
            SQLASession(target_engine) as target_session,
        ):
            for table_name in reference_tables:
                # Copy all rows from source to target
                rows = source_session.execute(
                    text(f"SELECT * FROM {table_name}")
                ).fetchall()

                if rows:
                    # Get column names from first row
                    columns = list(rows[0]._mapping.keys())
                    column_list = ", ".join(columns)
                    placeholders = ", ".join([f":{col}" for col in columns])

                    insert_stmt = text(
                        f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})"
                    )

                    for row in rows:
                        target_session.execute(insert_stmt, dict(row._mapping))

                    target_session.commit()
                    context.log.info(
                        f"✓ Copied {len(rows)} rows from {table_name} table"
                    )
                else:
                    context.log.warning(f"⚠ No rows found in {table_name} table")


@asset(
    automation_condition=None,  # Manual materialization or schedule-based
    required_resource_keys={"toltec_db", "simulator"},
)
def acquisition_simulator(context) -> Output[str]:
    """Simulate data acquisition by managing Valid flags in toltec table.

    Acts as a cron job with period = integration_time_seconds.

    Logic per tick:
    1. Check if latest quartet (obsnum/subobsnum/scannum) has all Valid=1 OR db is empty
       → YES: Select next quartet from real db, insert all interfaces with Valid=0
       → NO: Mark all latest quartet interfaces as Valid=1

    Parameters
    ----------
    context : AssetExecutionContext
        Dagster execution context with resources:
        - toltec_db: Test database engine (in-memory, reflected schema)
        - simulator: Simulator configuration

    Returns
    -------
    Output[str]
        Summary of simulator action
    """
    simulator = context.resources.simulator
    toltec_db = context.resources.toltec_db

    if not simulator.enabled:
        return Output("Simulator disabled")

    # TESTING: Only run 4 ticks to diagnose O(n²) run request issue
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
        # Using actual toltecdb schema: toltec table with Valid column
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
            # Database is empty - insert first quartet from real db
            context.log.info("Database empty - loading first quartet from real db")
            _insert_next_quartet_from_real_db(context, session, toltec_db.source_db_url)
            return Output("Inserted first quartet with Valid=0")

        # Check if all interfaces for latest quartet are valid
        if latest_quartet.invalid_count == 0:
            # All valid - insert next quartet
            context.log.info(
                f"Latest quartet {latest_quartet.ObsNum}-{latest_quartet.SubObsNum}-{latest_quartet.ScanNum} "
                f"all Valid=1 - loading next quartet"
            )
            _insert_next_quartet_from_real_db(context, session, toltec_db.source_db_url)
            return Output(
                f"Inserted next quartet after completing {latest_quartet.ObsNum}-"
                f"{latest_quartet.SubObsNum}-{latest_quartet.ScanNum}"
            )

        # Some interfaces still invalid - mark them all as valid
        context.log.info(
            f"Latest quartet {latest_quartet.ObsNum}-{latest_quartet.SubObsNum}-{latest_quartet.ScanNum} "
            f"has {latest_quartet.invalid_count}/{latest_quartet.interface_count} "
            f"invalid interfaces - marking all Valid=1"
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
            f"{latest_quartet.ObsNum}-{latest_quartet.SubObsNum}-{latest_quartet.ScanNum}"
        )


def _insert_next_quartet_from_real_db(
    context: AssetExecutionContext,
    test_session: Session,
    real_db_url: str,
) -> None:
    """Insert next quartet from real database into test database.

    Selects the next distinct quartet (ObsNum, SubObsNum, ScanNum, Master)
    from real database that hasn't been inserted yet, and inserts all its
    interface entries (all 13 RoachIndex values) with Valid=0.
    """
    # Get last inserted quartet from test db
    last_quartet = test_session.execute(
        text("""
        SELECT MAX(ObsNum) as obsnum, MAX(SubObsNum) as subobsnum, MAX(ScanNum) as scannum
        FROM toltec
    """)
    ).fetchone()

    # Connect to real database and get next quartet
    real_engine = create_engine(real_db_url)

    with Session(real_engine) as real_session:
        # Get next distinct quartet from real db
        if last_quartet.obsnum is None:
            # Empty db - get first quartet
            next_quartet_query = text("""
                SELECT ObsNum, SubObsNum, ScanNum, Master
                FROM toltec
                GROUP BY ObsNum, SubObsNum, ScanNum, Master
                ORDER BY ObsNum ASC, SubObsNum ASC, ScanNum ASC
                LIMIT 1
            """)
            next_quartet = real_session.execute(next_quartet_query).fetchone()
        else:
            # Get next quartet after last inserted
            next_quartet_query = text("""
                SELECT ObsNum, SubObsNum, ScanNum, Master
                FROM toltec
                WHERE (ObsNum > :obsnum)
                   OR (ObsNum = :obsnum AND SubObsNum > :subobsnum)
                   OR (ObsNum = :obsnum AND SubObsNum = :subobsnum AND ScanNum > :scannum)
                GROUP BY ObsNum, SubObsNum, ScanNum, Master
                ORDER BY ObsNum ASC, SubObsNum ASC, ScanNum ASC
                LIMIT 1
            """)
            next_quartet = real_session.execute(
                next_quartet_query,
                {
                    "obsnum": last_quartet.obsnum,
                    "subobsnum": last_quartet.subobsnum,
                    "scannum": last_quartet.scannum,
                },
            ).fetchone()

        if next_quartet is None:
            context.log.warning("No more quartets in real database")
            return

        # Note: master and obstype tables are already populated during schema setup
        # Get all interface entries for this quartet
        interfaces = real_session.execute(
            text("""
            SELECT id, Date, Time, RoachIndex, HostName, ObsType, Master,
                   RepeatLevel, ObsNum, SubObsNum, ScanNum,
                   TargSweepObsNum, TargSweepSubObsNum, TargSweepScanNum,
                   FileName, Valid
            FROM toltec
            WHERE ObsNum = :obsnum
              AND SubObsNum = :subobsnum
              AND ScanNum = :scannum
              AND Master = :master
            ORDER BY RoachIndex ASC
        """),
            {
                "obsnum": next_quartet.ObsNum,
                "subobsnum": next_quartet.SubObsNum,
                "scannum": next_quartet.ScanNum,
                "master": next_quartet.Master,
            },
        ).fetchall()

        # Insert all interfaces with Valid=0
        for iface in interfaces:
            test_session.execute(
                text("""
                INSERT INTO toltec (
                    id, Date, Time, RoachIndex, HostName, ObsType, Master,
                    RepeatLevel, ObsNum, SubObsNum, ScanNum,
                    TargSweepObsNum, TargSweepSubObsNum, TargSweepScanNum,
                    FileName, Valid
                )
                VALUES (
                    :id, :date, :time, :roachindex, :hostname, :obstype, :master,
                    :repeatlevel, :obsnum, :subobsnum, :scannum,
                    :targsweepobsnum, :targsweepsubobsnum, :targsweepscannum,
                    :filename, 0
                )
            """),
                {
                    "id": iface.id,
                    "date": iface.Date,
                    "time": iface.Time,
                    "roachindex": iface.RoachIndex,
                    "hostname": iface.HostName,
                    "obstype": iface.ObsType,
                    "master": iface.Master,
                    "repeatlevel": iface.RepeatLevel,
                    "obsnum": iface.ObsNum,
                    "subobsnum": iface.SubObsNum,
                    "scannum": iface.ScanNum,
                    "targsweepobsnum": iface.TargSweepObsNum,
                    "targsweepsubobsnum": iface.TargSweepSubObsNum,
                    "targsweepscannum": iface.TargSweepScanNum,
                    "filename": iface.FileName,
                },
            )

        test_session.commit()

        context.log.info(
            f"✓ Inserted quartet {next_quartet.ObsNum}-{next_quartet.SubObsNum}-{next_quartet.ScanNum} "
            f"with {len(interfaces)} interfaces (all Valid=0)"
        )


def create_test_definitions(
    real_db_url: str = "sqlite:///../run/toltecdb_last_30days.sqlite",
    integration_time_seconds: float = 5.0,
) -> Definitions:
    """Create Dagster definitions for testing with simulated acquisition.

    Parameters
    ----------
    real_db_url : str
        URL of production database for schema reflection and data
    integration_time_seconds : float
        Simulated integration time (simulator period)

    Returns
    -------
    Definitions
        Dagster definitions with test resources and simulator

    Examples
    --------
    Use in definitions.py:

    >>> import os
    >>> if os.getenv("DAGSTER_TEST_MODE"):
    ...     from tolteca_db.dagster.test_resources import create_test_definitions
    ...     defs = create_test_definitions()
    ... else:
    ...     # Production definitions
    ...     defs = Definitions(...)
    """
    from tolteca_db.dagster import assets, sensors
    from tolteca_db.dagster.resources import (
        LocationConfig,
        ToltecaDBResource,
        ValidationConfig,
    )

    # Load production assets
    all_assets = load_assets_from_modules([assets])

    # Add simulator asset
    test_assets = [acquisition_simulator]

    # Create sensor to trigger simulator at integration_time_seconds intervals
    # Use sensor instead of schedule because cron doesn't support seconds
    @sensor(
        name="simulator_trigger_sensor",
        minimum_interval_seconds=int(integration_time_seconds),
        target=acquisition_simulator,
        default_status=DefaultSensorStatus.RUNNING,
    )
    def simulator_trigger_sensor(context):
        """Trigger simulator asset at regular intervals (supports sub-minute periods)."""
        return RunRequest()

    return Definitions(
        assets=[*all_assets, *test_assets],
        sensors=[
            sensors.sync_with_toltec_db,
            simulator_trigger_sensor,
            AutomationConditionSensorDefinition(
                "quartet_automation_sensor",
                target=AssetSelection.all(),
                minimum_interval_seconds=5,
                default_status=DefaultSensorStatus.RUNNING,
            ),
        ],
        resources={
            # Test database (in-memory with reflected schema)
            "toltec_db": TestToltecDBResource(source_db_url=real_db_url),
            # Test tolteca_db (in-memory DuckDB)
            "tolteca_db": ToltecaDBResource(database_url="duckdb:///:memory:"),
            # Location config
            "location": LocationConfig(
                location_pk="LMT",
                location_name="Large Millimeter Telescope (Test)",
                data_root="./.dagster/test_data",
            ),
            # Validation config (shorter timeouts for testing)
            "validation": ValidationConfig(
                max_interface_count=13,
                disabled_interfaces=[5, 6, 10],
                validation_timeout_seconds=10.0,  # Shorter for testing
                sensor_poll_interval_seconds=2,  # More frequent for testing
                retry_on_incomplete=True,
            ),
            # Simulator config
            "simulator": SimulatorConfig(
                integration_time_seconds=integration_time_seconds,
                enabled=True,
            ),
        },
    )
