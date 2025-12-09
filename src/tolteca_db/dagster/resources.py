"""Resource definitions for Dagster pipeline.

Provides database connections and configuration as Dagster resources.

References
----------
- Dagster Resources: https://docs.dagster.io/guides/build/external-resources
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

__all__ = [
    "LocationConfig",
    "ToltecDBResource",
    "ToltecaDBResource",
    "ValidationConfig",
]

from dagster import ConfigurableResource
from sqlalchemy import create_engine
from sqlalchemy.orm import Session as SQLASession


class ToltecaDBResource(ConfigurableResource):
    """Resource for tolteca_db (local tracking database, read-write).

    Attributes
    ----------
    database_url : str
        Database connection URL. Default: duckdb:///tolteca.duckdb

    Examples
    --------
    >>> tolteca_db = ToltecaDBResource(database_url="duckdb:///tolteca.duckdb")
    >>> with tolteca_db.get_session() as session:
    ...     products = session.query(DataProd).all()
    """

    database_url: str = "duckdb:///tolteca.duckdb"

    @property
    def _effective_database_url(self) -> str:
        """Get effective database URL, respecting test mode.

        In test mode, override database_url to use SQLite with WAL for
        multiprocess safety. SQLite metadata + DuckDB queries avoids
        write conflicts between parallel Dagster workers.
        """
        import os

        if os.getenv("DAGSTER_TEST_MODE") == "1":
            from pathlib import Path

            # Use SQLite for metadata (WAL mode for concurrent writes)
            # DuckDB will be used internally for Parquet queries
            dagster_home = Path(os.getenv("DAGSTER_HOME", ".dagster"))
            test_db_path = dagster_home / "test_tolteca.sqlite"
            return f"sqlite:///{test_db_path.absolute()}"
        return self.database_url

    def setup_for_execution(self, context) -> None:
        """Initialize database schema and populate registry tables on Dagster startup.

        Creates all tables (location, data_prod, flag_definition, etc.) if they don't
        already exist, then populates registry tables (DataProdType, DataKind, Flag,
        Location) with standard TolTEC values. Safe to call multiple times (idempotent).

        In test mode, ensures shared test database exists and is initialized once
        across all subprocesses using file-based locking.
        """
        from tolteca_db.db import create_database, populate_registry_tables
        from pathlib import Path
        import os
        import fcntl

        db_url = self._effective_database_url

        # In test mode, use file lock to ensure only one process initializes database
        if os.getenv("DAGSTER_TEST_MODE") == "1":
            dagster_home = Path(os.getenv("DAGSTER_HOME", ".dagster"))
            test_db_path = dagster_home / "test_tolteca.sqlite"
            test_db_path.parent.mkdir(parents=True, exist_ok=True)
            lock_file_path = dagster_home / "test_tolteca.lock"
            init_marker_path = dagster_home / "test_tolteca.initialized"

            # Check if database is already initialized
            if test_db_path.exists() and init_marker_path.exists():
                context.log.info(f"✓ Using existing test database at {test_db_path}")
                return

            # Try to acquire exclusive lock
            lock_file = open(lock_file_path, "w")
            try:
                # Non-blocking lock attempt
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

                # We got the lock - check again if someone initialized while we waited
                if test_db_path.exists() and init_marker_path.exists():
                    context.log.info(
                        "✓ Database already initialized by another process"
                    )
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                    return

                # We're responsible for initialization - clear old DB if exists
                if test_db_path.exists():
                    test_db_path.unlink()
                    context.log.info(
                        f"✓ Cleared previous test database at {test_db_path}"
                    )
                if init_marker_path.exists():
                    init_marker_path.unlink()

                context.log.info(f"Initializing tolteca_db schema at {db_url}")
                db = create_database(database_url=db_url)
                db.create_tables()
                context.log.info("✓ tolteca_db schema initialized")

                # Populate registry tables
                context.log.info("Populating registry tables...")
                with db.session() as session:
                    counts = populate_registry_tables(session)
                    context.log.info(f"✓ Registry tables populated: {counts}")
                    
                    # Create Location entry for LMT using data root from environment
                    from tolteca_db.models import Location
                    from sqlalchemy import select
                    stmt = select(Location).where(Location.label == "LMT")
                    existing = session.scalar(stmt)
                    if not existing:
                        # Get data root from environment (should be expanded by CLI's load_dotenv)
                        data_lmt_root = os.getenv("TOLTECA_WEB_DATA_LMT_ROOTPATH", "/data/lmt")
                        root_uri = f"file://{data_lmt_root}" if not data_lmt_root.startswith("file://") else data_lmt_root
                        new_location = Location(
                            label="LMT",
                            location_type="filesystem",
                            root_uri=root_uri,
                            priority=1,
                            meta={"site": "Large Millimeter Telescope", "country": "Mexico"},
                        )
                        session.add(new_location)
                        session.commit()
                        context.log.info(f"✓ Created Location: LMT at {root_uri}")
                    else:
                        context.log.info(f"✓ Location LMT already exists: {existing.root_uri}")

                db.close()

                # Mark as initialized
                init_marker_path.touch()
                context.log.info("✓ Test database initialization complete")

                # Release lock
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

            except BlockingIOError:
                # Another process holds the lock - wait for it to finish
                context.log.info(
                    "Waiting for another process to initialize database..."
                )
                fcntl.flock(
                    lock_file.fileno(), fcntl.LOCK_SH
                )  # Shared lock - wait for exclusive to release
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                context.log.info("✓ Database initialized by another process")
            finally:
                lock_file.close()
        else:
            # Production mode - idempotent initialization
            context.log.info(f"Initializing tolteca_db schema at {db_url}")
            db = create_database(database_url=db_url)
            db.create_tables()
            context.log.info("✓ tolteca_db schema initialized")

            # Populate registry tables
            context.log.info("Populating registry tables...")
            with db.session() as session:
                counts = populate_registry_tables(session)
                context.log.info(f"✓ Registry tables populated: {counts}")
                
                # Create Location entry for LMT using data root from environment
                from tolteca_db.models import Location
                from sqlalchemy import select
                stmt = select(Location).where(Location.label == "LMT")
                existing = session.scalar(stmt)
                if not existing:
                    import os
                    # Get data root from environment (should be expanded by CLI's load_dotenv)
                    data_lmt_root = os.getenv("TOLTECA_WEB_DATA_LMT_ROOTPATH", "/data/lmt")
                    root_uri = f"file://{data_lmt_root}" if not data_lmt_root.startswith("file://") else data_lmt_root
                    new_location = Location(
                        label="LMT",
                        location_type="filesystem",
                        root_uri=root_uri,
                        priority=1,
                        meta={"site": "Large Millimeter Telescope", "country": "Mexico"},
                    )
                    session.add(new_location)
                    session.commit()
                    context.log.info(f"✓ Created Location: LMT at {root_uri}")
                else:
                    context.log.info(f"✓ Location LMT already exists: {existing.root_uri}")

            db.close()

    def get_session(self) -> Session:
        """Create SQLAlchemy session for tolteca_db.

        Returns
        -------
        Session
            SQLAlchemy session (read-write access)
        """
        from tolteca_db.db import create_database

        # Use effective database URL (respects test mode)
        db = create_database(database_url=self._effective_database_url)
        return db.session()


class ToltecDBResource(ConfigurableResource):
    """Resource for toltec_db (external rolling database, read-only).

    Attributes
    ----------
    database_url : str
        Database connection URL for external toltec_db
    read_only : bool
        Force read-only access (default: True)

    Examples
    --------
    >>> toltec_db = ToltecDBResource(
    ...     database_url="mysql://readonly@toltec_host/toltec"
    ... )
    >>> with toltec_db.get_session() as session:
    ...     obs = session.execute("SELECT * FROM toltec WHERE ObsNum=123456")

    Notes
    -----
    Security: Always use read-only credentials for external toltec_db.
    This resource should NEVER write to toltec_db.
    """

    database_url: str
    read_only: bool = True

    def get_session(self) -> Session:
        """Create SQLAlchemy session for toltec_db.

        Returns
        -------
        Session
            SQLAlchemy session (read-only access)

        Raises
        ------
        RuntimeError
            If read_only=False (safety check)
        """
        if not self.read_only:
            raise RuntimeError(
                "ToltecDBResource must be read-only. Never write to external toltec_db."
            )

        # SQLite doesn't support read_only in connect_args - use URI mode instead
        engine = create_engine(
            self.database_url,
            connect_args={"check_same_thread": False}
            if "sqlite" in self.database_url
            else {},
        )
        return SQLASession(engine)


class LocationConfig(ConfigurableResource):
    """Configuration for data storage location.

    Attributes
    ----------
    location_pk : str
        Location primary key (e.g., 'LMT', 'UMass')
    location_name : str
        Human-readable location name
    data_root : Path | None
        Root path for data files

    Examples
    --------
    >>> location = LocationConfig(
    ...     location_pk="LMT",
    ...     location_name="Large Millimeter Telescope",
    ...     data_root="/data/lmt"
    ... )
    """

    location_pk: str = "LMT"
    location_name: str = "Large Millimeter Telescope"
    data_root: str | None = None

    def get_data_root(self) -> Path | None:
        """Get data root as Path object.

        Returns
        -------
        Path | None
            Data root path or None if not configured
        """
        return Path(self.data_root) if self.data_root else None


class ValidationConfig(ConfigurableResource):
    """Configuration for interface validation behavior.

    Attributes
    ----------
    max_interface_count : int
        Maximum possible interfaces (default: 13 for TolTEC)
    disabled_interfaces : list[int]
        List of RoachIndex values for known-disabled interfaces (default: [])
    validation_timeout_seconds : float
        Timeout for no new interfaces becoming valid (default: 30s)
        An observation is complete when no new Valid=1 transitions occur within this window
    sensor_poll_interval_seconds : int
        Sensor polling interval for checking toltec_db (default: 5 seconds)
    retry_on_incomplete : bool
        Whether to raise DagsterExecutionInterruptedError for incomplete quartets (default: True)

    Examples
    --------
    >>> validation = ValidationConfig(
    ...     max_interface_count=13,
    ...     disabled_interfaces=[3, 7],  # toltec3 and toltec7 known broken
    ...     validation_timeout_seconds=30.0,
    ...     sensor_poll_interval_seconds=5,
    ...     retry_on_incomplete=True,
    ... )

    Notes
    -----
    Timeout-Based Completion Strategy:
    1. Observation starts with all interfaces Valid=0
    2. First interface turns Valid=1 → start timer
    3. Each time another interface turns Valid=1 → reset timer
    4. Observation marked complete when:
       - Timer expires (no new Valid=1 for validation_timeout_seconds), OR
       - All non-disabled interfaces are Valid=1
    5. This handles:
       - Disabled/broken interfaces (configured via disabled_interfaces)
       - Stuck data acquisition (timeout prevents indefinite waiting)
       - Variable number of active interfaces per observation

    Future Enhancement:
    - Dynamic interface list via API query for per-observation enabled interfaces
    - For now, uses static disabled_interfaces configuration
    - Timeout check ensures completion even if interface list is incomplete

    Timeout Semantics:
    - validation_timeout_seconds: Time since LAST Valid=1 transition
    - NOT total elapsed time since first Valid=1
    - Resets on every new interface becoming valid
    - Typical value: 30s (2-3× sensor poll interval for safety margin)
    """

    max_interface_count: int = 13
    disabled_interfaces: list[int] = []
    # Time since LAST Valid=1 transition (resets on each new valid) before marking complete
    validation_timeout_seconds: float = 30.0
    sensor_poll_interval_seconds: int = 5
    retry_on_incomplete: bool = True

    def get_expected_interfaces(self) -> set[int]:
        """Get set of expected (enabled) interface RoachIndex values.

        Returns
        -------
        set[int]
            Set of RoachIndex values (0-12) that should be present and valid

        Examples
        --------
        >>> config = ValidationConfig(disabled_interfaces=[3, 7])
        >>> config.get_expected_interfaces()
        {0, 1, 2, 4, 5, 6, 8, 9, 10, 11, 12}  # Excludes 3 and 7
        """
        all_interfaces = set(range(self.max_interface_count))
        return all_interfaces - set(self.disabled_interfaces)

    def is_interface_expected(self, roach_index: int) -> bool:
        """Check if interface is expected to be valid.

        Parameters
        ----------
        roach_index : int
            RoachIndex value (0-12)

        Returns
        -------
        bool
            True if interface is expected (not disabled)
        """
        return roach_index not in self.disabled_interfaces
