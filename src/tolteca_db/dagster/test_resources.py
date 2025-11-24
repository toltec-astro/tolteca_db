"""Test resources for simulating data acquisition.

Provides resources and utilities for testing quartet completion behavior
with simulated toltec_db.
"""

from __future__ import annotations

import os
from pathlib import Path

from dagster import ConfigurableResource, InitResourceContext
from pydantic import Field
from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from tolteca_db.db import Database, create_database

__all__ = [
    "TestToltecDBResource",
    "SimulatorConfig",
]

# Module-level singleton for shared test database
# Use file-based database to share across Dagster processes
# Use DAGSTER_HOME if set, otherwise fall back to .dagster
_DAGSTER_HOME = os.getenv("DAGSTER_HOME", ".dagster")
_SHARED_TEST_DB_PATH = Path(_DAGSTER_HOME) / "test_toltecdb.sqlite"
_SHARED_TEST_DB: Database | None = None


class SimulatorConfig(ConfigurableResource):
    """Configuration for data acquisition simulator.

    Attributes
    ----------
    integration_time_seconds : float
        Simulated integration time (period of simulator execution)
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
    """Test database resource with schema reflected from source database.

    Uses file-based database to share state across all Dagster processes
    (sensor, schedule, assets) during testing session.

    Attributes
    ----------
    source_db_url : str
        URL of source database to reflect schema from
    """

    source_db_url: str = Field(
        default="sqlite:///../run/toltecdb_last_30days.sqlite",
        description="Source database URL for schema reflection",
    )

    def setup_for_execution(self, context: InitResourceContext) -> None:
        """Initialize shared test database engine.

        Database is shared across all Dagster processes using a file.
        Only clears database on explicit request via environment variable.
        """
        global _SHARED_TEST_DB

        if _SHARED_TEST_DB is None:
            # Create database directory
            _SHARED_TEST_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

            # Check if database file exists
            db_existed = _SHARED_TEST_DB_PATH.exists()

            # Check if we should clear the database
            import os

            clear_db = os.getenv("DAGSTER_CLEAR_TEST_DB") == "1"

            if clear_db and db_existed:
                _SHARED_TEST_DB_PATH.unlink()
                context.log.info(f"✓ Cleared test database at {_SHARED_TEST_DB_PATH}")
                db_existed = False

            # Create Database instance using factory
            database_url = f"sqlite:///{_SHARED_TEST_DB_PATH}"
            _SHARED_TEST_DB = create_database(database_url, read_only=False, echo=False)

            # Check if database needs schema initialization
            # (even if file exists, it might be empty)
            needs_init = not db_existed
            if db_existed:
                # Check if database has tables
                with _SHARED_TEST_DB.session() as session:
                    result = session.execute(
                        text("SELECT name FROM sqlite_master WHERE type='table'")
                    ).fetchall()
                    if not result:
                        context.log.info(
                            "Database file exists but is empty, initializing schema..."
                        )
                        needs_init = True

            # Initialize schema if needed
            if needs_init:
                self._reflect_and_create_schema(context, _SHARED_TEST_DB)
                context.log.info(
                    f"✓ Created fresh test database at {_SHARED_TEST_DB_PATH}"
                )
            else:
                context.log.info(
                    f"✓ Using existing shared test database at {_SHARED_TEST_DB_PATH}"
                )
        else:
            context.log.info(
                f"✓ Using existing shared test database at {_SHARED_TEST_DB_PATH}"
            )

    def get_session(self) -> Session:
        """Create SQLAlchemy session for test toltec_db.

        Returns
        -------
        Session
            SQLAlchemy session for shared test database
        """
        global _SHARED_TEST_DB

        if _SHARED_TEST_DB is None:
            raise RuntimeError(
                "TestToltecDBResource not initialized. Call setup_for_execution first."
            )

        return _SHARED_TEST_DB.session()

    def _reflect_and_create_schema(
        self, context: InitResourceContext, target_db: Database
    ) -> None:
        """Reflect schema from source database.

        Parameters
        ----------
        context : InitResourceContext
            Dagster resource initialization context
        target_db : Database
            Target Database instance to create reflected schema in
        """
        # Check source database exists
        if self.source_db_url.startswith("sqlite:///"):
            db_path = self.source_db_url.replace("sqlite:///", "")
            if not Path(db_path).exists():
                raise FileNotFoundError(f"Source database not found: {db_path}")

        # Connect to source database (use raw engine for reflection)
        source_engine = create_engine(self.source_db_url)

        # Reflect all tables from source
        metadata = MetaData()
        metadata.reflect(bind=source_engine)

        context.log.info(
            f"Reflected {len(metadata.tables)} tables from source database"
        )

        # Create all tables in test database (empty, schema only)
        # Access underlying engine from Database instance
        metadata.create_all(bind=target_db.metadata_engine)

        # Populate reference tables (master, obstype)
        self._populate_reference_tables(context, source_engine, target_db)

        source_engine.dispose()

    def _populate_reference_tables(
        self,
        context: InitResourceContext,
        source_engine: Engine,
        target_db: Database,
    ) -> None:
        """Copy reference table data from source to test database.

        Copies all rows from master and obstype tables for foreign key relationships.

        Parameters
        ----------
        context : InitResourceContext
            Dagster resource initialization context
        source_engine : Engine
            Source database engine
        target_db : Database
            Target test Database instance
        """
        from sqlalchemy.orm import Session as SQLASession

        reference_tables = ["master", "obstype"]

        with SQLASession(source_engine) as source_session:
            # Use target_db.session() for proper Database pattern
            with target_db.session() as target_session:
                for table_name in reference_tables:
                    # Copy all rows from source to target
                    try:
                        rows = source_session.execute(
                            text(f"SELECT * FROM {table_name}")
                        ).fetchall()
                    except Exception as e:
                        context.log.warning(
                            f"⚠ Table {table_name} not found in source database, skipping: {e}"
                        )
                        continue

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
