"""Dagster Definitions - main entry point for the pipeline.

This module creates the Definitions object that Dagster loads when running
`dagster dev` or deploying to production.

Entry point configured in pyproject.toml:
    [tool.dagster]
    module_name = "tolteca_db.dagster.definitions"

References
----------
- Dagster Definitions: https://docs.dagster.io/concepts/code-locations
"""

from __future__ import annotations

__all__ = ["defs"]

import os

from dagster import (
    AssetSelection,
    AutomationConditionSensorDefinition,
    DefaultSensorStatus,
    Definitions,
    load_assets_from_modules,
)

from tolteca_db.dagster import assets, sensors
from tolteca_db.dagster.resources import (
    LocationConfig,
    ToltecaDBResource,
    ToltecDBResource,
    ValidationConfig,
)

# Check if running in test mode
if os.getenv("DAGSTER_TEST_MODE") == "1":
    # Use test definitions with simulator
    from tolteca_db.dagster.test_resources import create_test_definitions

    defs = create_test_definitions(
        real_db_url="sqlite:///../run/toltecdb_last_30days.sqlite",
        integration_time_seconds=10.0,  # Simulator runs every 10 seconds
    )
else:
    # Production definitions
    # Load all assets from assets module
    all_assets = load_assets_from_modules([assets])

    # Create Definitions object
    defs = Definitions(
        assets=all_assets,
        sensors=[
            sensors.sync_with_toltec_db,
            # Automation condition sensor for declarative automation (eager with allow_missing)
            # Default to RUNNING so automation starts immediately without manual toggle
            # Set to 5 seconds to minimize delay in quartet completion
            AutomationConditionSensorDefinition(
                "quartet_automation_sensor",
                target=AssetSelection.all(),
                minimum_interval_seconds=5,
                default_status=DefaultSensorStatus.RUNNING,
            ),
        ],
        resources={
            # Local tracking database (read-write)
            "tolteca_db": ToltecaDBResource(
                database_url="duckdb:///../run/tolteca.duckdb",
            ),
            # External rolling database (read-only)
            "toltec_db": ToltecDBResource(
                database_url="sqlite:///../run/toltecdb_last_30days.sqlite",
                read_only=True,
            ),
            # Location configuration
            "location": LocationConfig(
                location_pk="LMT",
                location_name="Large Millimeter Telescope",
                data_root="../run/data_lmt",
            ),
            # Interface validation configuration
            "validation": ValidationConfig(
                max_interface_count=13,
                disabled_interfaces=[
                    5,
                    6,
                    10,
                ],  # Interfaces not operational in current observations
                validation_timeout_seconds=30.0,  # 30s since last Valid=1
                sensor_poll_interval_seconds=5,
                retry_on_incomplete=True,  # Auto-retry incomplete quartets
            ),
        },
    )
