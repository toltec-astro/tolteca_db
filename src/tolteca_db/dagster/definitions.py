"""Dagster definitions for toltec_db pipeline.

Combines assets, sensors, and resources into a single Definitions object
for deployment. Supports both test and production modes.
"""

from __future__ import annotations

from pathlib import Path

from dagster import (
    AssetSelection,
    AutomationConditionSensorDefinition,
    DefaultSensorStatus,
    Definitions,
    EnvVar,
    RunRequest,
    sensor,
)

from .assets import association_groups, process_quartet
from .resources import (
    LocationConfig,
    ToltecaDBResource,
    ToltecDBResource,
    ValidationConfig,
)
from .sensors import quartet_sensor

__all__ = ["defs", "get_definitions", "get_test_definitions"]


# Default test configuration (uses test_toltecdb.sqlite)
DEFAULT_TEST_DB = Path.home() / ".dagster" / "test_toltecdb.sqlite"


def get_definitions(test_mode: bool = False) -> Definitions:
    """
    Create Dagster definitions for the pipeline.

    Parameters
    ----------
    test_mode : bool, optional
        If True, use test database paths and configuration.
        If False, use production paths from environment variables.

    Returns
    -------
    Definitions
        Dagster Definitions object with assets, sensors, and resources

    Examples
    --------
    Test mode (for development):
    >>> defs = get_definitions(test_mode=True)
    >>> # Uses ~/.dagster/test_toltecdb.sqlite

    Production mode:
    >>> defs = get_definitions(test_mode=False)
    >>> # Uses TOLTEC_DB_PATH and TOLTECA_DB_PATH from environment
    """
    if test_mode:
        # Test mode: Use local test database (SQLite for both)
        resources = {
            "toltec_db": ToltecDBResource(
                database_url=f"sqlite:///{DEFAULT_TEST_DB}",
            ),
            "tolteca_db": ToltecaDBResource(
                database_url="sqlite:///.dagster/test_tolteca.sqlite",
            ),
            "location": LocationConfig(
                location_pk="LMT",
                location_name="Large Millimeter Telescope",
                data_root=None,
            ),
            "validation": ValidationConfig(
                max_interface_count=13,
                disabled_interfaces=[1, 6],  # toltec1 and toltec6 disabled
                validation_timeout_seconds=30.0,  # 30 second timeout
            ),
        }
    else:
        # Production mode: Use environment variables
        resources = {
            "toltec_db": ToltecDBResource(
                database_url=EnvVar("TOLTEC_DB_URL"),
            ),
            "tolteca_db": ToltecaDBResource(
                database_url=EnvVar("TOLTECA_DB_URL"),
            ),
            "location": LocationConfig(
                location_pk="LMT",
                location_name="Large Millimeter Telescope",
                data_root=EnvVar("TOLTECA_WEB_DATA_LMT_ROOTPATH"),
            ),
            "validation": ValidationConfig(
                max_interface_count=13,
                disabled_interfaces=[1, 6],
                validation_timeout_seconds=30.0,
            ),
        }

    return Definitions(
        assets=[process_quartet, association_groups],
        sensors=[
            quartet_sensor,
            AutomationConditionSensorDefinition(
                name="auto_materialize_sensor",
                target=AssetSelection.all(),
                minimum_interval_seconds=10,
                default_status=DefaultSensorStatus.RUNNING,
            ),
        ],
        resources=resources,
    )


def get_test_definitions(
    source_db_url: str = "sqlite:///../run/toltecdb_last_30days.sqlite",
    integration_time_seconds: float = 5.0,
    date_filter: str | None = None,
    obsnum_filter: list[int] | None = None,
    data_root: str | None = None,
    source_csv_path: str | None = None,
    test_csv_path: str | None = None,
) -> Definitions:
    """
    Create test Dagster definitions with simulator for testing.

    Parameters
    ----------
    source_db_url : str
        URL of source database to copy quartets from
    integration_time_seconds : float
        Simulated integration time between simulator ticks
    date_filter : str | None
        Optional date to simulate (format: YYYY-MM-DD)
    obsnum_filter : list[int] | None
        Optional list of specific ObsNums to simulate (overrides date_filter)
    data_root : str | None
        Path to data root directory for file storage
    source_csv_path : str | None
        Path to source lmtmc CSV (full dataset)
    test_csv_path : str | None
        Path to test lmtmc CSV (simulator output)

    Returns
    -------
    Definitions
        Dagster Definitions with test assets, sensors, and resources

    Examples
    --------
    Use in test workspace:
    >>> from tolteca_db.dagster import get_test_definitions
    >>> defs = get_test_definitions(
    ...     source_db_url="sqlite:///../run/toltecdb_last_30days.sqlite",
    ...     integration_time_seconds=5.0,
    ...     obsnum_filter=[145647, 145648]
    ... )
    """
    from .test_assets import acquisition_simulator
    from .test_resources import SimulatorConfig, TestToltecDBResource

    test_resources = {
        # Use TestToltecDBResource that reflects schema from source
        "toltec_db": TestToltecDBResource(
            source_db_url=source_db_url,
        ),
        # Use SQLite for tolteca_db in test mode (concurrent write safe)
        "tolteca_db": ToltecaDBResource(
            database_url="sqlite:///.dagster/test_tolteca.sqlite",
        ),
        "location": LocationConfig(
            location_pk="LMT",
            location_name="Large Millimeter Telescope (Test)",
            data_root=data_root,
        ),
        "validation": ValidationConfig(
            max_interface_count=13,
            disabled_interfaces=[1, 6],
            validation_timeout_seconds=10.0,  # Shorter for testing
            sensor_poll_interval_seconds=2,  # More frequent for testing
        ),
        "simulator": SimulatorConfig(
            integration_time_seconds=integration_time_seconds,
            enabled=True,
            date_filter=date_filter,
            obsnum_filter=obsnum_filter,
            source_csv_path=source_csv_path,
            test_csv_path=test_csv_path,
        ),
    }

    # Create sensor to trigger simulator automatically (supports sub-minute intervals)
    @sensor(
        name="simulator_trigger_sensor",
        minimum_interval_seconds=int(integration_time_seconds),
        target=acquisition_simulator,
        default_status=DefaultSensorStatus.RUNNING,  # Auto-start
    )
    def simulator_trigger_sensor(context):
        """Trigger simulator asset at regular intervals."""
        return RunRequest()

    return Definitions(
        assets=[process_quartet, acquisition_simulator, association_groups],
        sensors=[
            quartet_sensor,
            simulator_trigger_sensor,
            AutomationConditionSensorDefinition(
                name="auto_materialize_sensor",
                target=AssetSelection.all(),
                minimum_interval_seconds=10,
                default_status=DefaultSensorStatus.RUNNING,
            ),
        ],
        resources=test_resources,
    )


# Default definitions - check TOLTECA_SIMULATOR_ENABLED to determine mode
# If simulator is enabled (or not specified), use test mode with simulator
# If simulator is disabled, use production mode without simulator

_simulator_enabled = EnvVar("TOLTECA_SIMULATOR_ENABLED").get_value("true").lower() in ("true", "1", "yes")

if _simulator_enabled:
    # Test mode with simulator for development
    # Parse obsnum filter from environment (comma-separated list)
    _obsnum_filter_str = EnvVar("TOLTECA_SIMULATOR_OBSNUMS").get_value("")
    _obsnum_filter = None
    if _obsnum_filter_str:
        try:
            _obsnum_filter = [int(x.strip()) for x in _obsnum_filter_str.split(",") if x.strip()]
        except ValueError:
            pass  # Invalid format, use None

    # Get date filter from environment (format: YYYY-MM-DD)
    _date_filter = EnvVar("TOLTECA_SIMULATOR_DATE").get_value("")
    if not _date_filter:
        _date_filter = None

    # Get CSV paths from environment
    _source_csv_path = EnvVar("LMTMC_CSV_SOURCE").get_value("")
    if not _source_csv_path:
        _source_csv_path = None

    _test_csv_path = EnvVar("LMTMC_CSV_TEST").get_value("")
    if not _test_csv_path:
        _test_csv_path = None

    defs = get_test_definitions(
        source_db_url=EnvVar("TOLTEC_DB_SOURCE_URL").get_value(
            "sqlite:///../run/toltecdb_last_30days.sqlite"
        ),
        integration_time_seconds=float(EnvVar("TOLTECA_SIMULATOR_INTEGRATION_TIME").get_value("15.0")),
        date_filter=_date_filter,
        obsnum_filter=_obsnum_filter,
        data_root=EnvVar("TOLTECA_WEB_DATA_LMT_ROOTPATH").get_value(None),
        source_csv_path=_source_csv_path,
        test_csv_path=_test_csv_path,
    )
else:
    # Production mode - no simulator, use real databases
    defs = get_definitions(test_mode=False)
