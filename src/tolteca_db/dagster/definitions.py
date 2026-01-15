"""Dagster definitions for toltec_db pipeline.

Combines assets, sensors, and resources into a single Definitions object
for deployment. Supports both simulator (test) and production modes.

Modes:
- Production: TOLTECA_SIMULATOR_ENABLED=false, uses get_definitions()
- Test/Simulator: TOLTECA_SIMULATOR_ENABLED=true, uses get_test_definitions()
"""

from __future__ import annotations

import json
import os
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


def _get_disabled_interfaces() -> list[int]:
    """
    Get list of disabled interfaces from environment variable.
    
    Returns
    -------
    list[int]
        List of disabled interface indices
        
    Notes
    -----
    Reads from TOLTECA_DISABLED_INTERFACES environment variable.
    Expected format: JSON array of integers, e.g., "[1, 6, 10]"
    If not set or invalid, returns empty list (no interfaces disabled).
    
    Examples
    --------
    >>> os.environ["TOLTECA_DISABLED_INTERFACES"] = "[1, 6, 10]"
    >>> _get_disabled_interfaces()
    [1, 6, 10]
    """
    disabled_str = os.getenv("TOLTECA_DISABLED_INTERFACES", "[]")
    try:
        disabled = json.loads(disabled_str)
        if not isinstance(disabled, list):
            print(f"Warning: TOLTECA_DISABLED_INTERFACES must be a JSON array, got: {disabled_str}")
            return []
        if not all(isinstance(x, int) for x in disabled):
            print(f"Warning: TOLTECA_DISABLED_INTERFACES must contain only integers, got: {disabled_str}")
            return []
        return disabled
    except json.JSONDecodeError as e:
        print(f"Warning: Failed to parse TOLTECA_DISABLED_INTERFACES: {e}")
        return []


def _get_validation_timeout(default: float = 30.0) -> float:
    """
    Get validation timeout from environment variable.
    
    Parameters
    ----------
    default : float
        Default timeout in seconds if not set
        
    Returns
    -------
    float
        Validation timeout in seconds
        
    Notes
    -----
    Reads from TOLTECA_VALIDATION_TIMEOUT environment variable.
    """
    timeout_str = os.getenv("TOLTECA_VALIDATION_TIMEOUT")
    if timeout_str:
        try:
            return float(timeout_str)
        except ValueError:
            print(f"Warning: TOLTECA_VALIDATION_TIMEOUT must be a number, got: {timeout_str}")
    return default


def _get_common_resources() -> dict:
    """
    Build common resources used by both production and test modes.
    
    Returns
    -------
    dict
        Dictionary with tolteca_db, location, and validation resources
        
    Notes
    -----
    These resources are identical between production and test modes.
    Mode-specific resources (toltec_db, simulator) are added separately.
    """
    return {
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
            disabled_interfaces=_get_disabled_interfaces(),
            validation_timeout_seconds=_get_validation_timeout(default=15.0),
        ),
    }


def _build_definitions(
    resources: dict,
    extra_assets: list | None = None,
    extra_sensors: list | None = None,
) -> Definitions:
    """
    Build Dagster Definitions with common structure.
    
    Parameters
    ----------
    resources : dict
        Resource configurations
    extra_assets : list | None
        Additional assets beyond the common ones (process_quartet, association_groups)
    extra_sensors : list | None
        Additional sensors beyond the common ones (quartet_sensor, auto_materialize_sensor)
        
    Returns
    -------
    Definitions
        Dagster Definitions object
    """
    # Common assets
    assets = [process_quartet, association_groups]
    if extra_assets:
        assets.extend(extra_assets)
    
    # Common sensors
    sensors = [
        quartet_sensor,
        AutomationConditionSensorDefinition(
            name="auto_materialize_sensor",
            target=AssetSelection.all(),
            minimum_interval_seconds=10,
            default_status=DefaultSensorStatus.RUNNING,
        ),
    ]
    if extra_sensors:
        sensors.extend(extra_sensors)
    
    return Definitions(
        assets=assets,
        sensors=sensors,
        resources=resources,
    )


def get_definitions() -> Definitions:
    """
    Create production Dagster definitions.
    
    Reads all configuration from environment variables:
    - TOLTEC_DB_URL: Source database URL (telescope metadata)
    - TOLTECA_DB_URL: Target database URL (data products)
    - TOLTECA_WEB_DATA_LMT_ROOTPATH: Data root directory
    - TOLTECA_DISABLED_INTERFACES: JSON array of disabled interfaces
    - TOLTECA_VALIDATION_TIMEOUT: Validation timeout in seconds

    Returns
    -------
    Definitions
        Dagster Definitions object with assets, sensors, and resources
        
    Notes
    -----
    For test mode with simulator, use get_test_definitions() instead.

    Examples
    --------
    >>> defs = get_definitions()
    >>> # Uses TOLTEC_DB_URL and TOLTECA_DB_URL from environment
    """
    resources = {
        "toltec_db": ToltecDBResource(
            database_url=EnvVar("TOLTEC_DB_URL"),
        ),
        **_get_common_resources(),
    }

    return _build_definitions(resources)


def get_test_definitions() -> Definitions:
    """
    Create test Dagster definitions with simulator.
    
    The simulator incrementally copies quartets from the source database
    to simulate real-time data acquisition for testing the pipeline.
    
    Reads all configuration from environment variables:
    - TOLTEC_DB_SOURCE_URL: Source database to copy from (required)
    - TOLTECA_DB_URL: Target test database
    - TOLTECA_WEB_DATA_LMT_ROOTPATH: Data root directory
    - TOLTECA_SIMULATOR_INTEGRATION_TIME: Seconds between simulator ticks
    - TOLTECA_SIMULATOR_DATE: Date filter (YYYY-MM-DD)
    - TOLTECA_SIMULATOR_OBSNUMS: ObsNum filter (comma-separated)
    - LMTMC_CSV_SOURCE: Source CSV path
    - LMTMC_CSV_TEST: Test CSV path
    - TOLTECA_DISABLED_INTERFACES: JSON array of disabled interfaces
    - TOLTECA_VALIDATION_TIMEOUT: Validation timeout in seconds

    Returns
    -------
    Definitions
        Dagster Definitions with simulator, test assets, sensors, and resources
        
    Notes
    -----
    For production mode without simulator, use get_definitions() instead.

    Examples
    --------
    >>> # Set environment variables first
    >>> os.environ['TOLTEC_DB_SOURCE_URL'] = 'sqlite:///source.db'
    >>> defs = get_test_definitions()
    """
    from .test_assets import acquisition_simulator
    from .test_resources import SimulatorConfig, TestToltecDBResource

    # Get source DB URL from environment (required)
    source_db_url = os.getenv("TOLTEC_DB_SOURCE_URL")
    if not source_db_url:
        raise ValueError(
            "TOLTEC_DB_SOURCE_URL must be set when using get_test_definitions(). "
            "Example: TOLTEC_DB_SOURCE_URL=sqlite:///path/to/toltecdb.sqlite"
        )
    
    # Get integration time from environment
    integration_time_seconds = float(os.getenv("TOLTECA_SIMULATOR_INTEGRATION_TIME", "5.0"))
    
    # Get date filter from environment
    date_filter = os.getenv("TOLTECA_SIMULATOR_DATE") or None
    
    # Parse obsnum filter from environment (comma-separated list)
    obsnum_filter = None
    obsnum_filter_str = os.getenv("TOLTECA_SIMULATOR_OBSNUMS")
    if obsnum_filter_str:
        try:
            obsnum_filter = [int(x.strip()) for x in obsnum_filter_str.split(",") if x.strip()]
        except ValueError:
            print(f"Warning: Invalid TOLTECA_SIMULATOR_OBSNUMS format: {obsnum_filter_str}")
    
    # Get CSV paths from environment
    source_csv_path = os.getenv("LMTMC_CSV_SOURCE") or None
    test_csv_path = os.getenv("LMTMC_CSV_TEST") or None

    resources = {
        "toltec_db": TestToltecDBResource(
            source_db_url=source_db_url,
        ),
        **_get_common_resources(),
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

    return _build_definitions(
        resources=resources,
        extra_assets=[acquisition_simulator],
        extra_sensors=[simulator_trigger_sensor],
    )


# Module-level definitions - mode determined by TOLTECA_SIMULATOR_ENABLED
#
# Test/Simulator Mode (TOLTECA_SIMULATOR_ENABLED=true):
#   - Uses get_test_definitions()
#   - Reads all config from environment variables
#   - Creates test databases and simulator
#
# Production Mode (TOLTECA_SIMULATOR_ENABLED=false):
#   - Uses get_definitions()
#   - Reads all config from environment variables
#   - Uses real databases without simulator

_simulator_enabled = os.getenv("TOLTECA_SIMULATOR_ENABLED", "true").lower() in ("true", "1", "yes")

if _simulator_enabled:
    # Test mode with simulator - all config from environment
    defs = get_test_definitions()
else:
    # Production mode - all config from environment
    defs = get_definitions()
