"""Modern Dagster pipeline for TolTEC data processing.

This module implements a fan-out/fan-in architecture inspired by TolTECPipe,
where:
1. Sensor detects complete quartets (timeout-based validation)
2. Creates 1D partition per quartet
3. Graph asset fans out to process interfaces in parallel
4. Collects and aggregates results

Architecture
------------
- 1D Partitioning: One partition per quartet (obsnum-subobsnum-scannum)
- Sensor-Driven: Polls toltec_db for quartet completion with timeout logic
- Parallel Processing: Fan-out to 11 interfaces within single run
- Modern Patterns: Uses @graph_asset with DynamicOut for parallelism
- Testing Support: Acquisition simulator for testing quartet completion
"""

from __future__ import annotations

__all__ = [
    # Definitions
    "defs",
    "get_definitions",
    "get_test_definitions",
    # Core components
    "process_quartet",
    "quartet_sensor",
    # Partitions
    "quartet_partitions",
    # Testing
    "acquisition_simulator",
    # Resources
    "ToltecDBResource",
    "ToltecaDBResource",
    "LocationConfig",
    "ValidationConfig",
    "TestToltecDBResource",
    "SimulatorConfig",
]

from .assets import process_quartet
from .definitions import defs, get_definitions, get_test_definitions
from .partitions import quartet_partitions
from .resources import (
    LocationConfig,
    ToltecaDBResource,
    ToltecDBResource,
    ValidationConfig,
)
from .sensors import quartet_sensor
from .test_assets import acquisition_simulator
from .test_resources import SimulatorConfig, TestToltecDBResource
