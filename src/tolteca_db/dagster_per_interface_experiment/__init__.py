"""Dagster pipeline integration for tolteca_db.

This module implements automated sync with rolling toltec_db using
1D dynamic partitions + tags for flexible filtering.

Architecture follows Dagster 2025 best practices:
- Asset-centric (not ops)
- Sensors for external system integration
- Helper functions (plain Python, not @op)
- Timeline order preservation via 1D partitions

References
----------
- Architecture: design/architecture.md → Dagster Pipeline Architecture
- Implementation: design/implementation.md → Phase 19
"""

from __future__ import annotations

__all__ = [
    "defs",
    "quartet_partitions",
    "raw_obs_product",
    "parquet_files",
    "association_groups",
    "sync_with_toltec_db",
    # quartet_completion_sensor removed - replaced by eager().without(~any_deps_missing())
]

# Import Definitions for dagster dev
from .definitions import defs
from .partitions import quartet_partitions
from .assets import (
    raw_obs_product,
    parquet_files,
    association_groups,
)
from .sensors import sync_with_toltec_db
# quartet_completion_sensor removed - replaced by AutomationCondition.any_deps_updated
