"""Data product association generation.

This module provides collators that group related observations and create
associations between them. Modeled after tolteca_web's collector patterns.
"""

from __future__ import annotations

from .collators import (
    CalGroupCollator,
    DriveFitCollator,
    FocusGroupCollator,
)
from .generator import AssociationGenerator, AssociationStats
from .pool import ObservationPool, PoolRow
from .state import (
    AssociationState,
    DatabaseBackend,
    FilesystemBackend,
    GroupInfo,
    StateBackend,
)

__all__ = [
    "AssociationGenerator",
    "AssociationStats",
    "CalGroupCollator",
    "DriveFitCollator",
    "FocusGroupCollator",
    "ObservationPool",
    "PoolRow",
    "AssociationState",
    "DatabaseBackend",
    "FilesystemBackend",
    "GroupInfo",
    "StateBackend",
]
