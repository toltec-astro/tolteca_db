"""Data models for tolteca_db."""

from __future__ import annotations

__all__ = [
    # ORM models
    "DataProduct",
    "DataProductStorage",
    "DataProductAssoc",
    "DataProductFlag",
    "FlagDefinition",
    "Location",
    "ReductionTask",
    "TaskInput",
    "TaskOutput",
    "EventLog",
    # Metadata models
    "DataProdMetaBase",
    "RawObsMeta",
    "ReducedObsMeta",
    "CalGroupMeta",
    "DrivefitMeta",
    "FocusGroupMeta",
    "NamedGroupMeta",
]

from .metadata import (
    CalGroupMeta,
    DataProdMetaBase,
    DrivefitMeta,
    FocusGroupMeta,
    NamedGroupMeta,
    RawObsMeta,
    ReducedObsMeta,
)
from .orm import (
    DataProduct,
    DataProductAssoc,
    DataProductFlag,
    DataProductStorage,
    EventLog,
    FlagDefinition,
    Location,
    ReductionTask,
    TaskInput,
    TaskOutput,
)
