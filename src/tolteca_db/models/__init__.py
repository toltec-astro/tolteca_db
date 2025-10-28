"""Data models for tolteca_db."""

from __future__ import annotations

__all__ = [
    # ORM models
    "DataProd",
    "DataProdSource",
    "DataProdAssoc",
    "DataProdFlag",
    "Flag",
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
    DataProd,
    DataProdAssoc,
    DataProdFlag,
    DataProdSource,
    EventLog,
    Flag,
    Location,
    ReductionTask,
    TaskInput,
    TaskOutput,
)
