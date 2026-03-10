"""Data models for tolteca_db."""

from __future__ import annotations

__all__ = [
    # ORM models
    "Base",
    "RawObsRecord",
    "DataProdRecord",
    "StorageRootRecord",
    "FileRecord",
    "AssocRecord",
    "AssocEdgeRecord",
    "ObsFlagRecord",
    "DataProdFlagRecord",
    "TaskRecord",
    "EventRecord",
]

from .orm import (
    AssocEdgeRecord,
    AssocRecord,
    Base,
    DataProdFlagRecord,
    DataProdRecord,
    EventRecord,
    FileRecord,
    ObsFlagRecord,
    RawObsRecord,
    StorageRootRecord,
    TaskRecord,
)

