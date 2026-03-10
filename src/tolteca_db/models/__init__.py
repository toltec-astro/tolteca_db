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
    # Domain dataclasses
    "RawObs",
    "DataProd",
    "StorageRoot",
    "StorageFile",
    "AssocGroup",
    "AssocEdge",
    "ObsFlag",
    "DataProdFlag",
    "Task",
    "Event",
    # ORM → domain converters
    "raw_obs_from_record",
    "data_prod_from_record",
    "storage_root_from_record",
    "storage_file_from_record",
    "assoc_group_from_record",
    "assoc_edge_from_record",
    "obs_flag_from_record",
    "data_prod_flag_from_record",
    "task_from_record",
    "event_from_record",
    # JSON serialisation
    "retort",
    "to_dict",
    "from_dict",
]

from .metadata import (
    AssocEdge,
    AssocGroup,
    DataProd,
    DataProdFlag,
    Event,
    ObsFlag,
    RawObs,
    StorageFile,
    StorageRoot,
    Task,
    assoc_edge_from_record,
    assoc_group_from_record,
    data_prod_flag_from_record,
    data_prod_from_record,
    event_from_record,
    from_dict,
    obs_flag_from_record,
    raw_obs_from_record,
    retort,
    storage_file_from_record,
    storage_root_from_record,
    task_from_record,
    to_dict,
)
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

