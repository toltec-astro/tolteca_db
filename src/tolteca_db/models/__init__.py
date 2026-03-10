"""Data models for tolteca_db."""

from __future__ import annotations

from tolteca_db.models.metadata import (
    AnyDataProdMeta,
    AnyInterfaceMeta,
    AstigGroupMeta,
    CalGroupMeta,
    DataProdMetaBase,
    DrivefitMeta,
    FocusGroupMeta,
    InterfaceFileMeta,
    NamedGroupMeta,
    OofGroupMeta,
    ProcessContext,
    RawObsMeta,
    ReducedObsMeta,
    RoachInterfaceMeta,
    TelInterfaceMeta,
    adaptix_json_type,
)
from tolteca_db.models.orm import (
    Base,
    DataKind,
    DataProd,
    DataProdAssoc,
    DataProdAssocType,
    DataProdDataKind,
    DataProdFlag,
    DataProdSource,
    DataProdType,
    EventLog,
    Flag,
    Location,
)

__all__ = [
    # ORM models
    "Base",
    "Location",
    "DataKind",
    "DataProdType",
    "DataProd",
    "DataProdDataKind",
    "DataProdSource",
    "DataProdAssocType",
    "DataProdAssoc",
    "Flag",
    "DataProdFlag",
    "EventLog",
    # Metadata dataclasses
    "AnyDataProdMeta",
    "AnyInterfaceMeta",
    "AstigGroupMeta",
    "CalGroupMeta",
    "DataProdMetaBase",
    "DrivefitMeta",
    "FocusGroupMeta",
    "InterfaceFileMeta",
    "NamedGroupMeta",
    "OofGroupMeta",
    "ProcessContext",
    "RawObsMeta",
    "ReducedObsMeta",
    "RoachInterfaceMeta",
    "TelInterfaceMeta",
    "adaptix_json_type",
]
