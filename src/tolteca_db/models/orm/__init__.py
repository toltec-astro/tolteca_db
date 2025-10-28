"""SQLAlchemy 2.0 ORM models for tolteca_db.

Split into logical modules for maintainability:
- base.py - Base class and common imports
- data_prod.py - Data product models (DataProd, DataKind, DataProdDataKind)
- storage.py - Source and location models (DataProdSource, Location)
- flag.py - Quality flag models (Flag, DataProdFlag)
- task.py - Task orchestration models (ReductionTask, TaskInput, TaskOutput)
- assoc.py - Provenance association models (DataProdAssoc)
- event.py - Event log model (EventLog)

Architecture:
- Unified DataProd table for RAW and REDUCED products (product_kind discriminator)
- Content-addressable IDs using blake3 hashing
- Multi-location storage support (composite keys)
- Quality flag system with severity levels
- Idempotent reduction tasks
- Append-only event log for audit trail

Note: Uses pure SQLAlchemy 2.0 ORM (not SQLModel) due to Python 3.13 + SQLAlchemy 2.0
      compatibility requirements. SQLModel 0.0.27 does not support Mapped[] type hints.

Migration: Converted from SQLModel to SQLAlchemy 2.0 on 2025-10-21
Refactored: Split into modules on 2025-10-22
"""

from __future__ import annotations

from tolteca_db.models.orm.assoc import DataProdAssoc, DataProdAssocType
from tolteca_db.models.orm.base import Base
from tolteca_db.models.orm.data_prod import DataKind, DataProd, DataProdDataKind, DataProdType
from tolteca_db.models.orm.event import EventLog
from tolteca_db.models.orm.flag import DataProdFlag, Flag
from tolteca_db.models.orm.source import DataProdSource, Location
from tolteca_db.models.orm.task import ReductionTask, TaskInput, TaskOutput

__all__ = [
    "Base",
    "DataKind",
    "DataProd",
    "DataProdAssoc",
    "DataProdAssocType",
    "DataProdDataKind",
    "DataProdType",
    "DataProdFlag",
    "DataProdSource",
    "EventLog",
    "Flag",
    "Location",
    "ReductionTask",
    "TaskInput",
    "TaskOutput",
]
