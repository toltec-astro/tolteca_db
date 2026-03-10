"""SQLAlchemy 2.x ORM models for tolteca_db v3.x.

Module layout
-------------
- base.py       — ``Base`` (DeclarativeBase)
- registry.py   — ``RawObsRecord``, ``DataProdRecord``
- data_prod.py  — ``StorageRootRecord``, ``FileRecord``
- assoc.py      — ``AssocRecord``, ``AssocEdgeRecord``
- flag.py       — ``ObsFlagRecord``, ``DataProdFlagRecord``
- task.py       — ``TaskRecord``
- event.py      — ``EventRecord``

Import this package to register all mappers with ``Base.metadata``.
"""

from __future__ import annotations

from tolteca_db.models.orm.assoc import AssocEdgeRecord, AssocRecord
from tolteca_db.models.orm.base import Base
from tolteca_db.models.orm.data_prod import FileRecord, StorageRootRecord
from tolteca_db.models.orm.event import EventRecord
from tolteca_db.models.orm.flag import DataProdFlagRecord, ObsFlagRecord
from tolteca_db.models.orm.registry import DataProdRecord, RawObsRecord
from tolteca_db.models.orm.task import TaskRecord

__all__ = [
    "Base",
    # Core registry
    "RawObsRecord",
    "DataProdRecord",
    # Storage
    "StorageRootRecord",
    "FileRecord",
    # Associations
    "AssocRecord",
    "AssocEdgeRecord",
    # Flags
    "ObsFlagRecord",
    "DataProdFlagRecord",
    # Tasks
    "TaskRecord",
    # Events
    "EventRecord",
]

