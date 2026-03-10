"""SQLAlchemy 2.x ORM models for tolteca_db v3.x.

Module layout
-------------
- base.py       — ``Base`` (DeclarativeBase)
- registry.py   — ``Location``
- data_prod.py  — ``DataKind``, ``DataProdType``, ``DataProd``,
                  ``DataProdDataKind``, ``DataProdSource``
- assoc.py      — ``DataProdAssocType``, ``DataProdAssoc``
- flag.py       — ``Flag``, ``DataProdFlag``
- event.py      — ``EventLog``

Import this package to register all mappers with ``Base.metadata``.
"""

from __future__ import annotations

from tolteca_db.models.orm.assoc import DataProdAssoc, DataProdAssocType
from tolteca_db.models.orm.base import Base
from tolteca_db.models.orm.data_prod import (
    DataKind,
    DataProd,
    DataProdDataKind,
    DataProdSource,
    DataProdType,
)
from tolteca_db.models.orm.event import EventLog
from tolteca_db.models.orm.flag import DataProdFlag, Flag
from tolteca_db.models.orm.registry import Location

__all__ = [
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
]
