"""Event log model: EventLog."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from tolteca_db.models.metadata import _json_type
from tolteca_db.models.orm.base import Base
from tolteca_db.utils import Created_at, Pk


class EventLog(Base):
    """
    Append-only event log for audit trail.
    
    All state transitions emit events here for replay, debugging, and compliance.
    
    Attributes
    ----------
    seq : int
        Autoincrement sequence number
    event_type : str
        Event type (FlagAdded, FileMissing, etc.)
    entity_type : str
        Entity type (product, task, etc.)
    entity_id : str
        Entity identifier
    payload : dict | None
        Event payload (JSON)
    occurred_at : datetime
        Event timestamp
    """

    __tablename__ = "event_log"

    seq: Mapped[Pk]

    event_type: Mapped[str] = mapped_column(String(64), index=True)

    entity_type: Mapped[str] = mapped_column(String(32), index=True)

    entity_id: Mapped[str] = mapped_column(String(128), index=True)

    payload: Mapped[dict[str, Any] | None] = mapped_column(_json_type)

    occurred_at: Mapped[Created_at]

    __table_args__ = (Index("ix_event_entity", "entity_type", "entity_id", "seq"),)
