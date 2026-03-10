"""Event ORM model: EventRecord.

Append-only audit log for all state-changing operations.
"""

from __future__ import annotations

from sqlalchemy import Index, String
from sqlalchemy.orm import Mapped, mapped_column

from tolteca_db.models.orm.base import Base
from tolteca_db.utils import Context, Created_at, Pk


class EventRecord(Base):
    """Append-only event log for audit trail.

    One row per state-changing operation.  Never updated, only inserted.

    Attributes
    ----------
    seq : int
        Autoincrement sequence number.
    event_type : str
        Event type label (e.g. ``obs_ingested``, ``zarr_written``,
        ``flag_added``, ``task_queued``).
    entity_type : str
        Entity type being described (e.g. ``raw_obs``, ``data_prod``,
        ``task``).
    entity_id : str
        Primary key or UID of the entity (e.g. ``tcs-98765-0-0``).
    payload : dict | None
        JSON payload with event details.
    occurred_at : datetime
        UTC timestamp of the event (database-generated).
    """

    __tablename__ = "event"

    seq: Mapped[Pk]
    event_type: Mapped[str] = mapped_column(String(64), index=True)
    entity_type: Mapped[str] = mapped_column(String(32), index=True)
    entity_id: Mapped[str] = mapped_column(String(128), index=True)
    payload: Mapped[Context]
    occurred_at: Mapped[Created_at]

    __table_args__ = (
        Index("ix_event_entity", "entity_type", "entity_id", "seq"),
    )

    def __repr__(self) -> str:
        return (
            f"EventRecord(seq={self.seq!r}, event_type={self.event_type!r}, "
            f"entity_id={self.entity_id!r})"
        )
