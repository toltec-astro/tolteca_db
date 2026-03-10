"""Task ORM model: TaskRecord.

Tracks reduction tasks triggered by association groups.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from tolteca_db.models.orm.base import Base
from tolteca_db.utils import Context, Created_at, Pk, Updated_at

if TYPE_CHECKING:
    from tolteca_db.models.orm.assoc import AssocRecord


class TaskRecord(Base):
    """Reduction task triggered by an association group.

    Attributes
    ----------
    pk : int
        Integer primary key.
    uid : str
        Unique task identifier.
    assoc_fk : int | None
        Optional foreign key to :class:`~tolteca_db.models.orm.assoc.AssocRecord`
        that triggered this task.
    task_type : str
        Task type label (e.g. ``zarr_convert``, ``reduce_sweep``).
    status : str
        Task status (``queued``, ``running``, ``done``, ``error``).
    started_at : datetime | None
        UTC timestamp when task started.
    completed_at : datetime | None
        UTC timestamp when task completed.
    error_msg : str | None
        Error message if task failed.
    meta : dict | None
        Additional JSON metadata (parameters, worker info, etc.).
    created_at : datetime
        Database insert timestamp.
    updated_at : datetime
        Last update timestamp.
    assoc : AssocRecord | None
        Parent association group.
    """

    __tablename__ = "task"

    pk: Mapped[Pk]
    uid: Mapped[str] = mapped_column(String(256), unique=True, index=True)
    assoc_fk: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("assoc.pk"), nullable=True, index=True
    )
    task_type: Mapped[str] = mapped_column(String(64), index=True)
    status: Mapped[str] = mapped_column(String(16), index=True, default="queued")
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    error_msg: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    meta: Mapped[Context]
    created_at: Mapped[Created_at]
    updated_at: Mapped[Updated_at]

    # Relationships
    assoc: Mapped[AssocRecord | None] = relationship(back_populates="tasks")

    def __repr__(self) -> str:
        return f"TaskRecord(uid={self.uid!r}, status={self.status!r})"
