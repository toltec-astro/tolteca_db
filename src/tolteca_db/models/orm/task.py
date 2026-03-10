"""Task orchestration models: ReductionTask, TaskInput, TaskOutput."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, ForeignKey, Index, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from tolteca_db.models.metadata import _json_type
from tolteca_db.models.orm.base import Base
from tolteca_db.utils import Created_at, Pk, fk, Label


class ReductionTask(Base):
    """
    Idempotent reduction task tracking.
    
    Deduplication via (params_hash, input_set_hash) composite.
    
    Attributes
    ----------
    pk : str
        Unique task identifier
    status : str
        QUEUED, RUNNING, DONE, ERROR
    params_hash : str
        Hash of reduction parameters
    params : dict
        Full reduction parameters
    input_set_hash : str
        Hash of input product IDs
    worker_host : str | None
        Worker host name
    started_at : datetime | None
        Task start time
    finished_at : datetime | None
        Task finish time
    error_message : str | None
        Error message if failed
    created_at : datetime
        Creation timestamp
    """

    __tablename__ = "reduction_task"

    pk: Mapped[Pk]

    status: Mapped[str] = mapped_column(
        String(16),
        default="QUEUED",
        index=True,
    )

    params_hash: Mapped[str] = mapped_column(String(64), index=True)

    params: Mapped[dict[str, Any]] = mapped_column(_json_type)

    input_set_hash: Mapped[str] = mapped_column(String(64), index=True)

    worker_host: Mapped[str | None] = mapped_column(String(128))

    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    error_message: Mapped[str | None] = mapped_column(String)

    created_at: Mapped[Created_at]

    # Relationships
    inputs: Mapped[list[TaskInput]] = relationship(
        back_populates="task",
        cascade="all, delete-orphan",
    )

    outputs: Mapped[list[TaskOutput]] = relationship(
        back_populates="task",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("ix_task_status_inputs", "status", "input_set_hash"),
        Index("ix_task_params", "params_hash"),
    )


class TaskInput(Base):
    """
    Task input products.
    
    Composite primary key (task_fk, data_prod_fk).
    
    Attributes
    ----------
    task_fk : str
        Foreign key to reduction_task
    data_prod_fk : str
        Foreign key to data_prod
    role : str | None
        Input role (optional)
    task : ReductionTask
        Relationship to parent task
    """

    __tablename__ = "task_input"

    task_fk: Mapped[int] = fk("reduction_task", primary_key=True)

    data_prod_fk: Mapped[int] = fk("data_prod", primary_key=True)

    role: Mapped[Label]

    # Relationships
    task: Mapped[ReductionTask] = relationship(back_populates="inputs")


class TaskOutput(Base):
    """
    Task output products.
    
    Composite primary key (task_fk, data_prod_fk).
    
    Attributes
    ----------
    task_fk : str
        Foreign key to reduction_task
    data_prod_fk : str
        Foreign key to data_prod
    task : ReductionTask
        Relationship to parent task
    """

    __tablename__ = "task_output"

    task_fk: Mapped[int] = fk("reduction_task", primary_key=True)

    data_prod_fk: Mapped[int] = fk("data_prod", primary_key=True)

    # Relationships
    task: Mapped[ReductionTask] = relationship(back_populates="outputs")
