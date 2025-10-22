"""SQLAlchemy 2.0 ORM models for tolteca_db.

This module defines the database schema using SQLAlchemy 2.0 with Mapped[]
type hints for Python 3.13 compatibility.

Architecture:
- Unified DataProduct table for RAW and REDUCED products (product_kind discriminator)
- Content-addressable IDs using blake3 hashing
- Multi-location storage support (composite keys)
- Quality flag system with severity levels
- Idempotent reduction tasks
- Append-only event log for audit trail

Note: Uses pure SQLAlchemy 2.0 ORM (not SQLModel) due to Python 3.13 + SQLAlchemy 2.0
      compatibility requirements. SQLModel 0.0.27 does not support Mapped[] type hints.

Migration: Converted from SQLModel to SQLAlchemy 2.0 on 2025-10-21
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, ForeignKey, Index, String, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.types import JSON

from tolteca_db.constants import (
    ProductKind,
    ReducedStatus,
    StorageRole,
)
from tolteca_db.utils import utcnow

__all__ = [
    "Base",
    "DataProduct",
    "DataProductAssoc",
    "DataProductFlag",
    "DataProductStorage",
    "EventLog",
    "FlagDefinition",
    "Location",
    "ReductionTask",
    "TaskInput",
    "TaskOutput",
]


class Base(DeclarativeBase):
    """Base class for all ORM models."""



class DataProduct(Base):
    """
    Unified data product table (RAW and REDUCED).
    
    The product_kind field discriminates between RAW and REDUCED semantics:
    - RAW: Physical telescope observations with availability_state
    - REDUCED: Derived data products with status (ACTIVE/SUPERSEDED)
    
    Attributes
    ----------
    product_pk : str
        Stable content-addressable ID (blake3 hash of canonical identity)
    base_type : str
        Product type (raw_obs, reduced_obs, input_set)
    subtype : str | None
        Optional classification/subtype
    name : str
        Unique name within base_type
    level : int | None
        Processing level (0=raw, 1+=reduced)
    product_kind : str
        RAW or REDUCED discriminator
    status : str
        Lifecycle status (ACTIVE, SUPERSEDED for REDUCED)
    availability_state : str | None
        Physical availability (AVAILABLE, MISSING, REMOTE, STAGED for RAW)
    content_hash : str | None
        Hash of file contents (blake3: or sha256: prefixed)
    meta : dict
        Flexible metadata (JSON)
    created_at : datetime
        Creation timestamp (UTC, timezone-aware)
    updated_at : datetime
        Last update timestamp (UTC, timezone-aware)
    storage_locations : list[DataProductStorage]
        Storage locations for this product (relationship)
    flags : list[DataProductFlag]
        Quality flags assigned to this product (relationship)
    """

    __tablename__ = "data_product"

    # Primary key
    product_pk: Mapped[str] = mapped_column(String(128), primary_key=True)

    # Indexed columns (use DataProdType enum for TolTEC products)
    base_type: Mapped[str] = mapped_column(String(32), index=True)
    subtype: Mapped[str | None] = mapped_column(String(32), index=True)
    name: Mapped[str] = mapped_column(String(256), index=True)
    level: Mapped[int | None] = mapped_column()

    # Discriminator and status
    product_kind: Mapped[str] = mapped_column(
        String(16),
        index=True,
        default=ProductKind.REDUCED.value,
    )
    status: Mapped[str] = mapped_column(
        String(16),
        index=True,
        default=ReducedStatus.ACTIVE.value,
    )
    availability_state: Mapped[str | None] = mapped_column(String(16), index=True)

    # Content addressing
    content_hash: Mapped[str | None] = mapped_column(String(128), index=True)

    # Flexible metadata
    meta: Mapped[dict[str, Any]] = mapped_column(
        JSON,
        server_default="{}",
        nullable=False,
    )

    # Timestamps with timezone awareness and database-generated defaults
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=utcnow(),
        index=True,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=utcnow(),
        server_onupdate=utcnow(),
        index=True,
    )

    # Relationships with Mapped[] type hints (Python 3.13 + SQLAlchemy 2.0 compatible)
    storage_locations: Mapped[list[DataProductStorage]] = relationship(
        back_populates="product",
        cascade="all, delete-orphan",
    )

    flags: Mapped[list[DataProductFlag]] = relationship(
        back_populates="product",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        UniqueConstraint("base_type", "name", name="uq_product_type_name"),
        Index("ix_product_kind_status", "product_kind", "status"),
        # Composite index for RAW product availability queries
        Index("ix_base_type_availability", "base_type", "availability_state"),
    )


class DataProductStorage(Base):
    """
    Multi-location storage records for products.
    
    Enables tracking replicas across sites (LMT, UMass, etc.) with
    composite primary key (product_fk, location_fk).
    
    Attributes
    ----------
    product_fk : str
        Foreign key to data_product
    location_fk : str
        Foreign key to location
    storage_key : str
        Storage path/URI (file://, s3://, etc.)
    role : str
        PRIMARY, MIRROR, or TEMP
    availability_state : str | None
        Per-location availability state
    size : int | None
        File size in bytes
    checksum : str | None
        Checksum for verification
    last_verified_at : datetime | None
        Last successful verification
    created_at : datetime
        Creation timestamp
    updated_at : datetime
        Last update timestamp
    product : DataProduct
        Relationship to parent product
    location : Location
        Relationship to storage location
    """

    __tablename__ = "data_product_storage"

    # Composite primary key
    product_fk: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("data_product.product_pk"),
        primary_key=True,
    )
    location_fk: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("location.location_pk"),
        primary_key=True,
    )

    # Columns
    storage_key: Mapped[str] = mapped_column(String(512))

    role: Mapped[str] = mapped_column(
        String(16),
        default=StorageRole.PRIMARY.value,
        index=True,
    )

    availability_state: Mapped[str | None] = mapped_column(String(16), index=True)

    size: Mapped[int | None] = mapped_column()

    checksum: Mapped[str | None] = mapped_column(String(128))

    last_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=utcnow(),
        index=True,
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=utcnow(),
        server_onupdate=utcnow(),
        index=True,
    )

    # Relationships
    product: Mapped[DataProduct] = relationship(back_populates="storage_locations")
    location: Mapped[Location] = relationship(back_populates="storage_records")

    __table_args__ = (
        Index("ix_storage_location_role", "location_fk", "role"),
    )


class Location(Base):
    """
    Storage location registry (sites, servers).
    
    Attributes
    ----------
    location_pk : str
        Unique location identifier
    label : str
        Human-readable label
    site_code : str
        Site code (LMT, UMass, etc.)
    priority : int
        Priority for selection (lower = higher priority)
    meta : dict
        Additional metadata
    created_at : datetime
        Creation timestamp
    updated_at : datetime
        Last update timestamp
    """

    __tablename__ = "location"

    location_pk: Mapped[str] = mapped_column(String(64), primary_key=True)

    label: Mapped[str] = mapped_column(String(128), unique=True, index=True)

    site_code: Mapped[str] = mapped_column(String(32), index=True)

    priority: Mapped[int] = mapped_column(default=100)

    meta: Mapped[dict[str, Any] | None] = mapped_column(JSON)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=utcnow(),
        index=True,
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=utcnow(),
        server_onupdate=utcnow(),
        index=True,
    )

    # Relationships
    storage_records: Mapped[list[DataProductStorage]] = relationship(
        back_populates="location",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("ix_location_site_priority", "site_code", "priority"),
    )


class FlagDefinition(Base):
    """
    Registry of quality flags.
    
    Attributes
    ----------
    flag_key : str
        Unique flag identifier (e.g., DET_DEAD_PIXEL)
    group_key : str
        Flag group (DET, TEL, QA, CAL, ING)
    severity : str
        INFO, WARN, BLOCK, CRITICAL
    description : str | None
        Human-readable description
    active : bool
        Whether flag is currently active
    created_at : datetime
        Creation timestamp
    updated_at : datetime
        Last update timestamp
    """

    __tablename__ = "flag_definition"

    flag_key: Mapped[str] = mapped_column(String(64), primary_key=True)

    group_key: Mapped[str] = mapped_column(String(32), index=True)

    severity: Mapped[str] = mapped_column(String(16), index=True)

    description: Mapped[str | None] = mapped_column(String)

    active: Mapped[bool] = mapped_column(
        String,  # SQLite boolean as string
        default=True,
        index=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=utcnow(),
        index=True,
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=utcnow(),
        server_onupdate=utcnow(),
        index=True,
    )

    # Relationships
    assigned_flags: Mapped[list[DataProductFlag]] = relationship(
        back_populates="flag_definition",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("ix_flag_group_active", "group_key", "active"),
    )


class DataProductFlag(Base):
    """
    Assigned flags to products.
    
    Composite primary key (product_fk, flag_key).
    
    Attributes
    ----------
    product_fk : str
        Foreign key to data_product
    flag_key : str
        Foreign key to flag_definition
    asserted_at : datetime
        When flag was asserted
    asserted_by : str
        Who/what asserted the flag
    details : dict | None
        Additional details (JSON)
    """

    __tablename__ = "data_product_flag"

    product_fk: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("data_product.product_pk"),
        primary_key=True,
    )

    flag_key: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("flag_definition.flag_key"),
        primary_key=True,
    )

    asserted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=utcnow(),
        index=True,
    )

    asserted_by: Mapped[str] = mapped_column(
        String(64),
        default="system",
        index=True,
    )

    details: Mapped[dict[str, Any] | None] = mapped_column(JSON)

    # Relationships
    product: Mapped[DataProduct] = relationship(back_populates="flags")
    flag_definition: Mapped[FlagDefinition] = relationship(back_populates="assigned_flags")

    __table_args__ = (
        Index("ix_flag_product_flagkey", "flag_key", "product_fk"),
    )


class DataProductAssoc(Base):
    """
    Provenance association edges.
    
    Directed edges representing relationships between products (process_edge,
    group_member, etc.).
    
    Attributes
    ----------
    assoc_pk : int
        Autoincrement primary key
    assoc_type : str
        Edge type (process_edge, group_member)
    src_product_fk : str
        Source product
    dst_product_fk : str
        Destination product
    process_module : str | None
        Processing module name
    process_version : str | None
        Processing version
    process_config : dict | None
        Processing configuration
    context : dict | None
        Additional context
    created_at : datetime
        Creation timestamp
    """

    __tablename__ = "data_product_assoc"

    assoc_pk: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Use DataProdAssocType enum for TolTEC associations
    assoc_type: Mapped[str] = mapped_column(String(32), index=True)

    src_product_fk: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("data_product.product_pk"),
        index=True,
    )

    dst_product_fk: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("data_product.product_pk"),
        index=True,
    )

    process_module: Mapped[str | None] = mapped_column(String(128))

    process_version: Mapped[str | None] = mapped_column(String(64))

    process_config: Mapped[dict[str, Any] | None] = mapped_column(JSON)

    context: Mapped[dict[str, Any] | None] = mapped_column(JSON)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=utcnow(),
        index=True,
    )

    __table_args__ = (
        UniqueConstraint("assoc_type", "src_product_fk", "dst_product_fk", name="uq_assoc_edge"),
        Index("ix_assoc_src_type", "assoc_type", "src_product_fk"),
        Index("ix_assoc_dst_type", "assoc_type", "dst_product_fk"),
    )


class ReductionTask(Base):
    """
    Idempotent reduction task tracking.
    
    Deduplication via (params_hash, input_set_hash) composite.
    
    Attributes
    ----------
    task_pk : str
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

    task_pk: Mapped[str] = mapped_column(String(64), primary_key=True)

    status: Mapped[str] = mapped_column(
        String(16),
        default="QUEUED",
        index=True,
    )

    params_hash: Mapped[str] = mapped_column(String(64), index=True)

    params: Mapped[dict[str, Any]] = mapped_column(JSON)

    input_set_hash: Mapped[str] = mapped_column(String(64), index=True)

    worker_host: Mapped[str | None] = mapped_column(String(128))

    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    error_message: Mapped[str | None] = mapped_column(String)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=utcnow(),
        index=True,
    )

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
    """Task input products.
    
    Composite primary key (task_fk, product_fk).
    """

    __tablename__ = "task_input"

    task_fk: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("reduction_task.task_pk"),
        primary_key=True,
    )

    product_fk: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("data_product.product_pk"),
        primary_key=True,
    )

    role: Mapped[str | None] = mapped_column(String(32))

    # Relationships
    task: Mapped[ReductionTask] = relationship(back_populates="inputs")


class TaskOutput(Base):
    """Task output products.
    
    Composite primary key (task_fk, product_fk).
    """

    __tablename__ = "task_output"

    task_fk: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("reduction_task.task_pk"),
        primary_key=True,
    )

    product_fk: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("data_product.product_pk"),
        primary_key=True,
    )

    # Relationships
    task: Mapped[ReductionTask] = relationship(back_populates="outputs")


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

    seq: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    event_type: Mapped[str] = mapped_column(String(32), index=True)

    entity_type: Mapped[str] = mapped_column(String(32), index=True)

    entity_id: Mapped[str] = mapped_column(String(128), index=True)

    payload: Mapped[dict[str, Any] | None] = mapped_column(JSON)

    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=utcnow(),
        index=True,
    )

    __table_args__ = (
        Index("ix_event_entity", "entity_type", "entity_id", "seq"),
    )
