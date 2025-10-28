"""Data product models: DataProd, DataKind, DataProdDataKind.

Phase 3: Structured metadata with AdaptixJSON for type-safe JSON storage.
Union types supported via Literal discriminator fields.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import DateTime, ForeignKey, Index, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from tolteca_db.constants import ReducedStatus
from tolteca_db.models.metadata import AnyDataProdMeta, adaptix_json_type
from tolteca_db.models.orm.base import Base
from tolteca_db.utils import Created_at, Desc, LabelKey, Pk, Updated_at, fk

if TYPE_CHECKING:
    from tolteca_db.models.orm.flag import DataProdFlag
    from tolteca_db.models.orm.source import DataProdSource


class DataKind(Base):
    """
    Registry of data kind classifications.
    
    Minimal design following tolteca_web pattern (ics/tcs tables).
    
    Attributes
    ----------
    pk : int
        Integer primary key
    label : str
        Kind name matching Python enum (Raw, TimeOrderedData, Image)
    category : str
        Classification (shape, calibration, measurement, ancillary)
    description : str | None
        Human-readable description
    assigned_kinds : list[DataProdDataKind]
        Products with this kind assigned (relationship)
    """

    __tablename__ = "data_kind"

    pk: Mapped[Pk]

    label: Mapped[LabelKey]

    category: Mapped[str] = mapped_column(String(32), index=True)

    description: Mapped[Desc | None]

    # Relationships
    data_prod: Mapped[list[DataProdDataKind]] = relationship(
        back_populates="kind",
        cascade="all, delete-orphan",
    )


class DataProdType(Base):
    """
    Registry of data product types.
    
    Minimal design following tolteca_web pattern (ics/tcs tables).
    Static metadata tied to each product type.
    
    Attributes
    ----------
    pk : int
        Integer primary key
    label : str
        Type label (dp_raw_obs, dp_reduced_obs, dp_cal_group, etc.)
    description : str | None
        Human-readable description
    level : int | None
        Processing level for this type (0=raw, 1+=reduced)
    data_prod : list[DataProd]
        Products of this type (relationship)
    """

    __tablename__ = "data_prod_type"

    pk: Mapped[Pk]

    label: Mapped[LabelKey]

    description: Mapped[Desc | None]

    # Static type metadata
    level: Mapped[int | None] = mapped_column()

    # Relationships
    data_prod: Mapped[list[DataProd]] = relationship(
        back_populates="data_prod_type",
        cascade="all, delete-orphan",
    )


class DataProd(Base):
    """
    Unified data product table (RAW and REDUCED).
    
    Note: Renamed from DataProduct to DataProd to align with reference
    implementations (tolteca_v1, tolteca_v2).
    
    Minimal columns - static metadata (level) moved to data_prod_type table.
    Dynamic instance data (name, etc.) stored in meta JSON.
    Classification via DataKind relationship (not columns).
    
    Attributes
    ----------
    pk : str
        Stable content-addressable ID (blake3 hash of canonical identity)
    data_prod_type_fk : int
        Foreign key to data_prod_type
    data_prod_type : DataProdType
        Product type relationship (access .label, .level)
    lifecycle_status : str
        Lifecycle state (ACTIVE, SUPERSEDED for REDUCED products)
    availability_state : str | None
        Physical availability (AVAILABLE, MISSING, REMOTE, STAGED for RAW)
    content_hash : str | None
        Hash of file contents (blake3: or sha256: prefixed)
    meta : dict
        Flexible metadata (JSON) - includes name and other dynamic attributes
    created_at : datetime
        Creation timestamp (UTC, timezone-aware)
    updated_at : datetime
        Last update timestamp (UTC, timezone-aware)
    sources : list[DataProdSource]
        Source locations for this product (interface files, URLs, etc.)
    flag : list[DataProdFlag]
        Quality flags assigned to this product (relationship)
    kind : list[DataProdDataKind]
        Data kind classifications for this product (relationship)
    """

    __tablename__ = "data_prod"

    # Primary key
    pk: Mapped[Pk]

    # Foreign key to data_prod_type
    data_prod_type_fk: Mapped[int] = fk("data_prod_type", index=True)

    # Lifecycle and availability state
    lifecycle_status: Mapped[str] = mapped_column(
        String(16),
        index=True,
        default=ReducedStatus.ACTIVE.value,
    )
    availability_state: Mapped[str | None] = mapped_column(String(16), index=True)

    # Content addressing
    content_hash: Mapped[str | None] = mapped_column(String(128), index=True)

    # Structured metadata (Phase 3 - AdaptixJSON with union types!)
    # AdaptixJSON DOES support union types using Literal discriminator fields!
    # Each metadata dataclass has a 'tag' field with a unique Literal value.
    # Automatic serialization/deserialization with type safety.
    # Example: product.meta = RawObsMeta(tag="raw_obs", obsnum=123, ...)
    # Retrieved as proper type: assert isinstance(product.meta, RawObsMeta)
    meta: Mapped[AnyDataProdMeta] = mapped_column(
        adaptix_json_type(AnyDataProdMeta),
        nullable=False,
    )

    # Timestamps with timezone awareness and database-generated defaults
    created_at: Mapped[Created_at]
    updated_at: Mapped[Updated_at]

    # Relationships - use singular target table name
    data_prod_type: Mapped[DataProdType] = relationship(
        back_populates="data_prod",
    )

    sources: Mapped[list[DataProdSource]] = relationship(
        back_populates="data_prod",
        cascade="all, delete-orphan",
    )

    flag: Mapped[list[DataProdFlag]] = relationship(
        back_populates="data_prod",
        cascade="all, delete-orphan",
    )

    kind: Mapped[list[DataProdDataKind]] = relationship(
        back_populates="data_prod",
        cascade="all, delete-orphan",
    )


class DataProdDataKind(Base):
    """
    Assigned data kinds to products.
    
    Composite primary key (data_prod_fk, data_kind_fk).
    
    Attributes
    ----------
    data_prod_fk : str
        Foreign key to data_prod
    data_kind_fk : int
        Foreign key to data_kind
    applied_at : datetime
        When kind was applied
    source : str
        How kind was determined (automatic/manual/inferred)
    confidence : float | None
        Confidence score for automatic assignment
    """

    __tablename__ = "data_prod_data_kind"

    data_prod_fk: Mapped[int] = fk("data_prod", primary_key=True)

    data_kind_fk: Mapped[int] = fk("data_kind", primary_key=True)

    applied_at: Mapped[Created_at]

    source: Mapped[str] = mapped_column(
        String(16),
        default="automatic",
        index=True,
    )

    confidence: Mapped[float | None] = mapped_column()

    # Relationships
    data_prod: Mapped[DataProd] = relationship(back_populates="kind")
    kind: Mapped[DataKind] = relationship(back_populates="data_prod")
