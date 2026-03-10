"""Data product models: DataKind, DataProdType, DataProd, DataProdDataKind, DataProdSource.

Phase 2R: Correct 11-table schema. DataProdSource is co-located here
(not a separate source.py) because it has tight FK coupling to DataProd.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import DateTime, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from tolteca_db.constants import ReducedStatus, StorageRole
from tolteca_db.models.metadata import (
    AnyDataProdMeta,
    AnyInterfaceMeta,
    adaptix_json_type,
)
from tolteca_db.models.orm.base import Base
from tolteca_db.utils import (
    Created_at,
    Desc,
    Label,
    LabelKey,
    Pk,
    Updated_at,
    fk,
)

if TYPE_CHECKING:
    from tolteca_db.models.orm.flag import DataProdFlag
    from tolteca_db.models.orm.registry import Location


class DataKind(Base):
    """
    Registry of data kind classifications.

    Minimal design following tolteca_web pattern.

    Attributes
    ----------
    pk : int
        Integer primary key.
    label : str
        Kind label matching Python enum (VnaSweep, RawTimeStream, etc.).
    category : str
        Classification (shape, calibration, measurement, ancillary).
    description : str | None
        Human-readable description.
    data_prod : list[DataProdDataKind]
        Products with this kind assigned (relationship).
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

    Static metadata tied to each product type.

    Attributes
    ----------
    pk : int
        Integer primary key.
    label : str
        Type label (dp_raw_obs, dp_reduced_obs, dp_cal_group, etc.).
    description : str | None
        Human-readable description.
    level : int | None
        Processing level (0=raw, 1+=reduced).
    data_prod : list[DataProd]
        Products of this type (relationship).
    """

    __tablename__ = "data_prod_type"

    pk: Mapped[Pk]

    label: Mapped[LabelKey]

    description: Mapped[Desc | None]

    level: Mapped[int | None] = mapped_column()

    # Relationships
    data_prod: Mapped[list[DataProd]] = relationship(
        back_populates="data_prod_type",
        cascade="all, delete-orphan",
    )


class DataProd(Base):
    """
    Unified data product table (RAW and REDUCED).

    Minimal columns — static metadata (level) lives in data_prod_type.
    Dynamic instance data (name, obsnum, etc.) stored in meta JSON.
    Classification via DataKind relationship (not columns).

    Attributes
    ----------
    pk : int
        Integer primary key.
    data_prod_type_fk : int
        Foreign key to data_prod_type.
    lifecycle_status : str
        Lifecycle state (default "active").
    availability_state : str | None
        Physical availability (available, missing, remote, staged).
    content_hash : str | None
        Hash of file contents.
    meta : AnyDataProdMeta
        Typed metadata (JSON, AdaptixJSON union type).
    created_at : datetime
        Creation timestamp.
    updated_at : datetime
        Last update timestamp.
    data_prod_type : DataProdType
        Product type relationship.
    sources : list[DataProdSource]
        Source files for this product.
    flag : list[DataProdFlag]
        Quality flags assigned to this product.
    kind : list[DataProdDataKind]
        Data kind classifications.
    """

    __tablename__ = "data_prod"

    pk: Mapped[Pk]

    uid: Mapped[str | None] = mapped_column(String(256), unique=True, index=True, nullable=True)

    data_prod_type_fk: Mapped[int] = fk("data_prod_type", index=True)

    lifecycle_status: Mapped[str] = mapped_column(
        String(16),
        index=True,
        default=ReducedStatus.ACTIVE.value,
    )

    availability_state: Mapped[str | None] = mapped_column(String(16), index=True)

    content_hash: Mapped[str | None] = mapped_column(String(128), index=True)

    meta: Mapped[AnyDataProdMeta | None] = mapped_column(
        adaptix_json_type(AnyDataProdMeta),
        nullable=True,
    )

    created_at: Mapped[Created_at]
    updated_at: Mapped[Updated_at]

    # Relationships
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
    Assigned data kinds to products (junction table).

    Composite primary key (data_prod_fk, data_kind_fk).

    Attributes
    ----------
    data_prod_fk : int
        Foreign key to data_prod.
    data_kind_fk : int
        Foreign key to data_kind.
    applied_at : datetime
        When kind was applied.
    source : str
        How kind was determined ("automatic", "manual", "inferred").
    confidence : float | None
        Confidence score for automatic assignment.
    data_prod : DataProd
        Relationship to parent product.
    kind : DataKind
        Relationship to data kind.
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


class DataProdSource(Base):
    """
    Data product source locations (files, URLs, S3 objects, APIs).

    Aligns with tolteca_v2 SourceInfoModel concept. One row per interface
    file. source_uri is the primary key to uniquely identify each file.

    Attributes
    ----------
    source_uri : str
        Source URI with protocol (file://, s3://, https://) — PRIMARY KEY.
    data_prod_fk : int
        Foreign key to data_prod.
    location_fk : int
        Foreign key to location.
    role : str
        Storage role ("primary", "mirror", "temp").
    availability_state : str | None
        Per-location availability state.
    size : int | None
        File size in bytes.
    checksum : str | None
        Checksum for verification.
    last_verified_at : datetime | None
        Last successful verification timestamp.
    meta : AnyInterfaceMeta
        Interface-level metadata (RoachInterfaceMeta or TelInterfaceMeta).
    created_at : datetime
        Creation timestamp.
    updated_at : datetime
        Last update timestamp.
    data_prod : DataProd
        Relationship to parent product.
    location : Location
        Relationship to source location.
    """

    __tablename__ = "data_prod_source"

    source_uri: Mapped[str] = mapped_column(String(512), primary_key=True)

    data_prod_fk: Mapped[int] = fk("data_prod", index=True)

    location_fk: Mapped[int] = fk("location", index=True)

    role: Mapped[Label] = mapped_column(
        default=StorageRole.PRIMARY.value,
    )

    availability_state: Mapped[str | None] = mapped_column(
        String(32), index=True
    )

    size: Mapped[int | None] = mapped_column()

    checksum: Mapped[str | None] = mapped_column(String(128))

    last_verified_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), index=True
    )

    meta: Mapped[AnyInterfaceMeta] = mapped_column(
        adaptix_json_type(AnyInterfaceMeta),
    )

    created_at: Mapped[Created_at]

    updated_at: Mapped[Updated_at]

    # Relationships
    data_prod: Mapped[DataProd] = relationship(back_populates="sources")
    location: Mapped[Location] = relationship(back_populates="sources")
