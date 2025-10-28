"""Source and location models: DataProdSource, Location.

Phase 3: Structured metadata with AdaptixJSON for type-safe JSON storage.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import DateTime, ForeignKey, Index, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from tolteca_db.constants import StorageRole
from tolteca_db.models.metadata import InterfaceFileMeta, _json_type, adaptix_json_type
from tolteca_db.models.orm.base import Base
from tolteca_db.utils import Created_at, Label, LabelKey, LongStr, Pk, Updated_at, fk, Name

if TYPE_CHECKING:
    from tolteca_db.models.orm.data_prod import DataProd


class DataProdSource(Base):
    """
    Data product source locations (files, URLs, S3 objects, APIs).
    
    Aligns with tolteca_v2 SourceInfoModel concept - abstract retrieval locations.
    Supports files, URLs, S3 objects, APIs. One row = one interface file.
    
    Uses source_uri as primary key to uniquely identify each file/source.
    Multiple interface files from the same observation can exist at the same location.
    
    Attributes
    ----------
    source_uri : str
        Source URI with protocol (file://, s3://, https://, etc.) - PRIMARY KEY
    data_prod_fk : int
        Foreign key to data_prod
    location_fk : int
        Foreign key to location
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
    meta : InterfaceFileMeta
        Interface-level metadata (nw_id, roach)
    created_at : datetime
        Creation timestamp
    updated_at : datetime
        Last update timestamp
    data_prod : DataProd
        Relationship to parent product
    location : Location
        Relationship to source location
    """

    __tablename__ = "data_prod_source"

    # Primary key - unique source URI
    source_uri: Mapped[str] = mapped_column(String(512), primary_key=True)

    # Foreign keys (indexed for queries)
    data_prod_fk: Mapped[int] = fk("data_prod", index=True)
    location_fk: Mapped[int] = fk("location", index=True)

    role: Mapped[Label] = mapped_column(
        default=StorageRole.PRIMARY.value,
    )

    # Structured metadata for interface files (Phase 3 - AdaptixJSON for type-safe JSON)
    # AdaptixJSON provides automatic serialization for InterfaceFileMeta dataclass
    # Example: source.meta = InterfaceFileMeta(interface="toltec", ...)
    # Type-safe metadata access without manual conversion
    meta: Mapped[InterfaceFileMeta] = mapped_column(
        adaptix_json_type(InterfaceFileMeta)
    )

    availability_state: Mapped[Name] = mapped_column(index=True)

    size: Mapped[int | None] = mapped_column()

    checksum: Mapped[str | None] = mapped_column(String(128))

    last_verified_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), index=True
    )

    created_at: Mapped[Created_at]

    updated_at: Mapped[Updated_at]

    # Relationships
    data_prod: Mapped[DataProd] = relationship(back_populates="sources")
    location: Mapped[Location] = relationship(back_populates="sources")


class Location(Base):
    """
    Data root registry - aligns with tolteca_v2 FileStore pattern.
    
    Represents abstract data roots: filesystem directories, S3 buckets,
    HTTP endpoints, API services. Maps to ToltecFileStore/LmtFileStore instances.
    
    Attributes
    ----------
    pk : str
        Unique location identifier
    label : str
        Human-readable label
    location_type : str
        Type of location (filesystem, s3, http, api)
    root_uri : str
        Root URI for this location (file://, s3://, https://)
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

    pk: Mapped[Pk]

    label: Mapped[LabelKey]

    location_type: Mapped[Label]

    root_uri: Mapped[LongStr]

    priority: Mapped[int] = mapped_column(default=100)

    meta: Mapped[dict[str, Any] | None] = mapped_column(_json_type)

    created_at: Mapped[Created_at]

    updated_at: Mapped[Updated_at]

    # Relationships
    sources: Mapped[list[DataProdSource]] = relationship(
        back_populates="location",
        cascade="all, delete-orphan",
    )
