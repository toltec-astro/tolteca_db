"""Data root registry model: Location.

Represents abstract storage roots (filesystem dirs, S3 buckets, etc.)
that anchor DataProdSource URIs.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy.orm import Mapped, mapped_column, relationship

from tolteca_db.models.metadata import _json_type
from tolteca_db.models.orm.base import Base
from tolteca_db.utils import Created_at, Label, LabelKey, LongStr, Pk, Updated_at

if TYPE_CHECKING:
    from tolteca_db.models.orm.data_prod import DataProdSource


class Location(Base):
    """
    Data root registry — aligns with tolteca_v2 FileStore pattern.

    Represents abstract data roots: filesystem directories, S3 buckets,
    HTTP endpoints, API services. Maps to ToltecFileStore/LmtFileStore
    instances. Each DataProdSource.source_uri is anchored at one Location.

    Attributes
    ----------
    pk : int
        Integer primary key.
    label : str
        Human-readable unique label (e.g. "site_data", "local", "archive").
    location_type : str
        Type of location ("filesystem", "s3", "http", "api").
    root_uri : str
        Root URI for this location (e.g. "file:///data_lmt/").
    priority : int
        Priority for selection (lower = higher priority), default 100.
    meta : dict | None
        Additional metadata (JSON).
    created_at : datetime
        Creation timestamp.
    updated_at : datetime
        Last update timestamp.
    sources : list[DataProdSource]
        Source files anchored at this location.
    """

    __tablename__ = "location"

    pk: Mapped[Pk]

    label: Mapped[LabelKey]

    location_type: Mapped[Label | None] = mapped_column(nullable=True)

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
