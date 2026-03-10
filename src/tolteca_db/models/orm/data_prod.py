"""Storage ORM models: StorageRootRecord, FileRecord.

StorageRootRecord — registry of data roots (filesystems, S3, etc.).
FileRecord — physical file tracking for health monitoring and checksumming.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Boolean, ForeignKey, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from tolteca_db.models.orm.base import Base
from tolteca_db.utils import Context, Created_at, LabelKey, Pk, Updated_at

if TYPE_CHECKING:
    from tolteca_db.models.orm.registry import DataProdRecord


class StorageRootRecord(Base):
    """Registry of data roots — filesystems, S3 buckets, remote servers.

    One row per distinct data root. The ``root_path`` is an absolute path
    or URI prefix (e.g. ``/data/toltec``, ``s3://toltec-archive``).

    Attributes
    ----------
    pk : int
        Integer primary key.
    label : str
        Unique human-readable label (e.g. ``site_data``, ``server_archive``).
    host : str | None
        Hostname for remote roots; ``None`` for local roots.
    root_path : str
        Absolute path or URI prefix for this storage root.
    is_local : bool
        ``True`` if the root is directly accessible on the local filesystem.
    meta : dict | None
        Additional JSON metadata.
    created_at : datetime
        Database insert timestamp.
    file_records : list[FileRecord]
        Physical files under this storage root.
    """

    __tablename__ = "storage_root"

    pk: Mapped[Pk]
    label: Mapped[LabelKey]
    host: Mapped[str | None] = mapped_column(String(256), nullable=True)
    root_path: Mapped[str] = mapped_column(String(512))
    is_local: Mapped[bool] = mapped_column(Boolean, default=True)
    meta: Mapped[Context]
    created_at: Mapped[Created_at]

    # Relationships
    file_records: Mapped[list[FileRecord]] = relationship(
        back_populates="storage_root",
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        return f"StorageRootRecord(label={self.label!r}, root_path={self.root_path!r})"


class FileRecord(Base):
    """Physical file tracking — one row per file, for health monitoring.

    Tracks mtime, size, and checksum of each physical file associated with a
    DataProdRecord.  Used by the file scanner to detect changes and verify integrity.

    Attributes
    ----------
    pk : int
        Integer primary key.
    data_prod_fk : int
        Foreign key to DataProdRecord.
    storage_root_fk : int | None
        Foreign key to StorageRootRecord; ``None`` if root is unknown.
    rel_path : str
        Path relative to the storage root (or absolute if root is ``None``).
    mtime : float | None
        POSIX modification timestamp of the file.
    file_size : int | None
        File size in bytes.
    checksum : str | None
        Content checksum, prefixed with algorithm (e.g. ``blake3:abcd...``).
    created_at : datetime
        Database insert timestamp.
    updated_at : datetime
        Last update timestamp.
    data_prod : DataProdRecord
        Parent data product.
    storage_root : StorageRootRecord | None
        Parent storage root.
    """

    __tablename__ = "file_record"

    pk: Mapped[Pk]
    data_prod_fk: Mapped[int] = mapped_column(
        Integer, ForeignKey("data_prod.pk"), index=True
    )
    storage_root_fk: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("storage_root.pk"), nullable=True, index=True
    )
    rel_path: Mapped[str] = mapped_column(String(512))
    mtime: Mapped[float | None] = mapped_column(nullable=True)
    file_size: Mapped[int | None] = mapped_column(Integer, nullable=True)
    checksum: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[Created_at]
    updated_at: Mapped[Updated_at]

    # Relationships
    data_prod: Mapped[DataProdRecord] = relationship(back_populates="file_records")
    storage_root: Mapped[StorageRootRecord | None] = relationship(
        back_populates="file_records"
    )

    __table_args__ = (
        Index("ix_file_record_data_prod_root", "data_prod_fk", "storage_root_fk"),
    )

    def __repr__(self) -> str:
        return f"FileRecord(pk={self.pk!r}, rel_path={self.rel_path!r})"
