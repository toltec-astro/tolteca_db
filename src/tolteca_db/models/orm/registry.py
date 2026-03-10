"""Core registry ORM models: RawObsRecord, DataProdRecord.

RawObsRecord — one row per raw observation, keyed by ObsSpec UID.
DataProdRecord — one row per logical data product (e.g. one roach sweep file),
    with all catalog-query columns inline for fast reads.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import DateTime, Float, ForeignKey, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import JSON

from tolteca_db.models.orm.base import Base
from tolteca_db.utils import Context, Created_at, Pk, Updated_at

if TYPE_CHECKING:
    from tolteca_db.models.orm.assoc import AssocEdgeRecord
    from tolteca_db.models.orm.flag import DataProdFlagRecord, ObsFlagRecord
    from tolteca_db.models.orm.data_prod import FileRecord


class RawObsRecord(Base):
    """One row per raw TolTEC observation, keyed by ObsSpec UID.

    The UID is the canonical ``{master}-{obsnum}-{subobsnum}-{scannum}`` string
    produced by :class:`~tolteca_db.obsspec.ObsSpec`.

    Attributes
    ----------
    uid : str
        Primary key — ObsSpec string, e.g. ``"tcs-98765-0-0"``.
    master : str
        Master type (``tcs``, ``ics``, ``clip``).
    obsnum : int
        Observation number.
    subobsnum : int
        Sub-observation number (default 0).
    scannum : int
        Scan number (default 0).
    timestamp_obs : datetime | None
        UTC timestamp of observation start.
    meta : dict | None
        Additional JSON metadata.
    created_at : datetime
        Database insert timestamp.
    data_prods : list[DataProdRecord]
        Relationship to logical data products for this observation.
    obs_flags : list[ObsFlagRecord]
        Quality flags on this observation.
    """

    __tablename__ = "raw_obs"

    uid: Mapped[str] = mapped_column(String(128), primary_key=True)
    master: Mapped[str] = mapped_column(String(32), index=True)
    obsnum: Mapped[int] = mapped_column(Integer, index=True)
    subobsnum: Mapped[int] = mapped_column(Integer)
    scannum: Mapped[int] = mapped_column(Integer)
    timestamp_obs: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )
    meta: Mapped[Context]
    created_at: Mapped[Created_at]

    # Relationships
    data_prods: Mapped[list[DataProdRecord]] = relationship(
        back_populates="raw_obs",
        cascade="all, delete-orphan",
    )
    obs_flags: Mapped[list[ObsFlagRecord]] = relationship(
        back_populates="raw_obs",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("ix_raw_obs_obsnum_master", "obsnum", "master"),
    )

    def __repr__(self) -> str:
        return f"RawObsRecord(uid={self.uid!r})"


class DataProdRecord(Base):
    """One row per logical TolTEC data product (e.g. one roach sweep file).

    Stores all columns needed by the tolteca_web catalog query inline, eliminating
    the need for JSON parsing at query time.  A DataProd maps to a single interface
    file (e.g. ``toltec0_098765_000_0000_sweep.nc``) and its derived zarr store.

    Attributes
    ----------
    pk : int
        Integer primary key.
    uid : str
        Human-readable unique identifier, e.g.
        ``"tcs-98765-0-0-toltec0-VnaSweep"``.
    raw_obs_uid : str
        Foreign key to :class:`RawObsRecord`.
    data_prod_type : str
        Product type string from :class:`~tolteca_db.constants.DataProdType`
        (e.g. ``dp_raw_obs``, ``dp_zarr_sweep``).
    interface : str
        Interface identifier (e.g. ``toltec0``, ``tel_toltec``).
    nw : int | None
        Roach network index (0–12).  ``None`` for non-roach interfaces.
    array_name : str | None
        Array name (``a1100``, ``a1400``, ``a2000``).
    data_kind : str | None
        Data kind label (``VnaSweep``, ``TargetSweep``, ``Tune``,
        ``RawTimeStream``, …).
    n_chans : int | None
        Number of detector channels.
    n_samples : int | None
        Number of samples (sweep steps or time samples).
    lo_center_freq_hz : float | None
        LO centre frequency in Hz.
    drive_atten_db : float | None
        Drive attenuation in dB.
    sense_atten_db : float | None
        Sense attenuation in dB.
    nc_path : str | None
        Absolute path (or URI) to the original netCDF interface file.
    zarr_path : str | None
        Absolute path (or URI) to the derived zarr data store.
    availability : str | None
        Availability state (``available``, ``missing``, ``remote``, …).
    meta : dict | None
        Additional JSON metadata.
    created_at : datetime
        Database insert timestamp.
    updated_at : datetime
        Last update timestamp.
    raw_obs : RawObsRecord
        Parent observation.
    file_records : list[FileRecord]
        Physical file tracking records.
    assoc_edges : list[AssocEdgeRecord]
        Association graph edges involving this product.
    data_prod_flags : list[DataProdFlagRecord]
        Quality flags on this product.
    """

    __tablename__ = "data_prod"

    pk: Mapped[Pk]
    uid: Mapped[str] = mapped_column(String(256), unique=True, index=True)
    raw_obs_uid: Mapped[str] = mapped_column(
        String(128), ForeignKey("raw_obs.uid"), index=True
    )
    data_prod_type: Mapped[str] = mapped_column(String(32), index=True)
    interface: Mapped[str] = mapped_column(String(32), index=True)

    # TolTEC catalog columns — mirrors tolteca_web _CATALOG_SCHEMA
    nw: Mapped[int | None] = mapped_column(Integer, nullable=True)
    array_name: Mapped[str | None] = mapped_column(
        String(16), nullable=True, index=True
    )
    data_kind: Mapped[str | None] = mapped_column(
        String(32), nullable=True, index=True
    )
    n_chans: Mapped[int | None] = mapped_column(Integer, nullable=True)
    n_samples: Mapped[int | None] = mapped_column(Integer, nullable=True)
    lo_center_freq_hz: Mapped[float | None] = mapped_column(Float, nullable=True)
    drive_atten_db: Mapped[float | None] = mapped_column(Float, nullable=True)
    sense_atten_db: Mapped[float | None] = mapped_column(Float, nullable=True)

    # File paths
    nc_path: Mapped[str | None] = mapped_column(String(512), nullable=True)
    zarr_path: Mapped[str | None] = mapped_column(String(512), nullable=True)

    availability: Mapped[str | None] = mapped_column(
        String(32), nullable=True, index=True
    )
    meta: Mapped[Context]
    created_at: Mapped[Created_at]
    updated_at: Mapped[Updated_at]

    # Relationships
    raw_obs: Mapped[RawObsRecord] = relationship(back_populates="data_prods")
    file_records: Mapped[list[FileRecord]] = relationship(
        back_populates="data_prod",
        cascade="all, delete-orphan",
    )
    assoc_edges: Mapped[list[AssocEdgeRecord]] = relationship(
        back_populates="data_prod",
        cascade="all, delete-orphan",
    )
    data_prod_flags: Mapped[list[DataProdFlagRecord]] = relationship(
        back_populates="data_prod",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("ix_data_prod_obs_interface", "raw_obs_uid", "interface"),
        Index("ix_data_prod_kind_array", "data_kind", "array_name"),
    )

    def __repr__(self) -> str:
        return f"DataProdRecord(uid={self.uid!r})"
