"""Flag ORM models: ObsFlagRecord, DataProdFlagRecord.

Quality flags on raw observations and data products.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from tolteca_db.models.orm.base import Base
from tolteca_db.utils import Created_at, Pk

if TYPE_CHECKING:
    from tolteca_db.models.orm.registry import DataProdRecord, RawObsRecord


class ObsFlagRecord(Base):
    """Quality flag on a raw observation.

    Attributes
    ----------
    pk : int
        Integer primary key.
    raw_obs_uid : str
        Foreign key to :class:`~tolteca_db.models.orm.registry.RawObsRecord`.
    flag_reason : str
        Human-readable reason for the flag.
    flag_source : str
        Origin of the flag (e.g. ``auto``, ``manual``, ``pipeline``).
    flagged_at : datetime
        UTC timestamp when the flag was asserted.
    raw_obs : RawObsRecord
        Parent raw observation.
    """

    __tablename__ = "obs_flag"

    pk: Mapped[Pk]
    raw_obs_uid: Mapped[str] = mapped_column(
        String(128), ForeignKey("raw_obs.uid"), index=True
    )
    flag_reason: Mapped[str] = mapped_column(String(256))
    flag_source: Mapped[str] = mapped_column(String(64))
    flagged_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    # Relationships
    raw_obs: Mapped[RawObsRecord] = relationship(back_populates="obs_flags")

    def __repr__(self) -> str:
        return (
            f"ObsFlagRecord(raw_obs_uid={self.raw_obs_uid!r}, "
            f"reason={self.flag_reason!r})"
        )


class DataProdFlagRecord(Base):
    """Quality flag on a data product.

    Attributes
    ----------
    pk : int
        Integer primary key.
    data_prod_fk : int
        Foreign key to :class:`~tolteca_db.models.orm.registry.DataProdRecord`.
    flag_reason : str
        Human-readable reason for the flag.
    flag_source : str
        Origin of the flag (e.g. ``auto``, ``manual``, ``pipeline``).
    flagged_at : datetime
        UTC timestamp when the flag was asserted.
    data_prod : DataProdRecord
        Parent data product.
    """

    __tablename__ = "data_prod_flag"

    pk: Mapped[Pk]
    data_prod_fk: Mapped[int] = mapped_column(
        Integer, ForeignKey("data_prod.pk"), index=True
    )
    flag_reason: Mapped[str] = mapped_column(String(256))
    flag_source: Mapped[str] = mapped_column(String(64))
    flagged_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    # Relationships
    data_prod: Mapped[DataProdRecord] = relationship(back_populates="data_prod_flags")

    def __repr__(self) -> str:
        return (
            f"DataProdFlagRecord(data_prod_fk={self.data_prod_fk!r}, "
            f"reason={self.flag_reason!r})"
        )
