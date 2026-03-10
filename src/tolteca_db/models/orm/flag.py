"""Quality flag models: Flag, DataProdFlag."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, Index, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from tolteca_db.models.orm.base import Base
from tolteca_db.utils import Context, Created_at, Desc, Label, LabelKey, Pk, fk, utcnow

if TYPE_CHECKING:
    from tolteca_db.models.orm.data_prod import DataProd


class Flag(Base):
    """
    Registry of quality flags.
    
    Merges multiple enum definitions (QAFlags, DetFlags, etc.) into single table.
    Uses composite unique constraint (namespace, label) to prevent collisions.
    
    Attributes
    ----------
    pk : int
        Integer primary key
    namespace : str
        Flag namespace/group (qa, detector, telescope, calibration, ingest)
    label : str
        Flag name WITHOUT prefix (SATURATED, DEAD_PIXEL, etc.)
    description : str | None
        Human-readable description
    
    Examples
    --------
    From QAFlags.SATURATED:
        namespace="qa", label="SATURATED"
    
    From DetFlags.SATURATED:
        namespace="detector", label="SATURATED"
    
    Both can coexist due to different namespaces.
    """

    __tablename__ = "flag"

    pk: Mapped[Pk]

    label: Mapped[LabelKey]

    namespace: Mapped[Label]

    description: Mapped[Desc | None]

    # Relationships
    data_prod: Mapped[list[DataProdFlag]] = relationship(
        back_populates="flag",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        UniqueConstraint("namespace", "label", name="uq_flag_namespace_label"),
    )


class DataProdFlag(Base):
    """
    Assigned flags to products.
    
    Composite primary key (data_prod_fk, flag_fk).
    
    Attributes
    ----------
    data_prod_fk : str
        Foreign key to data_prod
    flag_fk : int
        Foreign key to flag
    asserted_at : datetime
        When flag was asserted
    asserted_by : str
        Who/what asserted the flag
    context : dict | None
        Additional context (JSON)
    """

    __tablename__ = "data_prod_flag"

    data_prod_fk: Mapped[int] = fk("data_prod", primary_key=True)

    flag_fk: Mapped[int] = fk("flag", primary_key=True)

    asserted_at: Mapped[Created_at]

    asserted_by: Mapped[str] = mapped_column(
        String(64),
        default="system",
        index=True,
    )

    context: Mapped[Context]

    # Relationships
    data_prod: Mapped[DataProd] = relationship(back_populates="flag")
    flag: Mapped[Flag] = relationship(back_populates="data_prod")
