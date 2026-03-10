"""Quality flag models: Flag, DataProdFlag."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from tolteca_db.models.orm.base import Base
from tolteca_db.utils import Context, Created_at, Desc, Label, LabelKey, Pk, fk

if TYPE_CHECKING:
    from tolteca_db.models.orm.data_prod import DataProd


class Flag(Base):
    """
    Registry of quality flags.

    Uses composite unique constraint (namespace, label) to allow the same
    label name in different namespaces (e.g. namespace="qa" + label="SATURATED"
    vs namespace="detector" + label="SATURATED").

    Attributes
    ----------
    pk : int
        Integer primary key.
    label : str
        Flag name (e.g. "SATURATED", "DEAD_PIXEL").
    namespace : str
        Flag namespace (e.g. "qa", "detector", "telescope").
    description : str | None
        Human-readable description.
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
    Assigned flags to data products (junction table).

    Composite primary key (data_prod_fk, flag_fk).

    Attributes
    ----------
    data_prod_fk : int
        Foreign key to data_prod.
    flag_fk : int
        Foreign key to flag.
    asserted_at : datetime
        When flag was asserted.
    asserted_by : str
        Who/what asserted the flag ("system", "user:jsmith", etc.).
    context : dict | None
        Additional context (JSON).
    data_prod : DataProd
        Relationship to flagged product.
    flag : Flag
        Relationship to flag definition.
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
