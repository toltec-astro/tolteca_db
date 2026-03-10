"""Association ORM models: AssocRecord, AssocEdgeRecord.

AssocRecord — one row per association group (e.g. a cal→science mapping).
AssocEdgeRecord — one row per product→assoc edge (input or output role).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Float, ForeignKey, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from tolteca_db.models.orm.base import Base
from tolteca_db.utils import Context, Created_at, Pk, Updated_at

if TYPE_CHECKING:
    from tolteca_db.models.orm.registry import DataProdRecord
    from tolteca_db.models.orm.task import TaskRecord


class AssocRecord(Base):
    """One row per association group produced by an association rule.

    An association groups a set of :class:`~tolteca_db.models.orm.registry.DataProdRecord`
    rows into a named logical unit (e.g. a calibration set).  Edges are stored in
    :class:`AssocEdgeRecord`.

    Attributes
    ----------
    pk : int
        Integer primary key.
    uid : str
        Unique human-readable identifier.
    rule_name : str
        Name of the association rule that produced this group.
    context : dict | None
        JSON metadata about the association.
    status : str
        Status of the association (``pending``, ``accepted``, ``rejected``).
    score : float | None
        Confidence score from the association rule.
    created_at : datetime
        Database insert timestamp.
    updated_at : datetime
        Last update timestamp.
    edges : list[AssocEdgeRecord]
        Product edges (inputs + outputs) in this association.
    tasks : list[TaskRecord]
        Reduction tasks triggered by this association.
    """

    __tablename__ = "assoc"

    pk: Mapped[Pk]
    uid: Mapped[str] = mapped_column(String(256), unique=True, index=True)
    rule_name: Mapped[str] = mapped_column(String(64), index=True)
    context: Mapped[Context]
    status: Mapped[str] = mapped_column(
        String(16), index=True, default="pending"
    )
    score: Mapped[float | None] = mapped_column(Float, nullable=True)
    created_at: Mapped[Created_at]
    updated_at: Mapped[Updated_at]

    # Relationships
    edges: Mapped[list[AssocEdgeRecord]] = relationship(
        back_populates="assoc",
        cascade="all, delete-orphan",
    )
    tasks: Mapped[list[TaskRecord]] = relationship(
        back_populates="assoc",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("ix_assoc_rule_status", "rule_name", "status"),
    )

    def __repr__(self) -> str:
        return f"AssocRecord(uid={self.uid!r}, rule_name={self.rule_name!r})"


class AssocEdgeRecord(Base):
    """Edge linking a data product to an association group.

    Each row says: "data product *X* is an *input/output* for association *A*".

    Attributes
    ----------
    pk : int
        Integer primary key.
    assoc_fk : int
        Foreign key to :class:`AssocRecord`.
    data_prod_fk : int
        Foreign key to :class:`~tolteca_db.models.orm.registry.DataProdRecord`.
    role : str
        Edge role (``input`` or ``output``).
    assoc : AssocRecord
        Parent association.
    data_prod : DataProdRecord
        Referenced data product.
    """

    __tablename__ = "assoc_edge"

    pk: Mapped[Pk]
    assoc_fk: Mapped[int] = mapped_column(
        Integer, ForeignKey("assoc.pk"), index=True
    )
    data_prod_fk: Mapped[int] = mapped_column(
        Integer, ForeignKey("data_prod.pk"), index=True
    )
    role: Mapped[str] = mapped_column(String(32))

    # Relationships
    assoc: Mapped[AssocRecord] = relationship(back_populates="edges")
    data_prod: Mapped[DataProdRecord] = relationship(back_populates="assoc_edges")

    __table_args__ = (
        Index("ix_assoc_edge_assoc_role", "assoc_fk", "role"),
    )

    def __repr__(self) -> str:
        return (
            f"AssocEdgeRecord(assoc_fk={self.assoc_fk!r}, "
            f"data_prod_fk={self.data_prod_fk!r}, role={self.role!r})"
        )
