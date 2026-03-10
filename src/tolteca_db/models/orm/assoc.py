"""Provenance association models: DataProdAssocType, DataProdAssoc."""

from __future__ import annotations

from sqlalchemy import Index
from sqlalchemy.orm import Mapped, mapped_column, relationship

from tolteca_db.models.metadata import ProcessContext, adaptix_json_type
from tolteca_db.models.orm.base import Base
from tolteca_db.utils import Created_at, Desc, LabelKey, Pk, fk


class DataProdAssocType(Base):
    """
    Registry of data product association types.

    Defines edge types for the provenance graph.

    Attributes
    ----------
    pk : int
        Integer primary key.
    label : str
        Type label (e.g. "dpa_reduced_obs_raw_obs").
    description : str | None
        Human-readable description.
    """

    __tablename__ = "data_prod_assoc_type"

    pk: Mapped[Pk]

    label: Mapped[LabelKey]

    description: Mapped[Desc | None]


class DataProdAssoc(Base):
    """
    Provenance association edges.

    Directed edges representing relationships between products.

    Attributes
    ----------
    pk : int
        Autoincrement primary key.
    data_prod_assoc_type_fk : int
        Foreign key to data_prod_assoc_type.
    src_data_prod_fk : int
        Source product FK (data_prod.pk).
    dst_data_prod_fk : int
        Destination product FK (data_prod.pk).
    context : ProcessContext | None
        Process metadata (module, version, config).
    created_at : datetime
        Creation timestamp.
    """

    __tablename__ = "data_prod_assoc"

    pk: Mapped[Pk]

    data_prod_assoc_type_fk: Mapped[int] = fk("data_prod_assoc_type", index=True)

    src_data_prod_fk: Mapped[int] = fk("data_prod", index=True)

    dst_data_prod_fk: Mapped[int] = fk("data_prod", index=True)

    context: Mapped[ProcessContext] = mapped_column(
        adaptix_json_type(ProcessContext)
    )

    created_at: Mapped[Created_at]

    __table_args__ = (
        Index("ix_assoc_src_type", "data_prod_assoc_type_fk", "src_data_prod_fk"),
        Index("ix_assoc_dst_type", "data_prod_assoc_type_fk", "dst_data_prod_fk"),
    )
