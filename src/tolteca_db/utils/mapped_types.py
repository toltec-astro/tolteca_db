"""SQLAlchemy mapped type helpers for consistent column definitions."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any

from sqlalchemy import DateTime, ForeignKey, Integer, Sequence, String, Text
from sqlalchemy.orm import mapped_column
from sqlalchemy.types import JSON

from tolteca_db.utils.time import utcnow

__all__ = [
    "Pk",
    "LabelKey",
    "Label",
    "Name",
    "Desc",
    "LongStr",
    "Context",
    "Created_at",
    "Updated_at",
    "fk",
]

# Primary Key Types
# DuckDB best practice: Use Sequence() to avoid PostgreSQL SERIAL type
# See: https://github.com/Mause/duckdb_engine#auto-incrementing-id-columns
# 
# NOTE: Each table needs its own sequence. The sequence name is automatically
# generated based on the table name when you use this Pk type.
#
# WARNING: This creates a generic sequence that will be shared across tables!
# If you need per-table sequences, define them explicitly in your model.
Pk = Annotated[
    int,
    mapped_column(
        Integer,
        Sequence("generic_pk_seq"),  # Shared sequence for all tables
        primary_key=True,
        comment="Primary key",
    ),
]

# String Field Types
LabelKey = Annotated[
    str,
    mapped_column(
        String(128),
        unique=True,
        index=True,
        comment="Unique label",
    ),
]

Label = Annotated[
    str,
    mapped_column(
        String(128),
        index=True,
        comment="Label",
    ),
]

Name = Annotated[
    str,
    mapped_column(
        String(128),
        nullable=True,
        comment="Display name",
    ),
]

Desc = Annotated[
    str,
    mapped_column(
        Text,
        nullable=True,
        comment="Description",
    ),
]

LongStr = Annotated[
    str,
    mapped_column(
        String(512),
        comment="Long string (URIs, paths, etc.)",
    ),
]

# JSON Field Type
Context = Annotated[
    dict[str, Any] | None,
    mapped_column(
        JSON,
        nullable=True,
        comment="Additional context (JSON)",
    ),
]

# Timestamp Types
Created_at = Annotated[
    datetime,
    mapped_column(
        DateTime(timezone=True),
        server_default=utcnow(),
        index=True,
        comment="Creation timestamp",
    ),
]

Updated_at = Annotated[
    datetime,
    mapped_column(
        DateTime(timezone=True),
        server_default=utcnow(),
        server_onupdate=utcnow(),
        index=True,
        comment="Last update timestamp",
    ),
]


# Foreign Key Helper
def fk(
    target_table: str,
    **kwargs,
):
    """
    Create a foreign key column with DuckDB-compatible constraints.
    
    DuckDB does NOT support CASCADE, SET NULL, or SET DEFAULT in foreign keys.
    Foreign keys only validate referential integrity (prevent invalid references).
    Application logic must handle cascading deletes/updates if needed.
    
    See: https://duckdb.org/docs/sql/constraints
    
    Parameters
    ----------
    target_table : str
        Target table name (will reference {table}.pk)
    **kwargs
        Additional mapped_column arguments
    
    Returns
    -------
    mapped_column
        Configured foreign key column (integer)
    
    Examples
    --------
    >>> data_prod_type_fk: Mapped[int] = fk("data_prod_type", nullable=False)
    >>> location_fk: Mapped[int] = fk("location", primary_key=True)
    
    Notes
    -----
    DuckDB foreign keys provide referential integrity only:
    - INSERT: Validates referenced key exists
    - UPDATE: Validates new value exists if changed
    - DELETE: Does NOT cascade (application must handle)
    """
    kwargs.setdefault("comment", f"Foreign key to {target_table}.pk")
    
    return mapped_column(
        ForeignKey(f"{target_table}.pk"),
        **kwargs,
    )
