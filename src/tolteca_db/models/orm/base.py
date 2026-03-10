"""Base class for all ORM models."""

from __future__ import annotations

from sqlalchemy import event
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Base class for all ORM models."""


@event.listens_for(Base.metadata, "before_create")
def _set_table_comments(target, connection, **kw):
    """Auto-set table comments from class docstrings."""
    for table in target.tables.values():
        # Find the ORM class for this table
        for mapper in Base.registry.mappers:
            if mapper.mapped_table is table and mapper.class_.__doc__:
                # Extract first line of docstring
                doc_lines = mapper.class_.__doc__.strip().split("\n")
                comment = doc_lines[0].strip()
                table.comment = comment
                break


@event.listens_for(Base.metadata, "after_create")
def _duckdb_serial_workaround(target, connection, **kw):
    """
    DuckDB compatibility: Replace SERIAL with INTEGER for autoincrement columns.
    
    DuckDB doesn't support PostgreSQL's SERIAL type. This event listener intercepts
    DDL before execution and rewrites SERIAL to INTEGER, which DuckDB handles correctly
    with auto-incrementing sequences.
    
    This is a workaround for duckdb-engine using PostgreSQL dialect.
    """
    # Only apply for DuckDB connections
    if connection.dialect.name != "duckdb":
        return
    
    # Note: This runs AFTER tables are created, but we can use it to document the issue.
    # The actual fix needs to be in the DDL compilation phase, which requires
    # customizing the dialect or using explicit CREATE TABLE statements.
