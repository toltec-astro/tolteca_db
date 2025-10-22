"""Time utility functions."""

from __future__ import annotations

from datetime import datetime, timezone

import sqlalchemy as sa
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import expression

__all__ = ["utc_now", "utcnow"]


def utc_now() -> datetime:
    """
    Return current UTC time with timezone awareness.

    Used for Python-side datetime generation (e.g., test fixtures).

    Returns
    -------
    datetime
        Current UTC timestamp with tzinfo=timezone.utc

    Examples
    --------
    >>> from datetime import timezone
    >>> now = utc_now()
    >>> now.tzinfo == timezone.utc
    True
    """
    return datetime.now(tz=timezone.utc)


class utcnow(expression.FunctionElement):  # noqa: N801
    """SQL function element for database-generated UTC timestamps.
    
    This function compiles to database-specific SQL for generating
    UTC timestamps at the database level, avoiding Python datetime
    adapter issues and ensuring consistency across database backends.
    
    Examples
    --------
    Use as default value in ORM mapped columns:
    
    >>> from sqlalchemy.orm import mapped_column
    >>> created_at: Mapped[datetime] = mapped_column(
    ...     DateTime(timezone=True),
    ...     server_default=utcnow()
    ... )
    """
    
    type = sa.DateTime()
    inherit_cache = True


@compiles(utcnow, "postgresql")
def pg_utcnow(_element, _compiler, **_kw):
    """PostgreSQL: TIMEZONE('utc', CURRENT_TIMESTAMP)."""
    return "TIMEZONE('utc', CURRENT_TIMESTAMP)"


@compiles(utcnow, "mysql")
def mysql_utcnow(_element, _compiler, **_kw):
    """MySQL: UTC_TIMESTAMP()."""
    return "UTC_TIMESTAMP()"


@compiles(utcnow, "sqlite")
@compiles(utcnow)  # Default for other databases
def default_utcnow(_element, _compiler, **_kw):
    """SQLite and others: CURRENT_TIMESTAMP."""
    return "CURRENT_TIMESTAMP"
