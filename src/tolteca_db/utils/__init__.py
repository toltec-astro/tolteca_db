"""Utility functions for tolteca_db."""

from __future__ import annotations

__all__ = [
    "make_cal_group_uid",
    "make_group_uid",
    "make_raw_obs_uid",
    "make_reduced_obs_uid",
    "parse_raw_obs_uid",
    "product_id_hash",
    "utc_now",
    "utcnow",
    # Mapped types
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

from .hashing import product_id_hash
from .mapped_types import (
    Context,
    Created_at,
    Desc,
    LabelKey,
    Label,
    LongStr,
    Name,
    Pk,
    Updated_at,
    fk,
)
from .time import utc_now, utcnow
from .uid import (
    make_cal_group_uid,
    make_group_uid,
    make_raw_obs_uid,
    make_reduced_obs_uid,
    parse_raw_obs_uid,
)
