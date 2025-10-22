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
]

from .hashing import product_id_hash
from .time import utc_now, utcnow
from .uid import (
    make_cal_group_uid,
    make_group_uid,
    make_raw_obs_uid,
    make_reduced_obs_uid,
    parse_raw_obs_uid,
)
