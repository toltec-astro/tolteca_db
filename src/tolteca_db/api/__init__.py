"""API layer for tolteca_db.

Provides high-level interfaces for querying and accessing data products:
- ObsQuery: Query interface matching tolteca_v2 ObsSpec pattern
- DataFrame output compatible with tolteca_v2 file API
"""

from __future__ import annotations

from .obs import ObsQuery, SourceInfoModel

__all__ = [
    "ObsQuery",
    "SourceInfoModel",
]
