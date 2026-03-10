"""Repository layer for tolteca_db.

Provides high-level data access patterns and query interfaces.

Phase 3 Status: ✅ COMPLETE - AdaptixJSON automatic serialization
    Structured metadata models (dataclasses with union types) stored
    directly in DataProd.meta column using AdaptixJSON for automatic
    serialization/deserialization. This is the production-ready solution.

Phase 4 Status: ⚠️ ATTEMPTED - Repository DTOs
    Successfully resolved SQLAlchemy forward reference issue using
    adaptix.type_tools.exec_type_checking(). However, nested relationship
    conversion requires complex setup. Phase 3 is the optimal solution.

Phase 2 Status: 🚧 IN PROGRESS - File API Layer
    ObsQuery class and SourceInfoModel for tolteca_v2 compatibility.
    Provides DataFrame output matching existing .toltec_file accessor.

Architecture:
    - Direct ORM access with SQLAlchemy 2.0 patterns
    - Type-safe metadata using dataclasses (Phase 3)
    - File API Layer for tolteca_v2 compatibility (Phase 2)
"""

from __future__ import annotations

from .file_api import ObsQuery, SourceInfoModel

__all__ = [
    "ObsQuery",
    "SourceInfoModel",
]


