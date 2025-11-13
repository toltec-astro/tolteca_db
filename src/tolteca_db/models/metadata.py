"""Typed metadata models for TolTEC data products.

This module defines dataclass-based metadata models for structured metadata
stored in the DataProd.meta JSON field using adaptix's AdaptixJSON.
These provide type safety and IDE autocomplete for TolTEC-specific metadata.

Reference: ADR-009 in design/architecture.md, Phase 3 of adaptix integration
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, TypeVar

from adaptix import Retort
from adaptix.integrations.sqlalchemy import AdaptixJSON
from sqlalchemy.types import JSON

from tolteca_db.constants import DataProdType

# Global retort for adaptix conversions across all ORM models
# Used by AdaptixJSON in data_prod.py and source.py
# Centralized here to ensure consistent serialization behavior
_retort = Retort()

# Shared JSON type instance for DuckDB compatibility
# DuckDB uses JSON (not JSONB), and AdaptixJSON requires a type instance (not class)
_json_type = JSON()


def adaptix_json_type(metadata_type) -> AdaptixJSON:
    """
    Create AdaptixJSON column type for DuckDB-compatible JSON storage.

    Centralizes the configuration of AdaptixJSON to ensure:
    - Single JSON() type instance shared across all uses
    - DuckDB compatibility (uses JSON, not PostgreSQL's JSONB)
    - Type-safe metadata serialization via adaptix

    Parameters
    ----------
    metadata_type : type[T]
        The metadata dataclass type (e.g., AnyDataProdMeta, ProcessContext)

    Returns
    -------
    AdaptixJSON[T]
        Configured AdaptixJSON type for SQLAlchemy mapped_column()

    Examples
    --------
    >>> meta: Mapped[AnyDataProdMeta] = mapped_column(
    ...     adaptix_json_type(AnyDataProdMeta),
    ...     nullable=False,
    ... )

    Notes
    -----
    - Uses shared _json_type instance for memory efficiency
    - impl=JSON() is required because AdaptixJSON defaults to JSONB for
      PostgreSQL-derived dialects (duckdb-engine inherits from PostgreSQL)
    - DuckDB only supports JSON type, not JSONB
    """
    return AdaptixJSON(_retort, metadata_type, impl=_json_type)


__all__ = [
    "_retort",
    "_json_type",
    "adaptix_json_type",
    "AnyDataProdMeta",
    "CalGroupMeta",
    "DataProdMetaBase",
    "DrivefitMeta",
    "FocusGroupMeta",
    "InterfaceFileMeta",
    "MetadataType",
    "NamedGroupMeta",
    "ProcessContext",
    "RawObsMeta",
    "ReducedObsMeta",
]


@dataclass
class DataProdMetaBase:
    """Base metadata for all data products.

    Attributes
    ----------
    name : str
        Unique product name
    data_prod_type : DataProdType
        TolTEC product type (dp_raw_obs, dp_reduced_obs, etc.)
    description : str | None
        Optional human-readable description

    Notes
    -----
    Using dataclasses with adaptix's AdaptixJSON for type-safe JSON storage.
    No runtime validation overhead - type safety enforced by type checkers.
    """

    name: str
    data_prod_type: DataProdType
    description: str | None = None


@dataclass
class RawObsMeta(DataProdMetaBase):
    """Metadata specific to dp_raw_obs.

    Attributes
    ----------
    master : str
        Master identifier (e.g., 'toltec', 'tcs')
    obsnum : int
        Observation number
    subobsnum : int
        Sub-observation number
    scannum : int
        Scan number
    data_kind : int
        Data acquisition mode flags (from ToltecDataKind enum)
    nw_id : int | None
        Network ID for roach data
    obs_goal : str | None
        Observation goal/purpose
    source_name : str | None
        Target source name
    """

    tag: Literal["raw_obs"] = "raw_obs"  # Discriminator for union types
    master: str = ""
    obsnum: int = 0
    subobsnum: int = 0
    scannum: int = 0
    data_kind: int = 0  # ToltecDataKind flag value
    nw_id: int | None = None
    obs_goal: str | None = None
    source_name: str | None = None


@dataclass
class ReducedObsMeta(DataProdMetaBase):
    """Metadata specific to dp_reduced_obs.

    Attributes
    ----------
    master : str
        Master identifier
    obsnum : int
        Observation number
    subobsnum : int
        Sub-observation number
    scannum : int
        Scan number
    reduction_method : str | None
        Reduction method used (inline, offline, etc.)
    calibration_version : str | None
        Calibration version identifier
    processing_date : str | None
        ISO8601 processing timestamp
    quality_score : float | None
        Quality metric (0-1 scale, validated at application layer)
    """

    tag: Literal["reduced_obs"] = "reduced_obs"  # Discriminator for union types
    master: str = ""
    obsnum: int = 0
    subobsnum: int = 0
    scannum: int = 0
    reduction_method: str | None = None
    calibration_version: str | None = None
    processing_date: str | None = None
    quality_score: float | None = None


@dataclass
class CalGroupMeta(DataProdMetaBase):
    """Metadata specific to dp_cal_group.

    Attributes
    ----------
    master : str
        Master identifier
    obsnum : int
        Primary observation number
    n_items : int
        Number of raw observations in group
    group_type : str | None
        Calibration group type
    date_range : tuple[str, str] | None
        Start and end dates of calibration data
    """

    tag: Literal["cal_group"] = "cal_group"  # Discriminator for union types
    master: str = ""
    obsnum: int = 0
    n_items: int = 0
    group_type: str | None = None
    date_range: tuple[str, str] | None = None


@dataclass
class DrivefitMeta(DataProdMetaBase):
    """Metadata specific to dp_drivefit.

    Attributes
    ----------
    master : str
        Master identifier
    obsnum : int
        Primary observation number
    n_items : int
        Number of raw observations used
    fit_method : str | None
        Fitting method/algorithm
    convergence_status : str | None
        Fit convergence status
    chi_squared : float | None
        Chi-squared goodness of fit (non-negative, validated at application layer)
    """

    tag: Literal["drivefit"] = "drivefit"  # Discriminator for union types
    master: str = ""
    obsnum: int = 0
    n_items: int = 0
    fit_method: str | None = None
    convergence_status: str | None = None
    chi_squared: float | None = None


@dataclass
class FocusGroupMeta(DataProdMetaBase):
    """Metadata specific to dp_focus_group.

    Attributes
    ----------
    master : str
        Master identifier
    obsnum : int
        Primary observation number
    n_items : int
        Number of raw observations in group
    focus_positions : list[float] | None
        Focus positions sampled
    best_focus : float | None
        Optimal focus position
    focus_metric : str | None
        Metric used (FWHM, Strehl, etc.)
    """

    tag: Literal["focus_group"] = "focus_group"  # Discriminator for union types
    master: str = ""
    obsnum: int = 0
    n_items: int = 0
    focus_positions: list[float] | None = None
    best_focus: float | None = None
    focus_metric: str | None = None


@dataclass
class NamedGroupMeta(DataProdMetaBase):
    """Metadata specific to dp_named_group.

    Attributes
    ----------
    group_name : str
        User-defined group name
    n_items : int
        Number of products in group
    tags : list[str] | None
        Optional tags for categorization
    owner : str | None
        Group owner/creator
    notes : str | None
        Free-form notes
    """

    tag: Literal["named_group"] = "named_group"  # Discriminator for union types
    group_name: str = ""
    n_items: int = 0
    tags: list[str] | None = None
    owner: str | None = None
    notes: str | None = None


@dataclass
class InterfaceFileMeta:
    """Metadata for interface files in DataProdSource.

    Attributes
    ----------
    nw_id : int | None
        Network ID (for roach files)
    roach : int | None
        Roach number
    """

    nw_id: int | None = None
    roach: int | None = None


@dataclass
class ProcessContext:
    """Process metadata for DataProdAssoc context field.

    Attributes
    ----------
    module : str | None
        Processing module name
    version : str | None
        Processing version
    config : dict[str, Any] | None
        Processing configuration
    """

    module: str | None = None
    version: str | None = None
    config: dict[str, Any] | None = None


# Union type for DataProd.meta (polymorphic metadata with Literal discriminators)


# Union type for DataProd.meta (polymorphic metadata with Literal discriminators)
AnyDataProdMeta = (
    RawObsMeta
    | ReducedObsMeta
    | CalGroupMeta
    | DrivefitMeta
    | FocusGroupMeta
    | NamedGroupMeta
)

# Union type for all metadata (includes InterfaceFileMeta and dict fallback)
MetadataType = AnyDataProdMeta | InterfaceFileMeta | dict[str, Any]
