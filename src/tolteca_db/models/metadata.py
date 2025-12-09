"""Typed metadata models for TolTEC data products.

This module defines dataclass-based metadata models for structured metadata
stored in the DataProd.meta JSON field using adaptix's AdaptixJSON.
These provide type safety and IDE autocomplete for TolTEC-specific metadata.

Reference: ADR-009 in design/architecture.md, Phase 3 of adaptix integration
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, TypeVar

from adaptix import Retort
from adaptix.integrations.sqlalchemy import AdaptixJSON
from sqlalchemy.types import JSON

from tolteca_db.constants import DataProdType

# Global retort for adaptix conversions across all ORM models
# Used by AdaptixJSON in data_prod.py and source.py
# Centralized here to ensure consistent serialization behavior
# Union discrimination: Uses Literal type on TelInterfaceMeta.interface for discrimination
# - RoachInterfaceMeta: interface = str | None (roach interface names)
# - TelInterfaceMeta: interface = Literal["tel_toltec"] (explicit discriminator)
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
    "AnyInterfaceMeta",
    "CalGroupMeta",
    "DataProdMetaBase",
    "DrivefitMeta",
    "FocusGroupMeta",
    "MetadataType",
    "NamedGroupMeta",
    "ObsIdMixin",
    "ProcessContext",
    "RawObsMeta",
    "ReducedObsMeta",
    "RoachInterfaceMeta",
    "RoachMetaMixin",
    "TelInterfaceMeta",
    "TelMetaMixin",
]


# ==================== Shared Mixin Classes ====================


@dataclass
class ObsIdMixin:
    """Core observation identification fields shared across interfaces and products.
    
    These fields uniquely identify a TolTEC observation quartet.
    Used by both interface-level metadata (RoachInterfaceMeta, TelInterfaceMeta)
    and data product-level metadata (RawObsMeta, ReducedObsMeta, etc.).
    """

    obsnum: int = 0
    subobsnum: int = 0
    scannum: int = 0
    master: str = ""


@dataclass
class RoachMetaMixin:
    """Roach interface metadata fields.
    
    Used by RoachInterfaceMeta (interface-level, complete roach state).
    Each roach interface has its own metadata stored in DataProdSource.meta.
    """

    nw_id: int | None = None
    roach: int | None = None
    interface: str | None = None
    hostname: str | None = None


@dataclass
class TelMetaMixin:
    """LMT telescope metadata fields shared between interface and product levels.
    
    Used by:
    - TelInterfaceMeta (interface-level, complete telescope state)
    - RawObsMeta (data product-level, denormalized for query efficiency)
    
    Fields include project info, timing, pointing, optics, and conditions.
    Enables queries like "tau < 0.1" without joining through DataProdSource.
    """

    # Observation metadata
    obs_datetime: datetime | None = None
    source_name: str | None = None
    obs_goal: str | None = None

    # Project and program
    project_id: str | None = None
    obs_pgm: str | None = None

    # Timing
    integration_time: float | None = None

    # Pointing
    az_deg: float | None = None
    el_deg: float | None = None
    user_az_offset_arcsec: float | None = None
    user_el_offset_arcsec: float | None = None
    paddle_az_offset_arcsec: float | None = None
    paddle_el_offset_arcsec: float | None = None

    # Optics
    m1_zernike: list[float] | None = None  # 7 Zernike coefficients (microns)
    m2_offset_mm: tuple[float, float, float] | None = None  # X, Y, Z offsets (mm)

    # Atmospheric conditions
    tau: float | None = None
    crane_in_beam: bool | None = None


# ==================== Base Classes ====================


@dataclass(kw_only=True)
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
    Fields are keyword-only to allow mixing with mixin classes that have defaults.
    """

    name: str
    data_prod_type: DataProdType
    description: str | None = None


@dataclass
class RawObsMeta(DataProdMetaBase, ObsIdMixin, TelMetaMixin):
    """Metadata for dp_raw_obs - aggregates from all interface sources.
    
    Data product-level metadata combining:
    - Core identification (ObsIdMixin) - from all interfaces
    - Tel metadata (TelMetaMixin) - denormalized from TelInterfaceMeta for query efficiency
    - Data kind field - TolTEC-specific acquisition mode
    
    Architecture
    ------------
    Uses true inheritance from mixins to share field definitions:
    - ObsIdMixin: obsnum, subobsnum, scannum, master
    - TelMetaMixin: project_id, tau, az_deg, source_name, m1_zernike, etc.
    
    Denormalization Strategy
    ------------------------
    Tel fields are duplicated from TelInterfaceMeta (DataProdSource.meta) to enable
    efficient queries without JOINs:
    
    GOOD: WHERE meta['tau'] < 0.1  # Direct query, O(1) index lookup
    BAD:  JOIN data_prod_source ... WHERE src.meta['tau'] < 0.1  # Expensive JOIN
    
    For master='tcs': Tel fields populated from TelInterfaceMeta
    For master='ics': Tel fields remain None (no tel interface exists)
    
    Attributes
    ----------
    tag : Literal["raw_obs"]
        Discriminator for union type deserialization
    data_kind : int
        Data acquisition mode flags (from ToltecDataKind enum)
    
    Notes
    -----
    All observation identification and telescope metadata fields inherited from mixins.
    See ObsIdMixin and TelMetaMixin for complete field list.
    Roach interface metadata (nw_id, roach, interface, hostname) is stored in 
    DataProdSource.meta (RoachInterfaceMeta), not aggregated to DataProd level.
    """

    tag: Literal["raw_obs"] = "raw_obs"  # Discriminator for union types
    
    # TolTEC-specific field
    data_kind: int = 0  # ToltecDataKind flag value
    
    # Note: All other fields inherited from mixins:
    # - ObsIdMixin: obsnum, subobsnum, scannum, master
    # - TelMetaMixin: obs_datetime, source_name, obs_goal, project_id, obs_pgm,
    #                 integration_time, az_deg, el_deg, user_az_offset_arcsec,
    #                 user_el_offset_arcsec, paddle_az_offset_arcsec,
    #                 paddle_el_offset_arcsec, m1_zernike, m2_offset_mm,
    #                 tau, crane_in_beam


@dataclass
class ReducedObsMeta(DataProdMetaBase, ObsIdMixin):
    """Metadata for dp_reduced_obs.

    Attributes
    ----------
    tag : Literal["reduced_obs"]
        Discriminator for union type deserialization
    reduction_method : str | None
        Reduction method used (inline, offline, etc.)
    calibration_version : str | None
        Calibration version identifier
    processing_date : str | None
        ISO8601 processing timestamp
    quality_score : float | None
        Quality metric (0-1 scale, validated at application layer)
    
    Notes
    -----
    Identification fields (obsnum, subobsnum, scannum, master) inherited from ObsIdMixin.
    """

    tag: Literal["reduced_obs"] = "reduced_obs"  # Discriminator for union types
    reduction_method: str | None = None
    calibration_version: str | None = None
    processing_date: str | None = None
    quality_score: float | None = None


@dataclass
class CalGroupMeta(DataProdMetaBase, ObsIdMixin):
    """Metadata for dp_cal_group.

    Attributes
    ----------
    tag : Literal["cal_group"]
        Discriminator for union type deserialization
    n_items : int
        Number of raw observations in group
    group_type : str | None
        Calibration group type
    date_range : tuple[str, str] | None
        Start and end dates of calibration data
    
    Notes
    -----
    Identification fields (obsnum, master) inherited from ObsIdMixin.
    subobsnum and scannum not used for calibration groups.
    """

    tag: Literal["cal_group"] = "cal_group"  # Discriminator for union types
    n_items: int = 0
    group_type: str | None = None
    date_range: tuple[str, str] | None = None


@dataclass
class DrivefitMeta(DataProdMetaBase, ObsIdMixin):
    """Metadata for dp_drivefit.

    Attributes
    ----------
    tag : Literal["drivefit"]
        Discriminator for union type deserialization
    n_items : int
        Number of raw observations used
    fit_method : str | None
        Fitting method/algorithm
    convergence_status : str | None
        Fit convergence status
    chi_squared : float | None
        Chi-squared goodness of fit (non-negative, validated at application layer)
    
    Notes
    -----
    Identification fields (obsnum, master) inherited from ObsIdMixin.
    """

    tag: Literal["drivefit"] = "drivefit"  # Discriminator for union types
    n_items: int = 0
    fit_method: str | None = None
    convergence_status: str | None = None
    chi_squared: float | None = None


@dataclass
class FocusGroupMeta(DataProdMetaBase, ObsIdMixin):
    """Metadata for dp_focus_group.

    Attributes
    ----------
    tag : Literal["focus_group"]
        Discriminator for union type deserialization
    n_items : int
        Number of raw observations in group
    focus_positions : list[float] | None
        Focus positions sampled
    best_focus : float | None
        Optimal focus position
    focus_metric : str | None
        Metric used (FWHM, Strehl, etc.)
    
    Notes
    -----
    Identification fields (obsnum, master) inherited from ObsIdMixin.
    """

    tag: Literal["focus_group"] = "focus_group"  # Discriminator for union types
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


# ==================== Interface-Level Metadata ====================


@dataclass
class RoachInterfaceMeta(ObsIdMixin, RoachMetaMixin):
    """Roach interface metadata for TolTEC detector network files.
    
    Interface-level metadata for toltec0-12 roach interfaces.
    Stored in DataProdSource.meta for roach interface files.
    Contains detector acquisition state including network ID, roach number,
    interface name, and hostname.
    
    Attributes
    ----------
    (Inherited from ObsIdMixin)
    obsnum : int
        Observation number
    subobsnum : int
        Sub-observation number
    scannum : int
        Scan number
    master : str
        Master identifier (e.g., 'toltec', 'tcs', 'ics')
    
    (Inherited from RoachMetaMixin)
    nw_id : int | None
        Network ID for roach data
    roach : int | None
        Roach number (0-12)
    interface : str | None
        Interface name (e.g., 'toltec0', 'toltec1')
    hostname : str | None
        Acquisition computer hostname
    
    Notes
    -----
    Previously called InterfaceFileMeta.
    Renamed to RoachInterfaceMeta for consistency with TelInterfaceMeta.
    """
    
    # Explicit type discriminator for adaptix union handling
    type: Literal["roach"] = "roach"


@dataclass
class TelInterfaceMeta(ObsIdMixin, TelMetaMixin):
    """LMT telescope interface metadata for TolTEC observations.
    
    Interface-level metadata for tel_toltec interface.
    Stored in DataProdSource.meta for tel interface files.
    Represents complete telescope state during observation including
    pointing, mirror configuration, and atmospheric conditions.
    Sourced from LMT metadata database CSV export.
    
    Architecture
    ------------
    Uses mixins to share field definitions with RawObsMeta:
    - ObsIdMixin: obsnum, subobsnum, scannum, master
    - TelMetaMixin: project_id, tau, az_deg, source_name, m1_zernike, etc.
    
    This ensures type consistency between interface-level (complete state)
    and data product-level (denormalized subset) metadata.

    Attributes
    ----------
    interface : str
        Interface identifier (always "tel_toltec")
    receiver : str
        Receiver name (always "Toltec")
    instrument : str
        Instrument name (always "tel")
    main_time : float
        Main beam time (seconds)
    ref_time : float
        Reference time (seconds)
    valid : bool
        Valid data flag
    
    Notes
    -----
    Tel interface exists only for master='tcs' observations.
    ICS observations do not have corresponding tel metadata.
    
    Fields from mixins (obsnum, master, tau, az_deg, etc.) inherited.
    See ObsIdMixin and TelMetaMixin for complete field list.
    """

    # Explicit type discriminator for adaptix union handling
    type: Literal["tel"] = "tel"

    # Interface identifiers (Literal type for union discrimination)
    interface: Literal["tel_toltec"] = "tel_toltec"
    receiver: str = "Toltec"
    instrument: str = "tel"

    # Additional timing (not in TelMetaMixin)
    main_time: float = 0.0
    ref_time: float = 0.0

    # Validation flag
    valid: bool = True
    
    # Note: All other fields inherited from mixins:
    # - ObsIdMixin: obsnum, subobsnum, scannum, master
    # - TelMetaMixin: obs_datetime, source_name, project_id, integration_time,
    #                 az_deg, el_deg, tau, m1_zernike, m2_offset_mm, etc.


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


# Union type for interface-level metadata (DataProdSource.meta)
# For raw interface files: roach interfaces OR tel interface
AnyInterfaceMeta = TelInterfaceMeta | RoachInterfaceMeta

# Union type for DataProd.meta (polymorphic metadata with Literal discriminators)
# For data products: raw obs uses interface metadata, others have their own
AnyDataProdMeta = (
    RawObsMeta
    | ReducedObsMeta
    | CalGroupMeta
    | DrivefitMeta
    | FocusGroupMeta
    | NamedGroupMeta
)

# Union type for all metadata (includes interface metadata and dict fallback)
MetadataType = AnyDataProdMeta | AnyInterfaceMeta | dict[str, Any]
