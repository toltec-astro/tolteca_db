"""Typed metadata models for TolTEC data products.

This module defines Pydantic models for the structured metadata stored in the
DataProduct.meta JSON field. These provide type safety and validation for
TolTEC-specific product metadata.

Reference: ADR-009 in design/architecture.md
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from tolteca_db.constants import DataProdType

__all__ = [
    "CalGroupMeta",
    "DataProdMetaBase",
    "DrivefitMeta",
    "FocusGroupMeta",
    "NamedGroupMeta",
    "RawObsMeta",
    "ReducedObsMeta",
]


class DataProdMetaBase(BaseModel):
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
    Enhanced validation config for production-grade data integrity:
    - validate_default: Validates default values (Pydantic 2.12+)
    - validate_assignment: Re-validates when attributes change after creation
    - extra: "forbid" rejects unknown fields (strict mode)
    """

    model_config = {
        "use_enum_values": True,
        "validate_default": True,      # Pydantic 2.12+: Validate defaults
        "validate_assignment": True,    # Re-validate on attribute changes
        "extra": "forbid",              # Reject unknown fields (strict)
    }

    name: str
    data_prod_type: DataProdType
    description: str | None = None


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

    master: str
    obsnum: int
    subobsnum: int
    scannum: int
    data_kind: int  # ToltecDataKind flag value
    nw_id: int | None = None
    obs_goal: str | None = None
    source_name: str | None = None


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
        Quality metric (0-1 scale)
    """

    master: str
    obsnum: int
    subobsnum: int
    scannum: int
    reduction_method: str | None = None
    calibration_version: str | None = None
    processing_date: str | None = None
    quality_score: float | None = Field(None, ge=0.0, le=1.0)


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
        ISO8601 date range (start, end)
    """

    master: str
    obsnum: int
    n_items: int
    group_type: str | None = None
    date_range: tuple[str, str] | None = None


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
        Chi-squared goodness of fit
    """

    master: str
    obsnum: int
    n_items: int
    fit_method: str | None = None
    convergence_status: str | None = None
    chi_squared: float | None = Field(None, ge=0.0)


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

    master: str
    obsnum: int
    n_items: int
    focus_positions: list[float] | None = None
    best_focus: float | None = None
    focus_metric: str | None = None


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

    group_name: str
    n_items: int
    tags: list[str] | None = None
    owner: str | None = None
    notes: str | None = None
