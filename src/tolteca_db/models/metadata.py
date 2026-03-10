"""Typed metadata models for TolTEC data products.

Phase 3R: AdaptixJSON with union types for type-safe JSON storage.
Dataclass-based metadata models stored in DataProd.meta and
DataProdSource.meta JSON fields.

Reference: ADR-009 in design/architecture.md
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

from adaptix import Retort
from adaptix.integrations.sqlalchemy import AdaptixJSON
from sqlalchemy.types import JSON

from tolteca_db.constants import DataKind

# Global retort for adaptix conversions across all ORM models.
# Centralized here to ensure consistent serialization behavior.
_retort = Retort()

# Shared JSON type instance for DuckDB compatibility.
# DuckDB uses JSON (not JSONB), and AdaptixJSON requires a type instance.
_json_type = JSON()


def adaptix_json_type(metadata_type) -> AdaptixJSON:
    """
    Create AdaptixJSON column type for DuckDB-compatible JSON storage.

    Parameters
    ----------
    metadata_type : type
        The metadata dataclass type (e.g. AnyDataProdMeta, AnyInterfaceMeta).

    Returns
    -------
    AdaptixJSON
        Configured AdaptixJSON type for SQLAlchemy mapped_column().

    Notes
    -----
    Uses shared _json_type (JSON) because DuckDB does not support JSONB.
    duckdb-engine inherits from PostgreSQL dialect, which defaults to JSONB.
    """
    return AdaptixJSON(_retort, metadata_type, impl=_json_type)


# ====================  Shared Mixin Classes  ====================


@dataclass
class ObsIdMixin:
    """Core observation identification fields: obsnum quartet + master.

    Shared between interface-level and data product-level metadata.
    """

    obsnum: int = 0
    subobsnum: int = 0
    scannum: int = 0
    master: str = ""


@dataclass
class RoachMetaMixin:
    """Roach interface metadata fields for toltecN roach boards."""

    nw_id: int | None = None
    roach: int | None = None
    interface: str | None = None
    hostname: str | None = None


@dataclass
class TelMetaMixin:
    """LMT telescope metadata fields shared between interface and product levels.

    Enables queries like ``WHERE meta['tau'] < 0.1`` without JOINs.
    Note: obs_datetime is intentionally omitted here; it lives in
    DataProdMetaBase to avoid duplication in the MRO chain.
    """

    source_name: str | None = None
    obs_goal: str | None = None
    project_id: str | None = None
    obs_pgm: str | None = None
    integration_time: float | None = None
    az_deg: float | None = None
    el_deg: float | None = None
    user_az_offset_arcsec: float | None = None
    user_el_offset_arcsec: float | None = None
    paddle_az_offset_arcsec: float | None = None
    paddle_el_offset_arcsec: float | None = None
    m1_zernike: list[float] | None = None
    m2_offset_mm: tuple[float, float, float] | None = None
    tau: float | None = None
    crane_in_beam: bool | None = None


# ====================  Base Classes  ====================


@dataclass(kw_only=True)
class DataProdMetaBase:
    """Base metadata for all data products.

    Fields are keyword-only to allow mixing with mixin classes that have
    positional defaults. All fields default to empty/None so that subclasses
    can be instantiated with only the fields they care about.

    Attributes
    ----------
    name : str
        Unique product name.
    data_prod_type : str
        TolTEC product type string (e.g. "dp_raw_obs").
    description : str | None
        Optional human-readable description.
    obs_datetime : datetime | None
        Observation datetime or latest member datetime for groups.
    """

    name: str = ""
    data_prod_type: str = ""
    description: str | None = None
    obs_datetime: datetime | None = None


# ====================  DataProd Metadata Classes  ====================


@dataclass
class RawObsMeta(DataProdMetaBase, ObsIdMixin, TelMetaMixin):
    """Metadata for dp_raw_obs — aggregates from all interface sources.

    Combines core identification (ObsIdMixin), tel metadata (TelMetaMixin),
    and TolTEC-specific fields. nw_id is included for dagster asset workflows
    where a product corresponds to a single roach interface file.

    Attributes
    ----------
    tag : Literal["raw_obs"]
        Discriminator for union type deserialization.
    data_kind : int
        Data acquisition mode flags (from DataKind enum).
    nw_id : int | None
        Network/roach ID when product corresponds to one interface file.
    """

    tag: Literal["raw_obs"] = "raw_obs"
    data_kind: int = 0
    nw_id: int | None = None


@dataclass
class ReducedObsMeta(DataProdMetaBase, ObsIdMixin):
    """Metadata for dp_reduced_obs.

    Attributes
    ----------
    tag : Literal["reduced_obs"]
        Discriminator for union type deserialization.
    reduction_method : str | None
        Reduction method used (inline, offline, etc.).
    calibration_version : str | None
        Calibration version identifier.
    processing_date : str | None
        ISO 8601 processing timestamp.
    quality_score : float | None
        Quality metric (0-1 scale).
    """

    tag: Literal["reduced_obs"] = "reduced_obs"
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
        Discriminator for union type deserialization.
    n_items : int
        Number of raw observations in group.
    group_type : str | None
        Calibration group type.
    date_range : tuple[str, str] | None
        Start and end dates of calibration data.
    """

    tag: Literal["cal_group"] = "cal_group"
    n_items: int = 0
    group_type: str | None = None
    date_range: tuple[str, str] | None = None


@dataclass
class DrivefitMeta(DataProdMetaBase, ObsIdMixin):
    """Metadata for dp_drivefit.

    Attributes
    ----------
    tag : Literal["drivefit"]
        Discriminator for union type deserialization.
    n_items : int
        Number of raw observations used.
    fit_method : str | None
        Fitting method/algorithm.
    convergence_status : str | None
        Fit convergence status.
    chi_squared : float | None
        Chi-squared goodness of fit.
    """

    tag: Literal["drivefit"] = "drivefit"
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
        Discriminator for union type deserialization.
    n_items : int
        Number of raw observations in group.
    focus_positions : list[float] | None
        Focus positions sampled.
    best_focus : float | None
        Optimal focus position.
    focus_metric : str | None
        Metric used (FWHM, Strehl, etc.).
    """

    tag: Literal["focus_group"] = "focus_group"
    n_items: int = 0
    focus_positions: list[float] | None = None
    best_focus: float | None = None
    focus_metric: str | None = None


@dataclass
class AstigGroupMeta(DataProdMetaBase, ObsIdMixin):
    """Metadata for dp_astig_group.

    Attributes
    ----------
    tag : Literal["astig_group"]
        Discriminator for union type deserialization.
    n_items : int
        Number of raw observations in group.
    astig_positions : list[tuple[float, float]] | None
        Astigmatism correction positions sampled (x, y).
    best_astig : tuple[float, float] | None
        Optimal astigmatism correction (x, y).
    astig_metric : str | None
        Metric used (FWHM, Strehl, etc.).
    """

    tag: Literal["astig_group"] = "astig_group"
    n_items: int = 0
    astig_positions: list[tuple[float, float]] | None = None
    best_astig: tuple[float, float] | None = None
    astig_metric: str | None = None


@dataclass
class OofGroupMeta(DataProdMetaBase, ObsIdMixin):
    """Metadata for dp_oof_group (Out-of-Focus holography).

    Attributes
    ----------
    tag : Literal["oof_group"]
        Discriminator for union type deserialization.
    n_items : int
        Number of raw observations in group.
    oof_positions : list[float] | None
        Out-of-focus positions sampled.
    surface_rms : float | None
        Measured surface RMS from holography.
    oof_metric : str | None
        Metric used for analysis.
    """

    tag: Literal["oof_group"] = "oof_group"
    n_items: int = 0
    oof_positions: list[float] | None = None
    surface_rms: float | None = None
    oof_metric: str | None = None


@dataclass
class NamedGroupMeta(DataProdMetaBase):
    """Metadata for dp_named_group.

    Attributes
    ----------
    tag : Literal["named_group"]
        Discriminator for union type deserialization.
    group_name : str
        User-defined group name.
    n_items : int
        Number of products in group.
    tags : list[str] | None
        Optional tags for categorization.
    owner : str | None
        Group owner/creator.
    notes : str | None
        Free-form notes.
    """

    tag: Literal["named_group"] = "named_group"
    group_name: str = ""
    n_items: int = 0
    tags: list[str] | None = None
    owner: str | None = None
    notes: str | None = None


# Union type for DataProd.meta (polymorphic, Literal discriminators)
AnyDataProdMeta = (
    RawObsMeta
    | ReducedObsMeta
    | CalGroupMeta
    | DrivefitMeta
    | FocusGroupMeta
    | AstigGroupMeta
    | OofGroupMeta
    | NamedGroupMeta
)


# ====================  Interface-Level Metadata  ====================


@dataclass
class InterfaceFileMeta:
    """Simple interface file metadata (nw_id and roach only).

    Lightweight struct for use in dagster asset metadata creation.
    NOT included in AnyDataProdMeta (DataProdSource uses AnyInterfaceMeta).

    Attributes
    ----------
    nw_id : int | None
        Network/roach ID.
    roach : int | None
        Roach board index (0-12).
    """

    nw_id: int | None = None
    roach: int | None = None


@dataclass
class RoachInterfaceMeta(ObsIdMixin, RoachMetaMixin):
    """Roach interface metadata for toltec0-12 interface files.

    Stored in DataProdSource.meta for roach interface files.

    Attributes
    ----------
    type : Literal["roach"]
        Type discriminator for adaptix union handling.
    data_kind : int | None
        DataKind flag value for this interface file.
    """

    type: Literal["roach"] = "roach"
    data_kind: int | None = None


@dataclass
class TelInterfaceMeta(ObsIdMixin, TelMetaMixin):
    """LMT telescope interface metadata for tel_toltec interface files.

    Stored in DataProdSource.meta for tel interface files.

    Attributes
    ----------
    type : Literal["tel"]
        Type discriminator for adaptix union handling.
    interface : Literal["tel_toltec"]
        Interface identifier.
    data_kind : int
        DataKind flag value (LmtTel).
    receiver : str
        Receiver name.
    instrument : str
        Instrument name.
    main_time : float
        Main beam time (seconds).
    ref_time : float
        Reference time (seconds).
    valid : bool
        Valid data flag.
    """

    type: Literal["tel"] = "tel"
    data_kind: int = field(default_factory=lambda: DataKind.LmtTel.value)
    interface: Literal["tel_toltec"] = "tel_toltec"
    receiver: str = "Toltec"
    instrument: str = "tel"
    main_time: float = 0.0
    ref_time: float = 0.0
    valid: bool = True


# Union type for DataProdSource.meta (roach OR tel interface)
AnyInterfaceMeta = TelInterfaceMeta | RoachInterfaceMeta


# ====================  Process Context  ====================


@dataclass
class ProcessContext:
    """Process metadata for DataProdAssoc.context field.

    Attributes
    ----------
    module : str | None
        Processing module name.
    version : str | None
        Processing version.
    config : dict[str, Any] | None
        Processing configuration.
    """

    module: str | None = None
    version: str | None = None
    config: dict[str, Any] | None = None


__all__ = [
    "_json_type",
    "_retort",
    "adaptix_json_type",
    "AnyDataProdMeta",
    "AnyInterfaceMeta",
    "AstigGroupMeta",
    "CalGroupMeta",
    "DataProdMetaBase",
    "DrivefitMeta",
    "FocusGroupMeta",
    "InterfaceFileMeta",
    "MetadataType",
    "NamedGroupMeta",
    "ObsIdMixin",
    "OofGroupMeta",
    "ProcessContext",
    "RawObsMeta",
    "ReducedObsMeta",
    "RoachInterfaceMeta",
    "RoachMetaMixin",
    "TelInterfaceMeta",
    "TelMetaMixin",
]

# Union type for all metadata (includes interface metadata and dict fallback)
MetadataType = AnyDataProdMeta | AnyInterfaceMeta | dict[str, Any]
