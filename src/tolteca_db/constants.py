"""Constants and enumerations for tolteca_db."""

from __future__ import annotations

from enum import Enum, Flag, auto

__all__ = [
    "AssocType",
    "BaseType",
    "DataProdAssocType",
    "DataProdType",
    "FlagSeverity",
    "ProductKind",
    "RAWAvailability",
    "ReducedStatus",
    "StorageRole",
    "TaskStatus",
    "ToltecDataKind",
]


class BaseType(str, Enum):
    """Product base types."""

    RAW_OBS = "raw_obs"
    REDUCED_OBS = "reduced_obs"
    INPUT_SET = "input_set"


class ProductKind(str, Enum):
    """Product kind discriminator for unified table."""

    RAW = "RAW"
    REDUCED = "REDUCED"


class ReducedStatus(str, Enum):
    """Lifecycle status for REDUCED products."""

    ACTIVE = "ACTIVE"
    SUPERSEDED = "SUPERSEDED"


class RAWAvailability(str, Enum):
    """Physical availability state for RAW products."""

    AVAILABLE = "AVAILABLE"
    MISSING = "MISSING"
    REMOTE = "REMOTE"
    STAGED = "STAGED"


class AssocType(str, Enum):
    """Association edge types for provenance graph."""

    PROCESS_EDGE = "process_edge"
    GROUP_MEMBER = "group_member"


class TaskStatus(str, Enum):
    """Reduction task status."""

    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    DONE = "DONE"
    ERROR = "ERROR"


class FlagSeverity(str, Enum):
    """Quality flag severity levels."""

    INFO = "INFO"
    WARN = "WARN"
    BLOCK = "BLOCK"
    CRITICAL = "CRITICAL"


class StorageRole(str, Enum):
    """Storage location role."""

    PRIMARY = "PRIMARY"
    MIRROR = "MIRROR"
    TEMP = "TEMP"


# TolTEC-Specific Data Product Types and Associations
# Established 2025-10-22 via analysis of tolteca_v1, tolteca_v2, tolteca_web


class DataProdType(str, Enum):
    """TolTEC data product types.
    
    Naming convention:
    - Observation-level types have _obs suffix (L0/L1)
    - Analysis-level types have no suffix (L2+)
    - Meta-level types for collections
    
    Reference: ADR-009 in design/architecture.md
    """

    # Observation-level (L0/L1)
    DP_RAW_OBS = "dp_raw_obs"           # All detector acquisitions (VnaSweep, TargSweep, Tune, RawTimeStream)
    DP_REDUCED_OBS = "dp_reduced_obs"   # Calibrated observations (inline or offline)

    # Analysis-level (L2+)
    DP_CAL_GROUP = "dp_cal_group"       # Calibration groupings for analysis
    DP_DRIVEFIT = "dp_drivefit"         # Detector characterization
    DP_FOCUS_GROUP = "dp_focus_group"   # Focus analysis groupings
    DP_MAP = "dp_map"                   # Science maps (future)
    DP_CATALOG = "dp_catalog"           # Source catalogs (future)

    # Meta-level
    DP_NAMED_GROUP = "dp_named_group"   # User-defined collections


class DataProdAssocType(str, Enum):
    """TolTEC data product association types.
    
    Defines relationship semantics in provenance graph.
    
    Reference: ADR-009 in design/architecture.md
    """

    # Calibration relationships (self-referencing on dp_raw_obs)
    DPA_RAW_OBS_CAL_OBS = "dpa_raw_obs_cal_obs"
    # "raw obs uses cal obs as calibration source"
    # Examples: RawTimeStream → Tune, Tune → VnaSweep

    # Reduction relationships
    DPA_REDUCED_OBS_RAW_OBS = "dpa_reduced_obs_raw_obs"
    # "reduced obs derived from raw obs"

    # Analysis relationships
    DPA_CAL_GROUP_RAW_OBS = "dpa_cal_group_raw_obs"
    # "cal group contains raw obs"

    DPA_DRIVEFIT_RAW_OBS = "dpa_drivefit_raw_obs"
    # "drivefit uses raw obs"

    DPA_FOCUS_GROUP_RAW_OBS = "dpa_focus_group_raw_obs"
    # "focus group contains raw obs"

    # Generic collection
    DPA_NAMED_GROUP_DATA_PROD = "dpa_named_group_data_prod"
    # "named group contains any data product"


class ToltecDataKind(Flag):
    """TolTEC data acquisition kinds (bitwise flags).
    
    Used to differentiate dp_raw_obs types via metadata without
    creating separate product types. Calibration is a relationship,
    not a type.
    
    Reference: tolteca_v2/toltec/types.py, ADR-009
    """

    # Sweep types (calibration modes)
    VnaSweep = auto()       # Vector Network Analyzer sweep (bootstrapping)
    TargetSweep = auto()    # Target sweep (refinement)
    Tune = auto()           # Tune sweep (fine adjustment)

    # Composite sweep category
    RawSweep = VnaSweep | TargetSweep | Tune

    # Nominal observation mode
    RawTimeStream = auto()  # Science timestream data
