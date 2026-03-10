"""Constants for tolteca_db.

Defines TolTEC instrument constants, master types, data kinds, and
data product type enumerations used throughout the database layer.

Enum convention: UPPERCASE member names produce lowercase values via
``StrEnum + auto()``. E.g. ``ReducedStatus.ACTIVE == "active"``.
"""

from __future__ import annotations

from enum import Flag, StrEnum, auto
from typing import ClassVar, Literal

__all__ = [
    "DataKind",
    "DataProdAssocType",
    "DataProdType",
    "MasterType",
    "ReducedStatus",
    "StorageRole",
    "ToltecDataKind",
    "ToltecInfo",
]


class MasterType(StrEnum):
    """TolTEC master controller types."""

    TCS = auto()
    """The Telescope Control System."""

    ICS = auto()
    """The Instrument Control System."""

    CLIP = auto()
    """The ROACH manager (CLIP)."""


type MasterNameT = Literal["tcs", "ics", "clip"]
"""TolTEC master controller name literal type."""


class ReducedStatus(StrEnum):
    """Lifecycle status for reduced data products."""

    ACTIVE = auto()
    SUPERSEDED = auto()


class StorageRole(StrEnum):
    """Storage location role."""

    PRIMARY = auto()
    MIRROR = auto()
    TEMP = auto()


class DataProdType(StrEnum):
    """TolTEC data product types.

    Observation-level types (L0/L1) have the ``_obs`` suffix.
    Analysis-level types (L2+) describe the product kind.
    Meta-level types represent collections.

    Reference: design/architecture.md
    """

    # Observation-level (L0/L1)
    DP_RAW_OBS = auto()
    """All detector acquisitions (VnaSweep, TargSweep, Tune, RawTimeStream)."""

    DP_REDUCED_OBS = auto()
    """Calibrated observations (inline or offline reduction)."""

    # Analysis-level (L2+)
    DP_CAL_GROUP = auto()
    """Calibration groupings for analysis."""

    DP_DRIVEFIT = auto()
    """Detector characterisation."""

    DP_FOCUS_GROUP = auto()
    """Focus analysis groupings."""

    DP_ASTIG_GROUP = auto()
    """Astigmatism analysis groupings."""

    DP_OOF_GROUP = auto()
    """Out-of-focus holography groupings."""

    DP_MAP = auto()
    """Science maps (future)."""

    DP_CATALOG = auto()
    """Source catalogs (future)."""

    # Meta-level
    DP_NAMED_GROUP = auto()
    """User-defined collections."""


class DataProdAssocType(StrEnum):
    """TolTEC data product association types.

    Defines relationship semantics in the provenance graph.

    Reference: design/architecture.md
    """

    # Calibration relationships (self-referencing on dp_raw_obs)
    DPA_RAW_OBS_CAL_OBS = auto()
    """Raw obs uses cal obs as calibration source.

    Examples: RawTimeStream → Tune, Tune → VnaSweep.
    """

    # Reduction relationships
    DPA_REDUCED_OBS_RAW_OBS = auto()
    """Reduced obs derived from raw obs."""

    # Analysis relationships
    DPA_CAL_GROUP_RAW_OBS = auto()
    """Cal group contains raw obs."""

    DPA_DRIVEFIT_RAW_OBS = auto()
    """Drivefit uses raw obs."""

    DPA_FOCUS_GROUP_RAW_OBS = auto()
    """Focus group contains raw obs."""

    DPA_ASTIG_GROUP_RAW_OBS = auto()
    """Astigmatism group contains raw obs."""

    DPA_OOF_GROUP_RAW_OBS = auto()
    """OOF group contains raw obs."""

    # Generic collection
    DPA_NAMED_GROUP_DATA_PROD = auto()
    """Named group contains any data product."""


class DataKind(Flag):
    """TolTEC raw observation data acquisition kinds.

    Bitwise flags used to differentiate observation types within the
    ``dp_raw_obs`` product type without creating separate table rows.
    Calibration is a *relationship* (association), not a type.

    Reference: tolteca_datamodels/toltec/types.py
    """

    # Sweep types (KIDs calibration modes)
    VnaSweep = auto()
    """Full-range VNA sweep (bootstrapping)."""

    TargetSweep = auto()
    """Target sweep on a list of frequencies (refinement)."""

    Tune = auto()
    """Pack of back-to-back target sweeps (fine adjustment)."""

    RawSweep = VnaSweep | TargetSweep | Tune
    """All sweep kinds combined."""

    # Science observation mode
    RawTimeStream = auto()
    """Continuous capture at probe tones (science mode)."""

    RawKidsData = RawSweep | RawTimeStream
    """All raw KIDs data kinds."""

    # Reduced KIDs data
    D21 = auto()
    """D21 processed sweep data."""

    ReducedVnaSweep = auto()
    ReducedTargetSweep = auto()
    ReducedSweep = ReducedVnaSweep | ReducedTargetSweep
    SolvedTimeStream = auto()
    ReducedKidsData = D21 | ReducedSweep | SolvedTimeStream

    # Telescope / housekeeping
    LmtTel = auto()
    """LMT telescope interface data (tel_toltec)."""

    LmtTel2 = auto()
    """Supplementary LMT telescope data at original sample rate."""

    Unknown = auto()
    """Unknown data kind."""


# Backward-compatibility alias used by v2.5 code still in tree
ToltecDataKind = DataKind


class ToltecInfo:
    """TolTEC instrument information and lookup tables.

    All attributes are :data:`~typing.ClassVar`; do not instantiate.
    """

    masters: ClassVar[list[MasterNameT]] = [m.value for m in MasterType]  # type: ignore[misc]

    # Roach (ROACH board) interfaces
    roaches: ClassVar[list[int]] = list(range(13))
    roach_interface: ClassVar[dict[int, str]] = {r: f"toltec{r}" for r in roaches}
    interface_roach: ClassVar[dict[str, int]] = {v: k for k, v in roach_interface.items()}
    roach_interfaces: ClassVar[list[str]] = list(roach_interface.values())

    # ICS-controlled interfaces (roach boards + HWPR + ICS status files)
    ics_interfaces: ClassVar[list[str]] = [*roach_interfaces, "hwpr", "ics0", "ics1"]

    # TCS-controlled interfaces (telescope metadata)
    tcs_interfaces: ClassVar[list[str]] = ["tel_toltec", "tel_toltec2"]

    # All known interfaces
    interfaces: ClassVar[list[str]] = [*ics_interfaces, *tcs_interfaces]

    # Interface → master mapping
    interface_master: ClassVar[dict[str, MasterType]] = {
        **{iface: MasterType.ICS for iface in ics_interfaces},
        **{iface: MasterType.TCS for iface in tcs_interfaces},
    }

    # Valid obsnum range (from TolTEC operational history)
    obsnum_min: ClassVar[int] = 1
    obsnum_max: ClassVar[int] = 999_999

    # Roach → array mapping
    interface_array_name: ClassVar[dict[str, str]] = {
        "toltec0": "a1100",
        "toltec1": "a1100",
        "toltec2": "a1100",
        "toltec3": "a1100",
        "toltec4": "a1100",
        "toltec5": "a1100",
        "toltec6": "a1100",
        "toltec7": "a1400",
        "toltec8": "a1400",
        "toltec9": "a1400",
        "toltec10": "a1400",
        "toltec11": "a2000",
        "toltec12": "a2000",
    }
