"""Constants for tolteca_db.

Defines TolTEC instrument constants, master types, data kinds, and
data product type enumerations used throughout the database layer.
"""

from __future__ import annotations

from enum import Flag, StrEnum, auto
from typing import ClassVar, Literal

__all__ = [
    "DataKind",
    "DataProdAssocType",
    "DataProdType",
    "FlagSeverity",
    "MasterType",
    "ReducedStatus",
    "StorageRole",
    "TaskStatus",
    "ToltecDataKind",
    "ToltecInfo",
]


class MasterType(StrEnum):
    """TolTEC master controller types."""

    tcs = auto()
    """The Telescope Control System."""

    ics = auto()
    """The Instrument Control System."""

    clip = auto()
    """The ROACH manager (CLIP)."""


type MasterNameT = Literal["tcs", "ics", "clip"]
"""TolTEC master controller name literal type."""


class ReducedStatus(StrEnum):
    """Lifecycle status for reduced data products."""

    active = auto()
    superseded = auto()


class TaskStatus(StrEnum):
    """Reduction task status."""

    queued = auto()
    running = auto()
    done = auto()
    error = auto()


class FlagSeverity(StrEnum):
    """Quality flag severity levels."""

    info = auto()
    warn = auto()
    block = auto()
    critical = auto()


class StorageRole(StrEnum):
    """Storage location role."""

    primary = auto()
    mirror = auto()
    temp = auto()


class DataProdType(StrEnum):
    """TolTEC data product types.

    Observation-level types (L0/L1) have the ``_obs`` suffix.
    Analysis-level types (L2+) describe the product kind.
    Meta-level types represent collections.

    Reference: design/architecture.md
    """

    # Observation-level (L0/L1)
    dp_raw_obs = auto()
    """All detector acquisitions (VnaSweep, TargSweep, Tune, RawTimeStream)."""

    dp_reduced_obs = auto()
    """Calibrated observations (inline or offline reduction)."""

    # Analysis-level (L2+)
    dp_cal_group = auto()
    """Calibration groupings for analysis."""

    dp_drivefit = auto()
    """Detector characterisation."""

    dp_focus_group = auto()
    """Focus analysis groupings."""

    dp_astig_group = auto()
    """Astigmatism analysis groupings."""

    dp_oof_group = auto()
    """Out-of-focus holography groupings."""

    dp_map = auto()
    """Science maps (future)."""

    dp_catalog = auto()
    """Source catalogs (future)."""

    # Meta-level
    dp_named_group = auto()
    """User-defined collections."""


class DataProdAssocType(StrEnum):
    """TolTEC data product association types.

    Defines relationship semantics in the provenance graph.

    Reference: design/architecture.md
    """

    # Calibration relationships (self-referencing on dp_raw_obs)
    dpa_raw_obs_cal_obs = auto()
    """Raw obs uses cal obs as calibration source.

    Examples: RawTimeStream → Tune, Tune → VnaSweep.
    """

    # Reduction relationships
    dpa_reduced_obs_raw_obs = auto()
    """Reduced obs derived from raw obs."""

    # Analysis relationships
    dpa_cal_group_raw_obs = auto()
    """Cal group contains raw obs."""

    dpa_drivefit_raw_obs = auto()
    """Drivefit uses raw obs."""

    dpa_focus_group_raw_obs = auto()
    """Focus group contains raw obs."""

    dpa_astig_group_raw_obs = auto()
    """Astigmatism group contains raw obs."""

    dpa_oof_group_raw_obs = auto()
    """OOF group contains raw obs."""

    # Generic collection
    dpa_named_group_data_prod = auto()
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
        **{iface: MasterType.ics for iface in ics_interfaces},
        **{iface: MasterType.tcs for iface in tcs_interfaces},
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

