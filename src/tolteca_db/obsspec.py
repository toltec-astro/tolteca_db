"""Canonical observation specification (ObsSpec) for TolTEC data products.

An :class:`ObsSpec` uniquely identifies a raw observation as the quartet
``(master, obsnum, subobsnum, scannum)``.  It serialises to a human-readable
primary-key string ``"{master}-{obsnum}-{subobsnum}-{scannum}"`` used as the
``uid`` for raw-observation rows in the database.

The module intentionally has *no* external dependencies beyond the standard
library so it can be imported cheaply by any code that only needs the UID.
"""

from __future__ import annotations

import dataclasses
import re
from pathlib import Path
from typing import TYPE_CHECKING

from tolteca_db.constants import MasterType, ToltecInfo

if TYPE_CHECKING:
    pass

__all__ = [
    "ObsSpec",
    "ObsSpecError",
]

# ---------------------------------------------------------------------------
# Filename patterns for from_path()
#
# Each entry is (compiled-regex, inferred-master).
# The regex must capture three groups: (obsnum, subobsnum, scannum).
# The stem of the *filename* (not the full path) is matched.
# ---------------------------------------------------------------------------
_PATH_PATTERNS: list[tuple[re.Pattern[str], MasterType]] = [
    # toltecN_OBSNUM_SUBOBS_SCAN[_suffix].ext  (N = roach index 0-12)
    (re.compile(r"^toltec\d+_(\d+)_(\d+)_(\d+)"), MasterType.ics),
    # icsN_OBSNUM_SUBOBS_SCAN[_suffix].ext
    (re.compile(r"^ics\d*_(\d+)_(\d+)_(\d+)"), MasterType.ics),
    # hwpr_OBSNUM_SUBOBS_SCAN[_suffix].ext
    (re.compile(r"^hwpr_(\d+)_(\d+)_(\d+)"), MasterType.ics),
    # tel_toltec[N]_OBSNUM_SUBOBS_SCAN[_suffix].ext
    (re.compile(r"^tel_toltec\d*_(\d+)_(\d+)_(\d+)"), MasterType.tcs),
    # clip_OBSNUM_SUBOBS_SCAN[_suffix].ext
    (re.compile(r"^clip_(\d+)_(\d+)_(\d+)"), MasterType.clip),
]


class ObsSpecError(ValueError):
    """Raised when an :class:`ObsSpec` cannot be parsed or constructed."""


@dataclasses.dataclass(frozen=True, order=True, slots=True)
class ObsSpec:
    """Immutable observation quartet: ``(master, obsnum, subobsnum, scannum)``.

    :class:`ObsSpec` is a value object.  Two instances with identical fields
    compare equal, hash identically, and can be used as dict keys or set
    members.

    Parameters
    ----------
    master : str
        Master controller name — one of ``"tcs"``, ``"ics"``, ``"clip"``.
    obsnum : int
        Observation number.
    subobsnum : int
        Sub-observation number (repeat/dither index).
    scannum : int
        Scan number within a sub-observation.

    Examples
    --------
    >>> obs = ObsSpec(master="ics", obsnum=18596, subobsnum=32, scannum=0)
    >>> obs.uid
    'ics-18596-32-0'
    >>> str(obs)
    'ics-18596-32-0'
    >>> ObsSpec.parse("ics-18596-32-0") == obs
    True
    """

    master: str
    obsnum: int
    subobsnum: int
    scannum: int

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def __post_init__(self) -> None:
        if self.master not in ToltecInfo.masters:
            msg = (
                f"Unknown master {self.master!r}; "
                f"must be one of {ToltecInfo.masters}"
            )
            raise ObsSpecError(msg)
        if not (ToltecInfo.obsnum_min <= self.obsnum <= ToltecInfo.obsnum_max):
            msg = (
                f"obsnum {self.obsnum} is outside valid range "
                f"[{ToltecInfo.obsnum_min}, {ToltecInfo.obsnum_max}]"
            )
            raise ObsSpecError(msg)

    # ------------------------------------------------------------------
    # String representation
    # ------------------------------------------------------------------

    @property
    def uid(self) -> str:
        """Human-readable primary key: ``"{master}-{obsnum}-{subobsnum}-{scannum}"``."""
        return f"{self.master}-{self.obsnum}-{self.subobsnum}-{self.scannum}"

    def __str__(self) -> str:
        return self.uid

    def __repr__(self) -> str:
        return (
            f"ObsSpec(master={self.master!r}, obsnum={self.obsnum}, "
            f"subobsnum={self.subobsnum}, scannum={self.scannum})"
        )

    # ------------------------------------------------------------------
    # Constructors
    # ------------------------------------------------------------------

    @classmethod
    def parse(cls, s: str) -> ObsSpec:
        """Parse the canonical UID string ``"{master}-{obsnum}-{subobsnum}-{scannum}"``.

        Parameters
        ----------
        s : str
            String of the form ``"{master}-{obsnum}-{subobsnum}-{scannum}"``.

        Returns
        -------
        ObsSpec
            Parsed instance.

        Raises
        ------
        ObsSpecError
            If the string does not contain exactly four dash-separated parts,
            if the master is unknown, or if any numeric field is non-integer.

        Examples
        --------
        >>> ObsSpec.parse("ics-18596-32-0")
        ObsSpec(master='ics', obsnum=18596, subobsnum=32, scannum=0)
        >>> ObsSpec.parse("tcs-18596-0-1")
        ObsSpec(master='tcs', obsnum=18596, subobsnum=0, scannum=1)
        """
        parts = s.strip().split("-")
        if len(parts) != 4:  # noqa: PLR2004
            msg = (
                f"Expected 4 dash-separated parts "
                f"'{{master}}-{{obsnum}}-{{subobsnum}}-{{scannum}}', "
                f"got {len(parts)} in {s!r}"
            )
            raise ObsSpecError(msg)
        master, obsnum_s, subobsnum_s, scannum_s = parts
        try:
            return cls(
                master=master,
                obsnum=int(obsnum_s),
                subobsnum=int(subobsnum_s),
                scannum=int(scannum_s),
            )
        except ValueError as exc:
            msg = f"Invalid ObsSpec string {s!r}: {exc}"
            raise ObsSpecError(msg) from exc

    @classmethod
    def from_path(
        cls,
        path: str | Path,
        master: str | None = None,
    ) -> ObsSpec | None:
        """Extract an :class:`ObsSpec` from a TolTEC data file path.

        The master controller is inferred from the filename prefix unless
        *master* is supplied explicitly.

        Supported filename prefixes
        ---------------------------
        * ``toltecN_…``  → master ``"ics"``
        * ``icsN_…``     → master ``"ics"``
        * ``hwpr_…``     → master ``"ics"``
        * ``tel_toltec[N]_…`` → master ``"tcs"``
        * ``clip_…``     → master ``"clip"``

        Parameters
        ----------
        path : str or Path
            File path.  Only the *filename* (basename) is examined.
        master : str, optional
            Override the inferred master.  Validated against
            :attr:`~tolteca_db.constants.ToltecInfo.masters`.

        Returns
        -------
        ObsSpec or None
            Parsed instance, or ``None`` if the filename does not match
            any known TolTEC pattern.

        Raises
        ------
        ObsSpecError
            If *master* is supplied but is not a recognised master name.

        Examples
        --------
        >>> ObsSpec.from_path("toltec0_018596_032_0001_targsweep.nc")
        ObsSpec(master='ics', obsnum=18596, subobsnum=32, scannum=1)
        >>> ObsSpec.from_path("tel_toltec_018596_032_0001.nc")
        ObsSpec(master='tcs', obsnum=18596, subobsnum=32, scannum=1)
        >>> ObsSpec.from_path("unknown_file.fits") is None
        True
        """
        if master is not None and master not in ToltecInfo.masters:
            msg = (
                f"Unknown master override {master!r}; "
                f"must be one of {ToltecInfo.masters}"
            )
            raise ObsSpecError(msg)

        stem = Path(path).name  # basename only; ignore directory components
        for pattern, inferred_master in _PATH_PATTERNS:
            m = pattern.match(stem)
            if m:
                resolved_master = master if master is not None else str(inferred_master)
                return cls(
                    master=resolved_master,
                    obsnum=int(m.group(1)),
                    subobsnum=int(m.group(2)),
                    scannum=int(m.group(3)),
                )
        return None
