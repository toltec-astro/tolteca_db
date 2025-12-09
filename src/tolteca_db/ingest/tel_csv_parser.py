"""Parser for LMT telescope metadata CSV files.

Parses lmtmc_toltec_metadata.csv into TelInterfaceMeta instances.
CSV contains telescope state during TolTEC observations (pointing, optics, conditions).
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator

from tolteca_db.models.metadata import TelInterfaceMeta

__all__ = ["TelCSVRow", "parse_tel_csv"]


@dataclass
class TelCSVRow:
    """Parsed row from LMT telescope metadata CSV.
    
    Attributes
    ----------
    tel_metadata : TelInterfaceMeta
        Complete tel interface metadata
    obsnum : int
        Observation number (extracted from ObsNum column)
    subobsnum : int
        Sub-observation number (extracted from ObsNum column)
    scannum : int
        Scan number (extracted from ObsNum column)
    """
    
    tel_metadata: TelInterfaceMeta
    obsnum: int
    subobsnum: int
    scannum: int
    
    @classmethod
    def from_csv_row(cls, row: dict[str, str]) -> TelCSVRow:
        """Parse CSV row into TelCSVRow.
        
        Parameters
        ----------
        row : dict[str, str]
            CSV row as dictionary (column name → value)
        
        Returns
        -------
        TelCSVRow
            Parsed tel metadata row
        
        Notes
        -----
        ObsNum format: "93026.0.1" → obsnum=93026, subobsnum=0, scannum=1
        """
        # Parse ObsNum.SubObsNum.ScanNum format
        obsnum_str = row["ObsNum"]
        parts = obsnum_str.split(".")
        obsnum = int(float(parts[0]))
        subobsnum = int(parts[1]) if len(parts) > 1 else 0
        scannum = int(parts[2]) if len(parts) > 2 else 1
        
        # Parse datetime
        obs_datetime = datetime.strptime(
            row["Date/Time [UT]"],
            "%Y-%m-%d %H:%M:%S",
        )
        
        # Parse Zernike coefficients (7 values)
        m1_zernike = [
            float(row[f"M1Zernike{i} [micron]"])
            for i in range(7)
        ]
        
        # Parse M2 offsets (X, Y, Z)
        m2_offset_mm = (
            float(row["M2XOffset [mm]"]),
            float(row["M2YOffset [mm]"]),
            float(row["M2ZOffset [mm]"]),
        )
        
        # Create TelInterfaceMeta
        tel_meta = TelInterfaceMeta(
            # Interface identifiers
            interface="tel_toltec",
            receiver="Toltec",
            instrument="tel",
            # Core identification (from ObsIdMixin)
            obsnum=obsnum,
            subobsnum=subobsnum,
            scannum=scannum,
            master="tcs",
            # Observation metadata (from TelMetaMixin)
            obs_datetime=obs_datetime,
            source_name=row["SourceName"],
            obs_goal=row["ObsGoal"],
            # Project and program (from TelMetaMixin)
            project_id=row["ProjectId"],
            obs_pgm=row["ObsPgm"],
            # Timing (TelInterfaceMeta-specific + TelMetaMixin)
            integration_time=float(row["IntegrationTime"]),
            main_time=float(row["MainTime"]),
            ref_time=float(row["RefTime"]),
            # Pointing (from TelMetaMixin)
            az_deg=float(row["Az [deg]"]),
            el_deg=float(row["El [deg]"]),
            user_az_offset_arcsec=float(row['UserAzOffset ["]']),
            user_el_offset_arcsec=float(row['UserElOffset ["]']),
            paddle_az_offset_arcsec=float(row['PaddleAzOffset ["]']),
            paddle_el_offset_arcsec=float(row['PaddleElOffset ["]']),
            # Optics (from TelMetaMixin)
            m1_zernike=m1_zernike,
            m2_offset_mm=m2_offset_mm,
            # Atmospheric conditions (from TelMetaMixin)
            tau=float(row["Tau"]),
            crane_in_beam=bool(int(row["CraneInBeam"])),
            # Validation
            valid=bool(int(row["Valid"])),
        )
        
        return cls(
            tel_metadata=tel_meta,
            obsnum=obsnum,
            subobsnum=subobsnum,
            scannum=scannum,
        )


def parse_tel_csv(csv_path: Path | str) -> Iterator[TelCSVRow]:
    """Parse LMT telescope metadata CSV file.
    
    Parameters
    ----------
    csv_path : Path | str
        Path to lmtmc_toltec_metadata.csv file
    
    Yields
    ------
    TelCSVRow
        Parsed tel metadata rows
    
    Examples
    --------
    >>> for row in parse_tel_csv("run/lmtmc_toltec_metadata.csv"):
    ...     print(f"Obs {row.obsnum}: tau={row.tel_metadata.tau:.3f}")
    
    Notes
    -----
    Skips rows that fail to parse (logs error and continues).
    Empty or malformed rows are silently skipped.
    """
    csv_path = Path(csv_path)
    
    with csv_path.open("r") as f:
        reader = csv.DictReader(f)
        for row_num, row in enumerate(reader, start=2):  # Start at 2 (header is line 1)
            try:
                yield TelCSVRow.from_csv_row(row)
            except (ValueError, KeyError) as e:
                print(f"Warning: Failed to parse row {row_num}: {e}")
                continue
