"""File scanner for TolTEC data files.

Scans directories, parses filenames, and extracts metadata for database ingestion.
Supports tolteca_v2 filename patterns.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator

from tolteca_db.api.obs import SourceInfoModel
from tolteca_db.constants import ToltecDataKind

__all__ = ["FileScanner", "guess_info_from_file", "ParsedFileInfo"]


@dataclass
class ParsedFileInfo:
    """Parsed information from TolTEC filename.
    
    Attributes
    ----------
    filepath : Path
        Full path to the file
    interface : str
        Interface ID (toltec, hwp, tel_toltec, etc.)
    roach : int | None
        Roach number (0-12 for detector data)
    obsnum : int
        Observation number
    subobsnum : int
        Sub-observation number
    scannum : int
        Scan number
    file_suffix : str | None
        File type suffix (timestream, targsweep, etc.)
    file_ext : str
        File extension (.nc, .fits, etc.)
    data_kind : ToltecDataKind | None
        Inferred data kind from suffix
    """
    
    filepath: Path
    interface: str
    roach: int | None
    obsnum: int
    subobsnum: int
    scannum: int
    file_suffix: str | None
    file_ext: str
    data_kind: ToltecDataKind | None


# TolTEC filename patterns
# Format: interface_obsnum_subobsnum_scannum[_timestamp][_suffix].ext
# Examples:
#   toltec0_123456_001_0000_timestream.nc
#   toltec0_113533_000_0001_2024_03_19_05_27_52_targsweep.nc
#   hwp_123456_001_0000.nc
#   tel_toltec_123456_001_0000.nc

TOLTEC_FILENAME_PATTERN = re.compile(
    r"(?P<interface>toltec\d+|hwp|tel_toltec|toltec)"
    r"_(?P<obsnum>\d+)"
    r"_(?P<subobsnum>\d+)"
    r"_(?P<scannum>\d+)"
    r"(?:_\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2})?"  # Optional timestamp
    r"(?:_(?P<suffix>\w+))?"
    r"\.(?P<ext>\w+)$"
)


def guess_info_from_file(filepath: str | Path) -> ParsedFileInfo | None:
    """Parse TolTEC filename and extract metadata.
    
    Supports tolteca_v2 filename patterns for raw observation files.
    
    Parameters
    ----------
    filepath : str | Path
        Path to the file
    
    Returns
    -------
    ParsedFileInfo | None
        Parsed information if filename matches pattern, None otherwise
    
    Examples
    --------
    >>> info = guess_info_from_file("toltec0_123456_001_0000_timestream.nc")
    >>> info.interface
    'toltec0'
    >>> info.obsnum
    123456
    >>> info.roach
    0
    
    >>> info = guess_info_from_file("hwp_123456_001_0000.nc")
    >>> info.interface
    'hwp'
    >>> info.roach
    None
    """
    path = Path(filepath)
    filename = path.name
    
    # Match filename pattern
    match = TOLTEC_FILENAME_PATTERN.match(filename)
    if not match:
        return None
    
    # Extract fields
    interface = match.group("interface")
    obsnum = int(match.group("obsnum"))
    subobsnum = int(match.group("subobsnum"))
    scannum = int(match.group("scannum"))
    file_suffix = match.group("suffix")
    file_ext = match.group("ext")
    
    # Extract roach number from interface for detector data
    roach = None
    if interface.startswith("toltec") and len(interface) > 6:
        try:
            roach = int(interface[6:])  # toltec0 -> 0, toltec12 -> 12
        except ValueError:
            pass
    
    # Infer data kind from suffix
    data_kind = _infer_data_kind(file_suffix)
    
    return ParsedFileInfo(
        filepath=path,
        interface=interface,
        roach=roach,
        obsnum=obsnum,
        subobsnum=subobsnum,
        scannum=scannum,
        file_suffix=file_suffix,
        file_ext=file_ext,
        data_kind=data_kind,
    )


def _infer_data_kind(file_suffix: str | None) -> ToltecDataKind | None:
    """Infer ToltecDataKind from file suffix.
    
    Parameters
    ----------
    file_suffix : str | None
        File suffix (timestream, targsweep, etc.)
    
    Returns
    -------
    ToltecDataKind | None
        Inferred data kind, None if cannot determine
    """
    if not file_suffix:
        return None
    
    suffix_lower = file_suffix.lower()
    
    # Map suffix to data kind
    suffix_map = {
        "timestream": ToltecDataKind.RawTimeStream,
        "targetsweep": ToltecDataKind.TargetSweep,
        "targsweep": ToltecDataKind.TargetSweep,
        "vnasweep": ToltecDataKind.VnaSweep,
        "tune": ToltecDataKind.Tune,
    }
    
    return suffix_map.get(suffix_lower)


class FileScanner:
    """Scanner for TolTEC data directories.
    
    Recursively scans directories for TolTEC data files and parses metadata.
    
    Parameters
    ----------
    root_path : str | Path
        Root directory to scan
    recursive : bool, optional
        Scan subdirectories recursively, by default True
    pattern : str, optional
        Glob pattern for files to scan, by default "*.nc"
    
    Examples
    --------
    >>> scanner = FileScanner("/data/lmt/toltec/raw")
    >>> for file_info in scanner.scan():
    ...     print(f"{file_info.obsnum}: {file_info.filepath}")
    """
    
    def __init__(
        self,
        root_path: str | Path,
        recursive: bool = True,
        pattern: str = "*.nc",
    ):
        self.root_path = Path(root_path)
        self.recursive = recursive
        self.pattern = pattern
    
    def scan(self) -> Iterator[ParsedFileInfo]:
        """Scan directory and yield parsed file information.
        
        Yields
        ------
        ParsedFileInfo
            Parsed information for each matched file
        """
        if self.recursive:
            file_iter = self.root_path.rglob(self.pattern)
        else:
            file_iter = self.root_path.glob(self.pattern)
        
        for filepath in file_iter:
            # Parse filename
            file_info = guess_info_from_file(filepath)
            
            # Only yield if parsing succeeded
            if file_info is not None:
                yield file_info
    
    def scan_to_list(self) -> list[ParsedFileInfo]:
        """Scan directory and return list of all parsed files.
        
        Returns
        -------
        list[ParsedFileInfo]
            List of all successfully parsed files
        """
        return list(self.scan())
    
    def to_source_info_models(self) -> Iterator[SourceInfoModel]:
        """Scan and convert to SourceInfoModel instances.
        
        Yields
        ------
        SourceInfoModel
            Source info model for each file
        """
        for file_info in self.scan():
            # Get file timestamp
            file_timestamp = datetime.fromtimestamp(
                file_info.filepath.stat().st_mtime
            )
            
            # Build UIDs
            uid_obs = str(file_info.obsnum)
            uid_raw_obs = f"{file_info.obsnum}-{file_info.subobsnum}-{file_info.scannum}"
            uid_raw_obs_file = f"{uid_raw_obs}-{file_info.interface}"
            
            yield SourceInfoModel(
                source=f"file://{file_info.filepath.absolute()}",
                interface=file_info.interface,
                roach=file_info.roach,
                obsnum=file_info.obsnum,
                subobsnum=file_info.subobsnum,
                scannum=file_info.scannum,
                file_timestamp=file_timestamp,
                file_suffix=file_info.file_suffix,
                file_ext=file_info.file_ext,
                uid_obs=uid_obs,
                uid_raw_obs=uid_raw_obs,
                uid_raw_obs_file=uid_raw_obs_file,
            )
