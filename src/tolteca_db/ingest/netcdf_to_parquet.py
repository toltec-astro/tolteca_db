"""TolTEC netCDF to Parquet conversion for raw observation data.

This module handles conversion of TolTEC netCDF files to partitioned Parquet format:
- Wide format: (time, detector_columns) matching netCDF structure
- Hive partitioning: master={m}/obsnum={o}/subobsnum={s}/scannum={n}/roach_index={r}/
- Metadata summaries: aggregated info per dp_raw_obs quartet
- Sweep handling: LOFreq defines sweep axis for vnasweep/targsweep/tune files

Alignment with tolteca_v2:
- SourceInfoModel patterns for filename parsing and validation
- NcFileIO patterns for netCDF header extraction
- ORM schema: quartet (master, obsnum, subobsnum, scannum) uniqueness
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import xarray as xr


# Master value mapping (from Header.Toltec.Master)
MASTER_MAP = {
    0: "tcs",  # TCS master
    1: "ics",  # ICS master (if exists)
}


@dataclass
class SweepInfo:
    """Information about sweep structure in netCDF file."""

    n_sweeps: int
    n_samples_per_sweep: int | None
    lofreq_unique_values: int
    lofreq_change_points: int
    sweep_type: Literal["single", "multi"]


@dataclass
class SourceInfo:
    """File source information (filename-based metadata).

    This follows tolteca_v2 SourceInfoModel pattern for filename parsing.
    """

    interface: str  # "toltec0" - "toltec12"
    roach: int  # 0-12
    obsnum: int
    subobsnum: int
    scannum: int
    file_timestamp: datetime
    file_suffix: str  # "vnasweep", "tune", etc.
    file_ext: str  # ".nc"
    source_path: Path

    @property
    def uid_raw_obs(self) -> str:
        """Unique identifier for raw_obs (triplet without master)."""
        return f"{self.obsnum}-{self.subobsnum}-{self.scannum}"


@dataclass
class DataProdInfo:
    """Data product information (header-based metadata).

    This is extracted from netCDF headers and defines dp_raw_obs quartet.
    """

    master: str  # "tcs", "ics", etc.
    obsnum: int
    subobsnum: int
    scannum: int
    roach_index: int
    nw_id: int  # Network ID (same as roach_index)

    @property
    def uid_dp_raw_obs(self) -> str:
        """Unique identifier for dp_raw_obs (quartet with master)."""
        return f"{self.master}_{self.obsnum}_{self.subobsnum}_{self.scannum}"


@dataclass
class FileMetadata:
    """Complete metadata for a TolTEC netCDF file.

    Combines:
    - SourceInfo: filename-based metadata
    - DataProdInfo: header-based metadata for dp_raw_obs
    - TimeStreamInfo: data dimensions and timing
    - SweepInfo: sweep structure (if applicable)
    """

    source: SourceInfo
    data_prod: DataProdInfo
    file_type: str  # "vnasweep", "tune", etc.
    n_samples: int
    n_detectors: int
    t_start: float
    t_end: float
    source_path: str
    sweep_info: SweepInfo | None


def parse_filename(filepath: Path) -> SourceInfo:
    """Parse TolTEC filename to extract source metadata.

    Follows tolteca_v2 SourceInfoModel regex pattern:
    {interface}_{obsnum}_{subobsnum}_{scannum}_{timestamp}_{suffix}.{ext}

    Parameters
    ----------
    filepath : Path
        Path to netCDF file

    Returns
    -------
    SourceInfo
        Parsed filename metadata

    Examples
    --------
    >>> parse_filename(Path("toltec10_113515_000_0001_2024_03_19_03_45_17_vnasweep.nc"))
    SourceInfo(interface='toltec10', roach=10, obsnum=113515, subobsnum=0, scannum=1, ...)
    """
    # tolteca_v2 regex pattern
    pattern = (
        r"^(?P<interface>toltec(?P<roach>\d+))_"
        r"(?P<obsnum>\d+)_"
        r"(?P<subobsnum>\d+)_"
        r"(?P<scannum>\d+)_"
        r"(?P<file_timestamp>\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2})_"
        r"(?P<file_suffix>\w+)"
        r"(?P<file_ext>\.nc)$"
    )

    match = re.match(pattern, filepath.name)
    if not match:
        raise ValueError(f"Cannot parse filename: {filepath.name}")

    d = match.groupdict()

    # Parse timestamp
    ts_str = d["file_timestamp"].replace("_", "")
    file_timestamp = datetime.strptime(ts_str, "%Y%m%d%H%M%S")

    return SourceInfo(
        interface=d["interface"],
        roach=int(d["roach"]),
        obsnum=int(d["obsnum"]),
        subobsnum=int(d["subobsnum"]),
        scannum=int(d["scannum"]),
        file_timestamp=file_timestamp,
        file_suffix=d["file_suffix"],
        file_ext=d["file_ext"],
        source_path=filepath,
    )


def extract_dataprod_from_header(ds: xr.Dataset) -> DataProdInfo:
    """Extract data product metadata from netCDF headers.

    Follows tolteca_v2 NcFileIO NcNodeMapper pattern:
    - Header.Toltec.Master → master (int → str mapping)
    - Header.Toltec.ObsNum → obsnum
    - Header.Toltec.SubObsNum → subobsnum
    - Header.Toltec.ScanNum → scannum
    - Header.Toltec.RoachIndex → roach_index

    Parameters
    ----------
    ds : xr.Dataset
        Open xarray Dataset

    Returns
    -------
    DataProdInfo
        Data product quartet metadata
    """
    # Extract quartet from headers (tolteca_v2 NcNodeMapper paths)
    master_int = int(ds["Header.Toltec.Master"].values)
    master = MASTER_MAP.get(master_int, f"master_{master_int}")
    obsnum = int(ds["Header.Toltec.ObsNum"].values)
    subobsnum = int(ds["Header.Toltec.SubObsNum"].values)
    scannum = int(ds["Header.Toltec.ScanNum"].values)
    roach_index = int(ds["Header.Toltec.RoachIndex"].values)

    return DataProdInfo(
        master=master,
        obsnum=obsnum,
        subobsnum=subobsnum,
        scannum=scannum,
        roach_index=roach_index,
        nw_id=roach_index,  # Network ID same as roach_index
    )


def analyze_sweep_structure(ds: xr.Dataset) -> SweepInfo | None:
    """Analyze LOFreq to determine sweep structure.

    Parameters
    ----------
    ds : xr.Dataset
        Open xarray Dataset

    Returns
    -------
    SweepInfo | None
        Sweep information if LOFreq present, else None
    """
    if "Data.Toltec.LoFreq" not in ds.data_vars:
        return None

    lofreq = ds["Data.Toltec.LoFreq"].values
    unique_vals = np.unique(lofreq)
    changes = np.where(np.diff(lofreq) != 0)[0] + 1

    # Determine sweep type
    n_sweeps = int(ds.dims.get("numSweeps", 1))
    sweep_type = "multi" if n_sweeps > 1 else "single"

    # For multi-sweep (tune), estimate samples per sweep
    n_samples_per_sweep = None
    if sweep_type == "multi" and len(changes) > 0:
        # Changes should occur at sweep boundaries
        # Estimate from first few change points
        if len(changes) >= n_sweeps:
            sweep_lengths = np.diff([0] + changes[:n_sweeps].tolist())
            n_samples_per_sweep = int(np.median(sweep_lengths))

    return SweepInfo(
        n_sweeps=n_sweeps,
        n_samples_per_sweep=n_samples_per_sweep,
        lofreq_unique_values=len(unique_vals),
        lofreq_change_points=len(changes),
        sweep_type=sweep_type,
    )


def extract_metadata(filepath: Path, ds: xr.Dataset) -> FileMetadata:
    """Extract complete metadata from netCDF file.

    Combines:
    - SourceInfo: filename parsing (tolteca_v2 SourceInfoModel pattern)
    - DataProdInfo: header extraction (tolteca_v2 NcFileIO pattern)
    - TimeStreamInfo: data dimensions
    - SweepInfo: sweep structure

    Parameters
    ----------
    filepath : Path
        Path to netCDF file
    ds : xr.Dataset
        Open xarray Dataset

    Returns
    -------
    FileMetadata
        Complete file metadata
    """
    # Parse filename (tolteca_v2 SourceInfoModel)
    source = parse_filename(filepath)

    # Extract quartet from headers (tolteca_v2 NcFileIO)
    data_prod = extract_dataprod_from_header(ds)

    # Validate consistency between filename and headers
    if source.obsnum != data_prod.obsnum:
        raise ValueError(
            f"Obsnum mismatch: filename={source.obsnum}, header={data_prod.obsnum}"
        )
    if source.subobsnum != data_prod.subobsnum:
        raise ValueError(
            f"Subobsnum mismatch: filename={source.subobsnum}, header={data_prod.subobsnum}"
        )
    if source.scannum != data_prod.scannum:
        raise ValueError(
            f"Scannum mismatch: filename={source.scannum}, header={data_prod.scannum}"
        )
    if source.roach != data_prod.roach_index:
        raise ValueError(
            f"Roach mismatch: filename={source.roach}, header={data_prod.roach_index}"
        )

    # Get time range
    if "Data.Toltec.RecvTime" in ds.data_vars:
        recv_time = ds["Data.Toltec.RecvTime"].values
        t_start = float(recv_time[0])
        t_end = float(recv_time[-1])
    else:
        t_start = float(ds["Header.Toltec.ObsStartTime"].values)
        t_end = float(ds["Header.Toltec.ObsEndTime"].values)

    # Get dimensions
    n_samples = int(ds.sizes["time"])
    n_detectors = int(ds.sizes["iqlen"])

    # Analyze sweep structure
    sweep_info = analyze_sweep_structure(ds)

    return FileMetadata(
        source=source,
        data_prod=data_prod,
        file_type=source.file_suffix,
        n_samples=n_samples,
        n_detectors=n_detectors,
        t_start=t_start,
        t_end=t_end,
        source_path=str(filepath),
        sweep_info=sweep_info,
    )


def convert_to_wide_dataframe(ds: xr.Dataset) -> pd.DataFrame:
    """Convert netCDF dataset to wide-format DataFrame.

    Wide format: (n_time, 1 + 2*n_detectors + 1) columns
    Columns: timestamp, I_0, I_1, ..., I_N, Q_0, Q_1, ..., Q_N, lofreq (optional)

    Parameters
    ----------
    ds : xr.Dataset
        Open xarray Dataset

    Returns
    -------
    pd.DataFrame
        Wide-format DataFrame
    """
    # Get timestream data (tolteca_v2 NcFileIO data paths)
    is_data = ds["Data.Toltec.Is"].values  # (time, iqlen)
    qs_data = ds["Data.Toltec.Qs"].values  # (time, iqlen)
    recv_time = ds["Data.Toltec.RecvTime"].values  # (time,)

    n_time, n_det = is_data.shape

    # Create column names
    i_cols = [f"I_{i}" for i in range(n_det)]
    q_cols = [f"Q_{i}" for i in range(n_det)]

    # Build DataFrame
    df = pd.DataFrame(
        {
            "timestamp": recv_time,
            **{col: is_data[:, i] for i, col in enumerate(i_cols)},
            **{col: qs_data[:, i] for i, col in enumerate(q_cols)},
        }
    )

    # Add LOFreq if present (sweep axis)
    if "Data.Toltec.LoFreq" in ds.data_vars:
        df["lofreq"] = ds["Data.Toltec.LoFreq"].values

    return df


def write_parquet_partition(
    df: pd.DataFrame,
    output_dir: Path,
    data_prod: DataProdInfo,
    file_type: str,
    filename: str | None = None,
) -> Path:
    """Write DataFrame to partitioned Parquet file.

    Uses Hive-style partitioning aligned with ORM quartet:
    master={m}/obsnum={o}/subobsnum={s}/scannum={n}/roach_index={r}/file_type.parquet

    Parameters
    ----------
    df : pd.DataFrame
        Wide-format DataFrame
    output_dir : Path
        Output directory for raw_data/
    data_prod : DataProdInfo
        Data product quartet metadata
    file_type : str
        File type (vnasweep, tune, etc.)
    filename : str | None
        Custom filename. If None, use file_type.parquet

    Returns
    -------
    Path
        Path to written Parquet file
    """
    # Create partition directory (ORM quartet structure)
    partition_dir = (
        output_dir
        / f"master={data_prod.master}"
        / f"obsnum={data_prod.obsnum}"
        / f"subobsnum={data_prod.subobsnum}"
        / f"scannum={data_prod.scannum}"
        / f"roach_index={data_prod.roach_index}"
    )
    partition_dir.mkdir(parents=True, exist_ok=True)

    # Write Parquet
    if filename is None:
        output_file = partition_dir / f"{file_type}.parquet"
    else:
        output_file = partition_dir / filename

    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_file, compression="zstd")

    return output_file


def generate_metadata_summary(
    metadata_list: list[FileMetadata], output_dir: Path
) -> list[Path]:
    """Generate metadata summary Parquet files.

    Creates one summary per dp_raw_obs quartet (master, obsnum, subobsnum, scannum).

    Parameters
    ----------
    metadata_list : list[FileMetadata]
        List of metadata from all files
    output_dir : Path
        Output directory for metadata/

    Returns
    -------
    list[Path]
        Paths to written metadata summary files
    """
    # Group by quartet (dp_raw_obs uniqueness)
    by_quartet = {}
    for meta in metadata_list:
        quartet_key = meta.data_prod.uid_dp_raw_obs
        if quartet_key not in by_quartet:
            by_quartet[quartet_key] = []
        by_quartet[quartet_key].append(meta)

    # Create metadata summaries
    metadata_dir = output_dir
    metadata_dir.mkdir(parents=True, exist_ok=True)

    written_files = []
    for quartet_key, metas in by_quartet.items():
        # Build summary DataFrame
        records = []
        for meta in metas:
            record = {
                # Quartet fields
                "master": meta.data_prod.master,
                "obsnum": meta.data_prod.obsnum,
                "subobsnum": meta.data_prod.subobsnum,
                "scannum": meta.data_prod.scannum,
                # Interface fields
                "roach_index": meta.data_prod.roach_index,
                "interface": meta.source.interface,
                "nw_id": meta.data_prod.nw_id,
                # File fields
                "file_type": meta.file_type,
                "file_timestamp": meta.source.file_timestamp.isoformat(),
                "source_path": meta.source_path,
                # Data dimensions
                "n_samples": meta.n_samples,
                "n_detectors": meta.n_detectors,
                # Timing
                "t_start": meta.t_start,
                "t_end": meta.t_end,
                "duration": meta.t_end - meta.t_start,
            }

            # Add sweep info if present
            if meta.sweep_info:
                record["n_sweeps"] = meta.sweep_info.n_sweeps
                record["sweep_type"] = meta.sweep_info.sweep_type
                record["lofreq_unique_values"] = (
                    meta.sweep_info.lofreq_unique_values
                )
                record["lofreq_change_points"] = (
                    meta.sweep_info.lofreq_change_points
                )

            records.append(record)

        df = pd.DataFrame(records)

        # Write summary (one per quartet)
        dp = metas[0].data_prod
        output_file = metadata_dir / (
            f"master_{dp.master}_obs_{dp.obsnum}_"
            f"subobs_{dp.subobsnum}_scan_{dp.scannum}_summary.parquet"
        )

        table = pa.Table.from_pandas(df)
        pq.write_table(table, output_file, compression="zstd")
        written_files.append(output_file)

    return written_files


def process_netcdf_file(
    filepath: Path, output_raw_dir: Path
) -> tuple[Path, FileMetadata]:
    """Process a single netCDF file to Parquet.

    Parameters
    ----------
    filepath : Path
        Input netCDF file path
    output_raw_dir : Path
        Output directory for raw_data/

    Returns
    -------
    tuple[Path, FileMetadata]
        (output_file, metadata)
    """
    print(f"Processing: {filepath.name}")

    with xr.open_dataset(filepath) as ds:
        # Extract metadata (tolteca_v2 patterns)
        metadata = extract_metadata(filepath, ds)

        # Convert to wide DataFrame
        df = convert_to_wide_dataframe(ds)

        # Write partitioned Parquet (ORM quartet structure)
        output_file = write_parquet_partition(
            df,
            output_raw_dir,
            metadata.data_prod,
            metadata.file_type,
        )

    print(
        f"  ✓ Written: {output_file.relative_to(output_raw_dir.parent)} "
        f"({metadata.n_samples} samples × {metadata.n_detectors} detectors)"
    )

    return output_file, metadata


def process_observation(
    master: str | None,
    obsnum: int,
    subobsnum: int | None,
    scannum: int | None,
    input_dir: Path,
    output_dir: Path,
    file_types: list[str] | None = None,
) -> tuple[list[Path], list[Path]]:
    """Process all netCDF files for a raw observation.

    Parameters
    ----------
    master : str | None
        Master identifier ("tcs", "ics", etc.). If None, auto-detect from headers.
    obsnum : int
        Observation number
    subobsnum : int | None
        Sub-observation number. If None, process all subobsnums.
    scannum : int | None
        Scan number. If None, process all scannums.
    input_dir : Path
        Input directory containing netCDF files
    output_dir : Path
        Output directory for Parquet files
    file_types : list[str] | None
        File types to process (e.g., ['vnasweep', 'tune']).
        If None, process all types.

    Returns
    -------
    tuple[list[Path], list[Path]]
        (list of data files, list of metadata summary files)
    """
    print(f"\n{'=' * 80}")
    print(
        f"PROCESSING RAW OBSERVATION: master={master}, obsnum={obsnum}, "
        f"subobsnum={subobsnum}, scannum={scannum}"
    )
    print(f"{'=' * 80}\n")

    # Find all netCDF files for this obsnum
    pattern = f"*{obsnum}*.nc"
    all_files = list(input_dir.rglob(pattern))

    # Filter by file type if specified
    if file_types:
        files = [
            f for f in all_files if any(ft in f.name for ft in file_types)
        ]
    else:
        files = all_files

    if not files:
        raise FileNotFoundError(
            f"No netCDF files found for obsnum={obsnum} in {input_dir}"
        )

    print(f"Found {len(files)} files to process:")
    for f in files:
        print(f"  {f.name}")
    print()

    # Process each file
    output_raw_dir = output_dir / "raw_data"
    metadata_list = []
    data_files = []

    for filepath in files:
        try:
            output_file, metadata = process_netcdf_file(filepath, output_raw_dir)

            # Filter by subobsnum/scannum if specified
            if subobsnum is not None and metadata.data_prod.subobsnum != subobsnum:
                continue
            if scannum is not None and metadata.data_prod.scannum != scannum:
                continue
            if master is not None and metadata.data_prod.master != master:
                continue

            data_files.append(output_file)
            metadata_list.append(metadata)
        except Exception as e:
            print(f"  ✗ Error processing {filepath.name}: {e}")
            import traceback

            traceback.print_exc()
            continue

    if not metadata_list:
        raise ValueError(
            f"No files matched filters: master={master}, subobsnum={subobsnum}, "
            f"scannum={scannum}"
        )

    # Generate metadata summaries (one per quartet)
    print("\nGenerating metadata summaries...")
    metadata_dir = output_dir / "metadata"
    summary_files = generate_metadata_summary(metadata_list, metadata_dir)
    for sf in summary_files:
        print(f"  ✓ Written: {sf.relative_to(output_dir)}")

    print(f"\n{'=' * 80}")
    print(f"COMPLETED: {len(data_files)} data files + {len(summary_files)} metadata summaries")
    print(f"{'=' * 80}\n")

    # Print summary of processed quartets
    quartets = set(m.data_prod.uid_dp_raw_obs for m in metadata_list)
    print("Processed dp_raw_obs quartets:")
    for q in sorted(quartets):
        print(f"  {q}")
    print()

    return data_files, summary_files
