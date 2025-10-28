"""Observation query API - tolteca_v2 compatibility layer.

This module provides the ObsQuery interface for querying raw observation
data products and returning DataFrames compatible with tolteca_v2's file API.

The SourceInfoModel dataclass matches tolteca_v2/src/tolteca_datamodels/toltec/file.py
to ensure seamless integration with existing analysis tools.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from adaptix import Retort, name_mapping
from loguru import logger
from sqlalchemy import create_engine, desc, select
from sqlalchemy.orm import Session, joinedload

from tolteca_db.models.orm import DataProd, DataProdSource, DataProdType, Location


@dataclass
class SourceInfoModel:
    """Source information model with optional master support.
    
    This dataclass mirrors the structure from:
    tolteca_v2/src/tolteca_datamodels/toltec/file.py
    
    Field naming matches tolteca_v2 exactly for DataFrame compatibility.
    
    Notes
    -----
    The master field is OPTIONAL to maintain compatibility with both:
    - tolteca_v2 file-based usage (master=None, inferred from context)
    - tolteca_web database usage (master included in UIDs)
    
    UIDs conditionally include master:
    - If master is provided: "tcs-123456-0-0" (matches tolteca_web)
    - If master is None: "123456-0-0" (matches tolteca_v2)
    """

    source: str  # FileLoc - file:// URI
    interface: str  # Interface ID (toltec0, toltec1, etc.)
    roach: int | None  # Roach number (0-12 for each array)
    master: str | None = None  # Master type (tcs, ics, clip, simu) - optional
    obsnum: int = 0  # Observation number
    subobsnum: int = 0  # Sub-observation number
    scannum: int = 0  # Scan number
    file_timestamp: datetime | None = None  # File creation timestamp
    file_suffix: str | None = None  # File suffix (e.g., "timestream", "targsweep")
    file_ext: str = ""  # File extension (.nc, .fits, etc.)
    uid_obs: str = ""  # Unique observation ID (conditional master prefix)
    uid_raw_obs: str = ""  # Unique raw observation ID (conditional master prefix)
    uid_raw_obs_file: str = ""  # Unique file ID (conditional master prefix)


class ObsQuery:
    """High-level observation query interface.
    
    Provides a simplified API for querying raw observation data products
    and returning DataFrames compatible with tolteca_v2's file API.
    
    This allows existing tolteca_v2 analysis code to work seamlessly with
    the database-backed implementation.
    
    Examples
    --------
    >>> query = ObsQuery("duckdb:///tolteca.duckdb")
    >>> df = query.get_raw_obs_info_table("123456")
    >>> print(df.columns)
    Index(['source', 'interface', 'roach', 'obsnum', ...])
    
    >>> # Use with tolteca_v2 file API (future integration)
    >>> # df.toltec_file.make_raw_obs_groups()
    >>> # df.toltec_file.get_raw_obs_latest()
    """

    def __init__(
        self,
        db_url: str,
        location_label: str = "local",
        location_type: str | None = None,
    ):
        """Initialize ObsQuery.
        
        Parameters
        ----------
        db_url : str
            SQLAlchemy database URL
        location_label : str, optional
            Location label to filter sources, by default "local"
        location_type : str | None, optional
            Location type filter (filesystem, s3, http), by default None
        """
        self.db_url = db_url
        self.location_label = location_label
        self.location_type = location_type
        self.engine = create_engine(db_url)
        
        # Create retort for field name mapping (DB → API)
        self._retort = Retort(
            recipe=[
                name_mapping(
                    SourceInfoModel,  # Apply mapping only to SourceInfoModel
                    map={
                        'source_uri': 'source',  # Map internal name to API name
                    }
                )
            ]
        )
    
    @classmethod
    def parse_obs_spec(cls, obs_spec: str | int | None) -> dict[str, Any]:
        """Parse ObsSpec string to query parameters.
        
        Supports advanced separator notation with wildcards and shortcuts.
        
        Separator Notation
        ------------------
        - Forward separator `-`: Sequential specification (obsnum-subobsnum-scannum)
        - Backward separator `/`: Skip intermediate values
        
        Field Order: {master} {obsnum} {subobsnum} {scannum} {roach}
        
        Wildcard Notation
        -----------------
        - `{}`: Comma-separated list, empty `{}` matches all
        - `[]`: Slice notation, empty `[]` matches all (like `[:]`)
        - Required to omit obsnum: `{}/1` or `[]/1` → roach=1
        
        Parameters
        ----------
        obs_spec : str | int | None
            Observation specification string
            
            Basic Formats:
            - None → latest observation
            - 123456 (int) → obsnum only
            - "123456" → obsnum only
            - "tcs-123456" → with master prefix
            
            Sequential Format (forward separator `-`):
            - "123456-0" → obsnum + subobsnum
            - "123456-0-0" → obsnum + subobsnum + scannum
            - "tcs-123456-0-0" → full specification with master
            
            Shortcut Format (backward separator `/`):
            - "1000/0" → obsnum=1000, roach=0 (skips subobsnum, scannum)
            - "1000-0/0" → obsnum=1000, subobsnum=0, roach=0 (skips scannum)
            - "1000/0/0" → obsnum=1000, scannum=0, roach=0 (skips subobsnum)
            
            Wildcard Format:
            - "1000-[]-[]-1" ≡ "1000/1" → obsnum=1000, roach=1, wildcard sub/scan
            - "{}/1" or "[]/1" → roach=1, wildcard obsnum (matches all obsnum)
            - "1000-{0,1,2}" → obsnum=1000, subobsnum in [0,1,2]
            - "1000-[0:5]" → obsnum=1000, subobsnum in range [0,5)
            
            File Paths:
            - "/path/to/file.nc" → file path resolution
        
        Returns
        -------
        dict[str, Any]
            Parsed parameters with keys:
            - master, obsnum, subobsnum, scannum, roach: exact values
            - subobsnum_list, scannum_list: list of values (from `{}`)
            - subobsnum_slice, scannum_slice: slice object (from `[]`)
            - filepath: Path object
            
        Examples
        --------
        >>> ObsQuery.parse_obs_spec(None)
        {}
        
        >>> ObsQuery.parse_obs_spec(123456)
        {'obsnum': 123456}
        
        >>> ObsQuery.parse_obs_spec("123456-1-5")
        {'obsnum': 123456, 'subobsnum': 1, 'scannum': 5}
        
        >>> ObsQuery.parse_obs_spec("1000/0")
        {'obsnum': 1000, 'roach': 0}
        
        >>> ObsQuery.parse_obs_spec("1000-0/0")
        {'obsnum': 1000, 'subobsnum': 0, 'roach': 0}
        
        >>> ObsQuery.parse_obs_spec("1000/0/0")
        {'obsnum': 1000, 'scannum': 0, 'roach': 0}
        
        >>> ObsQuery.parse_obs_spec("1000-[]-[]-1")
        {'obsnum': 1000, 'roach': 1, 'subobsnum_slice': slice(None), 'scannum_slice': slice(None)}
        
        >>> ObsQuery.parse_obs_spec("{}/1")
        {'roach': 1}
        
        >>> ObsQuery.parse_obs_spec("1000-{0,1,2}")
        {'obsnum': 1000, 'subobsnum_list': [0, 1, 2]}
        """
        # Handle None → latest
        if obs_spec is None:
            logger.debug(f"parse_obs_spec({obs_spec=}) → latest")
            return {}
        
        # Handle int → obsnum
        if isinstance(obs_spec, int):
            result = {'obsnum': obs_spec}
            logger.debug(f"parse_obs_spec({obs_spec=}) → {result}")
            return result
        
        # Convert to string for pattern matching
        obs_spec_str = str(obs_spec).strip()
        
        # Check if it's a file path (absolute path or ends with .nc but not containing wildcards)
        if (obs_spec_str.startswith('/') or obs_spec_str.endswith('.nc')) and \
           '{' not in obs_spec_str and '[' not in obs_spec_str:
            result = {'filepath': Path(obs_spec_str)}
            logger.debug(f"parse_obs_spec({obs_spec=}) → filepath: {result['filepath']}")
            return result
        
        # Parse the obs_spec using the advanced notation
        result = cls._parse_advanced_obs_spec(obs_spec_str)
        
        logger.debug(f"parse_obs_spec({obs_spec=}) → {result}")
        return result
    
    @classmethod
    def _parse_advanced_obs_spec(cls, spec: str) -> dict[str, Any]:
        """Parse advanced ObsSpec with separator and wildcard notation.
        
        The first number is ALWAYS obsnum (left-anchored).
        
        Forward separator `-`: Parse left-to-right sequentially
            Example: "1000-0-0" → obsnum=1000, subobsnum=0, scannum=0
        
        Backward separator `/`: Parse remaining fields right-to-left
            Example: "1000/0" → obsnum=1000, then 0 from right = roach
            Example: "1000/0/0" → obsnum=1000, then right-to-left: 0=roach, 0=scannum
            Example: "1000-0/0" → obsnum=1000, subobsnum=0 (sequential), then 0=roach (from right)
        
        Field order (left-to-right): obsnum, subobsnum, scannum, roach
        Field order (right-to-left): roach, scannum, subobsnum, obsnum
        
        Parameters
        ----------
        spec : str
            ObsSpec string with separators and wildcards
        
        Returns
        -------
        dict[str, Any]
            Parsed parameters
        """
        result = {}
        
        # Extract optional master prefix (tcs-, ics-, etc.)
        master_match = re.match(r'^(tcs|ics|clip|simu)-(.+)$', spec)
        if master_match:
            result['master'] = master_match.group(1)
            spec = master_match.group(2)  # Remove master prefix
        
        # Check for single value (no separators)
        if '-' not in spec and '/' not in spec:
            # Single value - must be obsnum
            val = cls._parse_value(spec)
            if val is not None:
                if isinstance(val, int):
                    result['obsnum'] = val
                elif isinstance(val, slice):
                    result['obsnum_slice'] = val
                elif isinstance(val, list):
                    result['obsnum_list'] = val
            return result
        
        # Split into components, tracking separator types
        components = []
        sep_types = []  # separator BEFORE each component (empty for first)
        current = []
        
        for char in spec:
            if char == '-':
                components.append(''.join(current))
                current = []
                sep_types.append('forward')
            elif char == '/':
                components.append(''.join(current))
                current = []
                sep_types.append('backward')
            else:
                current.append(char)
        
        # Add last component
        components.append(''.join(current))
        
        if not components:
            return result
        
        # Field names
        forward_fields = ['obsnum', 'subobsnum', 'scannum', 'roach']
        backward_fields = ['roach', 'scannum', 'subobsnum', 'obsnum']
        
        # First component is ALWAYS obsnum
        parsed = cls._parse_value(components[0])
        cls._assign_parsed_value(result, 'obsnum', parsed)
        
        if len(components) == 1:
            return result
        
        # Find if there's a separator type change (forward to backward)
        # Split into segments based on separator type
        first_backward_idx = None
        for i, sep in enumerate(sep_types):
            if sep == 'backward':
                first_backward_idx = i
                break
        
        if first_backward_idx is None:
            # All forward separators - pure sequential left-to-right
            for i in range(1, len(components)):
                field_idx = i  # 1=subobsnum, 2=scannum, 3=roach
                if field_idx < len(forward_fields):
                    parsed = cls._parse_value(components[i])
                    cls._assign_parsed_value(result, forward_fields[field_idx], parsed)
        
        else:
            # Mixed: forward part (left-to-right), then backward part (right-to-left)
            # Note: sep_types[i] is the separator BEFORE components[i+1]
            # So if first_backward_idx=0, the backward part starts at components[1]
            
            # Parse forward part (sequential from subobsnum)
            for i in range(1, first_backward_idx + 1):
                field_idx = i  # 1=subobsnum, 2=scannum, 3=roach
                if field_idx < len(forward_fields):
                    parsed = cls._parse_value(components[i])
                    cls._assign_parsed_value(result, forward_fields[field_idx], parsed)
            
            # Parse backward part (right-to-left from roach)
            backward_components = components[first_backward_idx + 1:]
            for i, comp in enumerate(backward_components):
                # First backward component (index 0) maps to roach (rightmost)
                # Second backward component (index 1) maps to scannum
                # etc.
                if i < len(backward_fields):
                    field_name = backward_fields[i]
                    # Don't overwrite fields already set by forward parsing
                    if field_name not in result and \
                       f"{field_name}_list" not in result and \
                       f"{field_name}_slice" not in result:
                        parsed = cls._parse_value(comp)
                        cls._assign_parsed_value(result, field_name, parsed)
        
        return result
    
    @classmethod
    def _assign_parsed_value(cls, result: dict, field_name: str, parsed: Any) -> None:
        """Assign parsed value to result dict with appropriate suffix."""
        if isinstance(parsed, int):
            result[field_name] = parsed
        elif isinstance(parsed, list):
            result[f"{field_name}_list"] = parsed
        elif isinstance(parsed, slice):
            result[f"{field_name}_slice"] = parsed
        # None values are ignored
    
    @classmethod
    def _parse_value(cls, value: str) -> int | list | slice | None:
        """Parse a single value which can be int, list, slice, or wildcard.
        
        Parameters
        ----------
        value : str
            Value string (e.g., "123", "{0,1,2}", "[0:5]", "[]", "{}")
        
        Returns
        -------
        int | list | slice | None
            - int: exact value
            - list: from `{val1,val2,...}`
            - slice(None): from empty `[]` or `{}`  (match all)
            - slice(...): from `[start:stop:step]`
            - None: empty string or invalid
        """
        value = value.strip()
        
        if not value:
            return None
        
        # Check for empty wildcards - return slice(None) to indicate "match all"
        if value == '{}' or value == '[]':
            return slice(None)  # Match all
        
        # Check for list notation: {val1,val2,...}
        if value.startswith('{') and value.endswith('}'):
            inner = value[1:-1].strip()
            if not inner:
                return slice(None)  # Empty list matches all
            try:
                return [int(v.strip()) for v in inner.split(',')]
            except ValueError:
                logger.warning(f"Invalid list notation: {value}")
                return None
        
        # Check for slice notation: [start:stop:step] or [start:stop] or [:]
        if value.startswith('[') and value.endswith(']'):
            inner = value[1:-1].strip()
            if not inner or inner == ':':
                return slice(None)  # Match all
            
            try:
                # Parse slice notation
                parts = inner.split(':')
                if len(parts) == 1:
                    # Single index [n]
                    return int(parts[0]) if parts[0] else slice(None)
                elif len(parts) == 2:
                    # [start:stop]
                    start = int(parts[0]) if parts[0] else None
                    stop = int(parts[1]) if parts[1] else None
                    return slice(start, stop)
                elif len(parts) == 3:
                    # [start:stop:step]
                    start = int(parts[0]) if parts[0] else None
                    stop = int(parts[1]) if parts[1] else None
                    step = int(parts[2]) if parts[2] else None
                    return slice(start, stop, step)
            except ValueError:
                logger.warning(f"Invalid slice notation: {value}")
                return None
        
        # Try to parse as integer
        try:
            return int(value)
        except ValueError:
            logger.warning(f"Unable to parse value: {value}")
            return None
    
    def get_raw_obs_info_table(
        self,
        obs_spec: str | int | None = None,
        master: str | None = None,
        obsnum: int | None = None,
        subobsnum: int | None = None,
        scannum: int | None = None,
        interface: str | None = None,
        raise_on_multiple: bool = False,
        raise_on_empty: bool = False,
    ) -> pd.DataFrame:
        """Get raw observation info table from ObsSpec.
        
        Returns DataFrame with SourceInfoModel columns, compatible with
        tolteca_v2's file API and .toltec_file accessor.
        
        Parameters
        ----------
        obs_spec : str | int | None, optional
            Observation specification (see parse_obs_spec), by default None
        master : str | None, optional
            Override master from obs_spec, by default None
        obsnum : int | None, optional
            Override obsnum from obs_spec, by default None
        subobsnum : int | None, optional
            Override subobsnum from obs_spec, by default None
        scannum : int | None, optional
            Override scannum from obs_spec, by default None
        interface : str | None, optional
            Specific interface ID (toltec0, toltec1, etc.), by default None
        raise_on_multiple : bool, optional
            Raise ValueError if multiple files match, by default False
        raise_on_empty : bool, optional
            Raise ValueError if no files match, by default False
        
        Returns
        -------
        pd.DataFrame
            DataFrame with SourceInfoModel columns
        
        Raises
        ------
        ValueError
            If raise_on_multiple=True and multiple files match
            If raise_on_empty=True and no files match
        
        Examples
        --------
        >>> query = ObsQuery("duckdb:///tolteca.duckdb")
        
        >>> # Query by obsnum
        >>> df = query.get_raw_obs_info_table(123456)
        >>> df = query.get_raw_obs_info_table("123456")
        
        >>> # Query with master
        >>> df = query.get_raw_obs_info_table("tcs-123456")
        >>> df = query.get_raw_obs_info_table("123456", master="tcs")
        
        >>> # Full specification
        >>> df = query.get_raw_obs_info_table("tcs-123456-1-5")
        
        >>> # Latest observation
        >>> df = query.get_raw_obs_info_table(None)
        """
        # Parse obs_spec if provided
        if obs_spec is not None:
            parsed = self.parse_obs_spec(obs_spec)
            
            # Handle filepath
            if 'filepath' in parsed:
                # TODO: Implement file path resolution
                logger.warning(f"File path resolution not yet implemented: {parsed['filepath']}")
                parsed = {}
            
            # Merge with explicit parameters (explicit takes precedence)
            master = master or parsed.get('master')
            obsnum = obsnum or parsed.get('obsnum')
            subobsnum = subobsnum or parsed.get('subobsnum')
            scannum = scannum or parsed.get('scannum')
            
            # Handle roach mapping to interface
            if 'roach' in parsed:
                roach = parsed['roach']
                interface = interface or f"toltec{roach}"
            
            # Handle wildcard lists and slices
            # For lists: query multiple times and concatenate
            # For slices: query and filter in Python (or expand to list)
            
            subobsnum_list = parsed.get('subobsnum_list')
            subobsnum_slice = parsed.get('subobsnum_slice')
            scannum_list = parsed.get('scannum_list')
            scannum_slice = parsed.get('scannum_slice')
            
            # If we have lists or slices, we need to handle them specially
            if subobsnum_list or subobsnum_slice or scannum_list or scannum_slice:
                # Query with wildcards, then filter results
                all_sources = self._query_raw_obs_sources(
                    master=master,
                    obsnum=obsnum,
                    subobsnum=None,  # Don't filter - we'll do it in Python
                    scannum=None,
                    interface=interface,
                )
                
                # Filter by lists/slices
                sources = []
                for src in all_sources:
                    dp_meta = src.data_prod.meta
                    sub = getattr(dp_meta, 'subobsnum', 0)
                    scan = getattr(dp_meta, 'scannum', 0)
                    
                    # Check subobsnum filter
                    if subobsnum_list is not None and sub not in subobsnum_list:
                        continue
                    if subobsnum_slice is not None:
                        # Expand slice to check membership
                        # Note: This is approximate - assumes reasonable range
                        try:
                            range_obj = range(100)[subobsnum_slice]  # Assume max 100 subobs
                            if sub not in range_obj:
                                continue
                        except (IndexError, ValueError):
                            pass
                    
                    # Check scannum filter
                    if scannum_list is not None and scan not in scannum_list:
                        continue
                    if scannum_slice is not None:
                        try:
                            range_obj = range(10000)[scannum_slice]  # Assume max 10000 scans
                            if scan not in range_obj:
                                continue
                        except (IndexError, ValueError):
                            pass
                    
                    sources.append(src)
            else:
                # No wildcards - standard query
                sources = self._query_raw_obs_sources(
                    master=master,
                    obsnum=obsnum,
                    subobsnum=subobsnum,
                    scannum=scannum,
                    interface=interface,
                )
        else:
            # No obs_spec - use explicit parameters
            sources = self._query_raw_obs_sources(
                master=master,
                obsnum=obsnum,
                subobsnum=subobsnum,
                scannum=scannum,
                interface=interface,
            )
        
        # Check result count
        n_files = len(sources)
        if raise_on_multiple and n_files > 1:
            raise ValueError(
                f"Ambiguous: {n_files} files found for {obs_spec=}, "
                f"{master=}, {obsnum=}, {subobsnum=}, {scannum=}"
            )
        if raise_on_empty and n_files == 0:
            raise ValueError(
                f"No files found for {obs_spec=}, "
                f"{master=}, {obsnum=}, {subobsnum=}, {scannum=}"
            )
        
        # Convert to SourceInfoModel list
        source_models = [self._source_to_model(src) for src in sources]
        
        # Convert to DataFrame
        if not source_models:
            # Return empty DataFrame with correct schema
            return pd.DataFrame(columns=[
                'source', 'interface', 'roach', 'master', 'obsnum', 'subobsnum', 'scannum',
                'file_timestamp', 'file_suffix', 'file_ext',
                'uid_obs', 'uid_raw_obs', 'uid_raw_obs_file'
            ])
        
        data = [self._retort.dump(model) for model in source_models]
        df = pd.DataFrame(data)
        
        logger.debug(
            f"Resolved {len(df)} files from {obs_spec=}, "
            f"{master=}, {obsnum=}, {subobsnum=}, {scannum=}"
        )
        
        return df
    
    def _query_raw_obs_sources(
        self,
        master: str | None = None,
        obsnum: int | None = None,
        subobsnum: int | None = None,
        scannum: int | None = None,
        interface: str | None = None,
    ) -> list[DataProdSource]:
        """Query raw observation sources from database.
        
        Parameters
        ----------
        master : str | None
            Master type filter (tcs, ics, clip, simu)
        obsnum : int | None
            Observation number filter
        subobsnum : int | None
            Sub-observation number filter
        scannum : int | None
            Scan number filter
        interface : str | None
            Interface ID filter
        
        Returns
        -------
        list[DataProdSource]
            List of DataProdSource ORM objects
        """
        with Session(self.engine) as session:
            # Build query
            stmt = (
                select(DataProdSource)
                .join(DataProdSource.data_prod)
                .join(DataProdSource.location)
                .join(DataProd.data_prod_type)
                .where(DataProdType.label == "dp_raw_obs")  # Only raw obs products
                .options(
                    joinedload(DataProdSource.data_prod),
                    joinedload(DataProdSource.location),
                )
            )
            
            # Filter by location
            if self.location_label:
                stmt = stmt.where(Location.label.like(f"{self.location_label}%"))  # Allow prefix matching
            if self.location_type:
                stmt = stmt.where(Location.location_type == self.location_type)
            
            # Filter by metadata (stored in meta JSON column)
            if master is not None:
                stmt = stmt.where(
                    DataProd.meta['master'].as_string() == master
                )
            if obsnum is not None:
                stmt = stmt.where(
                    DataProd.meta['obsnum'].as_integer() == obsnum
                )
            if subobsnum is not None:
                stmt = stmt.where(
                    DataProd.meta['subobsnum'].as_integer() == subobsnum
                )
            if scannum is not None:
                stmt = stmt.where(
                    DataProd.meta['scannum'].as_integer() == scannum
                )
            if interface is not None:
                stmt = stmt.where(
                    DataProdSource.meta['interface_id'].as_string() == interface
                )
            
            # Execute query
            result = session.scalars(stmt).unique().all()
            
            # Detach from session (convert to plain objects)
            session.expunge_all()
            
            return list(result)
    
    def get_raw_obs_latest(
        self,
        master: str | None = None,
        interface: str | None = None,
    ) -> pd.DataFrame:
        """Get latest raw observation.
        
        Queries for the most recent observation (highest obsnum) matching
        the specified filters.
        
        Parameters
        ----------
        master : str | None, optional
            Filter by master (tcs, ics, clip, simu), by default None
        interface : str | None, optional
            Filter by interface (toltec0, toltec1, etc.), by default None
        
        Returns
        -------
        pd.DataFrame
            SourceInfoModel DataFrame for latest observation
        
        Examples
        --------
        >>> query = ObsQuery("duckdb:///tolteca.duckdb")
        
        >>> # Get absolute latest
        >>> df = query.get_raw_obs_latest()
        
        >>> # Get latest for master
        >>> df = query.get_raw_obs_latest(master="tcs")
        
        >>> # Get latest for specific interface
        >>> df = query.get_raw_obs_latest(master="tcs", interface="toltec0")
        """
        with Session(self.engine) as session:
            # Build query for latest obsnum
            stmt = (
                select(DataProd.meta['obsnum'].as_integer())
                .join(DataProd.data_prod_type)
                .where(DataProdType.label == "dp_raw_obs")
            )
            
            # Apply filters
            if master is not None:
                stmt = stmt.where(DataProd.meta['master'].as_string() == master)
            
            # Order by obsnum descending and limit to 1
            stmt = stmt.order_by(desc(DataProd.meta['obsnum'].as_integer())).limit(1)
            
            result = session.scalar(stmt)
            
            if result is None:
                logger.warning(f"No observations found for {master=}, {interface=}")
                return pd.DataFrame(columns=[
                    'source', 'interface', 'roach', 'master', 'obsnum', 'subobsnum', 'scannum',
                    'file_timestamp', 'file_suffix', 'file_ext',
                    'uid_obs', 'uid_raw_obs', 'uid_raw_obs_file'
                ])
            
            latest_obsnum = result
        
        # Query all files for latest obsnum
        logger.debug(f"Latest obsnum: {latest_obsnum} for {master=}, {interface=}")
        return self.get_raw_obs_info_table(
            obsnum=latest_obsnum,
            master=master,
            interface=interface,
        )
    
    def _source_to_model(self, src: DataProdSource) -> SourceInfoModel:
        """Convert DataProdSource ORM to SourceInfoModel.
        
        Parameters
        ----------
        src : DataProdSource
            ORM source object
        
        Returns
        -------
        SourceInfoModel
            API model with tolteca_v2 compatible fields
        
        Notes
        -----
        UIDs conditionally include master:
        - If master in metadata: "tcs-123456-0-0" (tolteca_web style)
        - If master is None/empty: "123456-0-0" (tolteca_v2 style)
        """
        # Extract metadata from both DataProd and DataProdSource
        dp_meta = src.data_prod.meta
        src_meta = src.meta
        
        # Get master (may be None for v2 compatibility)
        master = getattr(dp_meta, 'master', None)
        if master == "":  # Empty string treated as None
            master = None
            
        obsnum = getattr(dp_meta, 'obsnum', 0)
        subobsnum = getattr(dp_meta, 'subobsnum', 0)
        scannum = getattr(dp_meta, 'scannum', 0)
        interface_id = getattr(src_meta, 'interface_id', 'unknown')
        
        # Build UIDs with conditional master prefix
        if master:
            uid_obs = f"{master}-{obsnum}"
            uid_raw_obs = f"{master}-{obsnum}-{subobsnum}-{scannum}"
            uid_raw_obs_file = f"{master}-{obsnum}-{subobsnum}-{scannum}-{interface_id}"
        else:
            uid_obs = f"{obsnum}"
            uid_raw_obs = f"{obsnum}-{subobsnum}-{scannum}"
            uid_raw_obs_file = f"{obsnum}-{subobsnum}-{scannum}-{interface_id}"
        
        # Parse file path for extension
        source_path = Path(src.source_uri.replace('file://', ''))
        file_ext = source_path.suffix
        
        return SourceInfoModel(
            source=src.source_uri,
            interface=interface_id,
            roach=getattr(src_meta, 'roach', None),
            master=master,
            obsnum=obsnum,
            subobsnum=subobsnum,
            scannum=scannum,
            file_timestamp=src.created_at,
            file_suffix=getattr(src_meta, 'file_suffix', None),
            file_ext=file_ext,
            uid_obs=uid_obs,
            uid_raw_obs=uid_raw_obs,
            uid_raw_obs_file=uid_raw_obs_file,
        )
    
    def close(self):
        """Close database engine."""
        self.engine.dispose()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
