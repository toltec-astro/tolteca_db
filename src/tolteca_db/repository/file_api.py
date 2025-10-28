"""File API Layer for tolteca_v2 compatibility.

Provides ObsQuery class and SourceInfoModel dataclass to match tolteca_v2's
file-based API patterns. Enables existing tolteca_v2 scripts to work with
the new database backend without modification.

Phase 2: File API Layer

Architecture:
    - ObsQuery: High-level query interface matching tolteca_v2 ObsSpec
    - SourceInfoModel: Dataclass matching tolteca_v2 SourceInfo structure
    - DataFrame output compatible with .toltec_file accessor
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from tolteca_db.models.metadata import RawObsMeta, InterfaceFileMeta
from tolteca_db.models.orm import DataProd, DataProdSource, Location


@dataclass
class SourceInfoModel:
    """Source information matching tolteca_v2 format.
    
    Provides file:// URI and metadata for raw observation files.
    Compatible with tolteca_v2 .toltec_file accessor patterns.
    
    Attributes
    ----------
    source : str
        file:// URI to the data file
    interface : str
        Interface type (toltec, hwp, etc.)
    roach : int | None
        ROACH board number (for TolTEC data)
    obsnum : int
        Observation number
    subobsnum : int
        Sub-observation number
    scannum : int
        Scan number
    uid_raw_obs : str
        Unique identifier for raw observation (pk)
    """
    
    source: str  # file:// URI
    interface: str
    roach: int | None
    obsnum: int
    subobsnum: int
    scannum: int
    uid_raw_obs: str


class ObsQuery:
    """Query interface for observation data matching tolteca_v2 patterns.
    
    Provides high-level API for querying raw observation files from the
    database, returning results in DataFrame format compatible with
    existing tolteca_v2 workflows.
    
    Parameters
    ----------
    session : Session
        SQLAlchemy session
    location_label : str, optional
        Location label to query (default: "local")
    
    Examples
    --------
    >>> from tolteca_db.db import create_session
    >>> session = create_session("duckdb:///tolteca.duckdb")
    >>> query = ObsQuery(session, location_label="lmt")
    >>> df = query.get_raw_obs_info_table("123456")
    >>> print(df[['obsnum', 'subobsnum', 'source']])
    """
    
    def __init__(self, session: Session, location_label: str = "local"):
        self.session = session
        self.location_label = location_label
    
    def get_raw_obs_info_table(
        self,
        obs_spec: str | int,
        subobsnum: int | None = None,
        scannum: int | None = None,
    ) -> pd.DataFrame:
        """Get raw observation info as DataFrame.
        
        Queries database for raw observation files matching the specification,
        returning a DataFrame compatible with tolteca_v2 .toltec_file accessor.
        
        Parameters
        ----------
        obs_spec : str | int
            Observation number or pattern
        subobsnum : int | None, optional
            Filter by specific sub-observation
        scannum : int | None, optional
            Filter by specific scan number
        
        Returns
        -------
        pd.DataFrame
            DataFrame with columns: source, interface, roach, obsnum,
            subobsnum, scannum, uid_raw_obs
        
        Examples
        --------
        >>> df = query.get_raw_obs_info_table(123456)
        >>> df = query.get_raw_obs_info_table(123456, subobsnum=1)
        >>> df = query.get_raw_obs_info_table("123*")  # Pattern matching
        
        Notes
        -----
        Compatible with tolteca_v2 workflow:
        ```python
        from tollan.utils.dataclass_schema import add_schema
        from tolteca.datamodels.toltec import BasicObsDataset
        
        df = query.get_raw_obs_info_table("123456")
        # Use with existing .toltec_file accessor
        dataset = BasicObsDataset.from_files(df['source'].tolist())
        ```
        """
        # Parse obs_spec
        if isinstance(obs_spec, int):
            obsnum = obs_spec
            pattern_match = False
        else:
            obs_spec_str = str(obs_spec)
            if "*" in obs_spec_str or "?" in obs_spec_str:
                # Pattern matching
                pattern_match = True
                obsnum = None  # Will use LIKE query
            else:
                obsnum = int(obs_spec_str)
                pattern_match = False
        
        # Build query for DataProd with RawObsMeta
        stmt = (
            select(DataProd, DataProdSource, Location)
            .join(DataProdSource, DataProd.pk == DataProdSource.data_prod_fk)
            .join(Location, DataProdSource.location_fk == Location.pk)
            .where(Location.label == self.location_label)
        )
        
        # Filter by obsnum (either exact or pattern)
        if not pattern_match and obsnum is not None:
            # For RawObsMeta, we need to filter on the JSON field
            # This requires database-specific JSON operators
            # DuckDB uses PostgreSQL JSON syntax: meta->>'obsnum' = obsnum
            # For PostgreSQL: meta->>'obsnum' = str(obsnum)
            # Simplified: Load all and filter in Python for now
            pass  # TODO: Add JSON filtering in SQL
        
        # Execute query
        results = self.session.execute(stmt).all()
        
        # Convert to SourceInfoModel list
        source_infos = []
        for data_prod, data_prod_source, location in results:
            # Extract metadata
            if not isinstance(data_prod.meta, RawObsMeta):
                continue  # Skip non-raw products
            
            meta = data_prod.meta
            
            # Apply Python-side filters
            if obsnum is not None and meta.obsnum != obsnum:
                continue
            if subobsnum is not None and meta.subobsnum != subobsnum:
                continue
            if scannum is not None and meta.scannum != scannum:
                continue
            
            # Get interface from meta
            if isinstance(meta, RawObsMeta):
                # Look for InterfaceFileMeta in meta
                interface = "toltec"  # Default
                roach = None
                
                # Check if meta has interface info
                if hasattr(meta, 'interface'):
                    interface = meta.interface
                if hasattr(meta, 'roach'):
                    roach = meta.roach
            
            # Use source_uri directly (already contains full path)
            source_uri = data_prod_source.source_uri
            
            source_info = SourceInfoModel(
                source=source_uri,
                interface=interface,
                roach=roach,
                obsnum=meta.obsnum,
                subobsnum=meta.subobsnum,
                scannum=meta.scannum,
                uid_raw_obs=data_prod.pk,
            )
            source_infos.append(source_info)
        
        # Convert to DataFrame
        if not source_infos:
            # Return empty DataFrame with correct schema
            return pd.DataFrame(columns=[
                'source', 'interface', 'roach', 'obsnum',
                'subobsnum', 'scannum', 'uid_raw_obs'
            ])
        
        data = [
            {
                'source': si.source,
                'interface': si.interface,
                'roach': si.roach,
                'obsnum': si.obsnum,
                'subobsnum': si.subobsnum,
                'scannum': si.scannum,
                'uid_raw_obs': si.uid_raw_obs,
            }
            for si in source_infos
        ]
        
        return pd.DataFrame(data)
    
    def get_reduced_obs_info_table(
        self,
        master_obsnum: int,
        obs_type: str | None = None,
    ) -> pd.DataFrame:
        """Get reduced observation info as DataFrame.
        
        Queries database for reduced data products derived from raw observations.
        
        Parameters
        ----------
        master_obsnum : int
            Master observation number
        obs_type : str | None, optional
            Filter by observation type (pointing, beammap, etc.)
        
        Returns
        -------
        pd.DataFrame
            DataFrame with reduced product information
        """
        # TODO: Implement reduced product queries
        # This will query for ReducedObsMeta products
        raise NotImplementedError(
            "Reduced observation queries not yet implemented"
        )


__all__ = [
    "ObsQuery",
    "SourceInfoModel",
]
