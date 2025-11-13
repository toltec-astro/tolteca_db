"""Query interface for Parquet files with database-centric architecture.

This module provides multiple strategies for querying Parquet timestream data
using the database as the primary source of truth:

1. DuckDB Integration: Query database for URIs, then query Parquet files
2. ORM-First: Query DataProd/DataProdSource, then load Parquet data on-demand
3. Hybrid DataFrame: Combine ORM metadata with Parquet data in DataFrames

Architecture:
- Database (DataProd + DataProdSource) knows what files exist and where
- Query database first to discover available observations
- Load Parquet data using URIs from DataProdSource.source_uri
- Never scan filesystem to discover data (database is source of truth)

Follows tolteca_v2 patterns:
- ObsQuery-style interface for raw observations
- SourceInfoModel-compatible output
- DataFrame-based results with .toltec_file accessor compatibility
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import duckdb
import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from tolteca_db.models.metadata import RawObsMeta
from tolteca_db.models.orm.data_prod import DataProd, DataProdType
from tolteca_db.models.orm.source import DataProdSource


@dataclass
class QuartetSpec:
    """Specification for a raw observation quartet.
    
    Matches ORM dp_raw_obs uniqueness constraint.
    """

    master: str | None = None  # "tcs", "ics", etc.
    obsnum: int | None = None
    subobsnum: int | None = None
    scannum: int | None = None


@dataclass
class InterfaceSpec:
    """Specification for interface/roach filtering."""

    interface: str | list[str] | None = None  # "toltec0" or ["toltec0", "toltec1"]
    roach_index: int | list[int] | None = None  # 0 or [0, 1, 2]


@dataclass
class DataSpec:
    """Specification for data selection."""

    file_types: list[str] | None = None  # ["vnasweep", "tune"]
    detectors: list[int] | None = None  # [0, 1, 2, ...] or None for all
    time_range: tuple[float, float] | None = None  # (t_start, t_end)


class ParquetORMQuery:
    """Query interface for Parquet files with ORM integration.
    
    Examples
    --------
    >>> # Strategy 1: DuckDB integration (query both together)
    >>> query = ParquetORMQuery(session, parquet_root)
    >>> df = query.query_timestream_duckdb(
    ...     quartet=QuartetSpec(master="tcs", obsnum=113515),
    ...     interface=InterfaceSpec(roach_index=0)
    ... )
    
    >>> # Strategy 2: ORM-first (metadata, then lazy load)
    >>> metadata = query.get_raw_obs_metadata(QuartetSpec(obsnum=113515))
    >>> for meta in metadata:
    ...     df = query.load_parquet_for_quartet(meta)
    
    >>> # Strategy 3: Hybrid DataFrame (combine ORM + Parquet)
    >>> df = query.query_hybrid(
    ...     quartet=QuartetSpec(obsnum=113515),
    ...     interface=InterfaceSpec(roach_index=[0, 1])
    ... )
    """

    def __init__(
        self,
        session: Session,
        db_path: Path | str | None = None,
    ):
        """Initialize query interface.
        
        Parameters
        ----------
        session : Session
            SQLAlchemy session for ORM queries
        db_path : Path | str | None
            Path to SQLite/DuckDB database file. If None, extract from session.
            
        Notes
        -----
        This interface queries the database (DataProd + DataProdSource) to discover
        what Parquet files exist and where they are stored. The database is the
        single source of truth - we never scan filesystem directories.
        """
        self.session = session
        
        # Extract database path from session
        if db_path is None:
            db_url = str(session.bind.url)
            if db_url.startswith("sqlite:///"):
                self.db_path = db_url.replace("sqlite:///", "")
            elif db_url.startswith("duckdb:///"):
                self.db_path = db_url.replace("duckdb:///", "")
            else:
                self.db_path = None
        else:
            self.db_path = str(db_path)

    # =========================================================================
    # Strategy 1: DuckDB Integration (Query Both Together)
    # =========================================================================

    def query_timestream_duckdb(
        self,
        quartet: QuartetSpec,
        interface: InterfaceSpec | None = None,
        data: DataSpec | None = None,
    ) -> pd.DataFrame:
        """Query timestream data using DuckDB with database URIs.
        
        This strategy uses DuckDB to query Parquet files discovered through
        the database (DataProdSource), providing efficient execution.
        
        Parameters
        ----------
        quartet : QuartetSpec
            Raw observation quartet filter
        interface : InterfaceSpec | None
            Interface/roach filter
        data : DataSpec | None
            Data selection (detectors, time range)
        
        Returns
        -------
        pd.DataFrame
            Timestream data with metadata columns
        """
        # Get Parquet URIs from database
        products = self.get_raw_obs_products(quartet, with_parquet_sources=True)
        
        if not products:
            return pd.DataFrame()
        
        # Collect all Parquet URIs
        parquet_paths = []
        for product in products:
            for source in product.sources:
                if source.source_uri.endswith(".parquet"):
                    # Filter by interface
                    if interface and interface.roach_index is not None:
                        roach_indices = (
                            [interface.roach_index]
                            if isinstance(interface.roach_index, int)
                            else interface.roach_index
                        )
                        if source.meta.roach not in roach_indices:
                            continue
                    
                    # Convert URI to path
                    path = source.source_uri
                    if path.startswith("file://"):
                        path = path[7:]
                    parquet_paths.append(path)
        
        if not parquet_paths:
            return pd.DataFrame()
        
        # Use DuckDB to query Parquet files
        con = duckdb.connect(":memory:")
        
        # Build SELECT clause (detector columns)
        if data and data.detectors:
            det_cols = ", ".join([f"I_{d}, Q_{d}" for d in data.detectors])
            select_clause = f"timestamp, {det_cols}"
        else:
            select_clause = "*"
        
        # Build WHERE clause (time range)
        where_clauses = []
        if data and data.time_range:
            t_start, t_end = data.time_range
            where_clauses.append(f"timestamp BETWEEN {t_start} AND {t_end}")
        
        where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        
        # Query all Parquet files (pass list to DuckDB)
        query = f"""
        SELECT 
            {select_clause},
            filename
        FROM read_parquet({parquet_paths}, filename=true)
        {where_clause}
        """
        
        df = con.execute(query).df()
        con.close()
        
        return df

    def query_with_orm_metadata_duckdb(
        self,
        quartet: QuartetSpec,
        interface: InterfaceSpec | None = None,
    ) -> pd.DataFrame:
        """Query timestream with database metadata joined in DuckDB.
        
        This loads Parquet files discovered through the database and enriches
        them with metadata from DataProd using pandas operations.
        
        Parameters
        ----------
        quartet : QuartetSpec
            Raw observation quartet filter
        interface : InterfaceSpec | None
            Interface/roach filter
        
        Returns
        -------
        pd.DataFrame
            Timestream with database metadata columns
        """
        # Use hybrid query which already joins database metadata
        return self.query_hybrid(
            quartet=quartet,
            interface=interface,
            data=None,
            include_orm_metadata=True,
        )

    # =========================================================================
    # Strategy 2: ORM-First (Metadata, then Lazy Load)
    # =========================================================================
    
    def get_raw_obs_products(
        self,
        quartet: QuartetSpec,
        with_parquet_sources: bool = True,
    ) -> list[DataProd]:
        """Get raw observation products from database.
        
        This queries the database (DataProd + DataProdSource) to find available
        observations. The database is the source of truth for what exists.
        
        Parameters
        ----------
        quartet : QuartetSpec
            Raw observation quartet filter
        with_parquet_sources : bool
            Only return products that have Parquet sources
        
        Returns
        -------
        list[DataProd]
            List of DataProd objects with sources eager-loaded
        """
        # Build query with eager loading of sources
        from sqlalchemy.orm import selectinload
        
        stmt = (
            select(DataProd)
            .options(selectinload(DataProd.sources))
            .join(DataProd.data_prod_type)
            .where(DataProdType.label == "dp_raw_obs")
        )
        
        # Apply quartet filters
        if quartet.master:
            stmt = stmt.where(DataProd.meta["master"].as_string() == quartet.master)
        if quartet.obsnum is not None:
            stmt = stmt.where(DataProd.meta["obsnum"].as_integer() == quartet.obsnum)
        if quartet.subobsnum is not None:
            stmt = stmt.where(
                DataProd.meta["subobsnum"].as_integer() == quartet.subobsnum
            )
        if quartet.scannum is not None:
            stmt = stmt.where(DataProd.meta["scannum"].as_integer() == quartet.scannum)
        
        # Filter for products with Parquet sources if requested
        if with_parquet_sources:
            stmt = stmt.join(DataProd.sources).where(
                DataProdSource.source_uri.like("%.parquet")
            ).distinct()
        
        # Execute query
        results = self.session.execute(stmt).scalars().all()
        
        return results

    def get_raw_obs_metadata(
        self,
        quartet: QuartetSpec,
    ) -> list[RawObsMeta]:
        """Query ORM for raw observation metadata (backward compatibility).
        
        This is the first step in ORM-first strategy: get metadata,
        then load Parquet data on-demand for selected observations.
        
        Parameters
        ----------
        quartet : QuartetSpec
            Raw observation quartet filter
        
        Returns
        -------
        list[RawObsMeta]
            List of raw observation metadata matching filters
        """
        # Use new database-centric method
        products = self.get_raw_obs_products(quartet, with_parquet_sources=False)
        
        # Extract metadata (already RawObsMeta dataclass)
        return [product.meta for product in products]

    def load_parquet_for_product(
        self,
        product: DataProd,
        interface: InterfaceSpec | None = None,
        data: DataSpec | None = None,
    ) -> pd.DataFrame:
        """Load Parquet data for a DataProd using URIs from database.
        
        Queries database (DataProdSource) for Parquet file URIs, then loads data.
        This is the database-centric way to load Parquet data.
        
        Parameters
        ----------
        product : DataProd
            Data product with sources loaded
        interface : InterfaceSpec | None
            Interface/roach filter
        data : DataSpec | None
            Data selection (detectors, time range)
        
        Returns
        -------
        pd.DataFrame
            Timestream data for this product
        """
        # Get Parquet sources from database
        parquet_sources = [
            s for s in product.sources
            if s.source_uri.endswith(".parquet")
        ]
        
        if not parquet_sources:
            return pd.DataFrame()
        
        # Filter by interface if specified
        if interface:
            if interface.roach_index is not None:
                roach_indices = (
                    [interface.roach_index]
                    if isinstance(interface.roach_index, int)
                    else interface.roach_index
                )
                parquet_sources = [
                    s for s in parquet_sources
                    if s.meta and s.meta.roach in roach_indices
                ]
            elif interface.interface:
                interfaces = (
                    [interface.interface]
                    if isinstance(interface.interface, str)
                    else interface.interface
                )
                parquet_sources = [
                    s for s in parquet_sources
                    if s.meta and s.meta.interface in interfaces
                ]
        
        # Filter by file type if specified
        if data and data.file_types:
            parquet_sources = [
                s for s in parquet_sources
                if any(ft in s.source_uri for ft in data.file_types)
            ]
        
        if not parquet_sources:
            return pd.DataFrame()
        
        # Load data from URIs
        dfs = []
        for source in parquet_sources:
            # Convert URI to path (handle file:// prefix)
            path = source.source_uri
            if path.startswith("file://"):
                path = path[7:]  # Remove file:// prefix
            
            df = pd.read_parquet(path)
            
            # Apply detector filter if needed
            if data and data.detectors:
                cols = ["timestamp"]
                for det in data.detectors:
                    cols.extend([f"I_{det}", f"Q_{det}"])
                if "lofreq" in df.columns:
                    cols.append("lofreq")
                # Only select columns that exist
                cols = [c for c in cols if c in df.columns]
                df = df[cols]
            
            # Apply time filter if needed
            if data and data.time_range:
                t_start, t_end = data.time_range
                df = df[(df["timestamp"] >= t_start) & (df["timestamp"] <= t_end)]
            
            dfs.append(df)
        
        # Combine if multiple files
        if len(dfs) == 1:
            return dfs[0]
        else:
            return pd.concat(dfs, ignore_index=True)
    
    def load_parquet_for_quartet(
        self,
        meta: RawObsMeta,
        interface: InterfaceSpec | None = None,
        data: DataSpec | None = None,
    ) -> pd.DataFrame:
        """Load Parquet data for a quartet (backward compatibility).
        
        Second step in ORM-first strategy: given metadata from ORM,
        load the corresponding Parquet files.
        
        Parameters
        ----------
        meta : RawObsMeta
            Raw observation metadata from ORM
        interface : InterfaceSpec | None
            Interface/roach filter
        data : DataSpec | None
            Data selection
        
        Returns
        -------
        pd.DataFrame
            Timestream data for this quartet
        """
        # Query database for matching product
        quartet = QuartetSpec(
            master=meta.master,
            obsnum=meta.obsnum,
            subobsnum=meta.subobsnum,
            scannum=meta.scannum,
        )
        products = self.get_raw_obs_products(quartet, with_parquet_sources=True)
        
        if not products:
            return pd.DataFrame()
        
        # Load data from first matching product
        return self.load_parquet_for_product(products[0], interface, data)

    def get_parquet_uris_for_product(
        self,
        product: DataProd,
        interface: InterfaceSpec | None = None,
    ) -> list[str]:
        """Get Parquet URIs for a product from database.
        
        Queries database (DataProdSource) for Parquet file URIs without loading data.
        Useful for checking file availability or building query plans.
        
        Parameters
        ----------
        product : DataProd
            Data product with sources loaded
        interface : InterfaceSpec | None
            Interface/roach filter
        
        Returns
        -------
        list[str]
            List of Parquet source URIs from database
        """
        # Get Parquet sources from database
        parquet_sources = [
            s for s in product.sources
            if s.source_uri.endswith(".parquet")
        ]
        
        # Filter by interface if specified
        if interface:
            if interface.roach_index is not None:
                roach_indices = (
                    [interface.roach_index]
                    if isinstance(interface.roach_index, int)
                    else interface.roach_index
                )
                parquet_sources = [
                    s for s in parquet_sources
                    if s.meta and s.meta.roach in roach_indices
                ]
            elif interface.interface:
                interfaces = (
                    [interface.interface]
                    if isinstance(interface.interface, str)
                    else interface.interface
                )
                parquet_sources = [
                    s for s in parquet_sources
                    if s.meta and s.meta.interface in interfaces
                ]
        
        return [s.source_uri for s in parquet_sources]
    
    def get_parquet_paths_for_quartet(
        self,
        meta: RawObsMeta,
        interface: InterfaceSpec | None = None,
    ) -> list[Path]:
        """Get Parquet file paths for a quartet (backward compatibility).
        
        Parameters
        ----------
        meta : RawObsMeta
            Raw observation metadata
        interface : InterfaceSpec | None
            Interface/roach filter
        
        Returns
        -------
        list[Path]
            List of Parquet file paths
        """
        # Query database for matching product
        quartet = QuartetSpec(
            master=meta.master,
            obsnum=meta.obsnum,
            subobsnum=meta.subobsnum,
            scannum=meta.scannum,
        )
        products = self.get_raw_obs_products(quartet, with_parquet_sources=True)
        
        if not products:
            return []
        
        # Get URIs and convert to paths
        uris = self.get_parquet_uris_for_product(products[0], interface)
        paths = []
        for uri in uris:
            if uri.startswith("file://"):
                paths.append(Path(uri[7:]))
            else:
                paths.append(Path(uri))
        
        return paths

    # =========================================================================
    # Strategy 3: Hybrid DataFrame (Combine ORM + Parquet)
    # =========================================================================

    def query_hybrid(
        self,
        quartet: QuartetSpec,
        interface: InterfaceSpec | None = None,
        data: DataSpec | None = None,
        include_orm_metadata: bool = True,
    ) -> pd.DataFrame:
        """Query using hybrid approach: database metadata + Parquet data.
        
        This strategy:
        1. Queries database (DataProd) for products
        2. Loads corresponding Parquet files using URIs from database
        3. Combines them into a single DataFrame
        
        Parameters
        ----------
        quartet : QuartetSpec
            Raw observation quartet filter
        interface : InterfaceSpec | None
            Interface/roach filter
        data : DataSpec | None
            Data selection
        include_orm_metadata : bool
            Whether to include database metadata columns
        
        Returns
        -------
        pd.DataFrame
            Combined DataFrame with timestream and metadata
        """
        # Query database for products
        products = self.get_raw_obs_products(quartet, with_parquet_sources=True)
        
        if not products:
            return pd.DataFrame()
        
        # Load Parquet data for each product
        dfs = []
        for product in products:
            df = self.load_parquet_for_product(product, interface, data)
            
            if include_orm_metadata:
                # Add metadata columns from database
                meta = RawObsMeta(**product.meta)
                df["master"] = meta.master
                df["obsnum"] = meta.obsnum
                df["subobsnum"] = meta.subobsnum
                df["scannum"] = meta.scannum
                df["data_kind"] = meta.data_kind
                df["nw_id"] = meta.nw_id
            
            dfs.append(df)
        
        # Combine all DataFrames
        if len(dfs) == 1:
            return dfs[0]
        else:
            return pd.concat(dfs, ignore_index=True)

    def list_available_quartets(self) -> list[dict[str, Any]]:
        """List all available raw observation quartets from database.
        
        Queries database (DataProd) to find all available quartets with Parquet sources.
        
        Returns
        -------
        list[dict[str, Any]]
            List of quartet dictionaries with master, obsnum, subobsnum, scannum
        """
        # Query database for all RAW_OBS products with Parquet sources
        products = self.get_raw_obs_products(
            QuartetSpec(),  # No filtering - get all
            with_parquet_sources=True
        )
        
        # Extract quartet information from product metadata
        quartets = []
        for product in products:
            meta = product.meta  # Already RawObsMeta dataclass
            quartets.append({
                "master": meta.master,
                "obsnum": meta.obsnum,
                "subobsnum": meta.subobsnum,
                "scannum": meta.scannum,
            })
        
        return quartets


# =============================================================================
# Convenience Functions
# =============================================================================


def query_timestream(
    session: Session,
    master: str | None = None,
    obsnum: int | None = None,
    subobsnum: int | None = None,
    scannum: int | None = None,
    roach_index: int | list[int] | None = None,
    detectors: list[int] | None = None,
    strategy: Literal["duckdb", "orm_first", "hybrid"] = "duckdb",
) -> pd.DataFrame:
    """Convenience function for querying timestream data from database.
    
    Uses database (DataProd + DataProdSource) to discover Parquet files
    and load them. No filesystem paths required.
    
    Examples
    --------
    >>> # Query single interface
    >>> df = query_timestream(
    ...     session,
    ...     master="tcs", obsnum=113515, subobsnum=0, scannum=1,
    ...     roach_index=0
    ... )
    
    >>> # Query multiple interfaces
    >>> df = query_timestream(
    ...     session,
    ...     obsnum=113515,
    ...     roach_index=[0, 1, 2],
    ...     strategy="hybrid"
    ... )
    
    Parameters
    ----------
    session : Session
        SQLAlchemy session
    master : str | None
        Master filter
    obsnum : int | None
        Observation number filter
    subobsnum : int | None
        Sub-observation number filter
    scannum : int | None
        Scan number filter
    roach_index : int | list[int] | None
        Roach interface filter
    detectors : list[int] | None
        Detector indices to load
    strategy : Literal["duckdb", "orm_first", "hybrid"]
        Query strategy to use
    
    Returns
    -------
    pd.DataFrame
        Timestream data
    """
    query = ParquetORMQuery(session)
    
    quartet = QuartetSpec(
        master=master,
        obsnum=obsnum,
        subobsnum=subobsnum,
        scannum=scannum,
    )
    
    interface = InterfaceSpec(roach_index=roach_index) if roach_index else None
    data = DataSpec(detectors=detectors) if detectors else None
    
    if strategy == "duckdb":
        return query.query_timestream_duckdb(quartet, interface, data)
    elif strategy == "orm_first":
        products = query.get_raw_obs_products(quartet, with_parquet_sources=True)
        if not products:
            return pd.DataFrame()
        # Load first matching product
        return query.load_parquet_for_product(products[0], interface, data)
    elif strategy == "hybrid":
        return query.query_hybrid(quartet, interface, data)
    else:
        raise ValueError(f"Unknown strategy: {strategy}")
