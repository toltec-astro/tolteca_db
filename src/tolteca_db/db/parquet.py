"""Parquet file integration for DuckDB.

DuckDB native Parquet support enables zero-copy queries over external files.
This module provides helpers for querying Parquet data with metadata from tables.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
import pandas as pd

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from tolteca_db.models.orm import DataProdSource, Location

__all__ = ["ParquetQuery", "resolve_source_path"]


def resolve_source_path(source: DataProdSource, location: Location) -> str:
    """
    Resolve full path from Location + DataProdSource.

    Supports local files, S3 buckets, and HTTP URLs.

    Parameters
    ----------
    source : DataProdSource
        Source with relative URI
    location : Location
        Location with root URI

    Returns
    -------
    str
        Full path to Parquet file

    Examples
    --------
    >>> # Local filesystem
    >>> location.root_uri = "file:///mnt/lmt/data"
    >>> source.source_uri = "obs_1001.parquet"
    >>> resolve_source_path(source, location)
    '/mnt/lmt/data/obs_1001.parquet'

    >>> # S3 bucket
    >>> location.root_uri = "s3://toltec-archive/data"
    >>> source.source_uri = "obs_1001.parquet"
    >>> resolve_source_path(source, location)
    's3://toltec-archive/data/obs_1001.parquet'

    >>> # HTTP URL
    >>> location.root_uri = "https://data.example.com/toltec"
    >>> source.source_uri = "obs_1001.parquet"
    >>> resolve_source_path(source, location)
    'https://data.example.com/toltec/obs_1001.parquet'
    """
    # Get root URI
    root = location.root_uri

    # Remove file:// prefix for local paths
    if root.startswith("file://"):
        root = root.replace("file://", "")

    # Construct full path (works for local, S3, HTTP)
    return f"{root}/{source.source_uri}"


class ParquetQuery:
    """
    Query Parquet files with metadata from database.

    Leverages DuckDB's native Parquet support for zero-copy queries.
    Automatically resolves file paths using Location table.

    Parameters
    ----------
    duckdb_con : duckdb.DuckDBPyConnection
        DuckDB connection (can query Parquet directly)
    session : Session
        SQLAlchemy session for metadata queries

    Examples
    --------
    >>> import duckdb
    >>> from sqlalchemy import create_engine
    >>> from sqlalchemy.orm import Session
    >>>
    >>> # Create connections
    >>> engine = create_engine("duckdb:///metadata.duckdb")
    >>> duckdb_con = duckdb.connect("metadata.duckdb")
    >>> session = Session(engine)
    >>>
    >>> # Query Parquet via product ID
    >>> pq = ParquetQuery(duckdb_con, session)
    >>> df = pq.query_product_data(data_prod_pk=1001)
    >>> print(df.columns)
    Index(['obs_id', 'time', 'ra', 'dec', 'flux', 'noise', 'det_id'])

    >>> # Query with filters
    >>> df = pq.query_product_data(
    ...     data_prod_pk=1001,
    ...     filters="det_id BETWEEN 0 AND 12"
    ... )

    >>> # Query multiple products
    >>> df = pq.query_products_glob("data/obs_*.parquet")
    """

    def __init__(
        self,
        duckdb_con: duckdb.DuckDBPyConnection,
        session: Session,
    ):
        self.con = duckdb_con
        self.session = session

    def resolve_source(self, data_prod_fk: int, role: str = "PRIMARY") -> str:
        """
        Resolve Parquet file path for a data product.

        Parameters
        ----------
        data_prod_fk : int
            Data product foreign key
        role : str, optional
            Source role (PRIMARY, MIRROR, TEMP), by default "PRIMARY"

        Returns
        -------
        str
            Full path to Parquet file

        Raises
        ------
        ValueError
            If no source found for the product
        """
        from tolteca_db.models.orm import DataProdSource, Location

        # Get source
        source = (
            self.session.query(DataProdSource)
            .filter_by(data_prod_fk=data_prod_fk, role=role)
            .first()
        )

        if not source:
            msg = f"No {role} source for data_prod {data_prod_fk}"
            raise ValueError(msg)

        # Get location (FK constraint guarantees existence)
        location = self.session.get(Location, source.location_fk)
        assert location is not None

        # Resolve full path
        return resolve_source_path(source, location)

    def query_product_data(
        self,
        data_prod_pk: int,
        columns: str = "*",
        filters: str | None = None,
        role: str = "PRIMARY",
    ) -> pd.DataFrame:
        """
        Query Parquet data for a data product.

        Zero-copy query - DuckDB reads Parquet directly without loading into tables.

        Parameters
        ----------
        data_prod_pk : int
            Data product primary key
        columns : str, optional
            Column selection (SQL SELECT clause), by default "*"
        filters : str | None, optional
            SQL WHERE clause (without WHERE keyword), by default None
        role : str, optional
            Source role, by default "PRIMARY"

        Returns
        -------
        pd.DataFrame
            Query results as pandas DataFrame

        Examples
        --------
        >>> # Get all columns
        >>> df = pq.query_product_data(1001)

        >>> # Select specific columns
        >>> df = pq.query_product_data(1001, columns="time, flux, det_id")

        >>> # Filter data
        >>> df = pq.query_product_data(
        ...     1001,
        ...     filters="det_id < 10 AND flux > 0.01"
        ... )

        >>> # Get reduced maps
        >>> df = pq.query_product_data(
        ...     5001,
        ...     columns="map_id, signal, noise",
        ...     role="PRIMARY"
        ... )
        """
        # Resolve path
        path = self.resolve_source(data_prod_pk, role)

        # Build query
        query = f"SELECT {columns} FROM '{path}'"
        if filters:
            query += f" WHERE {filters}"

        # Execute and return DataFrame
        return self.con.execute(query).df()

    def query_products_glob(
        self,
        glob_pattern: str,
        columns: str = "*",
        filters: str | None = None,
    ) -> pd.DataFrame:
        """
        Query multiple Parquet files using glob pattern.

        DuckDB automatically combines files matching the pattern.

        Parameters
        ----------
        glob_pattern : str
            Glob pattern for Parquet files (e.g., "data/obs_*.parquet")
        columns : str, optional
            Column selection, by default "*"
        filters : str | None, optional
            SQL WHERE clause, by default None

        Returns
        -------
        pd.DataFrame
            Combined results from all matching files

        Examples
        --------
        >>> # Query all observations
        >>> df = pq.query_products_glob("data/obs_*.parquet")

        >>> # Query specific detector
        >>> df = pq.query_products_glob(
        ...     "data/obs_*.parquet",
        ...     filters="det_id = 5"
        ... )

        >>> # Query with aggregation
        >>> df = pq.query_products_glob(
        ...     "data/obs_*.parquet",
        ...     columns="obs_id, AVG(flux) as mean_flux",
        ... )
        """
        # Build query
        query = f"SELECT {columns} FROM '{glob_pattern}'"
        if filters:
            query += f" WHERE {filters}"

        return self.con.execute(query).df()

    def join_metadata_with_data(
        self,
        metadata_table: str = "data_prod",
        data_glob: str = "data/obs_*.parquet",
        join_key: str = "pk",
        parquet_key: str = "obs_id",
        columns: str = "*",
        filters: str | None = None,
    ) -> pd.DataFrame:
        """
        Join metadata from tables with data from Parquet files.

        This is the hybrid query pattern: metadata in DuckDB tables,
        raw data in Parquet files, joined seamlessly.

        Parameters
        ----------
        metadata_table : str, optional
            Table name for metadata, by default "data_prod"
        data_glob : str, optional
            Glob pattern for Parquet files, by default "data/obs_*.parquet"
        join_key : str, optional
            Join key from metadata table, by default "pk"
        parquet_key : str, optional
            Join key from Parquet files, by default "obs_id"
        columns : str, optional
            Column selection, by default "*"
        filters : str | None, optional
            SQL WHERE clause, by default None

        Returns
        -------
        pd.DataFrame
            Joined results

        Examples
        --------
        >>> # Join metadata with observations
        >>> df = pq.join_metadata_with_data(
        ...     metadata_table="data_prod",
        ...     data_glob="data/obs_*.parquet",
        ...     columns="dp.label, dp.meta, obs.*"
        ... )

        >>> # Filter by product type
        >>> df = pq.join_metadata_with_data(
        ...     columns="dp.label, obs.time, obs.flux",
        ...     filters="dp.data_prod_type_fk = 1"
        ... )
        """
        query = f"""
        SELECT {columns}
        FROM {metadata_table} dp
        JOIN '{data_glob}' obs
          ON obs.{parquet_key} = dp.{join_key}
        """

        if filters:
            query += f" WHERE {filters}"

        return self.con.execute(query).df()

    def create_view(self, view_name: str, parquet_path: str) -> None:
        """
        Create reusable view over Parquet file(s).

        Views are stored as queries, not data - zero storage overhead.

        Parameters
        ----------
        view_name : str
            Name for the view
        parquet_path : str
            Path or glob pattern for Parquet files

        Examples
        --------
        >>> # Create view for raw observations
        >>> pq.create_view("raw_observations", "data/obs_*.parquet")
        >>> df = pq.con.execute("SELECT * FROM raw_observations").df()

        >>> # Create view for reduced maps
        >>> pq.create_view("reduced_maps", "scratch/maps_*.parquet")
        >>> df = pq.con.execute(
        ...     "SELECT AVG(signal) FROM reduced_maps GROUP BY map_id"
        ... ).df()
        """
        query = f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM '{parquet_path}'"
        self.con.execute(query)

    def export_to_parquet(
        self,
        query: str,
        output_path: str,
        compression: str = "zstd",
        partition_by: str | None = None,
    ) -> None:
        """
        Export query results to Parquet file.

        Parameters
        ----------
        query : str
            SQL query to execute
        output_path : str
            Output Parquet file path
        compression : str, optional
            Compression codec (zstd, gzip, snappy), by default "zstd"
        partition_by : str | None, optional
            Column(s) to partition by, by default None

        Examples
        --------
        >>> # Export reduction results
        >>> pq.export_to_parquet(
        ...     "SELECT * FROM reduction_output WHERE task_id = 5001",
        ...     "scratch/maps_5001.parquet"
        ... )

        >>> # Export with partitioning
        >>> pq.export_to_parquet(
        ...     "SELECT * FROM observations",
        ...     "data/obs_archive.parquet",
        ...     partition_by="obs_id"
        ... )
        """
        copy_query = f"""
        COPY ({query})
        TO '{output_path}'
        (FORMAT PARQUET, COMPRESSION {compression.upper()}
        """

        if partition_by:
            copy_query += f", PARTITION_BY ({partition_by})"

        copy_query += ")"

        self.con.execute(copy_query)
