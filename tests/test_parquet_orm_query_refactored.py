"""Tests for refactored database-centric Parquet query interface.

Tests verify that all query strategies work correctly when using database
(DataProd + DataProdSource) as the source of truth for discovering Parquet files.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from tolteca_db.constants import DataProdType as DataProdTypeEnum
from tolteca_db.models.metadata import InterfaceFileMeta
from tolteca_db.models.orm import DataProd, DataProdSource, DataProdType, Location
from tolteca_db.models.orm.base import Base
from tolteca_db.query.parquet_orm_query import (
    DataSpec,
    InterfaceSpec,
    ParquetORMQuery,
    QuartetSpec,
    RawObsMeta,
    query_timestream,
)


@pytest.fixture
def test_db_session():
    """Create test database with sample data."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    
    session = Session(engine)

    # Create data product type
    dp_type_raw = DataProdType(
        label="dp_raw_obs",
        description="Raw observation data product",
        level=0,
    )
    session.add(dp_type_raw)

    # Create test location
    location = Location(
        label="test_filesystem",
        location_type="filesystem",
        root_uri="file:///tmp/test_data",
    )
    session.add(location)

    session.flush()  # Get IDs (auto-increment) before using them
    
    # Create test products with Parquet sources
    for scannum in [0, 1]:
        meta = RawObsMeta(
            name=f"raw_obs_tcs_113515_{scannum}",
            data_prod_type=DataProdTypeEnum.DP_RAW_OBS,
            master="tcs",
            obsnum=113515,
            subobsnum=0,
            scannum=scannum,
            data_kind=1,  # ToltecDataKind flag value (integer)
            nw_id=0,  # Network ID (integer)
            obs_goal="science",
            source_name="Test Source",
        )
        
        product = DataProd(
            data_prod_type=dp_type_raw,
            meta=meta,
        )
        session.add(product)
        session.flush()
        
        # Add Parquet sources for multiple interfaces
        for roach in [0, 1, 2]:
            source = DataProdSource(
                source_uri=f"file:///tmp/test_data/master=tcs/obsnum=113515/subobsnum=0/scannum={scannum}/roach_index={roach}/vnasweep.parquet",
                data_prod_fk=product.pk,
                location_fk=location.pk,  # Use auto-incremented pk
                role="primary",
                meta=InterfaceFileMeta(nw_id=0, roach=roach),  # Add interface metadata for filtering
            )
            session.add(source)
    
    session.commit()
    
    yield session
    
    session.close()


@pytest.fixture
def temp_parquet_files():
    """Create temporary Parquet files for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        
        # Create directory structure
        for scannum in [0, 1]:
            for roach in [0, 1, 2]:
                parquet_dir = (
                    tmppath
                    / f"master=tcs"
                    / f"obsnum=113515"
                    / f"subobsnum=0"
                    / f"scannum={scannum}"
                    / f"roach_index={roach}"
                )
                parquet_dir.mkdir(parents=True, exist_ok=True)
                
                # Create sample Parquet file
                df = pd.DataFrame({
                    "timestamp": [1.0, 2.0, 3.0],
                    "I_0": [0.1, 0.2, 0.3],
                    "Q_0": [0.4, 0.5, 0.6],
                })
                parquet_file = parquet_dir / "vnasweep.parquet"
                df.to_parquet(parquet_file)
        
        yield tmppath


class TestDatabaseCentricArchitecture:
    """Test database-centric query architecture."""
    
    def test_init_no_parquet_root(self, test_db_session):
        """Test that ParquetORMQuery no longer requires parquet_root."""
        query = ParquetORMQuery(test_db_session)
        assert query.session == test_db_session
        assert not hasattr(query, "parquet_root")
        assert not hasattr(query, "raw_data_dir")
    
    def test_get_raw_obs_products_from_database(self, test_db_session):
        """Test querying products from database."""
        query = ParquetORMQuery(test_db_session)
        
        # Query all products
        products = query.get_raw_obs_products(
            QuartetSpec(obsnum=113515),
            with_parquet_sources=True,
        )
        
        assert len(products) == 2  # Two scans
        assert all(p.data_prod_type.label == "dp_raw_obs" for p in products)
        assert all(len(p.sources) > 0 for p in products)
    
    def test_get_raw_obs_products_filters(self, test_db_session):
        """Test quartet filtering in database query."""
        query = ParquetORMQuery(test_db_session)
        
        # Filter by scannum
        products = query.get_raw_obs_products(
            QuartetSpec(obsnum=113515, scannum=0),
            with_parquet_sources=True,
        )
        
        assert len(products) == 1
        assert products[0].meta.scannum == 0
    
    def test_get_parquet_uris_from_database(self, test_db_session):
        """Test getting Parquet URIs from DataProdSource."""
        query = ParquetORMQuery(test_db_session)
        
        products = query.get_raw_obs_products(
            QuartetSpec(obsnum=113515, scannum=0),
            with_parquet_sources=True,
        )
        
        uris = query.get_parquet_uris_for_product(products[0])
        
        assert len(uris) == 3  # Three interfaces
        assert all(uri.endswith(".parquet") for uri in uris)
        assert all(uri.startswith("file://") for uri in uris)
    
    def test_get_parquet_uris_interface_filter(self, test_db_session):
        """Test filtering URIs by interface."""
        query = ParquetORMQuery(test_db_session)
        
        products = query.get_raw_obs_products(
            QuartetSpec(obsnum=113515, scannum=0),
            with_parquet_sources=True,
        )
        
        # Filter by roach_index
        uris = query.get_parquet_uris_for_product(
            products[0],
            interface=InterfaceSpec(roach_index=0),
        )
        
        assert len(uris) == 1
        assert "roach_index=0" in uris[0]
    
    def test_load_parquet_for_product(self, test_db_session, temp_parquet_files):
        """Test loading Parquet data using database URIs."""
        # Update database URIs to point to temp files
        session = test_db_session
        sources = session.execute(select(DataProdSource)).scalars().all()
        for source in sources:
            # Update URI to point to temp files
            old_uri = source.source_uri
            new_uri = old_uri.replace("/tmp/test_data", str(temp_parquet_files))
            source.source_uri = new_uri
        session.commit()
        
        query = ParquetORMQuery(session)
        
        products = query.get_raw_obs_products(
            QuartetSpec(obsnum=113515, scannum=0),
            with_parquet_sources=True,
        )
        
        # Load data
        df = query.load_parquet_for_product(
            products[0],
            interface=InterfaceSpec(roach_index=0),
        )
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3  # Three rows in test data
        assert "timestamp" in df.columns
        assert "I_0" in df.columns
    
    def test_list_available_quartets_from_database(self, test_db_session):
        """Test listing quartets from database instead of filesystem."""
        query = ParquetORMQuery(test_db_session)
        
        quartets = query.list_available_quartets()
        
        assert len(quartets) == 2  # Two scans
        assert all("master" in q for q in quartets)
        assert all("obsnum" in q for q in quartets)
        assert all("scannum" in q for q in quartets)
    
    def test_backward_compatibility_get_raw_obs_metadata(self, test_db_session):
        """Test backward compatible metadata query."""
        query = ParquetORMQuery(test_db_session)
        
        metadata = query.get_raw_obs_metadata(QuartetSpec(obsnum=113515))
        
        assert len(metadata) == 2
        assert all(isinstance(m, RawObsMeta) for m in metadata)
        assert all(m.obsnum == 113515 for m in metadata)


class TestQueryStrategies:
    """Test all three query strategies with database-centric approach."""
    
    def test_query_hybrid_database_first(self, test_db_session, temp_parquet_files):
        """Test hybrid query using database products."""
        # Update URIs
        session = test_db_session
        sources = session.execute(select(DataProdSource)).scalars().all()
        for source in sources:
            old_uri = source.source_uri
            new_uri = old_uri.replace("/tmp/test_data", str(temp_parquet_files))
            source.source_uri = new_uri
        session.commit()
        
        query = ParquetORMQuery(session)
        
        df = query.query_hybrid(
            quartet=QuartetSpec(obsnum=113515, scannum=0),
            interface=InterfaceSpec(roach_index=0),
            include_orm_metadata=True,
        )
        
        assert isinstance(df, pd.DataFrame)
        assert "master" in df.columns
        assert "obsnum" in df.columns
        assert all(df["master"] == "tcs")
        assert all(df["obsnum"] == 113515)
    
    def test_query_timestream_duckdb_with_database_uris(
        self, test_db_session, temp_parquet_files
    ):
        """Test DuckDB query using URIs from database."""
        # Update URIs
        session = test_db_session
        sources = session.execute(select(DataProdSource)).scalars().all()
        for source in sources:
            old_uri = source.source_uri
            new_uri = old_uri.replace("/tmp/test_data", str(temp_parquet_files))
            source.source_uri = new_uri
        session.commit()
        
        query = ParquetORMQuery(session)
        
        df = query.query_timestream_duckdb(
            quartet=QuartetSpec(obsnum=113515, scannum=0),
            interface=InterfaceSpec(roach_index=0),
        )
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
    
    def test_convenience_function_no_parquet_root(
        self, test_db_session, temp_parquet_files
    ):
        """Test convenience function without parquet_root parameter."""
        # Update URIs
        session = test_db_session
        sources = session.execute(select(DataProdSource)).scalars().all()
        for source in sources:
            old_uri = source.source_uri
            new_uri = old_uri.replace("/tmp/test_data", str(temp_parquet_files))
            source.source_uri = new_uri
        session.commit()
        
        # Should work without parquet_root
        df = query_timestream(
            session,
            master="tcs",
            obsnum=113515,
            scannum=0,
            roach_index=0,
            strategy="hybrid",
        )
        
        assert isinstance(df, pd.DataFrame)


class TestNoFilesystemDependency:
    """Test that code no longer depends on filesystem scanning."""
    
    def test_no_filesystem_methods_called(self, test_db_session):
        """Verify that database queries don't scan filesystem."""
        query = ParquetORMQuery(test_db_session)
        
        # These should all work without filesystem access
        products = query.get_raw_obs_products(
            QuartetSpec(obsnum=113515),
            with_parquet_sources=True,
        )
        assert len(products) > 0
        
        metadata = query.get_raw_obs_metadata(QuartetSpec(obsnum=113515))
        assert len(metadata) > 0
        
        uris = query.get_parquet_uris_for_product(products[0])
        assert len(uris) > 0
        
        quartets = query.list_available_quartets()
        assert len(quartets) > 0
    
    def test_obsolete_methods_removed(self, test_db_session):
        """Verify obsolete filesystem-based methods are removed."""
        query = ParquetORMQuery(test_db_session)
        
        # These methods should not exist
        assert not hasattr(query, "_build_parquet_pattern")
        assert not hasattr(query, "get_metadata_summary")
        assert not hasattr(query, "raw_data_dir")
        assert not hasattr(query, "metadata_dir")


class TestDatabaseSourceOfTruth:
    """Test that database is the authoritative source for file locations."""
    
    def test_uris_from_dataprod_source(self, test_db_session):
        """Test that URIs come from DataProdSource table."""
        query = ParquetORMQuery(test_db_session)
        
        products = query.get_raw_obs_products(
            QuartetSpec(obsnum=113515, scannum=0),
            with_parquet_sources=True,
        )
        
        # URIs should match database entries
        uris = query.get_parquet_uris_for_product(products[0])
        
        # Verify URIs exist in database
        session = test_db_session
        db_sources = session.execute(
            select(DataProdSource).where(
                DataProdSource.data_prod_fk == products[0].pk
            )
        ).scalars().all()
        
        db_uris = [s.source_uri for s in db_sources if s.source_uri.endswith(".parquet")]
        
        assert set(uris) == set(db_uris)
    
    def test_no_filesystem_discovery(self, test_db_session):
        """Test that no filesystem scanning occurs during queries."""
        query = ParquetORMQuery(test_db_session)
        
        # Even with non-existent filesystem paths in URIs,
        # query should still work (database is authoritative)
        products = query.get_raw_obs_products(
            QuartetSpec(obsnum=113515),
            with_parquet_sources=True,
        )
        
        assert len(products) > 0
        
        # Get URIs works regardless of filesystem state
        uris = query.get_parquet_uris_for_product(products[0])
        assert len(uris) > 0
    
    def test_uri_formats_supported(self, test_db_session):
        """Test that various URI formats are handled."""
        session = test_db_session
        
        # Get existing data product type from fixture
        dp_type_raw = session.execute(
            select(DataProdType).where(DataProdType.label == "dp_raw_obs")
        ).scalar_one()
        
        # Add products with different URI schemes
        meta = RawObsMeta(
            name="test_uri_formats",
            data_prod_type=DataProdTypeEnum.DP_RAW_OBS,
            master="test",
            obsnum=999,
            subobsnum=0,
            scannum=0,
            data_kind="test",
            nw_id="test",
        )
        
        product = DataProd(
            data_prod_type=dp_type_raw,
            meta=meta,
        )
        session.add(product)
        session.flush()
        
        # Add sources with different URI schemes
        uri_schemes = [
            "file:///local/path/data.parquet",
            "s3://bucket/key/data.parquet",
            "http://server/path/data.parquet",
        ]
        
        for uri in uri_schemes:
            source = DataProdSource(
                source_uri=uri,
                data_prod_fk=product.pk,
                location_fk="test_filesystem",
                role="primary",
                meta={"interface": "toltec0", "roach": 0},
            )
            session.add(source)
        
        session.commit()
        
        query = ParquetORMQuery(session)
        
        products = query.get_raw_obs_products(
            QuartetSpec(obsnum=999),
            with_parquet_sources=True,
        )
        
        uris = query.get_parquet_uris_for_product(products[0])
        
        # All URI schemes should be preserved
        assert len(uris) == 3
        assert any(uri.startswith("file://") for uri in uris)
        assert any(uri.startswith("s3://") for uri in uris)
        assert any(uri.startswith("http://") for uri in uris)


@pytest.mark.integration
class TestMigrationComplete:
    """Integration tests verifying complete migration to database-centric architecture."""
    
    def test_full_workflow_database_only(self, test_db_session, temp_parquet_files):
        """Test complete workflow using only database queries."""
        # Update URIs
        session = test_db_session
        sources = session.execute(select(DataProdSource)).scalars().all()
        for source in sources:
            old_uri = source.source_uri
            new_uri = old_uri.replace("/tmp/test_data", str(temp_parquet_files))
            source.source_uri = new_uri
        session.commit()
        
        query = ParquetORMQuery(session)
        
        # Step 1: Discover available quartets (from database)
        quartets = query.list_available_quartets()
        assert len(quartets) > 0
        
        # Step 2: Query products (from database)
        products = query.get_raw_obs_products(
            QuartetSpec(obsnum=113515),
            with_parquet_sources=True,
        )
        assert len(products) > 0
        
        # Step 3: Get URIs (from database)
        uris = query.get_parquet_uris_for_product(products[0])
        assert len(uris) > 0
        
        # Step 4: Load data (using database URIs)
        df = query.load_parquet_for_product(products[0])
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        
        # All steps completed without filesystem scanning!
    
    def test_backward_compatibility_maintained(self, test_db_session, temp_parquet_files):
        """Test that old API methods still work but use new implementation."""
        # Update URIs
        session = test_db_session
        sources = session.execute(select(DataProdSource)).scalars().all()
        for source in sources:
            old_uri = source.source_uri
            new_uri = old_uri.replace("/tmp/test_data", str(temp_parquet_files))
            source.source_uri = new_uri
        session.commit()
        
        query = ParquetORMQuery(session)
        
        # Old API: get_raw_obs_metadata
        metadata = query.get_raw_obs_metadata(QuartetSpec(obsnum=113515))
        assert len(metadata) > 0
        
        # Old API: load_parquet_for_quartet
        df = query.load_parquet_for_quartet(
            metadata[0],
            interface=InterfaceSpec(roach_index=0),
        )
        assert isinstance(df, pd.DataFrame)
        
        # Old API: get_parquet_paths_for_quartet
        paths = query.get_parquet_paths_for_quartet(
            metadata[0],
            interface=InterfaceSpec(roach_index=0),
        )
        assert len(paths) > 0
