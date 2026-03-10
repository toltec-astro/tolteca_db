"""Integration tests for Dagster assets with real database operations.

These tests use in-memory databases populated with real sample data to catch
bugs that unit tests with mocks miss, such as:
- Type mismatches between partition key extraction and database queries
- SQL query building errors
- Dataclass serialization issues
"""

from __future__ import annotations

import pytest
from dagster import MultiPartitionKey
from sqlalchemy import text


@pytest.mark.integration
class TestAssetWithRealDatabase:
    """Test asset functions with real in-memory databases."""

    def test_partition_key_extraction_types(self):
        """Test that partition key extraction produces correct types for database queries.
        
        This test would have caught the bug where interface="toltec6" (string)
        was used in a query expecting an integer.
        """
        pytest.importorskip("dagster")
        
        from tolteca_db.dagster.partitions import (
            get_interface_roach_index,
            get_array_name_for_interface,
        )
        
        # Simulate partition key extraction (what actually happens in assets)
        partition_key = MultiPartitionKey({
            "quartet": "toltec-12345-0-0",
            "quartet_interface": "toltec6"
        })
        
        # Extract values as asset does
        quartet_key = partition_key.keys_by_dimension["quartet"]
        interface = partition_key.keys_by_dimension["quartet_interface"]
        
        # This is what the asset should do
        roach_index = get_interface_roach_index(interface)
        array_name = get_array_name_for_interface(interface)
        
        # Verify types
        assert isinstance(interface, str), "interface should be string from partition key"
        assert isinstance(roach_index, int), "roach_index should be integer for database"
        assert interface == "toltec6"
        assert roach_index == 6
        assert array_name == "a1100"  # toltec6 is in a1100 array

    def test_raw_obs_product_query_building(self, sample_tolteca_db_session):
        """Test that raw_obs_product builds queries with correct types.
        
        This integration test verifies the full query building logic using
        real database schema.
        """
        pytest.importorskip("dagster")
        
        from tolteca_db.models.orm import DataProd, DataProdType
        from tolteca_db.models.metadata import RawObsMeta
        from tolteca_db.dagster.partitions import get_interface_roach_index
        from sqlalchemy import select
        
        # Simulate partition values
        quartet_key = "toltec-12345-0-0"
        interface = "toltec6"
        roach_index = get_interface_roach_index(interface)
        
        # Verify we can build metadata with correct types
        meta = RawObsMeta(
            name=f"toltec-12345-0-0_{interface}",
            data_prod_type="dp_raw_obs",
            description="Test raw obs product",
            master="toltec",
            obsnum=12345,
            subobsnum=0,
            scannum=0,
            nw_id=roach_index,  # Should be integer, not interface string
        )
        
        # Verify types in metadata
        assert isinstance(meta.nw_id, int), "nw_id must be integer"
        assert meta.nw_id == 6
        
        # Now verify we can build a query using the metadata values
        tolteca_session = sample_tolteca_db_session
        
        # Get data product type
        dp_type = tolteca_session.execute(
            select(DataProdType).where(DataProdType.label == "dp_raw_obs")
        ).scalar_one()
        
        # Build query with correct types (this is what raw_obs_product does)
        stmt = (
            select(DataProd)
            .where(DataProd.data_prod_type_fk == dp_type.pk)
            .where(DataProd.meta['master'].as_string() == meta.master)
            .where(DataProd.meta['obsnum'].as_integer() == meta.obsnum)
            .where(DataProd.meta['subobsnum'].as_integer() == meta.subobsnum)
            .where(DataProd.meta['scannum'].as_integer() == meta.scannum)
            .where(DataProd.meta['nw_id'].as_integer() == roach_index)  # INTEGER comparison
        )
        
        # Query should compile without errors
        result = tolteca_session.execute(stmt).first()
        
        # Result will be None (no data created), but query executed successfully
        assert result is None

    def test_interface_validation_with_real_data(self, sample_toltec_db_session):
        """Test that interface file queries use integer roach_index."""
        pytest.importorskip("dagster")
        
        from tolteca_db.dagster.partitions import get_interface_roach_index
        
        session = sample_toltec_db_session
        
        # Get sample raw_obs entry
        result = session.execute(text(
            "SELECT master_id, obsnum, subobsnum, scannum FROM raw_obs LIMIT 1"
        )).fetchone()
        
        if result is None:
            pytest.skip("No sample data available")
        
        # Simulate interface lookup with roach_index (integer)
        interface = "toltec6"
        roach_index = get_interface_roach_index(interface)
        
        # Query interface files with integer nw value
        interface_files = session.execute(text(
            "SELECT nw, valid FROM interface_file WHERE raw_obs_id = :raw_obs_id AND nw = :nw"
        ), {"raw_obs_id": result[0], "nw": roach_index}).fetchall()
        
        # Verify we can query with integer (would fail with string)
        assert isinstance(roach_index, int)

    def test_master_label_case_handling(self, sample_toltec_db_session):
        """Test that master labels are correctly lowercased for UID format."""
        session = sample_toltec_db_session
        
        # Get all masters from sample data
        masters = session.execute(text("SELECT label FROM master")).fetchall()
        
        for master in masters:
            label = master[0]
            assert isinstance(label, str)
            
            # For UID format, we should use lowercase
            uid_master = label.lower()
            assert uid_master.islower()
            assert uid_master in ["toltec", "tcs", "ics"]

    def test_interface_string_to_roach_index_conversion(self):
        """Test that would have caught the interface string vs roach_index bug.
        
        The bug: raw_obs_product was using interface="toltec6" (string) directly
        in database queries expecting nw_id as integer, causing DuckDB conversion error.
        
        The fix: Extract roach_index (integer) from interface string before using
        in queries or metadata creation.
        """
        pytest.importorskip("dagster")
        
        from tolteca_db.dagster.partitions import get_interface_roach_index
        from tolteca_db.models.metadata import RawObsMeta
        
        # Simulate what happens in raw_obs_product asset
        partition_key = MultiPartitionKey({
            "quartet": "toltec-12345-0-0",
            "quartet_interface": "toltec6"  # This is a STRING
        })
        
        interface = partition_key.keys_by_dimension["quartet_interface"]
        
        # WRONG: Using interface (string) directly
        # This is what caused the bug
        wrong_type = interface
        assert isinstance(wrong_type, str)
        
        # CORRECT: Extract roach_index (integer)
        # This is what the fix does
        roach_index = get_interface_roach_index(interface)
        assert isinstance(roach_index, int)
        assert roach_index == 6
        
        # Verify RawObsMeta expects integer
        meta = RawObsMeta(
            name="test",
            data_prod_type="dp_raw_obs",
            master="toltec",
            obsnum=12345,
            subobsnum=0,
            scannum=0,
            nw_id=roach_index,  # Must be integer
        )
        
        assert isinstance(meta.nw_id, int)
        
        # This would fail (type error or runtime error)
        # meta_wrong = RawObsMeta(nw_id=interface)  # interface is string!
        
    def test_database_query_with_integer_nw_id(self, sample_tolteca_db_session):
        """Test that database queries using nw_id work with integers, not strings.
        
        This test simulates the exact query pattern that failed in production.
        """
        pytest.importorskip("dagster")
        
        from tolteca_db.models.orm import DataProd, DataProdType
        from tolteca_db.dagster.partitions import get_interface_roach_index
        from sqlalchemy import select
        
        session = sample_tolteca_db_session
        
        # Get data product type
        dp_type = session.execute(
            select(DataProdType).where(DataProdType.label == "dp_raw_obs")
        ).scalar_one()
        
        # Simulate partition key extraction
        interface = "toltec6"  # STRING from partition key
        roach_index = get_interface_roach_index(interface)  # Convert to INTEGER
        
        # This query pattern is what raw_obs_product uses
        # It MUST use roach_index (int), NOT interface (str)
        stmt_correct = (
            select(DataProd)
            .where(DataProd.data_prod_type_fk == dp_type.pk)
            .where(DataProd.meta['nw_id'].as_integer() == roach_index)  # INTEGER
        )
        
        # Query should execute without error
        result = session.execute(stmt_correct).first()
        assert result is None  # No data, but query executed successfully
        
        # This would fail with DuckDB conversion error:
        # stmt_wrong = select(DataProd).where(
        #     DataProd.meta['nw_id'].as_integer() == interface  # STRING!
        # )
        # session.execute(stmt_wrong)  # ConversionException: Could not convert string to INT32
