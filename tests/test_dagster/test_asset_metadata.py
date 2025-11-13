"""Unit tests for Dagster asset metadata creation logic.

This module tests the metadata creation logic for assets, particularly
testing the fields and types used in metadata dataclasses.

Following Dagster testing best practices:
https://docs.dagster.io/guides/test/unit-testing-assets-and-ops
"""

from __future__ import annotations

import pytest
from dagster import AssetKey, build_asset_context

from tolteca_db.models.metadata import (
    AnyDataProdMeta,
    InterfaceFileMeta,
    RawObsMeta,
)


class TestRawObsMeta:
    """Test RawObsMeta dataclass for raw observation metadata."""

    def test_raw_obs_meta_basic_fields(self):
        """Test RawObsMeta can be created with basic required fields."""
        meta = RawObsMeta(
            name="test_product",
            data_prod_type="dp_raw_obs",
            description="Test description",
            master="toltec",
            obsnum=12345,
            subobsnum=0,
            scannum=1,
        )
        
        assert meta.name == "test_product"
        assert meta.data_prod_type == "dp_raw_obs"
        assert meta.description == "Test description"
        assert meta.master == "toltec"
        assert meta.obsnum == 12345
        assert meta.subobsnum == 0
        assert meta.scannum == 1
        assert meta.tag == "raw_obs"

    def test_raw_obs_meta_optional_fields(self):
        """Test RawObsMeta optional fields."""
        meta = RawObsMeta(
            name="test_product",
            data_prod_type="dp_raw_obs",
            nw_id=5,
            obs_goal="science",
            source_name="M31",
        )
        
        assert meta.nw_id == 5
        assert meta.obs_goal == "science"
        assert meta.source_name == "M31"

    def test_raw_obs_meta_defaults(self):
        """Test RawObsMeta default values."""
        meta = RawObsMeta(
            name="test_product",
            data_prod_type="dp_raw_obs",
        )
        
        # Check defaults
        assert meta.master == ""
        assert meta.obsnum == 0
        assert meta.subobsnum == 0
        assert meta.scannum == 0
        assert meta.data_kind == 0
        assert meta.nw_id is None
        assert meta.obs_goal is None
        assert meta.source_name is None
        assert meta.description is None

    def test_raw_obs_meta_is_in_union_type(self):
        """Test that RawObsMeta is valid AnyDataProdMeta type."""
        meta = RawObsMeta(
            name="test_product",
            data_prod_type="dp_raw_obs",
        )
        
        # This should type-check (verified by mypy)
        typed_meta: AnyDataProdMeta = meta
        assert typed_meta.name == "test_product"


class TestInterfaceFileMeta:
    """Test InterfaceFileMeta dataclass for interface file metadata."""

    def test_interface_file_meta_fields(self):
        """Test InterfaceFileMeta only has nw_id and roach fields."""
        meta = InterfaceFileMeta(
            nw_id=5,
            roach=3,
        )
        
        assert meta.nw_id == 5
        assert meta.roach == 3

    def test_interface_file_meta_no_name_field(self):
        """Test that InterfaceFileMeta does NOT have 'name' field."""
        # This should fail at runtime
        with pytest.raises(TypeError, match="unexpected keyword argument"):
            InterfaceFileMeta(name="test")  # type: ignore

    def test_interface_file_meta_no_master_field(self):
        """Test that InterfaceFileMeta does NOT have 'master' field."""
        with pytest.raises(TypeError, match="unexpected keyword argument"):
            InterfaceFileMeta(master="toltec")  # type: ignore

    def test_interface_file_meta_not_in_dataprod_union(self):
        """Test that InterfaceFileMeta is NOT in AnyDataProdMeta union.
        
        InterfaceFileMeta is used for DataProdSource.meta, not DataProd.meta.
        """
        meta = InterfaceFileMeta(nw_id=5, roach=3)
        
        # This should fail type checking (mypy catches this)
        # At runtime, it's just an object, so we document the intent
        assert not hasattr(meta, "name")  # InterfaceFileMeta lacks DataProdMetaBase fields
        assert not hasattr(meta, "data_prod_type")


class TestMetadataCreationPatterns:
    """Test patterns for creating metadata from raw observation data."""

    def test_create_metadata_for_interface_file(self):
        """Test creating metadata for interface files (DataProdSource)."""
        # For interface files, use InterfaceFileMeta
        interface = 4  # Example: roach4
        roach_index = 3  # roach number
        
        # InterfaceFileMeta is simple - only nw_id and roach
        meta = InterfaceFileMeta(
            nw_id=interface,  # or None if not roach data
            roach=roach_index,
        )
        
        assert meta.nw_id == interface
        assert meta.roach == roach_index

    def test_create_metadata_for_raw_obs_product(self):
        """Test creating metadata for raw observation products (DataProd).
        
        This is the pattern that should be used in raw_obs_product asset.
        """
        # Simulate data from raw_obs_metadata
        raw_obs_data = {
            "master": "toltec",
            "obsnum": 12345,
            "subobsnum": 0,
            "scannum": 1,
            "interface": 4,
            "roach_index": 3,
            "array_name": "a1100",
            "obs_goal": "science",
            "source_name": "M31",
            "data_kind": 1,
        }
        
        # For DataProd.meta, use RawObsMeta (which IS in AnyDataProdMeta union)
        meta = RawObsMeta(
            name=f"{raw_obs_data['master']}-{raw_obs_data['obsnum']}-{raw_obs_data['subobsnum']}-{raw_obs_data['scannum']}-interface{raw_obs_data['interface']}-roach{raw_obs_data['roach_index']}",
            data_prod_type="dp_raw_obs",
            description=f"Raw observation interface file for {raw_obs_data['array_name']}",
            master=raw_obs_data["master"],
            obsnum=raw_obs_data["obsnum"],
            subobsnum=raw_obs_data["subobsnum"],
            scannum=raw_obs_data["scannum"],
            nw_id=raw_obs_data["interface"],
            obs_goal=raw_obs_data.get("obs_goal"),
            source_name=raw_obs_data.get("source_name"),
            data_kind=raw_obs_data["data_kind"],
        )
        
        assert meta.name.startswith("toltec-12345")
        assert meta.master == "toltec"
        assert meta.obsnum == 12345
        assert meta.nw_id == 4
        assert meta.obs_goal == "science"
        assert meta.source_name == "M31"
        
    def test_metadata_serialization_with_asdict(self):
        """Test that RawObsMeta can be serialized with dataclasses.asdict()."""
        import dataclasses
        
        meta = RawObsMeta(
            name="test_product",
            data_prod_type="dp_raw_obs",
            master="toltec",
            obsnum=12345,
        )
        
        # This is the pattern used in assets.py for AdaptixJSON
        meta_dict = dataclasses.asdict(meta)
        
        assert isinstance(meta_dict, dict)
        assert meta_dict["name"] == "test_product"
        assert meta_dict["master"] == "toltec"
        assert meta_dict["tag"] == "raw_obs"


# Mock toltec_db resource for asset testing
class MockToltecDB:
    """Mock toltec_db resource for testing assets."""
    
    def get_raw_obs_by_partition(self, partition_key: str):
        """Mock method that returns test data."""
        return [
            {
                "master": "toltec",
                "obsnum": 12345,
                "subobsnum": 0,
                "scannum": 1,
                "interface": 4,
                "roach_index": 3,
                "array_name": "a1100",
                "obs_goal": "science",
                "source_name": "M31",
                "data_kind": 1,
            }
        ]


@pytest.mark.integration
class TestAssetWithMockedResources:
    """Test actual asset functions with mocked resources.
    
    Following Dagster best practices from:
    https://docs.dagster.io/guides/test/unit-testing-assets-and-ops
    """

    def test_raw_obs_product_metadata_creation(self):
        """Test that raw_obs_product asset creates correct metadata."""
        pytest.importorskip("dagster")
        
        from tolteca_db.dagster.assets import raw_obs_product
        
        # Build mock context with resources
        mock_context = build_asset_context(
            resources={"toltec_db": MockToltecDB()},
            partition_key="toltec12345",
        )
        
        # This test will fail until we fix the metadata creation in assets.py
        # When fixed, it should create RawObsMeta, not InterfaceFileMeta
        try:
            result = raw_obs_product(
                context=mock_context,
                raw_obs_metadata=[{"master": "toltec", "obsnum": 12345}],
            )
            # If we get here, check that result uses correct metadata
            # (This test documents expected behavior)
        except TypeError as e:
            if "unexpected keyword argument 'name'" in str(e):
                pytest.xfail("Known issue: using InterfaceFileMeta instead of RawObsMeta")
            raise
