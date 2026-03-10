"""Tests for Dagster assets."""

from __future__ import annotations

import pytest


@pytest.mark.integration
def test_assets_can_be_loaded():
    """Test that assets can be imported when Dagster is installed."""
    pytest.importorskip("dagster")
    
    from tolteca_db.dagster.assets import (
        raw_obs_metadata,
        raw_obs_product,
        parquet_files,
        association_groups,
    )
    
    assert raw_obs_metadata is not None
    assert raw_obs_product is not None
    assert parquet_files is not None
    assert association_groups is not None


@pytest.mark.integration
def test_definitions_can_be_loaded():
    """Test that Definitions object can be loaded."""
    pytest.importorskip("dagster")
    
    from tolteca_db.dagster.definitions import defs
    
    assert defs is not None
    assert len(defs.assets) >= 4  # At least 4 assets defined
    assert len(defs.sensors) > 0
    assert len(defs.resources) > 0


@pytest.mark.integration
def test_asset_dependency_chain():
    """Test that assets have correct dependencies."""
    pytest.importorskip("dagster")
    
    from tolteca_db.dagster.definitions import defs
    
    # Get asset graph
    asset_graph = defs.resolve_asset_graph()
    all_keys = asset_graph.get_all_asset_keys()
    
    # raw_obs_metadata should have no dependencies
    raw_meta_key = next(k for k in all_keys if "raw_obs_metadata" in str(k))
    assert len(asset_graph.get(raw_meta_key).parent_keys) == 0
    
    # raw_obs_product should depend on raw_obs_metadata
    raw_prod_key = next(k for k in all_keys if "raw_obs_product" in str(k))
    assert len(asset_graph.get(raw_prod_key).parent_keys) == 1
    
    # parquet_files should depend on raw_obs_product
    parquet_key = next(k for k in all_keys if "parquet_files" in str(k))
    assert len(asset_graph.get(parquet_key).parent_keys) == 1


@pytest.mark.integration
def test_partitioned_assets():
    """Test that partitioned assets are configured correctly."""
    pytest.importorskip("dagster")
    
    from tolteca_db.dagster.definitions import defs
    
    asset_graph = defs.resolve_asset_graph()
    all_keys = asset_graph.get_all_asset_keys()
    
    # Check that raw_obs_metadata is partitioned
    raw_meta_key = next(k for k in all_keys if "raw_obs_metadata" in str(k))
    assert asset_graph.get(raw_meta_key).partitions_def is not None
    
    # Check that association_groups is NOT partitioned
    assoc_key = next(k for k in all_keys if "association_groups" in str(k))
    assert asset_graph.get(assoc_key).partitions_def is None
