"""Test Phase 2 - File API Layer for tolteca_v2 compatibility.

Tests ObsQuery class and SourceInfoModel dataclass to ensure they
provide DataFrame output compatible with tolteca_v2 workflows.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

from tolteca_db.constants import (
    RAWAvailability,
    StorageRole,
    DataProdType as DataProdTypeEnum,
)
from tolteca_db.models.metadata import InterfaceFileMeta, RawObsMeta
from tolteca_db.models.orm import DataProd, DataProdSource, DataProdType, Location
from tolteca_db.repository import ObsQuery, SourceInfoModel


class TestSourceInfoModel:
    """Test SourceInfoModel dataclass structure."""
    
    def test_source_info_instantiation(self):
        """Test creating SourceInfoModel instance."""
        source_info = SourceInfoModel(
            source="file:///data/lmt/raw/obs_123456_1_0_roach0.nc",
            interface="toltec",
            roach=0,
            obsnum=123456,
            subobsnum=1,
            scannum=0,
            uid_raw_obs="dp_raw_obs_123456_1_0",
        )
        
        assert source_info.source == "file:///data/lmt/raw/obs_123456_1_0_roach0.nc"
        assert source_info.interface == "toltec"
        assert source_info.roach == 0
        assert source_info.obsnum == 123456
        assert source_info.uid_raw_obs == "dp_raw_obs_123456_1_0"
    
    def test_source_info_hwp(self):
        """Test SourceInfoModel for HWP interface."""
        source_info = SourceInfoModel(
            source="file:///data/lmt/raw/hwp_123456.nc",
            interface="hwp",
            roach=None,  # HWP has no roach
            obsnum=123456,
            subobsnum=1,
            scannum=0,
            uid_raw_obs="dp_raw_obs_hwp_123456",
        )
        
        assert source_info.interface == "hwp"
        assert source_info.roach is None


class TestObsQuery:
    """Test ObsQuery file API layer."""
    
    @pytest.fixture
    def setup_test_data(self, session):
        """Create test observation data."""
        now = datetime.now(timezone.utc)
        
        # Create location (timestamps auto-populated by server_default)
        location = Location(
            label="lmt",
            location_type="filesystem",
            root_uri="file:///data/lmt",
            priority=1,
            meta={},
        )
        session.add(location)
        session.flush()  # Get auto-assigned PK
        
        # Create data product type
        dp_type = DataProdType(
            label="raw_obs",
            description="Raw observation data",
        )
        session.add(dp_type)
        session.flush()  # Get auto-assigned PK
        
        # Create raw observation product
        raw_obs_meta = RawObsMeta(
            name="obs_123456_1_0",
            data_prod_type=DataProdTypeEnum.DP_RAW_OBS,
            tag="raw_obs",
            obsnum=123456,
            subobsnum=1,
            scannum=0,
        )
        
        data_prod = DataProd(
            data_prod_type_fk=dp_type.pk,
            lifecycle_status="completed",
            availability_state=RAWAvailability.AVAILABLE.value,
            content_hash="abc123",
            meta=raw_obs_meta,
        )
        session.add(data_prod)
        session.flush()  # Get auto-assigned PK
        
        # Create source
        # Construct source_uri from location root + relative path
        source_uri = f"{location.root_uri}/raw/obs_123456_1_0.nc"
        source = DataProdSource(
            data_prod_fk=data_prod.pk,
            location_fk=location.pk,
            source_uri=source_uri,
            role=StorageRole.PRIMARY.value,
        )
        session.add(source)
        session.commit()
        
        return data_prod.pk
    
    def test_obs_query_init(self, session):
        """Test ObsQuery initialization."""
        query = ObsQuery(session, location_label="lmt")
        assert query.session == session
        assert query.location_label == "lmt"
    
    def test_get_raw_obs_info_table_basic(self, session, setup_test_data):
        """Test basic raw observation query."""
        data_prod_pk = setup_test_data
        query = ObsQuery(session, location_label="lmt")
        df = query.get_raw_obs_info_table(123456)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        
        # Check DataFrame structure
        expected_columns = [
            'source', 'interface', 'roach', 'obsnum',
            'subobsnum', 'scannum', 'uid_raw_obs'
        ]
        assert list(df.columns) == expected_columns
        
        # Check data
        row = df.iloc[0]
        assert row['obsnum'] == 123456
        assert row['subobsnum'] == 1
        assert row['scannum'] == 0
        assert 'file://' in row['source']
        assert row['uid_raw_obs'] == data_prod_pk  # Now an integer PK
    
    def test_get_raw_obs_info_table_with_filters(self, session, setup_test_data):
        """Test raw observation query with filters."""
        query = ObsQuery(session, location_label="lmt")
        
        # Filter by subobsnum
        df = query.get_raw_obs_info_table(123456, subobsnum=1)
        assert len(df) > 0
        assert all(df['subobsnum'] == 1)
        
        # Filter by non-existent subobsnum
        df = query.get_raw_obs_info_table(123456, subobsnum=999)
        assert len(df) == 0
    
    def test_get_raw_obs_info_table_empty_result(self, session, setup_test_data):
        """Test query with no matching results."""
        query = ObsQuery(session, location_label="lmt")
        df = query.get_raw_obs_info_table(999999)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
        
        # Check empty DataFrame has correct schema
        expected_columns = [
            'source', 'interface', 'roach', 'obsnum',
            'subobsnum', 'scannum', 'uid_raw_obs'
        ]
        assert list(df.columns) == expected_columns
    
    def test_dataframe_tolteca_v2_compatibility(self, session, setup_test_data):
        """Test DataFrame structure matches tolteca_v2 expectations."""
        query = ObsQuery(session, location_label="lmt")
        df = query.get_raw_obs_info_table(123456)
        
        # These operations should work for tolteca_v2 compatibility
        assert 'source' in df.columns
        assert 'obsnum' in df.columns
        assert 'interface' in df.columns
        
        # Should be able to extract file list
        file_list = df['source'].tolist()
        assert len(file_list) > 0
        assert all(isinstance(f, str) for f in file_list)
        assert all(f.startswith('file://') for f in file_list)
        
        # Should be able to filter by interface
        toltec_files = df[df['interface'] == 'toltec']
        assert isinstance(toltec_files, pd.DataFrame)
