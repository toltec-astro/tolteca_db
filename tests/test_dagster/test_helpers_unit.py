"""Unit tests for Dagster helper functions.

Tests helper functions independently without external dependencies.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest


class TestQueryObsTimestamp:
    """Test query_obs_timestamp helper function."""

    def test_returns_datetime(self):
        """Test function returns datetime object."""
        from tolteca_db.dagster.helpers import query_obs_timestamp
        
        result = query_obs_timestamp("toltec", 123456, 0, 1)
        
        assert isinstance(result, datetime)
        assert result.tzinfo is not None  # Should be timezone-aware

    def test_accepts_all_parameters(self):
        """Test function accepts required parameters."""
        from tolteca_db.dagster.helpers import query_obs_timestamp
        
        # Should not raise
        result = query_obs_timestamp(
            master="toltec",
            obsnum=123456,
            subobsnum=0,
            scannum=1,
        )
        
        assert result is not None


class TestQueryToltecDBObservation:
    """Test query_toltec_db_observation helper function."""

    def test_returns_dict(self):
        """Test function returns dictionary with observation metadata."""
        from tolteca_db.dagster.helpers import query_toltec_db_observation
        
        result = query_toltec_db_observation("toltec", 123456, 0, 1)
        
        assert isinstance(result, dict)

    def test_has_required_keys(self):
        """Test result contains required metadata keys."""
        from tolteca_db.dagster.helpers import query_toltec_db_observation
        
        result = query_toltec_db_observation("toltec", 123456, 0, 1)
        
        # Check required keys
        assert "master" in result
        assert "obsnum" in result
        assert "subobsnum" in result
        assert "scannum" in result
        assert "obs_type" in result
        assert "timestamp" in result

    def test_preserves_input_values(self):
        """Test function preserves input values in result."""
        from tolteca_db.dagster.helpers import query_toltec_db_observation
        
        result = query_toltec_db_observation("tcs", 89001, 5, 10)
        
        assert result["master"] == "tcs"
        assert result["obsnum"] == 89001
        assert result["subobsnum"] == 5
        assert result["scannum"] == 10


class TestQueryToltecDBInterface:
    """Test query_toltec_db_interface helper function."""

    def test_returns_interface_metadata(self):
        """Test function returns interface-specific metadata."""
        from tolteca_db.dagster.helpers import query_toltec_db_interface
        
        result = query_toltec_db_interface("toltec", 1, 123456, 0, 0, 5)
        
        assert isinstance(result, dict)
        assert "interface" in result
        assert "array_name" in result
        assert "roach_index" in result

    def test_interface_name_format(self):
        """Test interface name is correctly formatted."""
        from tolteca_db.dagster.helpers import query_toltec_db_interface
        
        result = query_toltec_db_interface("toltec", 123456, 0, 0, 5)
        
        assert result["interface"] == "toltec5"
        assert result["roach_index"] == 5

    def test_array_name_mapping(self):
        """Test array name is correctly mapped from roach index."""
        from tolteca_db.dagster.helpers import query_toltec_db_interface
        
        # Test a1100 range (0-6)
        result = query_toltec_db_interface("toltec", 123456, 0, 0, 5)
        assert result["array_name"] == "a1100"
        
        # Test a1400 range (7-10)
        result = query_toltec_db_interface("toltec", 123456, 0, 0, 9)
        assert result["array_name"] == "a1400"
        
        # Test a2000 range (11-12)
        result = query_toltec_db_interface("toltec", 123456, 0, 0, 12)
        assert result["array_name"] == "a2000"


class TestQueryToltecDBQuartetStatus:
    """Test query_toltec_db_quartet_status helper function."""

    def test_returns_status_dict(self):
        """Test function returns status dictionary."""
        from tolteca_db.dagster.helpers import query_toltec_db_quartet_status
        
        result = query_toltec_db_quartet_status("toltec", 123456, 0, 0)
        
        assert isinstance(result, dict)

    def test_has_required_status_keys(self):
        """Test result contains required status keys."""
        from tolteca_db.dagster.helpers import query_toltec_db_quartet_status
        
        result = query_toltec_db_quartet_status("toltec", 123456, 0, 0)
        
        # Check required keys
        assert "interfaces" in result
        assert "valid_interfaces" in result
        assert "invalid_interfaces" in result
        assert "missing_interfaces" in result
        assert "valid_count" in result
        assert "total_found" in result
        assert "time_since_last_valid" in result
        assert "new_quartet_detected" in result

    def test_interface_lists_are_lists(self):
        """Test interface status lists are proper lists."""
        from tolteca_db.dagster.helpers import query_toltec_db_quartet_status
        
        result = query_toltec_db_quartet_status("toltec", 123456, 0, 0)
        
        assert isinstance(result["interfaces"], list)
        assert isinstance(result["valid_interfaces"], list)
        assert isinstance(result["invalid_interfaces"], list)
        assert isinstance(result["missing_interfaces"], list)

    def test_counts_are_integers(self):
        """Test count values are integers."""
        from tolteca_db.dagster.helpers import query_toltec_db_quartet_status
        
        result = query_toltec_db_quartet_status("toltec", 123456, 0, 0)
        
        assert isinstance(result["valid_count"], int)
        assert isinstance(result["total_found"], int)
        assert result["valid_count"] >= 0
        assert result["total_found"] >= 0

    def test_timing_fields(self):
        """Test timing fields are present and valid."""
        from tolteca_db.dagster.helpers import query_toltec_db_quartet_status
        
        result = query_toltec_db_quartet_status("toltec", 1, 123456, 0, 0)
        
        # Check timing fields
        assert isinstance(result["time_since_last_valid"], (int, float))
        assert result["time_since_last_valid"] >= 0
        
        # Check datetime fields
        assert isinstance(result["first_created"], datetime)
        assert isinstance(result["last_updated"], datetime)


class TestQueryToltecDBSince:
    """Test query_toltec_db_since helper function (requires session)."""

    def test_requires_session_parameter(self):
        """Test function requires session parameter."""
        from tolteca_db.dagster.helpers import query_toltec_db_since
        
        # Should raise ValueError when session is None
        with pytest.raises(ValueError, match="session parameter is required"):
            query_toltec_db_since(datetime.now(timezone.utc), session=None)

    def test_accepts_datetime_parameter(self):
        """Test function accepts datetime parameter."""
        from tolteca_db.dagster.helpers import query_toltec_db_since
        
        # Should not raise TypeError for datetime parameter
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        # Will fail on session=None, but that's expected
        with pytest.raises(ValueError, match="session parameter is required"):
            query_toltec_db_since(since, session=None)
