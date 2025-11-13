"""Tests for Dagster helper functions."""

from __future__ import annotations

from datetime import datetime, timezone


def test_query_obs_timestamp_returns_datetime():
    """Test that query_obs_timestamp returns a datetime."""
    from tolteca_db.dagster.helpers import query_obs_timestamp
    
    timestamp = query_obs_timestamp("toltec", 123456, 0, 1)
    
    assert isinstance(timestamp, datetime)
    assert timestamp.tzinfo is not None  # Should have timezone


def test_query_toltec_db_observation_returns_dict():
    """Test that query_toltec_db_observation returns expected structure."""
    from tolteca_db.dagster.helpers import query_toltec_db_observation
    
    obs = query_toltec_db_observation("toltec", 123456, 0, 1)
    
    assert isinstance(obs, dict)
    assert obs["master"] == "toltec"
    assert obs["obsnum"] == 123456
    assert obs["subobsnum"] == 0
    assert obs["scannum"] == 1
    assert "timestamp" in obs
    assert "data_kind" in obs


def test_query_toltec_db_since_returns_list():
    """Test that query_toltec_db_since returns a list."""
    from unittest.mock import MagicMock
    from tolteca_db.dagster.helpers import query_toltec_db_since
    
    # Create mock session
    mock_session = MagicMock()
    mock_session.execute.return_value = []
    
    since = datetime(2024, 1, 1, tzinfo=timezone.utc)
    result = query_toltec_db_since(since, session=mock_session)
    
    assert isinstance(result, list)
    # Currently returns empty list (placeholder implementation)
    assert len(result) == 0
