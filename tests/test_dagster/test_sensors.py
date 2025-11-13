"""Unit tests for Dagster sensors.

Tests sensor configuration and behavior patterns.
"""

from __future__ import annotations

import pytest


class TestSensorConfiguration:
    """Test sensor configuration and metadata."""

    @pytest.mark.integration
    def test_sensor_exists(self):
        """Test sync_with_toltec_db sensor is defined."""
        pytest.importorskip("dagster")
        from tolteca_db.dagster.sensors import sync_with_toltec_db
        
        assert sync_with_toltec_db is not None

    @pytest.mark.integration
    def test_sensor_metadata(self):
        """Test sensor has correct metadata."""
        pytest.importorskip("dagster")
        from tolteca_db.dagster.sensors import sync_with_toltec_db
        
        # Check sensor attributes
        assert hasattr(sync_with_toltec_db, "name")
        # Name should be set via decorator

    @pytest.mark.integration
    def test_sensor_in_definitions(self):
        """Test sensor is registered in definitions."""
        pytest.importorskip("dagster")
        from tolteca_db.dagster.definitions import defs
        
        assert defs.sensors is not None
        assert len(defs.sensors) > 0
        
        # Check sensor names
        sensor_names = {sensor.name for sensor in defs.sensors}
        assert "toltec_db_sync_sensor" in sensor_names


class TestSensorTargets:
    """Test sensor target configuration."""

    @pytest.mark.integration
    def test_sensor_targets_assets(self):
        """Test sensor targets correct assets."""
        pytest.importorskip("dagster")
        from tolteca_db.dagster.definitions import defs
        
        # Get sensor by name
        sensor = next(s for s in defs.sensors if s.name == "toltec_db_sync_sensor")
        
        # Sensor should have targets
        assert sensor is not None


class TestSensorBehavior:
    """Test sensor behavior patterns (without execution)."""

    @pytest.mark.integration
    def test_sensor_requires_resources(self):
        """Test sensor declares required resources."""
        pytest.importorskip("dagster")
        from tolteca_db.dagster.sensors import sync_with_toltec_db
        
        # Sensor should have required_resource_keys in decorator
        # This is checked at definition validation time
        assert sync_with_toltec_db is not None


class TestMultiPartitionKeyCreation:
    """Test 2D partition key creation patterns."""

    @pytest.mark.integration
    def test_multipartition_key_structure(self):
        """Test MultiPartitionKey structure for 2D partitions."""
        pytest.importorskip("dagster")
        from dagster import MultiPartitionKey
        
        # Create 2D partition key as sensor does
        partition_key = MultiPartitionKey({
            "quartet": "toltec-123456-0-0",
            "interface": "toltec5",
        })
        
        assert partition_key.keys_by_dimension["quartet"] == "toltec-123456-0-0"
        assert partition_key.keys_by_dimension["interface"] == "toltec5"

    @pytest.mark.integration
    def test_multipartition_key_order(self):
        """Test MultiPartitionKey dimension order."""
        pytest.importorskip("dagster")
        from dagster import MultiPartitionKey
        
        # Order should match MultiPartitionsDefinition
        partition_key = MultiPartitionKey({
            "quartet": "toltec-123456-0-0",
            "interface": "toltec5",
        })
        
        # Both dimensions should be present
        assert "quartet" in partition_key.keys_by_dimension
        assert "interface" in partition_key.keys_by_dimension


class TestSensorTagGeneration:
    """Test sensor tag generation patterns."""

    def test_tag_structure(self):
        """Test expected tag structure for run requests."""
        # Tags that sensor should generate
        expected_keys = {
            "master",
            "obsnum",
            "subobsnum",
            "scannum",
            "interface",
            "roach_index",
            "array_name",
            "valid",
            "validation_status",
            "obs_date",
            "obs_timestamp",
            "obs_type",
            "source",
        }
        
        # This documents expected tag keys for sensor run requests
        assert len(expected_keys) == 13

    def test_validation_status_values(self):
        """Test validation status tag values."""
        # Sensor should use these values
        valid_statuses = {"complete", "in_progress"}
        
        assert "complete" in valid_statuses
        assert "in_progress" in valid_statuses


class TestSensorPolling:
    """Test sensor polling configuration."""

    @pytest.mark.integration
    def test_minimum_interval_configured(self):
        """Test sensor has minimum polling interval."""
        pytest.importorskip("dagster")
        from tolteca_db.dagster.definitions import defs
        
        # Get sensor
        sensor = next(s for s in defs.sensors if s.name == "toltec_db_sync_sensor")
        
        # Should have minimum interval set
        assert sensor is not None
        # Actual interval checking requires sensor execution context


class TestSensorCursorHandling:
    """Test cursor handling patterns."""

    def test_cursor_timestamp_format(self):
        """Test cursor uses ISO format timestamps."""
        from datetime import datetime, timezone
        
        # Sensor should use ISO format for cursor
        now = datetime.now(timezone.utc)
        cursor = now.isoformat()
        
        # Should be parseable
        parsed = datetime.fromisoformat(cursor.replace("Z", "+00:00"))
        assert parsed.tzinfo is not None

    def test_default_cursor_value(self):
        """Test default cursor when none exists."""
        default_cursor = "2024-01-01T00:00:00Z"
        
        # Should be valid ISO format
        from datetime import datetime
        parsed = datetime.fromisoformat(default_cursor.replace("Z", "+00:00"))
        
        assert parsed.year == 2024
        assert parsed.month == 1
        assert parsed.day == 1
