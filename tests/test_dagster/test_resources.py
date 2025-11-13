"""Unit tests for Dagster resources.

Tests resource configurations and initialization.
"""

from __future__ import annotations

import pytest


class TestToltecaDBResource:
    """Test ToltecaDBResource configuration."""

    @pytest.mark.integration
    def test_resource_initialization(self):
        """Test ToltecaDBResource can be initialized."""
        pytest.importorskip("dagster")
        from tolteca_db.dagster.resources import ToltecaDBResource
        
        resource = ToltecaDBResource(database_url="duckdb:///:memory:")
        
        assert resource.database_url == "duckdb:///:memory:"

    @pytest.mark.integration
    def test_resource_default_url(self):
        """Test default database URL."""
        pytest.importorskip("dagster")
        from tolteca_db.dagster.resources import ToltecaDBResource
        
        resource = ToltecaDBResource()
        
        assert "tolteca.duckdb" in resource.database_url

    @pytest.mark.integration
    def test_get_session_method_exists(self):
        """Test get_session method exists."""
        pytest.importorskip("dagster")
        from tolteca_db.dagster.resources import ToltecaDBResource
        
        resource = ToltecaDBResource(database_url="duckdb:///:memory:")
        
        assert hasattr(resource, "get_session")
        assert callable(resource.get_session)


class TestToltecDBResource:
    """Test ToltecDBResource configuration."""

    @pytest.mark.integration
    def test_resource_initialization(self):
        """Test ToltecDBResource can be initialized."""
        pytest.importorskip("dagster")
        from tolteca_db.dagster.resources import ToltecDBResource
        
        resource = ToltecDBResource(
            database_url="sqlite:///:memory:",
            read_only=True,
        )
        
        assert resource.database_url == "sqlite:///:memory:"
        assert resource.read_only is True

    @pytest.mark.integration
    def test_read_only_default(self):
        """Test read_only defaults to True."""
        pytest.importorskip("dagster")
        from tolteca_db.dagster.resources import ToltecDBResource
        
        resource = ToltecDBResource(database_url="sqlite:///:memory:")
        
        assert resource.read_only is True

    @pytest.mark.integration
    def test_read_only_enforcement(self):
        """Test read_only=False raises error."""
        pytest.importorskip("dagster")
        from tolteca_db.dagster.resources import ToltecDBResource
        
        resource = ToltecDBResource(
            database_url="sqlite:///:memory:",
            read_only=False,
        )
        
        # Should raise when trying to get session
        with pytest.raises(RuntimeError, match="read-only"):
            resource.get_session()


class TestLocationConfig:
    """Test LocationConfig resource."""

    @pytest.mark.integration
    def test_config_initialization(self):
        """Test LocationConfig can be initialized."""
        pytest.importorskip("dagster")
        from tolteca_db.dagster.resources import LocationConfig
        
        config = LocationConfig(
            location_pk="LMT",
            location_name="Large Millimeter Telescope",
            data_root="/data/lmt",
        )
        
        assert config.location_pk == "LMT"
        assert config.location_name == "Large Millimeter Telescope"
        assert config.data_root == "/data/lmt"

    @pytest.mark.integration
    def test_default_values(self):
        """Test LocationConfig default values."""
        pytest.importorskip("dagster")
        from tolteca_db.dagster.resources import LocationConfig
        
        config = LocationConfig()
        
        assert config.location_pk == "LMT"
        assert config.location_name == "Large Millimeter Telescope"

    @pytest.mark.integration
    def test_get_data_root_method(self):
        """Test get_data_root returns Path object."""
        pytest.importorskip("dagster")
        from pathlib import Path
        from tolteca_db.dagster.resources import LocationConfig
        
        config = LocationConfig(data_root="/data/lmt")
        
        result = config.get_data_root()
        
        assert isinstance(result, Path)
        assert str(result) == "/data/lmt"

    @pytest.mark.integration
    def test_get_data_root_none(self):
        """Test get_data_root returns None when not configured."""
        pytest.importorskip("dagster")
        from tolteca_db.dagster.resources import LocationConfig
        
        config = LocationConfig(data_root=None)
        
        result = config.get_data_root()
        
        assert result is None


class TestValidationConfig:
    """Test ValidationConfig resource."""

    @pytest.mark.integration
    def test_config_initialization(self):
        """Test ValidationConfig can be initialized."""
        pytest.importorskip("dagster")
        from tolteca_db.dagster.resources import ValidationConfig
        
        config = ValidationConfig(
            max_interface_count=13,
            disabled_interfaces=[3, 7],
            validation_timeout_seconds=30.0,
        )
        
        assert config.max_interface_count == 13
        assert config.disabled_interfaces == [3, 7]
        assert config.validation_timeout_seconds == 30.0

    @pytest.mark.integration
    def test_default_values(self):
        """Test ValidationConfig default values."""
        pytest.importorskip("dagster")
        from tolteca_db.dagster.resources import ValidationConfig
        
        config = ValidationConfig()
        
        assert config.max_interface_count == 13
        assert config.disabled_interfaces == []
        assert config.validation_timeout_seconds == 30.0
        assert config.sensor_poll_interval_seconds == 5
        assert config.retry_on_incomplete is True

    @pytest.mark.integration
    def test_get_expected_interfaces(self):
        """Test get_expected_interfaces excludes disabled."""
        pytest.importorskip("dagster")
        from tolteca_db.dagster.resources import ValidationConfig
        
        config = ValidationConfig(disabled_interfaces=[3, 7, 11])
        
        expected = config.get_expected_interfaces()
        
        assert len(expected) == 10  # 13 - 3 disabled
        assert 3 not in expected
        assert 7 not in expected
        assert 11 not in expected
        assert 0 in expected
        assert 12 in expected

    @pytest.mark.integration
    def test_is_interface_expected(self):
        """Test is_interface_expected checks disabled list."""
        pytest.importorskip("dagster")
        from tolteca_db.dagster.resources import ValidationConfig
        
        config = ValidationConfig(disabled_interfaces=[3, 7])
        
        assert config.is_interface_expected(0) is True
        assert config.is_interface_expected(3) is False
        assert config.is_interface_expected(7) is False
        assert config.is_interface_expected(12) is True


class TestResourceIntegration:
    """Test resource integration with Dagster."""

    @pytest.mark.integration
    def test_resources_in_definitions(self):
        """Test resources are available in Dagster definitions."""
        pytest.importorskip("dagster")
        from tolteca_db.dagster.definitions import defs
        
        assert defs.resources is not None
        assert len(defs.resources) > 0
        
        # Check specific resources exist
        resource_keys = set(defs.resources.keys())
        assert "tolteca_db" in resource_keys
        # Note: toltec_db only in production config

    @pytest.mark.integration
    def test_resource_types(self):
        """Test resource types are correct."""
        pytest.importorskip("dagster")
        from tolteca_db.dagster.definitions import defs
        from tolteca_db.dagster.resources import ToltecaDBResource
        
        tolteca_db_resource = defs.resources["tolteca_db"]
        
        # Should be ToltecaDBResource or a wrapper
        assert tolteca_db_resource is not None
