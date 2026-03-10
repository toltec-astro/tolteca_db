"""Unit tests for Dagster partition logic.

Tests partition definitions, validation, and helper functions.
"""

from __future__ import annotations

import pytest

from tolteca_db.dagster.partitions import (
    TOLTEC_INTERFACES,
    get_array_name_for_interface,
    get_interface_roach_index,
    validate_partition_key,
)


class TestToltecInterfaces:
    """Test TolTEC interface constants."""

    def test_interface_count(self):
        """Test that TolTEC has 13 interfaces."""
        assert len(TOLTEC_INTERFACES) == 13

    def test_interface_names(self):
        """Test interface naming convention."""
        assert TOLTEC_INTERFACES[0] == "toltec0"
        assert TOLTEC_INTERFACES[5] == "toltec5"
        assert TOLTEC_INTERFACES[12] == "toltec12"

    def test_interface_range(self):
        """Test interfaces are toltec0 through toltec12."""
        expected = [f"toltec{i}" for i in range(13)]
        assert TOLTEC_INTERFACES == expected


class TestGetInterfaceRoachIndex:
    """Test roach index extraction from interface name."""

    def test_valid_interfaces(self):
        """Test extracting roach index from valid interface names."""
        assert get_interface_roach_index("toltec0") == 0
        assert get_interface_roach_index("toltec5") == 5
        assert get_interface_roach_index("toltec12") == 12

    def test_invalid_prefix(self):
        """Test error on invalid interface prefix."""
        with pytest.raises(ValueError, match="Invalid interface name"):
            get_interface_roach_index("invalid5")

    def test_invalid_number(self):
        """Test error on invalid roach number."""
        with pytest.raises(ValueError, match="Invalid interface name"):
            get_interface_roach_index("toltec")

    def test_out_of_range(self):
        """Test error on roach index out of range."""
        with pytest.raises(ValueError):
            get_interface_roach_index("toltec13")
        
        with pytest.raises(ValueError):
            get_interface_roach_index("toltec-1")


class TestGetArrayNameForInterface:
    """Test array name mapping for interfaces."""

    def test_a1100_interfaces(self):
        """Test toltec0-6 map to a1100."""
        for i in range(7):
            assert get_array_name_for_interface(f"toltec{i}") == "a1100"

    def test_a1400_interfaces(self):
        """Test toltec7-10 map to a1400."""
        for i in range(7, 11):
            assert get_array_name_for_interface(f"toltec{i}") == "a1400"

    def test_a2000_interfaces(self):
        """Test toltec11-12 map to a2000."""
        assert get_array_name_for_interface("toltec11") == "a2000"
        assert get_array_name_for_interface("toltec12") == "a2000"

    def test_invalid_interface(self):
        """Test error on invalid interface name."""
        with pytest.raises(ValueError):
            get_array_name_for_interface("invalid")


class TestValidatePartitionKey:
    """Test partition key validation."""

    def test_valid_quartet_keys(self):
        """Test valid quartet partition keys."""
        assert validate_partition_key("toltec-123456-0-0") is True
        assert validate_partition_key("tcs-89001-0-1") is True
        assert validate_partition_key("ics-12345-5-10") is True

    def test_invalid_quartet_keys(self):
        """Test invalid quartet partition keys."""
        assert validate_partition_key("invalid") is False
        assert validate_partition_key("toltec-123456") is False
        assert validate_partition_key("") is False


class TestPartitionDefinitions:
    """Test Dagster partition definition objects."""

    @pytest.mark.integration
    def test_quartet_partitions_definition(self):
        """Test quartet partitions are dynamic."""
        pytest.importorskip("dagster")
        from tolteca_db.dagster.partitions import quartet_partitions
        
        assert quartet_partitions.name == "quartet"
        # Dynamic partitions start empty
        assert hasattr(quartet_partitions, "get_partition_keys")

    @pytest.mark.integration
    def test_quartet_interface_partitions_definition(self):
        """Test 2D partitions (quartet × interface)."""
        pytest.importorskip("dagster")
        from tolteca_db.dagster.partitions import quartet_interface_partitions
        
        # Check dimensions (partitions_defs is a list, not a dict)
        dimension_names = [d.name for d in quartet_interface_partitions.partitions_defs]
        assert "quartet" in dimension_names
        assert "quartet_interface" in dimension_names
        
        # Interface dimension should be static with 13 keys
        interface_def = [d for d in quartet_interface_partitions.partitions_defs if d.name == "quartet_interface"][0].partitions_def
        interface_keys = interface_def.get_partition_keys()
        assert len(interface_keys) == 13
        assert "toltec0" in interface_keys
        assert "toltec12" in interface_keys


class TestTagsForPartitionFn:
    """Test partition tag generation."""

    @pytest.mark.integration
    def test_tags_structure(self):
        """Test tags contain expected keys."""
        pytest.importorskip("dagster")
        from tolteca_db.dagster.partitions import tags_for_partition_fn
        
        tags = tags_for_partition_fn("toltec-123456-0-1")
        
        # Check required keys
        assert "master" in tags
        assert "obsnum" in tags
        assert "subobsnum" in tags
        assert "scannum" in tags
        assert "obs_date" in tags
        assert "obs_timestamp" in tags
        assert "obs_year" in tags
        assert "obs_month" in tags

    @pytest.mark.integration
    def test_tags_values(self):
        """Test tag values are correctly parsed."""
        pytest.importorskip("dagster")
        from tolteca_db.dagster.partitions import tags_for_partition_fn
        
        tags = tags_for_partition_fn("toltec-123456-5-10")
        
        assert tags["master"] == "toltec"
        assert tags["obsnum"] == "123456"
        assert tags["subobsnum"] == "5"
        assert tags["scannum"] == "10"
        # Date fields depend on query_obs_timestamp mock


class TestPartitionKeyParsing:
    """Test partition key format parsing."""

    def test_parse_quartet_key(self):
        """Test parsing quartet partition key."""
        from tolteca_db.utils.uid import parse_raw_obs_uid
        
        result = parse_raw_obs_uid("toltec-123456-0-1")
        
        assert result["master"] == "toltec"
        assert result["obsnum"] == 123456
        assert result["subobsnum"] == 0
        assert result["scannum"] == 1

    def test_make_quartet_key(self):
        """Test creating quartet partition key."""
        from tolteca_db.utils.uid import make_raw_obs_uid
        
        key = make_raw_obs_uid("toltec", 123456, 0, 1)
        
        assert key == "toltec-123456-0-1"

    def test_round_trip(self):
        """Test parse/make round trip."""
        from tolteca_db.utils.uid import make_raw_obs_uid, parse_raw_obs_uid
        
        original = "tcs-89001-5-10"
        parsed = parse_raw_obs_uid(original)
        reconstructed = make_raw_obs_uid(
            parsed["master"],
            parsed["obsnum"],
            parsed["subobsnum"],
            parsed["scannum"],
        )
        
        assert reconstructed == original
