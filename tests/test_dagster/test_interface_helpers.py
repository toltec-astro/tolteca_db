"""Tests for interface-related helper functions and partition utilities."""

import pytest


def test_get_interface_roach_index():
    """Test RoachIndex extraction from interface name."""
    from tolteca_db.dagster.partitions import get_interface_roach_index
    
    # Valid interfaces
    assert get_interface_roach_index("toltec0") == 0
    assert get_interface_roach_index("toltec5") == 5
    assert get_interface_roach_index("toltec12") == 12
    
    # Invalid interfaces
    with pytest.raises(ValueError):
        get_interface_roach_index("invalid")
    
    with pytest.raises(ValueError):
        get_interface_roach_index("toltec13")  # Out of range
    
    with pytest.raises(ValueError):
        get_interface_roach_index("toltec-1")  # Negative


def test_get_array_name_for_interface():
    """Test array name lookup from interface name."""
    from tolteca_db.dagster.partitions import get_array_name_for_interface
    
    # a1100 array (toltec0-6)
    assert get_array_name_for_interface("toltec0") == "a1100"
    assert get_array_name_for_interface("toltec3") == "a1100"
    assert get_array_name_for_interface("toltec6") == "a1100"
    
    # a1400 array (toltec7-10)
    assert get_array_name_for_interface("toltec7") == "a1400"
    assert get_array_name_for_interface("toltec9") == "a1400"
    assert get_array_name_for_interface("toltec10") == "a1400"
    
    # a2000 array (toltec11-12)
    assert get_array_name_for_interface("toltec11") == "a2000"
    assert get_array_name_for_interface("toltec12") == "a2000"


def test_toltec_interfaces_list():
    """Test TOLTEC_INTERFACES constant."""
    from tolteca_db.dagster.partitions import TOLTEC_INTERFACES
    
    # Should have exactly 13 interfaces
    assert len(TOLTEC_INTERFACES) == 13
    
    # Should be in order toltec0-12
    expected = [f"toltec{i}" for i in range(13)]
    assert TOLTEC_INTERFACES == expected


def test_quartet_interface_partitions():
    """Test 2D partition definition exists."""
    from tolteca_db.dagster.partitions import quartet_interface_partitions
    
    # Should be a MultiPartitionsDefinition
    from dagster import MultiPartitionsDefinition
    assert isinstance(quartet_interface_partitions, MultiPartitionsDefinition)
    
    # Should have two dimensions - check partition names
    dimension_names = [dim.name for dim in quartet_interface_partitions.partitions_defs]
    assert "quartet" in dimension_names
    assert "quartet_interface" in dimension_names


def test_query_toltec_db_interface():
    """Test query_toltec_db_interface returns expected structure."""
    from tolteca_db.dagster.helpers import query_toltec_db_interface
    
    result = query_toltec_db_interface(
        master="toltec",
        obsnum=123456,
        subobsnum=0,
        scannum=0,
        roach_index=5,
    )
    
    # Check required keys
    assert "master" in result
    assert "obsnum" in result
    assert "roach_index" in result
    assert "interface" in result
    assert "array_name" in result
    assert "valid" in result
    assert "filename" in result
    assert "timestamp" in result
    
    # Check values
    assert result["master"] == "toltec"
    assert result["obsnum"] == 123456
    assert result["roach_index"] == 5
    assert result["interface"] == "toltec5"
    assert result["array_name"] == "a1100"  # toltec5 is in a1100 array


def test_query_toltec_db_quartet_status():
    """Test query_toltec_db_quartet_status returns expected structure."""
    from tolteca_db.dagster.helpers import query_toltec_db_quartet_status
    
    result = query_toltec_db_quartet_status(
        master="toltec",
        obsnum=123456,
        subobsnum=0,
        scannum=0,
    )
    
    # Check required keys for timeout-based completion
    assert "interfaces" in result
    assert "valid_interfaces" in result
    assert "invalid_interfaces" in result
    assert "missing_interfaces" in result
    assert "valid_count" in result
    # Check timing fields
    assert "first_valid_time" in result
    assert "last_valid_time" in result
    assert "time_since_last_valid" in result
    assert "first_created" in result
    assert "last_updated" in result
    
    # Check new quartet detection field
    assert "new_quartet_detected" in result
    assert isinstance(result["new_quartet_detected"], bool)
    
    # Check types
    assert isinstance(result["interfaces"], list)
    assert isinstance(result["valid_interfaces"], list)
    assert isinstance(result["invalid_interfaces"], list)
    assert isinstance(result["valid_count"], int)
    assert isinstance(result["total_found"], int)
    assert isinstance(result["time_since_last_valid"], float)
    assert isinstance(result["first_valid_time"], str) or result["first_valid_time"] is None
    assert isinstance(result["last_valid_time"], str) or result["last_valid_time"] is None
