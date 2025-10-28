"""Tests for ObsSpec parsing with advanced separator and wildcard notation."""

from __future__ import annotations

import pytest

from tolteca_db.api.obs import ObsQuery


class TestObsSpecParser:
    """Test ObsSpec parsing with various formats."""
    
    def test_none_returns_empty(self):
        """None should return empty dict (latest)."""
        result = ObsQuery.parse_obs_spec(None)
        assert result == {}
    
    def test_int_returns_obsnum(self):
        """Integer input should return obsnum."""
        result = ObsQuery.parse_obs_spec(123456)
        assert result == {'obsnum': 123456}
    
    def test_string_obsnum(self):
        """String obsnum should parse correctly."""
        result = ObsQuery.parse_obs_spec("123456")
        assert result == {'obsnum': 123456}
    
    def test_master_prefix(self):
        """Master prefix should be extracted."""
        result = ObsQuery.parse_obs_spec("tcs-123456")
        assert result == {'master': 'tcs', 'obsnum': 123456}
        
        result = ObsQuery.parse_obs_spec("ics-123456")
        assert result == {'master': 'ics', 'obsnum': 123456}
    
    # Sequential format (forward separator `-`)
    
    def test_sequential_subobsnum(self):
        """Sequential with subobsnum."""
        result = ObsQuery.parse_obs_spec("123456-1")
        assert result == {'obsnum': 123456, 'subobsnum': 1}
    
    def test_sequential_full(self):
        """Sequential with all components."""
        result = ObsQuery.parse_obs_spec("123456-1-5")
        assert result == {'obsnum': 123456, 'subobsnum': 1, 'scannum': 5}
    
    def test_sequential_with_master(self):
        """Sequential with master prefix."""
        result = ObsQuery.parse_obs_spec("tcs-123456-1-5")
        assert result == {
            'master': 'tcs',
            'obsnum': 123456,
            'subobsnum': 1,
            'scannum': 5
        }
    
    # Shortcut format (backward separator `/`)
    
    def test_shortcut_obsnum_roach(self):
        """Shortcut: obsnum/roach (skips subobsnum, scannum)."""
        result = ObsQuery.parse_obs_spec("1000/0")
        assert result == {'obsnum': 1000, 'roach': 0}
    
    def test_shortcut_obsnum_subobsnum_roach(self):
        """Shortcut: obsnum-subobsnum/roach (skips scannum)."""
        result = ObsQuery.parse_obs_spec("1000-0/0")
        assert result == {'obsnum': 1000, 'subobsnum': 0, 'roach': 0}
    
    def test_shortcut_obsnum_scannum_roach(self):
        """Shortcut: obsnum/scannum/roach (skips subobsnum)."""
        result = ObsQuery.parse_obs_spec("1000/0/0")
        assert result == {'obsnum': 1000, 'scannum': 0, 'roach': 0}
    
    def test_shortcut_with_master(self):
        """Shortcut with master prefix."""
        result = ObsQuery.parse_obs_spec("tcs-1000/0")
        assert result == {'master': 'tcs', 'obsnum': 1000, 'roach': 0}
    
    # Wildcard format
    
    def test_wildcard_empty_brackets_roach(self):
        """Empty brackets for obsnum, roach specified."""
        result = ObsQuery.parse_obs_spec("[]/1")
        assert result == {'roach': 1, 'obsnum_slice': slice(None)}
        # obsnum_slice indicates wildcard matching (all obsnum)
    
    def test_wildcard_empty_braces_roach(self):
        """Empty braces for obsnum, roach specified."""
        result = ObsQuery.parse_obs_spec("{}/1")
        assert result == {'roach': 1, 'obsnum_slice': slice(None)}
        # obsnum_slice indicates wildcard matching (all obsnum)
    
    def test_wildcard_brackets_in_sequence(self):
        """Wildcard brackets in sequential format."""
        result = ObsQuery.parse_obs_spec("1000-[]-[]-1")
        assert result['obsnum'] == 1000
        assert result['roach'] == 1
        assert 'subobsnum_slice' in result
        assert 'scannum_slice' in result
    
    def test_wildcard_list_subobsnum(self):
        """List notation for subobsnum."""
        result = ObsQuery.parse_obs_spec("1000-{0,1,2}")
        assert result == {
            'obsnum': 1000,
            'subobsnum_list': [0, 1, 2]
        }
    
    def test_wildcard_list_with_scannum(self):
        """List notation for subobsnum with scannum."""
        result = ObsQuery.parse_obs_spec("1000-{0,1,2}-5")
        assert result == {
            'obsnum': 1000,
            'subobsnum_list': [0, 1, 2],
            'scannum': 5
        }
    
    def test_wildcard_slice_subobsnum(self):
        """Slice notation for subobsnum."""
        result = ObsQuery.parse_obs_spec("1000-[0:5]")
        assert result['obsnum'] == 1000
        assert 'subobsnum_slice' in result
        assert result['subobsnum_slice'] == slice(0, 5)
    
    def test_wildcard_slice_full(self):
        """Slice notation with start:stop:step."""
        result = ObsQuery.parse_obs_spec("1000-[0:10:2]")
        assert result['obsnum'] == 1000
        assert result['subobsnum_slice'] == slice(0, 10, 2)
    
    def test_wildcard_empty_slice(self):
        """Empty slice [:] matches all."""
        result = ObsQuery.parse_obs_spec("1000-[:]")
        assert result['obsnum'] == 1000
        assert result['subobsnum_slice'] == slice(None)
    
    # Complex combinations
    
    def test_complex_master_list_roach(self):
        """Master + list + roach."""
        result = ObsQuery.parse_obs_spec("tcs-1000-{0,1,2}/0")
        assert result == {
            'master': 'tcs',
            'obsnum': 1000,
            'subobsnum_list': [0, 1, 2],
            'roach': 0
        }
    
    def test_complex_slice_scannum_roach(self):
        """Slice + scannum + roach."""
        result = ObsQuery.parse_obs_spec("1000-[0:5]-{0,1}/1")
        assert result['obsnum'] == 1000
        assert result['subobsnum_slice'] == slice(0, 5)
        assert result['scannum_list'] == [0, 1]
        assert result['roach'] == 1
    
    # Edge cases
    
    def test_single_value_no_separators(self):
        """Single numeric value without separators."""
        result = ObsQuery.parse_obs_spec("123456")
        assert result == {'obsnum': 123456}
    
    def test_file_path(self):
        """File path should be detected."""
        result = ObsQuery.parse_obs_spec("/data/toltec/tcs/file.nc")
        assert 'filepath' in result
        assert str(result['filepath']) == '/data/toltec/tcs/file.nc'
    
    def test_file_path_with_nc_extension(self):
        """File with .nc extension."""
        result = ObsQuery.parse_obs_spec("file.nc")
        assert 'filepath' in result
    
    def test_wildcard_not_confused_with_path(self):
        """Wildcard notation should not be confused with file path."""
        # This has wildcards, so it's not a file path
        result = ObsQuery.parse_obs_spec("1000-{}/1")
        assert 'filepath' not in result
        assert result['obsnum'] == 1000
        assert result['roach'] == 1


class TestValueParser:
    """Test individual value parsing."""
    
    def test_parse_integer(self):
        """Parse integer value."""
        result = ObsQuery._parse_value("123")
        assert result == 123
    
    def test_parse_empty_braces(self):
        """Empty braces should return slice(None) for wildcard."""
        result = ObsQuery._parse_value("{}")
        assert result == slice(None)
    
    def test_parse_empty_brackets(self):
        """Empty brackets should return slice(None) for wildcard."""
        result = ObsQuery._parse_value("[]")
        assert result == slice(None)
    
    def test_parse_list(self):
        """Parse comma-separated list."""
        result = ObsQuery._parse_value("{0,1,2}")
        assert result == [0, 1, 2]
    
    def test_parse_list_with_spaces(self):
        """Parse list with spaces."""
        result = ObsQuery._parse_value("{0, 1, 2}")
        assert result == [0, 1, 2]
    
    def test_parse_slice_full(self):
        """Parse full slice notation."""
        result = ObsQuery._parse_value("[0:10:2]")
        assert result == slice(0, 10, 2)
    
    def test_parse_slice_start_stop(self):
        """Parse slice with start and stop."""
        result = ObsQuery._parse_value("[0:10]")
        assert result == slice(0, 10)
    
    def test_parse_slice_colon_only(self):
        """Parse slice with colon only."""
        result = ObsQuery._parse_value("[:]")
        assert result == slice(None)
    
    def test_parse_slice_open_end(self):
        """Parse slice with open end."""
        result = ObsQuery._parse_value("[5:]")
        assert result == slice(5, None)
    
    def test_parse_slice_open_start(self):
        """Parse slice with open start."""
        result = ObsQuery._parse_value("[:10]")
        assert result == slice(None, 10)
    
    def test_parse_empty_string(self):
        """Empty string should return None."""
        result = ObsQuery._parse_value("")
        assert result is None
    
    def test_parse_whitespace(self):
        """Whitespace should return None."""
        result = ObsQuery._parse_value("   ")
        assert result is None


class TestObsSpecExamples:
    """Test the exact examples from the user request."""
    
    def test_example_1000_slash_0(self):
        """1000/0 -> obsnum=1000, roach=0"""
        result = ObsQuery.parse_obs_spec("1000/0")
        assert result == {'obsnum': 1000, 'roach': 0}
    
    def test_example_1000_dash_0_slash_0(self):
        """1000-0/0 -> obsnum=1000, subobsnum=0, roach=0"""
        result = ObsQuery.parse_obs_spec("1000-0/0")
        assert result == {'obsnum': 1000, 'subobsnum': 0, 'roach': 0}
    
    def test_example_1000_slash_0_slash_0(self):
        """1000/0/0 -> obsnum=1000, scannum=0, roach=0"""
        result = ObsQuery.parse_obs_spec("1000/0/0")
        assert result == {'obsnum': 1000, 'scannum': 0, 'roach': 0}
    
    def test_example_1000_dash_0_dash_0(self):
        """1000-0-0 -> obsnum=1000, subobsnum=0, scannum=0"""
        result = ObsQuery.parse_obs_spec("1000-0-0")
        assert result == {'obsnum': 1000, 'subobsnum': 0, 'scannum': 0}
    
    def test_example_1000_brackets_equiv_1000_slash_1(self):
        """1000-[]-[]-1 is equiv to 1000/1"""
        result1 = ObsQuery.parse_obs_spec("1000-[]-[]-1")
        result2 = ObsQuery.parse_obs_spec("1000/1")
        
        # Both should have obsnum=1000, roach=1
        assert result1['obsnum'] == 1000
        assert result1['roach'] == 1
        assert result2['obsnum'] == 1000
        assert result2['roach'] == 1
        
        # result1 also has wildcard slices
        assert 'subobsnum_slice' in result1
        assert 'scannum_slice' in result1
    
    def test_example_braces_slash_1(self):
        """{}/1 -> roach=1, with wildcard obsnum"""
        result = ObsQuery.parse_obs_spec("{}/1")
        assert result == {'roach': 1, 'obsnum_slice': slice(None)}
        # obsnum_slice indicates match all obsnum
    
    def test_example_brackets_slash_1(self):
        """[]/1 -> roach=1, with wildcard obsnum"""
        result = ObsQuery.parse_obs_spec("[]/1")
        assert result == {'roach': 1, 'obsnum_slice': slice(None)}
        # obsnum_slice indicates match all obsnum


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
