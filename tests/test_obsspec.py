"""Tests for tolteca_db.obsspec and tolteca_db.constants."""

from __future__ import annotations

import dataclasses

import pytest

from tolteca_db.constants import (
    DataKind,
    DataProdAssocType,
    DataProdType,
    FlagSeverity,
    MasterType,
    ReducedStatus,
    StorageRole,
    TaskStatus,
    ToltecDataKind,
    ToltecInfo,
)
from tolteca_db.obsspec import ObsSpec, ObsSpecError


# ===========================================================================
# constants.py
# ===========================================================================


class TestMasterType:
    """Tests for MasterType StrEnum."""

    def test_members(self):
        """MasterType has exactly tcs, ics, clip."""
        names = {m.value for m in MasterType}
        assert names == {"tcs", "ics", "clip"}

    def test_str_value_equals_name(self):
        """StrEnum: str(member) is its value."""
        assert str(MasterType.ics) == "ics"
        assert MasterType.tcs == "tcs"

    def test_is_str(self):
        """MasterType instances are str instances."""
        assert isinstance(MasterType.clip, str)


class TestToltecInfo:
    """Tests for ToltecInfo lookup tables."""

    def test_masters_matches_master_type(self):
        """ToltecInfo.masters contains all MasterType values in order."""
        assert ToltecInfo.masters == [m.value for m in MasterType]

    def test_roach_interfaces_count(self):
        """There are 13 roach interfaces (toltec0 … toltec12)."""
        assert len(ToltecInfo.roach_interfaces) == 13
        assert ToltecInfo.roach_interfaces[0] == "toltec0"
        assert ToltecInfo.roach_interfaces[12] == "toltec12"

    def test_roach_interface_round_trip(self):
        """roach_interface[k] → v and interface_roach[v] → k."""
        for roach, iface in ToltecInfo.roach_interface.items():
            assert ToltecInfo.interface_roach[iface] == roach

    def test_interface_master_mapping(self):
        """Roach interfaces map to ics; tel_toltec maps to tcs."""
        assert ToltecInfo.interface_master["toltec0"] == MasterType.ics
        assert ToltecInfo.interface_master["hwpr"] == MasterType.ics
        assert ToltecInfo.interface_master["tel_toltec"] == MasterType.tcs
        assert ToltecInfo.interface_master["tel_toltec2"] == MasterType.tcs

    def test_array_mapping_coverage(self):
        """All 13 roach interfaces have an array name."""
        for iface in ToltecInfo.roach_interfaces:
            assert iface in ToltecInfo.interface_array_name

    def test_array_names_valid(self):
        """Array names are one of a1100, a1400, a2000."""
        valid = {"a1100", "a1400", "a2000"}
        for iface, array in ToltecInfo.interface_array_name.items():
            assert array in valid, f"{iface} → {array!r} not in {valid}"


class TestDataProdType:
    """Tests for DataProdType StrEnum."""

    def test_value_is_lowercase_name(self):
        """auto() produces lowercase member-name values."""
        assert DataProdType.dp_raw_obs == "dp_raw_obs"
        assert DataProdType.dp_reduced_obs == "dp_reduced_obs"
        assert DataProdType.dp_named_group == "dp_named_group"

    def test_is_str(self):
        """Members are str instances."""
        assert isinstance(DataProdType.dp_raw_obs, str)

    def test_all_obs_types_present(self):
        """Check that the expected set of observation-level types exist."""
        expected = {"dp_raw_obs", "dp_reduced_obs"}
        values = {m.value for m in DataProdType}
        assert expected <= values


class TestDataKind:
    """Tests for DataKind (Flag enum)."""

    def test_vna_sweep_is_raw_sweep(self):
        """VnaSweep is contained in RawSweep composite."""
        assert DataKind.VnaSweep in DataKind.RawSweep

    def test_raw_time_stream_is_raw_kids_data(self):
        """RawTimeStream is contained in RawKidsData composite."""
        assert DataKind.RawTimeStream in DataKind.RawKidsData

    def test_sweep_not_in_raw_time_stream(self):
        """Sweep kinds and RawTimeStream are disjoint."""
        assert not (DataKind.VnaSweep & DataKind.RawTimeStream)

    def test_toltec_data_kind_alias(self):
        """ToltecDataKind is the backward-compat alias for DataKind."""
        assert ToltecDataKind is DataKind


class TestStrEnums:
    """Quick smoke tests for the smaller StrEnum classes."""

    def test_reduced_status_values(self):
        values = {m.value for m in ReducedStatus}
        assert "active" in values
        assert "superseded" in values

    def test_task_status_values(self):
        values = {m.value for m in TaskStatus}
        assert values >= {"queued", "running", "done", "error"}

    def test_flag_severity_order(self):
        """info<warn<block<critical by str comparison via sorted()."""
        levels = list(FlagSeverity)
        assert set(levels) == {
            FlagSeverity.info,
            FlagSeverity.warn,
            FlagSeverity.block,
            FlagSeverity.critical,
        }

    def test_storage_role_values(self):
        values = {m.value for m in StorageRole}
        assert values == {"primary", "mirror", "temp"}

    def test_data_prod_assoc_type_values(self):
        """All assoc-type values start with dpa_."""
        for member in DataProdAssocType:
            assert member.value.startswith("dpa_"), member


# ===========================================================================
# obsspec.py — ObsSpec creation
# ===========================================================================


class TestObsSpecCreate:
    """Tests for ObsSpec construction and basic properties."""

    def test_create_basic(self):
        """Can create an ObsSpec with all fields."""
        obs = ObsSpec(master="ics", obsnum=18596, subobsnum=32, scannum=0)
        assert obs.master == "ics"
        assert obs.obsnum == 18596
        assert obs.subobsnum == 32
        assert obs.scannum == 0

    def test_frozen(self):
        """ObsSpec is immutable (frozen dataclass)."""
        obs = ObsSpec(master="ics", obsnum=18596, subobsnum=32, scannum=0)
        with pytest.raises((AttributeError, dataclasses.FrozenInstanceError)):
            obs.obsnum = 999  # type: ignore[misc]

    def test_hashable(self):
        """ObsSpec can be used as a dict key and in a set."""
        obs = ObsSpec(master="ics", obsnum=18596, subobsnum=32, scannum=0)
        d = {obs: "value"}
        assert d[obs] == "value"
        assert obs in {obs}

    def test_equality(self):
        """Two ObsSpec with identical fields are equal."""
        a = ObsSpec(master="ics", obsnum=100, subobsnum=0, scannum=0)
        b = ObsSpec(master="ics", obsnum=100, subobsnum=0, scannum=0)
        assert a == b

    def test_inequality_different_obsnum(self):
        a = ObsSpec(master="ics", obsnum=100, subobsnum=0, scannum=0)
        b = ObsSpec(master="ics", obsnum=101, subobsnum=0, scannum=0)
        assert a != b

    def test_inequality_different_master(self):
        a = ObsSpec(master="ics", obsnum=100, subobsnum=0, scannum=0)
        b = ObsSpec(master="tcs", obsnum=100, subobsnum=0, scannum=0)
        assert a != b

    def test_ordering(self):
        """ObsSpec instances support < ordering (lexicographic on fields)."""
        early = ObsSpec(master="ics", obsnum=100, subobsnum=0, scannum=0)
        late = ObsSpec(master="ics", obsnum=200, subobsnum=0, scannum=0)
        assert early < late
        assert late > early

    def test_invalid_master_raises(self):
        """Unknown master raises ObsSpecError."""
        with pytest.raises(ObsSpecError, match="Unknown master"):
            ObsSpec(master="simu", obsnum=1000, subobsnum=0, scannum=0)

    def test_obsnum_out_of_range_raises(self):
        """obsnum outside valid range raises ObsSpecError."""
        with pytest.raises(ObsSpecError, match="obsnum"):
            ObsSpec(master="ics", obsnum=0, subobsnum=0, scannum=0)
        with pytest.raises(ObsSpecError, match="obsnum"):
            ObsSpec(master="ics", obsnum=1_000_000, subobsnum=0, scannum=0)

    def test_all_master_types_accepted(self):
        """Each member of MasterType is a valid master."""
        for master in MasterType:
            obs = ObsSpec(master=master, obsnum=1000, subobsnum=0, scannum=0)
            assert obs.master == master


# ===========================================================================
# obsspec.py — uid and __str__
# ===========================================================================


class TestObsSpecUid:
    """Tests for ObsSpec.uid property and string representation."""

    def test_uid_format(self):
        """uid is '{master}-{obsnum}-{subobsnum}-{scannum}' with no zero-padding."""
        obs = ObsSpec(master="ics", obsnum=18596, subobsnum=32, scannum=0)
        assert obs.uid == "ics-18596-32-0"

    def test_uid_no_zero_padding(self):
        """uid does not zero-pad any numeric field."""
        obs = ObsSpec(master="ics", obsnum=1, subobsnum=0, scannum=0)
        assert obs.uid == "ics-1-0-0"

    def test_uid_is_str(self):
        """uid returns a plain str."""
        obs = ObsSpec(master="ics", obsnum=18596, subobsnum=32, scannum=0)
        assert isinstance(obs.uid, str)

    def test_str_returns_uid(self):
        """str(obs) == obs.uid."""
        obs = ObsSpec(master="ics", obsnum=18596, subobsnum=32, scannum=0)
        assert str(obs) == obs.uid

    def test_repr_shows_fields(self):
        """repr shows all four fields."""
        obs = ObsSpec(master="ics", obsnum=18596, subobsnum=32, scannum=0)
        r = repr(obs)
        assert "master='ics'" in r
        assert "obsnum=18596" in r
        assert "subobsnum=32" in r
        assert "scannum=0" in r

    def test_uid_tcs(self):
        obs = ObsSpec(master="tcs", obsnum=18252, subobsnum=2, scannum=0)
        assert obs.uid == "tcs-18252-2-0"


# ===========================================================================
# obsspec.py — ObsSpec.parse()
# ===========================================================================


class TestObsSpecParse:
    """Tests for ObsSpec.parse() classmethod."""

    def test_parse_round_trip(self):
        """parse(obs.uid) returns an equal ObsSpec."""
        obs = ObsSpec(master="ics", obsnum=18596, subobsnum=32, scannum=0)
        assert ObsSpec.parse(obs.uid) == obs

    def test_parse_ics(self):
        """Parse a canonical ICS uid."""
        obs = ObsSpec.parse("ics-18596-32-0")
        assert obs == ObsSpec(master="ics", obsnum=18596, subobsnum=32, scannum=0)

    def test_parse_tcs(self):
        """Parse a canonical TCS uid."""
        obs = ObsSpec.parse("tcs-18596-0-1")
        assert obs.master == "tcs"
        assert obs.obsnum == 18596
        assert obs.subobsnum == 0
        assert obs.scannum == 1

    def test_parse_clip(self):
        """Parse a canonical CLIP uid."""
        obs = ObsSpec.parse("clip-1234-0-0")
        assert obs.master == "clip"

    def test_parse_whitespace_stripped(self):
        """Leading/trailing whitespace is stripped."""
        obs = ObsSpec.parse("  ics-18596-32-0  ")
        assert obs == ObsSpec(master="ics", obsnum=18596, subobsnum=32, scannum=0)

    def test_parse_too_few_parts(self):
        """Three parts raises ObsSpecError."""
        with pytest.raises(ObsSpecError, match="Expected 4"):
            ObsSpec.parse("ics-18596-32")

    def test_parse_too_many_parts(self):
        """Five parts raises ObsSpecError."""
        with pytest.raises(ObsSpecError, match="Expected 4"):
            ObsSpec.parse("ics-18596-32-0-extra")

    def test_parse_unknown_master(self):
        """Unknown master raises ObsSpecError."""
        with pytest.raises(ObsSpecError, match="Unknown master"):
            ObsSpec.parse("simu-18596-32-0")

    def test_parse_non_numeric_obsnum(self):
        """Non-numeric obsnum raises ObsSpecError."""
        with pytest.raises(ObsSpecError):
            ObsSpec.parse("ics-abc-32-0")

    def test_parse_non_numeric_scannum(self):
        """Non-numeric scannum raises ObsSpecError."""
        with pytest.raises(ObsSpecError):
            ObsSpec.parse("ics-18596-32-x")

    def test_parse_result_is_frozen(self):
        """Result is immutable."""
        obs = ObsSpec.parse("ics-18596-32-0")
        with pytest.raises((AttributeError, dataclasses.FrozenInstanceError)):
            obs.obsnum = 0  # type: ignore[misc]


# ===========================================================================
# obsspec.py — ObsSpec.from_path()
# ===========================================================================


class TestObsSpecFromPath:
    """Tests for ObsSpec.from_path() classmethod."""

    # -- ICS files -----------------------------------------------------------

    def test_toltec_roach_file(self):
        """toltecN_OBSNUM_SUBOBS_SCAN[_suffix].nc → master='ics'."""
        obs = ObsSpec.from_path("toltec0_018596_032_0001_targsweep.nc")
        assert obs is not None
        assert obs.master == "ics"
        assert obs.obsnum == 18596
        assert obs.subobsnum == 32
        assert obs.scannum == 1

    def test_toltec_high_roach(self):
        """toltec12_… is valid (highest roach index)."""
        obs = ObsSpec.from_path("toltec12_018596_032_0001.nc")
        assert obs is not None
        assert obs.master == "ics"

    def test_ics_status_file(self):
        """icsN_OBSNUM_SUBOBS_SCAN.txt → master='ics'."""
        obs = ObsSpec.from_path("ics0_018596_032_0001.txt")
        assert obs is not None
        assert obs.master == "ics"
        assert obs.obsnum == 18596
        assert obs.subobsnum == 32
        assert obs.scannum == 1

    def test_hwpr_file(self):
        """hwpr_OBSNUM_SUBOBS_SCAN.nc → master='ics'."""
        obs = ObsSpec.from_path("hwpr_018596_032_0001_hwp.nc")
        assert obs is not None
        assert obs.master == "ics"
        assert obs.obsnum == 18596

    # -- TCS files -----------------------------------------------------------

    def test_tel_toltec_file(self):
        """tel_toltec_OBSNUM_SUBOBS_SCAN.nc → master='tcs'."""
        obs = ObsSpec.from_path("tel_toltec_018596_032_0001.nc")
        assert obs is not None
        assert obs.master == "tcs"
        assert obs.obsnum == 18596

    def test_tel_toltec2_file(self):
        """tel_toltec2_OBSNUM_SUBOBS_SCAN.nc → master='tcs'."""
        obs = ObsSpec.from_path("tel_toltec2_018596_032_0001.nc")
        assert obs is not None
        assert obs.master == "tcs"

    # -- CLIP files ----------------------------------------------------------

    def test_clip_file(self):
        """clip_OBSNUM_SUBOBS_SCAN → master='clip'."""
        obs = ObsSpec.from_path("clip_018596_032_0001.nc")
        assert obs is not None
        assert obs.master == "clip"

    # -- master override -----------------------------------------------------

    def test_master_override(self):
        """Explicit master parameter supersedes inferred master."""
        obs = ObsSpec.from_path("toltec0_018596_032_0001.nc", master="tcs")
        assert obs is not None
        assert obs.master == "tcs"

    def test_master_override_invalid_raises(self):
        """Invalid master override raises ObsSpecError."""
        with pytest.raises(ObsSpecError, match="Unknown master"):
            ObsSpec.from_path("toltec0_018596_032_0001.nc", master="simu")

    # -- no match ------------------------------------------------------------

    def test_unknown_filename_returns_none(self):
        """Unrecognised filenames return None."""
        assert ObsSpec.from_path("unknown_file.fits") is None
        assert ObsSpec.from_path("README.md") is None
        assert ObsSpec.from_path("data.csv") is None

    # -- path handling -------------------------------------------------------

    def test_accepts_path_object(self):
        """from_path accepts a pathlib.Path."""
        from pathlib import Path

        obs = ObsSpec.from_path(Path("toltec0_018596_032_0001_targsweep.nc"))
        assert obs is not None
        assert obs.obsnum == 18596

    def test_uses_basename_only(self):
        """Directory components are ignored; only the filename is matched."""
        obs = ObsSpec.from_path(
            "/data/2024-01-15/commissioning/toltec0_018596_032_0001_targsweep.nc"
        )
        assert obs is not None
        assert obs.obsnum == 18596
        assert obs.master == "ics"

    def test_deep_nested_path(self):
        """Deep path still extracts correct ObsSpec from filename."""
        obs = ObsSpec.from_path(
            "/mnt/lmt/data/ics/2024-01-15/tel_toltec_018596_032_0001.nc"
        )
        assert obs is not None
        assert obs.master == "tcs"
        assert obs.obsnum == 18596

    # -- zero-padding handling -----------------------------------------------

    def test_leading_zeros_stripped(self):
        """Zero-padded numbers in filename parse to integer values without padding."""
        obs = ObsSpec.from_path("toltec0_018596_032_0001_targsweep.nc")
        assert obs is not None
        # uid must not contain zeros (int, not string)
        assert obs.uid == "ics-18596-32-1"


# ===========================================================================
# Integration: parse ↔ from_path consistency
# ===========================================================================


class TestObsSpecIntegration:
    """Cross-method consistency tests."""

    def test_from_path_parse_consistency(self):
        """from_path result can be round-tripped through parse."""
        obs = ObsSpec.from_path("ics0_018596_032_0001.txt")
        assert obs is not None
        reparsed = ObsSpec.parse(obs.uid)
        assert reparsed == obs

    def test_uid_used_as_db_key(self):
        """uid is suitable as a database primary key (str, unique per quartet)."""
        obs_a = ObsSpec(master="ics", obsnum=18596, subobsnum=32, scannum=0)
        obs_b = ObsSpec(master="ics", obsnum=18596, subobsnum=32, scannum=1)
        assert obs_a.uid != obs_b.uid
        # Different master → different uid
        obs_c = ObsSpec(master="tcs", obsnum=18596, subobsnum=32, scannum=0)
        assert obs_a.uid != obs_c.uid

    def test_set_deduplication(self):
        """Identical ObsSpec instances deduplicate in a set."""
        obs1 = ObsSpec(master="ics", obsnum=18596, subobsnum=32, scannum=0)
        obs2 = ObsSpec.parse("ics-18596-32-0")
        assert len({obs1, obs2}) == 1

    def test_sort_by_obsnum(self):
        """A list of ObsSpec sorts correctly by obsnum."""
        obsnums = [300, 100, 200]
        obs_list = [
            ObsSpec(master="ics", obsnum=n, subobsnum=0, scannum=0) for n in obsnums
        ]
        sorted_list = sorted(obs_list)
        assert [o.obsnum for o in sorted_list] == [100, 200, 300]
