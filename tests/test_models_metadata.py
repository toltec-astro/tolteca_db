"""Tests for tolteca_db.models.metadata — AdaptixJSON typed metadata.

Covers:
- Dataclass creation with defaults
- Union type discrimination (tag/type fields)
- AdaptixJSON round-trip (SQLAlchemy JSON serialize / deserialize)
- InterfaceFileMeta constraints (no name/master field)
"""

from __future__ import annotations

import dataclasses

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from tolteca_db.models.metadata import (
    AnyDataProdMeta,
    AnyInterfaceMeta,
    AstigGroupMeta,
    CalGroupMeta,
    DrivefitMeta,
    FocusGroupMeta,
    InterfaceFileMeta,
    NamedGroupMeta,
    OofGroupMeta,
    ProcessContext,
    RawObsMeta,
    ReducedObsMeta,
    RoachInterfaceMeta,
    TelInterfaceMeta,
    adaptix_json_type,
)
from tolteca_db.models.orm import (
    Base,
    DataProd,
    DataProdAssoc,
    DataProdAssocType,
    DataProdSource,
    DataProdType,
    Location,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def engine():
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def session(engine):
    with Session(engine) as session:
        yield session
        session.rollback()


# ---------------------------------------------------------------------------
# Tag discriminators
# ---------------------------------------------------------------------------


class TestTags:
    def test_raw_obs_tag(self):
        assert RawObsMeta(name="x", data_prod_type="dp_raw_obs").tag == "raw_obs"

    def test_reduced_obs_tag(self):
        assert ReducedObsMeta(name="x", data_prod_type="dp_reduced_obs").tag == "reduced_obs"

    def test_cal_group_tag(self):
        assert CalGroupMeta(name="x", data_prod_type="dp_cal_group").tag == "cal_group"

    def test_drivefit_tag(self):
        assert DrivefitMeta(name="x", data_prod_type="dp_drivefit").tag == "drivefit"

    def test_focus_group_tag(self):
        assert FocusGroupMeta(name="x", data_prod_type="dp_focus_group").tag == "focus_group"

    def test_astig_group_tag(self):
        assert AstigGroupMeta(name="x", data_prod_type="dp_astig_group").tag == "astig_group"

    def test_oof_group_tag(self):
        assert OofGroupMeta(name="x", data_prod_type="dp_oof_group").tag == "oof_group"

    def test_named_group_tag(self):
        assert NamedGroupMeta(name="x", data_prod_type="dp_named_group").tag == "named_group"


# ---------------------------------------------------------------------------
# RawObsMeta
# ---------------------------------------------------------------------------


class TestRawObsMeta:
    def test_defaults(self):
        meta = RawObsMeta(name="test", data_prod_type="dp_raw_obs")
        assert meta.master == ""
        assert meta.obsnum == 0
        assert meta.subobsnum == 0
        assert meta.scannum == 0
        assert meta.data_kind == 0
        assert meta.nw_id is None
        assert meta.obs_goal is None
        assert meta.description is None

    def test_optional_fields(self):
        meta = RawObsMeta(
            name="test",
            data_prod_type="dp_raw_obs",
            nw_id=3,
            obs_goal="science",
            source_name="M31",
            tau=0.08,
        )
        assert meta.nw_id == 3
        assert meta.obs_goal == "science"
        assert meta.source_name == "M31"
        assert meta.tau == 0.08

    def test_asdict_includes_tag(self):
        meta = RawObsMeta(name="x", data_prod_type="dp_raw_obs", obsnum=42)
        d = dataclasses.asdict(meta)
        assert d["tag"] == "raw_obs"
        assert d["obsnum"] == 42
        assert d["name"] == "x"

    def test_is_valid_any_data_prod_meta(self):
        meta = RawObsMeta(name="x", data_prod_type="dp_raw_obs")
        typed: AnyDataProdMeta = meta
        assert typed.name == "x"


# ---------------------------------------------------------------------------
# InterfaceFileMeta
# ---------------------------------------------------------------------------


class TestInterfaceFileMeta:
    def test_fields(self):
        meta = InterfaceFileMeta(nw_id=5, roach=3)
        assert meta.nw_id == 5
        assert meta.roach == 3

    def test_no_name_field(self):
        with pytest.raises(TypeError, match="unexpected keyword argument"):
            InterfaceFileMeta(name="test")  # type: ignore

    def test_no_master_field(self):
        with pytest.raises(TypeError, match="unexpected keyword argument"):
            InterfaceFileMeta(master="tcs")  # type: ignore

    def test_defaults(self):
        meta = InterfaceFileMeta()
        assert meta.nw_id is None
        assert meta.roach is None

    def test_no_data_prod_type_field(self):
        meta = InterfaceFileMeta(nw_id=1, roach=0)
        assert not hasattr(meta, "data_prod_type")
        assert not hasattr(meta, "name")


# ---------------------------------------------------------------------------
# RoachInterfaceMeta + TelInterfaceMeta
# ---------------------------------------------------------------------------


class TestRoachInterfaceMeta:
    def test_type_discriminator(self):
        meta = RoachInterfaceMeta(nw_id=0, roach=0, interface="toltec0")
        assert meta.type == "roach"

    def test_defaults(self):
        meta = RoachInterfaceMeta()
        assert meta.obsnum == 0
        assert meta.master == ""
        assert meta.nw_id is None

    def test_is_valid_any_interface_meta(self):
        meta = RoachInterfaceMeta(nw_id=1)
        typed: AnyInterfaceMeta = meta
        assert typed.type == "roach"


class TestTelInterfaceMeta:
    def test_type_discriminator(self):
        meta = TelInterfaceMeta()
        assert meta.type == "tel"
        assert meta.interface == "tel_toltec"

    def test_is_valid_any_interface_meta(self):
        meta = TelInterfaceMeta(tau=0.05)
        typed: AnyInterfaceMeta = meta
        assert typed.type == "tel"


# ---------------------------------------------------------------------------
# ProcessContext
# ---------------------------------------------------------------------------


class TestProcessContext:
    def test_defaults(self):
        ctx = ProcessContext()
        assert ctx.module is None
        assert ctx.version is None
        assert ctx.config is None

    def test_fields(self):
        ctx = ProcessContext(module="pipeline", version="1.0", config={"a": 1})
        assert ctx.module == "pipeline"
        assert ctx.config == {"a": 1}


# ---------------------------------------------------------------------------
# AdaptixJSON round-trips via SQLite
# ---------------------------------------------------------------------------


class TestAdaptixJSONRoundTrip:
    """Verifies meta survives a write → read cycle through SQLite."""

    def _setup_type(self, session: Session, label: str = "dp_raw_obs") -> DataProdType:
        dpt = DataProdType(label=label, level=0)
        session.add(dpt)
        session.flush()
        return dpt

    def test_raw_obs_meta_round_trip(self, session):
        dpt = self._setup_type(session)
        original = RawObsMeta(
            name="tcs-18596-0-0",
            data_prod_type="dp_raw_obs",
            obsnum=18596,
            master="tcs",
            nw_id=3,
            tau=0.08,
        )
        dp = DataProd(
            data_prod_type_fk=dpt.pk,
            meta=original,
        )
        session.add(dp)
        session.flush()
        pk = dp.pk
        session.expire_all()
        retrieved = session.get(DataProd, pk)
        assert retrieved is not None
        meta = retrieved.meta
        assert isinstance(meta, RawObsMeta)
        assert meta.name == "tcs-18596-0-0"
        assert meta.obsnum == 18596
        assert meta.nw_id == 3
        assert meta.tau == 0.08
        assert meta.tag == "raw_obs"

    def test_roach_interface_meta_round_trip(self, session):
        loc = Location(label="local", location_type="filesystem", root_uri="file:///data/")
        session.add(loc)
        dpt = self._setup_type(session)
        dp = DataProd(
            data_prod_type_fk=dpt.pk,
            meta=RawObsMeta(name="x", data_prod_type="dp_raw_obs"),
        )
        session.add(dp)
        session.flush()
        original = RoachInterfaceMeta(
            nw_id=0, roach=0, interface="toltec0", obsnum=18596
        )
        src = DataProdSource(
            source_uri="file:///data/toltec0_018596_032_0001.nc",
            data_prod_fk=dp.pk,
            location_fk=loc.pk,
            role="primary",
            meta=original,
        )
        session.add(src)
        session.flush()
        session.expire_all()
        retrieved = session.get(DataProdSource, "file:///data/toltec0_018596_032_0001.nc")
        assert retrieved is not None
        meta = retrieved.meta
        assert isinstance(meta, RoachInterfaceMeta)
        assert meta.nw_id == 0
        assert meta.type == "roach"
        assert meta.obsnum == 18596

    def test_data_prod_assoc_process_context_round_trip(self, session):
        dpt = self._setup_type(session)
        dp1 = DataProd(
            data_prod_type_fk=dpt.pk,
            meta=RawObsMeta(name="a", data_prod_type="dp_raw_obs"),
        )
        dp2 = DataProd(
            data_prod_type_fk=dpt.pk,
            meta=RawObsMeta(name="b", data_prod_type="dp_raw_obs"),
        )
        session.add_all([dp1, dp2])
        session.flush()
        assoc_type = DataProdAssocType(label="dpa_reduced_obs_raw_obs")
        session.add(assoc_type)
        session.flush()
        ctx = ProcessContext(module="pipeline", version="2.0", config={"k": "v"})
        assoc = DataProdAssoc(
            data_prod_assoc_type_fk=assoc_type.pk,
            src_data_prod_fk=dp1.pk,
            dst_data_prod_fk=dp2.pk,
            context=ctx,
        )
        session.add(assoc)
        session.flush()
        pk = assoc.pk
        session.expire_all()
        retrieved = session.get(DataProdAssoc, pk)
        assert retrieved is not None
        c = retrieved.context
        assert isinstance(c, ProcessContext)
        assert c.module == "pipeline"
        assert c.config == {"k": "v"}
