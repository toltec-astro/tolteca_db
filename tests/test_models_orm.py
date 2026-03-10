"""Tests for v3.x SQLAlchemy 2.x ORM models.

Verifies:
1. Schema creation (all 11 tables present, indexes created)
2. Model instantiation and CRUD round-trips
3. Relationships (FK inserts, back-populates)
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import Session

from tolteca_db.constants import ReducedStatus, StorageRole
from tolteca_db.models.metadata import RawObsMeta, RoachInterfaceMeta
from tolteca_db.models.orm import (
    Base,
    DataKind,
    DataProd,
    DataProdAssoc,
    DataProdAssocType,
    DataProdDataKind,
    DataProdFlag,
    DataProdSource,
    DataProdType,
    EventLog,
    Flag,
    Location,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def engine():
    """Create in-memory SQLite database with all v3.x tables."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def session(engine):
    """Create a transactional SQLAlchemy session."""
    with Session(engine) as session:
        yield session
        session.rollback()


# ---------------------------------------------------------------------------
# Helpers to insert pre-requisite registry rows
# ---------------------------------------------------------------------------


def _make_location(session: Session, label: str = "local") -> Location:
    loc = Location(label=label, location_type="filesystem", root_uri="file:///data/")
    session.add(loc)
    session.flush()
    return loc


def _make_data_prod_type(session: Session, label: str = "dp_raw_obs") -> DataProdType:
    dpt = DataProdType(label=label, level=0)
    session.add(dpt)
    session.flush()
    return dpt


def _make_data_prod(session: Session, dpt: DataProdType) -> DataProd:
    meta = RawObsMeta(name="test-obs", data_prod_type="dp_raw_obs")
    dp = DataProd(
        data_prod_type_fk=dpt.pk,
        lifecycle_status=ReducedStatus.ACTIVE.value,
        meta=meta,
    )
    session.add(dp)
    session.flush()
    return dp


# ---------------------------------------------------------------------------
# Schema creation
# ---------------------------------------------------------------------------


class TestSchemaCreation:
    """Test that all 11 tables are created."""

    def test_all_tables_created(self, engine):
        expected_tables = {
            "location",
            "data_kind",
            "data_prod_type",
            "data_prod",
            "data_prod_data_kind",
            "data_prod_source",
            "data_prod_assoc_type",
            "data_prod_assoc",
            "flag",
            "data_prod_flag",
            "event_log",
        }
        inspector = inspect(engine)
        actual_tables = set(inspector.get_table_names())
        assert actual_tables >= expected_tables

    def test_event_log_composite_index(self, engine):
        inspector = inspect(engine)
        indexes = inspector.get_indexes("event_log")
        names = {idx["name"] for idx in indexes}
        assert "ix_event_entity" in names


# ---------------------------------------------------------------------------
# Location
# ---------------------------------------------------------------------------


class TestLocation:
    def test_create_location(self, session):
        loc = Location(
            label="site_data",
            location_type="filesystem",
            root_uri="file:///data_lmt/",
            priority=10,
        )
        session.add(loc)
        session.flush()
        assert loc.pk is not None
        assert loc.priority == 10

    def test_label_unique(self, session):
        from sqlalchemy.exc import IntegrityError

        session.add(Location(label="dup", location_type="filesystem", root_uri="file:///a/"))
        session.flush()
        session.add(Location(label="dup", location_type="filesystem", root_uri="file:///b/"))
        with pytest.raises(IntegrityError):
            session.flush()


# ---------------------------------------------------------------------------
# DataProdType
# ---------------------------------------------------------------------------


class TestDataProdType:
    def test_create(self, session):
        dpt = DataProdType(label="dp_raw_obs", level=0)
        session.add(dpt)
        session.flush()
        assert dpt.pk is not None
        assert dpt.label == "dp_raw_obs"


# ---------------------------------------------------------------------------
# DataProd
# ---------------------------------------------------------------------------


class TestDataProd:
    def test_create_with_meta(self, session):
        dpt = _make_data_prod_type(session)
        meta = RawObsMeta(
            name="tcs-18596-0-0",
            data_prod_type="dp_raw_obs",
            obsnum=18596,
            master="tcs",
        )
        dp = DataProd(
            data_prod_type_fk=dpt.pk,
            lifecycle_status=ReducedStatus.ACTIVE.value,
            meta=meta,
        )
        session.add(dp)
        session.flush()
        assert dp.pk is not None
        assert dp.lifecycle_status == "active"

    def test_default_lifecycle_status(self, session):
        dpt = _make_data_prod_type(session)
        dp = DataProd(
            data_prod_type_fk=dpt.pk,
            meta=RawObsMeta(name="x", data_prod_type="dp_raw_obs"),
        )
        session.add(dp)
        session.flush()
        assert dp.lifecycle_status == "active"

    def test_relationship_to_type(self, session):
        dpt = _make_data_prod_type(session)
        dp = _make_data_prod(session, dpt)
        session.refresh(dp)
        assert dp.data_prod_type.label == "dp_raw_obs"


# ---------------------------------------------------------------------------
# DataProdSource
# ---------------------------------------------------------------------------


class TestDataProdSource:
    def test_create_source(self, session):
        loc = _make_location(session)
        dpt = _make_data_prod_type(session)
        dp = _make_data_prod(session, dpt)
        meta = RoachInterfaceMeta(nw_id=0, roach=0, interface="toltec0")
        src = DataProdSource(
            source_uri="file:///data/toltec0_018596_032_0001.nc",
            data_prod_fk=dp.pk,
            location_fk=loc.pk,
            role=StorageRole.PRIMARY.value,
            meta=meta,
        )
        session.add(src)
        session.flush()
        assert src.source_uri.startswith("file://")
        assert src.role == "primary"

    def test_relationship_to_data_prod(self, session):
        loc = _make_location(session)
        dpt = _make_data_prod_type(session)
        dp = _make_data_prod(session, dpt)
        src = DataProdSource(
            source_uri="file:///data/test.nc",
            data_prod_fk=dp.pk,
            location_fk=loc.pk,
            role=StorageRole.PRIMARY.value,
            meta=RoachInterfaceMeta(nw_id=1, roach=1),
        )
        session.add(src)
        session.flush()
        session.refresh(dp)
        assert len(dp.sources) == 1
        assert dp.sources[0].source_uri == "file:///data/test.nc"


# ---------------------------------------------------------------------------
# DataKind + DataProdDataKind
# ---------------------------------------------------------------------------


class TestDataKindAssignment:
    def test_assign_kind_to_product(self, session):
        dpt = _make_data_prod_type(session)
        dp = _make_data_prod(session, dpt)
        dk = DataKind(label="VnaSweep", category="calibration")
        session.add(dk)
        session.flush()
        assignment = DataProdDataKind(
            data_prod_fk=dp.pk,
            data_kind_fk=dk.pk,
            source="manual",
        )
        session.add(assignment)
        session.flush()
        session.refresh(dp)
        assert len(dp.kind) == 1
        assert dp.kind[0].kind.label == "VnaSweep"


# ---------------------------------------------------------------------------
# DataProdAssoc
# ---------------------------------------------------------------------------


class TestDataProdAssoc:
    def test_create_assoc(self, session):
        dpt = _make_data_prod_type(session)
        dp_src = _make_data_prod(session, dpt)
        dp_dst = _make_data_prod(session, dpt)
        assoc_type = DataProdAssocType(label="dpa_reduced_obs_raw_obs")
        session.add(assoc_type)
        session.flush()
        from tolteca_db.models.metadata import ProcessContext

        assoc = DataProdAssoc(
            data_prod_assoc_type_fk=assoc_type.pk,
            src_data_prod_fk=dp_src.pk,
            dst_data_prod_fk=dp_dst.pk,
            context=ProcessContext(module="pipeline", version="1.0"),
        )
        session.add(assoc)
        session.flush()
        assert assoc.pk is not None


# ---------------------------------------------------------------------------
# Flag + DataProdFlag
# ---------------------------------------------------------------------------


class TestFlag:
    def test_create_flag(self, session):
        flag = Flag(label="SATURATED", namespace="qa")
        session.add(flag)
        session.flush()
        assert flag.pk is not None

    def test_assign_flag_to_product(self, session):
        dpt = _make_data_prod_type(session)
        dp = _make_data_prod(session, dpt)
        flag = Flag(label="CORRUPTED", namespace="qa")
        session.add(flag)
        session.flush()
        dpf = DataProdFlag(
            data_prod_fk=dp.pk,
            flag_fk=flag.pk,
            asserted_by="user:test",
        )
        session.add(dpf)
        session.flush()
        session.refresh(dp)
        assert len(dp.flag) == 1
        assert dp.flag[0].flag.label == "CORRUPTED"


# ---------------------------------------------------------------------------
# EventLog
# ---------------------------------------------------------------------------


class TestEventLog:
    def test_create_event(self, session):
        event = EventLog(
            event_type="ProductCreated",
            entity_type="product",
            entity_id="42",
            payload={"obsnum": 18596},
        )
        session.add(event)
        session.flush()
        assert event.seq is not None
        assert event.event_type == "ProductCreated"
