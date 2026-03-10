"""Tests for v3.x SQLAlchemy 2.x ORM models.

Verifies:
1. Schema creation (all tables present, indexes created)
2. Model instantiation and CRUD round-trips
3. Relationships (FK inserts, back-populates)
4. Catalog-query columns on DataProdRecord
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import Session

from tolteca_db.models.orm import (
    AssocEdgeRecord,
    AssocRecord,
    Base,
    DataProdFlagRecord,
    DataProdRecord,
    EventRecord,
    FileRecord,
    ObsFlagRecord,
    RawObsRecord,
    StorageRootRecord,
    TaskRecord,
)


# ---------------------------------------------------------------------------
# Test fixtures
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
# Helpers
# ---------------------------------------------------------------------------

def _make_raw_obs(uid: str = "tcs-98765-0-0", **kwargs) -> RawObsRecord:
    defaults = dict(
        uid=uid,
        master="tcs",
        obsnum=98765,
        subobsnum=0,
        scannum=0,
    )
    defaults.update(kwargs)
    return RawObsRecord(**defaults)


def _make_data_prod(
    raw_obs_uid: str = "tcs-98765-0-0",
    uid: str = "tcs-98765-0-0-toltec0-VnaSweep",
    interface: str = "toltec0",
    **kwargs,
) -> DataProdRecord:
    defaults = dict(
        uid=uid,
        raw_obs_uid=raw_obs_uid,
        data_prod_type="dp_raw_obs",
        interface=interface,
        nw=0,
        array_name="a1100",
        data_kind="VnaSweep",
        n_chans=1000,
        n_samples=500,
        lo_center_freq_hz=349.0e9,
        drive_atten_db=30.0,
        sense_atten_db=30.0,
        nc_path="/data/toltec0_098765_000_0000_sweep.nc",
        zarr_path="/scratch/toltec0_098765_000_0000_sweep.zarr",
        availability="available",
    )
    defaults.update(kwargs)
    return DataProdRecord(**defaults)


# ---------------------------------------------------------------------------
# TestSchemaCreation
# ---------------------------------------------------------------------------

class TestSchemaCreation:
    """Table and index creation."""

    def test_all_tables_present(self, engine):
        """All 10 v3.x tables must be present."""
        expected = {
            "raw_obs",
            "data_prod",
            "storage_root",
            "file_record",
            "assoc",
            "assoc_edge",
            "obs_flag",
            "data_prod_flag",
            "task",
            "event",
        }
        actual = set(inspect(engine).get_table_names())
        assert actual == expected

    def test_raw_obs_columns(self, engine):
        """raw_obs table has required catalog columns."""
        cols = {c["name"] for c in inspect(engine).get_columns("raw_obs")}
        for col in ("uid", "master", "obsnum", "subobsnum", "scannum",
                     "timestamp_obs", "created_at"):
            assert col in cols, f"Missing column: {col}"

    def test_data_prod_catalog_columns(self, engine):
        """data_prod table has all tolteca_web catalog columns."""
        cols = {c["name"] for c in inspect(engine).get_columns("data_prod")}
        catalog_cols = (
            "uid", "raw_obs_uid", "data_prod_type", "interface",
            "nw", "array_name", "data_kind", "n_chans", "n_samples",
            "lo_center_freq_hz", "drive_atten_db", "sense_atten_db",
            "nc_path", "zarr_path", "availability",
            "created_at", "updated_at",
        )
        for col in catalog_cols:
            assert col in cols, f"Missing catalog column: {col}"

    def test_indexes_on_data_prod(self, engine):
        """data_prod has composite indexes for catalog queries."""
        indexes = inspect(engine).get_indexes("data_prod")
        index_cols = {
            frozenset(idx["column_names"])
            for idx in indexes
        }
        # Should have indexes on raw_obs_uid+interface and data_kind+array_name
        assert frozenset({"raw_obs_uid", "interface"}) in index_cols
        assert frozenset({"data_kind", "array_name"}) in index_cols


# ---------------------------------------------------------------------------
# TestRawObsRecord
# ---------------------------------------------------------------------------

class TestRawObsRecord:
    """RawObsRecord CRUD and constraints."""

    def test_insert_and_retrieve(self, session):
        """Insert a RawObsRecord and retrieve by PK."""
        obs = _make_raw_obs()
        session.add(obs)
        session.flush()

        retrieved = session.get(RawObsRecord, "tcs-98765-0-0")
        assert retrieved is not None
        assert retrieved.uid == "tcs-98765-0-0"
        assert retrieved.master == "tcs"
        assert retrieved.obsnum == 98765
        assert retrieved.subobsnum == 0
        assert retrieved.scannum == 0

    def test_uid_is_primary_key(self, session):
        """uid is the PK — duplicate insert must fail."""
        session.add(_make_raw_obs("tcs-1-0-0"))
        session.flush()

        from sqlalchemy.exc import IntegrityError
        with pytest.raises(IntegrityError):
            session.add(_make_raw_obs("tcs-1-0-0"))
            session.flush()

    def test_optional_timestamp_obs(self, session):
        """timestamp_obs is nullable."""
        obs = _make_raw_obs(timestamp_obs=None)
        session.add(obs)
        session.flush()

        retrieved = session.get(RawObsRecord, "tcs-98765-0-0")
        assert retrieved.timestamp_obs is None

    def test_timestamp_obs_stored(self, session):
        """timestamp_obs stores timezone-aware datetime."""
        ts = datetime(2024, 3, 15, 12, 0, 0, tzinfo=UTC)
        obs = _make_raw_obs(timestamp_obs=ts)
        session.add(obs)
        session.flush()

        retrieved = session.get(RawObsRecord, "tcs-98765-0-0")
        # SQLite strips timezone info; compare naive
        expected = ts.replace(tzinfo=None)
        assert retrieved.timestamp_obs.replace(tzinfo=None) == expected

    def test_multiple_masters(self, session):
        """Multiple masters can be inserted."""
        for master, obsnum in (("tcs", 1), ("ics", 2), ("clip", 3)):
            uid = f"{master}-{obsnum}-0-0"
            session.add(RawObsRecord(uid=uid, master=master, obsnum=obsnum,
                                     subobsnum=0, scannum=0))
        session.flush()
        count = session.query(RawObsRecord).count()
        assert count == 3

    def test_repr(self, session):
        """__repr__ includes uid."""
        obs = _make_raw_obs()
        assert "tcs-98765-0-0" in repr(obs)


# ---------------------------------------------------------------------------
# TestDataProdRecord
# ---------------------------------------------------------------------------

class TestDataProdRecord:
    """DataProdRecord CRUD, FK, and catalog columns."""

    def test_insert_with_raw_obs_fk(self, session):
        """DataProdRecord can be inserted under a RawObsRecord."""
        obs = _make_raw_obs()
        dp = _make_data_prod()
        session.add(obs)
        session.add(dp)
        session.flush()

        retrieved = session.query(DataProdRecord).filter_by(
            uid="tcs-98765-0-0-toltec0-VnaSweep"
        ).one()
        assert retrieved.raw_obs_uid == "tcs-98765-0-0"
        assert retrieved.nw == 0
        assert retrieved.array_name == "a1100"
        assert retrieved.data_kind == "VnaSweep"
        assert retrieved.n_chans == 1000
        assert retrieved.zarr_path == "/scratch/toltec0_098765_000_0000_sweep.zarr"

    def test_nullable_catalog_columns(self, session):
        """Catalog columns are nullable for non-roach interfaces."""
        obs = _make_raw_obs()
        dp = DataProdRecord(
            uid="tcs-98765-0-0-tel_toltec",
            raw_obs_uid="tcs-98765-0-0",
            data_prod_type="dp_raw_obs",
            interface="tel_toltec",
        )
        session.add(obs)
        session.add(dp)
        session.flush()

        retrieved = session.query(DataProdRecord).filter_by(
            uid="tcs-98765-0-0-tel_toltec"
        ).one()
        assert retrieved.nw is None
        assert retrieved.array_name is None
        assert retrieved.zarr_path is None

    def test_uid_unique_constraint(self, session):
        """uid column has a unique constraint."""
        obs = _make_raw_obs()
        dp1 = _make_data_prod()
        dp2 = _make_data_prod()  # same uid!
        session.add(obs)
        session.add(dp1)
        session.flush()

        from sqlalchemy.exc import IntegrityError
        with pytest.raises(IntegrityError):
            session.add(dp2)
            session.flush()

    def test_relationship_to_raw_obs(self, session):
        """raw_obs relationship back-populates data_prods."""
        obs = _make_raw_obs()
        dp = _make_data_prod()
        session.add(obs)
        session.add(dp)
        session.flush()
        session.expire_all()

        obs_reloaded = session.get(RawObsRecord, "tcs-98765-0-0")
        assert len(obs_reloaded.data_prods) == 1
        assert obs_reloaded.data_prods[0].uid == "tcs-98765-0-0-toltec0-VnaSweep"

    def test_autoincrement_pk(self, session):
        """pk is autoincrement — multiple inserts get distinct PKs."""
        obs = _make_raw_obs()
        dp1 = _make_data_prod(uid="u1")
        dp2 = _make_data_prod(uid="u2")
        session.add_all([obs, dp1, dp2])
        session.flush()
        assert dp1.pk != dp2.pk

    def test_catalog_all_thirteen_roaches(self, session):
        """Can store one DataProdRecord per roach (0-12) under one obs."""
        obs = _make_raw_obs()
        session.add(obs)
        for nw in range(13):
            session.add(DataProdRecord(
                uid=f"tcs-98765-0-0-toltec{nw}-VnaSweep",
                raw_obs_uid="tcs-98765-0-0",
                data_prod_type="dp_raw_obs",
                interface=f"toltec{nw}",
                nw=nw,
                data_kind="VnaSweep",
            ))
        session.flush()
        count = session.query(DataProdRecord).filter_by(
            raw_obs_uid="tcs-98765-0-0"
        ).count()
        assert count == 13


# ---------------------------------------------------------------------------
# TestStorageRootRecord
# ---------------------------------------------------------------------------

class TestStorageRootRecord:
    """StorageRootRecord CRUD."""

    def test_insert_and_retrieve(self, session):
        root = StorageRootRecord(
            label="site_data",
            root_path="/data/toltec",
            is_local=True,
        )
        session.add(root)
        session.flush()
        assert root.pk is not None

        retrieved = session.query(StorageRootRecord).filter_by(
            label="site_data"
        ).one()
        assert retrieved.root_path == "/data/toltec"
        assert retrieved.is_local is True

    def test_unique_label(self, session):
        session.add(StorageRootRecord(label="archive", root_path="/archive"))
        session.flush()

        from sqlalchemy.exc import IntegrityError
        with pytest.raises(IntegrityError):
            session.add(StorageRootRecord(label="archive", root_path="/other"))
            session.flush()


# ---------------------------------------------------------------------------
# TestFileRecord
# ---------------------------------------------------------------------------

class TestFileRecord:
    """FileRecord CRUD and FK."""

    def test_insert_with_data_prod_fk(self, session):
        obs = _make_raw_obs()
        dp = _make_data_prod()
        session.add_all([obs, dp])
        session.flush()

        fr = FileRecord(
            data_prod_fk=dp.pk,
            rel_path="toltec/tcs/toltec0_098765_000_0000_sweep.nc",
            file_size=104857600,
            checksum="blake3:abc123",
        )
        session.add(fr)
        session.flush()
        assert fr.pk is not None

        retrieved = session.query(FileRecord).filter_by(
            data_prod_fk=dp.pk
        ).one()
        assert retrieved.checksum == "blake3:abc123"

    def test_null_storage_root(self, session):
        obs = _make_raw_obs()
        dp = _make_data_prod()
        session.add_all([obs, dp])
        session.flush()

        fr = FileRecord(data_prod_fk=dp.pk, rel_path="some/path.nc")
        session.add(fr)
        session.flush()
        assert fr.storage_root_fk is None

    def test_relationship_to_data_prod(self, session):
        obs = _make_raw_obs()
        dp = _make_data_prod()
        fr = FileRecord(rel_path="x.nc")
        dp.file_records.append(fr)
        session.add_all([obs, dp])
        session.flush()
        session.expire_all()

        dp_reloaded = session.query(DataProdRecord).filter_by(
            uid="tcs-98765-0-0-toltec0-VnaSweep"
        ).one()
        assert len(dp_reloaded.file_records) == 1


# ---------------------------------------------------------------------------
# TestAssocRecord
# ---------------------------------------------------------------------------

class TestAssocRecord:
    """AssocRecord CRUD and edges."""

    def test_insert_assoc(self, session):
        assoc = AssocRecord(
            uid="cal-group-tcs-98765",
            rule_name="cal_match",
            status="pending",
        )
        session.add(assoc)
        session.flush()
        assert assoc.pk is not None

    def test_assoc_edge(self, session):
        obs = _make_raw_obs()
        dp = _make_data_prod()
        assoc = AssocRecord(uid="a1", rule_name="rule1")
        session.add_all([obs, dp, assoc])
        session.flush()

        edge = AssocEdgeRecord(
            assoc_fk=assoc.pk,
            data_prod_fk=dp.pk,
            role="input",
        )
        session.add(edge)
        session.flush()

        session.expire_all()
        assoc_r = session.query(AssocRecord).filter_by(uid="a1").one()
        assert len(assoc_r.edges) == 1
        assert assoc_r.edges[0].role == "input"


# ---------------------------------------------------------------------------
# TestFlagRecords
# ---------------------------------------------------------------------------

class TestFlagRecords:
    """ObsFlagRecord and DataProdFlagRecord."""

    def test_obs_flag(self, session):
        obs = _make_raw_obs()
        session.add(obs)
        session.flush()

        flag = ObsFlagRecord(
            raw_obs_uid="tcs-98765-0-0",
            flag_reason="bad weather",
            flag_source="manual",
            flagged_at=datetime(2024, 3, 15, 12, 0, tzinfo=UTC),
        )
        session.add(flag)
        session.flush()
        assert flag.pk is not None

        session.expire_all()
        obs_r = session.get(RawObsRecord, "tcs-98765-0-0")
        assert len(obs_r.obs_flags) == 1
        assert obs_r.obs_flags[0].flag_reason == "bad weather"

    def test_data_prod_flag(self, session):
        obs = _make_raw_obs()
        dp = _make_data_prod()
        session.add_all([obs, dp])
        session.flush()

        flag = DataProdFlagRecord(
            data_prod_fk=dp.pk,
            flag_reason="high noise",
            flag_source="auto",
            flagged_at=datetime(2024, 3, 15, 12, 0, tzinfo=UTC),
        )
        session.add(flag)
        session.flush()
        assert flag.pk is not None


# ---------------------------------------------------------------------------
# TestTaskRecord
# ---------------------------------------------------------------------------

class TestTaskRecord:
    """TaskRecord CRUD."""

    def test_insert_task(self, session):
        task = TaskRecord(
            uid="task-zarr-tcs-98765-toltec0",
            task_type="zarr_convert",
            status="queued",
            meta={"input_nc": "/data/toltec0_sweep.nc"},
        )
        session.add(task)
        session.flush()
        assert task.pk is not None

    def test_task_linked_to_assoc(self, session):
        assoc = AssocRecord(uid="a1", rule_name="r1")
        task = TaskRecord(uid="t1", task_type="zarr_convert")
        assoc.tasks.append(task)
        session.add(assoc)
        session.flush()
        session.expire_all()

        assoc_r = session.query(AssocRecord).filter_by(uid="a1").one()
        assert len(assoc_r.tasks) == 1

    def test_task_completed(self, session):
        task = TaskRecord(
            uid="t2",
            task_type="zarr_convert",
            status="done",
            started_at=datetime(2024, 3, 15, 12, 0, tzinfo=UTC),
            completed_at=datetime(2024, 3, 15, 12, 5, tzinfo=UTC),
        )
        session.add(task)
        session.flush()
        retrieved = session.query(TaskRecord).filter_by(uid="t2").one()
        assert retrieved.status == "done"


# ---------------------------------------------------------------------------
# TestEventRecord
# ---------------------------------------------------------------------------

class TestEventRecord:
    """EventRecord (append-only log)."""

    def test_insert_events(self, session):
        for etype, eid in (
            ("obs_ingested", "tcs-98765-0-0"),
            ("zarr_written", "tcs-98765-0-0-toltec0-VnaSweep"),
            ("flag_added", "tcs-98765-0-0"),
        ):
            session.add(EventRecord(
                event_type=etype,
                entity_type="raw_obs",
                entity_id=eid,
                payload={"detail": "test"},
            ))
        session.flush()
        count = session.query(EventRecord).count()
        assert count == 3

    def test_seq_autoincrement(self, session):
        e1 = EventRecord(event_type="t", entity_type="e", entity_id="id1")
        e2 = EventRecord(event_type="t", entity_type="e", entity_id="id2")
        session.add_all([e1, e2])
        session.flush()
        assert e1.seq != e2.seq
        assert abs(e2.seq - e1.seq) == 1


# ---------------------------------------------------------------------------
# TestCatalogQuery
# ---------------------------------------------------------------------------

class TestCatalogQuery:
    """Verify that the data_prod table supports the tolteca_web catalog query."""

    def _populate(self, session):
        """Insert obs + 13 roach prods for testing."""
        obs = _make_raw_obs(
            timestamp_obs=datetime(2024, 3, 15, 12, 0, tzinfo=UTC)
        )
        session.add(obs)
        array_map = {
            **{i: "a1100" for i in range(7)},
            **{i: "a1400" for i in range(7, 10)},
            **{i: "a2000" for i in range(10, 13)},
        }
        for nw in range(13):
            session.add(DataProdRecord(
                uid=f"tcs-98765-0-0-toltec{nw}-VnaSweep",
                raw_obs_uid="tcs-98765-0-0",
                data_prod_type="dp_raw_obs",
                interface=f"toltec{nw}",
                nw=nw,
                array_name=array_map[nw],
                data_kind="VnaSweep",
                n_chans=1000,
                n_samples=500,
                lo_center_freq_hz=349.0e9,
                drive_atten_db=30.0,
                sense_atten_db=30.0,
                nc_path=f"/data/toltec{nw}_098765_sweep.nc",
                zarr_path=f"/scratch/toltec{nw}_098765_sweep.zarr",
                availability="available",
            ))
        session.flush()

    def test_filter_by_array_name(self, session):
        """Filter data_prods by array_name returns correct subset."""
        self._populate(session)
        rows = session.query(DataProdRecord).filter_by(array_name="a1100").all()
        assert len(rows) == 7

    def test_filter_by_data_kind(self, session):
        """Filter by data_kind returns all 13 roach products."""
        self._populate(session)
        rows = session.query(DataProdRecord).filter_by(data_kind="VnaSweep").all()
        assert len(rows) == 13

    def test_join_raw_obs_for_date(self, session):
        """Join raw_obs to get timestamp for catalog query."""
        self._populate(session)
        from sqlalchemy import select
        stmt = (
            select(RawObsRecord.uid, RawObsRecord.timestamp_obs,
                   DataProdRecord.nw, DataProdRecord.zarr_path)
            .join(DataProdRecord, DataProdRecord.raw_obs_uid == RawObsRecord.uid)
            .where(DataProdRecord.array_name == "a1100")
            .order_by(DataProdRecord.nw)
        )
        rows = session.execute(stmt).all()
        assert len(rows) == 7
        assert rows[0].nw == 0
        assert rows[0].zarr_path == "/scratch/toltec0_098765_sweep.zarr"

    def test_zarr_path_nullable(self, session):
        """zarr_path may be NULL before zarr generation is complete."""
        obs = _make_raw_obs()
        dp = DataProdRecord(
            uid="tcs-98765-0-0-toltec0-notzarr",
            raw_obs_uid="tcs-98765-0-0",
            data_prod_type="dp_raw_obs",
            interface="toltec0",
            zarr_path=None,
        )
        session.add_all([obs, dp])
        session.flush()
        retrieved = session.query(DataProdRecord).filter_by(
            uid="tcs-98765-0-0-toltec0-notzarr"
        ).one()
        assert retrieved.zarr_path is None
