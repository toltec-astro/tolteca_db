"""Tests for tolteca_db.models.metadata — domain dataclasses and converters.

Covers:
- Frozen dataclass immutability
- ORM → domain conversion for all 10 entity types
- JSON round-trip via Retort (to_dict / from_dict)
"""

from __future__ import annotations

import dataclasses
from datetime import datetime, timezone

import pytest
from sqlalchemy.orm import Session

from tolteca_db.models.metadata import (
    AssocEdge,
    AssocGroup,
    DataProd,
    DataProdFlag,
    Event,
    ObsFlag,
    RawObs,
    StorageFile,
    StorageRoot,
    Task,
    assoc_edge_from_record,
    assoc_group_from_record,
    data_prod_flag_from_record,
    data_prod_from_record,
    event_from_record,
    from_dict,
    obs_flag_from_record,
    raw_obs_from_record,
    retort,
    storage_file_from_record,
    storage_root_from_record,
    task_from_record,
    to_dict,
)
from tolteca_db.models.orm import (
    AssocEdgeRecord,
    AssocRecord,
    DataProdFlagRecord,
    DataProdRecord,
    EventRecord,
    FileRecord,
    ObsFlagRecord,
    RawObsRecord,
    StorageRootRecord,
    TaskRecord,
)

_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_raw_obs(session: Session, uid: str = "tcs-1-0-0") -> RawObsRecord:
    rec = RawObsRecord(
        uid=uid, master="tcs", obsnum=1, subobsnum=0, scannum=0,
        timestamp_obs=_NOW, meta={"src": "test"},
    )
    session.add(rec)
    session.flush()
    return rec


def _make_data_prod(session: Session, raw_obs_uid: str = "tcs-1-0-0") -> DataProdRecord:
    rec = DataProdRecord(
        uid=f"{raw_obs_uid}-toltec0-VnaSweep",
        raw_obs_uid=raw_obs_uid,
        data_prod_type="dp_raw_obs",
        interface="toltec0",
        nw=0,
        array_name="a1100",
        data_kind="VnaSweep",
        n_chans=1000,
        n_samples=500,
        lo_center_freq_hz=1.0e9,
        drive_atten_db=30.0,
        sense_atten_db=30.0,
        nc_path="/data/toltec0.nc",
        zarr_path="/data/toltec0.zarr",
        availability="available",
        meta={"version": 1},
    )
    session.add(rec)
    session.flush()
    return rec


# ---------------------------------------------------------------------------
# TestFrozenDataclasses
# ---------------------------------------------------------------------------


class TestFrozenDataclasses:
    """Verify that all domain dataclasses are frozen (immutable)."""

    def test_raw_obs_frozen(self):
        obs = RawObs("tcs-1-0-0", "tcs", 1, 0, 0, None, None, _NOW)
        with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
            obs.obsnum = 99  # type: ignore[misc]

    def test_data_prod_frozen(self):
        dp = DataProd(1, "uid", "tcs-1-0-0", "dp_raw_obs", "toltec0",
                      None, None, None, None, None, None, None, None, None, None,
                      None, None, _NOW, _NOW)
        with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
            dp.pk = 99  # type: ignore[misc]

    def test_event_frozen(self):
        ev = Event(1, "obs.created", "raw_obs", "tcs-1-0-0", None, _NOW)
        with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
            ev.seq = 99  # type: ignore[misc]


# ---------------------------------------------------------------------------
# TestRawObsConverter
# ---------------------------------------------------------------------------


class TestRawObsConverter:
    """raw_obs_from_record: ORM RawObsRecord → RawObs domain object."""

    def test_all_fields_copied(self, session):
        rec = _make_raw_obs(session)
        obs = raw_obs_from_record(rec)
        assert obs.uid == rec.uid
        assert obs.master == rec.master
        assert obs.obsnum == rec.obsnum
        assert obs.subobsnum == rec.subobsnum
        assert obs.scannum == rec.scannum
        assert obs.timestamp_obs == rec.timestamp_obs
        assert obs.meta == rec.meta
        assert isinstance(obs.created_at, datetime)

    def test_null_timestamp(self, session):
        rec = RawObsRecord(uid="tcs-2-0-0", master="tcs", obsnum=2, subobsnum=0, scannum=0)
        session.add(rec)
        session.flush()
        obs = raw_obs_from_record(rec)
        assert obs.timestamp_obs is None

    def test_returns_frozen_dataclass(self, session):
        rec = _make_raw_obs(session)
        obs = raw_obs_from_record(rec)
        assert isinstance(obs, RawObs)
        assert dataclasses.is_dataclass(obs)


# ---------------------------------------------------------------------------
# TestDataProdConverter
# ---------------------------------------------------------------------------


class TestDataProdConverter:
    """data_prod_from_record: ORM DataProdRecord → DataProd domain object."""

    def test_scalar_fields(self, session):
        _make_raw_obs(session)
        rec = _make_data_prod(session)
        dp = data_prod_from_record(rec)
        assert dp.pk == rec.pk
        assert dp.uid == rec.uid
        assert dp.raw_obs_uid == rec.raw_obs_uid
        assert dp.data_prod_type == rec.data_prod_type
        assert dp.interface == rec.interface

    def test_catalog_columns(self, session):
        _make_raw_obs(session)
        rec = _make_data_prod(session)
        dp = data_prod_from_record(rec)
        assert dp.nw == 0
        assert dp.array_name == "a1100"
        assert dp.data_kind == "VnaSweep"
        assert dp.n_chans == 1000
        assert dp.n_samples == 500
        assert dp.lo_center_freq_hz == 1.0e9
        assert dp.drive_atten_db == 30.0
        assert dp.sense_atten_db == 30.0
        assert dp.nc_path == "/data/toltec0.nc"
        assert dp.zarr_path == "/data/toltec0.zarr"

    def test_nullable_catalog_columns(self, session):
        _make_raw_obs(session, "tcs-3-0-0")
        rec = DataProdRecord(
            uid="tcs-3-0-0-tel_toltec",
            raw_obs_uid="tcs-3-0-0",
            data_prod_type="dp_raw_obs",
            interface="tel_toltec",
        )
        session.add(rec)
        session.flush()
        dp = data_prod_from_record(rec)
        assert dp.nw is None
        assert dp.array_name is None
        assert dp.zarr_path is None

    def test_availability_and_meta(self, session):
        _make_raw_obs(session)
        rec = _make_data_prod(session)
        dp = data_prod_from_record(rec)
        assert dp.availability == "available"
        assert dp.meta == {"version": 1}


# ---------------------------------------------------------------------------
# TestStorageRootConverter
# ---------------------------------------------------------------------------


class TestStorageRootConverter:
    """storage_root_from_record: ORM StorageRootRecord → StorageRoot."""

    def test_fields(self, session):
        rec = StorageRootRecord(
            label="site_data", host=None, root_path="/data/toltec", is_local=True,
        )
        session.add(rec)
        session.flush()
        sr = storage_root_from_record(rec)
        assert sr.pk == rec.pk
        assert sr.label == "site_data"
        assert sr.host is None
        assert sr.root_path == "/data/toltec"
        assert sr.is_local is True


# ---------------------------------------------------------------------------
# TestStorageFileConverter
# ---------------------------------------------------------------------------


class TestStorageFileConverter:
    """storage_file_from_record: ORM FileRecord → StorageFile."""

    def test_fields(self, session):
        _make_raw_obs(session)
        dp_rec = _make_data_prod(session)
        rec = FileRecord(
            data_prod_fk=dp_rec.pk,
            storage_root_fk=None,
            rel_path="toltec0.nc",
            mtime=1.234e9,
            file_size=1024,
            checksum="blake3:abc",
        )
        session.add(rec)
        session.flush()
        sf = storage_file_from_record(rec)
        assert sf.pk == rec.pk
        assert sf.data_prod_fk == dp_rec.pk
        assert sf.storage_root_fk is None
        assert sf.rel_path == "toltec0.nc"
        assert sf.mtime == 1.234e9
        assert sf.file_size == 1024
        assert sf.checksum == "blake3:abc"


# ---------------------------------------------------------------------------
# TestAssocConverters
# ---------------------------------------------------------------------------


class TestAssocConverters:
    """assoc_group_from_record and assoc_edge_from_record."""

    def test_assoc_group(self, session):
        rec = AssocRecord(uid="ag-1", rule_name="sweep_pair", status="pending")
        session.add(rec)
        session.flush()
        ag = assoc_group_from_record(rec)
        assert ag.pk == rec.pk
        assert ag.uid == "ag-1"
        assert ag.rule_name == "sweep_pair"
        assert ag.status == "pending"
        assert ag.score is None

    def test_assoc_edge(self, session):
        _make_raw_obs(session)
        dp_rec = _make_data_prod(session)
        assoc_rec = AssocRecord(uid="ag-2", rule_name="r", status="done")
        session.add(assoc_rec)
        session.flush()
        edge_rec = AssocEdgeRecord(
            assoc_fk=assoc_rec.pk, data_prod_fk=dp_rec.pk, role="input",
        )
        session.add(edge_rec)
        session.flush()
        edge = assoc_edge_from_record(edge_rec)
        assert edge.pk == edge_rec.pk
        assert edge.assoc_fk == assoc_rec.pk
        assert edge.data_prod_fk == dp_rec.pk
        assert edge.role == "input"


# ---------------------------------------------------------------------------
# TestFlagConverters
# ---------------------------------------------------------------------------


class TestFlagConverters:
    """obs_flag_from_record and data_prod_flag_from_record."""

    def test_obs_flag(self, session):
        _make_raw_obs(session)
        rec = ObsFlagRecord(
            raw_obs_uid="tcs-1-0-0", flag_reason="bad_data",
            flag_source="manual", flagged_at=_NOW,
        )
        session.add(rec)
        session.flush()
        flag = obs_flag_from_record(rec)
        assert flag.pk == rec.pk
        assert flag.raw_obs_uid == "tcs-1-0-0"
        assert flag.flag_reason == "bad_data"
        assert flag.flag_source == "manual"
        assert flag.flagged_at == _NOW

    def test_data_prod_flag(self, session):
        _make_raw_obs(session)
        dp_rec = _make_data_prod(session)
        rec = DataProdFlagRecord(
            data_prod_fk=dp_rec.pk, flag_reason="saturated",
            flag_source="pipeline", flagged_at=_NOW,
        )
        session.add(rec)
        session.flush()
        flag = data_prod_flag_from_record(rec)
        assert flag.pk == rec.pk
        assert flag.data_prod_fk == dp_rec.pk
        assert flag.flag_reason == "saturated"


# ---------------------------------------------------------------------------
# TestTaskConverter
# ---------------------------------------------------------------------------


class TestTaskConverter:
    """task_from_record: ORM TaskRecord → Task."""

    def test_queued_task(self, session):
        rec = TaskRecord(uid="task-1", task_type="ingest", status="queued")
        session.add(rec)
        session.flush()
        t = task_from_record(rec)
        assert t.pk == rec.pk
        assert t.uid == "task-1"
        assert t.task_type == "ingest"
        assert t.status == "queued"
        assert t.assoc_fk is None
        assert t.started_at is None
        assert t.completed_at is None
        assert t.error_msg is None

    def test_completed_task(self, session):
        rec = TaskRecord(
            uid="task-2", task_type="reduce", status="done",
            started_at=_NOW, completed_at=_NOW,
        )
        session.add(rec)
        session.flush()
        t = task_from_record(rec)
        assert t.status == "done"
        assert t.started_at == _NOW
        assert t.completed_at == _NOW


# ---------------------------------------------------------------------------
# TestEventConverter
# ---------------------------------------------------------------------------


class TestEventConverter:
    """event_from_record: ORM EventRecord → Event."""

    def test_fields(self, session):
        rec = EventRecord(
            event_type="obs.created",
            entity_type="raw_obs",
            entity_id="tcs-1-0-0",
            payload={"obsnum": 1},
        )
        session.add(rec)
        session.flush()
        ev = event_from_record(rec)
        assert ev.seq == rec.seq
        assert ev.event_type == "obs.created"
        assert ev.entity_type == "raw_obs"
        assert ev.entity_id == "tcs-1-0-0"
        assert ev.payload == {"obsnum": 1}
        assert isinstance(ev.occurred_at, datetime)


# ---------------------------------------------------------------------------
# TestJsonSerialisation
# ---------------------------------------------------------------------------


class TestJsonSerialisation:
    """Retort.dump / Retort.load round-trips for domain objects."""

    def test_raw_obs_to_dict(self, session):
        rec = _make_raw_obs(session)
        obs = raw_obs_from_record(rec)
        d = to_dict(obs)
        assert isinstance(d, dict)
        assert d["uid"] == obs.uid
        assert d["master"] == obs.master
        assert d["obsnum"] == obs.obsnum
        assert isinstance(d["created_at"], str)  # datetime → ISO string

    def test_raw_obs_round_trip(self, session):
        rec = _make_raw_obs(session)
        obs = raw_obs_from_record(rec)
        d = to_dict(obs)
        obs2 = from_dict(d, RawObs)
        assert obs2 == obs

    def test_data_prod_round_trip(self, session):
        _make_raw_obs(session)
        rec = _make_data_prod(session)
        dp = data_prod_from_record(rec)
        dp2 = from_dict(to_dict(dp), DataProd)
        assert dp2 == dp

    def test_event_round_trip(self, session):
        rec = EventRecord(
            event_type="dp.updated",
            entity_type="data_prod",
            entity_id="uid-42",
            payload={"key": "val"},
        )
        with Session(next(iter(session.bind.connect().__class__.__mro__), None) or object()) as s:
            pass
        session.add(rec)
        session.flush()
        ev = event_from_record(rec)
        ev2 = from_dict(to_dict(ev), Event)
        assert ev2 == ev

    def test_null_fields_preserved(self, session):
        obs = RawObs("tcs-99-0-0", "tcs", 99, 0, 0,
                     timestamp_obs=None, meta=None, created_at=_NOW)
        d = to_dict(obs)
        assert d["timestamp_obs"] is None
        assert d["meta"] is None
        obs2 = from_dict(d, RawObs)
        assert obs2.timestamp_obs is None
        assert obs2.meta is None

    def test_retort_is_module_level_instance(self):
        from tolteca_db.models.metadata import retort as _retort
        from adaptix import Retort
        assert isinstance(_retort, Retort)
