"""Domain-layer dataclasses for tolteca_db entities.

These are plain Python frozen dataclasses that mirror the ORM record structure
but are fully decoupled from SQLAlchemy.  They serve as the currency of the
Repository layer — all Repository methods accept and return these objects.

ORM → domain
    Manual converter functions (e.g. :func:`raw_obs_from_record`) are provided
    for each entity.  We use manual converters rather than
    ``adaptix.conversion.get_converter`` because the ORM models use
    ``from __future__ import annotations`` combined with ``TYPE_CHECKING``-only
    imports for circular relationship references, which prevents adaptix from
    resolving the full type-hint graph at converter-creation time in Python 3.14.

domain ↔ JSON dict
    A module-level :data:`retort` (``adaptix.Retort``) handles bidirectional
    JSON serialisation:

    - ``retort.dump(obj)`` → JSON-compatible dict (``datetime`` → ISO-8601 string).
    - ``retort.load(data, Type)`` → domain object (ISO string → ``datetime``).

    Use :func:`to_dict` / :func:`from_dict` as thin convenience wrappers.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from adaptix import Retort

if TYPE_CHECKING:
    from tolteca_db.models.orm.assoc import AssocEdgeRecord, AssocRecord
    from tolteca_db.models.orm.data_prod import FileRecord, StorageRootRecord
    from tolteca_db.models.orm.event import EventRecord
    from tolteca_db.models.orm.flag import DataProdFlagRecord, ObsFlagRecord
    from tolteca_db.models.orm.registry import DataProdRecord, RawObsRecord
    from tolteca_db.models.orm.task import TaskRecord

__all__ = [
    # Domain dataclasses
    "RawObs",
    "DataProd",
    "StorageRoot",
    "StorageFile",
    "AssocGroup",
    "AssocEdge",
    "ObsFlag",
    "DataProdFlag",
    "Task",
    "Event",
    # ORM → domain converters
    "raw_obs_from_record",
    "data_prod_from_record",
    "storage_root_from_record",
    "storage_file_from_record",
    "assoc_group_from_record",
    "assoc_edge_from_record",
    "obs_flag_from_record",
    "data_prod_flag_from_record",
    "task_from_record",
    "event_from_record",
    # JSON serialisation
    "retort",
    "to_dict",
    "from_dict",
]


# ---------------------------------------------------------------------------
# Domain dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RawObs:
    """Domain object for a raw TolTEC observation.

    Mirrors :class:`~tolteca_db.models.orm.registry.RawObsRecord` without
    SQLAlchemy instrumentation.

    Attributes
    ----------
    uid : str
        ObsSpec UID, e.g. ``"tcs-98765-0-0"``.
    master : str
        Master type (``tcs``, ``ics``, ``clip``).
    obsnum : int
        Observation number.
    subobsnum : int
        Sub-observation number.
    scannum : int
        Scan number.
    timestamp_obs : datetime | None
        UTC timestamp of observation start.
    meta : dict | None
        Optional JSON metadata.
    created_at : datetime
        Database insert timestamp.
    """

    uid: str
    master: str
    obsnum: int
    subobsnum: int
    scannum: int
    timestamp_obs: datetime | None
    meta: dict[str, Any] | None
    created_at: datetime


@dataclass(frozen=True)
class DataProd:
    """Domain object for a logical TolTEC data product.

    Mirrors :class:`~tolteca_db.models.orm.registry.DataProdRecord`.
    All tolteca_web catalog columns are preserved so the Repository can return
    catalog-ready rows without additional SQL joins.

    Attributes
    ----------
    pk : int
        Integer primary key.
    uid : str
        Human-readable unique identifier.
    raw_obs_uid : str
        Parent observation UID.
    data_prod_type : str
        Product type (e.g. ``dp_raw_obs``, ``dp_zarr_sweep``).
    interface : str
        Interface identifier (e.g. ``toltec0``, ``tel_toltec``).
    nw : int | None
        Roach network index (0-12).
    array_name : str | None
        Array name (``a1100``, ``a1400``, ``a2000``).
    data_kind : str | None
        Data kind label (``VnaSweep``, ``TargetSweep``, …).
    n_chans : int | None
        Number of detector channels.
    n_samples : int | None
        Number of samples.
    lo_center_freq_hz : float | None
        LO centre frequency in Hz.
    drive_atten_db : float | None
        Drive attenuation in dB.
    sense_atten_db : float | None
        Sense attenuation in dB.
    nc_path : str | None
        Path to the original netCDF file.
    zarr_path : str | None
        Path to the derived zarr store.
    availability : str | None
        Availability state.
    meta : dict | None
        Optional JSON metadata.
    created_at : datetime
        Database insert timestamp.
    updated_at : datetime
        Last update timestamp.
    """

    pk: int
    uid: str
    raw_obs_uid: str
    data_prod_type: str
    interface: str
    nw: int | None
    array_name: str | None
    data_kind: str | None
    n_chans: int | None
    n_samples: int | None
    lo_center_freq_hz: float | None
    drive_atten_db: float | None
    sense_atten_db: float | None
    nc_path: str | None
    zarr_path: str | None
    availability: str | None
    meta: dict[str, Any] | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class StorageRoot:
    """Domain object for a data storage root.

    Mirrors :class:`~tolteca_db.models.orm.data_prod.StorageRootRecord`.

    Attributes
    ----------
    pk : int
        Integer primary key.
    label : str
        Unique human-readable label.
    host : str | None
        Hostname for remote roots.
    root_path : str
        Absolute path or URI prefix.
    is_local : bool
        ``True`` if root is locally accessible.
    meta : dict | None
        Optional JSON metadata.
    created_at : datetime
        Database insert timestamp.
    """

    pk: int
    label: str
    host: str | None
    root_path: str
    is_local: bool
    meta: dict[str, Any] | None
    created_at: datetime


@dataclass(frozen=True)
class StorageFile:
    """Domain object for a physical file record.

    Mirrors :class:`~tolteca_db.models.orm.data_prod.FileRecord` (renamed to
    ``StorageFile`` to avoid collision with the ORM class name).

    Attributes
    ----------
    pk : int
        Integer primary key.
    data_prod_fk : int
        Foreign key to :class:`DataProd`.
    storage_root_fk : int | None
        Foreign key to :class:`StorageRoot`; ``None`` if root is unknown.
    rel_path : str
        Path relative to the storage root.
    mtime : float | None
        POSIX modification timestamp.
    file_size : int | None
        File size in bytes.
    checksum : str | None
        Content checksum (e.g. ``"blake3:abcd..."``).
    created_at : datetime
        Database insert timestamp.
    updated_at : datetime
        Last update timestamp.
    """

    pk: int
    data_prod_fk: int
    storage_root_fk: int | None
    rel_path: str
    mtime: float | None
    file_size: int | None
    checksum: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class AssocEdge:
    """Domain object for a single association edge.

    Mirrors :class:`~tolteca_db.models.orm.assoc.AssocEdgeRecord`.

    Attributes
    ----------
    pk : int
        Integer primary key.
    assoc_fk : int
        Foreign key to the parent :class:`AssocGroup`.
    data_prod_fk : int
        Foreign key to the linked :class:`DataProd`.
    role : str
        Role label (e.g. ``"input"``, ``"output"``).
    """

    pk: int
    assoc_fk: int
    data_prod_fk: int
    role: str


@dataclass(frozen=True)
class AssocGroup:
    """Domain object for an association group.

    Mirrors :class:`~tolteca_db.models.orm.assoc.AssocRecord`.
    Related :class:`AssocEdge` objects are loaded separately by the Repository.

    Attributes
    ----------
    pk : int
        Integer primary key.
    uid : str
        Unique identifier.
    rule_name : str
        Name of the association rule that created this group.
    context : dict | None
        Rule-specific context parameters.
    status : str
        Status string (e.g. ``"pending"``, ``"complete"``).
    score : float | None
        Optional quality score.
    created_at : datetime
        Database insert timestamp.
    updated_at : datetime
        Last update timestamp.
    """

    pk: int
    uid: str
    rule_name: str
    context: dict[str, Any] | None
    status: str
    score: float | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class ObsFlag:
    """Domain object for an observation-level quality flag.

    Mirrors :class:`~tolteca_db.models.orm.flag.ObsFlagRecord`.

    Attributes
    ----------
    pk : int
        Integer primary key.
    raw_obs_uid : str
        UID of the flagged observation.
    flag_reason : str
        Human-readable reason for the flag.
    flag_source : str
        Source that set the flag (e.g. ``"manual"``, ``"pipeline"``).
    flagged_at : datetime
        Timestamp when the flag was set.
    """

    pk: int
    raw_obs_uid: str
    flag_reason: str
    flag_source: str
    flagged_at: datetime


@dataclass(frozen=True)
class DataProdFlag:
    """Domain object for a data-product-level quality flag.

    Mirrors :class:`~tolteca_db.models.orm.flag.DataProdFlagRecord`.

    Attributes
    ----------
    pk : int
        Integer primary key.
    data_prod_fk : int
        Foreign key to the flagged :class:`DataProd`.
    flag_reason : str
        Human-readable reason for the flag.
    flag_source : str
        Source that set the flag.
    flagged_at : datetime
        Timestamp when the flag was set.
    """

    pk: int
    data_prod_fk: int
    flag_reason: str
    flag_source: str
    flagged_at: datetime


@dataclass(frozen=True)
class Task:
    """Domain object for a pipeline task.

    Mirrors :class:`~tolteca_db.models.orm.task.TaskRecord`.

    Attributes
    ----------
    pk : int
        Integer primary key.
    uid : str
        Unique task identifier.
    assoc_fk : int | None
        Optional foreign key to parent :class:`AssocGroup`.
    task_type : str
        Task type label (e.g. ``"ingest"``, ``"reduce"``).
    status : str
        Current status (``"queued"``, ``"running"``, ``"done"``, …).
    started_at : datetime | None
        When the task started execution.
    completed_at : datetime | None
        When the task completed.
    error_msg : str | None
        Error message if the task failed.
    meta : dict | None
        Optional JSON metadata.
    created_at : datetime
        Database insert timestamp.
    updated_at : datetime
        Last update timestamp.
    """

    pk: int
    uid: str
    assoc_fk: int | None
    task_type: str
    status: str
    started_at: datetime | None
    completed_at: datetime | None
    error_msg: str | None
    meta: dict[str, Any] | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class Event:
    """Domain object for an immutable event log entry.

    Mirrors :class:`~tolteca_db.models.orm.event.EventRecord`.

    Attributes
    ----------
    seq : int
        Monotonically-increasing sequence number (primary key).
    event_type : str
        Event type label (e.g. ``"obs.created"``, ``"dp.updated"``).
    entity_type : str
        Type of the affected entity (e.g. ``"raw_obs"``, ``"data_prod"``).
    entity_id : str
        String identifier of the affected entity.
    payload : dict | None
        Optional event payload.
    occurred_at : datetime
        When the event occurred.
    """

    seq: int
    event_type: str
    entity_type: str
    entity_id: str
    payload: dict[str, Any] | None
    occurred_at: datetime


# ---------------------------------------------------------------------------
# ORM → Domain converters
#
# Manual implementations are used rather than adaptix.conversion.get_converter
# because the ORM models combine `from __future__ import annotations` with
# TYPE_CHECKING-only circular imports for relationship fields.  Python 3.14's
# stricter forward-reference evaluation causes NameError when adaptix calls
# get_type_hints() on those classes at converter-creation time.
# ---------------------------------------------------------------------------


def raw_obs_from_record(rec: RawObsRecord) -> RawObs:
    """Convert :class:`~tolteca_db.models.orm.registry.RawObsRecord` → :class:`RawObs`."""
    return RawObs(
        uid=rec.uid,
        master=rec.master,
        obsnum=rec.obsnum,
        subobsnum=rec.subobsnum,
        scannum=rec.scannum,
        timestamp_obs=rec.timestamp_obs,
        meta=rec.meta,
        created_at=rec.created_at,
    )


def data_prod_from_record(rec: DataProdRecord) -> DataProd:
    """Convert :class:`~tolteca_db.models.orm.registry.DataProdRecord` → :class:`DataProd`."""
    return DataProd(
        pk=rec.pk,
        uid=rec.uid,
        raw_obs_uid=rec.raw_obs_uid,
        data_prod_type=rec.data_prod_type,
        interface=rec.interface,
        nw=rec.nw,
        array_name=rec.array_name,
        data_kind=rec.data_kind,
        n_chans=rec.n_chans,
        n_samples=rec.n_samples,
        lo_center_freq_hz=rec.lo_center_freq_hz,
        drive_atten_db=rec.drive_atten_db,
        sense_atten_db=rec.sense_atten_db,
        nc_path=rec.nc_path,
        zarr_path=rec.zarr_path,
        availability=rec.availability,
        meta=rec.meta,
        created_at=rec.created_at,
        updated_at=rec.updated_at,
    )


def storage_root_from_record(rec: StorageRootRecord) -> StorageRoot:
    """Convert :class:`~tolteca_db.models.orm.data_prod.StorageRootRecord` → :class:`StorageRoot`."""
    return StorageRoot(
        pk=rec.pk,
        label=rec.label,
        host=rec.host,
        root_path=rec.root_path,
        is_local=rec.is_local,
        meta=rec.meta,
        created_at=rec.created_at,
    )


def storage_file_from_record(rec: FileRecord) -> StorageFile:
    """Convert :class:`~tolteca_db.models.orm.data_prod.FileRecord` → :class:`StorageFile`."""
    return StorageFile(
        pk=rec.pk,
        data_prod_fk=rec.data_prod_fk,
        storage_root_fk=rec.storage_root_fk,
        rel_path=rec.rel_path,
        mtime=rec.mtime,
        file_size=rec.file_size,
        checksum=rec.checksum,
        created_at=rec.created_at,
        updated_at=rec.updated_at,
    )


def assoc_group_from_record(rec: AssocRecord) -> AssocGroup:
    """Convert :class:`~tolteca_db.models.orm.assoc.AssocRecord` → :class:`AssocGroup`."""
    return AssocGroup(
        pk=rec.pk,
        uid=rec.uid,
        rule_name=rec.rule_name,
        context=rec.context,
        status=rec.status,
        score=rec.score,
        created_at=rec.created_at,
        updated_at=rec.updated_at,
    )


def assoc_edge_from_record(rec: AssocEdgeRecord) -> AssocEdge:
    """Convert :class:`~tolteca_db.models.orm.assoc.AssocEdgeRecord` → :class:`AssocEdge`."""
    return AssocEdge(
        pk=rec.pk,
        assoc_fk=rec.assoc_fk,
        data_prod_fk=rec.data_prod_fk,
        role=rec.role,
    )


def obs_flag_from_record(rec: ObsFlagRecord) -> ObsFlag:
    """Convert :class:`~tolteca_db.models.orm.flag.ObsFlagRecord` → :class:`ObsFlag`."""
    return ObsFlag(
        pk=rec.pk,
        raw_obs_uid=rec.raw_obs_uid,
        flag_reason=rec.flag_reason,
        flag_source=rec.flag_source,
        flagged_at=rec.flagged_at,
    )


def data_prod_flag_from_record(rec: DataProdFlagRecord) -> DataProdFlag:
    """Convert :class:`~tolteca_db.models.orm.flag.DataProdFlagRecord` → :class:`DataProdFlag`."""
    return DataProdFlag(
        pk=rec.pk,
        data_prod_fk=rec.data_prod_fk,
        flag_reason=rec.flag_reason,
        flag_source=rec.flag_source,
        flagged_at=rec.flagged_at,
    )


def task_from_record(rec: TaskRecord) -> Task:
    """Convert :class:`~tolteca_db.models.orm.task.TaskRecord` → :class:`Task`."""
    return Task(
        pk=rec.pk,
        uid=rec.uid,
        assoc_fk=rec.assoc_fk,
        task_type=rec.task_type,
        status=rec.status,
        started_at=rec.started_at,
        completed_at=rec.completed_at,
        error_msg=rec.error_msg,
        meta=rec.meta,
        created_at=rec.created_at,
        updated_at=rec.updated_at,
    )


def event_from_record(rec: EventRecord) -> Event:
    """Convert :class:`~tolteca_db.models.orm.event.EventRecord` → :class:`Event`."""
    return Event(
        seq=rec.seq,
        event_type=rec.event_type,
        entity_type=rec.entity_type,
        entity_id=rec.entity_id,
        payload=rec.payload,
        occurred_at=rec.occurred_at,
    )


# ---------------------------------------------------------------------------
# JSON serialisation via adaptix Retort
# ---------------------------------------------------------------------------

#: Global Retort for serialising domain objects to/from JSON-compatible dicts.
#:
#: - ``retort.dump(obj)`` → dict with ``datetime`` as ISO-8601 strings.
#: - ``retort.load(data, SomeType)`` → domain object.
retort: Retort = Retort()


def to_dict(obj: object) -> dict[str, Any]:
    """Serialise a domain object to a JSON-compatible ``dict``.

    Parameters
    ----------
    obj :
        Any domain dataclass (e.g. :class:`RawObs`, :class:`DataProd`).

    Returns
    -------
    dict[str, Any]
        A plain dict suitable for ``json.dumps()``.  ``datetime`` values are
        serialised to ISO-8601 strings.

    Examples
    --------
    >>> from datetime import datetime, timezone
    >>> obs = RawObs('tcs-1-0-0', 'tcs', 1, 0, 0, None, None, datetime(2024,1,1, tzinfo=timezone.utc))
    >>> d = to_dict(obs)
    >>> d['uid']
    'tcs-1-0-0'
    >>> d['created_at']
    '2024-01-01T00:00:00+00:00'
    """
    return retort.dump(obj)  # type: ignore[return-value]


def from_dict(data: dict[str, Any], tp: type) -> object:
    """Deserialise a ``dict`` to a domain object.

    Parameters
    ----------
    data :
        Dict produced by :func:`to_dict` or from a JSON response.
    tp :
        Target domain dataclass type (e.g. :class:`RawObs`).

    Returns
    -------
    object
        An instance of *tp*.

    Examples
    --------
    >>> from datetime import datetime, timezone
    >>> obs = RawObs('tcs-1-0-0', 'tcs', 1, 0, 0, None, None, datetime(2024,1,1, tzinfo=timezone.utc))
    >>> from_dict(to_dict(obs), RawObs) == obs
    True
    """
    return retort.load(data, tp)
