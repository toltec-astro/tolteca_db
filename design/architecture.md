# tolteca_db v3.x Architecture

**Status:** Active  
**Last Updated:** 2026-03-10  
**Branch:** v3.x  
**Constitutional Reference:** [toltec_data_file_and_data_flow.md](toltec_data_file_and_data_flow.md)

---

## Guiding Principles

This document defines the v3.x architecture, informed by the constitutional reference and lessons from v2.5. The central design goal is **conceptual clarity** — every layer, class, and API should have one job and one job only, with explicit, minimal interfaces between them.

### Core Tenets

1. **File locality is the source of truth.** A data product is what it is regardless of whether a database exists. The DB is an *index*, not the primary record.
2. **The ObsSpec is the universal query language.** `{master}-{obsnum}-{subobsnum}-{scannum}` is the atomic identifier for a raw observation. Everything derives from this.
3. **Physical files, logical products, database records are three distinct things.** Never conflate them.
4. **Multi-root, multi-location is first-class.** Site, server archive, server scratch, and user local are all equal peers. No "primary" location is hardcoded.
5. **Clean separation of concerns:** the ingestion pipeline, the ORM schema, the query/repository API, and the orchestration layer (Dagster) are independent modules with defined interfaces.
6. **Minimal surface area.** Expose the smallest API that covers real use cases. Do not add flexibility speculatively.

---

## System Overview

### Deployment Topology (from constitutional doc)

```
site                          server                        local (user)
─────────────────────         ──────────────────────        ─────────────────
raw acq (LMT instruments)     archive data_root(s)          downloaded data_root
  → data_root_site/           scratch data_root(s)          tolteca_db (SQLite)
  → dataprod_root_site/       tolteca_db (PostgreSQL)
  → tolteca_web (gen only)    tolteca_web (serve)
```

Key constraint: **one code base, multiple deployment contexts**. The `Location` registry is the runtime configuration that tells the system where it is.

### Functional Layers

```
┌─────────────────────────────────────────────────────────────────────────┐
│  CLI  /  Dagster Assets  /  tolteca_web API                             │  User-facing
├─────────────────────────────────────────────────────────────────────────┤
│  Query API  (obsspec.py, file_api.py)                                   │  High-level
│  DataFrame compatibility layer  (compat/file.py)                        │
├─────────────────────────────────────────────────────────────────────────┤
│  Repository  (repository.py)                                            │  Business logic
│  Association Engine  (associations/)                                    │
│  Ingestion Pipeline  (ingest/)                                          │
├─────────────────────────────────────────────────────────────────────────┤
│  ORM  (models/orm/)  +  Metadata Models  (models/metadata.py)          │  Data model
├─────────────────────────────────────────────────────────────────────────┤
│  Database layer  (db/)    DuckDB / SQLite / PostgreSQL                  │  Storage
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Domain Model

### Level 0: Identifiers and ObsSpec

The atomic identifier for a raw observation is the **ObsSpec**, composed of:

```
master      ::= "tcs" | "ics" | "clip" | "simu" | <instrument>
obsnum      ::= <positive integer>  (site-wide unique for tcs; instrument-unique for ics)
subobsnum   ::= <non-negative integer>  (default 0)
scannum     ::= <non-negative integer>  (default 0)

ObsSpec ::= "{master}-{obsnum}-{subobsnum}-{scannum}"
           | "{obsnum}-{subobsnum}-{scannum}"    # master-free short form

Examples:
  "tcs-98765-0-0"   # telescope-controlled obs
  "ics-18596-32-0"  # ICS obs (TolTEC-specific testing)
  "18596-0-0"       # short form when master is unambiguous
```

ObsSpec is parsed by a **single canonical function** in `tolteca_db/obsspec.py` — not duplicated anywhere else.

### Level 1: Physical Files

A raw observation consists of a set of **interface files** — one per (instrument, interface):

```
raw obs "tcs-98765-0-0"
├── toltec0_098765_000_0000_timestream.nc    (nw0)
├── toltec1_098765_000_0000_timestream.nc    (nw1)
├── ...
├── toltec12_098765_000_0000_timestream.nc   (nw12)
└── tel_098765_000_0000_tel.nc               (tel)
```

A **data root** is a filesystem root (local path, remote URL, S3, etc.) where such files live. Physical files are accessed via `fsspec`-compatible paths.

**Time-tagged files** (housekeeping) are identified by `(instrument, interface, time_range)` instead of ObsSpec.

### Level 2: Logical Data Products

A **DataProd** is a logical entity tracked in the database:

| Type | UID pattern | Description |
|------|-------------|-------------|
| `dp_raw_obs` | `{master}-{obsnum}-{subobsnum}-{scannum}` | All detector acquisitions + telescope obs |
| `dp_reduced_obs` | `{master}-{obsnum}-{subobsnum}-{scannum}-reduced` | Calibration + time-ordered reduced data |
| `dp_cal_group` | `{master}-{obsnum}-g{n}-cal` | Calibration groupings |
| `dp_drivefit` | `{master}-{obsnum}-g{n}-drivefit` | Detector characterization |
| `dp_focus_group` | `{master}-{obsnum}-g{n}-focus` | Focus analysis groupings |
| `dp_named_group` | user-defined | User-defined collections |
| `dp_map` (future) | blake3 hash | Science maps |
| `dp_catalog` (future) | blake3 hash | Source catalogs |

**Key:** `dp_raw_obs` is *pre-minted* — it exists logically even before files are acquired. Files are registered as `DataProdStorage` rows under the DataProd.

### Level 3: Storage Records

Each **DataProdStorage** row links a DataProd to a physical file at a specific Location:

```
DataProd (dp_raw_obs "tcs-98765-0-0")
└── DataProdStorage rows:
    ├── {location: site_data, filepath: "toltec/tcs/toltec0/toltec0_098765...nc",  interface: "nw0",  role: PRIMARY}
    ├── {location: site_data, filepath: "toltec/tel/tel_098765...nc",               interface: "tel",  role: PRIMARY}
    ├── {location: server_archive, filepath: "2024/toltec0_098765...nc",            interface: "nw0",  role: MIRROR}
    └── ...
```

**Availability state** (`PLANNED` / `PARTIAL` / `AVAILABLE` / `MISSING` / `REMOTE` / `STAGED`) is computed from storage rows, not stored directly.

---

## ORM Schema

### Tables

```
Location                  # Registry of data roots (site, server, local, ...)
DataProdType              # Registry of product types (dp_raw_obs, ...)
DataKind                  # Registry of data kinds (Raw, TimeOrderedData, ...)
DataProdAssocType         # Registry of association types (dpa_raw_obs_cal_obs, ...)
Flag                      # Registry of flag definitions

DataProd                  # Logical data products (pk = blake3 UID string)
DataProdStorage           # Physical file locations (one row per interface per location)
DataProdDataKind          # Junction: product ↔ kind
DataProdAssoc             # Provenance graph edges
DataProdFlag              # Quality flags on products
ReductionTask             # Idempotent reduction task tracking
Event                     # Append-only audit log
```

### Naming Conventions

- Primary keys: `pk` (integer) or `pk` (string for content-addressed DataProd)
- Foreign keys: `{entity}_fk` suffix (e.g., `data_prod_fk`, `location_fk`)
- All timestamps: timezone-aware, database-generated
- Metadata: JSON column named `meta`, type-safe via adaptix models

### Metadata Models (adaptix)

One metadata model per product type, in `models/metadata.py`:

```python
@dataclass
class RawObsMeta:
    master: str
    obsnum: int
    subobsnum: int
    scannum: int
    data_kind: str | None = None     # e.g. "vna_sweep", "targ_sweep", "timestream"

@dataclass
class InterfaceFileMeta:
    interface_id: str                 # e.g. "nw0", "nw1", "tel"
    toltec_db_id: int | None = None   # roach ID / ROACH ID from filename
    instrument: str | None = None    # e.g. "toltec"
```

No other metadata shapes are stored as arbitrary JSON dicts — all metadata is typed.

---

## Module Structure

```
src/tolteca_db/
├── obsspec.py                    # ObsSpec dataclass + canonical parser (single source of truth)
├── constants.py                  # Instrument metadata: interfaces, roach map, masters
├── __init__.py
├── __main__.py
├── py.typed
│
├── models/
│   ├── metadata.py               # adaptix dataclasses for all meta JSON fields
│   └── orm/
│       ├── base.py               # DeclarativeBase, JSON type, timestamp mixin
│       ├── registry.py           # Location, DataProdType, DataKind, DataProdAssocType, Flag
│       ├── data_prod.py          # DataProd, DataProdStorage, DataProdDataKind
│       ├── assoc.py              # DataProdAssoc
│       ├── flag.py               # DataProdFlag
│       ├── task.py               # ReductionTask
│       └── event.py              # Event
│
├── db/
│   ├── config.py                 # Pydantic config: DatabaseConfig, LocationConfig
│   ├── database.py               # Database ABC + DuckDB/SQLite/PostgreSQL impls + create_database()
│   └── parquet.py                # DuckDB-based Parquet query utilities
│
├── repository/
│   ├── __init__.py
│   └── repository.py             # Repository class: CRUD + business queries + ObsSpec resolution
│
├── ingest/
│   ├── __init__.py
│   ├── scanner.py                # FileScanner: walks data_root, yields InterfaceFileRecord
│   ├── ingestor.py               # DataIngestor: scanner → DB registration (Scan → Register → Associate)
│   ├── tel_ingestor.py           # Telescope telemetry file ingestor
│   └── lmtmc_api.py              # LMT machine control API client (live obsnum queries)
│
├── associations/
│   ├── __init__.py
│   ├── engine.py                 # AssociationEngine: produces DataProdAssoc edges from DataProds
│   ├── pool.py                   # ObservationPool: batch DataFrame backend for O(n) load
│   ├── state.py                  # AssociationState: incremental state tracking (DB or FS backend)
│   └── rules.py                  # Association rules (cal group, focus group, etc.) as pure functions
│
├── cli/
│   ├── __init__.py               # Typer app wiring
│   ├── db_commands.py            # db init, db status, db migrate
│   ├── ingest_commands.py        # ingest scan, ingest run, ingest watch
│   ├── query_commands.py         # query obs, query product, query assoc
│   └── assoc_commands.py         # assoc generate, assoc status
│
├── dagster/
│   ├── __init__.py
│   ├── resources.py              # Dagster resources: DatabaseResource, IngestionResource
│   ├── definitions.py            # Dagster Definitions: assets + sensors + jobs
│   ├── assets.py                 # @asset: raw_obs, reduced_obs, cal_group, etc.
│   ├── sensors.py                # Sensors: new quartet detection, file availability
│   └── partitions.py             # Partition strategies: quartet × interface (2D)
│
└── compat/
    └── file.py                   # tolteca_v2-compatible DataFrame accessor (toltec_file)
```

**Removed vs v2.5:**
- `dagster_per_interface_experiment/` — graduated or dropped, no experimental modules in production tree
- `dash/` — moved to `tolteca_web`; tolteca_db does not depend on Dash
- `dagster/test_assets.py`, `dagster/test_resources.py` — moved to `tests/`
- `db/registry.py` — merged into `models/orm/registry.py`
- `db/repository.py` — consolidated into `repository/repository.py`
- `api/` — replaced by `repository/` + `compat/`

---

## Key Interfaces Between Layers

### 1. FileScanner → DataIngestor

```python
@dataclass
class InterfaceFileRecord:
    """A single physical file found by the scanner."""
    filepath: Path          # Absolute path on this machine
    location_label: str     # Which data_root this came from
    master: str
    obsnum: int
    subobsnum: int
    scannum: int
    interface_id: str       # "nw0", "nw1", ..., "tel"
    instrument: str         # "toltec"
    toltec_db_id: int | None
    file_size: int
    mtime: datetime
    content_hash: str | None  # blake3, computed lazily
```

The scanner yields `InterfaceFileRecord` objects. The ingestor consumes them. No coupling to the ORM inside the scanner.

### 2. Repository → upper layers

The `Repository` class exposes:

```python
class Repository:
    # ObsSpec resolution
    def resolve_obs(self, obs_spec: str | ObsSpec) -> DataProd | None: ...
    def get_interface_files(self, obs_spec, location: str | None = None) -> list[DataProdStorage]: ...

    # Raw DataFrame query (tolteca_v2 compat)
    def get_raw_obs_table(self, obs_spec=None, location=None) -> pd.DataFrame: ...

    # Registration
    def register_interface_file(self, record: InterfaceFileRecord) -> DataProdStorage: ...
    def update_availability(self, obs_spec) -> str: ...  # returns new state

    # Provenance
    def get_associations(self, obs_spec, assoc_type=None) -> list[DataProdAssoc]: ...
```

### 3. AssociationEngine ↔ Repository

The engine works on `DataProd` objects (loaded from DB via repository). It produces `DataProdAssoc` candidates as plain objects; the caller (ingestor or Dagster asset) decides when to commit them.

### 4. Dagster ↔ ingestion

Dagster assets call ingestor methods directly. They do not contain business logic — they are thin orchestration wrappers:

```python
@asset
def raw_obs_index(context, db: DatabaseResource, scanner: ScannerResource):
    records = scanner.scan()
    with db.session() as s:
        ingestor = DataIngestor(s)
        for rec in records:
            ingestor.register(rec)
    return Output(metadata={"count": len(records)})
```

---

## Database Layer

### Backend Selection

| Deployment | Metadata DB | Parquet queries | Factory URL |
|------------|------------|-----------------|-------------|
| Dev / test | DuckDB | DuckDB | `duckdb:///tolteca.duckdb` |
| Site / server (write) | SQLite + WAL | DuckDB | `sqlite:///tolteca.db` |
| Server (read-only, concurrent) | SQLite read-only | DuckDB | `sqlite:///tolteca.db` + `read_only=True` |
| Production server | PostgreSQL | DuckDB | `postgresql://...` |
| User local | SQLite | DuckDB | `sqlite:///~/.tolteca/tolteca.db` |

The `create_database(url, read_only=False)` factory auto-selects the right implementation.

### Content Addressing

- `dp_raw_obs` PK: deterministic string `"{master}-{obsnum}-{subobsnum}-{scannum}"` — *not* a hash (human-readable, stable)  
- Derived products PK: blake3 hash of canonical JSON `{type, input_set, params}`  
- File content hash: blake3 of file bytes (with sha256 fallback), stored in `DataProdStorage.content_hash`

> **Rationale change from v2.5:** Raw obs have human-readable PKs because they are minted from a well-defined namespace. Using a hash for them added indirection without benefit.

---

## Association Types

```
dpa_raw_obs_cal_obs          Raw obs used as calibration for another raw obs
dpa_reduced_obs_raw_obs      Reduced product derives from raw obs
dpa_cal_group_raw_obs        Cal group contains raw obs
dpa_drivefit_raw_obs         Drivefit uses raw obs as input
dpa_focus_group_raw_obs      Focus group contains raw obs
dpa_named_group_data_prod    Named group contains any product (generic)
```

Association edges are directed: `(source_fk → target_fk)` where source depends on / contains target.

---

## Availability State Machine

```
PLANNED ──(files appear)──→ PARTIAL ──(all interfaces present)──→ AVAILABLE
   │                           │
   └──(never acquired)──────→ MISSING
                               │
                          (files removed)──→ MISSING
                          (files on remote only)──→ REMOTE
                          (transfer queued)──→ STAGED
```

State is *computed* on read from `DataProdStorage` rows — never stored as a denormalized column.

---

## tolteca_v2 Compatibility

The `compat/file.py` module provides the `toltec_file` pandas accessor backed by tolteca_db queries. A user with existing tolteca_v2 code changes only the import:

```python
# Before (filesystem)
from tolteca_datamodels.toltec.filestore import ToltecFileStore
tfs = ToltecFileStore(path="data_lmt/toltec")
df = tfs.get_raw_obs_info_table(obs_spec="98765-0-0")

# After (database-backed, same DataFrame API)
from tolteca_db.compat.file import ToltecDBFileStore
tfs = ToltecDBFileStore(db_url="sqlite:///tolteca.db", location="local")
df = tfs.get_raw_obs_info_table(obs_spec="98765-0-0")

# Both return a DataFrame with .toltec_file accessor
df.toltec_file.pformat()
with df.toltec_file.open(): ...
```

---

## Dagster Pipeline

### Partition Strategy

2D partitions: `quartet × interface`

- **Quartet** = `{master}-{obsnum}-{subobsnum}` (a set of scans from one observation sequence)
- **Interface** = one of 13 TolTEC interface channels

Completion detection (per quartet):
1. A new quartet appears in `toltec_db` (definitive) — *or*  
2. No `Valid=0→1` transitions for ≥ 30 seconds (timeout-based fallback)

The timeout resets on each new `Valid` transition. `disabled_interfaces` list handles broken channels.

### Asset Dependency Chain

```
file_scan ──→ raw_obs_index ──→ association_graph ──→ reduced_obs ──→ cal_group
                                                              ↓
                                                        focus_group
                                                              ↓
                                                         drivefit
```

---

## Architectural Decision Records

See `adr-0001-human-readable-raw-obs-pk.md` and subsequent ADR files for rationale on key design decisions.

---

## Key Differences from v2.5

| Aspect | v2.5 | v3.x |
|--------|-------|-------|
| Raw obs PK | blake3 hash string | `{master}-{obsnum}-{subobsnum}-{scannum}` (human-readable) |
| ObsSpec parser | Duplicated in `api/obs.py`, `associations/`, CLI | Single `obsspec.py` module |
| `dash/` module | Inside tolteca_db | Moved to tolteca_web |
| Experimental code | `dagster_per_interface_experiment/` in production tree | Tests only; no experimental modules in production |
| Test helpers in src | `dagster/test_assets.py`, `dagster/test_resources.py` | Moved to `tests/` |
| API surface | `api/obs.py`, `repository/file_api.py`, `db/repository.py` | Consolidated: `repository/repository.py` + `compat/file.py` |
| Association rules | Mixed into engine + helpers | Extracted to `associations/rules.py` (pure functions) |
| Availability state | Stored as column | Computed on read from storage rows |
