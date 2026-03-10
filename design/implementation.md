# tolteca_db v3.x Implementation Plan

**Package:** tolteca_db  
**Branch:** v3.x  
**Status:** Phase 0 — Architecture Complete, Implementation Starting  
**Last Updated:** 2026-03-10  
**Architecture Reference:** [architecture.md](architecture.md)

---

## Philosophy

v3.x is a clean-slate reimplementation. We **do not** iteratively patch v2.5 code. Instead:

1. The v2.5 source tree is available in the working directory as a reference (ported in commit `15d83b0`)
2. Each phase deletes the relevant v2.5 code and replaces it with the v3.x version
3. Tests are written before or alongside implementation (not after)

The order of phases respects dependency direction: low layers first, then up.

---

## Phase Map

```
Phase 0:  Architecture Design                            ✅ COMPLETE
Phase 1:  obsspec.py + constants.py                      ⬜ NEXT
Phase 2:  ORM models (registry + data_prod + assoc)      ⬜
Phase 3:  Metadata models (adaptix)                      ⬜
Phase 4:  Database layer (create_database factory)        ⬜
Phase 5:  Repository                                     ⬜
Phase 6:  FileScanner                                    ⬜
Phase 7:  DataIngestor (Scan → Register → Associate)     ⬜
Phase 8:  AssociationEngine (pool + state + rules)       ⬜
Phase 9:  CLI                                            ⬜
Phase 10: Dagster (resources + assets + sensors)         ⬜
Phase 11: compat/file.py (tolteca_v2 DataFrame compat)   ⬜
Phase 12: tel_ingestor + lmtmc_api                       ⬜
Phase 13: Parquet query layer                            ⬜
Phase 14: Integration tests + documentation              ⬜
```

---

## Phase 0: Architecture Design

**Status:** ✅ COMPLETE  
**Completed:** 2026-03-10

**Deliverables:**
- [x] `design/toltec_data_file_and_data_flow.md` — constitutional reference (copied from refs)
- [x] `design/archive/index.md` — v2.5 design document archive
- [x] `design/architecture.md` — v3.x architecture blueprint
- [x] `design/implementation.md` — this file
- [x] `design/tasks.md` — task tracking

---

## Phase 1: ObsSpec + Constants

**Status:** ⬜ NOT STARTED  
**Depends on:** Phase 0  
**Target files:**
- `src/tolteca_db/obsspec.py` (new — replaces scattered parsing in v2.5)
- `src/tolteca_db/constants.py` (update from v2.5)

### What to Build

**`obsspec.py`** — single source of truth for ObsSpec parsing:

```python
@dataclass(frozen=True)
class ObsSpec:
    obsnum: int
    subobsnum: int = 0
    scannum: int = 0
    master: str | None = None

    def __str__(self) -> str:
        if self.master:
            return f"{self.master}-{self.obsnum}-{self.subobsnum}-{self.scannum}"
        return f"{self.obsnum}-{self.subobsnum}-{self.scannum}"

    @classmethod
    def parse(cls, s: str) -> "ObsSpec": ...

    @property
    def uid(self) -> str:
        return str(self)

    @property
    def quartet_uid(self) -> str:
        """The quartet (obsnum-subobsnum level) UID."""
        if self.master:
            return f"{self.master}-{self.obsnum}-{self.subobsnum}"
        return f"{self.obsnum}-{self.subobsnum}"
```

**`constants.py`** — instrument metadata:
- Interface list: `["nw0", "nw1", ..., "nw12", "tel"]`
- ROACH → interface map: `{0: "nw0", 1: "nw1", ...}`
- Interface → ROACH map (inverse)
- Master list: `["tcs", "ics", "clip", "simu"]`
- File extension patterns per interface

### Tests
- Parse valid ObsSpec strings (with/without master, partial specs)
- Round-trip: `str(ObsSpec.parse(s)) == normalize(s)`
- Invalid strings raise `ValueError`
- quartet_uid, uid properties

---

## Phase 2: ORM Models

**Status:** ⬜ NOT STARTED  
**Depends on:** Phase 1  
**Target files:** `src/tolteca_db/models/orm/`

### Module Breakdown

| Module | Tables |
|--------|--------|
| `base.py` | `DeclarativeBase`, `TimestampMixin`, `JsonType` |
| `registry.py` | `Location`, `DataProdType`, `DataKind`, `DataProdAssocType`, `Flag` |
| `data_prod.py` | `DataProd`, `DataProdStorage`, `DataProdDataKind` |
| `assoc.py` | `DataProdAssocType`, `DataProdAssoc` |
| `flag.py` | `DataProdFlag` |
| `task.py` | `ReductionTask` |
| `event.py` | `Event` |

### Key Changes from v2.5

- `DataProd.pk` for `dp_raw_obs`: human-readable string `"{master}-{obsnum}-{subobsnum}-{scannum}"`, not blake3 hash
- `DataProdStorage` replaces `DataProdSource` (name aligned with architecture doc terminology)
- `availability_state` removed from `DataProd` — computed property on read
- `DataProdAssocType` merged from separate `db/registry.py` into `orm/registry.py`
- Table auto-comments from docstrings (via `__table_args__` or custom base class)

### Tests
- Schema creation (all tables present)
- FK integrity
- Timestamp auto-generation
- JSON round-trip for meta fields

---

## Phase 3: Metadata Models

**Status:** ⬜ NOT STARTED  
**Depends on:** Phase 2  
**Target files:** `src/tolteca_db/models/metadata.py`

### What to Build

Adaptix dataclasses for every `meta` JSON field in the ORM. No arbitrary dicts.

```python
@dataclass
class RawObsMeta:         # DataProd meta for dp_raw_obs
@dataclass
class InterfaceFileMeta:  # DataProdStorage meta
@dataclass
class ReducedObsMeta:     # DataProd meta for dp_reduced_obs
@dataclass
class CalGroupMeta:       # DataProd meta for dp_cal_group
@dataclass
class DrivefitMeta:
@dataclass
class FocusGroupMeta:
@dataclass
class NamedGroupMeta:
@dataclass
class TaskMeta:           # ReductionTask.meta
@dataclass
class EventMeta:          # Event.meta
```

Each has `AdaptixJSON` converter registered in `models/orm/base.py`.

### Tests
- Round-trip serialize/deserialize each metadata model
- Union type dispatch (adaptix with tagged union)
- Missing/extra field handling

---

## Phase 4: Database Layer

**Status:** ⬜ NOT STARTED  
**Depends on:** Phase 3  
**Target files:** `src/tolteca_db/db/`

### What to Build

- `config.py`: `DatabaseConfig(url, read_only=False)`, `LocationConfig`
- `database.py`: `Database` ABC + `DuckDBDatabase`, `SQLiteDatabase`, `PostgreSQLDatabase` + `create_database()` factory
- `parquet.py`: `ParquetQuery` class with `setup_remote_cache()`, DuckDB fsspec integration

### Key Points

- `create_database()` auto-detects backend from URL prefix
- `Database.session()` context manager for transactional operations
- `Database.parquet_query()` for DuckDB analytics queries
- `read_only=True` → DuckDB READ_ONLY mode (multiprocess-safe for Dagster readers)
- Carry over DuckDB-compatibility fixes from v2.5: `Sequence` (not `SERIAL`), no `CASCADE`, `JSON` (not `JSONB`)

### Tests
- Factory URL auto-detection
- Session lifecycle (commit/rollback)
- Schema operations (create_all, drop_all)
- Multiprocess read simulation

---

## Phase 5: Repository

**Status:** ⬜ NOT STARTED  
**Depends on:** Phase 4  
**Target files:** `src/tolteca_db/repository/repository.py`

### What to Build

Single `Repository` class with:

```python
class Repository:
    def __init__(self, session: Session): ...

    # Registry initialization
    def ensure_registry(self, location_configs: list[LocationConfig]) -> None: ...

    # ObsSpec-based queries
    def resolve_obs(self, obs_spec: str | ObsSpec) -> DataProd | None: ...
    def get_interface_files(self, obs: str | ObsSpec, location: str | None = None) -> list[DataProdStorage]: ...
    def get_raw_obs_table(self, obs_spec=None, location=None) -> pd.DataFrame: ...
    def compute_availability(self, obs: str | ObsSpec) -> str: ...

    # Registration
    def get_or_create_raw_obs(self, obs: ObsSpec) -> DataProd: ...
    def register_interface_file(self, record: InterfaceFileRecord) -> DataProdStorage: ...

    # Associations
    def create_association(self, src_pk, tgt_pk, assoc_type: str) -> DataProdAssoc: ...
    def get_associations(self, obs: str | ObsSpec, assoc_type: str | None = None) -> list[DataProdAssoc]: ...

    # Events
    def log_event(self, event_type: str, **meta) -> Event: ...
```

**No** `DataProductRepository`, `AssociationRepository`, etc. as separate classes. One repository, explicit method names. Separate classes add indirection with no benefit at this scale.

### Tests
- ObsSpec resolution (with/without master, partial specs)
- Interface file registration + retrieval
- Availability computation (PLANNED → PARTIAL → AVAILABLE)
- Association creation + retrieval
- Registry idempotency

---

## Phase 6: FileScanner

**Status:** ⬜ NOT STARTED  
**Depends on:** Phase 1 (ObsSpec, constants)  
**Target files:** `src/tolteca_db/ingest/scanner.py`

### What to Build

```python
class FileScanner:
    """Walk a data_root and yield InterfaceFileRecord objects."""

    def __init__(self, data_root: Path | str, location_label: str): ...

    def scan(self) -> Iterator[InterfaceFileRecord]: ...
    def scan_obs(self, obs_spec: str | ObsSpec) -> Iterator[InterfaceFileRecord]: ...
```

**`InterfaceFileRecord`** is the clean interface between scanner and ingestor (defined in `ingest/__init__.py`).

File pattern matching uses `constants.py` patterns, not hardcoded strings.

### Tests
- Scan fixture data_root, verify records
- Pattern matching: tcs vs ics, roach 0-12, tel
- Non-matching files silently ignored
- Missing data_root → `FileNotFoundError`

---

## Phase 7: DataIngestor

**Status:** ⬜ NOT STARTED  
**Depends on:** Phases 5, 6  
**Target files:** `src/tolteca_db/ingest/ingestor.py`

### What to Build

Three-stage pipeline:

```
Stage 1: Scan      FileScanner → stream of InterfaceFileRecord
Stage 2: Register  InterfaceFileRecord → DataProd + DataProdStorage (via Repository)
Stage 3: Associate InterfaceFileRecord stream → AssociationEngine.run()
```

```python
class DataIngestor:
    def __init__(self, session: Session): ...

    def ingest_scan(self, scanner: FileScanner, *, compute_hashes: bool = False) -> IngestResult: ...

    # Incremental (for Dagster / watch mode)
    def ingest_new(self, scanner: FileScanner, state: IngestState) -> IngestResult: ...
```

`IngestResult` is a plain dataclass: `{n_new: int, n_updated: int, n_skipped: int, errors: list[str]}`.

### Tests
- Full scan + ingest of fixture data
- Idempotency: second ingest does nothing
- New file added: only that file registered
- Content hash verification

---

## Phase 8: Association Engine

**Status:** ⬜ NOT STARTED  
**Depends on:** Phase 5  
**Target files:** `src/tolteca_db/associations/`

### What to Build

```
engine.py    AssociationEngine — orchestrates pool + state + rules
pool.py      ObservationPool — batch DataFrame backend (O(n) preload, O(1) lookup)
state.py     AssociationState — incremental state tracking (SQLite or filesystem backend)
rules.py     Pure functions: group_cal_obs(), group_focus(), group_drivefit()
```

Association **rules** are pure functions — they take a list of `DataProd` objects and return a list of `(source_pk, target_pk, assoc_type)` tuples. No DB access inside rules.

### Tests
- Individual rule functions with known input/output
- Pool batch preload
- Incremental: new obs added, only new associations generated
- State persistence across restarts

---

## Phase 9: CLI

**Status:** ⬜ NOT STARTED  
**Depends on:** Phases 5, 7, 8  
**Target files:** `src/tolteca_db/cli/`

CLI commands defined in `architecture.md`. Thin wrappers over Repository + Ingestor — no business logic in CLI code.

---

## Phase 10: Dagster

**Status:** ⬜ NOT STARTED  
**Depends on:** Phases 5, 7, 8  
**Target files:** `src/tolteca_db/dagster/`

### Key Changes from v2.5
- No test helpers in `dagster/` — all test utilities in `tests/`
- `definitions.py` is the only Dagster entrypoint
- Resources are thin wrappers over `Repository` and `DataIngestor`
- 2D partition strategy: quartet × interface (carried from v2.5)

---

## Phase 11: tolteca_v2 Compatibility Layer

**Status:** ⬜ NOT STARTED  
**Depends on:** Phase 5  
**Target files:** `src/tolteca_db/compat/file.py`

**Goal:** Users with existing tolteca_v2 code can switch to tolteca_db by changing one import. The `toltec_file` pandas accessor behavior is preserved.

---

## Phase 12: Tel Ingestor + LMTMC API

**Status:** ⬜ NOT STARTED  
**Depends on:** Phases 5, 6  
**Target files:** `src/tolteca_db/ingest/tel_ingestor.py`, `src/tolteca_db/ingest/lmtmc_api.py`

Carry over from v2.5 with cleanup. Separate from main ingestor (different file type conventions).

---

## Phase 13: Parquet Query Layer

**Status:** ⬜ NOT STARTED  
**Depends on:** Phase 4  
**Target files:** `src/tolteca_db/db/parquet.py`

Remote Parquet caching via DuckDB + fsspec `simplecache`. Carries over from v2.5 but as a first-class, documented feature rather than an add-on.

---

## Phase 14: Integration Tests + Documentation

**Status:** ⬜ NOT STARTED  
**Depends on:** All phases  

- End-to-end test: scan fixture data → ingest → query → associations
- Dagster pipeline test (test mode, no real LMT data)
- tolteca_v2 compatibility smoke test
- `docs/` update: README, API docs, examples

---

## Current File Inventory (v2.5 source, to be replaced phase by phase)

The v3.x branch currently contains the v2.5 source files as a starting point. Each phase replaces the corresponding v2.5 file(s). Files not yet touched by a phase remain as-is (they still work but are marked for replacement).

```
src/tolteca_db/
├── obsspec.py                    ← NEW in Phase 1 (does not exist in v2.5)
├── constants.py                  ← UPDATE in Phase 1
├── models/
│   ├── metadata.py               ← REPLACE in Phase 3
│   └── orm/                      ← REPLACE in Phase 2
├── db/                           ← Phase 4
├── repository/
│   └── file_api.py               ← REPLACE with repository.py in Phase 5
├── ingest/                       ← Phase 6, 7, 12
├── associations/                 ← Phase 8
├── cli/                          ← Phase 9
├── dagster/                      ← Phase 10
│   ├── test_assets.py            ← MOVE to tests/ in Phase 10
│   └── test_resources.py         ← MOVE to tests/ in Phase 10
├── dagster_per_interface_exp../  ← DELETE entirely in Phase 10
├── dash/                         ← DELETE in Phase 0 cleanup (belongs in tolteca_web)
├── api/                          ← DELETE in Phase 5 (replaced by repository)
└── query/                        ← DELETE in Phase 5 (merged into repository)
```
