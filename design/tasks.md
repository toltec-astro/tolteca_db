# tolteca_db v3.x Task Tracking

**Status:** Active  
**Last Updated:** 2025-07-15  
**Branch:** `v3.x`

---

## Phase 0: Architecture & Design ✅ COMPLETE

- [x] Read constitutional reference doc (toltec_data_file_and_data_flow.md)
- [x] Read and archive all v2.5 design docs → `design/archive/index.md`
- [x] Write v3.x `architecture.md`
- [x] Write v3.x `implementation.md`
- [x] Write v3.x `tasks.md` (this file)
- [x] `git commit design/` to v3.x branch

---

## Phase 1: obsspec.py + constants.py ✅ COMPLETE

**Goal:** Single authoritative ObsSpec parser; no more scattered parsing across modules.

- [x] `src/tolteca_db/constants.py`
  - [x] `MasterType(StrEnum)` — tcs, ics, clip
  - [x] `type MasterNameT = Literal[...]`
  - [x] `ToltecInfo` — ClassVar lookup tables: masters, roach_interface, interface_master, interface_array_name, obsnum_min/max
  - [x] `DataKind(Flag)` — replaces ToltecDataKind; backward-compat alias kept
  - [x] All enums migrated to `StrEnum` with `auto()` (lowercase values)

- [x] `src/tolteca_db/obsspec.py`
  - [x] `ObsSpec` frozen dataclass: `master`, `obsnum`, `subobsnum`, `scannum`
  - [x] `ObsSpec.uid` property → `{master}-{obsnum}-{subobsnum}-{scannum}`
  - [x] `ObsSpec.parse(s: str) -> ObsSpec` — parse canonical UID string
  - [x] `ObsSpec.from_path(path, master=None) -> ObsSpec | None` — extract from filename
  - [x] `__post_init__` validates master + obsnum range
  - [x] `ObsSpecError(ValueError)` exception class
  - [x] `__str__` returns uid; `__repr__` shows all fields

- [x] `tests/test_obsspec.py` — **66 tests, all passing**
  - [x] `TestMasterType`, `TestToltecInfo`, `TestDataProdType`, `TestDataKind`, `TestStrEnums`
  - [x] `TestObsSpecCreate`, `TestObsSpecUid`, `TestObsSpecParse`
  - [x] `TestObsSpecFromPath` (all prefix patterns, master override, basename-only, zero-padding)
  - [x] `TestObsSpecIntegration` (round-trips, set dedup, sort)

**Notes:**
- Used `[m.value for m in MasterType]` instead of `get_args(MasterNameT)` because PEP 695 `type X = Literal[...]` creates `TypeAliasType`; `get_args()` returns `()` for it.
- Commit: `616bcbc`

---

## Phase 2: ORM Models ✅ COMPLETE

**Goal:** Clean SQLAlchemy 2.x ORM layer; all tables defined once, no duplicates.

**10 tables, 34 tests — all passing. Commit: Phase 2 complete.**

### Schema summary

| Table | PK | Key columns |
|---|---|---|
| `raw_obs` | `uid` (str, e.g. `"tcs-98765-0-0"`) | `master`, `obsnum`, `subobsnum`, `scannum`, `timestamp_obs`, `meta` |
| `data_prod` | `pk` (int, seq) | `uid` (unique str), `raw_obs_uid` FK, `data_prod_type`, `interface`, catalog columns¹, `availability`, `meta` |
| `storage_root` | `pk` (int, seq) | `label` (unique), `host`, `root_path`, `is_local` |
| `file_record` | `pk` (int, seq) | `data_prod_fk`, `storage_root_fk`, `rel_path`, `mtime`, `file_size`, `checksum` |
| `assoc` | `pk` (int, seq) | `uid` (unique), `rule_name`, `context` (JSON), `status`, `score` |
| `assoc_edge` | `pk` (int, seq) | `assoc_fk`, `data_prod_fk`, `role` |
| `obs_flag` | `pk` (int, seq) | `raw_obs_uid` FK (str), `flag_reason`, `flag_source`, `flagged_at` |
| `data_prod_flag` | `pk` (int, seq) | `data_prod_fk`, `flag_reason`, `flag_source`, `flagged_at` |
| `task` | `pk` (int, seq) | `uid` (unique), `assoc_fk` nullable, `task_type`, `status`, `started_at`, `completed_at` |
| `event` | `pk` (int, seq) | `event_type`, `entity_type`, `entity_id`, `payload` (JSON), `occurred_at` |

¹ Catalog columns on `data_prod`: `nw`, `array_name`, `data_kind`, `n_chans`, `n_samples`, `lo_center_freq_hz`, `drive_atten_db`, `sense_atten_db`, `nc_path`, `zarr_path` — all nullable, for direct tolteca_web catalog join.

### Key design decisions (2025-07-15)
- `RawObsRecord.uid` is string PK (ObsSpec.uid, e.g. `"tcs-98765-0-0"`) — human-readable, no surrogate int
- `DataProdRecord` stores all tolteca_web catalog columns directly (no JSON parsing at query time)
- `Pk = Annotated[int, mapped_column(Integer, Sequence("tolteca_db_pk_seq"), primary_key=True)]` — DuckDB-compatible; SQLite ignores Sequence
- All int PK tables share one sequence name `"tolteca_db_pk_seq"` (uniqueness per table is guaranteed by auto-increment; shared name is fine)
- Test backend: SQLite `"sqlite:///:memory:"` — DuckDB compat deferred to Phase 4

### Files changed
- [x] `src/tolteca_db/utils/mapped_types.py` — renamed Sequence `"generic_pk_seq"` → `"tolteca_db_pk_seq"`
- [x] `src/tolteca_db/utils/__init__.py` — stripped v2.5 uid/hashing re-exports
- [x] `src/tolteca_db/models/orm/base.py` — simplified: just `DeclarativeBase`
- [x] `src/tolteca_db/models/orm/registry.py` — NEW: `RawObsRecord` + `DataProdRecord`
- [x] `src/tolteca_db/models/orm/data_prod.py` — REWRITTEN: `StorageRootRecord` + `FileRecord`
- [x] `src/tolteca_db/models/orm/assoc.py` — REWRITTEN: `AssocRecord` + `AssocEdgeRecord`
- [x] `src/tolteca_db/models/orm/flag.py` — REWRITTEN: `ObsFlagRecord` + `DataProdFlagRecord`
- [x] `src/tolteca_db/models/orm/task.py` — REWRITTEN: `TaskRecord`
- [x] `src/tolteca_db/models/orm/event.py` — REWRITTEN: `EventRecord`
- [x] `src/tolteca_db/models/orm/__init__.py` — re-exports all 11 v3.x classes
- [x] `src/tolteca_db/models/__init__.py` — updated to v3.x class names
- [x] `tests/test_models_orm.py` — NEW: 34 tests in 8 test classes
- [x] `tests/conftest.py` — updated: SQLite engine, removed v2.5 fixtures

---

## Phase 3: Metadata Models (adaptix) ⬜ NOT STARTED

**Goal:** Pydantic-style domain objects with adaptix (de)serialization; separate from ORM.

- [ ] `src/tolteca_db/models/metadata.py`
  - [ ] `RawObs` dataclass — mirrors `RawObsRecord` but domain-layer
  - [ ] `DataProd` dataclass — `obs: RawObs`, `interface`, `storage`, `availability`
  - [ ] `StorageRoot` dataclass
  - [ ] `AssocGroup` dataclass — `rule_name`, `inputs: list[DataProd]`, `outputs: list[DataProd]`
  - [ ] `ObsFlag`, `DataProdFlag` dataclasses
  - [ ] `adaptix.Converter` setup for ORM → metadata and metadata → ORM conversions

- [ ] `tests/test_models_metadata.py`
  - [ ] Round-trip: ORM record → metadata dataclass → ORM record
  - [ ] JSON serialization via adaptix

---

## Phase 4: Database Layer ⬜ NOT STARTED

**Goal:** Backend-agnostic `create_database()` factory; connection/session management.

- [ ] `src/tolteca_db/db/config.py`
  - [ ] `DatabaseConfig` (pydantic BaseSettings): `url`, `echo`, `pool_size`
  - [ ] Auto-detect backend from URL prefix: `duckdb://`, `sqlite://`, `postgresql://`

- [ ] `src/tolteca_db/db/database.py`
  - [ ] `Database` class: holds engine + session factory
  - [ ] `Database.create_all()` — init all tables
  - [ ] `Database.session()` — context manager returning `Session`
  - [ ] `create_database(url: str, **kwargs) -> Database` factory function

- [ ] `src/tolteca_db/db/parquet.py`
  - [ ] `ParquetQueryEngine` — DuckDB-backed ad-hoc queries against parquet files
  - [ ] `ParquetQueryEngine.query(sql: str, paths: list[Path]) -> pd.DataFrame`

- [ ] `tests/test_db.py`
  - [ ] `create_database("sqlite:///:memory:")` → creates tables
  - [ ] `create_database("duckdb:///:memory:")` → creates tables
  - [ ] Session round-trip insert/query

---

## Phase 5: Repository ⬜ NOT STARTED

**Goal:** Single consolidated `Repository` class; replaces split `api/` + `db/repository.py` from v2.5.

- [ ] `src/tolteca_db/repository/repository.py`
  - [ ] `Repository.__init__(db: Database)`
  - [ ] `Repository.get_raw_obs(uid: str) -> RawObs | None`
  - [ ] `Repository.list_raw_obs(master: str | None, ...)` → paginated list
  - [ ] `Repository.upsert_raw_obs(obs: RawObs) -> RawObs`
  - [ ] `Repository.get_data_prod(uid: str) -> DataProd | None`
  - [ ] `Repository.list_data_prods(obs_uid: str | None, interface: str | None, ...) -> list[DataProd]`
  - [ ] `Repository.upsert_data_prod(dp: DataProd) -> DataProd`
  - [ ] `Repository.get_assoc_group(uid: str) -> AssocGroup | None`
  - [ ] `Repository.list_assoc_groups(rule_name: str | None, ...) -> list[AssocGroup]`
  - [ ] `Repository.upsert_assoc_group(ag: AssocGroup) -> AssocGroup`
  - [ ] `Repository.get_availability(dp: DataProd) -> AvailabilityState` — computed on read

- [ ] `tests/test_repository.py`
  - [ ] CRUD round-trips for all entity types
  - [ ] Availability state computation tests

---

## Phase 6: FileScanner ⬜ NOT STARTED

**Goal:** Walk storage roots, emit file discovery events; decoupled from ingestor.

- [ ] `src/tolteca_db/ingest/scanner.py`
  - [ ] `ScanConfig` dataclass: `root: Path`, `patterns: list[str]`, `recursive: bool`
  - [ ] `FileScanner` class
  - [ ] `FileScanner.scan(config: ScanConfig) -> Iterator[DiscoveredFile]`
  - [ ] `DiscoveredFile` dataclass: `path`, `mtime`, `size`, `checksum`
  - [ ] Checksum: blake3 (use `blake3` library)
  - [ ] Skip `.git/`, `__pycache__/`, hidden dirs by default

- [ ] `tests/test_scanner.py`
  - [ ] Scan a temp dir with fixture files
  - [ ] Verify checksums match expected

---

## Phase 7: DataIngestor ⬜ NOT STARTED

**Goal:** Consume `DiscoveredFile` stream, parse ObsSpec, write to Repository.

- [ ] `src/tolteca_db/ingest/ingestor.py`
  - [ ] `IngestConfig` dataclass: `dry_run: bool`, `update_existing: bool`
  - [ ] `DataIngestor.__init__(repo: Repository, config: IngestConfig)`
  - [ ] `DataIngestor.ingest_file(file: DiscoveredFile) -> IngestResult`
  - [ ] `DataIngestor.ingest_directory(root: Path, ...) -> IngestSummary`
  - [ ] `IngestResult` dataclass: `status`, `raw_obs_uid`, `data_prod_uid`, `error`
  - [ ] `IngestSummary` dataclass: `total`, `added`, `updated`, `skipped`, `errors`

- [ ] `tests/test_ingestor.py`
  - [ ] Ingest fixture files into in-memory DB
  - [ ] Re-ingest → verify idempotent (update_existing=False skips)
  - [ ] Dry run → no DB changes

---

## Phase 8: Association Engine ⬜ NOT STARTED

**Goal:** Pure-function association rules evaluated via engine; replaces v2.5 scattered association logic.

- [ ] `src/tolteca_db/associations/rules.py`
  - [ ] `AssocRule` protocol: `rule_name`, `apply(inputs: list[DataProd]) -> list[AssocGroup]`
  - [ ] `CalibrationRule` — maps cal observations to science observations
  - [ ] `BeammapRule` — maps beammap observations to array configs
  - [ ] `register_rule(rule: AssocRule)` decorator

- [ ] `src/tolteca_db/associations/pool.py`
  - [ ] `AssocPool` — holds staged data prods not yet associated

- [ ] `src/tolteca_db/associations/state.py`
  - [ ] `AssocState` — current association table state snapshot

- [ ] `src/tolteca_db/associations/engine.py`
  - [ ] `AssocEngine.__init__(repo: Repository, rules: list[AssocRule])`
  - [ ] `AssocEngine.run_incremental(since: datetime | None) -> AssocSummary`
  - [ ] `AssocEngine.run_full() -> AssocSummary`

- [ ] `tests/test_associations.py`
  - [ ] Unit-test individual rule functions with fixture DataProd lists
  - [ ] Integration: ingest fixtures → run engine → verify expected assoc groups

---

## Phase 9: CLI ⬜ NOT STARTED

**Goal:** `tolteca-db` CLI via Typer; subcommands for each major function.

- [ ] `src/tolteca_db/cli/__init__.py` — app entry point
- [ ] `src/tolteca_db/cli/db.py` — `db init`, `db migrate`, `db status`
- [ ] `src/tolteca_db/cli/ingest.py` — `ingest scan`, `ingest run`, `ingest dry-run`
- [ ] `src/tolteca_db/cli/query.py` — `query obs`, `query data-prods`, `query assocs`
- [ ] `src/tolteca_db/cli/assoc.py` — `assoc run`, `assoc list`, `assoc show`
- [ ] Wire `tolteca-db` entry point in `pyproject.toml`

- [ ] `tests/test_cli.py`
  - [ ] `typer.testing.CliRunner` tests for each subcommand
  - [ ] `db init` → creates DB
  - [ ] `ingest run --dry-run` → no DB changes

---

## Phase 10: Dagster Integration ⬜ NOT STARTED

**Goal:** Dagster assets/sensors for automated ingest + association pipelines.

- [ ] `src/tolteca_db/dagster/resources.py`
  - [ ] `ToltecaDbResource` — wraps `Database` + `Repository` for Dagster IOManager pattern

- [ ] `src/tolteca_db/dagster/assets.py`
  - [ ] `raw_obs_asset` — materializes new raw_obs records from file scan
  - [ ] `data_prod_asset` — materializes data_prod records from file scan
  - [ ] `assoc_asset` — materializes assoc groups from AssocEngine

- [ ] `src/tolteca_db/dagster/sensors.py`
  - [ ] `file_system_sensor` — watches storage root, triggers ingest on new files

- [ ] `src/tolteca_db/dagster/partitions.py`
  - [ ] ObsNum partitions for parallel ingest if needed

- [ ] `src/tolteca_db/dagster/definitions.py`
  - [ ] `defs = Definitions(assets=[...], sensors=[...], resources={...})`

- [ ] `tests/test_dagster.py`
  - [ ] `dagster.build_asset_context()` unit tests for each asset
  - [ ] Sensor test with mock file events

---

## Phase 11: compat/file.py (tolteca_v2 compatibility) ⬜ NOT STARTED

**Goal:** Preserve backward-compatible DataFrame API for tolteca_v2 consumers.

- [ ] `src/tolteca_db/compat/file.py`
  - [ ] `load_data_prod_as_df(dp: DataProd) -> pd.DataFrame` — loads from file path
  - [ ] `DataProdFrame` — subclass of `pd.DataFrame` with metadata preserved
  - [ ] Column name mapping: v2 column names → v3 column names
  - [ ] Deprecation warnings via `warnings.warn(..., DeprecationWarning)`

- [ ] `tests/test_compat.py`
  - [ ] Load fixture file → `DataProdFrame` → verify expected columns present

---

## Phase 12: tel_ingestor + lmtmc_api ⬜ NOT STARTED

**Goal:** Telescope metadata ingest and LMT MC interface ported from v2.5.

- [ ] `src/tolteca_db/ingest/tel_ingestor.py`
  - [ ] `TelIngestor` — reads telescope pointing/status files
  - [ ] `TelIngestor.ingest(path: Path) -> TelMetadata`

- [ ] `src/tolteca_db/ingest/lmtmc_api.py`
  - [ ] `LmtMcApi` — reads LMT MC status/config from standard paths
  - [ ] `LmtMcApi.get_roach_configs() -> list[RoachConfig]`

- [ ] `tests/test_tel_ingestor.py`
- [ ] `tests/test_lmtmc_api.py`

---

## Phase 13: Parquet Query Engine ⬜ NOT STARTED

**Goal:** Ad-hoc SQL queries against parquet data product files via DuckDB.

- [ ] Finish `db/parquet.py` (skeleton created in Phase 4)
  - [ ] `ParquetQueryEngine.query_data_prod(dp: DataProd, sql: str) -> pd.DataFrame`
  - [ ] `ParquetQueryEngine.join_data_prods(dps: list[DataProd], sql: str) -> pd.DataFrame`
  - [ ] Support wildcard paths: `ParquetQueryEngine.query_glob(pattern: str, sql: str)`

- [ ] `tests/test_parquet.py`
  - [ ] Write fixture parquet file → query with DuckDB → verify results

---

## Phase 14: Integration Tests + Docs ⬜ NOT STARTED

**Goal:** End-to-end integration tests; Sphinx docs for all public APIs.

- [ ] `tests/integration/`
  - [ ] `test_full_pipeline.py` — scan → ingest → associate → query end-to-end
  - [ ] `test_dagster_pipeline.py` — full Dagster materializations with fixture data
  - [ ] `test_cli_workflow.py` — CLI commands drive full workflow

- [ ] `docs/`
  - [ ] Update `docs/index.rst` with v3.x module structure
  - [ ] Sphinx autodoc for all public modules
  - [ ] Tutorials: basic ingest, running associations, Dagster setup

- [ ] Final cleanup
  - [ ] Delete `src/tolteca_db/dash/` (moved to tolteca_web)
  - [ ] Delete `src/tolteca_db/dagster_per_interface_experiment/`
  - [ ] Move `src/tolteca_db/dagster/test_*.py` → `tests/`
  - [ ] Delete `src/tolteca_db/api/` (replaced by repository/)
  - [ ] Delete `src/tolteca_db/viewer_old.py` (if present)
  - [ ] Run `ruff check --fix` + `ruff format`
  - [ ] Run `mypy` and resolve all type errors
  - [ ] Ensure `pytest` passes all tests

---

## Cleanup: Files to DELETE from v2.5 Source

These paths exist in the ported v2.5 source and must be removed in v3.x:

| Path | Reason |
|---|---|
| `src/tolteca_db/dash/` | Moved to tolteca_web |
| `src/tolteca_db/dagster_per_interface_experiment/` | Experimental dead code |
| `src/tolteca_db/dagster/test_*.py` | Tests should live in `tests/` |
| `src/tolteca_db/api/` | Replaced by `repository/repository.py` |
| `src/tolteca_db/viewer_old.py` | Replaced by tolteca_web |
| `src/tolteca_db/db/repository.py` | Merged into `repository/repository.py` |

> These deletions happen in Phase 14 after all equivalent v3.x code is in place and tests pass.

---

## Blocked / Decisions Needed

| Item | Blocked On | Notes |
|---|---|---|
| PostgreSQL backend | No test server available | Use DuckDB + SQLite for now; PostgreSQL path stubbed |
| LMT MC API auth | Need real LMT MC credentials/schema | Use mock in Phase 12 |
| Dagster Cloud vs local | Deployment decision | Phase 10 targets local runner only |

---

*Living document — update as tasks are completed.*
