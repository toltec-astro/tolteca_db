# v2.5 Design Document Archive

**Source:** `refs/tolteca_db/design/` (snapshot from main branch, commit `1bafc53`)  
**Archived:** 2026-03-10  
**Purpose:** Reference record of all v2.5 design work. Not for active development — see `../architecture.md`, `../implementation.md`, `../tasks.md` for current v3.x state.

---

## Core Architecture & Design

| Document | Description | Status in v2.5 |
|----------|-------------|----------------|
| `architecture.md` | Full system blueprint — ORM models, layered architecture, DuckDB migration, Dagster pipeline, 2D partition strategy | Active, ~Phase 20 |
| `implementation.md` | 20-phase implementation plan, current status, completed milestones | Complete through Phase 20 |
| `tasks.md` | Task tracking (1,019 lines), spanning TASK-001 through Phase 20 | Completed |
| `ARCHITECTURE_AND_API_DESIGN.md` | Comprehensive API and architecture design reference | Superseded by architecture.md |
| `TOLTECA_DB_COMPLETE_DESIGN.md` | Full design compendium | Superseded |
| `Technology_Stack_Blueprint.md` | Stack choices: DuckDB, SQLAlchemy, adaptix, typer, Dagster | Reference |

---

## Domain & Schema Design

| Document | Description | Key Decisions |
|----------|-------------|---------------|
| `gap_analysis.md` | Alignment of tolteca_db terminology with tolteca_v1/v2 ecosystem | Finalized naming: `dp_raw_obs`, `dp_reduced_obs`, `dpa_*` |
| `database_schema_diagram.md` | ER diagram and table relationships | 14 ORM models |
| `schema_erd.md` | Entity-relationship diagram | |
| `master_and_uid_analysis.md` | UID convention analysis — `{master}-{obsnum}-{subobsnum}-{scannum}` | Established UID patterns |
| `location_and_source_uri.md` | DataProdStorage file URI and location tracking design | Multi-location per file |

---

## Data Flow & Ingestion

| Document | Description | Key Decisions |
|----------|-------------|---------------|
| `toltec_data_file_and_data_flow.md` | **Constitutional document** — data topology, UID semantics, schema pattern for interface files, hybrid file/DB API | Copied to `../toltec_data_file_and_data_flow.md` |
| `toltec_db_ingestion_workflow.md` | LMT file ingestion pipeline design | DataIngestor + FileScanner pattern |
| `LMTMC_API_INTEGRATION.md` | Integration with LMT machine control API for live obsnum queries | lmtmc_api client |
| `lmt_tel_metadata_integration.md` | Telescope telemetry/metadata ingestion (tel CSV files) | tel_ingestor pattern |

---

## Association Engine

| Document | Description | Key Decisions |
|----------|-------------|---------------|
| `ASSOCIATION_DIAGRAMS.md` | Visual diagrams of association types (dpa_*) and grouping logic | 9 association types |
| `obsspec_and_repository.md` | ObsSpec DSL query interface and Repository abstraction matching tolteca_v2 | ObsSpec regex, DataFrameAdapter |
| `parquet_query_architecture.md` | Parquet query layer with DuckDB for remote/cached analytics | fsspec simplecache |

---

## CLI & User Interface

| Document | Description |
|----------|-------------|
| `CLI_COMMAND_REFERENCE.md` | All 15 CLI commands across 4 categories |
| `CLI_User_Guide.md` | User-facing guide to `tolteca_db` CLI |
| `exemplars.md` | Concrete usage examples for each major operation |
| `v2_vs_db_api_comparison.md` | Side-by-side API comparison: tolteca_v2 filesystem vs tolteca_db |

---

## Cleanup & Migration Notes

| Document | Description |
|----------|-------------|
| `CLEANUP_OLD_PATTERNS.md` | Patterns identified for removal in cleanup passes |
| `UNIFIED_CSV_STRUCTURE_IMPLEMENTATION.md` | CSV tel file unified structure implementation |
| `UNIFIED_PATTERN_VERIFICATION.md` | Verification results for unified patterns |

---

## Internal Archive (in `archive/` subfolder of refs)

Already archived at the v2.5 stage; left as-is for reference:

| Document | Description |
|----------|-------------|
| `API_MIGRATION.md` | Pre-v2.5 API migration notes |
| `ASSOCIATION_TEST_RESULTS.md` | Association test results snapshot |
| `DAGSTER_MIGRATION_SUMMARY.md` | Dagster migration summary |
| `DAGSTER_PIPELINE_TEST_RESULTS.md` | Dagster pipeline test results |
| `DAGSTER_TEST_MODE_VERIFICATION.md` | Test mode verification results |
| `DATABASE_FIX_2025-12-14.md` | Database fix notes |
| `DATABASE.md` | Database usage guide |
| `INGESTION_STATUS_2025-12-14.md` | Ingestion status snapshot |
| `QUICK_REFERENCE.md` | Quick reference card |
| `README.md` | Archive README |

---

## Key v2.5 Lessons (for v3.x designers)

### What Worked Well

- **Content-addressable IDs** (blake3): Stable, deterministic, deduplication-friendly
- **adaptix** for metadata serialization: Type-safe, union-type aware, fast
- **SQLAlchemy 2.0** mapped_column: 60-70% boilerplate reduction over v1 style
- **DuckDB** for Parquet analytics: 10-100x faster than SQLite/PostgreSQL for bulk reads
- **Hybrid DB pattern** (`create_database()` factory): Solves DuckDB concurrent write limits while keeping analytics performance
- **Registry table pattern** (DataProdType, DataKind, DataProdAssocType): Clean enumeration management
- **Incremental association generation** with Pool + State pattern: 100-1000× speedup

### What Was Messy / Should Be Cleaned Up in v3.x

- **`dagster_per_interface_experiment/`** module: Experimental, never graduated — remove or graduate cleanly
- **`viewer_old.py`** in dash/: Dead code — remove
- **`db/repository.py`** + `db/database.py` separation: Two concepts (session management vs DB type) were conflated in places — clarify boundary
- **`api/obs.py`** ObsQuery: Was patched multiple times — needs clean rewrite
- **Metadata models** (adaptix models in `models/metadata.py`): Mixed responsibilities, some fields duplicated with ORM meta JSON — rationalize
- **Ingestion pipeline**: `ingest/ingest.py` DataIngestor grew organically; v3 should start fresh with clear `Scan → Register → Associate` stages
- **test_assets.py / test_resources.py** in dagster/: These belong in tests/, not in the production module tree
- **`db/parquet.py`**: Remote Parquet caching was added as an afterthought; in v3 it should be a first-class concern integrated with the file API
- **Two parallel association systems**: `associations/` (library) and `dagster/` (orchestration) were not cleanly separated — v3 should have a clear interface between them
- **ObsSpec regex parsing** duplicated in multiple places: Should be centralized in a single `obsspec.py` module
