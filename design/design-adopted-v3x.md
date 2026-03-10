# Adopted Key Design (v3.x)

**Status:** Active  
**Last Updated:** 2026-03-10  
**Constitutional Reference:** [toltec_data_file_and_data_flow.md](toltec_data_file_and_data_flow.md)  
**Comparison Spike:** [spike-orm-schema-v25-vs-v3.md](spike-orm-schema-v25-vs-v3.md)

This document records the adopted architectural decisions for the v3.x codebase, derived directly from the constitutional reference. It is AI-maintained and must be updated whenever the design evolves. Whenever there is a conflict between this document and the constitution, **the constitution wins**.

---

## 1. Core Design Decisions (from Constitution Q&A)

### 1.1 raw_obs is NOT a special table

A raw observation is a `data_prod` row with `data_prod_type.label = "dp_raw_obs"` and a typed `RawObsMeta` JSON. It is not special — it has one metadata type among many. This follows directly from the constitution:

> *"raw obs is one kind of data product in the data_prod table. We should not make it special."*

**What this means for the ORM:**
- No `raw_obs` table.
- Finding all files for obsnum 98765: `SELECT * FROM data_prod WHERE meta->>'obsnum' = 98765 AND data_prod_type.label = 'dp_raw_obs'`
- If benchmarks show JSON extraction is slow for common queries, **flat columns can be added** without changing the design principle. The key invariant is that `AdaptixJSON` provides the typed Python↔DB bridge regardless of whether the storage is JSON or columns.

### 1.2 Typed metadata via AdaptixJSON — always

Every `meta` column stores a **typed Python dataclass** via `AdaptixJSON`. There are no free-form `dict[str, Any]` metadata fields. Each product type has its own dataclass.

### 1.3 Interface-level metadata lives on DataProdSource

Per-file metadata (roach number, nw_id, hostname, telescope pointing, tau, m1_zernike, etc.) belongs to `data_prod_source.meta`, not on `data_prod`. A `data_prod` represents the logical product; a `data_prod_source` represents a specific physical file/URI.

### 1.4 Registry tables for all type/kind/flag constants

`data_prod_type`, `data_kind`, `data_prod_assoc_type`, and `flag` are **normalized registry tables** — first-class rows with label, description, and category. Each has a Python `StrEnum` counterpart. No inline type strings on `data_prod`.

### 1.5 Associations use directed edges

`DataProdAssoc` is a directed edge: `src_data_prod_fk → dst_data_prod_fk` via a typed `data_prod_assoc_type`. No group+edge indirection. A grouped product (e.g., a focus group) is itself a `data_prod` row with `data_prod_type = "dp_focus_group"`, and its constituent raw obs are linked via directed edges.

### 1.6 Flag registry with namespace

The `flag` table has `namespace` (e.g., `qa`, `detector`, `telescope`) and `label` (e.g., `SATURATED`) with a unique constraint on `(namespace, label)`. Flags are first-class rows, not free-form strings. Assignments go through `data_prod_flag` junction.

### 1.7 ObsSpec is a Python-layer DSL

`ObsSpec` is a user-friendly Python construct for input, display, and file parsing. It is **not** a core ORM identity. The database stores `obsnum`, `subobsnum`, `scannum`, `master` as typed meta fields inside `RawObsMeta`. A derived `obsspec_uid` string column *may* be added for indexing convenience, but it is derived, not primary.

### 1.8 Task table — keep simple for now

Task tracking context is stored in the `meta` JSON of the data_prod group that the task produces (e.g., a `dp_reduced_obs` row whose `ReducedObsMeta.task_context` records the processing parameters). A dedicated `reduction_task` table with explicit input/output junctions may be added later if query requirements demand it.

---

## 2. ORM Schema

### 2.1 Table Inventory (11 tables)

```
Registry tables (static, seeded at DB init):
  location            — Data root registry (site, server, local, S3, ...)
  data_prod_type      — Product type registry (dp_raw_obs, dp_cal_group, ...)
  data_kind           — Data kind registry (VnaSweep, RawTimeStream, ...)
  data_prod_assoc_type — Association type registry (dpa_cal_group_raw_obs, ...)
  flag                — Flag type registry (namespace + label)

Core tables:
  data_prod           — Unified data product (all types, typed JSON meta)
  data_prod_data_kind — Junction: product ↔ kind  (many-to-many)
  data_prod_source    — Physical file/URI per product per location
  data_prod_assoc     — Directed provenance edge between products
  data_prod_flag      — Junction: product ↔ flag

Audit:
  event_log           — Append-only audit log
```

### 2.2 Registry Tables

#### `location` — Data Root Registry

| Column | Type | Notes |
|--------|------|-------|
| `pk` | `int` PK | Auto-increment |
| `label` | `str(128)` unique | e.g., `"site_data_lmt"`, `"server_archive_2024"`, `"local"` |
| `location_type` | `str(32)` | `filesystem`, `s3`, `http`, `api` |
| `root_uri` | `str(512)` | Root URI: `file:///data_lmt/`, `s3://bucket/prefix/`, etc. |
| `priority` | `int` | Lower = preferred. Default 100 |
| `meta` | `JSON` | Free-form additional config |
| `created_at` | `datetime(tz)` | |
| `updated_at` | `datetime(tz)` | |

Relationships: `sources → list[DataProdSource]`

#### `data_prod_type` — Product Type Registry

| Column | Type | Notes |
|--------|------|-------|
| `pk` | `int` PK | Auto-increment |
| `label` | `str(128)` unique | `dp_raw_obs`, `dp_reduced_obs`, `dp_cal_group`, etc. |
| `level` | `int` | Processing level: 0=raw, 1=reduced, 2+=analysis |
| `description` | `Text` | Human-readable description |

Python counterpart: `DataProdType(StrEnum)`.

#### `data_kind` — Data Kind Registry

| Column | Type | Notes |
|--------|------|-------|
| `pk` | `int` PK | Auto-increment |
| `label` | `str(128)` unique | `VnaSweep`, `TargetSweep`, `Tune`, `RawTimeStream`, `D21`, etc. |
| `category` | `str(32)` | `sweep`, `timestream`, `reduced`, `telescope`, `ancillary` |
| `description` | `Text` | Human-readable description |

Python counterpart: `DataKind(Flag)` — supports bitwise combinations.

#### `data_prod_assoc_type` — Association Type Registry

| Column | Type | Notes |
|--------|------|-------|
| `pk` | `int` PK | Auto-increment |
| `label` | `str(128)` unique | `dpa_cal_group_raw_obs`, `dpa_reduced_obs_raw_obs`, etc. |
| `description` | `Text` | Human-readable description |

Python counterpart: `DataProdAssocType(StrEnum)`.

#### `flag` — Flag Type Registry

| Column | Type | Notes |
|--------|------|-------|
| `pk` | `int` PK | Auto-increment |
| `label` | `str(128)` | Flag name, e.g., `SATURATED`, `DEAD_PIXEL`, `BAD_POINTING` |
| `namespace` | `str(32)` | `qa`, `detector`, `telescope`, `calibration`, `ingest` |
| `description` | `Text` | Human-readable description |
| UNIQUE | `(namespace, label)` | |

Python counterpart: namespace-grouped StrEnum classes (e.g., `QAFlag`, `DetectorFlag`).

### 2.3 Core Tables

#### `data_prod` — Unified Data Product

| Column | Type | Notes |
|--------|------|-------|
| `pk` | `int` PK | Auto-increment |
| `data_prod_type_fk` | `int` FK→`data_prod_type.pk` | Product type |
| `lifecycle_status` | `str(16)` | `ACTIVE`, `SUPERSEDED`. Default `ACTIVE` |
| `availability_state` | `str(16)` | `PLANNED`, `PARTIAL`, `AVAILABLE`, `MISSING`, `REMOTE`, `STAGED` |
| `content_hash` | `str(128)` | blake3 hash for derived products; NULL for raw obs |
| `meta` | `AdaptixJSON[AnyDataProdMeta]` | Typed per product type — see §3 |
| `created_at` | `datetime(tz)` | |
| `updated_at` | `datetime(tz)` | |

Relationships: `data_prod_type`, `sources → list[DataProdSource]`, `kinds → list[DataProdDataKind]`, `flags → list[DataProdFlag]`, `src_assocs → list[DataProdAssoc]`, `dst_assocs → list[DataProdAssoc]`

**Raw obs as data_prod:** A raw obs is a `data_prod` row where `data_prod_type.label = "dp_raw_obs"` and `meta = RawObsMeta(master="tcs", obsnum=98765, ...)`. No special table.

#### `data_prod_data_kind` — Kind Assignment Junction

| Column | Type | Notes |
|--------|------|-------|
| `data_prod_fk` | `int` FK→`data_prod.pk` | (composite PK) |
| `data_kind_fk` | `int` FK→`data_kind.pk` | (composite PK) |
| `applied_at` | `datetime(tz)` | |
| `source` | `str(16)` | `automatic`, `manual`, `inferred` |
| `confidence` | `float` | For automatic assignments |

A product can carry **multiple** data kinds (e.g., a Tune is both `VnaSweep` and `TargetSweep`). This is the correct model for the `DataKind` flag enum.

#### `data_prod_source` — Physical File / URI

| Column | Type | Notes |
|--------|------|-------|
| `source_uri` | `str(512)` PK | Full URI: `file:///data_lmt/toltec/tcs/toltec0/toltec0_098765...nc` |
| `data_prod_fk` | `int` FK→`data_prod.pk` | Parent product |
| `location_fk` | `int` FK→`location.pk` | Which data root |
| `role` | `str(16)` | `PRIMARY`, `MIRROR`, `TEMP` |
| `availability_state` | `str(16)` | Per-source availability |
| `size` | `int` | Bytes |
| `checksum` | `str(128)` | `blake3:...` or `sha256:...` |
| `last_verified_at` | `datetime(tz)` | Last successful access check |
| `meta` | `AdaptixJSON[AnyInterfaceMeta]` | Interface-level typed metadata — see §3 |
| `created_at` | `datetime(tz)` | |
| `updated_at` | `datetime(tz)` | |

The `source_uri` is the self-describing primary key — it encodes location without requiring a join.

#### `data_prod_assoc` — Directed Provenance Edge

| Column | Type | Notes |
|--------|------|-------|
| `pk` | `int` PK | Auto-increment |
| `data_prod_assoc_type_fk` | `int` FK→`data_prod_assoc_type.pk` | Edge type |
| `src_data_prod_fk` | `int` FK→`data_prod.pk` | Source (the dependent product) |
| `dst_data_prod_fk` | `int` FK→`data_prod.pk` | Destination (the input/referenced product) |
| `context` | `AdaptixJSON[ProcessContext]` | Processing context (module, version, config) |
| `created_at` | `datetime(tz)` | |

Indexes: `(data_prod_assoc_type_fk, src_data_prod_fk)`, `(data_prod_assoc_type_fk, dst_data_prod_fk)`

**Semantics:** `src → dst` means "src depends on / contains / was derived from dst". Example: a `dp_cal_group` (src) contains a `dp_raw_obs` (dst) via `dpa_cal_group_raw_obs`.

#### `data_prod_flag` — Flag Assignment Junction

| Column | Type | Notes |
|--------|------|-------|
| `data_prod_fk` | `int` FK→`data_prod.pk` | (composite PK) |
| `flag_fk` | `int` FK→`flag.pk` | (composite PK) |
| `asserted_at` | `datetime(tz)` | |
| `asserted_by` | `str(64)` | `"system"`, `"user:jsmith"`, `"pipeline:drivefit"` |
| `context` | `JSON` | Additional context dict |

#### `event_log` — Append-Only Audit Log

| Column | Type | Notes |
|--------|------|-------|
| `seq` | `int` PK | Auto-increment sequence |
| `event_type` | `str(64)` | e.g., `FlagAdded`, `FileMissing`, `ProductCreated` |
| `entity_type` | `str(32)` | `data_prod`, `data_prod_source`, `location`, etc. |
| `entity_id` | `str(128)` | PK of the entity (pk int or source_uri string) |
| `payload` | `JSON` | Event details |
| `occurred_at` | `datetime(tz)` | Server default |

Index: `(entity_type, entity_id, seq)`

### 2.4 Entity-Relationship Summary

```
location ──────────────────────────────────────────── data_prod_source
                                                             │ (many)
data_prod_type ── data_prod ───────────────────── data_prod_data_kind
                      │ (src/dst)                       │
             data_prod_assoc                      data_kind
             (src_fk, dst_fk,
              assoc_type_fk)
                      │
             data_prod_assoc_type

data_prod ── data_prod_flag ── flag

event_log  (references any entity by type+id string)
```

---

## 3. Typed Metadata (AdaptixJSON)

### 3.1 Philosophy

All `meta` columns use `AdaptixJSON` from `adaptix.integrations.sqlalchemy`. A single global `Retort` instance in `models/metadata.py` handles all serialization. Union types use `Literal` discriminator fields (the `tag` pattern from v2.5).

### 3.2 DataProd Metadata Types (`AnyDataProdMeta`)

One typed dataclass per `data_prod_type`:

| Type label | Metadata class | Key fields |
|---|---|---|
| `dp_raw_obs` | `RawObsMeta` | `master`, `obsnum`, `subobsnum`, `scannum`, `data_kind` (int flags), telescope fields (tau, az_deg, el_deg, source_name, project_id, m1_zernike, etc.) |
| `dp_reduced_obs` | `ReducedObsMeta` | `master`, `obsnum`, `subobsnum`, `scannum`, `reduction_method`, `calibration_version`, `quality_score` |
| `dp_cal_group` | `CalGroupMeta` | `n_items`, `group_type`, `date_range`, `obs_datetime` |
| `dp_drivefit` | `DrivefitMeta` | `n_items`, `fit_method`, `chi_squared`, `obs_datetime` |
| `dp_focus_group` | `FocusGroupMeta` | `n_items`, `focus_positions`, `best_focus`, `obs_datetime` |
| `dp_astig_group` | `AstigGroupMeta` | `n_items`, `astig_positions`, `best_astig`, `obs_datetime` |
| `dp_oof_group` | `OofGroupMeta` | `n_items`, `oof_positions`, `surface_rms`, `obs_datetime` |
| `dp_named_group` | `NamedGroupMeta` | `group_name`, `n_items`, `tags`, `owner`, `notes` |

All inherit from `DataProdMetaBase` (which has `name: str`, `data_prod_type: DataProdType`, `description`, `obs_datetime`).

`RawObsMeta` also inherits `ObsIdMixin` (obsnum/subobsnum/scannum/master) and `TelMetaMixin` (all telescope fields, denormalized for query efficiency without joins through DataProdSource).

### 3.3 DataProdSource Metadata Types (`AnyInterfaceMeta`)

One typed dataclass per interface class:

| Interface | Metadata class | Key fields |
|---|---|---|
| `toltec[0-12]` (roach) | `RoachInterfaceMeta` | `nw_id`, `roach`, `interface`, `hostname`, `data_kind`, `obsnum`, `subobsnum`, `scannum`, `master` |
| `tel_toltec`, `tel_toltec2` | `TelInterfaceMeta` | all telescope fields: `az_deg`, `el_deg`, `tau`, `m1_zernike`, `m2_offset_mm`, `source_name`, `project_id`, `integration_time`, `obs_datetime`, `valid`, etc. |
| `hwpr` | `HwprInterfaceMeta` | `obsnum`, `subobsnum`, `scannum`, `master` — extend as needed |

Discriminated via `type: Literal["roach" | "tel" | "hwpr"]`.

### 3.4 Association Context

```python
@dataclass
class ProcessContext:
    module: str | None = None
    version: str | None = None
    config: dict[str, Any] | None = None
```

---

## 4. Constants and Enums

All enums are `StrEnum` (Python 3.11+). Values are **uppercase** to match v2.5 registration convention and to be visually distinct from column names.

| Enum | Values | Registry table |
|------|--------|---------------|
| `DataProdType` | `DP_RAW_OBS`, `DP_REDUCED_OBS`, `DP_CAL_GROUP`, `DP_DRIVEFIT`, `DP_FOCUS_GROUP`, `DP_ASTIG_GROUP`, `DP_OOF_GROUP`, `DP_MAP`, `DP_CATALOG`, `DP_NAMED_GROUP` | `data_prod_type` |
| `DataKind` | `VnaSweep`, `TargetSweep`, `Tune`, `RawTimeStream`, `D21`, `ReducedVnaSweep`, `ReducedTargetSweep`, `SolvedTimeStream`, `LmtTel`, `LmtTel2`, `Unknown` | `data_kind` |
| `DataProdAssocType` | `DPA_RAW_OBS_CAL_OBS`, `DPA_REDUCED_OBS_RAW_OBS`, `DPA_CAL_GROUP_RAW_OBS`, `DPA_DRIVEFIT_RAW_OBS`, `DPA_FOCUS_GROUP_RAW_OBS`, `DPA_ASTIG_GROUP_RAW_OBS`, `DPA_OOF_GROUP_RAW_OBS`, `DPA_NAMED_GROUP_DATA_PROD` | `data_prod_assoc_type` |
| `ReducedStatus` | `ACTIVE`, `SUPERSEDED` | (on `data_prod.lifecycle_status`) |
| `StorageRole` | `PRIMARY`, `MIRROR`, `TEMP` | (on `data_prod_source.role`) |
| `AvailabilityState` | `PLANNED`, `PARTIAL`, `AVAILABLE`, `MISSING`, `REMOTE`, `STAGED` | (on `data_prod.availability_state`, `data_prod_source.availability_state`) |
| `FlagNamespace` | `QA`, `DETECTOR`, `TELESCOPE`, `CALIBRATION`, `INGEST` | (namespaces in `flag` table) |
| `LocationType` | `FILESYSTEM`, `S3`, `HTTP`, `API` | (on `location.location_type`) |
| `MasterType` | `tcs`, `ics`, `clip` | (in `RawObsMeta`, `ObsSpec`) |

`DataKind` remains a `Flag` enum (bitwise) in Python. In the database it is stored as a flag `int` value in `RawObsMeta.data_kind` (packed bitfield). The `data_prod_data_kind` junction table expands this for queryability.

`ToltecInfo` class remains as a lookup table of instrument constants (roach↔interface, interface↔array_name, obsnum range, master lists, etc.).

---

## 5. ObsSpec (Python DSL Layer)

`ObsSpec` in `obsspec.py` is a Python-world utility, **not** an ORM identity:

- Frozen, ordered dataclass: `(master, obsnum, subobsnum, scannum)`
- `ObsSpec.uid` property → `"{master}-{obsnum}-{subobsnum}-{scannum}"` for display/logging
- `ObsSpec.parse(s)` — parse UID string
- `ObsSpec.from_path(path)` — extract from filename by interface prefix patterns

The database stores `obsnum`, `subobsnum`, `scannum`, `master` as typed fields inside `RawObsMeta.meta`. A denormalized index column `obsspec_uid str(128) UNIQUE` *may* be added to `data_prod` for performance on `dp_raw_obs` rows, but it is derived.

**Validation:** master ∈ `{tcs, ics, clip}`; obsnum ∈ `[1, 999_999]`.

---

## 6. Module Structure

```
src/tolteca_db/
├── obsspec.py                    # ObsSpec dataclass + parser (single source of truth)
├── constants.py                  # All StrEnum/Flag enums + ToltecInfo lookup tables
├── __init__.py
│
├── models/
│   ├── metadata.py               # AdaptixJSON dataclasses:
│   │                             #   RawObsMeta, ReducedObsMeta, CalGroupMeta, ...
│   │                             #   RoachInterfaceMeta, TelInterfaceMeta, HwprInterfaceMeta
│   │                             #   ProcessContext, DataProdMetaBase, ObsIdMixin, TelMetaMixin
│   │                             #   AnyDataProdMeta (union), AnyInterfaceMeta (union)
│   │                             #   adaptix_json_type() factory, global _retort
│   └── orm/
│       ├── base.py               # DeclarativeBase, JSON type instance
│       ├── registry.py           # Location, DataProdType, DataKind,
│       │                         #   DataProdAssocType, Flag
│       ├── data_prod.py          # DataProd, DataProdDataKind, DataProdSource
│       ├── assoc.py              # DataProdAssoc
│       ├── flag.py               # DataProdFlag
│       └── event.py              # EventLog
│
├── db/
│   ├── config.py                 # DatabaseConfig (pydantic BaseSettings)
│   ├── database.py               # Database class + create_database() factory
│   └── parquet.py                # DuckDB parquet query utilities
│
├── repository/
│   └── repository.py             # Repository: CRUD + business queries
│
├── ingest/
│   ├── scanner.py                # FileScanner → yields InterfaceFileRecord
│   ├── ingestor.py               # DataIngestor: scanner → DB registration
│   ├── tel_ingestor.py           # Telescope telemetry ingestor
│   └── lmtmc_api.py              # LMT machine control API client
│
├── associations/
│   ├── engine.py                 # AssociationEngine
│   ├── pool.py                   # ObservationPool (DataFrame backend)
│   ├── state.py                  # AssociationState
│   └── rules.py                  # Pure association rule functions
│
├── cli/
│   ├── db_commands.py
│   ├── ingest_commands.py
│   ├── query_commands.py
│   └── assoc_commands.py
│
├── dagster/
│   ├── resources.py
│   ├── definitions.py
│   ├── assets.py
│   ├── sensors.py
│   └── partitions.py
│
└── compat/
    └── file.py                   # tolteca_v2-compatible DataFrame accessor
```

Note: the `models/metadata.py` file **does not contain frozen domain dataclasses mirroring ORM tables**. It contains only `AdaptixJSON` metadata dataclasses for the `meta` column. Repository-layer domain objects (if needed) will be a separate concern.

---

## 7. Implementation Status and What Needs Rework

### ✅ Correct and Stable (no changes needed)

| Module | Status | Notes |
|--------|--------|-------|
| `obsspec.py` | ✅ Keep | Single canonical parser, frozen dataclass — correct |
| `constants.py` | ⚠️ Revise | DataKind extended correctly; enum values should be UPPERCASE to match registry labels; `DataProdType` values lowercase now, should be uppercase |
| `utils/mapped_types.py` | ✅ Keep | `Pk`, `LabelKey`, `Created_at`, `Updated_at`, `fk()` — correct patterns |

### ❌ Needs Rework (ORM — Phase 2 redo)

The current v3.x Phase 2 ORM deviates from the constitution in these ways:

| Current v3.x (wrong) | Correct design |
|-----------------------|----------------|
| `raw_obs` table with string PK | No `raw_obs` table — raw obs = `data_prod` row with `RawObsMeta` |
| `data_prod` with inline catalog columns (`nw`, `array_name`, `nc_path`, etc.) | `data_prod` minimal: type FK, status, hash, `AdaptixJSON` meta |
| `data_prod.data_prod_type` is a string column | `data_prod.data_prod_type_fk` → `data_prod_type` registry table |
| `data_prod.data_kind` single inline string | `data_prod_data_kind` junction (many-to-many) + `data_kind` registry |
| `storage_root` + `file_record` tables | `location` + `data_prod_source` (URI PK, typed interface meta) |
| `assoc` + `assoc_edge` (group+edge) | `data_prod_assoc` (directed edge, assoc_type FK) + `data_prod_assoc_type` registry |
| `obs_flag` table (separate obs flags) | No separate obs table → `data_prod_flag` covers all products including raw obs |
| `task` table (simple) | No `task` table for now — task context in `data_prod.meta` |
| Free-form `meta: JSON` on all tables | `AdaptixJSON[TypedClass]` — explicit typed union per table |
| No registry tables | `data_prod_type`, `data_kind`, `data_prod_assoc_type`, `flag` registries needed |
| `event` table (10 cols) | `event_log` table (same concept, minor rename) |
| 10 frozen "domain dataclasses" mirroring ORM | Remove — not needed at this layer |

### ❌ Needs Rework (Metadata — Phase 3 redo)

Current `models/metadata.py` has 10 frozen dataclasses mirroring ORM tables (e.g., `RawObs`, `DataProd`, `StorageFile` etc.) — these are NOT the correct metadata pattern. They should be replaced with the `AdaptixJSON` typed dataclasses described in §3.

---

## 8. Availability State Machine

Computed on read from `data_prod_source` rows — never stored on `data_prod` as a denormalized column (stored on `data_prod_source` per-source, and rolled up to product level on demand).

```
PLANNED ──(first source file appears)──→ PARTIAL
PARTIAL ──(all expected interfaces present)──→ AVAILABLE
PLANNED/PARTIAL/AVAILABLE ──(all sources become unreachable)──→ MISSING
AVAILABLE ──(source on remote-only location)──→ REMOTE
REMOTE ──(transfer queued)──→ STAGED
STAGED ──(transfer complete)──→ AVAILABLE
```

"Expected interfaces" for a `dp_raw_obs` = all interfaces active in `ToltecInfo` for the relevant master, minus any disabled interfaces recorded in the obs context.

---

## 9. DB Seeding: Registry Tables

At database init (`create_all()`), registry tables are seeded from Python enums:

```python
# Pseudo-code for seeding
for member in DataProdType:
    db.merge(DataProdTypeRecord(label=member.value, level=member.level, description=member.description))

for member in DataKind:
    db.merge(DataKindRecord(label=member.name, category=member.category, description=...))

# ... same for DataProdAssocType, Flag (seeded from QAFlag, DetectorFlag, etc.)
```

This ensures Python enums and DB registry rows are always in sync after any `create_all()` call.
