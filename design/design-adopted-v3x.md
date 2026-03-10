# Adopted Key Design (v3.x)

**Status:** Active — requires revision per design Q&A  
**Last Updated:** 2026-03-10  
**Constitutional Reference:** [toltec_data_file_and_data_flow.md](toltec_data_file_and_data_flow.md)

This document records the current adopted architectural decisions for the v3.x codebase. It is AI-maintained and should be updated whenever the design evolves. The user-authored requirements and design principles that govern this document live in the constitutional reference above.

> **Note (2026-03-10):** The design answers in the constitutional doc (questions 1–8) supersede several decisions recorded below. This document needs revision to align with those answers — in particular: raw_obs should be treated as a `data_prod` row (not a separate table), `DataProdType`/`DataKind`/flags should use normalized registry tables, interface metadata belongs on `DataProdSource`, and associations should use directed edges (not group+edge). See [spike-orm-schema-v25-vs-v3.md](spike-orm-schema-v25-vs-v3.md) for the detailed comparison.

---

## 1. ObsSpec: The Universal Identifier

Every raw observation is uniquely identified by the quartet `(master, obsnum, subobsnum, scannum)`.

**Canonical UID format:** `"{master}-{obsnum}-{subobsnum}-{scannum}"` (e.g., `"tcs-98765-0-0"`)

**Implemented in:** `tolteca_db/obsspec.py` — the single authoritative parser. No other module parses ObsSpec strings.

- `ObsSpec` is a frozen, ordered, slotted dataclass.
- `ObsSpec.parse(s)` — parse a UID string back into an ObsSpec.
- `ObsSpec.from_path(path)` — extract from any TolTEC filename by recognising interface prefixes (`toltecN_*`, `hwpr_*`, `tel_toltec*`, `clip_*`, `icsN_*`).
- Validation: master must be one of `tcs`, `ics`, `clip`; obsnum must be in `[1, 999_999]`.

**Rationale:** A single canonical parser eliminates the v2.5 problem of duplicated and inconsistent parsing logic scattered across modules.

---

## 2. Constants and Enums

**Implemented in:** `tolteca_db/constants.py`

All categorical values are `StrEnum` (string-valued), ensuring they serialise cleanly to/from JSON and SQL without translation layers.

| Enum | Values | Purpose |
|------|--------|---------|
| `MasterType` | `tcs`, `ics`, `clip` | Observation master types |
| `DataProdType` | `dp_raw_obs`, `dp_reduced_obs`, `dp_cal_group`, `dp_drivefit`, `dp_focus_group`, `dp_astig_group`, `dp_oof_group`, `dp_map`, `dp_catalog`, `dp_named_group` | Data product types |
| `DataProdAssocType` | `dpa_raw_obs_cal_obs`, `dpa_reduced_obs_raw_obs`, `dpa_cal_group_raw_obs`, `dpa_drivefit_raw_obs`, `dpa_focus_group_raw_obs`, `dpa_astig_group_raw_obs`, `dpa_oof_group_raw_obs`, `dpa_named_group_data_prod` | Association relationship types |
| `DataKind` | `VnaSweep`, `TargetSweep`, `Tune`, `RawTimeStream`, `D21`, ... (Flag-based, supports bitwise `\|`) | Data type classification |
| `TaskStatus` | `queued`, `running`, `done`, `error` | Task lifecycle states |
| `FlagSeverity` | `info`, `warn`, `block`, `critical` | Flag severity levels |
| `StorageRole` | `primary`, `mirror`, `temp` | Storage copy roles |
| `ReducedStatus` | `active`, `superseded` | Reduced data product lifecycle |

**`ToltecInfo`** — class-level lookup tables: `roach_interface`, `interface_master`, `interface_array_name`, `obsnum_min/max`. These are the single source of truth for TolTEC hardware→software mappings.

---

## 3. ORM Schema (10 Tables)

**Implemented in:** `tolteca_db/models/orm/` — SQLAlchemy 2.x `Mapped[]` / `mapped_column()` pattern.

The schema is designed around the conceptual hierarchy from the requirements:

```
raw_obs  (immutable logical observation — identified by ObsSpec UID)
  └── data_prod  (one per interface per obs — "the data item")
        ├── file_record  (physical file in a storage_root — "where the data item lives")
        │     └── storage_root  (a data root — site, server, local, scratch, etc.)
        ├── assoc_edge  (link to an association group)
        │     └── assoc  (a group of related data products)
        │           └── task  (a processing task derived from an association)
        └── data_prod_flag  (quality flags on data products)
  └── obs_flag  (quality flags on observations)

event  (append-only audit log — tracks changes to any entity)
```

### 3.1 `raw_obs` — Logical Observation

| Column | Type | Notes |
|--------|------|-------|
| **`uid`** | `str(128)` PK | ObsSpec UID (e.g., `"tcs-98765-0-0"`) — **string PK, not integer** |
| `master` | `str(32)` | `tcs`, `ics`, `clip` |
| `obsnum` | `int` | Indexed for range queries |
| `subobsnum` | `int` | |
| `scannum` | `int` | |
| `timestamp_obs` | `datetime(tz)` | UTC observation start (nullable — may not be known at mint time) |
| `meta` | `JSON` | Additional metadata |
| `created_at` | `datetime(tz)` | Insert timestamp |

**Design decision — string PK:** The ObsSpec UID is the natural key. Using it as PK avoids a surrogate integer and makes joins human-readable. Every downstream FK is a string (`raw_obs_uid`), directly interpretable without a lookup.

**Immutability:** `raw_obs` rows represent logical observations that exist a priori (per requirements). They are created once and never updated (no `updated_at` column).

### 3.2 `data_prod` — Data Product (One Per Interface Per Obs)

| Column | Type | Notes |
|--------|------|-------|
| **`pk`** | `int` PK | Auto-increment |
| `uid` | `str(256)` unique | Human-readable (e.g., `"dp_raw_obs/tcs-98765-0-0/toltec0"`) |
| `raw_obs_uid` | `str(128)` FK→`raw_obs.uid` | Parent observation |
| `data_prod_type` | `str(32)` | `dp_raw_obs`, `dp_reduced_obs`, etc. |
| `interface` | `str(32)` | `toltec0`…`toltec12`, `hwpr`, `tel_toltec`, etc. |
| **Catalog columns** | various, all nullable | See below |
| `availability` | `str(32)` | Availability state |
| `meta` | `JSON` | Additional metadata |
| `created_at` / `updated_at` | `datetime(tz)` | Timestamps |

**Catalog columns (inline on `data_prod`):**

| Column | Type | Purpose |
|--------|------|---------|
| `nw` | `int` | Roach/network index (0–12) |
| `array_name` | `str(16)` | `a1100`, `a1400`, `a2000` |
| `data_kind` | `str(32)` | VnaSweep, RawTimeStream, etc. |
| `n_chans` | `int` | Number of detector channels |
| `n_samples` | `int` | Number of time samples |
| `lo_center_freq_hz` | `float` | LO centre frequency |
| `drive_atten_db` | `float` | Drive attenuation |
| `sense_atten_db` | `float` | Sense attenuation |
| `nc_path` | `str(512)` | Path to netCDF file |
| `zarr_path` | `str(512)` | Path to zarr store |

**Design decision — inline catalog columns:** These columns are stored directly on `data_prod` instead of in a separate table or JSON blob. This enables direct SQL joins with `tolteca_web` catalog queries without JSON parsing at query time. All are nullable because not every product type has every field.

**Mapping to requirements:** Each `data_prod` row corresponds to one "data_item" in the requirements. A `dp_raw_obs` is one interface file; a `dp_reduced_obs` is one reduced product directory.

### 3.3 `storage_root` — Data Root

| Column | Type | Notes |
|--------|------|-------|
| **`pk`** | `int` PK | Auto-increment |
| `label` | `str(128)` unique | e.g., `"site_data_root"`, `"server_archive_2024"`, `"local_scratch"` |
| `host` | `str(256)` | Hostname for remote roots |
| `root_path` | `str(512)` | Absolute path or URI prefix |
| `is_local` | `bool` | `True` if directly accessible on this machine |
| `meta` | `JSON` | Additional metadata |
| `created_at` | `datetime(tz)` | Insert timestamp |

**Mapping to requirements:** Each `storage_root` is a "data_root" or "dataprod_root" from the requirements. Multiple roots are first-class — site, server archive, server scratch, commissioning batches, user local are all equal peers.

### 3.4 `file_record` — Physical File Location

| Column | Type | Notes |
|--------|------|-------|
| **`pk`** | `int` PK | Auto-increment |
| `data_prod_fk` | `int` FK→`data_prod.pk` | Which data product this file belongs to |
| `storage_root_fk` | `int` FK→`storage_root.pk` (nullable) | Which root it lives in |
| `rel_path` | `str(512)` | Path relative to storage root |
| `mtime` | `float` | POSIX modification timestamp |
| `file_size` | `int` | Bytes |
| `checksum` | `str(128)` | Content hash (e.g., `blake3:abcd…`) |
| `created_at` / `updated_at` | `datetime(tz)` | Timestamps |

**Mapping to requirements:** This is how "a data_item can exist in multiple storage locations" is implemented. One `data_prod` can have multiple `file_record` rows, each pointing to a different `storage_root`. Availability is determined by whether at least one `file_record` exists in a `storage_root` where `is_local = True`.

### 3.5 `assoc` + `assoc_edge` — Association Groups

Associations link related data products (e.g., "these raw obs form a calibration group"). The design uses a **group + edge** pattern:

- `assoc` — the group itself (uid, rule_name, status, score)
- `assoc_edge` — one edge per data product in the group (assoc_fk, data_prod_fk, role)

The `role` field on edges distinguishes `input` vs `output` within an association.

### 3.6 `obs_flag` + `data_prod_flag` — Quality Flags

Separate flag tables for observations vs data products. Each flag records: reason, source (manual/pipeline), and timestamp.

### 3.7 `task` — Processing Tasks

Tasks track reduction/processing jobs. Linked to an association (optional). Status: `queued` → `running` → `done` | `error`.

### 3.8 `event` — Audit Log

Append-only event log. Each event records: type, entity reference (type + id), payload, timestamp. Indexed for efficient entity-scoped queries.

---

## 4. Domain Dataclasses (Metadata Layer)

**Implemented in:** `tolteca_db/models/metadata.py`

10 frozen dataclasses mirror the ORM tables 1:1 but are completely decoupled from SQLAlchemy. These are the "currency" of the repository layer — all business logic operates on domain objects, never ORM records.

Manual converter functions (e.g., `raw_obs_from_record(rec) -> RawObs`) translate ORM→domain.

**JSON serialisation** via `adaptix.Retort`: `to_dict()` / `from_dict()` for all domain objects. `datetime` values serialise to ISO-8601 strings and round-trip perfectly.

**Why manual converters:** `adaptix.conversion.get_converter` fails with Python 3.14 because ORM modules use `from __future__ import annotations` + `TYPE_CHECKING`-only imports, causing `NameError` on forward references. Manual converters bypass this entirely.

---

## 5. How the Schema Maps to Requirements

| Requirement (from user content above) | How it's implemented |
|---------------------------------------|---------------------|
| Multiple data_root (site, server, user, historical batches) | `storage_root` table — one row per root, `is_local` flag for accessibility |
| dp_raw_obs are logical, immutable, exist a priori | `raw_obs` table — string PK from ObsSpec UID, no `updated_at`, created once |
| Each raw obs has per-interface files | `data_prod` table — one row per interface, FK to `raw_obs` |
| Data items can exist in multiple storage locations | `file_record` table — multiple rows per `data_prod`, each in a different `storage_root` |
| Availability = accessible by user | Computed from `file_record` JOIN `storage_root` WHERE `is_local = True` |
| Derived products identified by content_hash | `file_record.checksum` stores content hash; `data_prod.uid` includes derivation context |
| Derived data products are one-to-one with directory | `data_prod` row + single `file_record` pointing to directory |
| Interface files: toltec[0-12], hwpr, tel_toltec, etc. | `data_prod.interface` column + `ToltecInfo` lookup tables in constants.py |
| Master types: tcs, ics, clip | `MasterType` StrEnum + `raw_obs.master` column |
| Obsnum uniquely identifies observation | `ObsSpec` parser + `raw_obs.uid` as PK |

---

## 6. Key Architectural Invariants

These invariants must be preserved during all future development:

1. **ObsSpec is parsed in exactly one place** (`obsspec.py`). No other module constructs or parses ObsSpec UIDs.

2. **ORM records never leak past the repository boundary.** All public APIs return domain dataclasses, not SQLAlchemy model instances.

3. **`raw_obs.uid` is an immutable string PK** derived from ObsSpec. It cannot be changed to an integer surrogate without rethinking the entire FK chain.

4. **Catalog columns live on `data_prod`, not in JSON.** This is a deliberate denormalisation for query performance. New catalog fields should be added as nullable columns.

5. **`storage_root` is the multi-root mechanism.** All file paths are relative to a root. A file's absolute path is `storage_root.root_path / file_record.rel_path`.

6. **The `event` table is append-only.** Events are never updated or deleted.

7. **StrEnum values are lowercase strings.** They serialise to SQL as-is. No integer encoding.

---

## 7. Implementation Status

| Phase | Description | Status | Tests |
|-------|-------------|--------|-------|
| 0 | Architecture & design docs | ✅ Complete | — |
| 1 | `constants.py` + `obsspec.py` | ✅ Complete | 66 |
| 2 | ORM models (10 tables) | ✅ Complete | 34 |
| 3 | Domain dataclasses + converters + Retort | ✅ Complete | 25 |
| 4 | Database layer (multi-backend factory) | ⬜ Planned | — |
| 5 | Repository (CRUD API) | ⬜ Planned | — |
| 6 | FileScanner | ⬜ Planned | — |
| 7 | DataIngestor | ⬜ Planned | — |
| 8 | Association Engine | ⬜ Planned | — |
| 9 | CLI (Typer) | ⬜ Planned | — |

**Total tests passing:** 125
