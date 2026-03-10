# Spike: ORM Schema Comparison — v2.5 (refs) vs v3.x (current)

**Status:** Active — for design review and iteration  
**Created:** 2026-03-10  
**Purpose:** Side-by-side comparison of the two schema designs to inform which approach, or which blend, to carry forward.

---

## 1. Table Inventory

| v2.5 table | v2.5 purpose | v3.x table | v3.x purpose |
|---|---|---|---|
| *(absent)* | — | `raw_obs` | First-class logical observation with string PK |
| `data_prod_type` | **Registry** of product type labels + level | *(inline column)* | `data_prod.data_prod_type` string |
| `data_prod` | Unified product (typed JSON meta) | `data_prod` | Product with inline catalog columns |
| `data_kind` | **Registry** of data kind labels + category | *(inline column)* | `data_prod.data_kind` string |
| `data_prod_data_kind` | Junction: product ↔ kind (many-to-many) | *(absent)* | — |
| `data_prod_source` | Per-interface file locations (URI PK, typed JSON meta) | `file_record` | file path relative to a storage root |
| `location` | Data root registry (type, URI, priority) | `storage_root` | Data root (label, host, root_path, is_local) |
| `data_prod_assoc_type` | **Registry** of association type labels | *(inline column)* | `assoc.rule_name` string |
| `data_prod_assoc` | Directed src→dst edge + typed JSON context | `assoc` + `assoc_edge` | Group + N edges |
| `flag` | **Registry** of flag types (namespace + label) | *(absent)* | — |
| `data_prod_flag` | Junction: product ↔ flag (composite PK) | `data_prod_flag` | Inline reason + source strings |
| *(absent)* | — | `obs_flag` | Obs-level flag (separate from product flags) |
| `reduction_task` | Task with explicit hash-based deduplication | `task` | Task with optional assoc link |
| `task_input` | Explicit input products junction | *(absent / via assoc_edge)* | — |
| `task_output` | Explicit output products junction | *(absent / via assoc_edge)* | — |
| `event_log` | Append-only audit log | `event` | Append-only audit log |

**Count: v2.5 has 14 tables. v3.x has 10 tables.**

---

## 2. Difference-by-Difference Analysis

### 2.1 Observation Identity: `raw_obs` Table

**v2.5 — No `raw_obs` table.**  
Observation identification (`obsnum`, `subobsnum`, `scannum`, `master`) is embedded inside each `DataProd.meta` JSON via `ObsIdMixin`. To find all products for an observation you query:
```sql
-- v2.5: requires JSON extraction
SELECT * FROM data_prod
WHERE json_extract(meta, '$.obsnum') = 98765
  AND json_extract(meta, '$.master') = 'tcs';
```
The `meta` field is typed using `AdaptixJSON` discriminated unions, so the structure is well-defined in Python, but not enforced by the database schema.

**v3.x — Explicit `raw_obs` table with string PK.**  
`raw_obs.uid = "tcs-98765-0-0"`. All `data_prod` rows carry a `raw_obs_uid` FK as a plain indexed string column:
```sql
-- v3.x: plain indexed FK join
SELECT * FROM data_prod WHERE raw_obs_uid = 'tcs-98765-0-0';
```

**Trade-offs:**

| | v2.5 | v3.x |
|---|---|---|
| Query on obsnum | JSON extraction (slower, less portable) | Plain FK column join (fast, indexed) |
| Schema enforces obs structure | No — JSON is opaque to DB | Yes — normalized table with explicit columns |
| Obs-level operations (flags, timestamps) | Must go through `data_prod` rows | Direct on `raw_obs` row |
| Obs exists before files arrive | Implicit (meta in future data_prod rows) | Explicit (`raw_obs` row minted from ObsSpec) |
| Immutability expressed | No | Yes — no `updated_at` column |

---

### 2.2 Product Type and Data Kind: Registry Tables vs Inline Columns

**v2.5 — Normalized registry tables.**  
- `data_prod_type` table: `label` (unique), `description`, `level` (int — 0=raw, 1+=reduced). A `data_prod` carries a `data_prod_type_fk` FK.
- `data_kind` table: `label` (unique), `category` (shape/calibration/measurement/ancillary), `description`. A `data_prod_data_kind` junction assigns kinds to products (many-to-many). A product can carry multiple data kinds.

**v3.x — Inline string columns.**  
- `data_prod.data_prod_type` = plain string column (e.g., `"dp_raw_obs"`).
- `data_prod.data_kind` = plain string column — but the `DataKind` Python class is a `Flag` enum supporting bitwise combinations (e.g., `VnaSweep | TargetSweep`). In SQL this is stored as a single string, losing the bitwise nature.

**Trade-offs:**

| | v2.5 | v3.x |
|---|---|---|
| Multiple kinds per product | Yes (junction table) | No (single inline column) |
| Kind category metadata | Yes (category column in `data_kind` table) | No |
| Processing level on type | Yes (`level` column in `data_prod_type`) | No (only in application code) |
| Querying by type | FK join or subquery | Direct string column filter |
| Extensibility | Add row to registry table | Requires code + migration |
| Complexity | More tables, more joins | Simpler schema |

**Note:** TolTEC raw obs files can have `DataKind = VnaSweep | TargetSweep` (a Tune). The v2.5 junction table supports recording both kinds. The v3.x single column loses this without encoding it as a special string value.

---

### 2.3 File Location: `DataProdSource`/`Location` vs `file_record`/`storage_root`

**v2.5 — `DataProdSource` (URI PK) + `Location` (data root).**

`DataProdSource`:
- PK is `source_uri` (string, e.g., `"file:///data_lmt/toltec/tcs/toltec0_098765_000_0000.nc"`)
- Has `location_fk` → `location` table
- `meta` = `AnyInterfaceMeta` — typed JSON union: either `RoachInterfaceMeta` (nw_id, roach, hostname, data_kind, obsnum, etc.) or `TelInterfaceMeta` (pointing, tau, m1_zernike, etc.)
- Per-source `availability_state`, `size`, `checksum`, `last_verified_at`

`Location`:
- `label` (unique), `location_type` (`filesystem`, `s3`, `http`, `api`), `root_uri`, `priority` (int — lower = preferred)

**v3.x — `file_record` (int PK) + `storage_root`.**

`file_record`:
- PK is int. `rel_path` (relative to storage root) + `storage_root_fk`.
- No interface-level metadata at all — roach/tel info lives on `data_prod` as inline columns.
- Has `mtime`, `file_size`, `checksum`.

`storage_root`:
- `label` (unique), `host`, `root_path`, `is_local` (bool). No `location_type`, no `priority`.

**Trade-offs:**

| | v2.5 | v3.x |
|---|---|---|
| Per-source interface metadata | Yes (typed JSON in `DataProdSource.meta`) | No (interface info on `data_prod`, not file) |
| URI as primary key | Yes — self-describing, portable | No — surrogate int PK |
| Location type (s3, http, etc.) | Yes (`location_type` column) | No |
| Priority for source selection | Yes (`priority` int) | No |
| Per-source availability tracking | Yes (`availability_state` per source) | No (computed from `is_local`) |
| Last verified timestamp | Yes | No |
| Path decomposition (root + rel) | Implicit in URI | Explicit columns |

---

### 2.4 Rich Per-Product Metadata: Typed JSON versus Inline Columns

This is the most fundamental philosophical difference.

**v2.5 — `DataProd.meta` = discriminated union of typed dataclasses via `AdaptixJSON`.**

Each product type has its own rich metadata class:
- `RawObsMeta`: obsnum, subobsnum, scannum, master, obs_datetime, source_name, project_id, tau, az_deg, el_deg, m1_zernike, m2_offset_mm, data_kind, …
- `ReducedObsMeta`: obsnum, reduction_method, calibration_version, quality_score, …
- `CalGroupMeta`: n_items, group_type, date_range, …
- `DrivefitMeta`: n_items, fit_method, chi_squared, …
- `FocusGroupMeta`: n_items, best_focus, focus_metric, …
- `AstigGroupMeta`: n_items, best_astig, …
- `OofGroupMeta`: n_items, surface_rms, …
- `NamedGroupMeta`: group_name, tags, owner, notes, …

The `tag` literal discriminator field allows `adaptix` to deserialize the correct Python type from JSON automatically. Each type gets exactly the fields it needs.

`DataProdSource.meta` similarly has per-interface rich metadata:
- `RoachInterfaceMeta`: nw_id, roach, interface, hostname, data_kind, obsnum, …
- `TelInterfaceMeta`: all telescope pointing/m1/m2/tau fields — 20+ fields

**v3.x — Inline catalog columns on `data_prod`, free-form JSON `meta` blob.**

The 10 TolTEC-specific columns (`nw`, `array_name`, `data_kind`, `n_chans`, `n_samples`, `lo_center_freq_hz`, `drive_atten_db`, `sense_atten_db`, `nc_path`, `zarr_path`) are the only "typed" columns. Remaining metadata goes into the free-form `meta` JSON. Telescope pointing, tau, source name, project ID — none of these have dedicated columns.

**Trade-offs:**

| | v2.5 | v3.x |
|---|---|---|
| Per-type rich metadata fields | Yes — type-safe, Python autocomplete | No — free-form JSON or must add many nullable columns |
| Telescope/pointing metadata stored | Yes (RawObsMeta via TelMetaMixin) | No dedicated column |
| SQL-indexable non-JSON columns | Only `lifecycle_status`, `availability_state`, `content_hash` | Yes — 10 catalog columns + `raw_obs_uid`, `interface`, etc. |
| Query `tau < 0.1` | JSON extraction required | Not stored (would need to add column) |
| Type-safe Python metadata access | Strong — adaptix deserializes to typed class | Weak — `meta` is `dict[str, Any]` |
| Schema migration cost | Low (JSON absorbs new fields) | High (new fields = new nullable columns) |

---

### 2.5 Associations

**v2.5 — Simple directed edge.**

```
data_prod_assoc_type (registry)
  pk, label ("dpa_reduced_obs_raw_obs"), description

data_prod_assoc (directed edge)
  pk, data_prod_assoc_type_fk, src_data_prod_fk, dst_data_prod_fk
  context: JSON[ProcessContext]  (module, version, config)
  created_at
```
Semantics: one row = one directed relationship from `src` to `dst` via a typed edge. Multiple edges can exist between the same pair with different types.

**v3.x — Group + edge model.**

```
assoc (group)
  pk, uid, rule_name (raw string), context JSON, status, score
  created_at, updated_at

assoc_edge (edge)
  pk, assoc_fk, data_prod_fk, role ("input"/"output")
```
Semantics: one `assoc` row is a named group; its `assoc_edge` rows list the products in that group with roles.

**Trade-offs:**

| | v2.5 | v3.x |
|---|---|---|
| Association type registry | Yes (normalized `data_prod_assoc_type` table) | No (raw string `rule_name`) |
| Process context on edge | Yes (typed `ProcessContext` JSON) | No |
| N-to-M grouping pattern | Awkward (pair-wise edges) | Natural (`assoc` + multiple edges) |
| Directed src→dst semantics | Explicit | Implicit via `role` field |
| Status + score tracking | No | Yes (`assoc.status`, `assoc.score`) |
| Association lifecycle | Immutable (created_at only) | Mutable (updated_at, status transitions) |

---

### 2.6 Flags

**v2.5 — Normalized flag registry.**

```
flag (registry)
  pk, namespace ("qa", "detector", "telescope"), label ("SATURATED", "DEAD_PIXEL")
  UNIQUE(namespace, label)

data_prod_flag (junction, composite PK)
  data_prod_fk, flag_fk
  asserted_at, asserted_by, context JSON
```
Flags are defined once in the registry; multiple products can carry the same flag.

No obs-level flags — to flag an observation you flag its `data_prod` of type `dp_raw_obs`.

**v3.x — Inline flat flags.**

```
obs_flag
  pk INT, raw_obs_uid FK(string), flag_reason str, flag_source str, flagged_at

data_prod_flag
  pk INT, data_prod_fk, flag_reason str, flag_source str, flagged_at
```
Free-form reason string — no normalization. Obs-level flags have their own table.

**Trade-offs:**

| | v2.5 | v3.x |
|---|---|---|
| Flag type normalization | Yes (registry table) | No (free-form strings) |
| Flag namespacing | Yes (namespace + label) | No |
| Same flag on multiple products | Yes (FK to registry) | Repeated rows with same reason string |
| Obs-level flags | No separate table (use product flags) | Yes (`obs_flag` table) |
| Context on flag assignment | Yes (JSON context) | No |
| Querying by flag type | Join to registry | String match |

---

### 2.7 Tasks

**v2.5 — Hash-based deduplication, explicit I/O.**

```
reduction_task
  pk, status, params_hash, params JSON, input_set_hash
  worker_host, started_at, finished_at, error_message, created_at

task_input  (junction)
  task_fk, data_prod_fk, role

task_output (junction)
  task_fk, data_prod_fk
```
`params_hash` + `input_set_hash` allows detecting duplicate task submissions. Explicit input and output product tracking (separate tables, not a single role column).

**v3.x — Assoc-linked task.**

```
task
  pk, uid, assoc_fk (nullable), task_type, status
  started_at, completed_at, error_msg, meta JSON
  created_at, updated_at
```
Input/output products are implicit — they come from the linked `assoc` + `assoc_edge` rows (with roles "input"/"output"). Hash-based deduplication is absent.

**Trade-offs:**

| | v2.5 | v3.x |
|---|---|---|
| Hash-based idempotency | Yes | No |
| Explicit input tracking | Yes (`task_input` junction) | Implicit (via `assoc_edge`) |
| Explicit output tracking | Yes (`task_output` junction) | Implicit (via `assoc_edge`) |
| Separate input/output tables | Yes | No |
| Worker host tracking | Yes | No (would go in `meta` JSON) |

---

### 2.8 Enum Value Case Convention

| Aspect | v2.5 | v3.x |
|---|---|---|
| Enum base class | `str, Enum` | `StrEnum` |
| Values | UPPERCASE (`ACTIVE`, `QUEUED`, `PRIMARY`) | lowercase (`active`, `queued`, `primary`) |
| `DataKind` scope | Limited: VnaSweep, TargetSweep, Tune, RawTimeStream, LmtTel only | Extended: + D21, ReducedSweep, SolvedTimeStream, LmtTel2, Unknown, etc. |

---

## 3. Summary Table

| Design Dimension | v2.5 | v3.x |
|---|---|---|
| **Observation tracking** | Embedded in product meta JSON | Explicit `raw_obs` table + string FK |
| **Product type** | Normalized registry table | Inline string column |
| **Data kind** | Normalized registry + many-to-many junction | Inline string column (loses multi-kind for one product) |
| **Interface-level metadata** | Rich typed JSON on `DataProdSource` | Inline catalog columns on `data_prod` (no per-file metadata) |
| **File location** | URI-keyed `DataProdSource` + `Location` (with type + priority) | `file_record` (rel_path + int PK) + `storage_root` |
| **Telescope/pointing metadata** | Stored in `RawObsMeta` (JSON, indexed via DuckDB JSON functions) | Not stored (no dedicated columns) |
| **Associations** | Directed edge with typed registry | Group + edge (no registry, raw rule_name string) |
| **Process context on assoc** | Yes (typed `ProcessContext` JSON) | No |
| **Flag design** | Normalized registry (namespace + label) | Flat free-form strings |
| **Obs-level flags** | Not separate (use product flags) | Separate `obs_flag` table |
| **Task idempotency** | Yes (params_hash + input_set_hash) | No |
| **Explicit task I/O** | Yes (junction tables) | Implicit via assoc edges |
| **Table count** | 14 | 10 |
| **Schema complexity** | Higher (more joins needed) | Lower (direct column access) |
| **Type-safe Python metadata** | Strong (adaptix discriminated unions) | Weak (free-form `meta` dict) |
| **New product type fields** | Add fields to metadata dataclass + migrate JSON | Add nullable columns + migration |

---

## 4. Key v2.5 Strengths

1. **Typed structured metadata per product type.** `RawObsMeta` carries 20+ typed fields including all telescope state. Writing `product.meta.tau < 0.1` in Python is type-checked by mypy. The v3.x equivalent requires `product.meta["tau"]` with no type safety.

2. **Interface-level metadata is first-class.** `DataProdSource.meta` stores per-file roach and telescope metadata. This is the natural home for per-file data because each interface file independently has its own state (different roach numbers, per-file data kinds, per-file availability).

3. **URI-based source identity is self-describing.** `source_uri = "s3://tolteca-archive/toltec0_098765.nc"` is unambiguous. The v3.x `rel_path` + `storage_root` split is conceptually equivalent but requires a join to reconstruct the full path.

4. **Flag normalization allows reuse and categorization.** The `flag` registry with `namespace` enables queries like "show all products with any 'qa' namespace flag" without relying on string matching.

5. **Explicit task input/output tracking.** `task_input` and `task_output` junction tables make provenance unambiguous. In v3.x you'd have to infer task I/O from `assoc_edge.role`.

6. **Hash-based task idempotency** (`params_hash` + `input_set_hash`) prevents duplicate processing submissions.

---

## 5. Key v3.x Strengths

1. **`raw_obs` as first-class entity.** Observations exist before their files do. Obs-level attributes (timestamp, flags, metadata) belong to the obs row, not duplicated across 13+ product rows.

2. **ObsSpec UID as string PK.** `"tcs-98765-0-0"` as FK is directly human-readable in any SQL query or log.

3. **Inline catalog columns enable SQL-level filtering.** `WHERE interface = 'toltec0' AND data_kind = 'VnaSweep'` is fast without JSON extraction.

4. **Simpler schema** — fewer tables, fewer joins for common queries.

---

## 6. Open Questions for Design Iteration

1. **Should `raw_obs` exist as an explicit table?** (v3.x yes, v2.5 no) — v3.x approach seems clearly better here given the requirements ("dp_raw_obs are logical, immutable, exist a priori").

2. **Should product metadata use typed JSON (v2.5 approach) or inline columns (v3.x approach)?** v2.5 handles heterogeneous product types gracefully. v3.x needs many nullable columns or falls back to untyped JSON.

3. **Should interface metadata live on `data_prod` or on the file/source record?** v2.5 puts it on the source (`DataProdSource.meta`). v3.x puts it on `data_prod` as columns. The v2.5 location is semantically more correct — roach/tel state belongs to a specific file, not the abstract product.

4. **Should `data_prod_type` and `data_kind` be normalized registry tables or inline strings?** v2.5 normalizes both. v3.x inlines both.

5. **Should associations use directed edges (v2.5) or a group+edge model (v3.x)?** The v2.5 model is simpler. The v3.x group model adds score/status/lifecycle which may be useful.

6. **Should the flag system use a registry (v2.5) or free-form strings (v3.x)?**

7. **Should tasks have explicit input/output junction tables (v2.5) or rely on assoc edges (v3.x)?**
