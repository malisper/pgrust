# Audit: backend-utils-adt-amutils

Port of `src/backend/utils/adt/amutils.c` (PostgreSQL 18.3) — SQL-level APIs
related to index access methods. New unit (owner created); seam crate
`backend-utils-adt-amutils-seams`.

Self-audited function-by-function against
`postgres-18.3/src/backend/utils/adt/amutils.c`. Result: 100% logic ported, no
`todo!`/`unimplemented!`, no own-logic stubs.

## Function coverage

| C function | Rust | Notes |
|---|---|---|
| `am_propnames[]` table | `AM_PROPNAMES` | All 18 entries, 1:1 ordering. |
| `lookup_prop_name` | `lookup_prop_name` | Case-insensitive scan; unknown → `Unknown` (no error). |
| `pg_strcasecmp` | `pg_strcasecmp` (inline leaf) | ASCII fold, NUL-terminated semantics modeled (shorter prefix sorts first). |
| `test_indoption` | `test_indoption` | `guard=false`→`Some(false)`; else bit test; NULL→`None`. |
| `indexam_property` | `indexam_property` | Full decision tree, all `AMPROP_*` arms, attno range checks, iskey/nonkey, RETURNABLE generic fallback. |
| `pg_indexam_has_property` | same | `indexam_property(prop, amoid, InvalidOid, 0)`. |
| `pg_index_has_property` | same | `indexam_property(prop, InvalidOid, relid, 0)`. |
| `pg_index_column_has_property` | same | `attno <= 0` early NULL, then column path. |
| `pg_indexam_progress_phasename` | same | int8→int32 truncation reproduced exactly (`PG_GETARG_INT32` on an int8 datum). |

## Seam surface (every external call-out)

amutils.c is a thin SQL wrapper over the index-AM layer + catalog. Six outward
seams in `backend-utils-adt-amutils-seams`:

* `index_relation`, `index_form` — `SearchSysCache1(RELOID/INDEXRELID)`
  projections (relkind/relam/relnatts; indexrelid/indnatts/indnkeyatts/indoption).
  Installed by the **syscache owner** (`backend-utils-cache-syscache`), reading
  the catcache directly + decoding the `indoption int2vector` via a new
  `int2vector_to_i16s_bytes` arrayfuncs seam (mirrors the existing
  `oidvector_to_oids_bytes`).
* `am_routine` — `GetIndexAmRoutineByAmId(amoid, noerror=true)` projected to the
  scalar capability flags + `routine->amX != NULL` "callback present" booleans.
  Installed by the **amapi owner** (`backend-access-index-amapi`). `noerror=true`
  → `Ok(None)` on missing AM/handler (faithful to amutils's silent-NULL).
* `am_property` — the AM's `amproperty` callback, dispatched by AM OID by name
  (the unified `IndexAmRoutine` vtable does NOT carry `amproperty` — same model
  as `amvalidate`/`amadjustmembers`). Reaches the real `btproperty` /
  `gistproperty` / `spgproperty`. Other AMs assign `amproperty = NULL` in C and
  the caller gates on `has_amproperty`, so they are unreachable here.
* `index_can_return` — the generic `AMPROP_RETURNABLE` fallback:
  `index_open(AccessShareLock)` → `index_can_return` → `index_close`. Installed
  by amapi via the landed `backend-access-index-indexam`.
* `am_buildphasename` — the AM's `ambuildphasename` callback, dispatched by AM
  OID by name. Reaches `btbuildphasename` / `ginbuildphasename` (the only AMs
  assigning it non-NULL in C).

The `has_amproperty` / `has_ambuildphasename` booleans are derived per AM OID in
the amapi installer, matching the C `bthandler`/`gisthandler`/`spghandler`/
`ginhandler`/`brinhandler`/`hashhandler` assignments
(btree: property+phasename; gist/spgist: property; gin: phasename; brin/hash:
neither).

## Model reconciliations vs C / src-idiomatic

* The repo `IndexAmRoutine` vtable carries the capability flags but NOT the
  `amproperty` / `ambuildphasename` callbacks (trimmed; reached by name as
  `amvalidate` is). Resolved by per-AM-OID dispatch in the amapi installer
  rather than a vtable fn-ptr — faithful to how the repo already handles
  `amvalidate`.
* `index_open`/`index_can_return` need an `Mcx` (no ambient context in this
  repo), so `indexam_property` and the four public functions take `mcx: Mcx`
  (C's implicit `CurrentMemoryContext`). The `am_property` seam likewise takes
  `Mcx` because gist/spgist `amproperty` do catalog lookups + allocate.
* SQL arg unmarshalling (`PG_GETARG_*`, `text_to_cstring`,
  `CStringGetTextDatum`) is the bare-word `PGFunction` registry boundary
  (deferred); functions take/return already-unmarshalled scalars.

## Tests

12 in-crate parity tests reproduce the golden truth tables from
`src/test/regress/expected/amutils.out` (btree/gist column, index-level,
AM-level, multi-column `fooindex`, covering `foocover` INCLUDE, attno range,
missing index/AM, progress phasename + int8→int32 truncation). All pass.

No CONTRACT_RECONCILE_PENDING introduced. `cargo check --workspace`,
`no-todo-guard`, `seams-init` all green.
