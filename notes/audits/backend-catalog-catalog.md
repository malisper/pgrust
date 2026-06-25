# Audit: backend-catalog-catalog (`src/backend/catalog/catalog.c`)

**Verdict: PASS**
**Date:** 2026-06-13
**Model:** Claude Opus 4.8 (1M context) (`claude-opus-4-8[1m]`)

Independent function-by-function audit re-derived from the C source
(`postgres-18.3/src/backend/catalog/catalog.c`), the c2rust rendering
(`c2rust-runs/backend-catalog-small/src/catalog.rs`), and the Rust port
(`crates/backend-catalog-catalog/src/lib.rs`).

## 1. Function inventory

The C source defines 18 functions; the c2rust run lists all 18 as
`pub unsafe extern "C" fn` (matching — catalog.c has no `#if`-gated
definitions). `ProcNumberForTempRelations` is a `storage/procnumber.h` inline
helper used by `GetNewRelFileNumber`; the port reproduces it as a private fn.
`DatumGetObjectId`/`ObjectIdGetDatum`/`DatumGetPointer`/`DatumGetName`/`GETSTRUCT`
in c2rust are inlined macros, not catalog.c functions (the Datum/fmgr value layer
is the project-wide deferral).

| # | C function | C lines | Port location | Verdict | Notes |
|---|-----------|---------|---------------|---------|-------|
| 1 | `IsSystemRelation` | 73-77 | lib.rs:110 | MATCH | `IsSystemClassForm(rd_id, &rd_rel)`; reads owned `RelationData`, no catalog access (faithful to C comment). |
| 2 | `IsSystemClass` | 85-90 | lib.rs:125 (`IsSystemClass`) + :130 (`IsSystemClassForm`) | MATCH | `IsCatalogRelationOid(relid) || IsToastClass/IsToastNamespace(relnamespace)`. Split into a `PgClassForm` (standalone-tuple) face and a `FormData_pg_class` (`rd_rel`) face; both compute the identical predicate. Short-circuit order (`IsCatalogRelationOid` first) preserved. |
| 3 | `IsCatalogRelation` | 103-107 | lib.rs:145 | MATCH | `IsCatalogRelationOid(rd_id)`. |
| 4 | `IsCatalogRelationOid` | 120-137 | lib.rs:160 | MATCH | `relid < FIRST_UNPINNED_OBJECT_ID` (12000, verified vs c2rust). |
| 5 | `IsCatalogTextUniqueIndexOid` | 155-167 | lib.rs:173 | MATCH | 4-OID set: ParameterAclParname(6246), ReplicationOriginName(6002), SecLabelObject(3597), SharedSecLabelObject(3593). All verified vs c2rust. |
| 6 | `IsInplaceUpdateRelation` | 182-186 | lib.rs:187 | MATCH | `IsInplaceUpdateOid(rd_id)`. |
| 7 | `IsInplaceUpdateOid` | 192-197 | lib.rs:195 | MATCH | `relid == RELATION_RELATION_ID(1259) || relid == DATABASE_RELATION_ID(1262)`. |
| 8 | `IsToastRelation` | 205-217 | lib.rs:205 | MATCH | `IsToastNamespace(rd_rel.relnamespace)`. |
| 9 | `IsToastClass` | 225-231 | lib.rs:219 | MATCH | `IsToastNamespace(reltuple.relnamespace)`. |
| 10 | `IsCatalogNamespace` | 242-246 | lib.rs:229 | MATCH | `== PG_CATALOG_NAMESPACE` (11). |
| 11 | `IsToastNamespace` | 260-265 | lib.rs:242 | MATCH | `== PG_TOAST_NAMESPACE(99) || isTempToastNamespace(...)` — the temp-toast check crosses to ported namespace.c (`backend_catalog_namespace::isTempToastNamespace`), a direct in-tree call, not a seam. |
| 12 | `IsReservedName` | 277-284 | lib.rs:250 | MATCH | `pg_` prefix. C reads a NUL-terminated string `name[0..2]`; port guards `len() >= 3` first — identical on every input incl. "", "pg", "pg_". |
| 13 | `IsSharedRelation` | 303-359 | lib.rs:264 | MATCH | All three OID groups (11 catalogs, 21 indexes, 14 toast tables/indexes) compared one-for-one against c2rust; sample OIDs (AuthId 1260, TableSpace 1213, SharedDepend 1214, PgTablespaceToastIndex 4186, AuthIdOidIndexId 2677) verified. Group ordering and early `return true` per group preserved. |
| 14 | `IsPinnedObject` | 369-421 | lib.rs:333 | MATCH | 4 early-`false` guards (objectId>=FirstUnpinned; LargeObject(2613); Namespace(2615)+PG_PUBLIC(2200); Database(1262)) then `true`. Order and predicates exact. |
| 15 | `GetNewOidWithIndex` | 447-538 | lib.rs:380 | MATCH | Bootstrap-mode early return; do-while OID-probe loop via genam systable scan with SnapshotAny; exponential log-throttle (THRESHOLD 1_000_000, MAX_INTERVAL 128_000_000) with the `*2 <= MAX ? *2 : += MAX` rule; `retries++` then `if !collides break`; final `retries > THRESHOLD` completion log. `errmsg`/`errdetail_plural`/`errmsg_plural` strings + plural-count arg (`retries`) match. Asserts → `debug_assert!`. CHECK_FOR_INTERRUPTS → seam. |
| 16 | `GetNewRelFileNumber` | 556-631 | lib.rs:516 | MATCH | persistence switch (TEMP→ProcNumberForTempRelations; UNLOGGED/PERMANENT→INVALID_PROC_NUMBER; default→`elog(ERROR, "invalid relpersistence: %c")`); spcOid/dbOid derivation matching RelationInitPhysicalAddr; do-while with GetNewOidWithIndex-or-GetNewObjectId, relpath_backend + access(F_OK) collision test. Takes `Mcx` for the path alloc (§3b). |
| 17 | `pg_nextoid` | 640-711 | lib.rs:630 (`pg_nextoid`) + :666 (`pg_nextoid_inner`) | MATCH | superuser check (ERRCODE_INSUFFICIENT_PRIVILEGE); table_open/index_open (RowExclusiveLock, RAII close on success path); IsSystemRelation (ERRCODE_INVALID_PARAMETER_VALUE); indrelid mismatch; SearchSysCacheAttName (ERRCODE_UNDEFINED_COLUMN on miss); atttypid != OIDOID(26); `indnkeyatts != 1 || indkey.values[0] != attno`; `GetNewOidWithIndex(rel, idxoid, attno)` (port passes `idx.rd_id`, which equals idxoid since `idx = index_open(idxoid)` — behaviorally identical). fmgr arg/Datum layer is the project deferral. |
| 18 | `pg_stop_making_pinned_objects` | 719-735 | lib.rs:744 | MATCH | superuser check; `StopGeneratingPinnedObjectIds()` via varsup seam; returns `()` (PG_RETURN_VOID). |

All constants (OIDs, FirstUnpinnedObjectId, F_OIDEQ=184, BTEqualStrategyNumber=3,
RELPERSISTENCE 't'/'u'/'p', GLOBALTABLESPACE_OID=1664, TYPE_RELATION_ID=1247,
log thresholds) were verified against the c2rust-emitted preprocessor values, not
from memory. ERRCODE/severity (LOG=15, ERROR=21) match.

## 2. Seam audit

**Owned seam crate (by C-source coverage):** `backend-catalog-catalog-seams`
(maps to catalog.c).

Catalog.c-owned declarations — all 7 installed by `init_seams()`
(`backend_catalog_catalog::init_seams`, wired into `seams-init::init_all`):

- `is_pinned_object`, `is_catalog_relation_oid`, `is_catalog_relation`,
  `is_toast_relation`, `is_shared_relation` → set to the in-crate fns.
- `is_system_relation`, `is_system_class` → set to thin `Ok(..)` adapters
  (`is_system_relation_seam`/`is_system_class_seam`); the cross-crate carriers
  return `PgResult` (frozen caller contract), the catalog.c logic is infallible.
  Adapters are marshal-only — no branching/computation. OK.

`init_seams()` contains nothing but `set()` calls. No catalog.c `set()` lives
outside this crate.

**Outward seams added by this port (declarations in their true owners' crates,
installers belong to those units when they land — calling panics on an unported
callee, which is permitted):**

- `varsup-seams`: `get_new_object_id`, `stop_generating_pinned_object_ids`
  (varsup.c). Thin.
- `relpath-seams`: `relpath_backend` (relpath.c `relpathbackend` macro), takes
  `Mcx`, returns `PgString`. Thin. (§3b-clean.)
- `syscache-seams`: `search_syscache_attname` (SearchSysCacheAttName + GETSTRUCT
  projection → `(attnum, atttypid)`; `None` on miss). Thin.
- Existing seams reused: `genam-seams` (systable_beginscan/getnext/endscan),
  `fd-seams` (access_f_ok), `table-seams`/`indexam-seams` (open/close),
  `init-small-seams` (MyDatabaseId/MyDatabaseTableSpace/MyProcNumber/
  is_binary_upgrade), `parallel-rt-seams` (ParallelLeaderProcNumber),
  `miscinit-seams` (is_bootstrap_processing_mode), `superuser-seams`,
  `postgres-seams` (CHECK_FOR_INTERRUPTS). Each call is marshal + delegate.

No catalog.c function body was replaced by a seam to "somewhere else"; every
function's own control flow lives in this crate.

### Pre-existing note (not a catalog.c finding)

`backend-catalog-catalog-seams` also carries two declarations that are **not**
catalog.c functions and were parked here by earlier ports (relmapper.c commit
`edc7dec6`, inval.c commit `c5605dbb`), long before catalog.c was ported:

- `get_database_path` — `GetDatabasePath` is defined in `src/common/relpath.c`
  (verified). Owned by the relpath.c unit; a properly-shaped (`Mcx` + `PgString`)
  `get_database_path` already exists in `backend-common-relpath-seams`.
- `relation_invalidates_snapshots_only` — `RelationInvalidatesSnapshotsOnly` is
  defined in `src/backend/utils/cache/syscache.c` (verified). Owned by the
  syscache.c unit.

Per §3's ownership-by-C-source-coverage rule, these decls are owned by relpath.c
/ syscache.c, not catalog.c. The catalog.c port correctly installs only its own
7 seams and does not install these (their owners haven't landed; current callers
in relmapper/inval panic on the unported callee — allowed). Relocating these
decls to their true owners' seam crates (and consolidating the duplicate
`get_database_path` onto the canonical `Mcx` variant) is debt owned by the
relpath.c / syscache.c ports, to be discharged when they land; it would require
editing already-merged relmapper/inval units and is out of scope for the
catalog.c audit. Recorded here so it is not lost. This is not a catalog.c logic
or seam-ownership defect.

## 3b. Design conformance

- Allocating paths take `Mcx` and return `PgResult`: `GetNewRelFileNumber(mcx, ..)`,
  `relpath_backend(mcx, ..)`, `search_type_name`/`search_syscache_attname` (the
  pg_nextoid scratch). `pg_nextoid` opens a scratch `MemoryContext`. OK.
- No invented opacity: `Relation`/`RelationData`, `PgClassForm`,
  `FormData_pg_class`, `FormData_pg_index` (indrelid/indkey0) are the real types.
- No shared statics for per-backend globals (MyDatabaseId/MyProcNumber/Mode/
  binary-upgrade all reach their owners through seams).
- No locks held across `?`; no registry-shaped side tables; no unledgered
  divergence markers.

## 4. Result

Every catalog.c function is **MATCH**. Every catalog.c-owned seam declaration is
installed by `init_seams()`, which is `set()`-only and wired into
`init_all()`. No seam carries logic. `cargo test -p backend-catalog-catalog`:
9 passed. **PASS.**
