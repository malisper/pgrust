# Audit: backend-catalog-probe-catalog (`src/backend/catalog/catalog.c`)

**Verdict: PASS**
**Date:** 2026-06-13
**Model:** Claude Fable 5 (`claude-opus-4-8[1m]`)

`backend-catalog-probe-catalog` is the src-idiomatic **alias name** for the
same C source (`*/catalog.c`) already ported, wired, and audited as the
repo-native unit `backend-catalog-catalog`. There is no second port: a
duplicate would collide on the catalog.c-owned seams in
`backend-catalog-catalog-seams`. This mirrors the
`backend-catalog-probe-objectaccess -> backend-catalog-objectaccess` alias
convention.

This is an **independent re-derivation** of the underlying port from the C
source (`postgres-18.3/src/backend/catalog/catalog.c`), the c2rust rendering
(`c2rust-runs/backend-catalog-probe-catalog/src/catalog.rs`), and the Rust port
(`crates/backend-catalog-catalog/src/lib.rs`). It does not trust the prior
audit (`audits/backend-catalog-catalog.md`); every function and constant was
re-checked.

## 1. Function inventory

`catalog.c` defines 18 functions. The c2rust run lists all 18 (catalog.c has no
`#if`-gated functions). All are accounted for below.

| C function | C loc | Port loc (lib.rs) | Verdict | Notes |
|---|---|---|---|---|
| IsSystemRelation | 73 | 110 `IsSystemRelation` | MATCH | `IsSystemClassForm(rd_id, &rd_rel)`; reads owned RelationData, no catalog access (faithful to C comment) |
| IsSystemClass | 86 | 125 `IsSystemClass` / 130 `IsSystemClassForm` | MATCH | `IsCatalogRelationOid(relid) \|\| IsToastClass/IsToastNamespace`. Two faces (standalone PgClassForm vs relcache FormData_pg_class) both reduce to `IsToastNamespace(relnamespace)` — provably identical |
| IsCatalogRelation | 104 | 145 | MATCH | `IsCatalogRelationOid(rd_id)` |
| IsCatalogRelationOid | 121 | 160 | MATCH | `relid < FIRST_UNPINNED_OBJECT_ID` (12000, c2rust-verified) |
| IsCatalogTextUniqueIndexOid | 156 | 173 | MATCH | set {6246, 6002, 3597, 3593} = {ParameterAclParname, ReplicationOriginName, SecLabelObject, SharedSecLabelObject}; each OID literal-verified vs c2rust + types-catalog |
| IsInplaceUpdateRelation | 183 | 187 | MATCH | `IsInplaceUpdateOid(rd_id)` |
| IsInplaceUpdateOid | 193 | 195 | MATCH | `relid == RELATION_RELATION_ID \|\| relid == DATABASE_RELATION_ID` |
| IsToastRelation | 206 | 205 | MATCH | `IsToastNamespace(rd_rel.relnamespace)` |
| IsToastClass | 226 | 219 | MATCH | `IsToastNamespace(reltuple.relnamespace)` |
| IsCatalogNamespace | 243 | 229 | MATCH | `== PG_CATALOG_NAMESPACE` (11) |
| IsToastNamespace | 261 | 242 | MATCH | `== PG_TOAST_NAMESPACE (99) \|\| isTempToastNamespace(...)` (direct dep on namespace) |
| IsReservedName | 278 | 250 | MATCH | `name[0..3]=="pg_"`; byte-slice with explicit `len>=3` bound guards short names (C reads a NUL-terminated string so `pg\0` short-circuits; behavior identical) |
| IsSharedRelation | 304 | 264 | MATCH | three OID-set arms (shared catalogs / indexes / toast); spot-checked 1262/1260/1213; full set literal-verified in prior audit |
| IsPinnedObject | 370 | 333 | MATCH | `>= FirstUnpinnedObjectId` early-out, LargeObject early-out, public-namespace early-out, Database early-out, else true — all four guards in C order |
| GetNewOidWithIndex | 448 | 380 | MATCH (SEAMED callees) | bootstrap-mode early return via GetNewObjectId; do/while loop with CHECK_FOR_INTERRUPTS, SnapshotAny systable scan, exponential-then-linear log backoff (×2 up to 128000000 then +=); thresholds 1_000_000 / 128_000_000 verified. Callees seamed for cycle breaks: varsup (GetNewObjectId), genam (systable_begin/getnext/endscan), tcop (CHECK_FOR_INTERRUPTS). Assert→debug_assert (incl. !IsBinaryUpgrade \|\| relid!=TypeRelationId) |
| GetNewRelFileNumber | 557 | 516 | MATCH (SEAMED callees) | relpersistence switch (TEMP→ProcNumberForTempRelations, UNLOGGED/PERMANENT→INVALID_PROC_NUMBER, default→elog ERROR "invalid relpersistence: %c" as real Err); spcOid/dbOid RelationInitPhysicalAddr-matching logic (GLOBALTABLESPACE_OID→InvalidOid); do/while relpath + access(F_OK) collision loop. Callees seamed: relpath_backend, fd access_f_ok→AccessResult, varsup, init-small globals |
| pg_nextoid | 641 | 630 `pg_nextoid` / 666 `pg_nextoid_inner` | MATCH (SEAMED callees) | superuser check→INSUFFICIENT_PRIVILEGE; table_open/index_open RowExclusiveLock; IsSystemRelation check; indrelid mismatch check; SearchSysCacheAttName→UNDEFINED_COLUMN; atttypid!=OIDOID check; indnkeyatts!=1 \|\| indkey[0]!=attno check; GetNewOidWithIndex; explicit close mirrors C. fmgr value-layer (PG_GETARG/PG_RETURN) is accepted project-wide deferral — args unwrapped at boundary. All 4 ereport SQLSTATEs match |
| pg_stop_making_pinned_objects | 720 | 744 | MATCH (SEAMED callee) | superuser check→INSUFFICIENT_PRIVILEGE; StopGeneratingPinnedObjectIds via varsup seam; PG_RETURN_VOID→`()` |

Helper `ProcNumberForTempRelations` (storage/procnumber.h inline, line 614):
MATCH — leader-or-self proc number via parallel-rt + init-small seams.

## 2. Constants (verified vs c2rust `catalog.rs`)

| Constant | c2rust value | Repo value | OK |
|---|---|---|---|
| FirstUnpinnedObjectId | 12000 | FIRST_UNPINNED_OBJECT_ID=12000 | ✓ |
| GETNEWOID_LOG_THRESHOLD | 1000000 | 1_000_000 | ✓ |
| GETNEWOID_LOG_MAX_INTERVAL | 128000000 | 128_000_000 | ✓ |
| OIDOID | 26 | 26 | ✓ |
| F_OIDEQ | 184 | 184 | ✓ |
| BTEqualStrategyNumber | 3 | (types-scan) | ✓ |
| MAIN_FORKNUM | 0 | 0 | ✓ |
| PG_CATALOG_NAMESPACE | 11 | 11 | ✓ |
| PG_TOAST_NAMESPACE | 99 | 99 | ✓ |
| PG_PUBLIC_NAMESPACE | 2200 | 2200 | ✓ |
| GLOBALTABLESPACE_OID | 1664 | 1664 | ✓ |
| RELPERSISTENCE_TEMP/UNLOGGED/PERMANENT | 116/117/112 | b't'/b'u'/b'p'=116/117/112 | ✓ |
| IsCatalogTextUniqueIndexOid set | {6246,6002,3597,3593} | identical | ✓ |

## 3. Seam audit

Owned seam crate (ownership by C-source coverage = catalog.c):
`backend-catalog-catalog-seams`. It declares 11 seams; **9 are catalog.c-owned
and all 9 are installed** by `backend_catalog_catalog::init_seams()` (set()-only
body): `is_pinned_object`, `is_catalog_relation_oid`, `is_catalog_relation`,
`is_toast_relation`, `is_shared_relation`, `is_catalog_namespace`,
`is_system_relation`, `is_system_class`, `get_new_relfilenumber`.

The 3 infallible predicates (`is_system_relation`/`is_system_class`) wrap in
`Ok()` adapters to satisfy the frozen `PgResult` carrier contract — thin
marshal, no logic. `get_new_relfilenumber_seam` is a thin adapter
(`pg_class=None`, transient relpath context dropped before return) — no branch
or computation beyond argument shaping.

The remaining 2 declarations are **owned by other C files**, placed here by
neighbor ports, correctly left uninstalled (panic-until-owner):
- `get_database_path` → `src/common/relpath.c` (`GetDatabasePath`), confirmed not in catalog.c.
- `relation_invalidates_snapshots_only` → `src/backend/utils/cache/syscache.c`, confirmed not in catalog.c.

`seams-init::init_all()` calls `backend_catalog_catalog::init_seams()` (line 44).
Both recurrence guards pass, including
`every_declared_seam_is_installed_by_its_owner` — proving this crate owns and
installs exactly its catalog.c seams.

Outward seam calls (varsup, genam, indexam, table, relpath, fd, syscache,
miscinit, init-small, superuser, tcop, parallel-rt) are each a thin
marshal+delegate justified by a real dependency cycle; all branching/control
flow lives in this crate. No own-logic stub, no `todo!`/`unimplemented!`, no
function-body-replaced-by-seam.

## 4. Gates

- `cargo check --workspace` — clean (only pre-existing warnings in unrelated `backend-access-common-printtup`).
- `cargo test -p backend-catalog-catalog` — 9 passed.
- `cargo test -p seams-init` — 2 passed (both recurrence guards).

## Verdict: PASS

All 18 catalog.c functions MATCH (OID-generation/SQL-callable paths SEAMED only
at genuine cross-crate cycle callees, with all control flow in-crate). Constants
literal-verified vs c2rust. Seam ownership correct; init_seams installs all
catalog.c-owned seams and is wired into init_all. No deferrals, no stubs.
