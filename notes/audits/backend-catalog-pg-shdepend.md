# Audit: backend-catalog-pg-shdepend

**Verdict: PASS**
Date: 2026-06-12
Model: claude-opus-4-8[1m]
Re-audit reason: `reconcile/access-model` reconciliation of the sys-scan model
(removed the `systable_scan_foreach[_recheckable]` seams + genam-seams
`SysScanRow`; replaced with the explicit `systable_beginscan`/`getnext`/
`recheck`/`endscan` + `SysScanGuard` iterator). shdepend re-ported onto the new
model with crate-local helpers, mirroring `backend-catalog-pg-depend` on this
branch. See "Access-model reconciliation" below; the function-by-function logic
audit (unchanged behavior) follows.

Unit: `backend-catalog-pg-shdepend`
C source: `src/backend/catalog/pg_shdepend.c` (PostgreSQL 18.3)
c2rust: `c2rust-runs/backend-catalog-pg-shdepend/src/pg_shdepend.rs`
Port: `crates/backend-catalog-pg-shdepend/src/lib.rs`
Seam crate: `crates/backend-catalog-pg-shdepend-seams/src/lib.rs`

Audit is independent: function inventory re-derived from the C source and
cross-checked against the c2rust rendering; each function compared C / c2rust /
Rust; constants verified against the PG 18.3 headers.

## Access-model reconciliation (delta re-audited from scratch)

The reconciliation removed the genam-seams `systable_scan_foreach` /
`systable_scan_foreach_recheckable` callback seams and the shared
`types_scan::backend_access_index_genam::SysScanRow` type. shdepend's scan
plumbing was rebuilt to the iterator model, identically to how
`backend-catalog-pg-depend` does it on this branch. The change is confined to
the scan plumbing and key construction — **no entry-point logic changed**.
Re-derived against `genam.c` and `pg_shdepend.c`:

- **Crate-local `SysScanRow<'a> { tid, values, isnull }`** (was the deleted
  shared genam-seams type). Identical shape to pg-depend's local row: `tid` =
  `tup->t_self` (used by the `catalog_tuple_delete`/`update` legs at
  `shdepChangeDep`/`shdepDropDependency`), `values`/`isnull` = the
  `heap_deform_tuple` projection. `form_pg_shdepend` reads it unchanged. MATCH.

- **Crate-local `systable_scan_foreach`** — `systable_beginscan(rel, indexId,
  true, NULL, nkeys, key)` then the `while ((tup = systable_getnext(scan)))`
  loop then `systable_endscan(scan)`, via the iterator seams behind a
  `SysScanGuard` (Drop = error-path `systable_endscan`; `scan.end()` = the
  success-path `systable_endscan` surfacing its error). Each row is deformed in
  a per-iteration scratch `MemoryContext`. `body` `Ok(true)`=continue (C loop
  iteration / `continue`), `Ok(false)`=`break`. Mirrors `genam.c` and pg-depend
  exactly. Every pg_shdepend column is fixed-width NOT NULL so by-value;
  by-ref is an error (cannot occur). MATCH.

- **Crate-local `systable_scan_foreach_recheckable`** — same loop, but `body`
  also gets a `recheck` closure = `systable_recheck_tuple(scan, tuple)`. In C
  (`pg_shdepend.c` 1436/1478) recheck is called once per candidate *after*
  `AcquireDeletionLock`, on the scan's current (most-recently-fetched) row;
  the closure calls the new `systable_recheck_tuple` seam on
  `scan.desc_mut()`, which is exactly that row (getnext is not re-invoked until
  the next loop turn). `Ok(false)` from recheck → caller releases the lock and
  `continue`s, matching C. MATCH.

- **New `systable_recheck_tuple` seam** added to
  `backend-access-index-genam-seams` — this is the real `genam.c:573`
  primitive (`systable_recheck_tuple(SysScanDesc, HeapTuple)`); the C `tup`
  argument is only an `Assert` against `sysscan->slot`, so the owned model
  passes only the scan descriptor. Owned by the (unported) genam unit; panics
  until that unit installs it — correct seam-and-panic, the recheck logic lives
  in genam, not here. Declaration-only, thin. OK.

- **`oid_key` / `int4_key`** now build an owned `ScanKeyData` via the
  reconciliation's `ScanKeyInit(&mut key, attno, BTEqualStrategyNumber,
  F_OIDEQ|F_INT4EQ, datum)` (`backend-access-common-scankey`), returning
  `PgResult` (eager `fmgr_info` resolution crosses the fmgr seam, exactly where
  C resolves the comparison proc). Was the old by-field `ScanKeyInit` struct.
  Same `BTEqualStrategyNumber` / `F_OIDEQ` / `F_INT4EQ` / argument datums; the
  reconciliation's `ScanKeyInit` sets `sk_collation = C_COLLATION_OID` (correct
  for all catalog columns, ignored by the rest) and `sk_subtype = InvalidOid`.
  Call sites thread the `?`. MATCH.

- **Call sites** — all 7 scan sites (6 `systable_scan_foreach`, 1
  `systable_scan_foreach_recheckable`) rewired from `genam_seams::*::call(...)`
  to the crate-local helpers; closures unchanged except `&mut |row|` → `|row|`
  (helper takes `impl FnMut`). The `&key[..nkeys]` slice for the
  `drop_subobjects` 3-key case is preserved. The `recheckable` closure still
  hands `recheck` to `shdepDropOwned_owner_branch` unchanged. MATCH.

- **Cargo.toml** — `+ backend-access-common-heaptuple` (heap_deform_tuple),
  `+ backend-access-common-scankey` (ScanKeyInit); `- types-cache` (the old
  `ScanKeyInit`/`BTEqualStrategyNumber` source, now unused). OK.

No other lines of the crate changed; the full per-function audit below holds.

## Function inventory and verdicts

| # | C function (line) | Port location | Verdict | Notes |
|---|-------------------|---------------|---------|-------|
| 1 | `recordSharedDependencyOn` (124) | `recordSharedDependencyOn` L202 | MATCH | Asserts subId==0 (debug_assert); bootstrap early-return via seam; pinned check; delegates insert. table_open/close span as Relation guard. |
| 2 | `recordDependencyOnOwner` (167) | L245 | MATCH | Builds two ObjectAddress, calls #1 with OWNER. |
| 3 | `shdepChangeDep` (205) | `shdepChangeDep` L267 | MATCH | LockAndCheck; 4-key scan on Depender index; deptype filter; multiple-match elog ERROR (same msg/format); pinned->delete-if-any; else update in place; else insert. Duplicate error raised after scan terminates - same SQLSTATE/text. |
| 4 | `changeDependencyOnOwner` (315) | L354 | MATCH | ChangeDep(OWNER) then DropDependency(ACL, drop_subobjects=true). |
| 5 | `recordDependencyOnTablespace` (369) | L401 | MATCH | ObjectAddressSet helper, calls #1 with TABLESPACE. |
| 6 | `changeDependencyOnTablespace` (390) | L424 | MATCH | newTablespace != DEFAULTTABLESPACE_OID(1663) && != InvalidOid -> ChangeDep(TABLESPACE); else DropDependency(INVALID, drop_subobjects=true). |
| 7 | `getOidListDiff` (420) | L467 | MATCH | Sorted-merge diff; in-place compaction; identical index arithmetic. Unit-tested. |
| 8 | `updateAclDependencies` (490) | L517 | MATCH | Delegates to Worker with SHARED_DEPENDENCY_ACL + ownerId. |
| 9 | `updateInitAclDependencies` (511) | L544 | MATCH | Worker with SHARED_DEPENDENCY_INITACL, ownerId=InvalidOid. |
| 10 | `updateAclDependenciesWorker` (524) | L569 | MATCH | getOidListDiff; if either >0 open rel; add new (skip owner for ACL, skip pinned); drop old (skip owner for ACL, skip pinned, exact objsubId); pfree -> Rust drop of consumed PgVecs. |
| 11 | `shared_dependency_comparator` (609) | L664 | MATCH | OID, classId, objsubid (as u32), deptype ordering. subId compared unsigned so 0 first. Stable sort vs qsort unobservable (total order on distinct rows). Unit-tested. |
| 12 | `checkSharedDependencies` (675) | L715 | MATCH | Pinned->ERROR (ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST); ref-index scan; local/shared->objects array, remote->remoteDep list (linear find, matches C List); sort if >1; MAX_REPORTED_DEPS=100 split into descs/alldescs; ngettext tails; empty descs->(false,None,None). |
| 13 | `copyTemplateDependencies` (894) | L909 | MATCH | max_slots = 65535/size_of(Form); batch multi-insert; dbid rewritten to newDbId; dbid=0 rows skipped by the dbid=template scan. See divisor note below - non-FAIL. |
| 14 | `dropDatabaseDependencies` (998) | L967 | MATCH | Delete all rows with dbid=databaseId; then DropDependency(DatabaseRelationId, drop_subobjects=true, INVALID). |
| 15 | `deleteSharedDependencyRecordsFor` (1046) | L1010 | MATCH | DropDependency with drop_subobjects=(objsubId==0). |
| 16 | `shdepAddDependency` (1068) | L1038 | MATCH | LockAndCheck ref; form tuple (dbid via classIdGetDbId); insert. |
| 17 | `shdepDropDependency` (1123) | L1074 | MATCH | 3 or 4 keys (drop_subobjects->nkeys=3, key[3] sliced off); filter refclassId/refobjId/deptype (OidIsValid / != INVALID); delete matches. |
| 18 | `classIdGetDbId` (1189) | L1131 | MATCH | IsSharedRelation->InvalidOid else MyDatabaseId. |
| 19 | `shdepLockAndCheckObject` (1210) | L1147 | MATCH | LockSharedObject(AccessShareLock).keep(); AUTHOID exists check; tablespace name; database name; default->ERROR. SQLSTATEs match (ERRCODE_UNDEFINED_OBJECT; default elog). |
| 20 | `storeObjectDescription` (1275) | L1196 | MATCH | getObjectDescription None->return (concurrent drop); newline separator; per-deptype messages; REMOTE ngettext; unrecognized->ERROR with `(int)deptype`. |
| 21 | `shdepDropOwned` (1341) | L1266 | MATCH | Pinned role->ERROR; ref-index recheckable scan; dbid filter; INVALID->ERROR; POLICY (RemoveRoleFromObjectPolicy, else lock+recheck+add); ACL (non-AuthMem->RemoveRoleFromObjectACL+continue, else fallthrough); OWNER branch; INITACL->RemoveRoleFromInitPriv. sort + performMultipleDeletions. |
| 21b| OWNER-case body (1463) | `shdepDropOwned_owner_branch` L1423 | MATCH | Extracted helper shared by ACL-fallthrough and OWNER; dbid==MyDatabaseId or classid==AuthMem -> lock+recheck+add. |
| 22 | `shdepReassignOwned` (1529) | L1460 | MATCH | Pinned role->ERROR; ref-index scan; dbid filter; OWNER->_Owner; INITACL->_InitAcl; ACL/POLICY/TABLESPACE no-op; default->ERROR; CommandCounterIncrement per row. Per-row AllocSet context is a documented no-op (Rust ownership). |
| 23 | `shdepReassignOwned_Owner` (1646) | L1549 | MATCH | classid switch over 23 catalog OIDs (all verified against headers); generic group->AlterObjectOwner_internal; default->ERROR. |
| 24 | `shdepReassignOwned_InitAcl` (1733) | L1611 | MATCH | ReplaceRoleInInitPriv delegate. |

Helpers (file-private types / inline): `SharedDependencyObjectType`,
`ShDependObjectInfo`, `remoteDep`/`RemoteDep`, `ObjectAddressSet`,
`form_pg_shdepend` (GETSTRUCT), `ngettext_format`/`ngettext_format_s`,
`oid_key`/`int4_key` (now build owned `ScanKeyData` via the reconciliation's
`ScanKeyInit` — see the reconciliation section), `open_shdepend`,
`desc_or_null`, and the crate-local `SysScanRow` /
`systable_scan_foreach` / `systable_scan_foreach_recheckable` iterator helpers
- all MATCH.

## Constants verified against headers

- `MAX_CATALOG_MULTI_INSERT_BYTES` = 65535 (`catalog/indexing.h:33`). OK
- `MAX_REPORTED_DEPS` = 100 (pg_shdepend.c `#define`). OK
- `Natts_pg_shdepend` = 7; attnos dbid=1,classid=2,objid=3,objsubid=4,
  refclassid=5,refobjid=6,deptype=7 (pg_shdepend.h). OK
- `SharedDependRelationId`=1214, `SharedDependDependerIndexId`=1232,
  `SharedDependReferenceIndexId`=1233 (pg_shdepend.h). OK
- deptype codes: OWNER='o', ACL='a', INITACL='i', POLICY='r', TABLESPACE='t',
  INVALID=0 (dependency.h). OK (asserted in unit test)
- Relation OIDs in `_Owner`: RelationRelationId=1259, TypeRelationId=1247,
  NamespaceRelationId=2615, AuthIdRelationId=1260, AuthMemRelationId=1261,
  TableSpaceRelationId=1213, DatabaseRelationId=1262, DefaultAclRelationId=826,
  UserMappingRelationId=1418, StatisticExtRelationId=3381, TSConfigRelationId=3602,
  TSDictionaryRelationId=3600, DEFAULTTABLESPACE_OID=1663. OK
- SQLSTATEs: ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST, ERRCODE_UNDEFINED_OBJECT;
  internal elog ERRORs default to XX000. OK

## Note: max_slots divisor (non-FAIL)

C computes `max_slots = MAX_CATALOG_MULTI_INSERT_BYTES / sizeof(FormData_pg_shdepend)`.
The C struct is 32 bytes (6xOid + int32 + char, padded). The Rust
`FormData_pg_shdepend` is a `#[derive]` struct without `#[repr(C)]`, so
`size_of` may differ (~28 bytes), yielding a larger `max_slots`. This changes
only the multi-insert *batch granularity*; `CatalogTuplesMultiInsertWithInfo`
produces identical catalog contents regardless of batch boundaries, and no
caller observes batch size. Behaviorally identical on every input - acceptable
idiomatic restructuring.

## Seam audit

Ownership by C-source coverage: this unit's only C file is `pg_shdepend.c`, so
the single owned seam crate is `backend-catalog-pg-shdepend-seams`.

- `backend-catalog-pg-shdepend-seams` declares 14 entry points
  (recordSharedDependencyOn, recordDependencyOnOwner, changeDependencyOnOwner,
  recordDependencyOnTablespace, changeDependencyOnTablespace,
  updateAclDependencies, updateInitAclDependencies, checkSharedDependencies,
  copyTemplateDependencies, dropDatabaseDependencies,
  deleteSharedDependencyRecordsFor, shdepLockAndCheckObject, shdepDropOwned,
  shdepReassignOwned).
- `init_seams()` (L1627) installs **all 14**, and nothing else. It contains only
  `set()` calls. `seams-init::init_all()` calls
  `backend_catalog_pg_shdepend::init_seams()` (seams-init/src/lib.rs:23). OK
- The command `*-seams` crates created alongside the port
  (policy/typecmds/schemacmds/foreigncmds/event-trigger/publicationcmds/
  subscriptioncmds/alter) map to *other* C files (policy.c, typecmds.c, ...),
  not pg_shdepend.c. They are **outward** seams this unit consumes across the
  ALTER-OWNER dependency cycle; ownership/installation belongs to their command
  units, not here. The pg_shdepend `init_seams()` correctly does not install
  them. Each is a thin macro declaration of the exact `Alter*Owner_oid` /
  `RemoveRole*` / `AlterObjectOwner_internal` entry points used - no logic.
- Every outward seam call (table open/close, genam scan, indexing insert/
  update/delete/multi-insert, catalog IsPinned/IsSharedRelation, lmgr
  LockSharedObject, syscache AUTHOID, dbcommands/tablespace name lookups,
  objectaddress getObjectDescription, dependency Acquire/Release/recheck/sort/
  performMultipleDeletions, xact CommandCounterIncrement, the ALTER-OWNER cmds)
  is justified by a real dependency cycle and is thin marshal+delegate. No
  branching/computation lives in a seam path - the policy/ACL/owner branch logic
  is all in this crate (`shdepDropOwned`, `shdepDropOwned_owner_branch`,
  `shdepReassignOwned_Owner`).

## Design conformance

- Allocating entry points (`checkSharedDependencies`, `updateAclDependencies`,
  `updateInitAclDependencies`) take `Mcx<'mcx>` and return `PgResult`; output
  strings carry the caller lifetime. Consumed `PgVec` inputs model the C pfree. OK
- Void scratch-only functions create a short-lived `MemoryContext::new` for the
  relcache-owned `Relation` carrier - same pattern as sibling
  `backend-catalog-pg-depend`. OK
- No invented opacity, no shared statics for per-backend globals (MyDatabaseId,
  bootstrap mode reached via seams), no ambient-global seams, no locks held
  across `?` without a guard (LockSharedObject result `.keep()`s the
  transaction-scoped lock; the Relation guard releases on `?`), no registry-shaped
  side tables, no unledgered divergence markers. OK

## Build / tests

On `reconcile/access-model` after the merge of main + this re-port:
`cargo check --workspace` - clean (only pre-existing unrelated warnings).
`cargo test --workspace` - clean, exit 0, zero failures.
`cargo test -p backend-catalog-pg-shdepend` - 8 passed (getOidListDiff,
comparator, ngettext, deptype chars, catalog OIDs).
`cargo test -p backend-access-index-genam-seams` - clean (new
`systable_recheck_tuple` seam compiles and declares cleanly).

## Verdict: PASS

Every function MATCH; all 14 seams installed by the owner's `init_seams()`,
which `seams-init::init_all()` invokes; outward seams are thin and justified;
no design-rule violations. The single divergence (multi-insert batch divisor) is
behaviorally identical on all inputs and not a FAIL.
