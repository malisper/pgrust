# Audit: backend-catalog-pg-depend

- **Verdict: PASS**
- **Date:** 2026-06-12
- **Branch:** reconcile/access-model
- **Model:** Opus 4.8 (1M context)
- **C source:** `src/backend/catalog/pg_depend.c` (PostgreSQL 18.3), 1164 lines
- **Port:** `crates/backend-catalog-pg-depend/src/lib.rs`
- **c2rust:** `c2rust-runs/backend-catalog-pg-depend/src/pg_depend.rs`

Independent re-audit triggered because this unit's logic changed during the
`reconcile/access-model` reconciliation. Everything below is re-derived from the
C and the headers (not the port's comments). The reconciliation's substantive
change is the scan/relation model: the unit now drives the genuine genam
iterator seam (`systable_beginscan`/`systable_getnext`/`systable_endscan`)
through a local `systable_scan_foreach` adapter, and `getIdentitySequence`
takes the relation as the decided `&RelationData` carrier (reading
`rd_rel.relispartition` directly) instead of an opaque Oid hand-off.

## Function inventory (20 definitions)

C function-body starts confirmed at lines 50, 63, 197, 261, 305, 355, 402, 462,
569, 625, 712, 735, 781, 833, 902, 951, 1010, 1019, 1062, 1118 — 20 bodies,
matching the c2rust rendering (`pg_depend.rs`) and the catalog note's "20
functions".

| # | C function | C loc | Port loc | Verdict | Notes |
|---|------------|-------|----------|---------|-------|
| 1 | `recordDependencyOn` | 46 | lib.rs:194 | MATCH | delegates to `recordMultipleDependencies` with a 1-element slice (`from_ref`); C `nreferenced=1`. |
| 2 | `recordMultipleDependencies` | 58 | lib.rs:205 | MATCH | empty-slice early return (C `nreferenced<=0`); bootstrap early return via miscinit seam; `max_slots = min(len, MAX_CATALOG_MULTI_INSERT_BYTES/size_of::<FormData_pg_depend>())`; pinned-skip `continue`; flush at `max_slots` and flush remainder. C's slot-reuse bookkeeping (`slot_init_count`/`ExecClearTuple`/`ExecStoreVirtualTuple`) is a pure allocation optimization folded into the row accumulator; rows inserted and their per-batch grouping are identical. Tuple forming + index lifecycle owned by indexing seam. |
| 3 | `recordDependencyOnCurrentExtension` | 194 | lib.rs:308 | MATCH | `objectSubId==0` Assert -> debug_assert; `creating_extension` gate; isReplace branch: `getExtensionOfObject`, `OidIsValid`, equal-to-current early return, two distinct ereports (ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) with matching msg/detail; else records EXTENSION dep on ExtensionRelationId/CurrentExtensionObject. |
| 4 | `checkMembershipInCurrentExtension` | 259 | lib.rs:383 | MATCH | same gate; `getExtensionOfObject`, equal-to-current OK, else ereport with the distinct CREATE IF NOT EXISTS wording + detail. |
| 5 | `deleteDependencyRecordsFor` | 302 | lib.rs:431 | MATCH | RowExclusiveLock open; DependDependerIndexId scan on classid+objid; `skipExtensionDeps && deptype==DEPENDENCY_EXTENSION` -> skip (`continue`); else CatalogTupleDelete + count; returns i64 (C `long`). |
| 6 | `deleteDependencyRecordsForClass` | 352 | lib.rs:469 | MATCH | same scan; delete when `refclassid==refclassId && deptype==deptype`. |
| 7 | `deleteDependencyRecordsForSpecific` | 399 | lib.rs:504 | MATCH | same scan; delete when refclassid+refobjid+deptype all match. |
| 8 | `changeDependencyFor` | 458 | lib.rs:555 | MATCH | old/new pin checks before opening rel; both-pinned -> return 1; old-pinned/new-unpinned -> insert NORMAL dep, return 1; else scan DependDependerIndexId, for matching (refclassid,refobjid): newIsPinned -> delete, else update refobjid (modifiable copy -> CatalogTupleUpdate); count. TID passed is original row TID (C `heap_copytuple` preserves `t_self`). |
| 9 | `changeDependenciesOf` | 566 | lib.rs:648 | MATCH | scan DependDependerIndexId; every row -> copy, set objid=newObjectId, update, count. |
| 10 | `changeDependenciesOn` | 622 | lib.rs:685 | MATCH | old-pinned -> ereport ERRCODE_FEATURE_NOT_SUPPORTED; newIsPinned computed; scan DependReferenceIndexId on refclassid+refobjid; newIsPinned -> delete else update refobjid; count. |
| 11 | `isObjectPinned` (static) | 710 | lib.rs:759 | SEAMED | one-line delegate to `IsPinnedObject(classId,objectId)` via catalog seam (infallible bool); subId ignored exactly as C. |
| 12 | `getExtensionOfObject` | 733 | lib.rs:776 | MATCH | AccessShareLock; DependDependerIndexId; first row with refclassid==ExtensionRelationId && deptype==EXTENSION -> result, break (`Ok(false)`). |
| 13 | `getAutoExtensionsOfObject` | 779 | lib.rs:807 | MATCH | AccessShareLock; collect refobjid where refclassid==ExtensionRelationId && deptype==AUTO_EXTENSION into PgVec (C `lappend_oid`, NIL==empty vec). |
| 14 | `getExtensionType` | 831 | lib.rs:854 | MATCH | DependReferenceIndexId 3-key scan (refclassid=ExtensionRelationId, refobjid=extensionOid, refobjsubid=0); for classid==TypeRelationId && deptype==EXTENSION: syscache TYPEOID typname lookup folded into `search_type_name` seam (None == invalid tuple `continue`), match strcmp, set result + break. |
| 15 | `sequenceIsOwned` | 900 | lib.rs:898 | MATCH | out-params -> `Option<(Oid,i32)>`; AccessShareLock; DependDependerIndexId classid=RelationRelationId+objid=seqId; first row refclassid==RelationRelationId && deptype==deptype -> Some((refobjid,refobjsubid)), break. |
| 16 | `getOwnedSequences_internal` (static) | 949 | lib.rs:928 | MATCH | DependReferenceIndexId; 3rd key (refobjsubid=attnum) present only when attnum!=0 (`&key[..]` vs `&key[..2]`, mirroring C `attnum ? 3 : 2`); predicate classid==RelationRelationId && objsubid==0 && refobjsubid!=0 && (deptype AUTO||INTERNAL) && get_rel_relkind(objid)==RELKIND_SEQUENCE; then `!deptype || deptype match` -> append objid. |
| 17 | `getOwnedSequences` | 1008 | lib.rs:980 | MATCH | `getOwnedSequences_internal(relid,0,0)`. |
| 18 | `getIdentitySequence` | 1017 | lib.rs:988 | MATCH | `relid=RelationGetRelid`; partition branch reads `rd_rel.relispartition`, `get_partition_ancestors`, `get_attname(relid,attnum,false)` (before relid reassignment, matching C), `relid=llast_oid`, `get_attnum`, InvalidAttrNumber -> elog; `getOwnedSequences_internal(...,DEPENDENCY_INTERNAL)`; len>1 -> elog "more than one"; empty -> missing_ok?InvalidOid:elog "no owned sequence"; else `linitial_oid`. |
| 19 | `get_index_constraint` | 1060 | lib.rs:1045 | MATCH | DependDependerIndexId 3-key (classid=RelationRelationId, objid=indexId, objsubid=0); first row refclassid==ConstraintRelationId && refobjsubid==0 && deptype==INTERNAL -> result, break. |
| 20 | `get_index_ref_constraints` | 1116 | lib.rs:1084 | MATCH | DependReferenceIndexId 3-key (refclassid=RelationRelationId, refobjid=indexId, refobjsubid=0); collect objid where classid==ConstraintRelationId && objsubid==0 && deptype==NORMAL. |

Local non-C helpers (`open_depend`, `oid_key`, `int4_key`, `systable_scan_foreach`,
`form_pg_depend`, `name_or_null`, `SysScanRow`): pure idiomatic adapters
mirroring `table_open`/`ScanKeyInit`/the genam scan loop/`GETSTRUCT`/PG snprintf
NULL rendering. No business logic — verified against C lines 85, 314-324, 326,
378.

## Constants verified against headers

- `DEPEND_RELATION_ID`=2608, `DependDependerIndexId`=2673, `DependReferenceIndexId`=2674
  — `catalog/pg_depend.h:42,74,75`.
- `MAX_CATALOG_MULTI_INSERT_BYTES`=65535 — `catalog/indexing.h:33`.
- `DependencyType`: NORMAL='n', AUTO='a', INTERNAL='i', EXTENSION='e',
  AUTO_EXTENSION='x' — `catalog/dependency.h:33-39`. Asserted in unit tests and
  matched.
- `BTEqualStrategyNumber`=3, `F_OIDEQ`=184, `F_INT4EQ`=65; `Anum_pg_depend_*`
  1..7, `Natts_pg_depend`=7; `RELATION_RELATION_ID`=1259, `TYPE_RELATION_ID`=1247,
  `CONSTRAINT_RELATION_ID`=2606, `EXTENSION_RELATION_ID`=3079,
  `RELKIND_SEQUENCE`='S', `AccessShareLock`=1, `RowExclusiveLock`=3. All
  asserted in-crate and confirmed.

## Seam audit

Owned seam crate (pg_depend.c is the sole c_source): `backend-catalog-pg-depend-seams`.
It declares all 18 public functions; `init_seams()` (lib.rs:1125) contains
nothing but the 18 `set()` calls covering every declaration; `seams-init::init_all()`
calls `backend_catalog_pg_depend::init_seams()` (seams-init/src/lib.rs:24).
No uninstalled declaration, no `set()` outside the owner. No empty installer.

Outward seam calls — each justified by a real cross-unit dependency, thin
marshal+delegate, no business logic in the seam path:

- genam (`systable_beginscan`/`getnext`/`endscan`) — the decided sys-scan
  iterator; the in-crate loop/predicate/break logic lives in this crate.
- indexing (`catalog_tuple_delete`, `catalog_tuple_update_pg_depend`,
  `catalog_tuples_multi_insert_pg_depend`) — CatalogTupleDelete/Update and the
  CatalogOpenIndexes/MultiInsertWithInfo/CloseIndexes batch. pg_depend rows
  cross as the deformed `FormData_pg_depend` projection. Index-state lifecycle
  is per-batch in the seam vs. once-across-batches in C: logic-invisible
  (identical catalog state; only index open/close overhead differs), ledgered in
  the seam doc.
- table (`table_open`/`close` via `OpenRelation` guard), catalog
  (`is_pinned_object`), miscinit (`is_bootstrap_processing_mode`), extension
  (`creating_extension`/`current_extension_object`/`get_extension_name`),
  objectaddress (`get_object_description`), partition (`get_partition_ancestors`),
  lsyscache (`get_rel_relkind`/`get_attname`/`get_attnum`), syscache
  (`search_type_name` folding SearchSysCache1(TYPEOID)+NameStr). All declarations
  confirmed present in their seam crates with matching signatures.

`getExtensionType`'s `search_type_name` seam folds the syscache lookup + typname
extraction; the strcmp/match/break decision remains in-crate (lib.rs:877-880) —
no decision logic crossed the seam.

## Design conformance

- Allocating functions/seams carry `Mcx` + return `PgResult`; List*-of-Oid ->
  `PgVec<'mcx, Oid>` in the caller's mcx; `long` -> i64; out-params ->
  `Option<(Oid,i32)>`. Conforms.
- No invented opacity: `RelationData` is the decided real carrier (rd_rel Form),
  not a stand-in handle; the reconciliation moved the relation hand-off onto it
  and reads `relispartition` directly. Conforms (types.md rules 6-7).
- No shared statics for per-backend globals, no ambient-global seams introduced
  here, no locks held across `?` without the `OpenRelation` guard (the guard's
  Drop is the error-path `table_close`), no registry-shaped side tables. Conforms.
- ereport mappings: ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE (x3),
  ERRCODE_FEATURE_NOT_SUPPORTED (x1), and the elog ERRORs in getIdentitySequence
  map with matching severity/message text. PG snprintf `(null)` rendering for a
  NULL `get_extension_name` is preserved (`name_or_null`).

## Build / test

`cargo build -p backend-catalog-pg-depend` clean; `cargo test -p
backend-catalog-pg-depend` — 3 passed (constant/vocabulary asserts), 0 failed.

## Conclusion

All 20 C functions MATCH (one static SEAMED per step-3 rules). Every owned-seam
declaration installed; all outward seams justified and thin; constants verified
against headers; design rules satisfied. **PASS.**
