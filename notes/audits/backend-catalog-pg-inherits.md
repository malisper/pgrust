# Audit: backend-catalog-pg-inherits

Independent function-by-function audit of the port at
`crates/backend-catalog-pg-inherits/src/lib.rs` (+ `crates/types-catalog/src/pg_inherits.rs`)
against `postgres-18.3/src/backend/catalog/pg_inherits.c`. Re-derived from the C
and the c2rust translation; port comments/self-review not trusted.

## Per-function table

| C function (pg_inherits.c) | Port location | Verdict | Notes |
|---|---|---|---|
| `oid_cmp` (static) | `find_inheritance_children_extended` sort | MATCH | Subsumed into `oidarr.sort_unstable()` (ascending OID, deadlock-avoidance order); guarded by `len()>1`, exactly the C `numoids>1` `qsort`. |
| `find_inheritance_children` | `find_inheritance_children` | MATCH | Delegates to `_extended(true, lockmode, None, None)`. |
| `find_inheritance_children_extended` | same | MATCH | `has_subclass` short-circuit → empty; scan `InheritsParentIndexId` on `inhparent`; detach-pending → `*detached_exist`, `omit_detached && ActiveSnapshotSet` → `XidInMVCCSnapshot(xmin, GetActiveSnapshot)`; newer-xmin tracking + `WARNING "more than one partition pending detach found for table with OID %u"` + `TransactionIdFollows`; `continue` skip. qsort-by-OID. Per-child `LockRelationOid` + `SearchSysCacheExists1(RELOID)` double-check; useless lock `release()`. `repalloc`/maxoids=32 correctly elided. |
| `find_all_inheritors` | `find_all_inheritors` | MATCH | BFS over single growing list used as seen-set + agenda (index walk, not fetch-ahead). `seen_rels` HashMap oid→list_index; first-seen push numparents=1, re-seen bump. numparents returned as optional tuple element (C out-`List **`). Root numparents=0. |
| `has_subclass` | `has_subclass` | MATCH | `search_pg_class_full_form`(RELOID) → `relhassubclass`; `None` → `elog(ERROR) "cache lookup failed for relation %u"`. |
| `has_superclass` | `has_superclass` | MATCH | Scan `InheritsRelidSeqnoIndexId` on `inhrelid`; result = first tuple exists. |
| `typeInheritsFrom` | `typeInheritsFrom` | MATCH | `typeOrDomainTypeRelid`/`typeidTypeRelid` InvalidOid short-circuits; `has_subclass(super)` short-circuit; BFS with `visited` skip; `inhparent == superclassRelid` → result + break. |
| `StoreSingleInheritance` | `StoreSingleInheritance` | MATCH (insert SEAMED) | `RowExclusiveLock`; `PgInheritsInsertRow{..,inhdetachpending:false}`; `heap_form_tuple`+`CatalogTupleInsert` delegated to `indexing_seams::catalog_tuple_insert_pg_inherits` (declared, uninstalled = panic-until; gated on catalog-indexing insert owner). |
| `DeleteInheritsTuple` | `DeleteInheritsTuple` | MATCH (delete SEAMED) | Scan on `inhrelid`; `!OidIsValid(inhparent) || parent==inhparent`; both ereports `ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE` with exact errmsg/errdetail/errhint + `childname ? : "unknown relation"`. `CatalogTupleDelete` via `catalog_tuple_delete` (uninstalled = panic-until). |
| `PartitionHasPendingDetach` | `PartitionHasPendingDetach` | MATCH | `RowExclusiveLock`; first tuple returns `inhdetachpending` (closes first); no-row → `elog(ERROR) "relation %u is not a partition"`; C's intentional no-`table_close` on the error path = RAII drop (abort-time release). |
| `TransactionIdFollows` (helper) | helper | MATCH | `(id1.wrapping_sub(id2) as i32) > 0` (access/transam.h); unit-tested incl. wraparound. |

## Constants
`InheritsRelationId=2611`, `InheritsParentIndexId=2187`, `InheritsRelidSeqnoIndexId=2680`,
`Anum inhrelid=1 / inhparent=2 / inhseqno=3 / inhdetachpending=4`, `Natts=4`,
`F_OIDEQ=184`, `BTEqualStrategyNumber=3`, `AccessShareLock=1`, `RowExclusiveLock=3`,
`NoLock=0`. All verified against pg_inherits.h / fmgroids / lockdefs; asserted by
the `catalog_constants_match_headers` test.

## Seams / wiring
- Inward seams (`backend-catalog-pg-inherits-seams`): `find_all_inheritors`,
  `type_inherits_from` — both installed by `init_seams()` (registered in
  `seams-init::init_all`). Consumers: cluster, coerce.
- Outward, installed by their owners: snapmgr `active_snapshot_set` /
  `get_active_snapshot` / `xid_in_mvcc_snapshot` (the last added + installed in
  this change); syscache `search_pg_class_full_form` /
  `search_syscache_exists_reloid`; lmgr `lock_relation_oid` (LockGuard).
- `catalog_tuple_insert_pg_inherits` (added to indexing-seams) +
  `catalog_tuple_delete`: declared, uninstalled — correct seam-and-panic gated
  on the catalog-indexing insert owner. The recurrence guard
  (`seams-init` test `every_declared_seam_is_installed_by_its_owner`) passes,
  confirming these are correctly exempt (unported owner).

## Design conformance
- mcx/PgVec/PgResult on all allocating fns; bare `Vec` only for the transient
  `oidarr` working array (dropped before return).
- LockGuard discipline correct: `release()` on the vanished-relation path,
  `keep()` for hold-to-xact-end; `?` before either relies on Drop = release
  (= C abort cleanup). No lock held across `?` without a guard.
- No invented opacity; real typed row carriers. No ambient-global seam misuse
  (active-snapshot reads are snapmgr's own state, behind snapmgr-owned seams).

## Verdict: PASS

All functions faithfully ported. The two uninstalled catalog-write seams are
legitimate keystone-gated panic-until seams, not divergences.
