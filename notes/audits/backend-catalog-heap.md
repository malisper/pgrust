# backend-catalog-heap — PARTIAL (validation spine + relation-CREATE core landed; drop/constraint/partition/truncate STOP)

C source: `postgres-18.3/src/backend/catalog/heap.c` (4130 LOC, ~45 functions).

Builds on the K1/K2/K3 catalog-write carrier keystone (commit 8555b8130 / 6e18000d1):
the full-row INSERT carriers `PgClassInsertRow` / `PgAttributeInsertRow` (types-catalog)
+ producers `catalog_tuple_insert_pg_class` / `catalog_insert_pg_attribute_tuples`
(backend-catalog-indexing), and `RelationBuildLocalRelation` carrying the real
tupDesc + relam.

## What landed (faithful, 100% logic, no stubs)

### Validation + system-attribute spine (prior pass)

- `SystemAttributeDefinition` (heap.c:236), `SystemAttributeByName` (heap.c:248),
  the `SysAtt[]` table (ctid/xmin/cmin/xmax/cmax/tableoid), 1:1.
- `CheckAttributeNamesTypes` (heap.c:452), `CheckAttributeType` (heap.c:544) —
  full recursion (pseudo/domain/composite/range/array), all SQLSTATEs/messages.
- `RELKIND_HAS_{STORAGE,PARTITIONS,TABLESPACE,TABLE_AM}` + `RelFileNumberIsValid`
  + the `CHKATYPE_*` flag bits.

### Relation-CREATE core (this pass)

- `heap_create` (heap.c:285) — the catalog-namespace/toast permission guard
  (`IsCatalogNamespace`/`IsToastNamespace` + `IsNormalProcessingMode`), the
  `RELKIND_HAS_TABLESPACE`/`RELKIND_HAS_STORAGE` reltablespace/relfilenumber
  normalization, the `MyDatabaseTableSpace` hack, `RelationBuildLocalRelation`,
  the storage-create dispatch (`table_relation_set_new_filelocator` for table-AM
  relkinds returning the freeze/minmxid out-params, else
  `RelationCreateStorage` main fork), the no-storage `recordDependencyOnTablespace`,
  and `pgstat_create_relation`. The out-params `*relfrozenxid`/`*relminmxid`
  return as `HeapCreateXids` (idiomatic; the C return `Relation` is the new
  entry's Oid).
- `heap_create_with_catalog` (heap.c:1122) — full: sanity asserts,
  `CheckAttributeNamesTypes`, the duplicate-relation (`get_relname_relid`) and
  duplicate-type-name (`get_type_oid` + `moveArrayTypeName`) checks, the
  shared-relation pg_global check, OID allocation (incl. the full binary-upgrade
  toast/heap branch via `consume_next_pg_class_oid`/`consume_next_pg_class_relfilenumber`,
  else `GetNewRelFileNumber`), `LockRelationOid(AccessExclusiveLock)`, the
  `use_user_acl` default-ACL switch, `heap_create`, the rowtype-pg_type decision
  (`AssignTypeArrayOid` + `AddNewRelationType` + the array `TypeCreate` with the
  exact F_RECORD_*/F_ARRAY_* fmgr OIDs and constant args), `AddNewRelationTuple`,
  `AddNewAttributeTuples`, the dependency block (`recordDependencyOnOwner` /
  `recordDependencyOnNewAcl` / `recordDependencyOnCurrentExtension` / namespace +
  reloftype + access-method `add_exact_object_address` +
  `record_object_address_dependencies`), `InvokeObjectPostCreateHookArg`,
  `StoreConstraints` (NIL — no-op), and the `register_on_commit_action`.
  Returns the new relation OID. The C `cooked_constraints` / `typaddress`
  out-param are NIL/NULL at every current call site (the inward seam args carry
  neither), faithfully matching `StoreConstraints(NIL)` = nothing-to-do.
- `InsertPgClassTuple` (heap.c:910), `AddNewRelationTuple` (heap.c:984),
  `AddNewRelationType` (heap.c:1043), `AddNewAttributeTuples` (heap.c:834),
  `InsertPgAttributeTuples` (heap.c:717). The value layer is the K1/K2
  producers: each builds the typed `*InsertRow` from the live relcache `rd_rel`
  (read side) + the explicit write-only columns AddNewRelationTuple supplies,
  then calls the producer. `tupdesc_extra` is NULL at both pg_attribute call
  sites (attstattarget/attoptions inserted SQL NULL), exactly as C.
- delete family: `DeleteRelationTuple` (heap.c:1576) via
  `search_syscache_copy_pg_class` (t_self) + `catalog_tuple_delete`;
  `DeleteAttributeTuples` (heap.c:1605) / `DeleteSystemAttributeTuples`
  (heap.c:1642) / `RelationRemoveInheritance` (heap.c:1543) via
  `systable_beginscan`/`systable_getnext`/`catalog_tuple_delete` with the exact
  ScanKeys (BTEqual F_OIDEQ attrelid; the system-attr `BTLessEqual F_INT2LE 0`).

### Inward seam installed

`heap_create_with_catalog` (backend-catalog-heap-seams) is installed from
`init_seams()` (wired into `seams-init`). The seam body converts the
`RelOptionsToken` reloptions to the producer's `Option<Vec<u8>>` and threads the
args; it creates its own working `MemoryContext` (the result is just the Oid).

## Faithful-shape notes

- `CheckAttributeNamesTypes` takes `&[FormData_pg_attribute]` (the relcache
  `TupleDescData.attrs` slice), not a wrapped `TupleDesc`.
- relacl crosses as `Option<ArrayType>` (the C `Acl *`); the pg_class producer
  stores it at the repo's catalog array fidelity (the 16-byte `ArrayType`
  on-disk header — same level as pg_namespace.nspacl / pg_type.typacl via
  `arraytype_header_bytes`). `None` ⇒ SQL NULL.
- `AddNewRelationTuple`'s C `new_rel_desc->rd_att->tdtypeid = new_type_oid`
  relcache write is behaviour-preservingly omitted: the pg_class tuple is formed
  from the typed `PgClassInsertRow` (reltype = new_type_oid), and the following
  `AddNewAttributeTuples` reads only per-column `Form_pg_attribute`s, never
  `tdtypeid`; the next relcache rebuild reloads `tdtypeid` from the catalog. The
  trimmed relcache entry exposes no `tdtypeid` setter.
- New owner seams declared in their correct owners (not contract divergences —
  new seams for unported owners): `pgstat_create_relation` / `pgstat_drop_relation`
  (pgstat-seams), `register_on_commit_action` / `remove_on_commit_action`
  (tablecmds-seams), `consume_next_pg_class_oid` / `consume_next_pg_class_relfilenumber`
  (binary-upgrade-seams).

## STOP — deeper-keystone-blocked (inward seams left declared-but-uninstalled = mirror-and-panic)

These inward seams (`backend-catalog-heap-seams`) are NOT installed; a call
panics loudly (the correct mirror-and-panic posture for an unported owner),
never a stub:

- `heap_drop_with_catalog` (heap.c:1784) — needs a cluster of collaborators with
  no owner crate yet or no callable seam: `get_partition_parent` /
  `get_default_partition_oid` / `update_default_partition_oid` /
  `RemovePartitionKeyByRelId` (partition), `CheckTableForSerializableConflictIn`
  (predicate), `RemoveSubscriptionRel` (no seam crate), `RelationDropStorage` by
  handle, `RemoveStatistics`-by-relid, `pgstat_drop_relation` (declared here,
  uninstalled). The buildable leaves (DeleteAttributeTuples/DeleteRelationTuple/
  RelationRemoveInheritance/remove_on_commit_action) landed; the orchestrator does
  not, because the partition/subscription/predicate legs are absent.
- `RemoveAttributeById` (heap.c:1683) — needs a writable full-row
  `SearchSysCacheCopy2(ATTNUM)` (the syscache crate exposes only narrow
  pg_attribute projections), a pg_attribute `catalog_tuple_update` seam, and
  `RemoveStatistics`. None exist.
- `relation_clear_missing` (heap.c:1964) — same writable-ATTNUM-row +
  pg_attribute update primitives, absent.
- `get_attr_default_oid` (heap.c, GetAttrDefaultOid) — needs a pg_attrdef
  systable scan projection that is not yet declared.
- `heap_create_with_catalog_transient` (the cluster.c specialization) — depends
  on the same create core but takes an OldHeap `Relation`; deferred with the
  drop family.

Other unported heap.c families (no inward seam to install): the constraint-cooker
(`StoreAttrDefault` / `StoreRelCheck` / `AddRelationNewConstraints` /
`AddRelationNotNullConstraints` / `cookDefault` / `SetRelationNumChecks` /
`StoreAttrMissingVal` / `SetAttrMissing`) — needs the `CookedConstraint`/
`RawColumnDefault` node model + the parser default-cooker + `nodeToString` +
`construct_array`-of-missingval; the partition family (`StorePartitionKey` /
`RemovePartitionKeyByRelId` / `StorePartitionBound`) — partition-key node +
partcache owner; the truncate family (`heap_truncate` / `heap_truncate_one_rel`
/ `RelationTruncateIndexes` / `heap_truncate_check_FKs` / `heap_truncate_find_FKs`)
— table-AM truncate + the FK-scan engine.

## Unblocks

`index.c` #334 and `tablecmds`: `heap_create` / `heap_create_with_catalog` are
now callable (the inward `heap_create_with_catalog` seam is installed).

## Gate

`cargo check --workspace` green; no `todo!`/`unimplemented!`/owned-logic `panic!`;
`init_seams()` wired into `seams-init`; CONTRACT_RECONCILE_PENDING count unchanged
(28, matches origin/main — no contract divergences introduced).
