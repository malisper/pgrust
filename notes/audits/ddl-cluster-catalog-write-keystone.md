# DDL-cluster catalog-write keystone (K1/K2/K3)

Self-audit of the pg_class + pg_attribute + pg_attrdef catalog-write carriers,
INSERT producers, and `RelationBuildLocalRelation` — the CREATE TABLE/INDEX
equivalent of the RTE-seam keystone. Delivers the CARRIERS + INSERT PRODUCERS +
RelationBuildLocalRelation only; NOT heap.c/index.c (those re-fire after this).

## K1 — pg_class full-row INSERT carrier + producer

- `types-catalog/src/pg_class.rs` (NEW): `PgClassInsertRow` = the full 34-column
  `pg_class` row `InsertPgClassTuple` (catalog/heap.c) writes — every fixed
  column 1..=31 plus the nullable tail `relacl` (32, `Option<Vec<u8>>` aclitem[]
  image), `reloptions` (33, `Option<Vec<u8>>` text[] image), `relpartbound` (34,
  always NULL). Plus `RelationRelationId`/`ClassOidIndexId`/`ClassNameNspIndexId`/
  `ClassTblspcRelfilenodeIndexId` OIDs and `Anum_pg_class_*` / `Natts_pg_class=34`.
- DESIGN DECISION: the carrier is a DEDICATED full-row `*InsertRow` (the repo's
  established pattern: `PgConstraintInsertRow`, `PgCastInsertRow`, etc.), NOT a
  widening of the 66-reader relcache projection `types_rel::FormData_pg_class`.
  The relcache `rd_rel` C definition explicitly EXCLUDES the varlen tail
  (`/* NOTE: These fields are not present in a relcache entry's rd_rel field */`
  in pg_class.h), so adding `relacl`/`reloptions`/`relpartbound` there would be a
  contract divergence. The fixed columns the task lists (reltype/relam/.../
  relminmxid) ALREADY exist on the owned relcache entry `FormPgClass`
  (types-relcache-entry, 29 fields) which is the entry's real rd_rel mirror; the
  66 readers read the trimmed projection unchanged (ZERO reader edits).
- `backend-catalog-indexing/src/family3.rs` (NEW) `catalog_tuple_insert_pg_class`:
  `heap_form_tuple(RelationGetDescr(pg_class_desc), values, nulls)` +
  `CatalogTupleInsert`, building `values[]`/`nulls[]` from the row via the
  `Anum_pg_class_*` constants (field-for-field vs the C InsertPgClassTuple),
  `relpartbound` NULL. Installed via `family3::install()`.

## K2 — pg_attribute full-row INSERT carrier + bulk producer

- `types-catalog/src/pg_attribute.rs` (NEW): `PgAttributeInsertRow` = the full
  25-column `pg_attribute` row — fixed-layout 1..=20 + the nullable tail
  `attstattarget` (21, `Option<i16>`), `attoptions` (23, `Option<Vec<u8>>`);
  `attacl`/`attfdwoptions`/`attmissingval` always NULL ("not set for new
  columns"). Plus the catalog/index OIDs + `Anum_pg_attribute_*` /
  `Natts_pg_attribute=25`.
- `family3.rs` `catalog_insert_pg_attribute_tuples` = the bulk
  `InsertPgAttributeTuples` path: one heap tuple per row, then
  `CatalogOpenIndexes` + `CatalogTuplesMultiInsertWithInfo` + `CatalogCloseIndexes`
  (the C slot-batch + multi-insert; index-state lifecycle per-call — same index
  entries, logic-invisible, the precedent the multi-insert seams document).

## K3 — RelationBuildLocalRelation carries real tupDesc + relam

- `backend-utils-cache-relcache/src/initfile.rs`: widened
  `RelationBuildLocalRelation` to the full C signature — added `tup_desc:
  &TupleDescData<'mcx>` and `accessmtd: Oid`. The previously trimmed stub
  (`natts=0`, empty attrs, `constr: None`, `relam=InvalidOid`) now:
  - deep-copies the passed descriptor into `OwnedTupleDesc` (natts, tdtypeid,
    tdtypmod, per-column OwnedAttr rows copying attname/atttypid/attlen/attnum/
    atttypmod/attbyval/attalign/attnotnull/attidentity/attgenerated/attisdropped/
    attcollation + attnullability), the C `CreateTupleDescCopy` + the attr loop;
  - stamps `constr = Some(OwnedTupleConstr { has_not_null: true, .. })` when any
    column is NOT NULL (the C `if (has_not_null) { ... }`);
  - sets `relform.relnatts = natts`;
  - sets `rel.rd_rel.relam = accessmtd` (was hardcoded InvalidOid).
- `types-relcache-entry/src/lib.rs`: ADDITIVELY added `attidentity` / `attgenerated`
  to `OwnedAttr` (no literal constructors anywhere — Default-built — so additive)
  and propagated them in `build_form_attrs`.
- `FormData_pg_attrdef` + `Anum_pg_attrdef_*` + `Natts_pg_attrdef=4` +
  `PgAttrdefInsertRow` added (`types-catalog/src/pg_attrdef.rs`) + the
  `catalog_tuple_insert_pg_attrdef` producer (StoreAttrDefault's insert, returns
  the GetNewOidWithIndex-allocated OID).

## CONSTANT-TABLE AUDIT (mandatory; silent-corruption risk)

Verified every Anum constant against the C catalog struct field order (the
genbki Anum_* numbers are STRUCT-ordered, NOT heap.c-write-ordered):

- **pg_class** (34 cols): oid=1, relname=2, relnamespace=3, reltype=4,
  reloftype=5, relowner=6, relam=7, relfilenode=8, reltablespace=9, relpages=10,
  reltuples=11, relallvisible=12, relallfrozen=13, reltoastrelid=14,
  relhasindex=15, relisshared=16, relpersistence=17, relkind=18, relnatts=19,
  relchecks=20, relhasrules=21, relhastriggers=22, **relhassubclass=23,
  relrowsecurity=24, relforcerowsecurity=25**, relispopulated=26, relreplident=27,
  relispartition=28, relrewrite=29, relfrozenxid=30, relminmxid=31, relacl=32,
  reloptions=33, relpartbound=34. **BUG FOUND + FIXED**: the first draft had
  relrowsecurity=23/relforcerowsecurity=24/relhassubclass=25 (following heap.c's
  WRITE order, which differs from struct order). Corrected to struct order. The
  producer uses the named constants, so the fix propagates automatically.
- **pg_attribute** (25 cols): attrelid=1 .. attcollation=20, attstattarget=21,
  attacl=22, attoptions=23, attfdwoptions=24, attmissingval=25. CORRECT.
- **pg_attrdef** (4 cols): oid=1, adrelid=2, adnum=3, adbin=4. CORRECT.

## GATES

- `cargo check --workspace`: GREEN (0 errors).
- `cargo test -p no-todo-guard`: GREEN (no todo!/unimplemented! added).
- `cargo test -p seams-init` (both recurrence guards): GREEN —
  `every_declared_seam_is_installed_by_its_owner` confirms the 3 new DDL-cluster
  seams are installed by family3.
- CONTRACT_RECONCILE_PENDING: unchanged at 28 (work is purely additive — no
  value/handle contract divergences introduced; the producers cross owned
  `&Relation<'mcx>` + typed `*InsertRow`, the established F1 shape).

## heap.c + index.c #334 RE-FIREABLE

The three keystone prereqs are satisfied: the carriers + INSERT producers
(`catalog_tuple_insert_pg_class` / `catalog_insert_pg_attribute_tuples` /
`catalog_tuple_insert_pg_attrdef`) and the real-tupDesc/relam
`RelationBuildLocalRelation` are landed. heap.c (`heap_create_with_catalog`,
spine `CheckAttributeType` already on branch port/backend-catalog-heap) and
index.c (#334) can now form full relation+attribute rows.
