//! Seam declarations for the `backend-catalog-heap` unit (`catalog/heap.c`).
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

use mcx::{Mcx, PgVec};
use ::types_cluster::RelOptionsToken;
use ::types_core::primitive::{Oid, RelFileNumber, TransactionId};
use ::types_error::PgResult;
use ::nodes::nodes::NodePtr;
use ::nodes::primnodes::OnCommitAction;
use ::rel::Relation;
use ::types_tuple::heaptuple::TupleDescData;

/// Arguments to [`heap_create_with_catalog`], mirroring the C
/// `heap_create_with_catalog(...)` parameter list (catalog/heap.c) trimmed to
/// the fields the current callers supply. The C `TupleDesc tupdesc` crosses
/// by value as an owned [`TupleDescData`]; `cooked_constraints` is carried as
/// a `PgVec` of `Node::Constraint` carriers (the C `cooked_constraints` list).
/// The `ObjectAddress *typaddress` out-parameter is NULL at the current call
/// sites, so it is not carried.
#[derive(Debug)]
pub struct HeapCreateWithCatalogArgs<'mcx> {
    /// `const char *relname`.
    pub relname: std::string::String,
    /// `Oid relnamespace`.
    pub relnamespace: Oid,
    /// `Oid reltablespace`.
    pub reltablespace: Oid,
    /// `Oid relid`.
    pub relid: Oid,
    /// `Oid reltypeid`.
    pub reltypeid: Oid,
    /// `Oid reloftypeid`.
    pub reloftypeid: Oid,
    /// `Oid ownerid`.
    pub ownerid: Oid,
    /// `Oid accessmtd`.
    pub accessmtd: Oid,
    /// `TupleDesc tupdesc`.
    pub tupdesc: TupleDescData<'mcx>,
    /// `char relkind`.
    pub relkind: u8,
    /// `char relpersistence`.
    pub relpersistence: u8,
    /// `bool shared_relation`.
    pub shared_relation: bool,
    /// `bool mapped_relation`.
    pub mapped_relation: bool,
    /// `OnCommitAction oncommit`.
    pub oncommit: OnCommitAction,
    /// `Datum reloptions` — the opaque `bytea`/varlena reloptions token round-
    /// tripped from the parent's pg_class row; the catalog owner forwards it
    /// into the toast-table catalog entry. (See [`RelOptionsToken`].)
    pub reloptions: RelOptionsToken,
    /// `bool use_user_acl`.
    pub use_user_acl: bool,
    /// `bool allow_system_table_mods`.
    pub allow_system_table_mods: bool,
    /// `bool is_internal`.
    pub is_internal: bool,
    /// `Oid relrewrite`.
    pub relrewrite: Oid,
    /// `List *cooked_constraints` — pre-cooked CHECK constraints and column
    /// defaults inherited from parent relations (the C
    /// `list_concat(cookedDefaults, old_constraints)`). Each element is a
    /// `Node::Constraint` carrier (CONSTR_CHECK / CONSTR_DEFAULT), consumed by
    /// `StoreConstraints`. Empty at non-inheriting call sites.
    pub cooked_constraints: PgVec<'mcx, NodePtr<'mcx>>,
}

seam_core::seam!(
    /// `heap_create_with_catalog(relname, ...)` (catalog/heap.c): create the
    /// catalog entries for a new relation and return its OID. `Err` carries
    /// the catalog-mutation / validation `ereport(ERROR)`s and OOM.
    pub fn heap_create_with_catalog<'mcx>(
        args: HeapCreateWithCatalogArgs<'mcx>,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `GetAttrDefaultOid(relid, attnum)` (catalog/heap.c): scan `pg_attrdef`
    /// for the default-expression row of column `attnum` on relation `relid`,
    /// returning the `pg_attrdef` OID (`InvalidOid` if none). Used by
    /// `get_object_address`'s `OBJECT_DEFAULT` arm. Can `ereport(ERROR)`,
    /// carried on `Err`.
    pub fn get_attr_default_oid(
        relid: Oid,
        attnum: ::types_core::AttrNumber,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `heap_create_with_catalog(...)` (heap.c) as specialized for the cluster
    /// transient heap: the NewHeap clones OldHeap's tuple descriptor, owner,
    /// AM, persistence, mapped-ness and reloptions, with `relid = OIDOldHeap`
    /// passed for the relrewrite/identity bookkeeping. Returns the new OID.
    pub fn heap_create_with_catalog_transient<'mcx>(
        mcx: Mcx<'mcx>,
        new_heap_name: &str,
        namespaceid: Oid,
        new_tablespace: Oid,
        owner: Oid,
        new_access_method: Oid,
        old_heap: &Relation<'_>,
        relpersistence: u8,
        mapped: bool,
        reloptions: RelOptionsToken,
        old_heap_oid: Oid,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `RelationClearMissing(rel)` (heap.c).
    pub fn relation_clear_missing(rel: &Relation<'_>) -> PgResult<()>
);

seam_core::seam!(
    /// `SystemAttributeByName(attname)` (catalog/heap.c): the system column
    /// (`ctid`/`xmin`/`cmin`/`xmax`/`cmax`/`tableoid`) named `attname`, or
    /// `None` if the name is not a system attribute. Projects the fields a
    /// field-reference resolver reads off the `FormData_pg_attribute`:
    /// `(attnum, atttypid, atttypmod, attcollation)`. Consumed by
    /// expandedrecord.c's `expanded_record_lookup_field`.
    pub fn system_attribute_by_name(attname: &str) -> Option<(i32, Oid, i32, Oid)>
);

seam_core::seam!(
    /// `RemoveAttributeById(relid, attnum)` (catalog/heap.c): the per-class
    /// `OCLASS_CLASS` column-drop handler dependency.c's `doDeletion` invokes
    /// for a `pg_attribute` (table-column) object. Marks the column dropped.
    /// Can `ereport(ERROR)`, carried on `Err`.
    pub fn RemoveAttributeById(relid: Oid, attnum: i32) -> PgResult<()>
);

seam_core::seam!(
    /// `heap_drop_with_catalog(relid)` (catalog/heap.c): the per-class
    /// `OCLASS_CLASS` relation-drop handler dependency.c's `doDeletion` invokes
    /// for an ordinary table/relation object. Removes the relation and its
    /// catalog rows. Can `ereport(ERROR)`, carried on `Err`.
    pub fn heap_drop_with_catalog(relid: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `DeleteRelationTuple(relid)` (catalog/heap.c): remove the relation's
    /// `pg_class` row. Used by `index_drop`'s catalog cleanup. The C allocates
    /// in `CurrentMemoryContext`, so the seam carries no `mcx`. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn DeleteRelationTuple(relid: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `DeleteAttributeTuples(relid)` (catalog/heap.c): remove the relation's
    /// `pg_attribute` rows. Used by `index_drop`'s catalog cleanup. The C
    /// allocates in `CurrentMemoryContext`, so the seam carries no `mcx`. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn DeleteAttributeTuples(relid: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `RemoveStatistics(relid, attnum)` (catalog/heap.c): remove `pg_statistic`
    /// rows for a relation (`attnum == 0`) or one column. Used by `index_drop`
    /// for expressional indexes. The C allocates in `CurrentMemoryContext`, so
    /// the seam carries no `mcx`. Can `ereport(ERROR)`, carried on `Err`.
    pub fn RemoveStatistics(relid: Oid, attnum: i16) -> PgResult<()>
);

seam_core::seam!(
    /// `CheckAttributeNamesTypes(tupdesc, relkind, flags)` (catalog/heap.c):
    /// validate a candidate `TupleDesc`'s column names (no duplicates / system
    /// names) and datatypes (no disallowed pseudo-types) for a relation of the
    /// given `relkind`. `flags` is the `CHKATYPE_*` bitmask. Consumed by the
    /// `addRangeTableEntryForFunction` RECORD-coldeflist arm (parse_relation.c)
    /// — catalog/heap.c sits above parse_relation.c, so the call crosses the
    /// owner seam. The descriptor crosses by reference; catalog-heap reads its
    /// `attrs`. `Err` carries the validation `ereport(ERROR)` surface.
    pub fn check_attribute_names_types<'mcx>(
        mcx: Mcx<'mcx>,
        tupdesc: &TupleDescData<'mcx>,
        relkind: u8,
        flags: i32,
    ) -> PgResult<()>
);

/* ===========================================================================
 * Constraint-cooker / attribute-mutate sub-seams: the catalog-write carriers
 * the typed model has not yet assembled. `backend-catalog-heap` drives these
 * from its real `SetRelationNumChecks` / `MergeWithExistingConstraint` /
 * `RemoveAttributeById` / `RelationClearMissing` / `StoreAttrMissingVal`
 * bodies; each panics loudly (mirror-and-panic) until the carrier lands.
 * ========================================================================= */

seam_core::seam!(
    /// `SetRelationNumChecks`'s disk-store branch (heap.c): `table_open(
    /// RelationRelationId, RowExclusiveLock)` + `SearchSysCacheCopy1(RELOID)` +
    /// `relStruct->relchecks = numchecks` + `CatalogTupleUpdate` + `table_close`.
    /// Blocked: the trimmed `PgClassForm` carries no `relchecks` field and the
    /// typed catalog-write model exposes no pg_class relchecks-set carrier. The
    /// caller has already read the current value and confirmed it differs from
    /// `numchecks` (the `relchecks == numchecks` `CacheInvalidate` branch is
    /// real in the caller). `Err` carries the heap-mutation `ereport(ERROR)`s.
    pub fn set_relation_num_checks(relid: Oid, numchecks: i32) -> PgResult<()>
);

// `MergeWithExistingConstraint`'s `pg_constraint` lookup + conflict checks +
// `conislocal`/`coninhcount`/`connoinherit`/`conenforced`/`convalidated`
// field-update path is now REAL in the pg_constraint owner crate
// (`pg_constraint::MergeWithExistingConstraint`), which
// `backend-catalog-heap` calls directly, so the former
// `merge_with_existing_constraint` mirror-and-panic seam is gone.

// `RemoveAttributeById`'s pg_attribute mutation is now REAL in-crate
// (`backend-catalog-heap` constraints.rs: keyed `systable_beginscan` on the
// `AttributeRelidNumIndexId` over `attrelid = relid`, match `attnum`, then
// `heap_modify_tuple` setting `attisdropped = true` / `atttypid = 0` /
// `attnotnull = false` / `attgenerated = 0` / rename to
// `........pg.dropped.N........` / `atthasmissing = false` + nulling
// `attmissingval` / `attstattarget` / `attacl` / `attoptions` /
// `attfdwoptions`, then `CatalogTupleUpdate`), so the former
// `remove_attribute_by_id_update` mirror-and-panic seam is gone.

// `RelationClearMissing`'s pg_attribute mutation is now REAL in-crate
// (`backend-catalog-heap` constraints.rs: systable scan on pg_attribute by
// `attrelid` + `heap_modify_tuple` + `CatalogTupleUpdate`), so the former
// `relation_clear_missing_update` mirror-and-panic sub-seam is gone.

// `StoreAttrMissingVal`'s pg_attribute mutation is now REAL in-crate
// (`backend-catalog-heap` constraints.rs: keyed `systable_beginscan` on the
// `AttributeRelidNumIndexId` over `attrelid = relid`, match `attnum`, then
// `construct_array(&missingval, 1, atttypid, attlen, attbyval, attalign)` +
// `heap_modify_tuple` setting `atthasmissing = true` + `attmissingval`, then
// `CatalogTupleUpdate`), so the former `store_attr_missing_val`
// mirror-and-panic seam is gone.

/* ===========================================================================
 * Low-level relation-create seams `index_create` (catalog/index.c) calls
 * directly (it does NOT go through `heap_create_with_catalog`): `heap_create`
 * creates the uncataloged index relcache entry and `InsertPgClassTuple`
 * registers its pg_class row. (`RelationBuildLocalRelation`, which
 * `heap_create` itself calls, is owned by the relcache and seamed there.)
 * ========================================================================= */

/// The frozen-xid / min-mxid `heap_create` writes through its
/// `relfrozenxid` / `relminmxid` out-parameters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HeapCreateXids {
    /// `*relfrozenxid`.
    pub relfrozenxid: TransactionId,
    /// `*relminmxid` (the underlying `uint32` `MultiXactId`).
    pub relminmxid: u32,
}

/// The created relation plus the frozen-xid out-parameters `heap_create`
/// computes (in C, `rel` is the return value and the xids are written through
/// out-params).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HeapCreateResult {
    /// The new relcache entry's OID (`heap_create`'s return `Relation`).
    pub rel: Oid,
    /// The frozen-xid / min-mxid out-parameters.
    pub xids: HeapCreateXids,
}

/// The pg_class columns that `AddNewRelationTuple` writes onto `rd_rel` just
/// before `InsertPgClassTuple` (the ones the trimmed relcache
/// `FormData_pg_class` does not carry, or that `AddNewRelationTuple`
/// overrides). Carried explicitly so `InsertPgClassTuple` is a faithful image
/// of every value the C scribbles on `new_rel_desc->rd_rel`.
#[derive(Clone, Copy, Debug)]
pub struct PgClassWriteFields {
    pub relpages: i32,
    pub reltuples: f32,
    pub relallvisible: i32,
    pub relallfrozen: i32,
    pub relfrozenxid: TransactionId,
    pub relminmxid: u32,
    pub relowner: Oid,
    pub reltype: Oid,
    pub reloftype: Oid,
    pub relispartition: bool,
    pub relrewrite: Oid,
}

seam_core::seam!(
    /// `CheckAttributeType(attname, atttypid, attcollation, containing_rowtypes,
    /// flags)` (catalog/heap.c): verify a type is safe to store in a table /
    /// index column (rejects pseudo-types such as anonymous `record`, walks
    /// composite/array element types, checks collation derivability). As called
    /// by `catalog/index.c` `ConstructTupleDescriptor` for an expression index
    /// column: `containing_rowtypes = NIL`, `flags = 0`. Can `ereport(ERROR)`,
    /// carried on `Err`.
    pub fn check_attribute_type(
        attname: &str,
        atttypid: Oid,
        attcollation: Oid,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `heap_create(relname, relnamespace, reltablespace, relid, relfilenumber,
    /// accessmtd, tupDesc, relkind, relpersistence, shared_relation,
    /// mapped_relation, allow_system_table_mods, &relfrozenxid, &relminmxid,
    /// create_storage)` (catalog/heap.c): create an uncataloged relation. In C
    /// the return is the new `Relation` and the frozen xids are written through
    /// the two out-parameters; the owned model returns the new relcache entry's
    /// OID plus the xids in [`HeapCreateResult`]. Can `ereport(ERROR)`, carried
    /// on `Err`.
    pub fn heap_create<'mcx>(
        mcx: Mcx<'mcx>,
        relname: &str,
        relnamespace: Oid,
        reltablespace: Oid,
        relid: Oid,
        relfilenumber: RelFileNumber,
        accessmtd: Oid,
        tup_desc: &TupleDescData<'_>,
        relkind: u8,
        relpersistence: u8,
        shared_relation: bool,
        mapped_relation: bool,
        allow_system_table_mods: bool,
        create_storage: bool,
    ) -> PgResult<HeapCreateResult>
);

seam_core::seam!(
    /// `InsertPgClassTuple(pg_class_desc, new_rel_desc, new_rel_oid, relacl,
    /// reloptions)` (catalog/heap.c): construct and insert a new pg_class tuple.
    /// Tuple data is taken from `new_rel_desc->rd_rel` plus the write-only
    /// columns (`write`) that `AddNewRelationTuple` scribbles on `rd_rel` but
    /// the trimmed relcache `FormData_pg_class` does not carry. The C `Datum
    /// relacl` crosses as the full on-disk `aclitem[]` varlena image
    /// (`Option<&[u8]>`); `Datum reloptions` as `Option<Vec<u8>>` (`None` is
    /// the C `(Datum) 0` → SQL NULL). Can `ereport(ERROR)`, carried on `Err`.
    pub fn InsertPgClassTuple<'mcx>(
        mcx: Mcx<'mcx>,
        pg_class_desc: &Relation<'mcx>,
        new_rel_desc: &Relation<'mcx>,
        new_rel_oid: Oid,
        write: &PgClassWriteFields,
        relacl: Option<&[u8]>,
        reloptions: Option<std::vec::Vec<u8>>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `CopyStatistics(fromrelid, torelid)` (catalog/heap.c): copy every
    /// `pg_statistic` row from `fromrelid` to `torelid` (used by
    /// `index_concurrently_swap` to carry an index's stats to its rebuilt copy).
    /// `Err` carries the catalog-mutation `ereport(ERROR)`s.
    pub fn copy_statistics<'mcx>(
        mcx: Mcx<'mcx>,
        fromrelid: Oid,
        torelid: Oid,
    ) -> PgResult<()>
);
