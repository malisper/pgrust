//! Seam declarations for the `backend-catalog-heap` unit (`catalog/heap.c`).
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

use mcx::Mcx;
use types_cluster::RelOptionsToken;
use types_core::primitive::{Oid, RelFileNumber, TransactionId};
use types_error::PgResult;
use types_nodes::primnodes::OnCommitAction;
use types_rel::Relation;
use types_tuple::heaptuple::TupleDescData;

/// Arguments to [`heap_create_with_catalog`], mirroring the C
/// `heap_create_with_catalog(...)` parameter list (catalog/heap.c) trimmed to
/// the fields the current callers supply. The C `TupleDesc tupdesc` crosses
/// by value as an owned [`TupleDescData`]; `cooked_constraints` is NIL and the
/// `ObjectAddress *typaddress` out-parameter is NULL at the current call
/// sites, so neither is carried.
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
        attnum: types_core::AttrNumber,
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

seam_core::seam!(
    /// `MergeWithExistingConstraint(rel, ccname, expr, ...)` (heap.c): the
    /// `pg_constraint` lookup + conflict checks + `conislocal` / `coninhcount`
    /// / `connoinherit` / `conenforced` field-update path. Blocked: needs a
    /// `conbin` reader (`fastgetattr` + `stringToNode` + `equal`) and an
    /// extended `pg_constraint` field-update carrier the typed catalog-write
    /// model has not assembled. `expr` is passed pre-flattened as its
    /// `nodeToString` image (`ccbin`) so the seam can compare against the
    /// stored `conbin`. Returns `true` if merged (duplicate). `Err` carries the
    /// conflict / mutation `ereport(ERROR)`s.
    pub fn merge_with_existing_constraint(
        relid: Oid,
        ccname: &str,
        ccbin: &str,
        allow_merge: bool,
        is_local: bool,
        is_enforced: bool,
        is_initially_valid: bool,
        is_no_inherit: bool,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `RemoveAttributeById`'s pg_attribute mutation (heap.c): `relation_open(
    /// relid, AccessExclusiveLock)` + `table_open(AttributeRelationId,
    /// RowExclusiveLock)` + `SearchSysCacheCopy2(ATTNUM)` + the GETSTRUCT field
    /// mutations (`attisdropped = true`, `atttypid = 0`, `attnotnull = false`,
    /// `attgenerated = 0`, rename to `........pg.dropped.N........`,
    /// `atthasmissing = false`) + nulling `attmissingval` / `attstattarget` /
    /// `attacl` / `attoptions` / `attfdwoptions` via `heap_modify_tuple` +
    /// `CatalogTupleUpdate` + `table_close`. Holds the AccessExclusiveLock on
    /// the owning relation until end of transaction (NoLock close). Blocked on
    /// the writable full-row `ATTNUM` syscache copy + `pg_attribute`
    /// `CatalogTupleUpdate` carrier. The `RemoveStatistics` half is real in the
    /// caller. `Err` carries the heap-mutation `ereport(ERROR)`s.
    pub fn remove_attribute_by_id_update(
        relid: Oid,
        attnum: types_core::AttrNumber,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `RelationClearMissing`'s pg_attribute mutation (heap.c): `table_open(
    /// AttributeRelationId, RowExclusiveLock)` + for each of the `natts`
    /// columns `SearchSysCache2(ATTNUM)`, and where `atthasmissing` is set,
    /// `heap_modify_tuple` clearing `atthasmissing` + nulling `attmissingval`
    /// + `CatalogTupleUpdate` + `table_close`. Blocked on the writable full-row
    /// `ATTNUM` syscache copy + `pg_attribute` `CatalogTupleUpdate` carrier.
    /// `Err` carries the heap-mutation `ereport(ERROR)`s.
    pub fn relation_clear_missing_update(relid: Oid, natts: i32) -> PgResult<()>
);

seam_core::seam!(
    /// `StoreAttrMissingVal`'s pg_attribute mutation (heap.c): `table_open(
    /// AttributeRelationId, RowExclusiveLock)` + `SearchSysCache2(ATTNUM)` +
    /// `construct_array(&missingval, 1, atttypid, attlen, attbyval, attalign)` +
    /// `heap_modify_tuple` setting `atthasmissing = true` + `attmissingval` +
    /// `CatalogTupleUpdate` + `table_close`. Blocked on the writable full-row
    /// `ATTNUM` syscache copy + `pg_attribute` `CatalogTupleUpdate` carrier (and
    /// `construct_array`). `missingval` is the single by-value/by-ref element.
    /// `Err` carries the heap-mutation `ereport(ERROR)`s.
    pub fn store_attr_missing_val(
        relid: Oid,
        attnum: types_core::AttrNumber,
        missingval: &types_tuple::backend_access_common_heaptuple::Datum<'_>,
    ) -> PgResult<()>
);

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
    /// relacl` / `Datum reloptions` cross as `Option<ArrayType>` / `Option<Vec<u8>>`
    /// (`None` is the C `(Datum) 0` → SQL NULL). Can `ereport(ERROR)`, carried
    /// on `Err`.
    pub fn InsertPgClassTuple<'mcx>(
        mcx: Mcx<'mcx>,
        pg_class_desc: &Relation<'mcx>,
        new_rel_desc: &Relation<'mcx>,
        new_rel_oid: Oid,
        write: &PgClassWriteFields,
        relacl: Option<types_array::ArrayType>,
        reloptions: Option<std::vec::Vec<u8>>,
    ) -> PgResult<()>
);
