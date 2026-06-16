//! Seam declarations for the `backend-catalog-heap` unit (`catalog/heap.c`).
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

use mcx::Mcx;
use types_cluster::RelOptionsToken;
use types_core::primitive::Oid;
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
