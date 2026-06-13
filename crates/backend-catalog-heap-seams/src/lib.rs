//! Seam declarations for the `backend-catalog-heap` unit
//! (`catalog/heap.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::primitive::Oid;
use types_datum::Datum;
use types_error::PgResult;
use types_nodes::primnodes::OnCommitAction;
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
    /// `Datum reloptions`.
    pub reloptions: Datum,
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
