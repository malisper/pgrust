//! Inward seam installation for `backend-catalog-heap` (`catalog/heap.c`).
//!
//! Only the seams whose owner logic is fully landed in this crate are installed
//! here. `heap_create_with_catalog` is wired; the remaining inward seams
//! (`heap_drop_with_catalog`, `RemoveAttributeById`, `relation_clear_missing`,
//! `get_attr_default_oid`, `heap_create_with_catalog_transient`) are
//! deeper-keystone-blocked (see the crate-level STOP note) and remain
//! declared-but-uninstalled — a call panics loudly, the mirror-and-panic
//! posture, rather than running a stub.

extern crate alloc;

use backend_catalog_heap_seams::HeapCreateWithCatalogArgs;
use mcx::MemoryContext;
use types_error::PgResult;

use crate::heap_create_with_catalog;

/// Seam body for `heap_create_with_catalog` (the C signature, trimmed to the
/// fields the callers supply: NIL `cooked_constraints`, NULL `typaddress`).
/// The `reloptions` `RelOptionsToken` carries the already-built `text[]` varlena
/// image (`is_null` ⇒ SQL NULL).
fn heap_create_with_catalog_seam(args: HeapCreateWithCatalogArgs<'_>) -> PgResult<types_core::Oid> {
    let ctx = MemoryContext::new("heap_create_with_catalog");
    let mcx = ctx.mcx();

    let reloptions: Option<alloc::vec::Vec<u8>> = if args.reloptions.is_null {
        None
    } else {
        Some(args.reloptions.bytes.clone())
    };

    heap_create_with_catalog(
        mcx,
        &args.relname,
        args.relnamespace,
        args.reltablespace,
        args.relid,
        args.reltypeid,
        args.reloftypeid,
        args.ownerid,
        args.accessmtd,
        &args.tupdesc,
        args.relkind,
        args.relpersistence,
        args.shared_relation,
        args.mapped_relation,
        args.oncommit,
        reloptions,
        args.use_user_acl,
        args.allow_system_table_mods,
        args.is_internal,
        args.relrewrite,
    )
}

/// Seam body for `heap_drop_with_catalog(relid)` (dependency.c's `doDeletion`
/// `OCLASS_CLASS` relation arm). The inward seam carries no `mcx`, so allocate
/// a scratch context for the catalog scans / deletes.
fn heap_drop_with_catalog_seam(relid: types_core::Oid) -> PgResult<()> {
    let ctx = MemoryContext::new("heap_drop_with_catalog");
    crate::heap_drop_with_catalog(ctx.mcx(), relid)
}

/// Seam body for `RemoveAttributeById(relid, attnum)` (dependency.c's
/// `doDeletion` `OCLASS_CLASS` column arm). The inward seam carries no `mcx`.
fn remove_attribute_by_id_seam(relid: types_core::Oid, attnum: i32) -> PgResult<()> {
    let ctx = MemoryContext::new("RemoveAttributeById");
    crate::RemoveAttributeById(ctx.mcx(), relid, attnum as types_core::AttrNumber)
}

/// Seam body for `RelationClearMissing(rel)` (the ALTER ... DROP DEFAULT path).
fn relation_clear_missing_seam(rel: &types_rel::Relation<'_>) -> PgResult<()> {
    let ctx = MemoryContext::new("RelationClearMissing");
    crate::RelationClearMissing(ctx.mcx(), rel)
}

/// `init_seams()` — install the heap.c inward seams this crate owns. Wired into
/// the workspace `seams-init` aggregator.
pub fn init_seams() {
    backend_catalog_heap_seams::heap_create_with_catalog::set(heap_create_with_catalog_seam);
    backend_catalog_heap_seams::heap_drop_with_catalog::set(heap_drop_with_catalog_seam);
    // Low-level relation-create seams `index_create` (catalog/index.c) calls
    // directly. Their owner signatures match the seam signatures exactly, so
    // they install without a wrapper.
    backend_catalog_heap_seams::heap_create::set(crate::heap_create);
    backend_catalog_heap_seams::InsertPgClassTuple::set(crate::create::InsertPgClassTuple);

    // Attribute-mutate inward seams (dependency.c / ALTER paths). The bodies
    // are real; their writable-pg_attribute-carrier sub-seams panic until that
    // keystone lands (mirror-and-panic).
    backend_catalog_heap_seams::RemoveAttributeById::set(remove_attribute_by_id_seam);
    backend_catalog_heap_seams::relation_clear_missing::set(relation_clear_missing_seam);

    // Constraint-cooker outward seams the tablecmds CREATE-TABLE path consumes
    // (declared in `backend-commands-tablecmds-seams`, owned here). Signatures
    // match exactly, so they install without a wrapper.
    backend_commands_tablecmds_seams::add_relation_new_constraints::set(
        crate::AddRelationNewConstraints,
    );
    backend_commands_tablecmds_seams::add_relation_not_null_constraints::set(
        crate::AddRelationNotNullConstraints,
    );
}
