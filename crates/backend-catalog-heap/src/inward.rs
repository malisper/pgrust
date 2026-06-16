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

/// `init_seams()` — install the heap.c inward seams this crate owns. Wired into
/// the workspace `seams-init` aggregator.
pub fn init_seams() {
    backend_catalog_heap_seams::heap_create_with_catalog::set(heap_create_with_catalog_seam);
}
