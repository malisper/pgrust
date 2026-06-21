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
        &args.cooked_constraints,
    )
}

/// Seam body for `heap_create_with_catalog_transient` — the inner
/// `heap_create_with_catalog(...)` call of `cluster.c`'s `make_new_heap`
/// (cluster.c:687-707). The transient NewHeap clones the OldHeap's tuple
/// descriptor, owner, AM, persistence, mapped-ness and reloptions, is created as
/// a plain `RELKIND_RELATION` with no defaults/constraints (NIL cooked
/// constraints), `shared_relation = false`, `oncommit = ONCOMMIT_NOOP`,
/// `use_user_acl = false`, `allow_system_table_mods = true`, `is_internal =
/// true`, and `relid`/`reltypeid`/`reloftypeid = InvalidOid`, with the OldHeap
/// OID passed as `relrewrite` for the rebuild bookkeeping.
#[allow(clippy::too_many_arguments)]
fn heap_create_with_catalog_transient_seam<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    new_heap_name: &str,
    namespaceid: types_core::Oid,
    new_tablespace: types_core::Oid,
    owner: types_core::Oid,
    new_access_method: types_core::Oid,
    old_heap: &types_rel::Relation<'_>,
    relpersistence: u8,
    mapped: bool,
    reloptions: types_cluster::RelOptionsToken,
    old_heap_oid: types_core::Oid,
) -> PgResult<types_core::Oid> {
    use types_core::InvalidOid;
    use types_nodes::primnodes::OnCommitAction;
    use types_tuple::access::RELKIND_RELATION;

    let reloptions: Option<alloc::vec::Vec<u8>> = if reloptions.is_null {
        None
    } else {
        Some(reloptions.bytes)
    };

    // OldHeapDesc = RelationGetDescr(OldHeap).
    //
    // C comment (cluster.c make_new_heap): "the NewHeap will not receive any of
    // the defaults or constraints associated with the OldHeap; we don't need
    // 'em, and there's no reason to spend cycles inserting them into the
    // catalogs only to delete them." C achieves that by passing NIL
    // cooked_constraints — but the OldHeapDesc it hands down still carries
    // per-attribute `atthasdef`/`atthasmissing` flags, which `AddNewAttributeTuples`
    // copies verbatim into the transient heap's pg_attribute. With NIL
    // constraints no pg_attrdef rows are inserted, so a transient heap built from
    // a relation that has column defaults ends up with `atthasdef=true` and no
    // matching pg_attrdef record. The port's relcache `AttrDefaultFetch` then
    // emits a spurious `N pg_attrdef record(s) missing for relation
    // "pg_temp_..."` WARNING when that transient relation's tuple descriptor is
    // built. Honor the "no defaults" contract literally: clone OldHeapDesc and
    // clear the default-bearing flags (and the constraint payload) before
    // creating the transient heap, so the temp heap's catalog is internally
    // consistent.
    let mut transient_desc = old_heap.rd_att.clone_in(mcx)?;
    for i in 0..(transient_desc.natts as usize) {
        let attr = transient_desc.attr_mut(i);
        attr.atthasdef = false;
        attr.atthasmissing = false;
    }
    if let Some(constr) = transient_desc.constr.as_mut() {
        constr.defval = mcx::PgVec::new_in(mcx);
        constr.num_defval = 0;
        constr.missing = mcx::PgVec::new_in(mcx);
    }

    heap_create_with_catalog(
        mcx,
        new_heap_name,
        namespaceid,
        new_tablespace,
        InvalidOid, /* relid */
        InvalidOid, /* reltypeid */
        InvalidOid, /* reloftypeid */
        owner,
        new_access_method,
        &transient_desc,
        RELKIND_RELATION,
        relpersistence,
        false, /* shared_relation: a transient heap is never shared */
        mapped,
        OnCommitAction::ONCOMMIT_NOOP,
        reloptions,
        false, /* use_user_acl */
        true,  /* allow_system_table_mods */
        true,  /* is_internal */
        old_heap_oid, /* relrewrite */
        &[],   /* cooked_constraints: a transient heap carries none */
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

fn set_attribute_has_default_seam(
    relid: types_core::Oid,
    attnum: types_core::AttrNumber,
) -> PgResult<Option<i8>> {
    let ctx = MemoryContext::new("SetAttributeHasDefault");
    crate::SetAttributeHasDefault(ctx.mcx(), relid, attnum)
}

fn clear_attribute_has_default_seam(
    relid: types_core::Oid,
    attnum: types_core::AttrNumber,
) -> PgResult<bool> {
    let ctx = MemoryContext::new("ClearAttributeHasDefault");
    crate::ClearAttributeHasDefault(ctx.mcx(), relid, attnum)
}

/// `SystemAttributeDefinition(attno)` for `plancat.c`'s negative-index-key path:
/// returns `(atttypid, atttypmod, attcollation)` of the system column.
fn system_attribute_definition_seam(
    attno: i32,
) -> PgResult<(types_core::Oid, i32, types_core::Oid)> {
    let att = crate::SystemAttributeDefinition(attno as types_core::AttrNumber)?;
    Ok((att.atttypid, att.atttypmod, att.attcollation))
}

/// `SystemAttributeByName(attname)` (`specialAttNum`): the system column's
/// negative `attnum`, or `None` if the name is not a system attribute.
fn system_attribute_by_name_seam(attname: &str) -> PgResult<Option<i32>> {
    Ok(crate::SystemAttributeByName(attname.as_bytes()).map(|att| att.attnum as i32))
}

/// `SystemAttributeByName(attname)` projected to the `(attnum, atttypid,
/// atttypmod, attcollation)` a field-reference resolver needs (expandedrecord.c).
fn system_attribute_by_name_fields(
    attname: &str,
) -> Option<(i32, types_core::Oid, i32, types_core::Oid)> {
    crate::SystemAttributeByName(attname.as_bytes())
        .map(|att| (att.attnum as i32, att.atttypid, att.atttypmod, att.attcollation))
}

/// `init_seams()` — install the heap.c inward seams this crate owns. Wired into
/// the workspace `seams-init` aggregator.
pub fn init_seams() {
    backend_optimizer_util_plancat_ext_seams::system_attribute_definition::set(
        system_attribute_definition_seam,
    );
    backend_optimizer_util_plancat_ext_seams::system_attribute_by_name::set(
        system_attribute_by_name_seam,
    );
    backend_catalog_heap_seams::system_attribute_by_name::set(system_attribute_by_name_fields);
    backend_catalog_heap_seams::heap_create_with_catalog::set(heap_create_with_catalog_seam);
    backend_catalog_heap_seams::heap_create_with_catalog_transient::set(
        heap_create_with_catalog_transient_seam,
    );
    backend_catalog_heap_seams::heap_drop_with_catalog::set(heap_drop_with_catalog_seam);
    backend_catalog_heap_seams::copy_statistics::set(|mcx, fromrelid, torelid| {
        crate::CopyStatistics(mcx, fromrelid, torelid)
    });
    backend_catalog_heap_seams::check_attribute_names_types::set(check_attribute_names_types_seam);
    backend_catalog_heap_seams::check_attribute_type::set(check_attribute_type_seam);
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
    backend_utils_cache_syscache_seams::set_attribute_has_default::set(
        set_attribute_has_default_seam,
    );
    backend_utils_cache_syscache_seams::clear_attribute_has_default::set(
        clear_attribute_has_default_seam,
    );

    // Constraint-cooker outward seams the tablecmds CREATE-TABLE path consumes
    // (declared in `backend-commands-tablecmds-seams`, owned here). Signatures
    // match exactly, so they install without a wrapper.
    backend_commands_tablecmds_seams::add_relation_new_constraints::set(
        crate::AddRelationNewConstraints,
    );
    backend_commands_tablecmds_seams::add_relation_not_null_constraints::set(
        crate::AddRelationNotNullConstraints,
    );

    // Catalog-row delete helpers that `index_drop` (catalog/index.c) calls to
    // clean up a dropped index's pg_class / pg_attribute / pg_statistic rows.
    // The inward seams carry no `mcx`; the shims allocate a scratch context.
    backend_catalog_heap_seams::DeleteRelationTuple::set(delete_relation_tuple_seam);
    backend_catalog_heap_seams::DeleteAttributeTuples::set(delete_attribute_tuples_seam);
    backend_catalog_heap_seams::RemoveStatistics::set(remove_statistics_seam);

    // TRUNCATE FK-check tail (tablecmds.c's `ExecuteTruncateGuts`). `find_fks`
    // carries `mcx` and matches the owner signature; `check_fks` does not, so its
    // shim allocates a scratch context.
    backend_commands_tablecmds_seams::heap_truncate_find_fks::set(crate::heap_truncate_find_FKs);
    backend_commands_tablecmds_seams::heap_truncate_check_fks::set(heap_truncate_check_fks_seam);

    // Storage-truncate chain: the immediate, non-rollbackable single-rel
    // truncation (in-place TRUNCATE / ON COMMIT path) and its `heap_truncate`
    // wrapper. Both carry `mcx` and match the owner signatures.
    backend_commands_tablecmds_seams::heap_truncate_one_rel::set(crate::truncate::heap_truncate_one_rel);
    backend_commands_tablecmds_seams::heap_truncate::set(crate::truncate::heap_truncate);
}

/// Seam body for `heap_truncate_check_FKs(relids, tempTables)` (catalog/heap.c).
/// The owner seam carries no `mcx`; allocate a scratch context for the scans.
fn heap_truncate_check_fks_seam(relids: &[types_core::Oid], temp_tables: bool) -> PgResult<()> {
    let ctx = MemoryContext::new("heap_truncate_check_FKs");
    crate::heap_truncate_check_FKs(ctx.mcx(), relids, temp_tables)
}

/// Seam body for `DeleteRelationTuple(relid)` (catalog/heap.c). The inward seam
/// carries no `mcx`.
fn delete_relation_tuple_seam(relid: types_core::Oid) -> PgResult<()> {
    let ctx = MemoryContext::new("DeleteRelationTuple");
    crate::DeleteRelationTuple(ctx.mcx(), relid)
}

/// Seam body for `DeleteAttributeTuples(relid)` (catalog/heap.c). The inward
/// seam carries no `mcx`.
fn delete_attribute_tuples_seam(relid: types_core::Oid) -> PgResult<()> {
    let ctx = MemoryContext::new("DeleteAttributeTuples");
    crate::DeleteAttributeTuples(ctx.mcx(), relid)
}

/// Seam body for `RemoveStatistics(relid, attnum)` (catalog/heap.c). The inward
/// seam carries no `mcx`.
fn remove_statistics_seam(relid: types_core::Oid, attnum: i16) -> PgResult<()> {
    let ctx = MemoryContext::new("RemoveStatistics");
    crate::RemoveStatistics(ctx.mcx(), relid, attnum)
}

/// Seam body for `CheckAttributeNamesTypes` (catalog/heap.c). The descriptor
/// crosses by reference; the real validator reads its `attrs`.
fn check_attribute_names_types_seam<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    tupdesc: &types_tuple::heaptuple::TupleDescData<'mcx>,
    relkind: u8,
    flags: i32,
) -> PgResult<()> {
    crate::CheckAttributeNamesTypes(mcx, &tupdesc.attrs, relkind, flags)
}

/// Seam body for `CheckAttributeType` (catalog/heap.c). As called by
/// `ConstructTupleDescriptor` (catalog/index.c) for an expression-index column:
/// `containing_rowtypes = NIL`, `flags = 0`. The inward seam carries no `mcx`.
fn check_attribute_type_seam(
    attname: &str,
    atttypid: types_core::Oid,
    attcollation: types_core::Oid,
) -> PgResult<()> {
    let ctx = MemoryContext::new("CheckAttributeType");
    let mut containing_rowtypes: alloc::vec::Vec<types_core::Oid> = alloc::vec::Vec::new();
    crate::CheckAttributeType(
        ctx.mcx(),
        attname,
        atttypid,
        attcollation,
        &mut containing_rowtypes,
        0,
    )
}
