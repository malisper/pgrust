//! Inward seam installation for `backend-catalog-dependency`.
//!
//! The collection seams pass the owned `ObjectAddresses` value across the
//! crate boundary directly. Each seam is a thin marshal + delegate to the real
//! engine.

use backend_catalog_dependency_seams as seams;
use mcx::MemoryContext;
use types_catalog::catalog_dependency::{DependencyType, ObjectAddress, ObjectAddresses};
use types_error::PgResult;
use types_nodes::nodes::Node;
use types_nodes::parsenodes::DropBehavior;

pub use seams::{
    PERFORM_DELETION_CONCURRENTLY, PERFORM_DELETION_CONCURRENT_LOCK, PERFORM_DELETION_INTERNAL,
    PERFORM_DELETION_QUIETLY, PERFORM_DELETION_SKIP_EXTENSIONS, PERFORM_DELETION_SKIP_ORIGINAL,
};

fn seam_new_object_addresses() -> PgResult<ObjectAddresses> {
    Ok(crate::new_object_addresses())
}

fn seam_add_exact_object_address(
    object: ObjectAddress,
    addrs: &mut ObjectAddresses,
) -> PgResult<()> {
    crate::add_exact_object_address(&object, addrs);
    Ok(())
}

fn seam_object_address_present(
    object: ObjectAddress,
    addrs: &ObjectAddresses,
) -> PgResult<bool> {
    Ok(crate::object_address_present(&object, addrs))
}

fn seam_free_object_addresses(_addrs: ObjectAddresses) -> PgResult<()> {
    // Owned value dropped here, releasing its storage.
    Ok(())
}

fn seam_perform_deletion(
    class_id: types_core::primitive::Oid,
    object_id: types_core::primitive::Oid,
    object_sub_id: i32,
    behavior: DropBehavior,
    flags: i32,
) -> PgResult<()> {
    let object = ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: object_sub_id,
    };
    let ctx = MemoryContext::new("performDeletion");
    crate::performDeletion(ctx.mcx(), &object, behavior, flags)
}

fn seam_acquire_deletion_lock(object: &ObjectAddress, flags: i32) -> PgResult<()> {
    crate::AcquireDeletionLock(object, flags)
}

fn seam_release_deletion_lock(object: &ObjectAddress) -> PgResult<()> {
    crate::ReleaseDeletionLock(object)
}

fn seam_sort_object_addresses(addrs: &mut [ObjectAddress]) {
    if addrs.len() > 1 {
        addrs.sort_by(crate::object_address_comparator);
    }
}

fn seam_perform_multiple_deletions(
    objects: &[ObjectAddress],
    behavior: DropBehavior,
    flags: i32,
) -> PgResult<()> {
    let addrs = types_catalog::catalog_dependency::ObjectAddresses {
        refs: objects.to_vec(),
        extras: Vec::new(),
        numrefs: objects.len() as i32,
        maxrefs: objects.len() as i32,
    };
    let ctx = MemoryContext::new("performMultipleDeletions");
    crate::performMultipleDeletions(ctx.mcx(), &addrs, behavior, flags)
}

fn seam_record_object_address_dependencies(
    depender: ObjectAddress,
    refs: &mut ObjectAddresses,
    behavior: DependencyType,
) -> PgResult<()> {
    crate::record_object_address_dependencies(&depender, refs, behavior)
}

fn seam_record_dependency_on_single_rel_expr(
    depender: ObjectAddress,
    expr: &Node<'_>,
    rel_id: types_core::primitive::Oid,
    self_behavior: DependencyType,
    other_behavior: DependencyType,
    reverse_self: bool,
) -> PgResult<()> {
    crate::recordDependencyOnSingleRelExpr(
        &depender,
        expr,
        rel_id,
        other_behavior,
        self_behavior,
        reverse_self,
    )
}

/// Install every seam declared in `backend-catalog-dependency-seams`.
pub fn init_seams() {
    seams::new_object_addresses::set(seam_new_object_addresses);
    seams::add_exact_object_address::set(seam_add_exact_object_address);
    seams::object_address_present::set(seam_object_address_present);
    seams::free_object_addresses::set(seam_free_object_addresses);
    seams::perform_deletion::set(seam_perform_deletion);
    seams::acquire_deletion_lock::set(seam_acquire_deletion_lock);
    seams::release_deletion_lock::set(seam_release_deletion_lock);
    seams::sort_object_addresses::set(seam_sort_object_addresses);
    seams::perform_multiple_deletions::set(seam_perform_multiple_deletions);
    seams::record_object_address_dependencies::set(seam_record_object_address_dependencies);
    seams::record_dependency_on_single_rel_expr::set(seam_record_dependency_on_single_rel_expr);
}
