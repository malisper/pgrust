//! Seam declarations for the `backend-catalog-dependency` unit
//! (`catalog/dependency.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_catalog::catalog_dependency::{ObjectAddress, ObjectAddresses};
use types_core::Oid;
use types_error::PgResult;
use types_nodes::parsenodes::DropBehavior;

/// `PERFORM_DELETION_INTERNAL` (`catalog/dependency.h`) — internal action.
pub const PERFORM_DELETION_INTERNAL: i32 = 0x0001;
/// `PERFORM_DELETION_CONCURRENTLY` — concurrent drop.
pub const PERFORM_DELETION_CONCURRENTLY: i32 = 0x0002;
/// `PERFORM_DELETION_QUIETLY` — suppress notices.
pub const PERFORM_DELETION_QUIETLY: i32 = 0x0004;
/// `PERFORM_DELETION_SKIP_ORIGINAL` — keep the original object.
pub const PERFORM_DELETION_SKIP_ORIGINAL: i32 = 0x0008;
/// `PERFORM_DELETION_SKIP_EXTENSIONS` — keep extensions.
pub const PERFORM_DELETION_SKIP_EXTENSIONS: i32 = 0x0010;
/// `PERFORM_DELETION_CONCURRENT_LOCK` — normal drop with concurrent lock mode.
pub const PERFORM_DELETION_CONCURRENT_LOCK: i32 = 0x0020;

seam_core::seam!(
    /// `new_object_addresses()` (dependency.c): allocate a fresh
    /// `ObjectAddresses` accumulator and return it by value. `Err` carries OOM.
    pub fn new_object_addresses() -> PgResult<ObjectAddresses>
);

seam_core::seam!(
    /// `add_exact_object_address(&object, addrs)` (dependency.c): append
    /// `object` to the accumulator (growing it as needed). `Err` carries OOM.
    pub fn add_exact_object_address(
        object: ObjectAddress,
        addrs: &mut ObjectAddresses,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `object_address_present(&object, addrs)` (dependency.c): is `object`
    /// (matching class/object, and subid present or whole-object) already in
    /// the accumulator?
    pub fn object_address_present(
        object: ObjectAddress,
        addrs: &ObjectAddresses,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `free_object_addresses(addrs)` (dependency.c): release the accumulator
    /// and its hashtable. The owned value is consumed (dropped).
    pub fn free_object_addresses(addrs: ObjectAddresses) -> PgResult<()>
);

seam_core::seam!(
    /// `performDeletion(&object, behavior, flags)` (dependency.c) for the
    /// `ObjectAddress {classId, objectId, objectSubId}`: delete the object
    /// and everything depending on it. Can `ereport(ERROR)`, carried on
    /// `Err`.
    pub fn perform_deletion(
        class_id: Oid,
        object_id: Oid,
        object_sub_id: i32,
        behavior: DropBehavior,
        flags: i32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `AcquireDeletionLock(&object, flags)` (dependency.c): take the
    /// appropriate lock (AccessExclusiveLock on a relation, else a generic
    /// object lock) before deleting, so the object can't disappear underneath
    /// us. Returns whether a lock was actually taken (`AcquireDeletionLock`
    /// only locks relations and generic objects; it is a no-op for objects of
    /// classes it doesn't lock). The interim release contract is the paired
    /// [`release_deletion_lock`]; can `ereport(ERROR)`, carried on `Err`.
    pub fn acquire_deletion_lock(object: &ObjectAddress, flags: i32) -> PgResult<()>
);

seam_core::seam!(
    /// `ReleaseDeletionLock(&object)` (dependency.c): release the lock taken
    /// by [`acquire_deletion_lock`] for an object we decided not to delete
    /// after all. Can `elog(WARNING/ERROR)` on a lock-table inconsistency,
    /// carried on `Err`.
    pub fn release_deletion_lock(object: &ObjectAddress) -> PgResult<()>
);

seam_core::seam!(
    /// `sort_object_addresses(addrs)` (dependency.c): sort the collected
    /// addresses into approximate reverse creation order, for stable deletion
    /// reporting. The owned model passes the whole address list; the owner
    /// sorts it in place. Infallible.
    pub fn sort_object_addresses(addrs: &mut [ObjectAddress])
);

seam_core::seam!(
    /// `performMultipleDeletions(objects, behavior, flags)` (dependency.c):
    /// delete the given set of objects and everything that depends on them, in
    /// dependency order. The owned model passes the collected address list as
    /// a slice (the C `ObjectAddresses` collection). Can `ereport(ERROR)`,
    /// carried on `Err`.
    pub fn perform_multiple_deletions(
        objects: &[ObjectAddress],
        behavior: DropBehavior,
        flags: i32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `record_object_address_dependencies(&depender, refs, behavior)`
    /// (dependency.c): record a dependency of `behavior` from `depender` on
    /// every object accumulated in `refs` (the `ObjectAddresses *`). `Err`
    /// carries the pg_depend-insert `ereport(ERROR)`s.
    pub fn record_object_address_dependencies(
        depender: ObjectAddress,
        refs: &mut ObjectAddresses,
        behavior: types_catalog::catalog_dependency::DependencyType,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `recordDependencyOnSingleRelExpr(&depender, expr, relId, self_behavior,
    /// other_behavior, reverse_self)` (dependency.c): scan a single-relation
    /// expression (a CHECK expression `Node *`) for object references and record
    /// the dependencies. `Err` carries the `ereport(ERROR)`s.
    pub fn record_dependency_on_single_rel_expr<'mcx>(
        depender: ObjectAddress,
        expr: &types_nodes::nodes::Node<'mcx>,
        rel_id: Oid,
        behavior: types_catalog::catalog_dependency::DependencyType,
        self_behavior: types_catalog::catalog_dependency::DependencyType,
        reverse_self: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `recordDependencyOnExpr(&depender, expr, rtable, behavior)`
    /// (dependency.c): scan a multi-relation expression (e.g. a trigger WHEN
    /// clause, whose Vars reference the OLD/NEW pseudo-relations carried in
    /// `rtable`) for object references and record the dependencies with the
    /// given behavior. `Err` carries the `ereport(ERROR)`s.
    pub fn record_dependency_on_expr<'mcx>(
        depender: ObjectAddress,
        expr: &types_nodes::nodes::Node<'mcx>,
        rtable: &[types_nodes::parsenodes::RangeTblEntry<'mcx>],
        behavior: types_catalog::catalog_dependency::DependencyType,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `deleteDependencyRecordsForClass(classId, objectId, refclassId,
    /// deptype)` (dependency.c): delete pg_depend rows for the given object
    /// that have the given referenced class and dependency type. Returns the
    /// count removed. `Err` carries the catalog-scan/delete `ereport(ERROR)`s.
    pub fn delete_dependency_records_for_class(
        class_id: Oid,
        object_id: Oid,
        ref_class_id: Oid,
        deptype: types_catalog::catalog_dependency::DependencyType,
    ) -> PgResult<i64>
);

seam_core::seam!(
    /// `recordDependencyOn(depender, referenced, behavior)` (dependency.c):
    /// record a single pg_depend dependency edge. `Err` carries the
    /// catalog-insert `ereport(ERROR)`s.
    pub fn record_dependency_on(
        depender: ObjectAddress,
        referenced: ObjectAddress,
        behavior: types_catalog::catalog_dependency::DependencyType,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `sequenceIsOwned(seqId, deptype, &tableId, &colId)` (pg_depend.c):
    /// whether the sequence has an owned-by dependency of the given type;
    /// `Ok(Some((tableId, colId)))` if so, `Ok(None)` otherwise. `Err` carries
    /// the catalog-scan `ereport(ERROR)`s.
    pub fn sequence_is_owned(
        seq_id: Oid,
        deptype: types_catalog::catalog_dependency::DependencyType,
    ) -> PgResult<Option<(Oid, i32)>>
);
