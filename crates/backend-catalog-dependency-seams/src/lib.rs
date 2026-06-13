//! Seam declarations for the `backend-catalog-dependency` unit
//! (`catalog/dependency.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

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
