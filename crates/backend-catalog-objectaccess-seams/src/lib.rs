//! Seam declarations for the `backend-catalog-objectaccess` unit
//! (`catalog/objectaccess.c` and the `object_access_hook` variable).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `InvokeNamespaceSearchHook(objectId, ereport_on_violation)`
    /// (objectaccess.h macro / `RunNamespaceSearchHook`): consult the
    /// object-access hook about a namespace search. `true` when no hook is
    /// installed. With `ereport_on_violation` the hook may raise, carried on
    /// `Err`.
    pub fn invoke_namespace_search_hook(
        object_id: Oid,
        ereport_on_violation: bool,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `object_access_hook != NULL` — whether an object_access_hook is
    /// installed (drives namespace.c's finalPath recompute). Pure read.
    pub fn object_access_hook_present() -> bool
);

seam_core::seam!(
    /// `RunObjectPostCreateHook(classId, objectId, subId, is_internal)`
    /// (objectaccess.c) — the post-create object-access hook body. The
    /// `InvokeObjectPostCreateHook` macro's `if (object_access_hook)` guard is
    /// the caller's (use [`object_access_hook_present`]); the C macro passes
    /// `is_internal = false`. The hook may `ereport(ERROR)`, carried on `Err`.
    pub fn run_object_post_create_hook(
        class_id: Oid,
        object_id: Oid,
        sub_id: i32,
        is_internal: bool,
    ) -> PgResult<()>
);
