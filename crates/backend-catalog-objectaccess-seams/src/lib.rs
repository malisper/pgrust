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
