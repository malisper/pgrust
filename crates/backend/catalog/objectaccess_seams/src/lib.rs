//! Seam declarations for the `backend-catalog-objectaccess` unit
//! (`catalog/objectaccess.c` and the `object_access_hook` variable).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

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
    /// `InvokeObjectPostCreateHook(classId, objectId, subId)` (objectaccess.h
    /// macro / `RunObjectPostCreateHook`): fire the post-create object-access
    /// hook if one is installed; a no-op otherwise. The hook may raise,
    /// carried on `Err`.
    pub fn invoke_object_post_create_hook(
        class_id: Oid,
        object_id: Oid,
        sub_id: i32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `InvokeObjectPostCreateHookArg(classId, objectId, subId, is_internal)`
    /// (objectaccess.h macro / `RunObjectPostCreateHook`): fire the post-create
    /// object-access hook with the `is_internal` flag. The hook may raise,
    /// carried on `Err`.
    pub fn invoke_object_post_create_hook_arg(
        class_id: Oid,
        object_id: Oid,
        sub_id: i32,
        is_internal: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `InvokeObjectPostAlterHook(classId, objectId, subId)` (objectaccess.h
    /// macro / `RunObjectPostAlterHook`): fire the post-alter object-access
    /// hook if one is installed; a no-op otherwise. The hook may raise,
    /// carried on `Err`.
    pub fn invoke_object_post_alter_hook(
        class_id: Oid,
        object_id: Oid,
        sub_id: i32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `InvokeObjectPostAlterHookArg(classId, objectId, subId, auxObjId,
    /// is_internal)` (objectaccess.h): fire the post-alter object-access hook.
    pub fn invoke_object_post_alter_hook_arg(
        class_id: Oid,
        object_id: Oid,
        sub_id: i32,
        aux_obj_id: Oid,
        is_internal: bool,
    ) -> PgResult<()>
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

seam_core::seam!(
    /// `InvokeFunctionExecuteHook(objectId)` (objectaccess.h macro /
    /// `RunFunctionExecuteHook`): fire the object-access hook for a function
    /// about to be executed if one is installed; a no-op otherwise. Used by
    /// `tcop/fastpath.c`'s `HandleFunctionRequest` after the `ACL_EXECUTE`
    /// check. The hook may raise, carried on `Err`.
    pub fn invoke_function_execute_hook(object_id: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `InvokeObjectDropHookArg(classId, objectId, subId, dropflags)`
    /// (objectaccess.h macro / `RunObjectDropHook`): fire the object-access drop
    /// hook for an object dependency.c is about to delete, passing the
    /// `PERFORM_DELETION_*` drop flags. A no-op when no hook is installed. The
    /// hook may `ereport(ERROR)`, carried on `Err`.
    pub fn InvokeObjectDropHookArg(
        class_id: Oid,
        object_id: Oid,
        sub_id: i32,
        drop_flags: i32,
    ) -> PgResult<()>
);
