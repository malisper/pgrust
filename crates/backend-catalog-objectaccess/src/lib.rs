//! Port of `backend/catalog/objectaccess.c` — the thin layer that runs the
//! global `object_access_hook` (and `object_access_hook_str`) for the various
//! `OAT_*` object-access events. This is infrastructure for security and
//! logging plugins (notably sepgsql).
//!
//! The `Run*Hook` entrypoints assemble the per-event argument struct and call
//! the registered hook. Each `OAT_*` event has both an object-ID form and a
//! `*Str` object-name form. The `Invoke*Hook*` header macros are the
//! check-then-run wrappers core code normally uses (each tests whether the hook
//! is installed before dispatching); they are ported here as `invoke_*`
//! functions.
//!
//! ## The hook (`object_access_hook` / `object_access_hook_str`)
//!
//! In C these are process-wide function pointers an extension installs at load
//! time (`object_access_hook_type` / `object_access_hook_type_str`). They are a
//! genuine external: the registered function comes from a not-yet-ported plugin
//! (sepgsql / a security-label provider). We model them as per-backend slots
//! ([`thread_local`]) holding an optional function pointer; no ported code
//! installs one, so they default to `None` and the `Invoke*` wrappers take the
//! "no hook" path exactly as the C macros do.
//!
//! The hook's `void *arg` is carried as the typed [`ObjectAccessArg`] enum so a
//! hook can read and mutate the per-event struct (notably the
//! `ObjectAccessNamespaceSearch.result` out-parameter) just as the C does
//! through the pointer. A hook may `ereport(ERROR)`, so every entrypoint
//! returns [`PgResult`].

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::Cell;

use types_catalog::catalog::{
    NAMESPACE_RELATION_ID as NamespaceRelationId, PROCEDURE_RELATION_ID as ProcedureRelationId,
    RELATION_RELATION_ID as RelationRelationId,
};
use types_catalog::object_access::{
    ObjectAccessArg, ObjectAccessDrop, ObjectAccessNamespaceSearch, ObjectAccessPostAlter,
    ObjectAccessPostCreate, ObjectAccessType, OAT_DROP, OAT_FUNCTION_EXECUTE, OAT_NAMESPACE_SEARCH,
    OAT_POST_ALTER, OAT_POST_CREATE, OAT_TRUNCATE,
};
use types_core::primitive::Oid;
use types_error::PgResult;

/// `object_access_hook_type` (`catalog/objectaccess.h:127-131`) — the
/// object-ID hook prototype. Higher-ranked over the argument's lifetime so the
/// installed function works for any borrow of the event struct.
pub type ObjectAccessHookType =
    for<'a> fn(ObjectAccessType, Oid, Oid, i32, &mut ObjectAccessArg<'a>) -> PgResult<()>;

/// `object_access_hook_type_str` (`catalog/objectaccess.h:133-137`) — the
/// object-name hook prototype.
pub type ObjectAccessHookTypeStr =
    for<'a> fn(ObjectAccessType, Oid, &str, i32, &mut ObjectAccessArg<'a>) -> PgResult<()>;

thread_local! {
    /// `object_access_hook` (`objectaccess.c:22`) — `NULL` until a plugin
    /// installs one.
    static OBJECT_ACCESS_HOOK: Cell<Option<ObjectAccessHookType>> = const { Cell::new(None) };
    /// `object_access_hook_str` (`objectaccess.c:23`).
    static OBJECT_ACCESS_HOOK_STR: Cell<Option<ObjectAccessHookTypeStr>> =
        const { Cell::new(None) };
}

/// Install the object-ID `object_access_hook` (or clear it with `None`).
/// Mirrors a plugin assigning `object_access_hook = my_hook;`.
pub fn set_object_access_hook(hook: Option<ObjectAccessHookType>) {
    OBJECT_ACCESS_HOOK.with(|h| h.set(hook));
}

/// Install the object-name `object_access_hook_str`.
pub fn set_object_access_hook_str(hook: Option<ObjectAccessHookTypeStr>) {
    OBJECT_ACCESS_HOOK_STR.with(|h| h.set(hook));
}

/// Whether an `object_access_hook` is installed (`object_access_hook != NULL`).
#[inline]
pub fn object_access_hook_present() -> bool {
    OBJECT_ACCESS_HOOK.with(|h| h.get().is_some())
}

/// Whether an `object_access_hook_str` is installed
/// (`object_access_hook_str != NULL`).
#[inline]
pub fn object_access_hook_str_present() -> bool {
    OBJECT_ACCESS_HOOK_STR.with(|h| h.get().is_some())
}

#[inline]
fn call_hook(
    access: ObjectAccessType,
    class_id: Oid,
    object_id: Oid,
    sub_id: i32,
    arg: &mut ObjectAccessArg<'_>,
) -> PgResult<()> {
    // Assert(object_access_hook != NULL); caller should check, but just in case.
    let hook = OBJECT_ACCESS_HOOK
        .with(|h| h.get())
        .expect("RunObject*Hook called with no object_access_hook installed");
    hook(access, class_id, object_id, sub_id, arg)
}

#[inline]
fn call_hook_str(
    access: ObjectAccessType,
    class_id: Oid,
    object_name: &str,
    sub_id: i32,
    arg: &mut ObjectAccessArg<'_>,
) -> PgResult<()> {
    let hook = OBJECT_ACCESS_HOOK_STR
        .with(|h| h.get())
        .expect("RunObject*HookStr called with no object_access_hook_str installed");
    hook(access, class_id, object_name, sub_id, arg)
}

// ---------------------------------------------------------------------------
// OAT_* object-ID based event hook entrypoints (objectaccess.c:31-148)
// ---------------------------------------------------------------------------

/// `RunObjectPostCreateHook` (`objectaccess.c:31-46`).
pub fn run_object_post_create_hook(
    class_id: Oid,
    object_id: Oid,
    sub_id: i32,
    is_internal: bool,
) -> PgResult<()> {
    let mut pc_arg = ObjectAccessPostCreate { is_internal };
    call_hook(
        OAT_POST_CREATE,
        class_id,
        object_id,
        sub_id,
        &mut ObjectAccessArg::PostCreate(&mut pc_arg),
    )
}

/// `RunObjectDropHook` (`objectaccess.c:53-68`).
pub fn run_object_drop_hook(
    class_id: Oid,
    object_id: Oid,
    sub_id: i32,
    dropflags: i32,
) -> PgResult<()> {
    let mut drop_arg = ObjectAccessDrop { dropflags };
    call_hook(
        OAT_DROP,
        class_id,
        object_id,
        sub_id,
        &mut ObjectAccessArg::Drop(&mut drop_arg),
    )
}

/// `RunObjectTruncateHook` (`objectaccess.c:75-84`).
pub fn run_object_truncate_hook(object_id: Oid) -> PgResult<()> {
    call_hook(
        OAT_TRUNCATE,
        RelationRelationId,
        object_id,
        0,
        &mut ObjectAccessArg::None,
    )
}

/// `RunObjectPostAlterHook` (`objectaccess.c:91-107`).
pub fn run_object_post_alter_hook(
    class_id: Oid,
    object_id: Oid,
    sub_id: i32,
    auxiliary_id: Oid,
    is_internal: bool,
) -> PgResult<()> {
    let mut pa_arg = ObjectAccessPostAlter {
        auxiliary_id,
        is_internal,
    };
    call_hook(
        OAT_POST_ALTER,
        class_id,
        object_id,
        sub_id,
        &mut ObjectAccessArg::PostAlter(&mut pa_arg),
    )
}

/// `RunNamespaceSearchHook` (`objectaccess.c:114-131`). Returns the hook's
/// verdict (`ns_arg.result`): `true` unless a hook denied access.
pub fn run_namespace_search_hook(object_id: Oid, ereport_on_violation: bool) -> PgResult<bool> {
    let mut ns_arg = ObjectAccessNamespaceSearch {
        ereport_on_violation,
        result: true,
    };
    call_hook(
        OAT_NAMESPACE_SEARCH,
        NamespaceRelationId,
        object_id,
        0,
        &mut ObjectAccessArg::NamespaceSearch(&mut ns_arg),
    )?;
    Ok(ns_arg.result)
}

/// `RunFunctionExecuteHook` (`objectaccess.c:138-147`).
pub fn run_function_execute_hook(object_id: Oid) -> PgResult<()> {
    call_hook(
        OAT_FUNCTION_EXECUTE,
        ProcedureRelationId,
        object_id,
        0,
        &mut ObjectAccessArg::None,
    )
}

// ---------------------------------------------------------------------------
// String versions — OAT_* object-name based event hook entrypoints
// (objectaccess.c:157-273)
// ---------------------------------------------------------------------------

/// `RunObjectPostCreateHookStr` (`objectaccess.c:157-172`).
pub fn run_object_post_create_hook_str(
    class_id: Oid,
    object_name: &str,
    sub_id: i32,
    is_internal: bool,
) -> PgResult<()> {
    let mut pc_arg = ObjectAccessPostCreate { is_internal };
    call_hook_str(
        OAT_POST_CREATE,
        class_id,
        object_name,
        sub_id,
        &mut ObjectAccessArg::PostCreate(&mut pc_arg),
    )
}

/// `RunObjectDropHookStr` (`objectaccess.c:179-194`).
pub fn run_object_drop_hook_str(
    class_id: Oid,
    object_name: &str,
    sub_id: i32,
    dropflags: i32,
) -> PgResult<()> {
    let mut drop_arg = ObjectAccessDrop { dropflags };
    call_hook_str(
        OAT_DROP,
        class_id,
        object_name,
        sub_id,
        &mut ObjectAccessArg::Drop(&mut drop_arg),
    )
}

/// `RunObjectTruncateHookStr` (`objectaccess.c:201-210`).
pub fn run_object_truncate_hook_str(object_name: &str) -> PgResult<()> {
    call_hook_str(
        OAT_TRUNCATE,
        RelationRelationId,
        object_name,
        0,
        &mut ObjectAccessArg::None,
    )
}

/// `RunObjectPostAlterHookStr` (`objectaccess.c:217-233`).
pub fn run_object_post_alter_hook_str(
    class_id: Oid,
    object_name: &str,
    sub_id: i32,
    auxiliary_id: Oid,
    is_internal: bool,
) -> PgResult<()> {
    let mut pa_arg = ObjectAccessPostAlter {
        auxiliary_id,
        is_internal,
    };
    call_hook_str(
        OAT_POST_ALTER,
        class_id,
        object_name,
        sub_id,
        &mut ObjectAccessArg::PostAlter(&mut pa_arg),
    )
}

/// `RunNamespaceSearchHookStr` (`objectaccess.c:240-257`).
pub fn run_namespace_search_hook_str(
    object_name: &str,
    ereport_on_violation: bool,
) -> PgResult<bool> {
    let mut ns_arg = ObjectAccessNamespaceSearch {
        ereport_on_violation,
        result: true,
    };
    call_hook_str(
        OAT_NAMESPACE_SEARCH,
        NamespaceRelationId,
        object_name,
        0,
        &mut ObjectAccessArg::NamespaceSearch(&mut ns_arg),
    )?;
    Ok(ns_arg.result)
}

/// `RunFunctionExecuteHookStr` (`objectaccess.c:264-273`).
pub fn run_function_execute_hook_str(object_name: &str) -> PgResult<()> {
    call_hook_str(
        OAT_FUNCTION_EXECUTE,
        ProcedureRelationId,
        object_name,
        0,
        &mut ObjectAccessArg::None,
    )
}

// ---------------------------------------------------------------------------
// `Invoke*Hook*` wrappers (catalog/objectaccess.h:173-264).
//
// Each tests the relevant hook pointer and only dispatches when installed. The
// `InvokeNamespaceSearchHook*` macros return `true` when no hook is installed
// (access allowed by default).
// ---------------------------------------------------------------------------

/// `InvokeObjectPostCreateHookArg` (`objectaccess.h:175-180`); the
/// `InvokeObjectPostCreateHook` macro is this with `is_internal = false`.
#[inline]
pub fn invoke_object_post_create_hook(
    class_id: Oid,
    object_id: Oid,
    sub_id: i32,
    is_internal: bool,
) -> PgResult<()> {
    if object_access_hook_present() {
        run_object_post_create_hook(class_id, object_id, sub_id, is_internal)?;
    }
    Ok(())
}

/// `InvokeObjectDropHookArg` (`objectaccess.h:184-189`); the
/// `InvokeObjectDropHook` macro is this with `dropflags = 0`.
#[inline]
pub fn invoke_object_drop_hook(
    class_id: Oid,
    object_id: Oid,
    sub_id: i32,
    dropflags: i32,
) -> PgResult<()> {
    if object_access_hook_present() {
        run_object_drop_hook(class_id, object_id, sub_id, dropflags)?;
    }
    Ok(())
}

/// `InvokeObjectTruncateHook` (`objectaccess.h:191-195`).
#[inline]
pub fn invoke_object_truncate_hook(object_id: Oid) -> PgResult<()> {
    if object_access_hook_present() {
        run_object_truncate_hook(object_id)?;
    }
    Ok(())
}

/// `InvokeObjectPostAlterHookArg` (`objectaccess.h:200-206`); the
/// `InvokeObjectPostAlterHook` macro is this with `auxiliary_id = InvalidOid`
/// and `is_internal = false`.
#[inline]
pub fn invoke_object_post_alter_hook(
    class_id: Oid,
    object_id: Oid,
    sub_id: i32,
    auxiliary_id: Oid,
    is_internal: bool,
) -> PgResult<()> {
    if object_access_hook_present() {
        run_object_post_alter_hook(class_id, object_id, sub_id, auxiliary_id, is_internal)?;
    }
    Ok(())
}

/// `InvokeNamespaceSearchHook` (`objectaccess.h:208-211`): returns `true` when
/// no hook is installed, else the hook's verdict.
#[inline]
pub fn invoke_namespace_search_hook(
    object_id: Oid,
    ereport_on_violation: bool,
) -> PgResult<bool> {
    if object_access_hook_present() {
        run_namespace_search_hook(object_id, ereport_on_violation)
    } else {
        Ok(true)
    }
}

/// `InvokeFunctionExecuteHook` (`objectaccess.h:213-217`).
#[inline]
pub fn invoke_function_execute_hook(object_id: Oid) -> PgResult<()> {
    if object_access_hook_present() {
        run_function_execute_hook(object_id)?;
    }
    Ok(())
}

/// `InvokeObjectPostCreateHookArgStr` (`objectaccess.h:222-227`).
#[inline]
pub fn invoke_object_post_create_hook_str(
    class_id: Oid,
    object_name: &str,
    sub_id: i32,
    is_internal: bool,
) -> PgResult<()> {
    if object_access_hook_str_present() {
        run_object_post_create_hook_str(class_id, object_name, sub_id, is_internal)?;
    }
    Ok(())
}

/// `InvokeObjectDropHookArgStr` (`objectaccess.h:231-236`).
#[inline]
pub fn invoke_object_drop_hook_str(
    class_id: Oid,
    object_name: &str,
    sub_id: i32,
    dropflags: i32,
) -> PgResult<()> {
    if object_access_hook_str_present() {
        run_object_drop_hook_str(class_id, object_name, sub_id, dropflags)?;
    }
    Ok(())
}

/// `InvokeObjectTruncateHookStr` (`objectaccess.h:238-242`).
#[inline]
pub fn invoke_object_truncate_hook_str(object_name: &str) -> PgResult<()> {
    if object_access_hook_str_present() {
        run_object_truncate_hook_str(object_name)?;
    }
    Ok(())
}

/// `InvokeObjectPostAlterHookArgStr` (`objectaccess.h:247-253`).
#[inline]
pub fn invoke_object_post_alter_hook_str(
    class_id: Oid,
    object_name: &str,
    sub_id: i32,
    auxiliary_id: Oid,
    is_internal: bool,
) -> PgResult<()> {
    if object_access_hook_str_present() {
        run_object_post_alter_hook_str(class_id, object_name, sub_id, auxiliary_id, is_internal)?;
    }
    Ok(())
}

/// `InvokeNamespaceSearchHookStr` (`objectaccess.h:255-258`): returns `true`
/// when no hook is installed.
#[inline]
pub fn invoke_namespace_search_hook_str(
    object_name: &str,
    ereport_on_violation: bool,
) -> PgResult<bool> {
    if object_access_hook_str_present() {
        run_namespace_search_hook_str(object_name, ereport_on_violation)
    } else {
        Ok(true)
    }
}

/// `InvokeFunctionExecuteHookStr` (`objectaccess.h:260-264`).
#[inline]
pub fn invoke_function_execute_hook_str(object_name: &str) -> PgResult<()> {
    if object_access_hook_str_present() {
        run_function_execute_hook_str(object_name)?;
    }
    Ok(())
}

/// Install this crate's inward seams (declared in
/// `backend-catalog-objectaccess-seams`). Only `set()` calls.
pub fn init_seams() {
    use backend_catalog_objectaccess_seams as s;

    s::object_access_hook_present::set(object_access_hook_present);
    s::invoke_namespace_search_hook::set(invoke_namespace_search_hook);
    s::run_object_post_create_hook::set(run_object_post_create_hook);
    s::invoke_object_post_create_hook::set(|class_id, object_id, sub_id| {
        invoke_object_post_create_hook(class_id, object_id, sub_id, false)
    });
    s::invoke_object_post_create_hook_arg::set(invoke_object_post_create_hook);
    s::invoke_object_post_alter_hook::set(|class_id, object_id, sub_id| {
        invoke_object_post_alter_hook(class_id, object_id, sub_id, InvalidOid, false)
    });
    s::invoke_object_post_alter_hook_arg::set(invoke_object_post_alter_hook);
    s::invoke_function_execute_hook::set(invoke_function_execute_hook);
    s::InvokeObjectDropHookArg::set(invoke_object_drop_hook);

    // guc_funcs.c (SetPGVariable) invokes the by-name post-alter hook for the
    // GUC change through its own outward seam crate. C macro
    // `InvokeObjectPostAlterHookArgStr(classId, name, subId, auxiliaryId,
    // is_internal)` -> `RunObjectPostAlterHookStr`. The seam carries `name` as an
    // owned String; borrow it for the &str-taking owner body.
    backend_utils_misc_guc_funcs_seams::invoke_object_post_alter_hook_arg_str::set(
        |class_id, object_name, sub_id, auxiliary_id, is_internal| {
            invoke_object_post_alter_hook_str(
                class_id,
                &object_name,
                sub_id,
                auxiliary_id,
                is_internal,
            )
        },
    );
}

use types_core::primitive::InvalidOid;

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    #[derive(Clone, Default)]
    struct Recorded {
        access: Option<ObjectAccessType>,
        class_id: Oid,
        object_id: Oid,
        sub_id: i32,
        object_name: Option<String>,
        is_internal: Option<bool>,
        dropflags: Option<i32>,
        auxiliary_id: Option<Oid>,
        ereport_on_violation: Option<bool>,
        had_none_arg: bool,
    }

    thread_local! {
        static REC: RefCell<Recorded> = RefCell::new(Recorded::default());
        static DENY_NAMESPACE: Cell<bool> = const { Cell::new(false) };
    }

    fn record_arg(rec: &mut Recorded, arg: &mut ObjectAccessArg<'_>) {
        match arg {
            ObjectAccessArg::PostCreate(a) => rec.is_internal = Some(a.is_internal),
            ObjectAccessArg::Drop(a) => rec.dropflags = Some(a.dropflags),
            ObjectAccessArg::PostAlter(a) => {
                rec.auxiliary_id = Some(a.auxiliary_id);
                rec.is_internal = Some(a.is_internal);
            }
            ObjectAccessArg::NamespaceSearch(a) => {
                rec.ereport_on_violation = Some(a.ereport_on_violation);
                if DENY_NAMESPACE.with(|d| d.get()) {
                    a.result = false;
                }
            }
            ObjectAccessArg::None => rec.had_none_arg = true,
        }
    }

    fn id_hook(
        access: ObjectAccessType,
        class_id: Oid,
        object_id: Oid,
        sub_id: i32,
        arg: &mut ObjectAccessArg<'_>,
    ) -> PgResult<()> {
        REC.with(|r| {
            let mut rec = r.borrow_mut();
            rec.access = Some(access);
            rec.class_id = class_id;
            rec.object_id = object_id;
            rec.sub_id = sub_id;
            record_arg(&mut rec, arg);
        });
        Ok(())
    }

    fn str_hook(
        access: ObjectAccessType,
        class_id: Oid,
        object_str: &str,
        sub_id: i32,
        arg: &mut ObjectAccessArg<'_>,
    ) -> PgResult<()> {
        REC.with(|r| {
            let mut rec = r.borrow_mut();
            rec.access = Some(access);
            rec.class_id = class_id;
            rec.object_name = Some(object_str.to_string());
            rec.sub_id = sub_id;
            record_arg(&mut rec, arg);
        });
        Ok(())
    }

    fn install_hooks() {
        set_object_access_hook(Some(id_hook));
        set_object_access_hook_str(Some(str_hook));
    }

    fn reset() {
        REC.with(|r| *r.borrow_mut() = Recorded::default());
        DENY_NAMESPACE.with(|d| d.set(false));
    }

    #[test]
    fn post_create_assembles_arg_and_dispatches() {
        install_hooks();
        reset();
        run_object_post_create_hook(RelationRelationId, 12345, 7, true).unwrap();
        REC.with(|r| {
            let rec = r.borrow();
            assert_eq!(rec.access, Some(OAT_POST_CREATE));
            assert_eq!(rec.class_id, RelationRelationId);
            assert_eq!(rec.object_id, 12345);
            assert_eq!(rec.sub_id, 7);
            assert_eq!(rec.is_internal, Some(true));
        });
    }

    #[test]
    fn drop_hook_threads_dropflags() {
        install_hooks();
        reset();
        run_object_drop_hook(NamespaceRelationId, 99, 0, 0x10).unwrap();
        REC.with(|r| {
            let rec = r.borrow();
            assert_eq!(rec.access, Some(OAT_DROP));
            assert_eq!(rec.class_id, NamespaceRelationId);
            assert_eq!(rec.dropflags, Some(0x10));
        });
    }

    #[test]
    fn truncate_uses_relation_class_and_null_arg() {
        install_hooks();
        reset();
        run_object_truncate_hook(42).unwrap();
        REC.with(|r| {
            let rec = r.borrow();
            assert_eq!(rec.access, Some(OAT_TRUNCATE));
            assert_eq!(rec.class_id, RelationRelationId);
            assert_eq!(rec.object_id, 42);
            assert_eq!(rec.sub_id, 0);
            assert!(rec.had_none_arg);
        });
    }

    #[test]
    fn post_alter_carries_auxiliary_id() {
        install_hooks();
        reset();
        run_object_post_alter_hook(RelationRelationId, 5, 2, 777, false).unwrap();
        REC.with(|r| {
            let rec = r.borrow();
            assert_eq!(rec.access, Some(OAT_POST_ALTER));
            assert_eq!(rec.auxiliary_id, Some(777));
            assert_eq!(rec.is_internal, Some(false));
        });
    }

    #[test]
    fn namespace_search_default_allows() {
        install_hooks();
        reset();
        let allowed = run_namespace_search_hook(2200, true).unwrap();
        assert!(allowed);
        REC.with(|r| {
            let rec = r.borrow();
            assert_eq!(rec.access, Some(OAT_NAMESPACE_SEARCH));
            assert_eq!(rec.class_id, NamespaceRelationId);
            assert_eq!(rec.ereport_on_violation, Some(true));
        });
    }

    #[test]
    fn namespace_search_hook_can_deny() {
        install_hooks();
        reset();
        DENY_NAMESPACE.with(|d| d.set(true));
        let allowed = run_namespace_search_hook(2200, false).unwrap();
        assert!(!allowed, "hook clearing result must propagate to caller");
    }

    #[test]
    fn function_execute_uses_procedure_class() {
        install_hooks();
        reset();
        run_function_execute_hook(8).unwrap();
        REC.with(|r| {
            let rec = r.borrow();
            assert_eq!(rec.access, Some(OAT_FUNCTION_EXECUTE));
            assert_eq!(rec.class_id, ProcedureRelationId);
            assert!(rec.had_none_arg);
        });
    }

    #[test]
    fn str_variants_pass_the_name() {
        install_hooks();
        reset();
        run_object_post_create_hook_str(RelationRelationId, "my_table", 0, false).unwrap();
        REC.with(|r| {
            let rec = r.borrow();
            assert_eq!(rec.access, Some(OAT_POST_CREATE));
            assert_eq!(rec.object_name.as_deref(), Some("my_table"));
        });
        reset();
        let allowed = run_namespace_search_hook_str("public", true).unwrap();
        assert!(allowed);
        REC.with(|r| {
            assert_eq!(r.borrow().object_name.as_deref(), Some("public"));
        });
    }

    #[test]
    fn invoke_wrappers_skip_when_no_hook() {
        set_object_access_hook(None);
        set_object_access_hook_str(None);
        reset();
        invoke_object_post_create_hook(RelationRelationId, 1, 0, false).unwrap();
        invoke_object_truncate_hook(1).unwrap();
        invoke_function_execute_hook_str("f").unwrap();
        assert!(invoke_namespace_search_hook(2200, true).unwrap());
        assert!(invoke_namespace_search_hook_str("public", true).unwrap());
        REC.with(|r| assert!(r.borrow().access.is_none(), "no hook should have run"));
    }
}
