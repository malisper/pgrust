//! src/test/modules/test_oat_hooks (test_oat_hooks.c): the object-access-hook
//! recorder the regression suite uses to witness `InvokeObject*Hook` sites.
//! `LOAD 'test_oat_hooks'` (or session_preload_libraries) runs `_PG_init`,
//! which defines the `test_oat_hooks.*` GUCs, reserves the prefix and installs
//! `REGRESS_object_access_hook` / `REGRESS_object_access_hook_str`. With
//! `test_oat_hooks.audit = on` every hook invocation emits C's NOTICE pair
//! (`in object access: superuser attempting create (subId=0x0) [explicit]`,
//! then `... finished ...`), and `test_oat_hooks.deny_object_access = on`
//! refuses non-superusers with C's 42501 message.
//!
//! SCOPE: the two object-access arms are ported byte-exact. C's module also
//! chains `ExecutorCheckPerms_hook` and `ProcessUtility_hook`; pgrust has no
//! ExecutorCheckPerms hook and its ProcessUtility tap is a single-consumer
//! boot-time seam (pg_stat_statements owns it), so those two arms (the
//! `in executor check perms:` / `in process utility:` NOTICEs and
//! `deny_exec_perms` / `deny_utility_commands`) are not ported; their GUCs are
//! still defined so the C regression SQL parses identically.
//!
//! DIVERGENCE (GUC): C's `_PG_init` runs `DefineCustomBoolVariable` for the
//! ten parameters. pgrust has no typed custom-GUC store (the passwordcheck /
//! isn pattern), so each is a custom *string* with boot value "off", parsed
//! with `parse_bool` on every read: a non-boolean SET is accepted (and reads
//! as false) where C would refuse it, and SHOW echoes the SET spelling.
//!
//! Hook installation is per backend (`LOAD` / session_preload_libraries), as
//! the objectaccess slots are per thread; a postmaster-side
//! shared_preload_libraries load does not reach the backends' slots.

#![allow(non_snake_case)]

use pgsync::Mutex;

use elog::ereport;
use objectaccess::{
    ObjectAccessArg, ObjectAccessHook, ObjectAccessHookStr, ObjectAccessType, OAT_DROP,
    OAT_FUNCTION_EXECUTE, OAT_NAMESPACE_SEARCH, OAT_POST_ALTER, OAT_POST_CREATE, OAT_TRUNCATE,
};
use types_core::{InvalidOid, Oid};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INTERNAL_ERROR,
    NOTICE,
};
use types_fmgr::PGFunction;
use types_nodes::parsenodes::{AclMode, ACL_ALTER_SYSTEM, ACL_SET};

const LIBRARY: &str = "test_oat_hooks";

// dependency.h PERFORM_DELETION_* (the OAT_DROP dropflags vocabulary).
const PERFORM_DELETION_INTERNAL: i32 = 0x0001;
const PERFORM_DELETION_CONCURRENTLY: i32 = 0x0002;
const PERFORM_DELETION_QUIETLY: i32 = 0x0004;
const PERFORM_DELETION_SKIP_ORIGINAL: i32 = 0x0008;
const PERFORM_DELETION_SKIP_EXTENSIONS: i32 = 0x0010;
const PERFORM_DELETION_CONCURRENT_LOCK: i32 = 0x0020;

/// The GUCs of `_PG_init` (test_oat_hooks.c:75-205): name, description,
/// context. All boot to false.
const GUCS: &[(&str, &str, types_guc::GucContext)] = &[
    ("test_oat_hooks.deny_set_variable", "Deny non-superuser set permissions", types_guc::PGC_SUSET),
    (
        "test_oat_hooks.deny_alter_system",
        "Deny non-superuser alter system set permissions",
        types_guc::PGC_SUSET,
    ),
    (
        "test_oat_hooks.deny_object_access",
        "Deny non-superuser object access permissions",
        types_guc::PGC_SUSET,
    ),
    ("test_oat_hooks.deny_exec_perms", "Deny non-superuser exec permissions", types_guc::PGC_SUSET),
    (
        "test_oat_hooks.deny_utility_commands",
        "Deny non-superuser utility commands",
        types_guc::PGC_SUSET,
    ),
    ("test_oat_hooks.audit", "Turn on/off debug audit messages", types_guc::PGC_SUSET),
    ("test_oat_hooks.user_var1", "Dummy parameter settable by public", types_guc::PGC_USERSET),
    ("test_oat_hooks.user_var2", "Dummy parameter settable by public", types_guc::PGC_USERSET),
    ("test_oat_hooks.super_var1", "Dummy parameter settable by superuser", types_guc::PGC_SUSET),
    ("test_oat_hooks.super_var2", "Dummy parameter settable by superuser", types_guc::PGC_SUSET),
];

// Saved hook values (`next_object_access_hook` / `next_object_access_hook_str`):
// the chain C's `_PG_init` builds by saving the previous pointer.
pgsync::process_global! {
    static NEXT_OBJECT_ACCESS_HOOK: Mutex<Option<ObjectAccessHook>> = Mutex::new(None);
    static NEXT_OBJECT_ACCESS_HOOK_STR: Mutex<Option<ObjectAccessHookStr>> = Mutex::new(None);
}

/// One of the `REGRESS_*` bool globals, read from its custom GUC (see the
/// DIVERGENCE note above).
fn guc_bool(name: &str) -> bool {
    match guc::GetConfigOption(name, true, false) {
        Ok(Some(s)) => adt_bool::parse_bool(&s).unwrap_or(false),
        _ => false,
    }
}

fn REGRESS_audit() -> bool {
    guc_bool("test_oat_hooks.audit")
}

fn REGRESS_deny_object_access() -> bool {
    guc_bool("test_oat_hooks.deny_object_access")
}

fn REGRESS_deny_set_variable() -> bool {
    guc_bool("test_oat_hooks.deny_set_variable")
}

fn REGRESS_deny_alter_system() -> bool {
    guc_bool("test_oat_hooks.deny_alter_system")
}

fn loc(line: i32, funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("test_oat_hooks.c", line, funcname)
}

fn current_user_is_superuser() -> PgResult<bool> {
    superuser_seams::superuser_arg::call(miscinit::GetUserId())
}

/// `emit_audit_message` (test_oat_hooks.c:230-256): NOTICE with
/// ERRCODE_INTERNAL_ERROR, from a leader process only.
fn emit_audit_message(
    ty: &str,
    hook: &str,
    action: &str,
    obj_name: Option<&str>,
) -> PgResult<()> {
    if REGRESS_audit() && !parallel_seams::is_parallel_worker::call() {
        let who = if current_user_is_superuser()? { "superuser" } else { "non-superuser" };
        let msg = match obj_name {
            Some(obj_name) => format!("in {hook}: {who} {ty} {action} [{obj_name}]"),
            None => format!("in {hook}: {who} {ty} {action}"),
        };
        ereport(NOTICE)
            .errcode(ERRCODE_INTERNAL_ERROR)
            .errmsg(msg)
            .finish(loc(243, "emit_audit_message"))?;
    }
    Ok(())
}

fn audit_attempt(hook: &str, action: &str, obj_name: Option<&str>) -> PgResult<()> {
    emit_audit_message("attempting", hook, action, obj_name)
}

fn audit_success(hook: &str, action: &str, obj_name: Option<&str>) -> PgResult<()> {
    emit_audit_message("finished", hook, action, obj_name)
}

fn permission_denied(msg: String, line: i32, funcname: &'static str) -> Box<PgError> {
    Box::new(
        PgError::error(msg)
            .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .with_error_location(loc(line, funcname)),
    )
}

/// `REGRESS_object_access_hook_str` (test_oat_hooks.c:278-329).
fn REGRESS_object_access_hook_str(
    access: ObjectAccessType,
    class_id: Oid,
    obj_name: &str,
    sub_id: i32,
    arg: &mut ObjectAccessArg<'_>,
) -> PgResult<()> {
    audit_attempt("object_access_hook_str", &accesstype_to_string(access, sub_id), Some(obj_name))?;

    let next = *NEXT_OBJECT_ACCESS_HOOK_STR.lock().unwrap();
    if let Some(next) = next {
        next(access, class_id, obj_name, sub_id, arg)?;
    }

    if access == OAT_POST_ALTER {
        let sub = sub_id as AclMode;
        if (sub & ACL_SET) != 0 && (sub & ACL_ALTER_SYSTEM) != 0 {
            if REGRESS_deny_set_variable() && !current_user_is_superuser()? {
                return Err(permission_denied(
                    format!("permission denied: all privileges {obj_name}"),
                    297,
                    "REGRESS_object_access_hook_str",
                ));
            }
        } else if (sub & ACL_SET) != 0 {
            if REGRESS_deny_set_variable() && !current_user_is_superuser()? {
                return Err(permission_denied(
                    format!("permission denied: set {obj_name}"),
                    304,
                    "REGRESS_object_access_hook_str",
                ));
            }
        } else if (sub & ACL_ALTER_SYSTEM) != 0 {
            if REGRESS_deny_alter_system() && !current_user_is_superuser()? {
                return Err(permission_denied(
                    format!("permission denied: alter system set {obj_name}"),
                    311,
                    "REGRESS_object_access_hook_str",
                ));
            }
        } else {
            // elog(ERROR, ...): XX000.
            return Err(Box::new(
                PgError::error(format!("Unknown ParameterAclRelationId subId: {sub_id}"))
                    .with_sqlstate(ERRCODE_INTERNAL_ERROR)
                    .with_error_location(loc(320, "REGRESS_object_access_hook_str")),
            ));
        }
    }

    audit_success("object_access_hook_str", &accesstype_to_string(access, sub_id), Some(obj_name))
}

/// `REGRESS_object_access_hook` (test_oat_hooks.c:331-356).
fn REGRESS_object_access_hook(
    access: ObjectAccessType,
    class_id: Oid,
    object_id: Oid,
    sub_id: i32,
    arg: &mut ObjectAccessArg<'_>,
) -> PgResult<()> {
    audit_attempt(
        "object access",
        &accesstype_to_string(access, 0),
        Some(&accesstype_arg_to_string(access, arg)),
    )?;

    if REGRESS_deny_object_access() && !current_user_is_superuser()? {
        return Err(permission_denied(
            format!(
                "permission denied: {} [{}]",
                accesstype_to_string(access, 0),
                accesstype_arg_to_string(access, arg)
            ),
            341,
            "REGRESS_object_access_hook",
        ));
    }

    // Forward to next hook in the chain
    let next = *NEXT_OBJECT_ACCESS_HOOK.lock().unwrap();
    if let Some(next) = next {
        next(access, class_id, object_id, sub_id, arg)?;
    }

    audit_success(
        "object access",
        &accesstype_to_string(access, 0),
        Some(&accesstype_arg_to_string(access, arg)),
    )
}

/// `accesstype_to_string` (test_oat_hooks.c:434-473).
fn accesstype_to_string(access: ObjectAccessType, sub_id: i32) -> String {
    let ty = match access {
        OAT_POST_CREATE => "create",
        OAT_DROP => "drop",
        OAT_POST_ALTER => "alter",
        OAT_NAMESPACE_SEARCH => "namespace search",
        OAT_FUNCTION_EXECUTE => "execute",
        OAT_TRUNCATE => "truncate",
    };
    let sub = sub_id as AclMode;
    if (sub & ACL_SET) != 0 && (sub & ACL_ALTER_SYSTEM) != 0 {
        return format!("{ty} (subId=0x{sub_id:x}, all privileges)");
    }
    if (sub & ACL_SET) != 0 {
        return format!("{ty} (subId=0x{sub_id:x}, set)");
    }
    if (sub & ACL_ALTER_SYSTEM) != 0 {
        return format!("{ty} (subId=0x{sub_id:x}, alter system)");
    }
    format!("{ty} (subId=0x{sub_id:x})")
}

/// `accesstype_arg_to_string` (test_oat_hooks.c:475-539). The Rust hook arg
/// is an enum: `ObjectAccessArg::None` is C's NULL `arg` (the truncate and
/// function-execute invocations), so the "unexpected extra info pointer
/// received" arm is unreachable here.
fn accesstype_arg_to_string(access: ObjectAccessType, arg: &ObjectAccessArg<'_>) -> String {
    match arg {
        ObjectAccessArg::None => "extra info null".to_string(),
        ObjectAccessArg::PostCreate(pc_arg) if access == OAT_POST_CREATE => {
            (if pc_arg.is_internal { "internal" } else { "explicit" }).to_string()
        }
        ObjectAccessArg::Drop(drop_arg) if access == OAT_DROP => {
            let f = drop_arg.dropflags;
            format!(
                "{}{}{}{}{}{}",
                if f & PERFORM_DELETION_INTERNAL != 0 { "internal action," } else { "" },
                if f & PERFORM_DELETION_CONCURRENTLY != 0 { "concurrent drop," } else { "" },
                if f & PERFORM_DELETION_QUIETLY != 0 { "suppress notices," } else { "" },
                if f & PERFORM_DELETION_SKIP_ORIGINAL != 0 { "keep original object," } else { "" },
                if f & PERFORM_DELETION_SKIP_EXTENSIONS != 0 { "keep extensions," } else { "" },
                if f & PERFORM_DELETION_CONCURRENT_LOCK != 0 { "normal concurrent drop," } else { "" },
            )
        }
        ObjectAccessArg::PostAlter(pa_arg) if access == OAT_POST_ALTER => format!(
            "{} {} auxiliary object",
            if pa_arg.is_internal { "internal" } else { "explicit" },
            if pa_arg.auxiliary_id != InvalidOid { "with" } else { "without" }
        ),
        ObjectAccessArg::NamespaceSearch(ns_arg) if access == OAT_NAMESPACE_SEARCH => format!(
            "{}, {}",
            if ns_arg.ereport_on_violation { "report on violation" } else { "no report on violation" },
            if ns_arg.result { "allowed" } else { "denied" }
        ),
        // A non-NULL arg for OAT_TRUNCATE / OAT_FUNCTION_EXECUTE, or an arg
        // shape the access type does not own.
        _ => match access {
            OAT_TRUNCATE | OAT_FUNCTION_EXECUTE => {
                "unexpected extra info pointer received".to_string()
            }
            _ => "unknown".to_string(),
        },
    }
}

fn lookup(_function: &str) -> Option<PGFunction> {
    // test_oat_hooks exposes no SQL functions.
    None
}

/// `_PG_init` (test_oat_hooks.c:70-226): the GUCs, the reserved prefix, then
/// the hooks (saving the previous values for the chain).
fn pg_init() -> PgResult<()> {
    for &(name, short_desc, context) in GUCS {
        guc::DefineCustomStringVariable(
            name,
            Some(short_desc),
            None,
            Some("off"),
            context,
            types_guc::GUC_NOT_IN_SAMPLE,
        )?;
    }
    guc::MarkGUCPrefixReserved(LIBRARY);

    // Object access hook
    let prev = objectaccess::set_object_access_hook(Some(REGRESS_object_access_hook));
    *NEXT_OBJECT_ACCESS_HOOK.lock().unwrap() = prev;

    // Object access hook str
    let prev_str = objectaccess::set_object_access_hook_str(Some(REGRESS_object_access_hook_str));
    *NEXT_OBJECT_ACCESS_HOOK_STR.lock().unwrap() = prev_str;

    Ok(())
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: Some(pg_init),
    });
}

#[cfg(test)]
mod tests;
