// NB: not `#![no_std]` — the `fmgr_builtins` registration layer raises a
// builtin's `ereport(ERROR)` through `std::panic::panic_any` (the one dispatch
// point every builtin crosses, `invoke_pgfunction`'s `catch_unwind`), which
// needs `std`. The rest of the crate remains `alloc`-only in spirit.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// The shared `PgResult` carries an un-boxed `PgError`; the un-boxed return is
// the project-wide error contract, so we accept the large-`Err` lint crate-wide.
#![allow(clippy::result_large_err)]

//! `backend-utils-misc-guc-funcs` — Rust port of PostgreSQL 18.3
//! `src/backend/utils/misc/guc_funcs.c`: the SQL/statement layer on top of the
//! GUC variable machinery (the W1 core, [`backend_utils_misc_guc`]).
//!
//! # What is ported (1:1 control flow)
//!
//! * [`ExecSetVariableStmt`] — the `SET` / `RESET` statement executor.
//! * [`ExtractSetVariableArgs`] / [`flatten_set_variable_args`] — converting a
//!   `SET` parsenode list to GUC's flat string form.
//! * [`SetPGVariable`] — the easily-callable `SET` entry point.
//! * [`GetPGVariable`], [`GetPGVariableResultDesc`], `ShowGUCConfigOption`,
//!   `ShowAllGUCConfig` — the `SHOW` / `SHOW ALL` statement.
//! * [`ConfigOptionIsVisible`] — the `pg_read_all_settings` visibility rule.
//! * [`get_explain_guc_options`] (guc.c:5337) over the live registry.
//!
//! The GUC **core** operations (`set_config_option`, `GetConfigOptionByName`,
//! `find_option`, `ResetAllOptions`, `get_guc_variables`, `ShowGUCOption`) are
//! called on the W1 core ([`backend_utils_misc_guc`]) directly. The remaining
//! cross-subsystem calls (process / permission / transaction state, the
//! snapshot adoption, the catalog post-alter hook, identifier quoting, and the
//! executor tuple-output path) cross seams declared in
//! [`backend_utils_misc_guc_funcs_seams`].
//!
//! # Reconciliation with the K1 parse-tree model
//!
//! In the K1 `types-parsenodes` `Node` model, a `SET` argument `A_Const` is
//! carried **directly** as its value node (`Node::Integer` / `Node::Float` /
//! `Node::String`) — there is no `A_Const` wrapper and no `TypeCast` variant.
//! So `flatten_set_variable_args` switches on the `Node` value variant directly
//! (the C `switch (nodeTag(&con->val))`), and the `SET TIME ZONE INTERVAL '...'`
//! branch (which the C reaches only through a `TypeCast`) is structurally
//! unreachable until the K1 model gains `TypeCast`. The `default` arm raises the
//! same `unrecognized node type` error C does.
//!
//! # What is deferred (honest partial)
//!
//! The `PG_FUNCTION_ARGS` / `Datum` SQL-callable functions
//! (`set_config_by_name`, `pg_settings_get_flags`, `show_config_by_name`,
//! `show_config_by_name_missing_ok`, `show_all_file_settings`) belong to the
//! project-wide fmgr/Datum-layer deferral. They live behind a loud-panic module
//! ([`fmgr_deferred`]) — never a pretend success (and never a
//! placeholder/unported stub).
//!
//! `show_all_settings` (OID 2084, the `pg_settings` view) IS ported: its row
//! projection ([`GetConfigOptionValues`](get_config_option_values) /
//! [`pg_settings_rows`]) lives here, and the executor-frame SRF adapter that runs
//! `InitMaterializedSRF` and emits the 17-column rows lives in
//! `backend-executor-execSRF`.

extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec;
use alloc::vec::Vec;

use core::cmp::Ordering;

use mcx::Mcx;

use backend_utils_error::ereport;
use types_error::{
    ErrorLevel, PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_INVALID_TRANSACTION_STATE, ERROR,
};

use backend_utils_misc_guc::{
    guc_name_compare, live, registry::GucVariable, set_config_option_global,
    GUC_ACTION_LOCAL, GUC_ACTION_SET,
};
use backend_utils_misc_guc::model::config_generic;
use backend_utils_misc_guc::registry::{get_config_option_by_name, show_guc_option, GucRegistry};

use types_acl::acl::ACL_SET;
use types_catalog::catalog::PARAMETER_ACL_RELATION_ID as ParameterAclRelationId;
use types_core::primitive::Oid;
use types_guc::{
    GucContext, GUC_EXPLAIN, GUC_LIST_INPUT, GUC_LIST_QUOTE, GUC_NO_SHOW_ALL,
    GUC_SUPERUSER_ONLY, PGC_SUSET, PGC_USERSET, PGC_S_DEFAULT, PGC_S_SESSION,
};
use types_tuple::heaptuple::{TupleDesc, TEXTOID};

use types_parsenodes::{Node, VariableSetKind, VariableSetStmt};

use backend_utils_misc_guc_funcs_seams as seam;
use backend_utils_time_snapmgr_seams as snapmgr_seam;

/// The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the `text`-boundary
/// `guc_funcs.c` SQL functions (`current_setting` / `set_config`), registered
/// into the fmgr-core builtin table so `fmgr_isbuiltin` resolves them.
pub mod fmgr_builtins;

#[cfg(test)]
mod tests;

// ---------------------------------------------------------------------------
// Catalog / type constants referenced by guc_funcs.c.
// ---------------------------------------------------------------------------

/// `ROLE_PG_READ_ALL_SETTINGS` (catalog/pg_authid_d.h) = 3374.
pub const ROLE_PG_READ_ALL_SETTINGS: Oid = 3374;

/// `GucAction` (`utils/guc.h`). `GUC_ACTION_LOCAL` if `is_local`, else
/// `GUC_ACTION_SET`.
pub type GucAction = u32;

/// C: `superuser() ? PGC_SUSET : PGC_USERSET`.
#[inline]
fn suset_or_userset() -> GucContext {
    if seam::superuser::call() {
        PGC_SUSET
    } else {
        PGC_USERSET
    }
}

/// `set_config_option(name, value, context, source, action, changeVal=true,
/// elevel=0, is_reload=false)` (guc_funcs.c) over the W1 core's live store. The
/// 8-arg C wrapper resolves `srole = GetUserId()` and `elevel = 0` ("let the
/// GUC core choose"); the core's `set_config_option_global` carries both.
fn set_config_option_call(
    name: &str,
    value: Option<&str>,
    context: GucContext,
    action: GucAction,
) -> PgResult<i32> {
    set_config_option_global(
        name,
        value,
        context,
        PGC_S_SESSION,
        seam::get_user_id::call(),
        action,
        true,
        ErrorLevel(0),
        false,
    )
}

// ===========================================================================
// SET command — ExecSetVariableStmt (guc_funcs.c:42).
// ===========================================================================

/// `ExecSetVariableStmt(stmt, isTopLevel)` (guc_funcs.c:42): execute a `SET` /
/// `RESET` statement.
pub fn ExecSetVariableStmt(stmt: &VariableSetStmt, isTopLevel: bool) -> PgResult<()> {
    let action: GucAction = if stmt.is_local {
        GUC_ACTION_LOCAL
    } else {
        GUC_ACTION_SET
    };

    // Workers synchronize these parameters at the start of the parallel
    // operation; then, we block SET during the operation.
    if seam::is_in_parallel_mode::call() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_TRANSACTION_STATE)
            .errmsg("cannot set parameters during a parallel operation")
            .into_error());
    }

    let name = stmt.name.as_deref().unwrap_or("");

    match stmt.kind {
        VariableSetKind::SetValue | VariableSetKind::SetCurrent => {
            if stmt.is_local {
                seam::warn_no_transaction_block::call(isTopLevel, "SET LOCAL".to_string());
            }
            let value = ExtractSetVariableArgs(stmt)?;
            let _ = set_config_option_call(name, value.as_deref(), suset_or_userset(), action)?;
        }
        VariableSetKind::SetMulti => {
            // Special-case SQL syntaxes. The TRANSACTION and SESSION
            // CHARACTERISTICS cases effectively set more than one variable per
            // statement. TRANSACTION SNAPSHOT only takes one argument, but we
            // put it here anyway since it's a special case and not related to
            // any GUC variable.
            if name == "TRANSACTION" {
                seam::warn_no_transaction_block::call(isTopLevel, "SET TRANSACTION".to_string());

                for item_node in &stmt.args {
                    let item = as_def_elem(item_node)?;
                    let defname = item.defname.as_deref().unwrap_or("");
                    let arg = item.arg.as_deref();
                    if defname == "transaction_isolation" {
                        SetPGVariable("transaction_isolation", arg, stmt.is_local)?;
                    } else if defname == "transaction_read_only" {
                        SetPGVariable("transaction_read_only", arg, stmt.is_local)?;
                    } else if defname == "transaction_deferrable" {
                        SetPGVariable("transaction_deferrable", arg, stmt.is_local)?;
                    } else {
                        return Err(ereport(ERROR)
                            .errmsg_internal(format!(
                                "unexpected SET TRANSACTION element: {defname}"
                            ))
                            .into_error());
                    }
                }
            } else if name == "SESSION CHARACTERISTICS" {
                for item_node in &stmt.args {
                    let item = as_def_elem(item_node)?;
                    let defname = item.defname.as_deref().unwrap_or("");
                    let arg = item.arg.as_deref();
                    if defname == "transaction_isolation" {
                        SetPGVariable("default_transaction_isolation", arg, stmt.is_local)?;
                    } else if defname == "transaction_read_only" {
                        SetPGVariable("default_transaction_read_only", arg, stmt.is_local)?;
                    } else if defname == "transaction_deferrable" {
                        SetPGVariable("default_transaction_deferrable", arg, stmt.is_local)?;
                    } else {
                        return Err(ereport(ERROR)
                            .errmsg_internal(format!("unexpected SET SESSION element: {defname}"))
                            .into_error());
                    }
                }
            } else if name == "TRANSACTION SNAPSHOT" {
                // con = linitial_node(A_Const, stmt->args)
                let con = linitial_a_const(&stmt.args)?;

                if stmt.is_local {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg("SET LOCAL TRANSACTION SNAPSHOT is not implemented")
                        .into_error());
                }

                seam::warn_no_transaction_block::call(isTopLevel, "SET TRANSACTION".to_string());
                // ImportSnapshot(strVal(&con->val))
                let sval = a_const_strval(con)?;
                snapmgr_seam::import_snapshot::call(sval)?;
            } else {
                return Err(ereport(ERROR)
                    .errmsg_internal(format!("unexpected SET MULTI element: {name}"))
                    .into_error());
            }
        }
        VariableSetKind::SetDefault | VariableSetKind::Reset => {
            // SET ... TO DEFAULT (and RESET) both flow to set_config_option with
            // value == NULL.
            if stmt.is_local && stmt.kind == VariableSetKind::SetDefault {
                seam::warn_no_transaction_block::call(isTopLevel, "SET LOCAL".to_string());
            }
            let _ = set_config_option_call(name, None, suset_or_userset(), action)?;
        }
        VariableSetKind::ResetAll => {
            live::reset_all_options_global();
        }
    }

    // Invoke the post-alter hook for setting this GUC variable, by name.
    // InvokeObjectPostAlterHookArgStr(ParameterAclRelationId, stmt->name,
    //                                 ACL_SET, stmt->kind, false);
    // C: subId = ACL_SET (the access mode), auxiliaryId = (Oid) stmt->kind.
    seam::invoke_object_post_alter_hook_arg_str::call(
        ParameterAclRelationId,
        name.to_string(),
        ACL_SET as i32,
        variable_set_kind_as_subid(stmt.kind) as Oid,
        false,
    )?;
    Ok(())
}

/// C: `(int) stmt->kind` — the `VariableSetKind` enumerator passed as the
/// post-alter hook `subId`.
fn variable_set_kind_as_subid(kind: VariableSetKind) -> i32 {
    match kind {
        VariableSetKind::SetValue => 0,
        VariableSetKind::SetDefault => 1,
        VariableSetKind::SetCurrent => 2,
        VariableSetKind::SetMulti => 3,
        VariableSetKind::Reset => 4,
        VariableSetKind::ResetAll => 5,
    }
}

// ===========================================================================
// ExtractSetVariableArgs (guc_funcs.c:166).
// ===========================================================================

/// `ExtractSetVariableArgs(stmt)` (guc_funcs.c:166): the value to assign for a
/// `VariableSetStmt`, or `None` if it is a `RESET`. Exported for use by
/// `ALTER ROLE SET` etc.
pub fn ExtractSetVariableArgs(stmt: &VariableSetStmt) -> PgResult<Option<String>> {
    let name = stmt.name.as_deref().unwrap_or("");
    match stmt.kind {
        VariableSetKind::SetValue => flatten_set_variable_args(name, &stmt.args),
        // GetConfigOptionByName(stmt->name, NULL, false) — value only.
        VariableSetKind::SetCurrent => config_option_value(name, false),
        _ => Ok(None),
    }
}

// ===========================================================================
// flatten_set_variable_args (guc_funcs.c:191).
// ===========================================================================

/// `flatten_set_variable_args(name, args)` (guc_funcs.c:191): convert a `SET`
/// parsenode list to GUC's flat string representation. Returns `None` for
/// `SET ... TO DEFAULT` (empty `args`).
pub fn flatten_set_variable_args(name: &str, args: &[Node]) -> PgResult<Option<String>> {
    // Fast path if just DEFAULT
    if args.is_empty() {
        return Ok(None);
    }

    // Get flags for the variable; if it's not known, use default flags.
    // (Caller might throw error later, but not our business to do so here.)
    // record = find_option(name, false, true, WARNING);
    let flags = find_option_flags(name).unwrap_or(0);

    // Complain if list input and non-list variable
    if (flags & GUC_LIST_INPUT) == 0 && args.len() != 1 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("SET {name} takes only one argument"))
            .into_error());
    }

    let mut buf = String::new();

    // Each list member is an `A_Const` value node. (In C the member may be an
    // `A_Const` within a `TypeCast`, for `SET TIME ZONE INTERVAL`; the K1
    // parse-tree model has no `TypeCast`, so that branch is unreachable — see
    // the module docs.) The K1 model carries the `A_Const` value directly as a
    // `Node::Integer` / `Node::Float` / `Node::String`.
    for (idx, arg_node) in args.iter().enumerate() {
        // if (l != list_head(args)) appendStringInfoString(&buf, ", ");
        if idx != 0 {
            buf.push_str(", ");
        }

        // switch (nodeTag(&con->val))
        match arg_node {
            // case T_Integer: appendStringInfo(&buf, "%d", intVal(&con->val));
            Node::Integer(i) => {
                buf.push_str(&format!("{}", i.ival));
            }
            // case T_Float: appendStringInfoString(&buf, castNode(Float, ...)->fval);
            Node::Float(f) => {
                // represented as a string, so just copy it
                buf.push_str(f.fval.as_deref().unwrap_or(""));
            }
            // case T_String: val = strVal(&con->val);
            Node::String(s) => {
                let val = s.sval.as_deref().unwrap_or("");
                // Plain string literal or identifier. For quote mode, quote it
                // if it's not a vanilla identifier.
                if (flags & GUC_LIST_QUOTE) != 0 {
                    buf.push_str(&seam::quote_identifier::call(val.to_string()));
                } else {
                    buf.push_str(val);
                }
            }
            // default: elog(ERROR, "unrecognized node type: %d", nodeTag(&con->val));
            other => {
                return Err(unrecognized_node_type(other));
            }
        }
    }

    Ok(Some(buf))
}

// ===========================================================================
// SetPGVariable (guc_funcs.c:314).
// ===========================================================================

/// `SetPGVariable(name, args, is_local)` (guc_funcs.c:314): `SET` exported as an
/// easily-callable function. `args == None` (or empty) is `SET TO DEFAULT`,
/// equivalent to `RESET`.
///
/// The C signature is `SetPGVariable(const char *name, List *args, bool)`; the
/// in-tree call sites in `ExecSetVariableStmt` pass `list_make1(item->arg)`,
/// i.e. a one-element list. We accept the single optional node directly,
/// matching every in-tree caller.
pub fn SetPGVariable(name: &str, arg: Option<&Node>, is_local: bool) -> PgResult<()> {
    // flatten_set_variable_args(name, args) where args == list_make1(arg) (or NIL).
    let args: &[Node] = match arg {
        Some(n) => core::slice::from_ref(n),
        None => &[],
    };
    let argstring = flatten_set_variable_args(name, args)?;

    // Note SET DEFAULT (argstring == NULL) is equivalent to RESET
    let _ = set_config_option_call(
        name,
        argstring.as_deref(),
        suset_or_userset(),
        if is_local {
            GUC_ACTION_LOCAL
        } else {
            GUC_ACTION_SET
        },
    )?;
    Ok(())
}

// ===========================================================================
// ALTER SYSTEM — AlterSystemSetConfigFile (guc.c:4607).
// ===========================================================================

/// `PG_AUTOCONF_FILENAME` (`utils/conffiles.h`).
const PG_AUTOCONF_FILENAME: &str = "postgresql.auto.conf";
/// `AutoFile` LWLock offset (`storage/lwlocklist.h` index 35), serializing
/// updates of `postgresql.auto.conf`.
const AUTO_FILE_LOCK: usize = 35;
/// `CONF_FILE_START_DEPTH` (guc-file.l): the include depth of the top-level
/// config file.
const CONF_FILE_START_DEPTH: i32 = 0;

/// `replace_auto_config_value(head, tail, name, value)` (guc.c:4537): remove any
/// existing entry (or entries — external tools may add duplicates) for `name`
/// from the parsed `postgresql.auto.conf` list, then, unless `value` is `None`
/// (a deletion), append a fresh `name = value` entry. The C doubly-linked list
/// is the owning `Vec` here; removal preserves the relative order of the rest,
/// and the new entry is appended at the tail.
fn replace_auto_config_value(
    list: &mut Vec<backend_utils_misc_guc_file::ConfigVariable>,
    name: &str,
    value: Option<&str>,
) {
    use backend_utils_misc_guc_file::ConfigVariable;

    // Remove any existing match(es) for "name".
    list.retain(|item| match &item.name {
        Some(item_name) => guc_name_compare(item_name, name) != Ordering::Equal,
        None => true,
    });

    // Done if we're trying to delete it.
    let Some(value) = value else {
        return;
    };

    // OK, append a new entry (new item has no location: filename "", line 0).
    list.push(ConfigVariable::setting(
        name.to_string(),
        value.to_string(),
        alloc::string::String::new().into(),
        0,
    ));
}

/// `write_auto_conf_file`'s buffer construction (guc.c:4469): the warning header
/// plus one `name = 'value'\n` line per entry, the value escaped by
/// `escape_single_quotes_ascii`. The actual fd write/fsync is done by the
/// `write_auto_conf_atomic` seam; this builds the bytes it writes.
fn render_auto_conf_file(list: &[backend_utils_misc_guc_file::ConfigVariable]) -> String {
    let mut buf = String::new();
    // Emit file header containing warning comment.
    buf.push_str("# Do not edit this file manually!\n");
    buf.push_str("# It will be overwritten by the ALTER SYSTEM command.\n");
    // Emit each parameter, properly quoting the value.
    for item in list {
        // Error records (item->name == NULL) are never present in a list built
        // from a successful parse; skip them defensively.
        let (Some(name), Some(value)) = (&item.name, &item.value) else {
            continue;
        };
        buf.push_str(name);
        buf.push_str(" = '");
        buf.push_str(&backend_utils_misc_guc::escape_single_quotes_ascii(value));
        buf.push_str("'\n");
    }
    buf
}

/// `AlterSystemSetConfigFile(altersysstmt)` (guc.c:4607): execute `ALTER SYSTEM`.
/// Read the old `postgresql.auto.conf`, merge in the new variable value (or
/// remove it for `RESET`, or start empty for `RESET ALL`), and write out an
/// updated file via a temp file + atomic rename. An LWLock (`AutoFileLock`)
/// serializes updates of the configuration file; on error the original file is
/// left intact.
pub fn AlterSystemSetConfigFile(stmt: &VariableSetStmt) -> PgResult<()> {
    use backend_storage_lmgr_lwlock_seams::lwlock_acquire_main;
    use backend_utils_misc_guc_file::{FreeConfigVariables, ParseConfigFp};
    use types_storage::LWLockMode::LW_EXCLUSIVE;

    // Extract statement arguments: name = altersysstmt->setstmt->name.
    let name = stmt.name.as_deref().unwrap_or("");

    // if (!AllowAlterSystem) ereport(ERROR, FEATURE_NOT_SUPPORTED).
    if !backend_utils_misc_guc_tables::backing::AllowAlterSystem() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("ALTER SYSTEM is not allowed in this environment")
            .into_error());
    }

    let mut resetall = false;
    let value: Option<String> = match stmt.kind {
        VariableSetKind::SetValue => ExtractSetVariableArgs(stmt)?,
        VariableSetKind::SetDefault | VariableSetKind::Reset => None,
        VariableSetKind::ResetAll => {
            resetall = true;
            None
        }
        other => {
            return Err(ereport(ERROR)
                .errmsg_internal(format!("unrecognized alter system stmt type: {other:?}"))
                .into_error());
        }
    };

    // Check permission to run ALTER SYSTEM on the target variable.
    if !seam::superuser::call() {
        if resetall {
            return Err(ereport(ERROR)
                .errcode(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg("permission denied to perform ALTER SYSTEM RESET ALL")
                .into_error());
        } else {
            // pg_parameter_aclcheck(name, GetUserId(), ACL_ALTER_SYSTEM).
            let ok = seam::pg_parameter_aclcheck_ok::call(
                name.to_string(),
                seam::get_user_id::call(),
                types_acl::acl::ACL_ALTER_SYSTEM,
            )?;
            if !ok {
                return Err(ereport(ERROR)
                    .errcode(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE)
                    .errmsg(format!("permission denied to set parameter \"{name}\""))
                    .into_error());
            }
        }
    }

    // Unless it's RESET_ALL, validate the target variable and value.
    if !resetall {
        backend_utils_misc_guc::validate_auto_config_value(name, value.as_deref())?;
    }

    // PG_AUTOCONF_FILENAME and its temp file are always in the data directory,
    // referenced by simple relative paths.
    let auto_conf_file_name = PG_AUTOCONF_FILENAME.to_string();
    let auto_conf_tmp_file_name = format!("{PG_AUTOCONF_FILENAME}.tmp");

    // Only one backend may operate on PG_AUTOCONF_FILENAME at a time. Use
    // AutoFileLock; hold it while reading the old file contents. The guard
    // releases on drop (the abort path) and is released explicitly at the end.
    let auto_file_lock = lwlock_acquire_main::call(AUTO_FILE_LOCK, LW_EXCLUSIVE)?;

    let mut head: Vec<backend_utils_misc_guc_file::ConfigVariable> = Vec::new();

    // If we're resetting everything, no need to read/parse the old file.
    if !resetall {
        // if (stat(AutoConfFileName, &st) == 0) { open + ParseConfigFp }
        match std::fs::metadata(&auto_conf_file_name) {
            Ok(_) => {
                // infile = AllocateFile(AutoConfFileName, "r");
                let contents = std::fs::read(&auto_conf_file_name).map_err(|e| {
                    let mut b = ereport(ERROR);
                    if let Some(errno) = e.raw_os_error() {
                        b = b.with_saved_errno(errno).errcode_for_file_access();
                    }
                    b.errmsg(format!(
                        "could not open file \"{auto_conf_file_name}\": %m"
                    ))
                    .into_error()
                })?;
                // if (!ParseConfigFp(infile, AutoConfFileName, ..., LOG, &head, &tail))
                //     ereport(ERROR, CONFIG_FILE_ERROR).
                let parsed = ParseConfigFp(
                    &contents,
                    std::path::Path::new(&auto_conf_file_name),
                    CONF_FILE_START_DEPTH,
                    types_error::LOG,
                    &mut head,
                )?;
                if !parsed {
                    FreeConfigVariables(&mut head);
                    return Err(ereport(ERROR)
                        .errcode(types_error::ERRCODE_CONFIG_FILE_ERROR)
                        .errmsg(format!(
                            "could not parse contents of file \"{auto_conf_file_name}\""
                        ))
                        .into_error());
                }
            }
            Err(_) => {
                // No existing file: start with an empty list.
            }
        }

        // Replace any existing entry with the new value, or add it if absent.
        replace_auto_config_value(&mut head, name, value.as_deref());
    }

    // Invoke the post-alter hook for setting this GUC variable, by name, before
    // touching any files (ALTER SYSTEM is not transactional). "name" is the
    // empty string in the RESET ALL case (C passes NULL).
    // InvokeObjectPostAlterHookArgStr(ParameterAclRelationId, name,
    //                                 ACL_ALTER_SYSTEM, stmt->kind, false).
    seam::invoke_object_post_alter_hook_arg_str::call(
        ParameterAclRelationId,
        name.to_string(),
        types_acl::acl::ACL_ALTER_SYSTEM as i32,
        variable_set_kind_as_subid(stmt.kind) as Oid,
        false,
    )?;

    // To ensure crash safety, write the new file data to a temp file then
    // atomically rename it into place (the seam owns the open/write/fsync/
    // rename + PG_CATCH unlink cleanup).
    let content = render_auto_conf_file(&head);
    let write_result = backend_storage_file_seams::write_auto_conf_atomic::call(
        &auto_conf_tmp_file_name,
        &auto_conf_file_name,
        content.as_bytes(),
    );

    FreeConfigVariables(&mut head);

    // LWLockRelease(AutoFileLock) — surface the write error after releasing.
    write_result?;
    auto_file_lock.release()?;
    Ok(())
}

// ===========================================================================
// SHOW command — GetPGVariable / GetPGVariableResultDesc (guc_funcs.c:382/394).
// ===========================================================================

/// `GetPGVariable(name, dest)` (guc_funcs.c:382): the `SHOW` statement.
pub fn GetPGVariable<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
    dest: types_nodes::parsestmt::DestReceiverHandle,
) -> PgResult<()> {
    if guc_name_compare(name, "all") == Ordering::Equal {
        ShowAllGUCConfig(mcx, dest)
    } else {
        ShowGUCConfigOption(mcx, name, dest)
    }
}

/// `GetPGVariableResultDesc(name)` (guc_funcs.c:394): a tuple descriptor for the
/// `SHOW` result.
pub fn GetPGVariableResultDesc<'mcx>(mcx: Mcx<'mcx>, name: &str) -> PgResult<TupleDesc<'mcx>> {
    if guc_name_compare(name, "all") == Ordering::Equal {
        // need a tuple descriptor representing three TEXT columns
        let mut tupdesc = backend_access_common_tupdesc::CreateTemplateTupleDesc(mcx, 3)?;
        backend_access_common_tupdesc::TupleDescInitEntry(
            &mut tupdesc, 1, Some("name"), TEXTOID, -1, 0,
        )?;
        backend_access_common_tupdesc::TupleDescInitEntry(
            &mut tupdesc, 2, Some("setting"), TEXTOID, -1, 0,
        )?;
        backend_access_common_tupdesc::TupleDescInitEntry(
            &mut tupdesc, 3, Some("description"), TEXTOID, -1, 0,
        )?;
        Ok(Some(mcx::alloc_in(mcx, tupdesc)?))
    } else {
        // Get the canonical spelling of name
        // (void) GetConfigOptionByName(name, &varname, false);
        let varname = config_option_canonical_name(name, false)?.unwrap_or_default();

        // need a tuple descriptor representing a single TEXT column
        let mut tupdesc = backend_access_common_tupdesc::CreateTemplateTupleDesc(mcx, 1)?;
        backend_access_common_tupdesc::TupleDescInitEntry(
            &mut tupdesc, 1, Some(&varname), TEXTOID, -1, 0,
        )?;
        Ok(Some(mcx::alloc_in(mcx, tupdesc)?))
    }
}

/// `ShowGUCConfigOption(name, dest)` (guc_funcs.c:427): `SHOW` one variable.
fn ShowGUCConfigOption<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
    dest: types_nodes::parsestmt::DestReceiverHandle,
) -> PgResult<()> {
    // Get the value and canonical spelling of name
    // value = GetConfigOptionByName(name, &varname, false);
    let (value, varname) = config_option_value_and_name(name, false)?.ok_or_else(|| {
        // missing_ok == false, so the lookup returns Err for an unknown name;
        // None should not occur here, but guard rather than panic.
        ereport(ERROR)
            .errmsg(format!("unrecognized configuration parameter \"{name}\""))
            .into_error()
    })?;

    // need a tuple descriptor representing a single TEXT column
    // tupdesc = CreateTemplateTupleDesc(1);
    // TupleDescInitBuiltinEntry(tupdesc, 1, varname, TEXTOID, -1, 0);
    //
    // C uses TupleDescInitBuiltinEntry (not TupleDescInitEntry) here: the
    // latter does a SearchSysCache1(TYPEOID) lookup, which a database-less
    // physical walsender (no Phase-3 relcache; pg_class/pg_type not nailed)
    // cannot satisfy without recursing into RelationBuildDesc(pg_class). The
    // builtin variant uses hardcoded type info, so SHOW works without a
    // database (this is what `pg_basebackup`'s `SHOW data_directory_mode` on a
    // physical replication connection relies on).
    let mut tupdesc = backend_access_common_tupdesc::CreateTemplateTupleDesc(mcx, 1)?;
    backend_access_common_tupdesc::TupleDescInitBuiltinEntry(
        &mut tupdesc, 1, &varname, TEXTOID, -1, 0,
    )?;
    let tupdesc = Some(mcx::alloc_in(mcx, tupdesc)?);

    // prepare for projection of tuples
    let mut tstate = seam::begin_tup_output_tupdesc::call(mcx, dest, tupdesc)?;

    // Send it
    seam::do_text_output_oneline::call(mcx, &mut tstate, value)?;

    seam::end_tup_output::call(mcx, tstate)?;
    Ok(())
}

/// `ShowAllGUCConfig(dest)` (guc_funcs.c:455): the `SHOW ALL` command.
fn ShowAllGUCConfig<'mcx>(
    mcx: Mcx<'mcx>,
    dest: types_nodes::parsestmt::DestReceiverHandle,
) -> PgResult<()> {
    // need a tuple descriptor representing three TEXT columns
    // tupdesc = CreateTemplateTupleDesc(3);
    // TupleDescInitBuiltinEntry(tupdesc, 1, "name",        TEXTOID, -1, 0);
    // TupleDescInitBuiltinEntry(tupdesc, 2, "setting",     TEXTOID, -1, 0);
    // TupleDescInitBuiltinEntry(tupdesc, 3, "description", TEXTOID, -1, 0);
    //
    // C uses TupleDescInitBuiltinEntry (hardcoded type info, no syscache) so
    // `SHOW ALL` runs on a database-less walsender. See ShowGUCConfigOption.
    let mut tupdesc = backend_access_common_tupdesc::CreateTemplateTupleDesc(mcx, 3)?;
    backend_access_common_tupdesc::TupleDescInitBuiltinEntry(
        &mut tupdesc, 1, "name", TEXTOID, -1, 0,
    )?;
    backend_access_common_tupdesc::TupleDescInitBuiltinEntry(
        &mut tupdesc, 2, "setting", TEXTOID, -1, 0,
    )?;
    backend_access_common_tupdesc::TupleDescInitBuiltinEntry(
        &mut tupdesc, 3, "description", TEXTOID, -1, 0,
    )?;
    let tupdesc = Some(mcx::alloc_in(mcx, tupdesc)?);

    // prepare for projection of tuples
    let mut tstate = seam::begin_tup_output_tupdesc::call(mcx, dest, tupdesc)?;

    // collect the variables, in sorted order, and emit each visible row.
    let rows = with_registry(|reg| {
        let mut rows: Vec<Vec<Option<String>>> = Vec::new();
        for var in sorted_variables(reg) {
            let conf = var.gen();

            // skip if marked NO_SHOW_ALL
            if (conf.flags & GUC_NO_SHOW_ALL) != 0 {
                continue;
            }

            // return only options visible to the current user
            if !ConfigOptionIsVisible(conf) {
                continue;
            }

            // values[0] = name; values[1] = ShowGUCOption(conf, true);
            // values[2] = conf->short_desc; with the matching isnull flags.
            let vname = Some(conf.name.to_string());
            let setting = Some(show_guc_option(var, true));
            let short_desc = conf.short_desc.map(|s| s.to_string());

            rows.push(vec![vname, setting, short_desc]);
        }
        rows
    });

    for values in rows {
        // send it to dest
        seam::do_tup_output::call(mcx, &mut tstate, values)?;
    }

    seam::end_tup_output::call(mcx, tstate)?;
    Ok(())
}

// ===========================================================================
// ConfigOptionIsVisible (guc_funcs.c:580).
// ===========================================================================

/// `ConfigOptionIsVisible(conf)` (guc_funcs.c:580): is the GUC variable visible
/// to the current user?
pub fn ConfigOptionIsVisible(conf: &config_generic) -> bool {
    if (conf.flags & GUC_SUPERUSER_ONLY) != 0
        && !seam::has_privs_of_role::call(seam::get_user_id::call(), ROLE_PG_READ_ALL_SETTINGS)
    {
        false
    } else {
        true
    }
}

// ===========================================================================
// get_explain_guc_options (guc.c:5337).
// ===========================================================================

/// `get_explain_guc_options(&num)` (guc.c:5337) over the live registry: the
/// names of the GUCs whose source is not `PGC_S_DEFAULT` (the `guc_nondef_list`
/// membership test), that are marked `GUC_EXPLAIN`, that are visible to the
/// current user, and whose current value differs from the boot value. The C
/// returns `config_generic **`; here the names are returned (the consumer
/// renders each via `GetConfigOptionByName`).
pub fn get_explain_guc_options() -> Vec<String> {
    with_registry(|reg| {
        let mut result: Vec<String> = Vec::new();
        for var in reg.iter() {
            let gen = var.gen();

            // We need only consider GUCs with source not PGC_S_DEFAULT.
            if gen.source == PGC_S_DEFAULT {
                continue;
            }
            // return only parameters marked for inclusion in explain
            if (gen.flags & GUC_EXPLAIN) == 0 {
                continue;
            }
            // return only options visible to the current user
            if !ConfigOptionIsVisible(gen) {
                continue;
            }

            // return only options that are different from their boot values
            // (C: `boot_val != *conf->variable`).
            let modified = match var {
                GucVariable::Bool(c) => c.boot_val != c.variable.read(),
                GucVariable::Int(c) => c.boot_val != c.variable.read(),
                GucVariable::Real(c) => c.boot_val != c.variable.read(),
                GucVariable::String(c) => c.boot_val != c.variable.read(),
                GucVariable::Enum(c) => c.boot_val != c.variable.read(),
            };
            if modified {
                result.push(var.name_pub().to_string());
            }
        }
        result
    })
}

// ===========================================================================
// GUC-core call helpers (on the W1 core's live store).
// ===========================================================================

/// Run `f` against the live GUC registry. The C accesses the file-static
/// `guc_hashtab` directly; here it is the W1 core's process-global store.
/// Panics loudly if called before `initialize_guc_options` (mirrors the C
/// requirement that the GUC table is built before any SET/SHOW).
fn with_registry<R>(f: impl FnOnce(&GucRegistry) -> R) -> R {
    live::with_store(f).unwrap_or_else(|| {
        panic!(
            "GUC SET/SHOW reached before initialize_guc_options seeded the global GUC store"
        )
    })
}

/// `find_option(name, false, true, WARNING)->flags` (guc.c:1235): the flag word
/// of a known variable, or `None` if unknown.
pub(crate) fn find_option_flags(name: &str) -> Option<i32> {
    with_registry(|reg| reg.find_option(name).map(|var| var.gen().flags))
}

/// `GetConfigOptionByName(name, NULL, missing_ok)` (guc.c:5438): the current
/// value of a GUC (no canonical name needed).
fn config_option_value(name: &str, missing_ok: bool) -> PgResult<Option<String>> {
    with_registry(|reg| get_config_option_by_name(reg, name, missing_ok))
}

/// `GetConfigOptionByName(name, &varname, missing_ok)` (guc.c:5438): the
/// canonical spelling of `name` (C: `*varname = record->name`).
fn config_option_canonical_name(name: &str, missing_ok: bool) -> PgResult<Option<String>> {
    with_registry(|reg| match reg.find_option(name) {
        Some(record) => Ok(Some(record.gen().name.to_string())),
        None => {
            if missing_ok {
                Ok(None)
            } else {
                Err(ereport(ERROR)
                    .errmsg(format!("unrecognized configuration parameter \"{name}\""))
                    .into_error())
            }
        }
    })
}

/// `GetConfigOptionByName(name, &varname, missing_ok)` (guc.c:5438): both the
/// rendered current value and the canonical name.
fn config_option_value_and_name(
    name: &str,
    missing_ok: bool,
) -> PgResult<Option<(String, String)>> {
    with_registry(|reg| match reg.find_option(name) {
        Some(record) => {
            let value = get_config_option_by_name(reg, name, missing_ok)?.unwrap_or_default();
            Ok(Some((value, record.gen().name.to_string())))
        }
        None => {
            if missing_ok {
                Ok(None)
            } else {
                Err(ereport(ERROR)
                    .errmsg(format!("unrecognized configuration parameter \"{name}\""))
                    .into_error())
            }
        }
    })
}

/// The GUC records in sorted-name order (C `qsort` of the hashtab entries for
/// `SHOW ALL`). The registry iterates in arbitrary order; sort by name here.
fn sorted_variables(reg: &GucRegistry) -> Vec<&GucVariable> {
    let mut vars: Vec<&GucVariable> = reg.iter().collect();
    vars.sort_by(|a, b| guc_name_compare(a.gen().name, b.gen().name));
    vars
}

// ===========================================================================
// fmgr / Datum-layer SQL functions + GetConfigOptionValues (deferred).
// ===========================================================================

/// Number of attributes in `pg_settings` — `NUM_PG_SETTINGS_ATTS`.
pub const NUM_PG_SETTINGS_ATTS: usize = 17;
/// Columns of `pg_file_settings` — `NUM_PG_FILE_SETTINGS_ATTS`.
pub const NUM_PG_FILE_SETTINGS_ATTS: usize = 7;
/// `MAX_GUC_FLAGS` (guc_funcs.c:544).
pub const MAX_GUC_FLAGS: usize = 6;

// ===========================================================================
// GetConfigOptionValues / show_all_settings row builder (guc_funcs.c:593/848).
// ===========================================================================

/// One `pg_settings` row (`GetConfigOptionValues` output, guc_funcs.c:593).
///
/// The 17 columns of `pg_settings`. Every text column is an `Option<String>`
/// (`NULL` is `None`, matching C's `values[i] = NULL`); `enumvals` is the set of
/// enum option strings (`text[]`, `None` for non-enum variables); `sourceline`
/// is an `int4` (only set for `PGC_S_FILE` sources visible to the caller);
/// `pending_restart` is the bool drawn from `conf->status & GUC_PENDING_RESTART`.
#[derive(Debug, Clone)]
pub struct PgSettingsRow {
    /// `[0]` name — text.
    pub name: String,
    /// `[1]` setting — text (`ShowGUCOption(conf, false)`).
    pub setting: String,
    /// `[2]` unit — text / NULL.
    pub unit: Option<String>,
    /// `[3]` category — text (`config_group_names[conf->group]`).
    pub category: String,
    /// `[4]` short_desc — text / NULL.
    pub short_desc: Option<String>,
    /// `[5]` extra_desc — text / NULL (`conf->long_desc`).
    pub extra_desc: Option<String>,
    /// `[6]` context — text (`GucContext_Names[conf->context]`).
    pub context: String,
    /// `[7]` vartype — text (`config_type_names[conf->vartype]`).
    pub vartype: String,
    /// `[8]` source — text (`GucSource_Names[conf->source]`).
    pub source: String,
    /// `[9]` min_val — text / NULL.
    pub min_val: Option<String>,
    /// `[10]` max_val — text / NULL.
    pub max_val: Option<String>,
    /// `[11]` enumvals — text[] / NULL (the enum option names, `None` for
    /// non-enum variables).
    pub enumvals: Option<Vec<String>>,
    /// `[12]` boot_val — text / NULL.
    pub boot_val: Option<String>,
    /// `[13]` reset_val — text / NULL.
    pub reset_val: Option<String>,
    /// `[14]` sourcefile — text / NULL.
    pub sourcefile: Option<String>,
    /// `[15]` sourceline — int4 / NULL.
    pub sourceline: Option<i32>,
    /// `[16]` pending_restart — bool.
    pub pending_restart: bool,
}

/// `GetConfigOptionValues(conf, values)` (guc_funcs.c:593): extract the 17
/// `pg_settings` fields for one GUC variable. The generic attributes come from
/// `config_generic`; the typed `min/max/enumvals/boot/reset` attributes are read
/// off the `config_*` record per `conf->vartype` (the C downcast switch). The
/// source-file/line fields are only filled for `PGC_S_FILE` sources visible to a
/// `pg_read_all_settings` caller.
fn get_config_option_values(var: &GucVariable) -> PgSettingsRow {
    use backend_utils_misc_guc::enum_lookup::{
        config_enum_get_options, config_enum_lookup_by_value,
    };
    use backend_utils_misc_guc::model::GUC_PENDING_RESTART;
    use backend_utils_misc_guc::units::{fmt_g, get_config_unit_name};
    use backend_utils_misc_guc_tables::{
        config_group_names, config_type_names, GucContext_Names, GucSource_Names,
    };
    use types_guc::PGC_S_FILE;

    let conf = var.gen();

    // Generic attributes.
    let name = conf.name.to_string();
    // C: ShowGUCOption(conf, false) — no unit conversion for pg_settings.
    let setting = show_guc_option(var, false);
    let unit = get_config_unit_name(conf.flags).map(|s| s.to_string());
    let category = config_group_names[conf.group as i32 as usize].to_string();
    let short_desc = conf.short_desc.map(|s| s.to_string());
    let extra_desc = conf.long_desc.map(|s| s.to_string());
    let context = GucContext_Names[conf.context as i32 as usize].to_string();
    let vartype = config_type_names[conf.vartype as i32 as usize].to_string();
    let source = GucSource_Names[conf.source as i32 as usize].to_string();

    // Type-specific attributes (the C downcast switch on conf->vartype).
    let (min_val, max_val, enumvals, boot_val, reset_val) = match var {
        GucVariable::Bool(lconf) => (
            None,
            None,
            None,
            Some(if lconf.boot_val { "on" } else { "off" }.to_string()),
            Some(if lconf.reset_val { "on" } else { "off" }.to_string()),
        ),
        GucVariable::Int(lconf) => (
            Some(format!("{}", lconf.min)),
            Some(format!("{}", lconf.max)),
            None,
            Some(format!("{}", lconf.boot_val)),
            Some(format!("{}", lconf.reset_val)),
        ),
        GucVariable::Real(lconf) => (
            Some(fmt_g(lconf.min)),
            Some(fmt_g(lconf.max)),
            None,
            Some(fmt_g(lconf.boot_val)),
            Some(fmt_g(lconf.reset_val)),
        ),
        GucVariable::String(lconf) => (
            None,
            None,
            None,
            lconf.boot_val.clone(),
            lconf.reset_val.clone(),
        ),
        GucVariable::Enum(lconf) => {
            // C builds the text[] literal via config_enum_get_options(conf,
            // "{\"", "\"}", "\",\"") then array_in parses it. The owned model
            // carries the option names directly and lets the executor frame
            // build the text[] Datum (construct_text_array). Use a NUL separator
            // (no enum option name contains NUL) so the split is exact.
            let opts_csv = config_enum_get_options(lconf, "", "", "\u{0}");
            let enumvals: Vec<String> = if opts_csv.is_empty() {
                Vec::new()
            } else {
                opts_csv.split('\u{0}').map(|s| s.to_string()).collect()
            };
            let boot = config_enum_lookup_by_value(lconf, lconf.boot_val)
                .map(|s| s.to_string());
            let reset = config_enum_lookup_by_value(lconf, lconf.reset_val)
                .map(|s| s.to_string());
            (None, None, Some(enumvals), boot, reset)
        }
    };

    // Source file/line: only for PGC_S_FILE sources visible to a
    // pg_read_all_settings caller (C's security gate).
    let (sourcefile, sourceline) = if conf.source == PGC_S_FILE
        && seam::has_privs_of_role::call(seam::get_user_id::call(), ROLE_PG_READ_ALL_SETTINGS)
    {
        (conf.sourcefile.clone(), Some(conf.sourceline))
    } else {
        (None, None)
    };

    // pending_restart: conf->status & GUC_PENDING_RESTART.
    let pending_restart = (conf.status & GUC_PENDING_RESTART) != 0;

    PgSettingsRow {
        name,
        setting,
        unit,
        category,
        short_desc,
        extra_desc,
        context,
        vartype,
        source,
        min_val,
        max_val,
        enumvals,
        boot_val,
        reset_val,
        sourcefile,
        sourceline,
        pending_restart,
    }
}

/// `show_all_settings()` row source (guc_funcs.c:848): the `pg_settings` rows in
/// sorted-name order, filtered to the visible / non-`NO_SHOW_ALL` variables (the
/// per-call skip the C SRF applies inside its loop). Each row is the
/// `GetConfigOptionValues` projection of one GUC variable.
pub fn pg_settings_rows() -> Vec<PgSettingsRow> {
    with_registry(|reg| {
        let mut rows: Vec<PgSettingsRow> = Vec::new();
        for var in sorted_variables(reg) {
            let conf = var.gen();
            // skip if marked NO_SHOW_ALL or not visible to the current user.
            if (conf.flags & GUC_NO_SHOW_ALL) != 0 || !ConfigOptionIsVisible(conf) {
                continue;
            }
            rows.push(get_config_option_values(var));
        }
        rows
    })
}

/// The `PG_FUNCTION_ARGS` / `Datum` SQL-callable functions of guc_funcs.c, and
/// the `GetConfigOptionValues` helper that feeds `show_all_settings`.
///
/// These belong to the **project-wide fmgr/Datum-layer deferral**: argument
/// extraction (`PG_GETARG_*`), result datumization (`PG_RETURN_*`,
/// `cstring_to_text`, `construct_array_builtin`), and the
/// set-returning-function (`SRF_*` / `InitMaterializedSRF`) plumbing.
/// `GetConfigOptionValues` additionally reads the *typed* per-variable fields
/// (min/max/boot/reset/enum options) and has no caller outside
/// `show_all_settings`.
///
/// They are NOT stubbed behind a pretend success and use NO
/// placeholder/unported stub macros — each is a loud `panic!` until the fmgr/Datum
/// layer lands.
pub mod fmgr_deferred {
    /// `set_config_by_name(name text, value text, is_local bool) -> text`
    /// (guc_funcs.c:331).
    pub fn set_config_by_name() -> ! {
        panic!("fmgr/Datum-layer deferral: set_config_by_name (guc_funcs.c)")
    }

    /// `pg_settings_get_flags(text) -> text[]` (guc_funcs.c:541).
    pub fn pg_settings_get_flags() -> ! {
        panic!("fmgr/Datum-layer deferral: pg_settings_get_flags (guc_funcs.c)")
    }

    /// `show_config_by_name(text) -> text` (guc_funcs.c:806).
    pub fn show_config_by_name() -> ! {
        panic!("fmgr/Datum-layer deferral: show_config_by_name (guc_funcs.c)")
    }

    /// `show_config_by_name_missing_ok(text, bool) -> text` (guc_funcs.c:824).
    pub fn show_config_by_name_missing_ok() -> ! {
        panic!("fmgr/Datum-layer deferral: show_config_by_name_missing_ok (guc_funcs.c)")
    }

    // `GetConfigOptionValues` (guc_funcs.c:593) and `show_all_settings`
    // (guc_funcs.c:848) are now ported: see `get_config_option_values` /
    // `pg_settings_rows` above (the executor-frame SRF adapter for OID 2084 lives
    // in `backend-executor-execSRF`).

    /// `show_all_file_settings() -> setof record` (guc_funcs.c:983).
    pub fn show_all_file_settings() -> ! {
        panic!("fmgr/Datum-layer deferral: show_all_file_settings (guc_funcs.c)")
    }
}

// ===========================================================================
// Small helpers (node accessors, the unrecognized-node-type error).
// ===========================================================================

/// C: `(DefElem *) lfirst(head)` — borrow a list cell as a `DefElem`.
fn as_def_elem(node: &Node) -> PgResult<&types_parsenodes::DefElem> {
    node.as_defelem()
        .ok_or_else(|| unrecognized_node_type(node))
}

/// C: `linitial_node(A_Const, stmt->args)` — the first cell, asserted to be an
/// `A_Const`. In the K1 model the `A_Const` value is the cell itself, which for
/// `TRANSACTION SNAPSHOT` is a `String` node.
fn linitial_a_const(args: &[Node]) -> PgResult<&Node> {
    match args.first() {
        Some(node @ Node::String(_)) => Ok(node),
        Some(node @ Node::Integer(_)) => Ok(node),
        Some(node @ Node::Float(_)) => Ok(node),
        Some(other) => Err(unrecognized_node_type(other)),
        None => Err(ereport(ERROR)
            .errmsg_internal("unrecognized node type: (empty A_Const list)")
            .into_error()),
    }
}

/// C: `strVal(&con->val)` for the `TRANSACTION SNAPSHOT` argument (always a
/// `String` value node).
fn a_const_strval(con: &Node) -> PgResult<String> {
    match con {
        Node::String(s) => Ok(s.sval.clone().unwrap_or_default()),
        other => Err(unrecognized_node_type(other)),
    }
}

/// C: `elog(ERROR, "unrecognized node type: %d", (int) nodeTag(...))`. The K1
/// trimmed parse-tree `Node` has no numeric tag, so we print the C type name.
fn unrecognized_node_type(node: &Node) -> PgError {
    ereport(ERROR)
        .errmsg_internal(format!("unrecognized node type: {}", node.node_tag_name()))
        .into_error()
}

// ===========================================================================
// InitializeShmemGUCs (guc_funcs.c) — runtime-computed shared-memory GUCs.
// ===========================================================================

/// `InitializeShmemGUCs(void)` (guc_funcs.c). Now that all the GUCs are set,
/// the shared-memory size has been requested by extensions, and the shared
/// memory has been sized, compute the runtime-derived GUCs
/// `shared_memory_size` (in MB) and `shared_memory_size_in_huge_pages`, and
/// set them as `PGC_INTERNAL` / `PGC_S_DYNAMIC_DEFAULT` so they show in
/// `SHOW`/`pg_settings` without being writable.
pub fn InitializeShmemGUCs() -> PgResult<()> {
    // Calculate the actual shared memory size required for the system.
    // C: `size_b = CalculateShmemSize(NULL);`
    let (size_b, _num_semaphores) =
        backend_storage_ipc_ipci_seams::calculate_shmem_size::call()?;

    // Set the shared memory size, rounded up to the nearest whole megabyte
    // (C: `size_mb = add_size(size_b, (1024 * 1024) - 1) / (1024 * 1024);`).
    let size_mb = size_b
        .checked_add((1024 * 1024) - 1)
        .ok_or_else(|| {
            ereport(ERROR)
                .errmsg_internal("requested shared memory size overflows size_t")
                .into_error()
        })?
        / (1024 * 1024);
    let buf = format!("{size_mb}");
    backend_utils_misc_guc_seams::set_config_option_internal_dynamic_default::call(
        "shared_memory_size",
        &buf,
    )?;

    // Calculate the number of huge pages required.
    // C: `GetHugePageSize(&hp_size, NULL);`
    let (hp_size, _mmap_flags) = backend_port_sysv_shmem_seams::get_huge_page_size::call();
    if hp_size != 0 {
        // C: `hp_required = add_size(size_b / hp_size, 1);`
        let hp_required = (size_b / hp_size)
            .checked_add(1)
            .ok_or_else(|| {
                ereport(ERROR)
                    .errmsg_internal("requested huge page count overflows size_t")
                    .into_error()
            })?;
        let buf = format!("{hp_required}");
        backend_utils_misc_guc_seams::set_config_option_internal_dynamic_default::call(
            "shared_memory_size_in_huge_pages",
            &buf,
        )?;
    }

    Ok(())
}

// ===========================================================================
// Seam install — this crate is guc_funcs.c's home.
// ===========================================================================

/// Install the inward seam this crate OWNS. `ExtractSetVariableArgs` is
/// guc_funcs.c's own function; its seam decl now lives on this crate's own
/// `-seams` crate (`backend-utils-misc-guc-funcs-seams`), so the install is
/// dir-owner-attributable and the guard re-asserts the contract. The
/// Project a post-analyze `types_nodes::ddlnodes::VariableSetStmt` into the
/// trimmed `types_parsenodes::VariableSetStmt` that `ExecSetVariableStmt` /
/// `flatten_set_variable_args` consume. `kind` maps 1:1 across the two enums;
/// `name` is copied; each `args` member is an `A_Const` literal whose inner
/// value node (`Integer`/`Float`/`Boolean`/`String`) becomes the corresponding
/// `types_parsenodes::Node` value (the SET-args flattener only inspects these
/// four value-node tags). This mirrors C's `castNode(VariableSetStmt,
/// parsetree)` — the same node, viewed through the trimmed projection.
/// Project a single SET/transaction-option value node from the post-analyze
/// `types_nodes` universe into the `types_parsenodes` node that `SetPGVariable`
/// / `flatten_set_variable_args` consume. Mirrors the per-arg arm of
/// `variable_set_stmt_from_nodes`.
fn set_arg_from_nodes(arg: &types_nodes::nodes::Node<'_>) -> PgResult<Node> {
    use types_nodes::nodes::{ntag, Node as TnNode};

    // The DefElem arg is an `A_Const` (a SET literal); read its inner value.
    let val_node: &TnNode = match arg.node_tag() {
        ntag::T_A_Const => {
            let c = arg.expect_a_const();
            match &c.val {
                Some(v) => &**v,
                None => {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                        .errmsg("SET argument is NULL".to_string())
                        .into_error());
                }
            }
        }
        // The parse model may carry the value node directly (no A_Const wrapper).
        _ => arg,
    };
    match val_node.node_tag() {
        ntag::T_Integer => {
            let i = val_node.expect_integer();
            Ok(Node::Integer(types_parsenodes::Integer { ival: i.ival }))
        }
        ntag::T_Float => {
            let f = val_node.expect_float();
            Ok(Node::Float(types_parsenodes::Float {
                fval: Some(f.fval.to_string()),
            }))
        }
        ntag::T_Boolean => {
            let b = val_node.expect_boolean();
            Ok(Node::Boolean(types_parsenodes::Boolean { boolval: b.boolval }))
        }
        ntag::T_String => {
            let st = val_node.expect_string();
            Ok(Node::String(types_parsenodes::StringNode {
                sval: Some(st.sval.to_string()),
            }))
        }
        _ => {
            let other = val_node;
            Err(types_error::PgError::error(format!(
                "set_pg_variable: unexpected SET argument node {:?}",
                other.node_tag()
            )))
        }
    }
}

fn variable_set_stmt_from_nodes(
    s: &types_nodes::ddlnodes::VariableSetStmt<'_>,
) -> PgResult<VariableSetStmt> {
    use types_nodes::ddlnodes::VariableSetKind as TnKind;
    use types_nodes::nodes::{ntag, Node as TnNode};

    let kind = match s.kind {
        TnKind::VAR_SET_VALUE => VariableSetKind::SetValue,
        TnKind::VAR_SET_DEFAULT => VariableSetKind::SetDefault,
        TnKind::VAR_SET_CURRENT => VariableSetKind::SetCurrent,
        TnKind::VAR_SET_MULTI => VariableSetKind::SetMulti,
        TnKind::VAR_RESET => VariableSetKind::Reset,
        TnKind::VAR_RESET_ALL => VariableSetKind::ResetAll,
    };

    let name = s.name.as_ref().map(|n| n.to_string());

    let mut args: Vec<Node> = Vec::with_capacity(s.args.len());
    for arg in s.args.iter() {
        // Each member is an `A_Const` (a SET literal). Read its inner value node.
        let val_node: &TnNode = match (&**arg).node_tag() {
            ntag::T_A_Const => {
                let c = (&**arg).expect_a_const();
                match &c.val {
                    Some(v) => &**v,
                    // `A_Const` with no `val` is a NULL constant; SET literals are
                    // never NULL, so this is unreachable in practice.
                    None => {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                            .errmsg("SET argument is NULL".to_string())
                            .into_error());
                    }
                }
            }
            // The K1 parse model may carry the value node directly (no A_Const
            // wrapper) — handle both, mirroring the flattener's switch.
            _ => &**arg,
        };
        let projected = match val_node.node_tag() {
            ntag::T_Integer => {
                let i = val_node.expect_integer();
                Node::Integer(types_parsenodes::Integer { ival: i.ival })
            }
            ntag::T_Float => {
                let f = val_node.expect_float();
                Node::Float(types_parsenodes::Float {
                    fval: Some(f.fval.to_string()),
                })
            }
            ntag::T_Boolean => {
                let b = val_node.expect_boolean();
                Node::Boolean(types_parsenodes::Boolean { boolval: b.boolval })
            }
            ntag::T_String => {
                let st = val_node.expect_string();
                Node::String(types_parsenodes::StringNode {
                    sval: Some(st.sval.to_string()),
                })
            }
            // `SET TRANSACTION ...` / `SET SESSION CHARACTERISTICS ...`
            // (VAR_SET_MULTI) carry `DefElem` members (one per option, e.g.
            // `DefElem("transaction_read_only", A_Const(true))`). Project the
            // DefElem into the flat node, recursively converting its inner value
            // arg with the same value-node projection. Mirrors C's
            // `castNode(VariableSetStmt, ...)` — the DefElems are just viewed
            // through the trimmed projection, not reconstructed.
            ntag::T_DefElem => {
                let de = val_node.expect_defelem();
                let arg = match de.arg.as_deref() {
                    Some(a) => Some(Box::new(set_arg_from_nodes(a)?)),
                    None => None,
                };
                Node::DefElem(types_parsenodes::DefElem {
                    defnamespace: de.defnamespace.as_ref().map(|s| s.to_string()),
                    defname: de.defname.as_ref().map(|s| s.to_string()),
                    arg,
                    defaction: de.defaction,
                    location: de.location,
                })
            }
            _ => {
                let other = val_node;
                return Err(types_error::PgError::error(format!(
                    "exec_set_variable_stmt: unexpected SET argument node {:?}",
                    other.node_tag()
                )));
            }
        };
        args.push(projected);
    }

    Ok(VariableSetStmt {
        kind,
        name,
        args,
        is_local: s.is_local,
        location: s.location,
    })
}

/// value-typed `VariableSetStmt` crosses the seam, so the body borrows it.
pub fn init_seams() {
    // `case T_VariableSetStmt: ExecSetVariableStmt(castNode(VariableSetStmt,
    // parsetree), isTopLevel)` (utility.c standard-processing arm). The
    // dispatcher crosses the post-analyze `types_nodes::Node`; this body is the
    // castNode-equivalent (`Node::VariableSetStmt(s)`) plus a projection from
    // the `types_nodes` parse-tree node universe into the trimmed
    // `types_parsenodes::VariableSetStmt` that `ExecSetVariableStmt` consumes.
    backend_tcop_utility_out_seams::exec_set_variable_stmt::set(|stmt, is_top_level| {
        match stmt.node_tag() {
            types_nodes::nodes::ntag::T_VariableSetStmt => {
                let s = stmt.expect_variablesetstmt();
                let projected = variable_set_stmt_from_nodes(s)?;
                ExecSetVariableStmt(&projected, is_top_level)
            }
            _ => {
                let other = stmt;
                Err(types_error::PgError::error(format!(
                    "exec_set_variable_stmt: expected VariableSetStmt, got {:?}",
                    other.node_tag()
                )))
            }
        }
    });
    backend_utils_misc_guc_funcs_seams::extract_set_variable_args::set(|sstmt| {
        ExtractSetVariableArgs(&sstmt)
    });
    // `case T_AlterSystemStmt: AlterSystemSetConfigFile(castNode(AlterSystemStmt,
    // parsetree))` (utility.c). The dispatcher crosses the post-analyze
    // `types_nodes::Node`; this body is the castNode-equivalent
    // (`Node::AlterSystemStmt(s)` → its embedded `VariableSetStmt`) plus the
    // projection into the trimmed `types_parsenodes::VariableSetStmt`
    // `AlterSystemSetConfigFile` consumes.
    backend_tcop_utility_out_seams::alter_system_set_config_file::set(|stmt| {
        match stmt.node_tag() {
            types_nodes::nodes::ntag::T_AlterSystemStmt => {
                let s = stmt.expect_altersystemstmt();
                let setstmt = s.setstmt.as_ref().ok_or_else(|| {
                    types_error::PgError::error(
                        "AlterSystemSetConfigFile: missing setstmt".to_string(),
                    )
                })?;
                if setstmt.node_tag() != types_nodes::nodes::ntag::T_VariableSetStmt {
                    return Err(types_error::PgError::error(format!(
                        "AlterSystemSetConfigFile: expected VariableSetStmt, got {:?}",
                        setstmt.node_tag()
                    )));
                }
                let projected = variable_set_stmt_from_nodes(setstmt.expect_variablesetstmt())?;
                AlterSystemSetConfigFile(&projected)
            }
            other => Err(types_error::PgError::error(format!(
                "alter_system_set_config_file: expected AlterSystemStmt, got {other:?}"
            ))),
        }
    });
    // `SetPGVariable("session_authorization", NIL, false)` (guc.c DISCARD ALL).
    // The seam decl lives in the GUC owner's seam crate, but `SetPGVariable` is
    // guc_funcs.c's own body (this crate), so it installs the seam here. A NIL
    // args list maps to `arg = None`; `is_local = false`.
    backend_utils_misc_guc_seams::set_pg_variable_session_authorization_reset::set(|| {
        SetPGVariable("session_authorization", None, false)
    });
    // `GetConfigOptionFlags(name, missing_ok)` (guc.c) — the GUC flag word, for
    // pg_get_functiondef's proconfig `GUC_LIST_QUOTE` detection. A missing var
    // under `missing_ok` yields 0 (the C default); the caller passes true.
    backend_utils_misc_guc_funcs_seams::get_config_option_flags::set(|name, missing_ok| {
        match find_option_flags(&name) {
            Some(f) => Ok(f),
            None => {
                if missing_ok {
                    Ok(0)
                } else {
                    Err(types_error::PgError::error(format!(
                        "unrecognized configuration parameter \"{name}\""
                    )))
                }
            }
        }
    });
    // `SetPGVariable(name, list_make1(item->arg), true)` — the BEGIN /
    // START TRANSACTION transaction-characteristics options
    // (transaction_isolation / transaction_read_only / transaction_deferrable;
    // utility.c T_TransactionStmt arm). `SetPGVariable` is guc_funcs.c's body
    // (this crate); the dispatch reaches it through the utility-out-seam. The
    // option-value `Node` arrives in the post-analyze `types_nodes` universe and
    // is projected into the `types_parsenodes` node `SetPGVariable` consumes.
    backend_tcop_utility_out_seams::set_pg_variable::set(|name, arg, is_local| {
        let projected = set_arg_from_nodes(arg)?;
        SetPGVariable(name, Some(&projected), is_local)
    });
    // `case T_VariableShowStmt: GetPGVariable(n->name, dest)` (SHOW). The body
    // (`GetPGVariable`) is guc_funcs.c's own; the dispatch reaches it through
    // the utility-out-seam. The grammar always sets `n->name` (`SHOW ALL` maps
    // to the literal name "all"), so a missing name is a malformed parse tree.
    backend_tcop_utility_out_seams::get_pg_variable::set(|mcx, name, dest| {
        let name = name.ok_or_else(|| {
            types_error::PgError::error("GetPGVariable: missing variable name".to_string())
        })?;
        GetPGVariable(mcx, name, dest)
    });
    // `case T_VariableShowStmt: GetPGVariableResultDesc(n->name)` — the SHOW
    // result tuple descriptor (one TEXT column, named for the variable; three
    // TEXT columns for `SHOW ALL`). guc_funcs.c's own body, installed here.
    backend_tcop_utility_out_seams::get_pg_variable_result_desc::set(|mcx, name| {
        let name = name.ok_or_else(|| {
            types_error::PgError::error(
                "GetPGVariableResultDesc: missing variable name".to_string(),
            )
        })?;
        GetPGVariableResultDesc(mcx, name)
    });
    // `InitializeShmemGUCs()` is guc_funcs.c's own body; its standalone-boot
    // seam is declared in `backend-tcop-postgres-seams` (the boot driver's seam
    // crate), installed here by its true owner.
    backend_tcop_postgres_seams::initialize_shmem_gucs::set(InitializeShmemGUCs);

    // Register guc_funcs.c's `text`-boundary SQL functions into the fmgr-core
    // builtin table (C: their `fmgr_builtins[]` rows), so `fmgr_isbuiltin` and
    // by-OID dispatch resolve `current_setting` / `set_config`.
    fmgr_builtins::register_guc_funcs_builtins();

    // `get_explain_guc_options(&num)` (guc.c:5337) — EXPLAIN (SETTINGS) reads
    // the live GUC registry, which lives in this crate. The seam decl is owned
    // by `backend-commands-explain-seams`; `ExplainPrintSettings` consumes it.
    backend_commands_explain_seams::get_explain_guc_options::set(|| {
        Ok(get_explain_guc_options())
    });
    // `GetConfigOptionByName(name, NULL, missing_ok=true)` (guc.c:5438) — render
    // each printed setting's current value. SETTINGS always passes missing_ok.
    backend_commands_explain_seams::explain_get_config_option_by_name::set(|name| {
        config_option_value(name, true)
    });
}
