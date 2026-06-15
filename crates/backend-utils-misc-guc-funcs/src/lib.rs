#![no_std]
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
//! `show_config_by_name_missing_ok`, `show_all_settings`,
//! `show_all_file_settings`) and the `GetConfigOptionValues` helper feeding
//! `show_all_settings` belong to the project-wide fmgr/Datum-layer deferral.
//! They live behind a loud-panic module ([`fmgr_deferred`]) — never a pretend
//! success (and never a placeholder/unported stub).

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
                seam::import_snapshot::call(sval)?;
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
    let _ = ACL_SET; // C: passes ACL_SET; the hook records the access mode.
    seam::invoke_object_post_alter_hook_arg_str::call(
        ParameterAclRelationId,
        name.to_string(),
        variable_set_kind_as_subid(stmt.kind),
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
// SHOW command — GetPGVariable / GetPGVariableResultDesc (guc_funcs.c:382/394).
// ===========================================================================

/// `GetPGVariable(name, dest)` (guc_funcs.c:382): the `SHOW` statement.
pub fn GetPGVariable(
    name: &str,
    dest: types_nodes::parsestmt::DestReceiverHandle,
) -> PgResult<()> {
    if guc_name_compare(name, "all") == Ordering::Equal {
        ShowAllGUCConfig(dest)
    } else {
        ShowGUCConfigOption(name, dest)
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
fn ShowGUCConfigOption(
    name: &str,
    dest: types_nodes::parsestmt::DestReceiverHandle,
) -> PgResult<()> {
    // Get the value and canonical spelling of name
    // value = GetConfigOptionByName(name, &varname, false);
    let (value, _varname) = config_option_value_and_name(name, false)?.ok_or_else(|| {
        // missing_ok == false, so the lookup returns Err for an unknown name;
        // None should not occur here, but guard rather than panic.
        ereport(ERROR)
            .errmsg(format!("unrecognized configuration parameter \"{name}\""))
            .into_error()
    })?;

    // need a tuple descriptor representing a single TEXT column
    // (begin_tup_output_tupdesc builds the slot from this column count). The
    // canonical name is the column name (varname); the row value is `value`.

    // prepare for projection of tuples
    let tstate = seam::begin_tup_output_tupdesc::call(dest, 1);

    // Send it
    seam::do_text_output_oneline::call(tstate, value);

    seam::end_tup_output::call(tstate);
    Ok(())
}

/// `ShowAllGUCConfig(dest)` (guc_funcs.c:455): the `SHOW ALL` command.
fn ShowAllGUCConfig(dest: types_nodes::parsestmt::DestReceiverHandle) -> PgResult<()> {
    // need a tuple descriptor representing three TEXT columns
    // prepare for projection of tuples
    let tstate = seam::begin_tup_output_tupdesc::call(dest, 3);

    // collect the variables, in sorted order, and emit each visible row.
    with_registry(|reg| {
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
    })
    .into_iter()
    .for_each(|values| {
        // send it to dest
        seam::do_tup_output::call(tstate, values);
    });

    seam::end_tup_output::call(tstate);
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
fn find_option_flags(name: &str) -> Option<i32> {
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

    /// `GetConfigOptionValues(conf, values)` (guc_funcs.c:593): the
    /// `pg_settings` row builder feeding `show_all_settings`.
    pub fn GetConfigOptionValues() -> ! {
        panic!("fmgr/Datum-layer deferral: GetConfigOptionValues (guc_funcs.c)")
    }

    /// `show_all_settings() -> setof record` (guc_funcs.c:848).
    pub fn show_all_settings() -> ! {
        panic!("fmgr/Datum-layer deferral: show_all_settings (guc_funcs.c)")
    }

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
// Seam install — this crate is guc_funcs.c's home.
// ===========================================================================

/// Install the inward seam this crate OWNS. `ExtractSetVariableArgs` is
/// guc_funcs.c's own function; its seam decl now lives on this crate's own
/// `-seams` crate (`backend-utils-misc-guc-funcs-seams`), so the install is
/// dir-owner-attributable and the guard re-asserts the contract. The
/// value-typed `VariableSetStmt` crosses the seam, so the body borrows it.
pub fn init_seams() {
    backend_utils_misc_guc_funcs_seams::extract_set_variable_args::set(|sstmt| {
        ExtractSetVariableArgs(&sstmt)
    });
    // `SetPGVariable("session_authorization", NIL, false)` (guc.c DISCARD ALL).
    // The seam decl lives in the GUC owner's seam crate, but `SetPGVariable` is
    // guc_funcs.c's own body (this crate), so it installs the seam here. A NIL
    // args list maps to `arg = None`; `is_local = false`.
    backend_utils_misc_guc_seams::set_pg_variable_session_authorization_reset::set(|| {
        SetPGVariable("session_authorization", None, false)
    });
}
