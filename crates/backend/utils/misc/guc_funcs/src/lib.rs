#![allow(non_snake_case)]

use datum::Datum;
use elog::ereport;
use guc::registry::GucVariable;
use guc::{GUC_ACTION_LOCAL, GUC_ACTION_SET};
use mcx::Mcx;
use std::rc::Rc;

use tcop_dest::DestReceiver;
use tupdesc::{CreateTemplateTupleDesc, TupleDescInitBuiltinEntry, TupleDescInitEntry};
use types_core::{Oid, TEXTOID};
use types_error::{
    ErrorLevel, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_INVALID_TRANSACTION_STATE,
    ERROR,
};
use types_guc::{
    GucContext, GUC_LIST_INPUT, GUC_LIST_QUOTE, GUC_NO_SHOW_ALL, GUC_SUPERUSER_ONLY, PGC_SUSET,
    PGC_USERSET, PGC_S_SESSION,
};
use types_nodes::node_tree::Node;
use types_nodes::parsenodes::{VariableSetKind, VariableSetStmt};
use types_nodes::rawnodes::ValUnion;
use types_tuple::TupleDescData;

pub use guc::registry::show_guc_option as ShowGUCOption;

mod alter_system;
pub use alter_system::AlterSystemSetConfigFile;

mod settings;
pub use settings::{fc_pg_settings_get_flags, fc_show_all_settings, GUC_FUNCS_BUILTINS};

#[cfg(test)]
mod tests;

// ROLE_PG_READ_ALL_SETTINGS (pg_authid.dat).
const ROLE_PG_READ_ALL_SETTINGS: Oid = 3374;

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("guc_funcs.c arm not ported: {what}");
}

fn suset_or_userset() -> PgResult<GucContext> {
    Ok(if superuser::superuser()? { PGC_SUSET } else { PGC_USERSET })
}

fn set_config_option_session(name: &str, value: Option<&str>, is_local: bool) -> PgResult<()> {
    let action = if is_local { GUC_ACTION_LOCAL } else { GUC_ACTION_SET };
    guc::set_config_option(
        name,
        value,
        suset_or_userset()?,
        PGC_S_SESSION,
        action,
        true,
        ErrorLevel(0),
        false,
    )
    .map(|_| ())
}

pub fn ExecSetVariableStmt(stmt: &VariableSetStmt<'_>, is_top_level: bool) -> PgResult<()> {
    if xact::IsInParallelMode() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_TRANSACTION_STATE)
            .errmsg("cannot set parameters during a parallel operation")
            .into_error()
            .into());
    }

    let name = stmt.name.unwrap_or("");
    match stmt.kind {
        VariableSetKind::VAR_SET_VALUE | VariableSetKind::VAR_SET_CURRENT => {
            if stmt.is_local {
                xact::WarnNoTransactionBlock(is_top_level, "SET LOCAL")?;
            }
            let value = ExtractSetVariableArgs(stmt)?;
            set_config_option_session(name, value.as_deref(), stmt.is_local)?;
        }
        VariableSetKind::VAR_SET_MULTI => match name {
            "TRANSACTION" => {
                xact::WarnNoTransactionBlock(is_top_level, "SET TRANSACTION")?;
                set_transaction_elements(stmt, "")?;
            }
            "SESSION CHARACTERISTICS" => {
                set_transaction_elements(stmt, "default_")?;
            }
            "TRANSACTION SNAPSHOT" => {
                let con = stmt
                    .args
                    .iter()
                    .next()
                    .and_then(Node::as_a_const)
                    .expect("SET TRANSACTION SNAPSHOT: A_Const argument");
                if stmt.is_local {
                    return Err(ereport(ERROR)
                        .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg("SET LOCAL TRANSACTION SNAPSHOT is not implemented")
                        .into_error()
                        .into());
                }
                xact::WarnNoTransactionBlock(is_top_level, "SET TRANSACTION")?;
                let Some(ValUnion::String(s)) = con.val else {
                    panic!("SET TRANSACTION SNAPSHOT: non-string A_Const");
                };
                snapmgr_seams::import_snapshot::call(s.sval)?;
            }
            other => panic!("unexpected SET MULTI element: {other}"),
        },
        VariableSetKind::VAR_SET_DEFAULT | VariableSetKind::VAR_RESET => {
            if stmt.is_local && stmt.kind == VariableSetKind::VAR_SET_DEFAULT {
                xact::WarnNoTransactionBlock(is_top_level, "SET LOCAL")?;
            }
            set_config_option_session(name, None, stmt.is_local)?;
        }
        VariableSetKind::VAR_RESET_ALL => {
            guc::ResetAllOptions();
        }
    }

    // C: InvokeObjectPostAlterHookArgStr(ParameterAclRelationId, ...) — the
    // object_access_hook surface is absent by design in this port.
    Ok(())
}

fn set_transaction_elements(stmt: &VariableSetStmt<'_>, prefix: &str) -> PgResult<()> {
    for item in stmt.args.iter() {
        let item = item.as_def_elem().expect("SET TRANSACTION: DefElem list");
        let defname = item.defname.unwrap_or("");
        match defname {
            "transaction_isolation" | "transaction_read_only" | "transaction_deferrable" => {
                SetPGVariable(&format!("{prefix}{defname}"), item.arg, stmt.is_local)?;
            }
            other => panic!("unexpected SET TRANSACTION element: {other}"),
        }
    }
    Ok(())
}

pub fn ExtractSetVariableArgs(stmt: &VariableSetStmt<'_>) -> PgResult<Option<String>> {
    match stmt.kind {
        VariableSetKind::VAR_SET_VALUE => {
            let args: Vec<Node<'_>> = stmt.args.iter().collect();
            flatten_set_variable_args(stmt.name.unwrap_or(""), &args)
        }
        VariableSetKind::VAR_SET_CURRENT => {
            config_option_named_value(stmt.name.unwrap_or("")).map(|(_, v)| Some(v))
        }
        _ => Ok(None),
    }
}

fn option_flags(name: &str) -> i32 {
    guc::store::with_store(|reg| reg.find_option(name).map(|r| r.gen().flags))
        .flatten()
        .unwrap_or(0)
}

pub fn flatten_set_variable_args(name: &str, args: &[Node<'_>]) -> PgResult<Option<String>> {
    if args.is_empty() {
        return Ok(None);
    }

    let flags = option_flags(name);

    if (flags & GUC_LIST_INPUT) == 0 && args.len() != 1 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("SET {name} takes only one argument"))
            .into_error()
            .into());
    }

    let mut buf = String::new();
    for (idx, arg) in args.iter().enumerate() {
        if idx != 0 {
            buf.push_str(", ");
        }
        let (arg, type_name) = match arg.as_variant::<types_nodes::rawnodes::TypeCast>() {
            Some(tc) => (tc.arg.expect("TypeCast arg"), tc.typeName),
            None => (*arg, None),
        };
        let con = arg
            .as_a_const()
            .unwrap_or_else(|| panic!("unrecognized node type: {:?}", arg.node_tag()));
        match con.val {
            Some(ValUnion::Integer(i)) => buf.push_str(&i.ival.to_string()),
            Some(ValUnion::Float(f)) => buf.push_str(f.fval),
            Some(ValUnion::String(s)) if type_name.is_some() => {
                // ConstInterval argument for TIME ZONE: coerce to interval
                // and back to normalize the value and apply the typmod.
                let tn = type_name.unwrap();
                let tn = tn.as_variant::<types_nodes::rawnodes::TypeName>().expect("TypeName");
                let ctx = mcx::MemoryContext::new("flatten SET interval");
                let (typoid, typmod) = parse_utilcmd::typenameTypeIdAndMod(ctx.mcx(), None, tn)?;
                debug_assert_eq!(typoid, types_core::INTERVALOID);
                let iv = adt_timestamp::interval::interval_in(s.sval, typmod, None)?;
                let mut out = [0u8; adt_datetime::MAXDATELEN + 1];
                let n = adt_timestamp::interval::interval_out(&iv, &mut out);
                buf.push_str("INTERVAL '");
                buf.push_str(core::str::from_utf8(&out[..n]).expect("interval_out is ascii"));
                buf.push('\'');
            }
            Some(ValUnion::String(s)) => {
                if (flags & GUC_LIST_QUOTE) != 0 {
                    buf.push_str(&format_type::quote_identifier(s.sval));
                } else {
                    buf.push_str(s.sval);
                }
            }
            _ => panic!("unrecognized node type in SET argument"),
        }
    }

    Ok(Some(buf))
}

// C signature takes a List*; every in-tree caller passes list_make1(arg) or NIL.
pub fn SetPGVariable(name: &str, arg: Option<Node<'_>>, is_local: bool) -> PgResult<()> {
    let argstring = match arg {
        Some(node) => flatten_set_variable_args(name, &[node])?,
        None => None,
    };
    set_config_option_session(name, argstring.as_deref(), is_local)
}

pub fn GetPGVariable<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
    dest: &mut DestReceiver<'mcx>,
) -> PgResult<()> {
    if guc::guc_name_compare(name, "all") == std::cmp::Ordering::Equal {
        ShowAllGUCConfig(mcx, dest)
    } else {
        ShowGUCConfigOption(mcx, name, dest)
    }
}

pub fn GetPGVariableResultDesc<'mcx>(mcx: Mcx<'mcx>, name: &str) -> PgResult<TupleDescData<'mcx>> {
    if guc::guc_name_compare(name, "all") == std::cmp::Ordering::Equal {
        let mut tupdesc = CreateTemplateTupleDesc(mcx, 3)?;
        TupleDescInitEntry(&mut tupdesc, 1, Some("name"), TEXTOID, -1, 0)?;
        TupleDescInitEntry(&mut tupdesc, 2, Some("setting"), TEXTOID, -1, 0)?;
        TupleDescInitEntry(&mut tupdesc, 3, Some("description"), TEXTOID, -1, 0)?;
        Ok(tupdesc)
    } else {
        let (varname, _) = config_option_named_value(name)?;
        let mut tupdesc = CreateTemplateTupleDesc(mcx, 1)?;
        TupleDescInitEntry(&mut tupdesc, 1, Some(&varname), TEXTOID, -1, 0)?;
        Ok(tupdesc)
    }
}

// GetConfigOptionByName(name, &varname, missing_ok=false): (canonical, value).
pub fn config_option_named_value(name: &str) -> PgResult<(String, String)> {
    guc::store::with_store(|reg| {
        let value = guc::registry::get_config_option_by_name(reg, name, false)?
            .expect("missing_ok=false returned None");
        let varname = reg.find_option(name).expect("option vanished").gen().name;
        Ok((varname.to_string(), value))
    })
    .expect("GUC store not initialized")
}

fn ShowGUCConfigOption<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
    dest: &mut DestReceiver<'mcx>,
) -> PgResult<()> {
    let (varname, value) = config_option_named_value(name)?;
    let mut tupdesc = CreateTemplateTupleDesc(mcx, 1)?;
    // Builtin entry: no catalog access, so SHOW works on a database-less
    // walsender (guc_funcs.c ShowGUCConfigOption).
    TupleDescInitBuiltinEntry(&mut tupdesc, 1, &varname, TEXTOID, -1, 0)?;
    let mut tstate = exectuples_output::begin_tup_output_tupdesc(mcx, dest, Rc::new(tupdesc))?;
    exectuples_output::do_text_output_oneline(&mut tstate, mcx, &value)?;
    exectuples_output::end_tup_output(tstate)
}

fn ShowAllGUCConfig<'mcx>(mcx: Mcx<'mcx>, dest: &mut DestReceiver<'mcx>) -> PgResult<()> {
    let rows = show_all_guc_config_rows()?;
    let mut tupdesc = CreateTemplateTupleDesc(mcx, 3)?;
    // Builtin entries: no catalog access, so SHOW ALL works on a
    // database-less walsender (guc_funcs.c ShowAllGUCConfig).
    TupleDescInitBuiltinEntry(&mut tupdesc, 1, "name", TEXTOID, -1, 0)?;
    TupleDescInitBuiltinEntry(&mut tupdesc, 2, "setting", TEXTOID, -1, 0)?;
    TupleDescInitBuiltinEntry(&mut tupdesc, 3, "description", TEXTOID, -1, 0)?;
    let mut tstate = exectuples_output::begin_tup_output_tupdesc(mcx, dest, Rc::new(tupdesc))?;
    for (name, setting, short_desc) in &rows {
        let mut values = [Datum::null(); 3];
        let mut isnull = [false; 3];
        let name_v = varlena::cstring_to_text(mcx, name.as_bytes())?;
        values[0] = Datum::from_usize(name_v.as_bytes().as_ptr() as usize);
        let setting_v = match setting {
            Some(s) => Some(varlena::cstring_to_text(mcx, s.as_bytes())?),
            None => None,
        };
        match &setting_v {
            Some(v) => values[1] = Datum::from_usize(v.as_bytes().as_ptr() as usize),
            None => isnull[1] = true,
        }
        let desc_v = match short_desc {
            Some(s) => Some(varlena::cstring_to_text(mcx, s.as_bytes())?),
            None => None,
        };
        match &desc_v {
            Some(v) => values[2] = Datum::from_usize(v.as_bytes().as_ptr() as usize),
            None => isnull[2] = true,
        }
        exectuples_output::do_tup_output(&mut tstate, mcx, &values, &isnull)?;
    }
    exectuples_output::end_tup_output(tstate)
}

// The (name, setting, short_desc) projection of SHOW ALL, C row order.
pub fn show_all_guc_config_rows() -> PgResult<Vec<(String, Option<String>, Option<String>)>> {
    guc::store::with_store(|reg| {
        // C's get_guc_variables array is kept sorted by guc_name_compare.
        let mut sorted: Vec<&GucVariable> = reg.iter().collect();
        sorted.sort_by(|a, b| guc::guc_name_compare(a.gen().name, b.gen().name));
        let mut rows = Vec::new();
        for conf in sorted {
            let gen = conf.gen();
            if gen.flags & GUC_NO_SHOW_ALL != 0 {
                continue;
            }
            if !ConfigOptionIsVisible(conf)? {
                continue;
            }
            rows.push((
                gen.name.to_string(),
                Some(ShowGUCOption(conf, true)),
                gen.short_desc.map(str::to_string),
            ));
        }
        Ok(rows)
    })
    .expect("GUC store not initialized")
}

pub fn ConfigOptionIsVisible(conf: &GucVariable) -> PgResult<bool> {
    if conf.gen().flags & GUC_SUPERUSER_ONLY != 0
        && !adt_acl::has_privs_of_role(miscinit::GetUserId(), ROLE_PG_READ_ALL_SETTINGS)?
    {
        Ok(false)
    } else {
        Ok(true)
    }
}

// get_explain_guc_options (guc.c) with the visibility filter bound.
pub fn get_explain_guc_options() -> PgResult<Vec<(&'static str, Option<String>)>> {
    guc::store::with_store(|reg| {
        guc::registry::get_explain_guc_options(reg, &mut |conf| ConfigOptionIsVisible(conf))
    })
    .expect("GUC store not initialized")
}

pub fn init_seams() {
    guc_seams::get_explain_guc_options::set(get_explain_guc_options);
}
