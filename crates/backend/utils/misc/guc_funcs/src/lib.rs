#![allow(non_snake_case)]

use elog::ereport;
use guc::registry::GucVariable;
use guc::{GUC_ACTION_LOCAL, GUC_ACTION_SET};
use mcx::Mcx;
use tcop_dest::DestReceiver;
use tupdesc::{CreateTemplateTupleDesc, TupleDescInitEntry};
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
use types_nodes::NodeTag;
use types_tuple::TupleDescData;

pub use guc::registry::show_guc_option as ShowGUCOption;

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
        if arg.node_tag() == NodeTag::T_TypeCast {
            // C coerces ConstInterval args (SET TIME ZONE INTERVAL 'x')
            // through interval_in/interval_out.
            unported("flatten_set_variable_args TypeCast (interval lane)");
        }
        let con = arg
            .as_a_const()
            .unwrap_or_else(|| panic!("unrecognized node type: {:?}", arg.node_tag()));
        match con.val {
            Some(ValUnion::Integer(i)) => buf.push_str(&i.ival.to_string()),
            Some(ValUnion::Float(f)) => buf.push_str(f.fval),
            Some(ValUnion::String(s)) => {
                if (flags & GUC_LIST_QUOTE) != 0 {
                    unported("flatten_set_variable_args quote_identifier (ruleutils lane)");
                }
                buf.push_str(s.sval);
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

pub fn GetPGVariable(name: &str, dest: &mut DestReceiver<'_>) -> PgResult<()> {
    if guc::guc_name_compare(name, "all") == std::cmp::Ordering::Equal {
        ShowAllGUCConfig(dest)
    } else {
        ShowGUCConfigOption(name, dest)
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

fn ShowGUCConfigOption(name: &str, _dest: &mut DestReceiver<'_>) -> PgResult<()> {
    let (_varname, _value) = config_option_named_value(name)?;
    unported("ShowGUCConfigOption tuple emission: begin_tup_output_tupdesc (exectuples lane)");
}

fn ShowAllGUCConfig(_dest: &mut DestReceiver<'_>) -> PgResult<()> {
    let _rows = show_all_guc_config_rows()?;
    unported("ShowAllGUCConfig tuple emission: begin_tup_output_tupdesc (exectuples lane)");
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

pub fn init_seams() {}
