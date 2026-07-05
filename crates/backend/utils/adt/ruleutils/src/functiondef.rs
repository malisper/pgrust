//! pg_get_functiondef / pg_get_function_arguments / _identity_arguments /
//! _result (ruleutils.c).

use cache_syscache::{ReleaseSysCache, SearchSysCache1, SysCacheKey, AGGFNOID, PROCOID};
use datum::Datum;
use mcx::Mcx;
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult, ERRCODE_WRONG_OBJECT_TYPE};

use crate::deparse::simple_quote_literal;
use crate::{
    generate_function_name, getattr, getattr_null, name_at, namespace_name_or_temp,
    quote_identifier, quote_qualified_identifier, text_array_at, text_at,
};

const ANUM_PG_PROC_PRONAME: i32 = 2;
const ANUM_PG_PROC_PRONAMESPACE: i32 = 3;
const ANUM_PG_PROC_PROLANG: i32 = 5;
const ANUM_PG_PROC_PROCOST: i32 = 6;
const ANUM_PG_PROC_PROROWS: i32 = 7;
const ANUM_PG_PROC_PROSUPPORT: i32 = 9;
const ANUM_PG_PROC_PROKIND: i32 = 10;
const ANUM_PG_PROC_PROSECDEF: i32 = 11;
const ANUM_PG_PROC_PROLEAKPROOF: i32 = 12;
const ANUM_PG_PROC_PROISSTRICT: i32 = 13;
const ANUM_PG_PROC_PRORETSET: i32 = 14;
const ANUM_PG_PROC_PROVOLATILE: i32 = 15;
const ANUM_PG_PROC_PROPARALLEL: i32 = 16;
const ANUM_PG_PROC_PRONARGS: i32 = 17;
const ANUM_PG_PROC_PRONARGDEFAULTS: i32 = 18;
const ANUM_PG_PROC_PRORETTYPE: i32 = 19;
const ANUM_PG_PROC_PROARGTYPES: i32 = 20;
const ANUM_PG_PROC_PROALLARGTYPES: i32 = 21;
const ANUM_PG_PROC_PROARGMODES: i32 = 22;
const ANUM_PG_PROC_PROARGNAMES: i32 = 23;
const ANUM_PG_PROC_PROARGDEFAULTS: i32 = 24;
const ANUM_PG_PROC_PROTRFTYPES: i32 = 25;
const ANUM_PG_PROC_PROSRC: i32 = 26;
const ANUM_PG_PROC_PROBIN: i32 = 27;
const ANUM_PG_PROC_PROSQLBODY: i32 = 28;
const ANUM_PG_PROC_PROCONFIG: i32 = 29;

const ANUM_PG_AGGREGATE_AGGKIND: i32 = 2;
const ANUM_PG_AGGREGATE_AGGNUMDIRECTARGS: i32 = 3;
const AGGKIND_NORMAL: u8 = b'n';

const PROKIND_AGGREGATE: u8 = b'a';
const PROKIND_WINDOW: u8 = b'w';
const PROKIND_PROCEDURE: u8 = b'p';

const PROVOLATILE_IMMUTABLE: u8 = b'i';
const PROVOLATILE_STABLE: u8 = b's';

const PROPARALLEL_SAFE: u8 = b's';
const PROPARALLEL_RESTRICTED: u8 = b'r';

const PROARGMODE_IN: u8 = b'i';
const PROARGMODE_OUT: u8 = b'o';
const PROARGMODE_INOUT: u8 = b'b';
const PROARGMODE_VARIADIC: u8 = b'v';
const PROARGMODE_TABLE: u8 = b't';

const INTERNAL_LANGUAGE_ID: Oid = 12;
const C_LANGUAGE_ID: Oid = 13;
const SQL_LANGUAGE_ID: Oid = 14;

const INTERNALOID: Oid = 2281;

struct PgProcRow {
    oid: Oid,
    proname: String,
    pronamespace: Oid,
    prolang: Oid,
    procost: f32,
    prorows: f32,
    prosupport: Oid,
    prokind: u8,
    prosecdef: bool,
    proleakproof: bool,
    proisstrict: bool,
    proretset: bool,
    provolatile: u8,
    proparallel: u8,
    pronargs: i16,
    pronargdefaults: i16,
    prorettype: Oid,
    proargtypes: Vec<Oid>,
    proallargtypes: Option<Vec<Oid>>,
    proargmodes: Option<Vec<u8>>,
    proargnames: Option<Vec<String>>,
    proargdefaults: Option<String>,
    trftypes: Option<Vec<Oid>>,
    prosrc: String,
    probin: Option<String>,
    prosqlbody: Option<String>,
    proconfig: Option<Vec<String>>,
}

fn pg_proc_row(funcid: Oid) -> PgResult<Option<PgProcRow>> {
    let Some(ht) = SearchSysCache1(PROCOID, SysCacheKey::Value(Datum::from_oid(funcid)))? else {
        return Ok(None);
    };
    let t = ht.tuple();
    let row = PgProcRow {
        oid: funcid,
        proname: name_at(getattr(&t, PROCOID, ANUM_PG_PROC_PRONAME)),
        pronamespace: getattr(&t, PROCOID, ANUM_PG_PROC_PRONAMESPACE).as_oid(),
        prolang: getattr(&t, PROCOID, ANUM_PG_PROC_PROLANG).as_oid(),
        procost: getattr(&t, PROCOID, ANUM_PG_PROC_PROCOST).as_f32(),
        prorows: getattr(&t, PROCOID, ANUM_PG_PROC_PROROWS).as_f32(),
        prosupport: getattr(&t, PROCOID, ANUM_PG_PROC_PROSUPPORT).as_oid(),
        prokind: getattr(&t, PROCOID, ANUM_PG_PROC_PROKIND).as_i8() as u8,
        prosecdef: getattr(&t, PROCOID, ANUM_PG_PROC_PROSECDEF).as_bool(),
        proleakproof: getattr(&t, PROCOID, ANUM_PG_PROC_PROLEAKPROOF).as_bool(),
        proisstrict: getattr(&t, PROCOID, ANUM_PG_PROC_PROISSTRICT).as_bool(),
        proretset: getattr(&t, PROCOID, ANUM_PG_PROC_PRORETSET).as_bool(),
        provolatile: getattr(&t, PROCOID, ANUM_PG_PROC_PROVOLATILE).as_i8() as u8,
        proparallel: getattr(&t, PROCOID, ANUM_PG_PROC_PROPARALLEL).as_i8() as u8,
        pronargs: getattr(&t, PROCOID, ANUM_PG_PROC_PRONARGS).as_i16(),
        pronargdefaults: getattr(&t, PROCOID, ANUM_PG_PROC_PRONARGDEFAULTS).as_i16(),
        prorettype: getattr(&t, PROCOID, ANUM_PG_PROC_PRORETTYPE).as_oid(),
        proargtypes: crate::oid_array_at(
            getattr_null(&t, PROCOID, ANUM_PG_PROC_PROARGTYPES)
                .expect("proargtypes is NOT NULL"),
        ),
        proallargtypes: getattr_null(&t, PROCOID, ANUM_PG_PROC_PROALLARGTYPES)
            .map(crate::oid_array_at),
        proargmodes: getattr_null(&t, PROCOID, ANUM_PG_PROC_PROARGMODES).map(char_array_at),
        proargnames: getattr_null(&t, PROCOID, ANUM_PG_PROC_PROARGNAMES).map(text_array_at),
        proargdefaults: getattr_null(&t, PROCOID, ANUM_PG_PROC_PROARGDEFAULTS).map(text_at),
        trftypes: getattr_null(&t, PROCOID, ANUM_PG_PROC_PROTRFTYPES).map(crate::oid_array_at),
        prosrc: text_at(getattr_null(&t, PROCOID, ANUM_PG_PROC_PROSRC).expect("prosrc NOT NULL")),
        probin: getattr_null(&t, PROCOID, ANUM_PG_PROC_PROBIN).map(text_at),
        prosqlbody: getattr_null(&t, PROCOID, ANUM_PG_PROC_PROSQLBODY).map(text_at),
        proconfig: getattr_null(&t, PROCOID, ANUM_PG_PROC_PROCONFIG).map(text_array_at),
    };
    drop(t);
    ReleaseSysCache(ht);
    Ok(Some(row))
}

// One-dimensional no-null "char" array body.
fn char_array_at(d: Datum) -> Vec<u8> {
    crate::array_body(d, 1)
}

// %g for the COST/ROWS values CREATE FUNCTION accepts.
fn fmt_g(f: f32) -> String {
    if f == f.trunc() && f.abs() < 1e15 {
        format!("{}", f as i64)
    } else {
        format!("{f}")
    }
}

struct ArgInfo {
    argtypes: Vec<Oid>,
    argnames: Option<Vec<String>>,
    argmodes: Option<Vec<u8>>,
}

// get_func_arg_info (funcapi.c).
fn func_arg_info(proc: &PgProcRow) -> ArgInfo {
    match &proc.proallargtypes {
        Some(all) => ArgInfo {
            argtypes: all.clone(),
            argnames: proc.proargnames.clone(),
            argmodes: proc.proargmodes.clone(),
        },
        None => ArgInfo {
            argtypes: proc.proargtypes.clone(),
            argnames: proc.proargnames.clone(),
            argmodes: None,
        },
    }
}

fn print_function_arguments(
    mcx: Mcx<'_>,
    buf: &mut String,
    proc: &PgProcRow,
    print_table_args: bool,
    print_defaults: bool,
) -> PgResult<usize> {
    let info = func_arg_info(proc);
    let numargs = info.argtypes.len();

    let mut argdefaults: Vec<types_nodes::Node<'_>> = Vec::new();
    let mut nlackdefaults = numargs as i32;
    if print_defaults && proc.pronargdefaults > 0 {
        if let Some(defs) = &proc.proargdefaults {
            let node = readfuncs::stringToNode(mcx, defs)?;
            let list = node.as_list().expect("proargdefaults is a List");
            argdefaults = list.iter().collect();
            nlackdefaults = proc.pronargs as i32 - argdefaults.len() as i32;
        }
    }

    let mut insertorderbyat: i64 = -1;
    if proc.prokind == PROKIND_AGGREGATE {
        let Some(ht) =
            SearchSysCache1(AGGFNOID, SysCacheKey::Value(Datum::from_oid(proc.oid)))?
        else {
            return Err(crate::cache_lookup_failed("aggregate", proc.oid));
        };
        let t = ht.tuple();
        let aggkind = getattr(&t, AGGFNOID, ANUM_PG_AGGREGATE_AGGKIND).as_i8() as u8;
        let aggnumdirectargs =
            getattr(&t, AGGFNOID, ANUM_PG_AGGREGATE_AGGNUMDIRECTARGS).as_i16();
        drop(t);
        ReleaseSysCache(ht);
        if aggkind != AGGKIND_NORMAL {
            insertorderbyat = aggnumdirectargs as i64;
        }
    }

    let mut argsprinted = 0usize;
    let mut inputargno = 0i32;
    let mut nextdefault = 0usize;
    let mut print_defaults = print_defaults;
    let mut i = 0usize;
    while i < numargs {
        let argtype = info.argtypes[i];
        let argname: Option<&str> = info
            .argnames
            .as_ref()
            .and_then(|n| n.get(i))
            .map(String::as_str)
            .filter(|s| !s.is_empty());
        let argmode = info.argmodes.as_ref().map(|m| m[i]).unwrap_or(PROARGMODE_IN);
        let (modename, isinput) = match argmode {
            PROARGMODE_IN => {
                if proc.prokind == PROKIND_PROCEDURE {
                    ("IN ", true)
                } else {
                    ("", true)
                }
            }
            PROARGMODE_INOUT => ("INOUT ", true),
            PROARGMODE_OUT => ("OUT ", false),
            PROARGMODE_VARIADIC => ("VARIADIC ", true),
            PROARGMODE_TABLE => ("", false),
            other => panic!("invalid parameter mode '{}'", other as char),
        };
        if isinput {
            inputargno += 1;
        }
        if print_table_args != (argmode == PROARGMODE_TABLE) {
            i += 1;
            continue;
        }
        if argsprinted as i64 == insertorderbyat {
            if argsprinted > 0 {
                buf.push(' ');
            }
            buf.push_str("ORDER BY ");
        } else if argsprinted > 0 {
            buf.push_str(", ");
        }
        buf.push_str(modename);
        if let Some(name) = argname {
            buf.push_str(&format!("{} ", quote_identifier(name)));
        }
        buf.push_str(&format_type::format_type_be(argtype)?);
        if print_defaults && isinput && inputargno > nlackdefaults {
            let expr = argdefaults[nextdefault];
            nextdefault += 1;
            let text = crate::deparse_expression_pretty(mcx, expr, InvalidOid, false, 0)?;
            buf.push_str(&format!(" DEFAULT {text}"));
        }
        argsprinted += 1;

        // nasty hack: print the last arg twice for variadic ordered-set agg
        if argsprinted as i64 == insertorderbyat && i == numargs - 1 {
            print_defaults = false;
            continue;
        }
        i += 1;
    }
    Ok(argsprinted)
}

fn print_function_rettype(mcx: Mcx<'_>, buf: &mut String, proc: &PgProcRow) -> PgResult<()> {
    let mut rbuf = String::new();
    let mut ntabargs = 0usize;
    if proc.proretset {
        rbuf.push_str("TABLE(");
        ntabargs = print_function_arguments(mcx, &mut rbuf, proc, true, false)?;
        if ntabargs > 0 {
            rbuf.push(')');
        } else {
            rbuf.clear();
        }
    }
    if ntabargs == 0 {
        if proc.proretset {
            rbuf.push_str("SETOF ");
        }
        rbuf.push_str(&format_type::format_type_be(proc.prorettype)?);
    }
    buf.push_str(&rbuf);
    Ok(())
}

pub fn pg_get_functiondef_worker(mcx: Mcx<'_>, funcid: Oid) -> PgResult<Option<String>> {
    let Some(proc) = pg_proc_row(funcid)? else {
        return Ok(None);
    };
    if proc.prokind == PROKIND_AGGREGATE {
        return Err(PgError::error(format!(
            "\"{}\" is an aggregate function",
            proc.proname
        ))
        .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
        .into());
    }
    let isfunction = proc.prokind != PROKIND_PROCEDURE;

    let mut buf = String::new();
    let nsp = namespace_name_or_temp(mcx, proc.pronamespace)?;
    buf.push_str(&format!(
        "CREATE OR REPLACE {} {}(",
        if isfunction { "FUNCTION" } else { "PROCEDURE" },
        quote_qualified_identifier(nsp.as_deref(), &proc.proname)
    ));
    print_function_arguments(mcx, &mut buf, &proc, false, true)?;
    buf.push_str(")\n");
    if isfunction {
        buf.push_str(" RETURNS ");
        print_function_rettype(mcx, &mut buf, &proc)?;
        buf.push('\n');
    }

    if let Some(trftypes) = &proc.trftypes {
        if !trftypes.is_empty() {
            buf.push_str(" TRANSFORM ");
            for (i, t) in trftypes.iter().enumerate() {
                if i != 0 {
                    buf.push_str(", ");
                }
                buf.push_str(&format!("FOR TYPE {}", format_type::format_type_be(*t)?));
            }
            buf.push('\n');
        }
    }

    let langname = lsyscache::get_language_name(mcx, proc.prolang, false)?
        .expect("missing_ok=false");
    buf.push_str(&format!(" LANGUAGE {}\n", quote_identifier(langname.as_str())));

    let oldlen = buf.len();
    if proc.prokind == PROKIND_WINDOW {
        buf.push_str(" WINDOW");
    }
    match proc.provolatile {
        PROVOLATILE_IMMUTABLE => buf.push_str(" IMMUTABLE"),
        PROVOLATILE_STABLE => buf.push_str(" STABLE"),
        _ => {}
    }
    match proc.proparallel {
        PROPARALLEL_SAFE => buf.push_str(" PARALLEL SAFE"),
        PROPARALLEL_RESTRICTED => buf.push_str(" PARALLEL RESTRICTED"),
        _ => {}
    }
    if proc.proisstrict {
        buf.push_str(" STRICT");
    }
    if proc.prosecdef {
        buf.push_str(" SECURITY DEFINER");
    }
    if proc.proleakproof {
        buf.push_str(" LEAKPROOF");
    }
    let default_cost: f32 =
        if proc.prolang == INTERNAL_LANGUAGE_ID || proc.prolang == C_LANGUAGE_ID {
            1.0
        } else {
            100.0
        };
    if proc.procost != default_cost {
        buf.push_str(&format!(" COST {}", fmt_g(proc.procost)));
    }
    if proc.prorows > 0.0 && proc.prorows != 1000.0 {
        buf.push_str(&format!(" ROWS {}", fmt_g(proc.prorows)));
    }
    if proc.prosupport != InvalidOid {
        let name = generate_function_name(mcx, proc.prosupport, &[INTERNALOID], &[], false)?;
        buf.push_str(&format!(" SUPPORT {name}"));
    }
    if oldlen != buf.len() {
        buf.push('\n');
    }

    if let Some(config) = &proc.proconfig {
        for item in config {
            let Some(pos) = item.find('=') else { continue };
            let (name, value) = (&item[..pos], &item[pos + 1..]);
            buf.push_str(&format!(" SET {} TO ", quote_identifier(name)));
            if guc::GetConfigOptionFlags(name, true)? & types_guc::GUC_LIST_QUOTE != 0 {
                let namelist = varlena::split_guc_list(value, b',')
                    .expect("invalid list syntax in proconfig item");
                let mut first = true;
                for curname in &namelist {
                    if !first {
                        buf.push_str(", ");
                    }
                    first = false;
                    simple_quote_literal(&mut buf, curname);
                }
            } else {
                simple_quote_literal(&mut buf, value);
            }
            buf.push('\n');
        }
    }

    if proc.prolang == SQL_LANGUAGE_ID && proc.prosqlbody.is_some() {
        print_function_sqlbody(mcx, &mut buf, &proc)?;
    } else {
        buf.push_str("AS ");
        if let Some(probin) = &proc.probin {
            simple_quote_literal(&mut buf, probin);
            buf.push_str(", ");
        }
        let tag = if isfunction { "function" } else { "procedure" };
        let mut dq = format!("${tag}");
        while proc.prosrc.contains(&dq) {
            dq.push('x');
        }
        dq.push('$');
        buf.push_str(&dq);
        buf.push_str(&proc.prosrc);
        buf.push_str(&dq);
    }
    buf.push('\n');
    Ok(Some(buf))
}

// print_function_sqlbody (ruleutils.c:3556). C AcquireRewriteLocks each
// query; lock acquisition is another lane (matches get_query_def note).
fn print_function_sqlbody(mcx: Mcx<'_>, buf: &mut String, proc: &PgProcRow) -> PgResult<()> {
    let info = func_arg_info(proc);
    let mut dpns = crate::query::DeparseNamespace::empty(Vec::new());
    dpns.funcname = Some(proc.proname.clone());
    dpns.argnames = Some(info.argnames.clone().unwrap_or_default());
    let src = proc.prosqlbody.as_deref().expect("caller checked prosqlbody");
    let n = readfuncs::stringToNode(mcx, src)?;
    let dpns = std::rc::Rc::new(dpns);
    if let Some(list) = n.as_list() {
        let stmts = list.nth(0).as_list().expect("prosqlbody stmt list");
        buf.push_str("BEGIN ATOMIC\n");
        for q in stmts.iter() {
            let query = q.as_query().expect("prosqlbody stmt is a Query");
            let mut ctx =
                crate::deparse::DeparseContext::new(mcx, crate::PRETTYFLAG_INDENT);
            ctx.namespaces.push(dpns.clone());
            // C passes WRAP_COLUMN_DEFAULT: pretty-indent wraps each target on
            // its own line.
            ctx.wrap_column = crate::viewdef::WRAP_COLUMN_DEFAULT;
            ctx.indent_level = 1;
            crate::query::get_query_def(query, &mut ctx, None, false)?;
            buf.push_str(&ctx.buf);
            buf.push(';');
            buf.push('\n');
        }
        buf.push_str("END");
    } else {
        let query = n.as_query().expect("prosqlbody is a Query");
        let mut ctx = crate::deparse::DeparseContext::new(mcx, 0);
        ctx.namespaces.push(dpns);
        ctx.wrap_column = crate::viewdef::WRAP_COLUMN_DEFAULT;
        crate::query::get_query_def(query, &mut ctx, None, false)?;
        buf.push_str(&ctx.buf);
    }
    Ok(())
}

pub fn pg_get_function_sqlbody_worker(mcx: Mcx<'_>, funcid: Oid) -> PgResult<Option<String>> {
    let Some(proc) = pg_proc_row(funcid)? else {
        return Ok(None);
    };
    if proc.prosqlbody.is_none() {
        return Ok(None);
    }
    let mut buf = String::new();
    print_function_sqlbody(mcx, &mut buf, &proc)?;
    Ok(Some(buf))
}

pub fn pg_get_function_arguments_worker(mcx: Mcx<'_>, funcid: Oid) -> PgResult<Option<String>> {
    let Some(proc) = pg_proc_row(funcid)? else {
        return Ok(None);
    };
    let mut buf = String::new();
    print_function_arguments(mcx, &mut buf, &proc, false, true)?;
    Ok(Some(buf))
}

pub fn pg_get_function_identity_arguments_worker(
    mcx: Mcx<'_>,
    funcid: Oid,
) -> PgResult<Option<String>> {
    let Some(proc) = pg_proc_row(funcid)? else {
        return Ok(None);
    };
    let mut buf = String::new();
    print_function_arguments(mcx, &mut buf, &proc, false, false)?;
    Ok(Some(buf))
}

// is_input_argument (ruleutils.c).
fn is_input_argument(nth: usize, argmodes: Option<&Vec<u8>>) -> bool {
    match argmodes {
        None => true,
        Some(m) => {
            matches!(m[nth], PROARGMODE_IN | PROARGMODE_INOUT | PROARGMODE_VARIADIC)
        }
    }
}

pub fn pg_get_function_arg_default_worker(
    mcx: Mcx<'_>,
    funcid: Oid,
    nth_arg: i32,
) -> PgResult<Option<String>> {
    let Some(proc) = pg_proc_row(funcid)? else {
        return Ok(None);
    };
    let info = func_arg_info(&proc);
    let numargs = info.argtypes.len() as i32;
    if nth_arg < 1
        || nth_arg > numargs
        || !is_input_argument((nth_arg - 1) as usize, info.argmodes.as_ref())
    {
        return Ok(None);
    }

    let mut nth_inputarg = 0i32;
    for i in 0..nth_arg {
        if is_input_argument(i as usize, info.argmodes.as_ref()) {
            nth_inputarg += 1;
        }
    }

    let Some(defs) = &proc.proargdefaults else {
        return Ok(None);
    };
    let node = readfuncs::stringToNode(mcx, defs)?;
    let argdefaults: Vec<types_nodes::Node<'_>> =
        node.as_list().expect("proargdefaults is a List").iter().collect();

    let nth_default = nth_inputarg - 1 - (proc.pronargs - proc.pronargdefaults) as i32;
    if nth_default < 0 || nth_default >= argdefaults.len() as i32 {
        return Ok(None);
    }
    let s = crate::deparse_expression_pretty(
        mcx,
        argdefaults[nth_default as usize],
        InvalidOid,
        false,
        0,
    )?;
    Ok(Some(s))
}

pub fn pg_get_function_result_worker(mcx: Mcx<'_>, funcid: Oid) -> PgResult<Option<String>> {
    let Some(proc) = pg_proc_row(funcid)? else {
        return Ok(None);
    };
    if proc.prokind == PROKIND_PROCEDURE {
        return Ok(None);
    }
    let mut buf = String::new();
    print_function_rettype(mcx, &mut buf, &proc)?;
    Ok(Some(buf))
}
