//! `utils/adt/ruleutils.c` — the function-definition deparser
//! (`pg_get_functiondef`, ruleutils.c 2926-3170, plus the `print_function_*`
//! helpers 3259-3607).
//!
//! The worker reverse-lists a `pg_proc` row into a `CREATE OR REPLACE
//! FUNCTION|PROCEDURE … (args) RETURNS … LANGUAGE … <options> AS …` statement.
//! It reads the proc tuple via the `search_pg_functiondef_info` syscache
//! projection (`SearchSysCache1(PROCOID)` + the `proconfig`/`prosqlbody`/
//! `probin`/`prosrc`/`proargdefaults`/`protrftypes` by-reference columns), the
//! argument vectors via `get_func_arg_info`, and renders names / types through
//! `quote_qualified_identifier` / `format_type_be` / `generate_function_name`,
//! the argument defaults and SQL body through the ported deparse engine.

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use ::mcx::{Mcx, PgString};
use ::types_core::primitive::Oid;
use ::types_error::{PgError, PgResult};

use crate::{
    deparse_expression, generate_function_name_catalog, quote_identifier,
    quote_qualified_identifier, simple_quote_literal_into_pub, DeparseNamespace,
    WRAP_COLUMN_DEFAULT,
};

/// `prokind` codes (`catalog/pg_proc.h`).
const PROKIND_AGGREGATE: i8 = b'a' as i8;
const PROKIND_PROCEDURE: i8 = b'p' as i8;
const PROKIND_WINDOW: i8 = b'w' as i8;
/// `provolatile` codes.
const PROVOLATILE_IMMUTABLE: i8 = b'i' as i8;
const PROVOLATILE_STABLE: i8 = b's' as i8;
const PROVOLATILE_VOLATILE: i8 = b'v' as i8;
/// `proparallel` codes.
const PROPARALLEL_SAFE: i8 = b's' as i8;
const PROPARALLEL_RESTRICTED: i8 = b'r' as i8;
const PROPARALLEL_UNSAFE: i8 = b'u' as i8;
/// `proargmode` codes (`catalog/pg_proc.h`).
const PROARGMODE_IN: u8 = b'i';
const PROARGMODE_INOUT: u8 = b'b';
const PROARGMODE_OUT: u8 = b'o';
const PROARGMODE_VARIADIC: u8 = b'v';
const PROARGMODE_TABLE: u8 = b't';
/// `INTERNALlanguageId` / `ClanguageId` / `SQLlanguageId` (`catalog/pg_language.h`).
const INTERNALlanguageId: Oid = 12;
const ClanguageId: Oid = 13;
const SQLlanguageId: Oid = 14;
/// `INTERNALOID` (`catalog/pg_type.h`).
const INTERNALOID: Oid = 2281;
/// `GUC_LIST_QUOTE` (`utils/guc.h`): `0x000002`, "double-quote list elements".
const GUC_LIST_QUOTE: i32 = 0x0002;

/// `pg_get_functiondef(funcid)` (ruleutils.c 2926-3170). Returns the full
/// `CREATE OR REPLACE FUNCTION|PROCEDURE` text, or `Ok(None)` when the function
/// is gone (`PG_RETURN_NULL`).
pub fn pg_get_functiondef<'mcx>(
    mcx: Mcx<'mcx>,
    funcid: Oid,
) -> PgResult<Option<PgString<'mcx>>> {
    let info =
        match syscache_seams::search_pg_functiondef_info::call(mcx, funcid)? {
            Some(i) => i,
            None => return Ok(None),
        };
    let proc = &info.form;
    let name = proc.proname.as_str();

    if proc.prokind == PROKIND_AGGREGATE {
        return Err(PgError::error(format!(
            "\"{name}\" is an aggregate function"
        )));
    }

    let isfunction = proc.prokind != PROKIND_PROCEDURE;

    let mut buf = String::new();

    // Always qualify the function name, to ensure the right function gets
    // replaced.
    let nsp = lsyscache_seams::get_namespace_name_or_temp::call(
        mcx,
        proc.pronamespace,
    )?
    .ok_or_else(|| {
        PgError::error(format!("cache lookup failed for namespace {}", proc.pronamespace))
    })?;
    let qname = quote_qualified_identifier(mcx, Some(nsp.as_str()), name)?;
    buf.push_str("CREATE OR REPLACE ");
    buf.push_str(if isfunction { "FUNCTION " } else { "PROCEDURE " });
    buf.push_str(qname.as_str());
    buf.push('(');
    print_function_arguments(mcx, &mut buf, &info, false, true)?;
    buf.push_str(")\n");

    if isfunction {
        buf.push_str(" RETURNS ");
        print_function_rettype(mcx, &mut buf, &info)?;
        buf.push('\n');
    }

    print_function_trftypes(mcx, &mut buf, &info)?;

    let langname =
        functioncmds_seams::get_language_name::call(proc.prolang)?;
    let qlang = quote_identifier(mcx, langname.as_str())?;
    buf.push_str(" LANGUAGE ");
    buf.push_str(qlang.as_str());
    buf.push('\n');

    // Emit some miscellaneous options on one line.
    let oldlen = buf.len();

    if proc.prokind == PROKIND_WINDOW {
        buf.push_str(" WINDOW");
    }
    match proc.provolatile {
        v if v == PROVOLATILE_IMMUTABLE => buf.push_str(" IMMUTABLE"),
        v if v == PROVOLATILE_STABLE => buf.push_str(" STABLE"),
        v if v == PROVOLATILE_VOLATILE => {}
        _ => {}
    }
    match proc.proparallel {
        p if p == PROPARALLEL_SAFE => buf.push_str(" PARALLEL SAFE"),
        p if p == PROPARALLEL_RESTRICTED => buf.push_str(" PARALLEL RESTRICTED"),
        p if p == PROPARALLEL_UNSAFE => {}
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

    // Default cost / rows (must match functioncmds.c).
    let default_cost: f32 =
        if proc.prolang == INTERNALlanguageId || proc.prolang == ClanguageId {
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

    if crate::oid_is_valid_pub(proc.prosupport) {
        // Qualify the support function's name if not resolvable in the path.
        let argtypes = {
            let mut v = ::mcx::PgVec::new_in(mcx);
            v.try_reserve(1).map_err(|_| mcx.oom(0))?;
            v.push(INTERNALOID);
            v
        };
        let (sname, _uv) = generate_function_name_catalog(
            mcx,
            proc.prosupport,
            1,
            ::mcx::PgVec::new_in(mcx),
            argtypes,
            false,
            false,
            false,
        )?;
        buf.push_str(" SUPPORT ");
        buf.push_str(sname.as_str());
    }

    if oldlen != buf.len() {
        buf.push('\n');
    }

    // Emit any proconfig options, one per line.
    if let Some(config) = info.proconfig.as_ref() {
        for item in config.iter() {
            // name=value split on the first '='; no '=' -> skip.
            let pos = match item.find('=') {
                Some(p) => p,
                None => continue,
            };
            let cfgname = &item[..pos];
            let value = &item[pos + 1..];

            buf.push_str(" SET ");
            let qcfg = quote_identifier(mcx, cfgname)?;
            buf.push_str(qcfg.as_str());
            buf.push_str(" TO ");

            // GUC_LIST_QUOTE vars are pre-quoted as a comma list; re-quote each
            // element as a string literal. Otherwise emit a simple literal.
            let flags = guc_funcs_seams::get_config_option_flags::call(
                String::from(cfgname),
                true,
            )?;
            if flags & GUC_LIST_QUOTE != 0 {
                let namelist = guc_funcs_seams::split_guc_list::call(
                    String::from(value),
                    b',',
                )?
                .ok_or_else(|| PgError::error("invalid list syntax in proconfig item"))?;
                for (i, curname) in namelist.iter().enumerate() {
                    simple_quote_literal_into_pub(&mut buf, curname.as_str());
                    if i + 1 < namelist.len() {
                        buf.push_str(", ");
                    }
                }
            } else {
                simple_quote_literal_into_pub(&mut buf, value);
            }
            buf.push('\n');
        }
    }

    // And finally the function definition.
    if proc.prolang == SQLlanguageId && info.prosqlbody.is_some() {
        print_function_sqlbody(mcx, &mut buf, &info)?;
    } else {
        buf.push_str("AS ");

        if let Some(bin) = info.probin.as_ref() {
            simple_quote_literal_into_pub(&mut buf, bin.as_str());
            buf.push_str(", "); // assume prosrc isn't null
        }

        let prosrc = info.prosrc.as_str();

        // Always dollar-quote; extend the delimiter until it doesn't collide.
        let mut dq = String::from("$");
        dq.push_str(if isfunction { "function" } else { "procedure" });
        while prosrc.contains(&dq) {
            dq.push('x');
        }
        dq.push('$');

        buf.push_str(&dq);
        buf.push_str(prosrc);
        buf.push_str(&dq);
    }

    buf.push('\n');

    Ok(Some(PgString::from_str_in(&buf, mcx)?))
}

/// `pg_get_function_arguments(funcid)` (ruleutils.c 3179-3196). A
/// nicely-formatted list of arguments for a function: everything that would go
/// between the parentheses in `CREATE FUNCTION` (defaults included).
/// `Ok(None)` (`PG_RETURN_NULL`) when the proc tuple is gone.
pub fn pg_get_function_arguments<'mcx>(
    mcx: Mcx<'mcx>,
    funcid: Oid,
) -> PgResult<Option<PgString<'mcx>>> {
    let info =
        match syscache_seams::search_pg_functiondef_info::call(mcx, funcid)? {
            Some(i) => i,
            None => return Ok(None),
        };
    let mut buf = String::new();
    print_function_arguments(mcx, &mut buf, &info, false, true)?;
    Ok(Some(PgString::from_str_in(&buf, mcx)?))
}

/// `pg_get_function_identity_arguments(funcid)` (ruleutils.c 3205-3221). The
/// formatted argument list for `ALTER FUNCTION` etc.: like
/// `pg_get_function_arguments` but without printing defaults.
/// `Ok(None)` (`PG_RETURN_NULL`) when the proc tuple is gone.
pub fn pg_get_function_identity_arguments<'mcx>(
    mcx: Mcx<'mcx>,
    funcid: Oid,
) -> PgResult<Option<PgString<'mcx>>> {
    let info =
        match syscache_seams::search_pg_functiondef_info::call(mcx, funcid)? {
            Some(i) => i,
            None => return Ok(None),
        };
    let mut buf = String::new();
    print_function_arguments(mcx, &mut buf, &info, false, false)?;
    Ok(Some(PgString::from_str_in(&buf, mcx)?))
}

/// `pg_get_function_result(funcid)` (ruleutils.c 3230-3253). A
/// nicely-formatted version of the result type of a function: what would appear
/// after `RETURNS` in `CREATE FUNCTION`. `Ok(None)` (`PG_RETURN_NULL`) when the
/// proc tuple is gone or the object is a procedure (which has no result type).
pub fn pg_get_function_result<'mcx>(
    mcx: Mcx<'mcx>,
    funcid: Oid,
) -> PgResult<Option<PgString<'mcx>>> {
    let info =
        match syscache_seams::search_pg_functiondef_info::call(mcx, funcid)? {
            Some(i) => i,
            None => return Ok(None),
        };
    if info.form.prokind == PROKIND_PROCEDURE {
        return Ok(None);
    }
    let mut buf = String::new();
    print_function_rettype(mcx, &mut buf, &info)?;
    Ok(Some(PgString::from_str_in(&buf, mcx)?))
}

/// `is_input_argument(nth, argmodes)` (ruleutils.c 3290-3295): an argument
/// counts as an input if there are no per-arg modes, or the mode is IN / INOUT
/// / VARIADIC.
fn is_input_argument(nth: usize, argmodes: &[u8]) -> bool {
    argmodes.is_empty()
        || matches!(
            argmodes.get(nth).copied(),
            Some(PROARGMODE_IN) | Some(PROARGMODE_INOUT) | Some(PROARGMODE_VARIADIC)
        )
}

/// `pg_get_function_arg_default(funcid, nth_arg)` (ruleutils.c 3577-3650).
/// Returns the deparsed default expression of the `nth_arg`-th argument
/// (1-based, counting only input arguments per C's `is_input_argument`), or
/// `Ok(None)` (`PG_RETURN_NULL`) when the proc is gone, the argument index is
/// out of range / not an input argument, or that argument has no default.
pub fn pg_get_function_arg_default<'mcx>(
    mcx: Mcx<'mcx>,
    funcid: Oid,
    nth_arg: i32,
) -> PgResult<Option<PgString<'mcx>>> {
    let info =
        match syscache_seams::search_pg_functiondef_info::call(mcx, funcid)? {
            Some(i) => i,
            None => return Ok(None),
        };
    let proc = &info.form;

    let arginfo = funcapi_seams::get_func_arg_info::call(mcx, proc.oid)?;
    let numargs = arginfo.argtypes.len() as i32;
    let argmodes: &[u8] = &arginfo.argmodes;

    if nth_arg < 1 || nth_arg > numargs || !is_input_argument((nth_arg - 1) as usize, argmodes) {
        return Ok(None);
    }

    // nth_inputarg = number of input args among the first nth_arg args.
    let mut nth_inputarg = 0i32;
    for i in 0..nth_arg {
        if is_input_argument(i as usize, argmodes) {
            nth_inputarg += 1;
        }
    }

    // proargdefaults is a List of default Exprs (last N input args); NULL means
    // no defaults at all.
    let defstr = match info.proargdefaults.as_ref() {
        Some(s) => s,
        None => return Ok(None),
    };
    let node = read_seams::string_to_node::call(mcx, defstr.as_str())?;
    let list = node
        .as_list()
        .ok_or_else(|| PgError::error("proargdefaults is not a List"))?;

    // proargdefaults corresponds to the last N input arguments, where
    // N = pronargdefaults.
    let nth_default = nth_inputarg - 1 - (proc.pronargs as i32 - proc.pronargdefaults as i32);
    if nth_default < 0 || nth_default >= list.len() as i32 {
        return Ok(None);
    }

    let expr = list
        .get(nth_default as usize)
        .ok_or_else(|| PgError::error("too few default expressions"))?;
    let s = deparse_expression(mcx, expr, ::mcx::PgVec::new_in(mcx), false, false)?;
    Ok(Some(PgString::from_str_in(s.as_str(), mcx)?))
}

/// `%g`-style rendering of an `f32` cost/rows value (C `appendStringInfo(..,
/// "%g", ..)`). `%g` drops trailing zeros and uses the shortest of fixed/exp.
fn fmt_g(v: f32) -> String {
    // The values here are small whole numbers in practice (COST/ROWS); render
    // an integral value without a decimal point, otherwise the shortest decimal.
    if v == v.trunc() && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

/// `print_function_rettype(buf, proctup)` (ruleutils.c 3259-3288).
fn print_function_rettype<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &mut String,
    info: &types_catalog::pg_proc::PgFunctiondefInfo,
) -> PgResult<()> {
    let proc = &info.form;
    let mut rbuf = String::new();
    let mut ntabargs = 0;

    if proc.proretset {
        // It might be a table function; try to print the arguments.
        rbuf.push_str("TABLE(");
        ntabargs = print_function_arguments(mcx, &mut rbuf, info, true, false)?;
        if ntabargs > 0 {
            rbuf.push(')');
        } else {
            rbuf.clear();
        }
    }

    if ntabargs == 0 {
        // Not a table function, so do the normal thing.
        if proc.proretset {
            rbuf.push_str("SETOF ");
        }
        let ty = format_type_seams::format_type_be::call(mcx, proc.prorettype)?;
        rbuf.push_str(ty.as_str());
    }

    buf.push_str(&rbuf);
    Ok(())
}

/// `print_function_arguments(buf, proctup, print_table_args, print_defaults)`
/// (ruleutils.c 3297-3443). Returns the number of arguments printed. The
/// ordered-set-aggregate path (`insertorderbyat`) is unreachable here
/// (`pg_get_functiondef` rejects aggregates), but is ported for the shared
/// helper contract.
fn print_function_arguments<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &mut String,
    info: &types_catalog::pg_proc::PgFunctiondefInfo,
    print_table_args: bool,
    print_defaults_in: bool,
) -> PgResult<i32> {
    let proc = &info.form;
    let arginfo = funcapi_seams::get_func_arg_info::call(mcx, proc.oid)?;
    let numargs = arginfo.argtypes.len();

    let mut nlackdefaults = numargs as i32;
    // argdefaults: stringToNode(proargdefaults) is a List of default Exprs.
    let mut argdefaults: Vec<nodes::nodes::Node<'mcx>> = Vec::new();
    let mut print_defaults = print_defaults_in;
    if print_defaults && proc.pronargdefaults > 0 {
        if let Some(defstr) = info.proargdefaults.as_ref() {
            let node = read_seams::string_to_node::call(mcx, defstr.as_str())?;
            let list = node
                .as_list()
                .ok_or_else(|| PgError::error("proargdefaults is not a List"))?;
            argdefaults.reserve(list.len());
            for cell in list.iter() {
                argdefaults.push(cell.clone_in(mcx)?);
            }
            // nlackdefaults counts only *input* arguments lacking defaults.
            nlackdefaults = proc.pronargs as i32 - argdefaults.len() as i32;
        }
    }
    let mut nextargdefault: usize = 0;

    // Check for special treatment of ordered-set aggregates.
    let mut insertorderbyat: i32 = -1;
    if proc.prokind == PROKIND_AGGREGATE {
        // aggtup = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(proc->oid));
        // if (!HeapTupleIsValid(aggtup)) elog(ERROR, "cache lookup failed ...");
        let agg = syscache_seams::agg_row_by_oid::call(mcx, proc.oid)?
            .ok_or_else(|| {
                PgError::error(format!("cache lookup failed for aggregate {}", proc.oid))
            })?;
        if types_catalog::pg_aggregate::AGGKIND_IS_ORDERED_SET(agg.aggkind) {
            insertorderbyat = agg.aggnumdirectargs;
        }
    }

    let mut argsprinted = 0i32;
    let mut inputargno = 0i32;
    let mut i = 0usize;
    while i < numargs {
        let argtype = arginfo.argtypes[i];
        let argname = arginfo.argnames.get(i).and_then(|o| o.as_ref());
        let argmode = if arginfo.argmodes.is_empty() {
            PROARGMODE_IN
        } else {
            arginfo.argmodes[i]
        };

        let (modename, isinput) = match argmode {
            PROARGMODE_IN => {
                // For procedures, mark all argument modes explicitly.
                let m = if proc.prokind == PROKIND_PROCEDURE { "IN " } else { "" };
                (m, true)
            }
            PROARGMODE_INOUT => ("INOUT ", true),
            PROARGMODE_OUT => ("OUT ", false),
            PROARGMODE_VARIADIC => ("VARIADIC ", true),
            PROARGMODE_TABLE => ("", false),
            other => {
                return Err(PgError::error(format!(
                    "invalid parameter mode '{}'",
                    other as char
                )))
            }
        };
        if isinput {
            inputargno += 1; // 1-based counter
        }

        if print_table_args != (argmode == PROARGMODE_TABLE) {
            i += 1;
            continue;
        }

        if argsprinted == insertorderbyat {
            if argsprinted > 0 {
                buf.push(' ');
            }
            buf.push_str("ORDER BY ");
        } else if argsprinted > 0 {
            buf.push_str(", ");
        }

        buf.push_str(modename);
        if let Some(an) = argname {
            if !an.as_str().is_empty() {
                let q = quote_identifier(mcx, an.as_str())?;
                buf.push_str(q.as_str());
                buf.push(' ');
            }
        }
        let ty = format_type_seams::format_type_be::call(mcx, argtype)?;
        buf.push_str(ty.as_str());

        if print_defaults && isinput && inputargno > nlackdefaults {
            let expr = argdefaults
                .get(nextargdefault)
                .ok_or_else(|| PgError::error("too few default expressions"))?;
            nextargdefault += 1;
            let s = deparse_expression(mcx, expr, ::mcx::PgVec::new_in(mcx), false, false)?;
            buf.push_str(" DEFAULT ");
            buf.push_str(s.as_str());
        }
        argsprinted += 1;

        // Nasty hack for variadic ordered-set agg (unreachable here).
        if argsprinted == insertorderbyat && i == numargs - 1 {
            // i--;  -- re-print the last arg.
            print_defaults = false;
            continue; // do not advance i
        }
        i += 1;
    }

    Ok(argsprinted)
}

/// `print_function_trftypes(buf, proctup)` (ruleutils.c 3457-3477).
fn print_function_trftypes<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &mut String,
    info: &types_catalog::pg_proc::PgFunctiondefInfo,
) -> PgResult<()> {
    if let Some(trftypes) = info.protrftypes.as_ref() {
        if !trftypes.is_empty() {
            buf.push_str(" TRANSFORM ");
            for (i, &t) in trftypes.iter().enumerate() {
                if i != 0 {
                    buf.push_str(", ");
                }
                let ty = format_type_seams::format_type_be::call(mcx, t)?;
                buf.push_str("FOR TYPE ");
                buf.push_str(ty.as_str());
            }
            buf.push('\n');
        }
    }
    Ok(())
}

/// `print_function_sqlbody(buf, proctup)` (ruleutils.c 3555-3607). Renders the
/// `prosqlbody` `pg_node_tree`: a `List` (multi-statement `BEGIN ATOMIC … END`)
/// or a single `Query` (single-expression body).
fn print_function_sqlbody<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &mut String,
    info: &types_catalog::pg_proc::PgFunctiondefInfo,
) -> PgResult<()> {
    let proc = &info.form;
    // dpns.funcname / numargs / argnames — the function-signature namespace.
    let arginfo = funcapi_seams::get_func_arg_info::call(mcx, proc.oid)?;
    let mut dpns = DeparseNamespace::zeroed(mcx);
    dpns.funcname = Some(PgString::from_str_in(proc.proname.as_str(), mcx)?);
    dpns.numargs = arginfo.argtypes.len() as i32;
    dpns.argnames = {
        let mut v = ::mcx::PgVec::new_in(mcx);
        v.try_reserve(arginfo.argnames.len()).map_err(|_| mcx.oom(0))?;
        for a in arginfo.argnames.iter() {
            v.push(match a {
                Some(s) => Some(PgString::from_str_in(s.as_str(), mcx)?),
                None => None,
            });
        }
        v
    };

    let body = info
        .prosqlbody
        .as_ref()
        .ok_or_else(|| PgError::error("prosqlbody is null"))?;
    let n = read_seams::string_to_node::call(mcx, body.as_str())?;

    let mut out_buf = stringinfo::StringInfo::new_in(mcx);
    out_buf.data.extend_from_slice(buf.as_bytes());

    if let Some(outer) = n.as_list() {
        // stmts = linitial(castNode(List, n)) — the first element is the
        // statement (Query) list.
        let first = outer
            .first()
            .ok_or_else(|| PgError::error("empty prosqlbody List"))?;
        let stmts = first
            .as_list()
            .ok_or_else(|| PgError::error("prosqlbody first element is not a List"))?;

        out_buf.data.extend_from_slice(b"BEGIN ATOMIC\n");

        for cell in stmts.iter() {
            let q = cell
                .as_query()
                .ok_or_else(|| PgError::error("prosqlbody statement is not a Query"))?;
            let mut query = q.clone_in(mcx)?;
            // AcquireRewriteLocks(query, false, false).
            ruleutils_seams::acquire_rewrite_locks::call(
                mcx, &mut query, false, false,
            )?;
            let one = clone_one_ns(mcx, &dpns)?;
            out_buf = crate::query_deparse::get_query_def(
                mcx,
                &query,
                out_buf,
                &one,
                None,
                false,
                crate::PRETTYFLAG_INDENT,
                WRAP_COLUMN_DEFAULT,
                1,
            )?;
            out_buf.data.extend_from_slice(b";\n");
        }

        out_buf.data.extend_from_slice(b"END");
    } else {
        let q = n
            .as_query()
            .ok_or_else(|| PgError::error("prosqlbody is neither List nor Query"))?;
        let mut query = q.clone_in(mcx)?;
        ruleutils_seams::acquire_rewrite_locks::call(
            mcx, &mut query, false, false,
        )?;
        let one = clone_one_ns(mcx, &dpns)?;
        out_buf = crate::query_deparse::get_query_def(
            mcx,
            &query,
            out_buf,
            &one,
            None,
            false,
            0,
            WRAP_COLUMN_DEFAULT,
            0,
        )?;
    }

    *buf = String::from(
        core::str::from_utf8(out_buf.data.as_slice())
            .map_err(|_| PgError::error("function body deparse produced invalid UTF-8"))?,
    );
    Ok(())
}

/// `list_make1(&dpns)` — a one-element namespace slice carrying a deep copy of
/// the function-signature namespace (the owned model has no shared pointers).
fn clone_one_ns<'mcx>(
    mcx: Mcx<'mcx>,
    dpns: &DeparseNamespace<'mcx>,
) -> PgResult<[DeparseNamespace<'mcx>; 1]> {
    Ok([crate::clone_namespace_pub(mcx, dpns)?])
}
