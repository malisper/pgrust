// inline_function's parser-dependent middle (clauses.c) + the functions.c
// pieces it calls (prepare_sql_fn_parse_info, check_sql_fn_retval). Lives
// here because a clauses->parser dependency cycles; the cheap gates, ACL
// check, recursion guard and re-simplification run in clauses::fold.

use datum::Datum;
use mcx::{Mcx, PgString, PgVec};
use types_core::{InvalidOid, Oid, OidIsValid};
use types_error::{PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERROR};
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::Query;
use types_nodes::primnodes::{CollateExpr, ParamKind};
use types_nodes::{Node, NodeList, NodeTag};
use types_portal::QueryEnvHandle;

use cache_syscache::{ReleaseSysCache, SearchSysCache1, SysCacheGetAttr, SysCacheKey, PROCOID};
use elog::ereport;

use crate::{
    lookup_failed, name_str, read_oidvector_attr, varlena_str,
    ANUM_PG_PROC_PROARGMODES, ANUM_PG_PROC_PROARGNAMES, ANUM_PG_PROC_PROARGTYPES,
    ANUM_PG_PROC_PROISSTRICT, ANUM_PG_PROC_PRONAME, ANUM_PG_PROC_PROSQLBODY,
    ANUM_PG_PROC_PROSRC, ANUM_PG_PROC_PROVOLATILE,
};

const PROVOLATILE_IMMUTABLE: i8 = b'i' as i8;
const PROVOLATILE_STABLE: i8 = b's' as i8;

pub fn init_seams() {
    clauses_seams::inline_sql_function::set(inline_sql_function);
}

pub(crate) struct InlineProcRow<'mcx> {
    pub(crate) proname: PgString<'mcx>,
    pub(crate) prosrc: PgString<'mcx>,
    pub(crate) prosqlbody: Option<PgString<'mcx>>,
    pub(crate) argtypes: PgVec<'mcx, Oid>,
    pub(crate) argnames: PgVec<'mcx, PgString<'mcx>>,
    provolatile: i8,
    proisstrict: bool,
}

pub(crate) fn read_inline_proc_row<'mcx>(
    mcx: Mcx<'mcx>,
    funcid: Oid,
) -> PgResult<InlineProcRow<'mcx>> {
    let Some(tup) = SearchSysCache1(PROCOID, SysCacheKey::Value(Datum::from_oid(funcid)))? else {
        return Err(lookup_failed(funcid));
    };
    let (proname_d, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PRONAME)?;
    let proname = name_str(mcx, proname_d)?;
    let (prosrc_d, prosrc_null) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROSRC)?;
    assert!(!prosrc_null, "null prosrc for function {funcid}");
    let prosrc = varlena_str(mcx, prosrc_d)?;
    let (sqlbody_d, sqlbody_null) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROSQLBODY)?;
    let prosqlbody = if sqlbody_null { None } else { Some(varlena_str(mcx, sqlbody_d)?) };
    // proargtypes holds input args only (pronargs), so OUT params affect
    // nothing here; proargmodes only filters proargnames down to input names
    // (prepare_sql_fn_parse_info -> get_func_input_arg_names).
    let (argv, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROARGTYPES)?;
    let argtypes = read_oidvector_attr(mcx, argv)?;
    let (argnames_d, argnames_null) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROARGNAMES)?;
    let (modes_d, modes_null) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROARGMODES)?;
    let argnames = crate::cache::read_input_argnames(
        mcx,
        argnames_d,
        argnames_null,
        modes_d,
        modes_null,
        argtypes.len(),
    )?;
    let (provolatile, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROVOLATILE)?;
    let (proisstrict, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROISSTRICT)?;
    ReleaseSysCache(tup);
    Ok(InlineProcRow {
        proname,
        prosrc,
        prosqlbody,
        argtypes,
        argnames,
        provolatile: provolatile.as_i8(),
        proisstrict: proisstrict.as_bool(),
    })
}

fn inline_sql_function<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    funcid: Oid,
    result_type: Oid,
    result_collid: Oid,
    input_collid: Oid,
    args: &'a NodeList<'mcx>,
) -> PgResult<Option<Node<'mcx>>> {
    let row = read_inline_proc_row(mcx, funcid)?;
    inline_body(mcx, &row, result_type, result_collid, input_collid, args)
        .map_err(|e| sql_inline_error_callback(e, row.proname.as_str(), row.prosrc.as_str()))
}

// sql_inline_error_callback (clauses.c): a body syntax error is transposed
// to an internal error report pointing at prosrc; every error gets the
// during-inlining context line.
#[cold]
pub(crate) fn sql_inline_error_callback(
    e: Box<PgError>,
    proname: &str,
    prosrc: &str,
) -> Box<PgError> {
    let mut err = *e;
    if let Some(pos) = err.cursor_position() {
        if pos > 0 {
            err = err
                .with_cursor_position(0)
                .with_internal_position(pos)
                .with_internal_query(prosrc.to_string());
        }
    }
    err.add_context_line(format!("SQL function \"{proname}\" during inlining"));
    Box::new(err)
}

// prepare_sql_fn_parse_info (functions.c): resolve polymorphic declared
// argtypes to the concrete types of the actual call (get_call_expr_argtype ==
// exprType of the simplified argument).
pub(crate) fn resolve_polymorphic_argtypes<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    declared_types: &PgVec<'mcx, Oid>,
    args: &'a NodeList<'mcx>,
) -> PgResult<PgVec<'mcx, Oid>> {
    let nargs = declared_types.len();
    let mut argtypes: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, nargs)?;
    for (i, &declared) in declared_types.iter().enumerate() {
        let t = if clauses::fold::is_polymorphic_type(declared) {
            let resolved = match args.len() > i {
                true => nodes_core::node_funcs::expr_type(args.nth(i)),
                false => InvalidOid,
            };
            if !OidIsValid(resolved) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_DATATYPE_MISMATCH)
                    .errmsg(format!(
                        "could not determine actual type of argument declared {}",
                        format_type::format_type_be(declared)?
                    ))
                    .into_error()
                    .into());
            }
            resolved
        } else {
            declared
        };
        argtypes.push(t);
    }
    Ok(argtypes)
}

fn inline_body<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    row: &InlineProcRow<'mcx>,
    result_type: Oid,
    result_collid: Oid,
    input_collid: Oid,
    args: &'a NodeList<'mcx>,
) -> PgResult<Option<Node<'mcx>>> {
    let parsed_query: Query<'mcx>;
    let query: &Query<'mcx> = if let Some(body) = row.prosqlbody.as_ref() {
        // prosqlbody: a List whose first element is the Query list, or a Query.
        let n = readfuncs::stringToNode(mcx, body.as_str())?;
        let single = match n.as_list() {
            Some(outer) => {
                if outer.is_nil() {
                    return Ok(None);
                }
                let first = outer.nth(0);
                match first.node_tag() {
                    NodeTag::T_List => {
                        let inner = first.as_list().expect("tag-checked");
                        if inner.len() != 1 {
                            return Ok(None);
                        }
                        inner.nth(0)
                    }
                    _ => {
                        if outer.len() != 1 {
                            return Ok(None);
                        }
                        first
                    }
                }
            }
            None => n,
        };
        match single.as_query() {
            Some(q) => q,
            None => return Ok(None),
        }
    } else {
        let raw_list = parser_seams::raw_parser::call(
            mcx,
            row.prosrc.as_str(),
            parser_seams::RawParseMode::RAW_PARSE_DEFAULT,
        )?;
        if raw_list.len() != 1 {
            return Ok(None);
        }
        let argtypes = resolve_polymorphic_argtypes(mcx, &row.argtypes, args)?;
        let nargs = row.argtypes.len();
        let mut name_refs: PgVec<'mcx, &str> = mcx::vec_with_capacity_in(mcx, nargs)?;
        for n in row.argnames.iter() {
            name_refs.push(n.as_str());
        }
        parsed_query = analyze_seams::parse_analyze_sql_fn::call(
            mcx,
            &raw_list[0],
            row.prosrc.as_str(),
            row.proname.as_str(),
            &argtypes,
            &name_refs,
            input_collid,
            QueryEnvHandle::NULL,
        )?;
        &parsed_query
    };

    let jointree_empty = match query.jointree {
        None => true,
        Some(jt) => jt.fromlist.is_nil() && jt.quals.is_none(),
    };
    if query.commandType != CmdType::CMD_SELECT
        || query.hasAggs
        || query.hasWindowFuncs
        || query.hasTargetSRFs
        || query.hasSubLinks
        || !query.cteList.is_nil()
        || !query.rtable.is_nil()
        || !jointree_empty
        || !query.groupClause.is_nil()
        || !query.groupingSets.is_nil()
        || query.havingQual.is_some()
        || !query.windowClause.is_nil()
        || !query.distinctClause.is_nil()
        || !query.sortClause.is_nil()
        || query.limitOffset.is_some()
        || query.limitCount.is_some()
        || query.setOperations.is_some()
        || query.targetList.len() != 1
    {
        return Ok(None);
    }

    // check_sql_fn_retval: coerces the lone tlist expression to the call's
    // resolved result type in place, or errors (as C: the function would
    // fail at runtime anyway). Tuple results and injected projections
    // decline inlining (clauses.c:4743-4753).
    let Some(checked) =
        crate::retval::check_query_retval_inline(mcx, crate::clone_query(query), result_type)?
    else {
        return Ok(None);
    };
    let query = mcx::leak_in(mcx::alloc_in(mcx, checked)?);

    let tle = query
        .targetList
        .nth(0)
        .as_target_entry()
        .expect("tlist entry is a TargetEntry");
    let mut newexpr = tle.expr;

    // VOID inlines only when the body already returns VOID.
    if nodes_core::node_funcs::expr_type(newexpr) != result_type {
        return Ok(None);
    }

    if row.provolatile == PROVOLATILE_IMMUTABLE && clauses::contain_mutable_functions(newexpr)? {
        return Ok(None);
    } else if row.provolatile == PROVOLATILE_STABLE
        && clauses::contain_volatile_functions(newexpr)?
    {
        return Ok(None);
    }
    if row.proisstrict && clauses::contain_nonstrict_functions(newexpr)? {
        return Ok(None);
    }
    for a in args {
        if clauses::contain_context_dependent_node(a)? {
            return Ok(None);
        }
    }

    let mut usecounts: PgVec<'mcx, i32> = mcx::vec_with_capacity_in(mcx, args.len())?;
    usecounts.resize(args.len(), 0);
    newexpr = substitute_actual_parameters(mcx, newexpr, args, &mut usecounts)?.unwrap_or(newexpr);

    for (i, param) in args.iter().enumerate() {
        let count = usecounts[i];
        if count == 0 {
            if row.proisstrict {
                return Ok(None);
            }
        } else if count != 1 {
            // Multi-use param: reject subplans, expensive expressions
            // (cost_qual_eval > 10 * cpu_operator_cost), volatiles.
            if clauses::contain_subplans(param)? {
                return Ok(None);
            }
            // C passes a NULL root here.
            let qc = planner::costsize::cost_qual_eval_node(None, param)?;
            if qc.startup + qc.per_tuple > 10.0 * planner::gucs::cpu_operator_cost() {
                return Ok(None);
            }
            if clauses::contain_volatile_functions(param)? {
                return Ok(None);
            }
        }
    }

    if OidIsValid(result_collid) {
        let exprcoll = nodes_core::node_funcs::expr_collation(newexpr);
        if OidIsValid(exprcoll) && exprcoll != result_collid {
            newexpr = Node::mk(
                mcx,
                CollateExpr { arg: newexpr, collOid: result_collid, location: -1 },
            )?;
        }
    }
    Ok(Some(newexpr))
}

// substitute_actual_parameters (clauses.c): PARAM_EXTERN Params become the
// actual argument expressions (shared, as C pre-copyObject), counting uses.
fn substitute_actual_parameters<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    args: &'a NodeList<'mcx>,
    usecounts: &mut PgVec<'mcx, i32>,
) -> PgResult<Option<Node<'mcx>>> {
    if node.node_tag() == NodeTag::T_Param {
        let p = node.as_param().expect("tag-checked");
        if p.paramkind != ParamKind::PARAM_EXTERN {
            return Err(
                PgError::error(format!("unexpected paramkind: {}", p.paramkind as i32)).into()
            );
        }
        if p.paramid <= 0 || p.paramid as usize > args.len() {
            return Err(PgError::error(format!("invalid paramid: {}", p.paramid)).into());
        }
        usecounts[(p.paramid - 1) as usize] += 1;
        return Ok(Some(args.nth((p.paramid - 1) as usize)));
    }
    nodes_core::expression_tree_mutator(mcx, node, &mut |n| {
        substitute_actual_parameters(mcx, n, args, usecounts)
    })
}
