//! The SQL-language body parse/rewrite/validate core of
//! `inline_set_returning_function` (clauses.c:5175 onward) — installed into the
//! `inline_set_returning_function_sql_body` seam from this crate.
//!
//! `clauses.c`'s `inline_set_returning_function` runs the cheap pg_proc catalog
//! gate ladder (ORDINALITY, single simple `FuncExpr`, `funcretset`,
//! volatile/sub-select-free args, EXECUTE privilege, no fmgr hook, LANGUAGE SQL /
//! not STRICT / not VOLATILE / not SECURITY DEFINER / non-VOID rettype / no
//! `proconfig`) in-crate (it owns the gate over the real owned-`Expr` `FuncExpr`).
//! Only once the ladder confirms an inlinable SQL-language SRF does it enter this
//! parser-dependent core, which cannot live in the optimizer/clauses crate
//! (which must not depend on the parser/rewriter). It rides the
//! `inline_set_returning_function_sql_body` seam, installed here:
//! `backend-parser-analyze` is the lowest crate that owns the parser
//! (`raw_parser` / `parse_analyze_sql_function`), the rewriter
//! (`query_rewrite_canonical` / `acquire_rewrite_locks`), and may reach the
//! funcapi/setrefs helpers without a dependency cycle.
//!
//! The core: fetch `prosrc`/`prosqlbody`, parse + rewrite + validate that the
//! body is a single plain SELECT, resolve the function result tupdesc, run
//! `check_sql_fn_retval` (coercing the final tlist to the declared return type
//! and inserting dummy NULL columns for dropped composite columns), then
//! substitute the actual call arguments for the body Params. Returns the inlined
//! `Query` (`Ok(Some)`) to substitute as the RTE's subquery, or `Ok(None)` to
//! decline (every C `goto fail`). `record_plan_function_dependency` and the
//! RLS `dependsOnRole` marking happen in the gate caller (the clauses crate,
//! which holds `root`).

use alloc::string::String;
use alloc::vec::Vec;

use mcx::Mcx;
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult};
use types_nodes::copy_query::Query;
use types_nodes::funcapi::TypeFuncClass;
use types_nodes::nodes::{ntag, CmdType, Node};
use types_nodes::parsenodes::{RangeTblEntry, RTEKind};
use types_nodes::primnodes::{CoercionForm, Expr, FuncExpr, ParamKind, TargetEntry};
use types_nodes::rawnodes::RangeTblRef;
use types_parsenodes::{CoercionContext, RawParseMode};

use backend_nodes_core::nodefuncs::expr_type;
use backend_optimizer_util_clauses_seams as clauses_seam;

/// `VOIDOID` / `RECORDOID` / `INT4OID` (pg_type.dat).
const VOIDOID: Oid = 2278;
const RECORDOID: Oid = 2249;
const INT4OID: Oid = 23;

/// `TYPTYPE_*` (pg_type.h) — `typtype` bytes.
const TYPTYPE_BASE: u8 = b'b';
const TYPTYPE_COMPOSITE: u8 = b'c';
const TYPTYPE_DOMAIN: u8 = b'd';
const TYPTYPE_ENUM: u8 = b'e';
const TYPTYPE_RANGE: u8 = b'r';
const TYPTYPE_MULTIRANGE: u8 = b'm';

/// `PROKIND_PROCEDURE` (pg_proc.h).
const PROKIND_PROCEDURE: u8 = b'p';

/// The `inline_set_returning_function_sql_body` seam body. See module docs. The
/// gate ladder is already cleared by the in-crate caller; here we parse, rewrite,
/// validate, and substitute parameters.
pub fn inline_set_returning_function_sql_body<'mcx>(
    mcx: Mcx<'mcx>,
    _root: &mut types_pathnodes::PlannerInfo,
    rte: &RangeTblEntry<'mcx>,
    funcid: Oid,
) -> PgResult<Option<Query<'mcx>>> {
    debug_assert_eq!(rte.rtekind, RTEKind::RTE_FUNCTION);

    // The gate caller already confirmed a single simple FuncExpr; re-extract it.
    // `rte.functions[0]` is a `Node` wrapping a `RangeTblFunction`.
    let rtfunc = match rte.functions[0].as_rangetblfunction() {
        Some(r) => r,
        None => return Ok(None),
    };
    let fexpr: &FuncExpr = match rtfunc.funcexpr.as_deref().and_then(|n| n.as_funcexpr()) {
        Some(fe) => fe,
        None => return Ok(None),
    };

    // pg_proc form: pronargs / prorettype / prokind, fetched by OID (the caller
    // already verified all the showstopper properties).
    let form = clauses_seam::get_func_form::call(funcid)?;

    // Fetch the function body (prosrc; prosqlbody node-string when present). C:
    //   tmp = SysCacheGetAttrNotNull(PROCOID, func_tuple, Anum_pg_proc_prosrc);
    //   src = TextDatumGetCString(tmp);
    //   tmp = SysCacheGetAttr(PROCOID, func_tuple, Anum_pg_proc_prosqlbody, &isNull);
    let (prosrc, prosqlbody) = clauses_seam::get_func_sql_body::call(mcx, funcid)?;

    // Re-home prosrc into the mcx arena and leak the binding to 'mcx so
    // raw_parser (which borrows its source for 'mcx) can hold it.
    let prosrc_mcx: &'mcx str = {
        let boxed = mcx::alloc_in(mcx, prosrc)?;
        mcx::leak_in(boxed).as_str()
    };

    // ---- parse / rewrite the body (clauses.c:5197/5224) -------------------
    // querytree_list is the rewritten single-query list (C's querytree_list).
    let mut querytree: Query<'mcx> = if let Some(body) = prosqlbody.as_ref() {
        // If we have prosqlbody, pay attention to that not prosrc.
        //   n = stringToNode(prosqlbody);
        //   if (IsA(n, List)) querytree_list = linitial_node(List, n);
        //   else              querytree_list = list_make1(n);
        //   if (list_length(querytree_list) != 1) goto fail;
        //   querytree = linitial(querytree_list);
        //   AcquireRewriteLocks(querytree, true, false);
        //   querytree_list = pg_rewrite_query(querytree);
        //   if (list_length(querytree_list) != 1) goto fail;
        let n = backend_nodes_core::read::string_to_node(mcx, body.as_str())?;
        let query = match extract_single_body_query(mcx, &n)? {
            Some(q) => q,
            None => return Ok(None),
        };
        // AcquireRewriteLocks(querytree, true, false).
        let query = backend_rewrite_rewritehandler_seams::acquire_rewrite_locks::call(
            mcx, query, true, false,
        )?;
        // pg_rewrite_query(querytree).
        let rewritten =
            backend_rewrite_rewritehandler_seams::query_rewrite_canonical::call(mcx, query)?;
        if rewritten.len() != 1 {
            return Ok(None);
        }
        rewritten.into_iter().next().unwrap()
    } else {
        // pg_parse_query(src); fail as soon as we find more than one query.
        let raw_list =
            backend_parser_driver::raw_parser(mcx, prosrc_mcx, RawParseMode::RAW_PARSE_DEFAULT)?;
        if raw_list.len() != 1 {
            return Ok(None);
        }
        // prepare_sql_fn_parse_info + sql_fn_parser_setup (clauses.c:5208/5217):
        // a `$n` resolves against proargtypes and a body bareword that names an
        // argument resolves to its Param, under the call's input collation.
        // Polymorphic declared argument types resolve to the concrete type
        // implied by the actual call (get_call_expr_argtype) — see inline_sql.
        let nargs = form.proargtypes.len();
        let mut argtypes = form.proargtypes.clone();
        for (argnum, argtype) in argtypes.iter_mut().enumerate() {
            if is_polymorphic_type(*argtype) {
                let resolved = match fexpr.args.get(argnum) {
                    Some(a) => expr_type(Some(a))?,
                    None => InvalidOid,
                };
                if resolved == InvalidOid {
                    return Err(PgError::error(format!(
                        "could not determine actual type of argument declared {}",
                        *argtype
                    )));
                }
                *argtype = resolved;
            }
        }
        let argnames = match &form.proargnames {
            Some(names) if names.len() >= nargs && nargs > 0 => Some(names.clone()),
            _ => None,
        };
        let pinfo = types_nodes::parsestmt::SqlFnParseInfo::new(
            form.proname.clone(),
            fexpr.inputcollid,
            argtypes,
            argnames,
        );
        // pg_analyze_and_rewrite_withcb: analyze with the SQL-function hooks,
        // then apply the rewriter (unlike inline_function, we can't skip it).
        let analyzed = crate::parse_analyze_sql_function(mcx, &raw_list[0], prosrc_mcx, pinfo)?;
        let rewritten =
            backend_rewrite_rewritehandler_seams::query_rewrite_canonical::call(mcx, analyzed)?;
        if rewritten.len() != 1 {
            return Ok(None);
        }
        rewritten.into_iter().next().unwrap()
    };

    // ---- resolve the function result tupdesc (clauses.c:5253) -------------
    // If we have a coldeflist, believe that; otherwise use get_expr_result_type.
    // (This logic should match ExecInitFunctionScan.) `rettupdesc` is the C
    // `TupleDesc` (`Option<PgBox<TupleDescData>>`); `None` is the C NULL.
    let functypclass: TypeFuncClass;
    let rettupdesc: types_tuple::heaptuple::TupleDesc<'mcx>;
    if !rtfunc.funccolnames.is_empty() {
        functypclass = TypeFuncClass::Record;
        let names: Vec<&str> = rtfunc
            .funccolnames
            .iter()
            .map(|n| n.as_string().map(|s| s.sval.as_str()).unwrap_or(""))
            .collect();
        let types: Vec<Oid> = rtfunc.funccoltypes.iter().copied().collect();
        let typmods: Vec<i32> = rtfunc.funccoltypmods.iter().copied().collect();
        let collations: Vec<Oid> = rtfunc.funccolcollations.iter().copied().collect();
        let desc = backend_access_common_tupdesc::BuildDescFromLists(
            mcx,
            &names,
            &types,
            &typmods,
            &collations,
        )?;
        rettupdesc = Some(mcx::alloc_in(mcx, desc)?);
    } else {
        // get_expr_result_type((Node *) fexpr, NULL, &rettupdesc).
        let fexpr_node = Node::mk_expr(mcx, Expr::FuncExpr(fexpr.clone()))?;
        let resolved =
            backend_utils_fmgr_funcapi_seams::get_expr_result_type::call(mcx, Some(&fexpr_node))?;
        functypclass = resolved.class.unwrap_or(TypeFuncClass::Other);
        rettupdesc = resolved.result_tuple_desc;
    }

    // ---- the single command must be a plain SELECT (clauses.c:5266) -------
    if querytree.commandType != CmdType::CMD_SELECT {
        return Ok(None);
    }

    // ---- check_sql_fn_retval (clauses.c:5283) -----------------------------
    // Make sure the function (still) returns what it's declared to. This raises
    // an error if wrong (fine — the function would fail at runtime anyway), and
    // inserts coercions / dummy dropped-column NULLs in place. If the function
    // returns composite, don't inline unless the check shows a whole-tuple
    // result; otherwise it's a single composite column, not what we need.
    let mut query_list = vec![querytree];
    let is_tuple_result = check_sql_fn_retval(
        mcx,
        &mut query_list,
        fexpr.funcresulttype,
        rettupdesc.as_deref(),
        form.prokind,
        true,
    )?;
    if !is_tuple_result
        && (functypclass == TypeFuncClass::Composite
            || functypclass == TypeFuncClass::CompositeDomain
            || functypclass == TypeFuncClass::Record)
    {
        return Ok(None); // reject not-whole-tuple-result cases
    }

    // check_sql_fn_retval might've inserted a projection step (replacing the
    // element in query_list); use the upper Query.
    querytree = query_list.into_iter().next().unwrap();

    // ---- substitute parameters into the query (clauses.c:5300) ------------
    substitute_actual_srf_parameters(mcx, &mut querytree, form.pronargs as i32, &fexpr.args)?;

    // C copies the modified query out of the temp context here; in the owned
    // model the query is already arena-allocated in `mcx`, so no extra copy.
    Ok(Some(querytree))
}

/// `stringToNode(prosqlbody)` → the single body `Query` (clauses.c:5226). `n`
/// is either a `List` (whose first element is the query list) or a bare Query.
/// `Ok(None)` for `list_length(query_list) != 1`.
fn extract_single_body_query<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    n: &'a Node<'mcx>,
) -> PgResult<Option<Query<'mcx>>> {
    let query_list: &[types_nodes::nodes::NodePtr<'mcx>] = match n.node_tag() {
        ntag::T_List => {
            let outer = n.expect_list();
            let first = match outer.first() {
                Some(p) => p,
                None => return Ok(None),
            };
            match (**first).node_tag() {
                ntag::T_List => &(**first).expect_list()[..],
                ntag::T_Query => core::slice::from_ref(first),
                _ => return Ok(None),
            }
        }
        ntag::T_Query => {
            return Ok(Some(query_clone_from_node(mcx, n)?));
        }
        _ => return Ok(None),
    };
    if query_list.len() != 1 {
        return Ok(None);
    }
    query_clone_from_node(mcx, &query_list[0]).map(Some)
}

/// Clone a `Query` out of a `Node::Query` node ptr.
fn query_clone_from_node<'mcx>(mcx: Mcx<'mcx>, n: &Node<'mcx>) -> PgResult<Query<'mcx>> {
    match n.as_query() {
        Some(q) => q.clone_in(mcx),
        None => Err(PgError::error(
            "inline_set_returning_function: prosqlbody element is not a Query",
        )),
    }
}

/* ===========================================================================
 * check_sql_fn_retval (functions.c:2116) and its helpers
 * =========================================================================== */

/// `check_sql_fn_retval(queryTreeLists, rettype, rettupdesc, prokind,
/// insertDroppedCols)` (functions.c:2116). We consider only the last sublist;
/// the SRF caller passes `list_make1(querytree_list)`, i.e. exactly one sublist,
/// whose lone Query is the body. Returns `true` if the function returns the
/// entire tuple result of its final statement, `false` if just the first column.
/// May modify the final Query (in `query_list[0]`) by inserting a projection
/// level. Raises an error if the final statement can't return the right type.
/// Public entry point onto `check_sql_fn_retval` for the SQL-function call
/// handler (`backend-executor-functions`'s `fmgr_sql` / `init_sql_fcache`).
///
/// `init_sql_fcache` (functions.c:973-978) runs `check_sql_stmt_retval` over the
/// last analyzed body query with the call-time-resolved `rettype`/`rettupdesc`
/// (from `get_call_result_type`), so the body's final targetlist is coerced to
/// the *resolved* polymorphic result types and `returnsTuple` is computed. This
/// re-exports the same machinery the SRF inliner uses, for the call-time path.
pub fn check_sql_fn_retval_public<'mcx>(
    mcx: Mcx<'mcx>,
    query_list: &mut [Query<'mcx>],
    rettype: Oid,
    rettupdesc: Option<&types_tuple::heaptuple::TupleDescData<'mcx>>,
    prokind: u8,
    insert_dropped_cols: bool,
) -> PgResult<bool> {
    check_sql_fn_retval(
        mcx,
        query_list,
        rettype,
        rettupdesc,
        prokind,
        insert_dropped_cols,
    )
}

fn check_sql_fn_retval<'mcx>(
    mcx: Mcx<'mcx>,
    query_list: &mut [Query<'mcx>],
    rettype: Oid,
    rettupdesc: Option<&types_tuple::heaptuple::TupleDescData<'mcx>>,
    prokind: u8,
    insert_dropped_cols: bool,
) -> PgResult<bool> {
    // The SRF inliner always passes a single non-empty sublist; we operate on
    // its lone Query (the last canSetTag query, which is the body SELECT).
    check_sql_stmt_retval(
        mcx,
        query_list,
        rettype,
        rettupdesc,
        prokind,
        insert_dropped_cols,
    )
}

/// `check_sql_stmt_retval(queryTreeList, ...)` (functions.c:2150). Given the last
/// query's rewritten-queries list, validate/coerce the result tlist. Here the
/// list is `query_list` and the candidate result query is the last `canSetTag`
/// one (for the SRF body inliner there is exactly one).
fn check_sql_stmt_retval<'mcx>(
    mcx: Mcx<'mcx>,
    query_list: &mut [Query<'mcx>],
    rettype: Oid,
    rettupdesc: Option<&types_tuple::heaptuple::TupleDescData<'mcx>>,
    prokind: u8,
    insert_dropped_cols: bool,
) -> PgResult<bool> {
    let mut is_tuple_result = false;

    // If declared to return VOID, we don't care what's in the function.
    if rettype == VOIDOID {
        return Ok(false);
    }

    // Find the last canSetTag query in the list.
    let parse_idx = {
        let mut found: Option<usize> = None;
        for (i, q) in query_list.iter().enumerate() {
            if q.canSetTag {
                found = Some(i);
            }
        }
        found
    };

    // Determine the tlist (target list) we'll coerce, and whether it's
    // modifiable in place. Plain SELECT → targetList (modifiable unless a setop
    // dummy); INSERT/UPDATE/DELETE/MERGE RETURNING → returningList (always
    // modifiable). Otherwise the final statement is a utility command or it
    // rewrote to nothing — error.
    let tlist_is_returning: bool;
    let tlist_is_modifiable: bool;
    match parse_idx {
        Some(i) if query_list[i].commandType == CmdType::CMD_SELECT => {
            tlist_is_returning = false;
            tlist_is_modifiable = query_list[i].setOperations.is_none();
        }
        Some(i)
            if matches!(
                query_list[i].commandType,
                CmdType::CMD_INSERT
                    | CmdType::CMD_UPDATE
                    | CmdType::CMD_DELETE
                    | CmdType::CMD_MERGE
            ) && !query_list[i].returningList.is_empty() =>
        {
            tlist_is_returning = true;
            tlist_is_modifiable = true;
        }
        _ => {
            return Err(retval_mismatch(
                rettype,
                "Function's final statement must be SELECT or INSERT/UPDATE/DELETE/MERGE RETURNING.",
            ));
        }
    }
    let parse_idx = parse_idx.unwrap();

    // Count the non-junk entries in the result targetlist.
    let tlistlen = {
        let tlist = current_tlist(&query_list[parse_idx], tlist_is_returning);
        exec_clean_target_list_length(tlist)
    };

    let fn_typtype = backend_utils_cache_lsyscache_seams::get_typtype::call(rettype)?;

    // upper_tlist accumulates a projection if in-place coercion isn't possible.
    let mut upper_tlist: Vec<TargetEntry<'mcx>> = Vec::new();
    let mut upper_tlist_nontrivial = false;

    if fn_typtype == TYPTYPE_BASE
        || fn_typtype == TYPTYPE_DOMAIN
        || fn_typtype == TYPTYPE_ENUM
        || fn_typtype == TYPTYPE_RANGE
        || fn_typtype == TYPTYPE_MULTIRANGE
    {
        // Scalar-type returns: exactly one non-junk entry, coercible to rettype.
        if tlistlen != 1 {
            return Err(retval_mismatch(
                rettype,
                "Final statement must return exactly one column.",
            ));
        }
        // Non-junk TLEs come first; the first entry is the result column.
        let src_type = {
            let tlist = current_tlist(&query_list[parse_idx], tlist_is_returning);
            expr_type(tlist[0].expr.as_deref())?
        };
        if !coerce_fn_result_column(
            mcx,
            current_tlist_mut(&mut query_list[parse_idx], tlist_is_returning),
            0,
            rettype,
            -1,
            tlist_is_modifiable,
            &mut upper_tlist,
            &mut upper_tlist_nontrivial,
        )? {
            return Err(retval_mismatch_actual(rettype, src_type));
        }
    } else if fn_typtype == TYPTYPE_COMPOSITE || rettype == RECORDOID {
        // Returns a rowtype.
        //
        // If the tlist has one non-junk entry coercible to the declared return
        // type, take it as the result (e.g. SELECT func2() of the same composite
        // type). Not for a procedure (RECORD rettype with output params).
        if tlistlen == 1 && prokind != PROKIND_PROCEDURE {
            let coerced = coerce_fn_result_column(
                mcx,
                current_tlist_mut(&mut query_list[parse_idx], tlist_is_returning),
                0,
                rettype,
                -1,
                tlist_is_modifiable,
                &mut upper_tlist,
                &mut upper_tlist_nontrivial,
            )?;
            if coerced {
                // NOT setting is_tuple_result.
                return finish_tlist_coercion(
                    mcx,
                    query_list,
                    parse_idx,
                    tlist_is_returning,
                    upper_tlist,
                    upper_tlist_nontrivial,
                    is_tuple_result,
                );
            }
        }

        // If the caller didn't provide an expected tupdesc, we can't check
        // further; assume we're returning the whole tuple.
        let rettupdesc = match rettupdesc {
            Some(d) => d,
            None => return Ok(true),
        };

        // Verify the targetlist matches the return tuple type. Scan the non-junk
        // columns, coercing them to the non-deleted attributes; insert NULL
        // result columns for dropped attributes if asked.
        let tupnatts = rettupdesc.natts;
        let mut tuplogcols: i32 = 0;
        let mut colindex: i32 = 0;

        let n_tlist = current_tlist(&query_list[parse_idx], tlist_is_returning).len();
        for src_index in 0..n_tlist {
            // resjunk columns can simply be ignored.
            if current_tlist(&query_list[parse_idx], tlist_is_returning)[src_index].resjunk {
                continue;
            }

            // Advance colindex past dropped columns, inserting NULLs for them.
            let attr;
            loop {
                colindex += 1;
                if colindex > tupnatts {
                    return Err(retval_mismatch(
                        rettype,
                        "Final statement returns too many columns.",
                    ));
                }
                let a = rettupdesc.attr((colindex - 1) as usize);
                if a.attisdropped {
                    if insert_dropped_cols {
                        push_null_column(mcx, &mut upper_tlist)?;
                        upper_tlist_nontrivial = true;
                    }
                    continue;
                }
                attr = a;
                break;
            }
            tuplogcols += 1;

            let atttypid = attr.atttypid;
            let atttypmod = attr.atttypmod;
            let src_type = {
                let tlist = current_tlist(&query_list[parse_idx], tlist_is_returning);
                expr_type(tlist[src_index].expr.as_deref())?
            };
            if !coerce_fn_result_column(
                mcx,
                current_tlist_mut(&mut query_list[parse_idx], tlist_is_returning),
                src_index,
                atttypid,
                atttypmod,
                tlist_is_modifiable,
                &mut upper_tlist,
                &mut upper_tlist_nontrivial,
            )? {
                return Err(retval_mismatch_col(
                    rettype, src_type, atttypid, tuplogcols,
                ));
            }
        }

        // Remaining columns in rettupdesc had better all be dropped.
        colindex += 1;
        while colindex <= tupnatts {
            if !rettupdesc.attr((colindex - 1) as usize).attisdropped {
                return Err(retval_mismatch(
                    rettype,
                    "Final statement returns too few columns.",
                ));
            }
            if insert_dropped_cols {
                push_null_column(mcx, &mut upper_tlist)?;
                upper_tlist_nontrivial = true;
            }
            colindex += 1;
        }

        // Report that we are returning entire tuple result.
        is_tuple_result = true;
    } else {
        return Err(PgError::error(format!(
            "return type {rettype} is not supported for SQL functions"
        )));
    }

    finish_tlist_coercion(
        mcx,
        query_list,
        parse_idx,
        tlist_is_returning,
        upper_tlist,
        upper_tlist_nontrivial,
        is_tuple_result,
    )
}

/// The `tlist_coercion_finished:` tail of `check_sql_stmt_retval`
/// (functions.c:2446): if a projection is needed, inject an extra Query level
/// that does just the projection, replacing the body Query in `query_list`.
#[allow(clippy::too_many_arguments)]
fn finish_tlist_coercion<'mcx>(
    mcx: Mcx<'mcx>,
    query_list: &mut [Query<'mcx>],
    parse_idx: usize,
    tlist_is_returning: bool,
    upper_tlist: Vec<TargetEntry<'mcx>>,
    upper_tlist_nontrivial: bool,
    is_tuple_result: bool,
) -> PgResult<bool> {
    if !upper_tlist_nontrivial {
        return Ok(is_tuple_result);
    }

    // Only ever applied to a plain SELECT (RETURNING lists are modified in
    // place, so upper_tlist_nontrivial can't be set for them).
    debug_assert!(!tlist_is_returning);
    debug_assert_eq!(query_list[parse_idx].commandType, CmdType::CMD_SELECT);

    // Build a column-name list for the subquery RTE eref from the original
    // (lower) query's non-junk tlist. C points `rte->eref` and `rte->alias` at
    // the same `makeAlias` node; the owned model needs two equal copies, so
    // gather the names once and build two String-node lists.
    let names: Vec<&str> = query_list[parse_idx]
        .targetList
        .iter()
        .filter(|tle| !tle.resjunk)
        .map(|tle| tle.resname.as_deref().unwrap_or(""))
        .collect();
    let make_colnames = |mcx: Mcx<'mcx>| -> PgResult<mcx::PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>>> {
        let mut colnames: mcx::PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>> =
            mcx::vec_with_capacity_in(mcx, names.len())?;
        for name in &names {
            let sval = mcx::PgString::from_str_in(name, mcx)?;
            let node = Node::mk_string(mcx, types_nodes::value::StringNode { sval })?;
            colnames.push(mcx::alloc_in(mcx, node)?);
        }
        Ok(colnames)
    };
    let eref = backend_nodes_core::makefuncs::make_alias(mcx, "*SELECT*", make_colnames(mcx)?)?;
    let alias = backend_nodes_core::makefuncs::make_alias(mcx, "*SELECT*", make_colnames(mcx)?)?;

    // Move the original query out, build a subquery RTE around it.
    let lower = core::mem::replace(&mut query_list[parse_idx], Query::new(mcx));
    let query_source = lower.querySource;
    let has_row_security = lower.hasRowSecurity;

    let mut rte = RangeTblEntry::new_in(mcx);
    rte.rtekind = RTEKind::RTE_SUBQUERY;
    rte.subquery = Some(mcx::alloc_in(mcx, lower)?);
    rte.eref = Some(mcx::alloc_in(mcx, eref)?);
    rte.alias = Some(mcx::alloc_in(mcx, alias)?);
    rte.lateral = false;
    rte.inh = false;
    rte.inFromCl = true;

    // newquery: most of the upper Query is zeroes/nulls.
    let mut newquery = Query::new(mcx);
    newquery.commandType = CmdType::CMD_SELECT;
    newquery.querySource = query_source;
    newquery.canSetTag = true;
    newquery.targetList = vec_to_pgvec(mcx, upper_tlist)?;
    newquery.hasRowSecurity = has_row_security;

    let mut rtable: mcx::PgVec<'mcx, RangeTblEntry<'mcx>> = mcx::vec_with_capacity_in(mcx, 1)?;
    rtable.push(rte);
    newquery.rtable = rtable;

    // jointree = makeFromExpr(list_make1(makeNode(RangeTblRef){rtindex=1}), NULL).
    let rtr = RangeTblRef { rtindex: 1 };
    let rtr_node = Node::mk_range_tbl_ref(mcx, rtr)?;
    let mut fromlist: mcx::PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>> =
        mcx::vec_with_capacity_in(mcx, 1)?;
    fromlist.push(mcx::alloc_in(mcx, rtr_node)?);
    let fromexpr = backend_nodes_core::makefuncs::make_from_expr(fromlist, None);
    newquery.jointree = Some(mcx::alloc_in(mcx, fromexpr)?);

    query_list[parse_idx] = newquery;
    Ok(is_tuple_result)
}

/// `coerce_fn_result_column(src_tle, res_type, res_typmod, tlist_is_modifiable,
/// upper_tlist, upper_tlist_nontrivial)` (functions.c:2519). Coerce the
/// `tlist[src_index]` output value to the required type/typmod and append a
/// column to `upper_tlist`. Returns `false` (no changes) if not coercible.
#[allow(clippy::too_many_arguments)]
fn coerce_fn_result_column<'mcx>(
    mcx: Mcx<'mcx>,
    tlist: &mut [TargetEntry<'mcx>],
    src_index: usize,
    res_type: Oid,
    res_typmod: i32,
    tlist_is_modifiable: bool,
    upper_tlist: &mut Vec<TargetEntry<'mcx>>,
    upper_tlist_nontrivial: &mut bool,
) -> PgResult<bool> {
    let new_tle_expr: Expr<'mcx>;
    let resname = tlist[src_index].resname.as_deref().map(String::from);

    // If the TLE has a sortgroupref, don't change it (referenced by
    // ORDER BY/DISTINCT etc); otherwise modify in place if the query allows.
    if tlist_is_modifiable && tlist[src_index].ressortgroupref == 0 {
        let src_expr = match tlist[src_index].expr.as_deref() {
            Some(e) => e.clone(),
            None => return Ok(false),
        };
        let exprtype = expr_type(Some(&src_expr))?;
        // `coerce_to_target_type` works in the parser-arena `'static`; erase the
        // `'mcx` source going in and re-clone the result back into `'mcx`
        // (invariant `Expr`).
        let cast = backend_parser_coerce::coerce_to_target_type(
            mcx,
            None,
            src_expr.erase_lifetime(),
            exprtype,
            res_type,
            res_typmod,
            CoercionContext::COERCION_ASSIGNMENT,
            CoercionForm::COERCE_IMPLICIT_CAST,
            -1,
        )?;
        let mut cast: Expr<'mcx> = match cast {
            Some(c) => c.clone_in(mcx)?,
            None => return Ok(false),
        };
        backend_parser_parse_collate::assign_expr_collations_in(mcx, &mut cast)?;
        // src_tle->expr = cast_result.
        tlist[src_index].expr = Some(mcx::alloc_in(mcx, cast)?);
        // Make a Var referencing the possibly-modified TLE.
        let var = backend_nodes_core::makefuncs::make_var_from_target_entry(1, &tlist[src_index])?;
        new_tle_expr = Expr::Var(var);
    } else {
        // Any casting must happen in the upper tlist.
        let var = backend_nodes_core::makefuncs::make_var_from_target_entry(1, &tlist[src_index])?;
        let vartype = var.vartype;
        let var_expr = Expr::Var(var);
        let cast = backend_parser_coerce::coerce_to_target_type(
            mcx,
            None,
            var_expr.clone().erase_lifetime(),
            vartype,
            res_type,
            res_typmod,
            CoercionContext::COERCION_ASSIGNMENT,
            CoercionForm::COERCE_IMPLICIT_CAST,
            -1,
        )?;
        let mut cast: Expr<'mcx> = match cast {
            Some(c) => c.clone_in(mcx)?,
            None => return Ok(false),
        };
        backend_parser_parse_collate::assign_expr_collations_in(mcx, &mut cast)?;
        // Did the coercion actually do anything? (cast_result != (Node *) var)
        if !exprs_identical(&cast, &var_expr) {
            *upper_tlist_nontrivial = true;
        }
        new_tle_expr = cast;
    }

    let resno = (upper_tlist.len() + 1) as i16;
    let new_tle = backend_nodes_core::makefuncs::make_target_entry(
        mcx,
        new_tle_expr,
        resno,
        resname.as_deref(),
        false,
    )?;
    upper_tlist.push(new_tle);
    Ok(true)
}

/// `upper_tlist = lappend(upper_tlist, makeTargetEntry(makeConst(INT4OID, ...
/// isnull), list_length+1, NULL, false))` — a dummy NULL column for a dropped
/// attribute (functions.c:2400/2429). The type of the NULL isn't important.
fn push_null_column<'mcx>(
    mcx: Mcx<'mcx>,
    upper_tlist: &mut Vec<TargetEntry<'mcx>>,
) -> PgResult<()> {
    // makeConst(INT4OID, -1, InvalidOid, sizeof(int32), (Datum) 0, true, true).
    // The NULL value's content is irrelevant (constisnull = true), so build the
    // Const directly with a zero by-value Datum (no detoast/alloc can occur for
    // a fixed-length by-value type).
    let null_const = types_nodes::primnodes::Const {
        consttype: INT4OID,
        consttypmod: -1,
        constcollid: InvalidOid,
        constlen: 4,
        constvalue: types_tuple::backend_access_common_heaptuple::Datum::null(),
        constisnull: true,
        constbyval: true,
        location: -1,
    };
    let resno = (upper_tlist.len() + 1) as i16;
    let tle = backend_nodes_core::makefuncs::make_target_entry(
        mcx,
        Expr::Const(null_const),
        resno,
        None,
        false,
    )?;
    upper_tlist.push(tle);
    Ok(())
}

/// The result tlist of a query: `targetList` for SELECT, `returningList` for DML.
fn current_tlist<'a, 'mcx>(q: &'a Query<'mcx>, is_returning: bool) -> &'a [TargetEntry<'mcx>] {
    if is_returning {
        &q.returningList
    } else {
        &q.targetList
    }
}

fn current_tlist_mut<'a, 'mcx>(
    q: &'a mut Query<'mcx>,
    is_returning: bool,
) -> &'a mut [TargetEntry<'mcx>] {
    if is_returning {
        &mut q.returningList
    } else {
        &mut q.targetList
    }
}

/// `ExecCleanTargetListLength(tlist)` — number of non-junk entries.
fn exec_clean_target_list_length(tlist: &[TargetEntry<'_>]) -> i32 {
    tlist.iter().filter(|tle| !tle.resjunk).count() as i32
}

fn retval_mismatch(rettype: Oid, detail: &str) -> PgError {
    PgError::error(format!(
        "return type mismatch in function declared to return type {rettype}: {detail}"
    ))
}

fn retval_mismatch_actual(rettype: Oid, actual: Oid) -> PgError {
    PgError::error(format!(
        "return type mismatch in function declared to return type {rettype}: actual return type is {actual}"
    ))
}

fn retval_mismatch_col(rettype: Oid, actual: Oid, want: Oid, col: i32) -> PgError {
    PgError::error(format!(
        "return type mismatch in function declared to return type {rettype}: final statement returns {actual} instead of {want} at column {col}"
    ))
}

/// Whether two `Expr`s are the same node (C's pointer identity `cast_result !=
/// (Node *) var`): a coercion that returned its input unchanged is detected by
/// the result still being a bare `Var` with the same fields.
fn exprs_identical(a: &Expr, b: &Expr) -> bool {
    match (a, b) {
        (Expr::Var(va), Expr::Var(vb)) => {
            va.varno == vb.varno
                && va.varattno == vb.varattno
                && va.vartype == vb.vartype
                && va.vartypmod == vb.vartypmod
                && va.varlevelsup == vb.varlevelsup
        }
        _ => false,
    }
}

/* ===========================================================================
 * substitute_actual_srf_parameters (clauses.c:5353)
 * =========================================================================== */

/// `substitute_actual_srf_parameters(expr, nargs, args)` (clauses.c:5358):
/// replace `PARAM_EXTERN` Param nodes by the actual argument expressions,
/// adjusting Var sublevels for the descent into the subquery. The mutator starts
/// at `sublevels_up = 1` (the body becomes a subquery one level below the call).
fn substitute_actual_srf_parameters<'mcx>(
    mcx: Mcx<'mcx>,
    query: &mut Query<'mcx>,
    nargs: i32,
    args: &[Expr],
) -> PgResult<()> {
    let mut sublevels_up: i32 = 1;
    let mut err: Option<PgError> = None;
    // query_tree_mutator over the body Query; the mutator handles Query and
    // Param nodes (clauses.c:5371). flags=0.
    backend_nodes_core::node_walker::query_tree_mutator(
        query,
        &mut |node| srf_param_mutator(node, nargs, args, &mut sublevels_up, &mut err, mcx),
        0,
        mcx,
    );
    match err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// `substitute_actual_srf_parameters_mutator(node, context)` (clauses.c:5376),
/// in the in-place owned-node walker model. Returns `true` to abort the walk
/// (only on error, since substitution never short-circuits otherwise).
fn srf_param_mutator<'mcx>(
    node: &mut Node<'mcx>,
    nargs: i32,
    args: &[Expr],
    sublevels_up: &mut i32,
    err: &mut Option<PgError>,
    mcx: Mcx<'mcx>,
) -> bool {
    if err.is_some() {
        return true;
    }
    // A nested Query: bump sublevels_up, recurse via query_tree_mutator, restore.
    if node.node_tag() == ntag::T_Query {
        *sublevels_up += 1;
        let q = node.expect_query_mut();
        backend_nodes_core::node_walker::query_tree_mutator(
            q,
            &mut |n| srf_param_mutator(n, nargs, args, sublevels_up, err, mcx),
            0,
            mcx,
        );
        *sublevels_up -= 1;
        return err.is_some();
    }
    // A PARAM_EXTERN Param: replace with copyObject(args[paramid-1]) and bump
    // Var sublevels by sublevels_up.
    if let Some(param) = node.as_expr().and_then(|e| e.as_param()) {
        if param.paramkind == ParamKind::PARAM_EXTERN {
            let paramid = param.paramid;
            if paramid <= 0 || paramid > nargs {
                *err = Some(PgError::error(format!("invalid paramid: {paramid}")));
                return true;
            }
            let arg = match args.get((paramid - 1) as usize) {
                Some(a) => a,
                None => {
                    *err = Some(PgError::error(format!("invalid paramid: {paramid}")));
                    return true;
                }
            };
            let cloned = match arg.clone_in(mcx) {
                Ok(c) => c,
                Err(e) => {
                    *err = Some(e);
                    return true;
                }
            };
            let mut new_node = match Node::mk_expr(mcx, cloned) {
                Ok(n) => n,
                Err(e) => {
                    *err = Some(e);
                    return true;
                }
            };
            // IncrementVarSublevelsUp(result, sublevels_up, 0).
            if let Err(e) =
                backend_rewrite_core::increment::IncrementVarSublevelsUp(&mut new_node, *sublevels_up, 0, mcx)
            {
                *err = Some(e);
                return true;
            }
            *node = new_node;
            return false;
        }
    }
    // Otherwise recurse into the node's children.
    backend_nodes_core::node_walker::expression_tree_walker_mut(
        node,
        &mut |child| srf_param_mutator(child, nargs, args, sublevels_up, err, mcx),
        mcx,
    )
}

/// `IsPolymorphicType(typid)` (catalog/pg_type.h) — the union of polymorphic
/// type families 1 and 2.
fn is_polymorphic_type(typid: Oid) -> bool {
    use types_tuple::heaptuple::{
        ANYARRAYOID, ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLEMULTIRANGEOID, ANYCOMPATIBLENONARRAYOID,
        ANYCOMPATIBLEOID, ANYCOMPATIBLERANGEOID, ANYELEMENTOID, ANYENUMOID, ANYMULTIRANGEOID,
        ANYNONARRAYOID, ANYRANGEOID,
    };
    typid == ANYELEMENTOID
        || typid == ANYARRAYOID
        || typid == ANYNONARRAYOID
        || typid == ANYENUMOID
        || typid == ANYRANGEOID
        || typid == ANYMULTIRANGEOID
        || typid == ANYCOMPATIBLEOID
        || typid == ANYCOMPATIBLEARRAYOID
        || typid == ANYCOMPATIBLENONARRAYOID
        || typid == ANYCOMPATIBLERANGEOID
        || typid == ANYCOMPATIBLEMULTIRANGEOID
}

/// Move a `Vec<TargetEntry>` into an mcx `PgVec`.
fn vec_to_pgvec<'mcx>(
    mcx: Mcx<'mcx>,
    v: Vec<TargetEntry<'mcx>>,
) -> PgResult<mcx::PgVec<'mcx, TargetEntry<'mcx>>> {
    let mut out = mcx::vec_with_capacity_in(mcx, v.len())?;
    for e in v {
        out.push(e);
    }
    Ok(out)
}
