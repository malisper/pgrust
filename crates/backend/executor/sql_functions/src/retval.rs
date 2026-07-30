// check_sql_fn_retval / check_sql_stmt_retval / coerce_fn_result_column
// (executor/functions.c 2115-2610).
use coerce::CoercionContext;
use elog::ereport;
use mcx::{Mcx, PgVec};
use nodes_core::node_funcs::{expr_collation, expr_type, expr_typmod};
use types_core::catalog::{INT4OID, RECORDOID, VOIDOID};
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult, ERRCODE_INVALID_FUNCTION_DEFINITION, ERROR};
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{Query, RTEKind, RangeTblEntry};
use types_nodes::primnodes::{
    Alias, CoercionForm, Const, FromExpr, RangeTblRef, TargetEntry, Var,
};
use types_nodes::{Node, NodeList};
use types_tuple::TupleDescData;

use lsyscache::typ::{
    TYPTYPE_BASE, TYPTYPE_COMPOSITE, TYPTYPE_DOMAIN, TYPTYPE_ENUM, TYPTYPE_MULTIRANGE,
    TYPTYPE_RANGE,
};

const PROKIND_PROCEDURE: i8 = b'p' as i8;

#[cold]
pub(crate) fn retval_mismatch_final_stmt(rettype: Oid) -> Box<PgError> {
    retval_mismatch(
        rettype,
        "Function's final statement must be SELECT or INSERT/UPDATE/DELETE/MERGE RETURNING."
            .into(),
    )
}

#[track_caller]
#[cold]
fn retval_mismatch(rettype: Oid, detail: String) -> Box<PgError> {
    let tn = format_type::format_type_be(rettype).unwrap_or_else(|_| "???".into());
    ereport(ERROR)
        .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
        .errmsg(format!("return type mismatch in function declared to return {tn}"))
        .errdetail(detail)
        .into_error()
        .into()
}

fn null_const_tle<'mcx>(mcx: Mcx<'mcx>, resno: usize) -> PgResult<Node<'mcx>> {
    let ne = Node::mk(
        mcx,
        Const {
            consttype: INT4OID,
            consttypmod: -1,
            constcollid: InvalidOid,
            constlen: 4,
            constvalue: datum::Datum::null(),
            constisnull: true,
            constbyval: true,
            location: -1,
        },
    )?;
    Node::mk(
        mcx,
        TargetEntry {
            expr: ne,
            resno: resno as i16,
            resname: None,
            ressortgroupref: 0,
            resorigtbl: InvalidOid,
            resorigcol: 0,
            resjunk: false,
        },
    )
}

fn var_from_tle<'mcx>(mcx: Mcx<'mcx>, tle: &TargetEntry<'mcx>) -> PgResult<Node<'mcx>> {
    Node::mk(
        mcx,
        Var {
            varno: 1,
            varattno: tle.resno,
            vartype: expr_type(tle.expr),
            vartypmod: expr_typmod(tle.expr),
            varcollid: expr_collation(tle.expr),
            ..Var::default()
        },
    )
}

// coerce_fn_result_column (functions.c:2519).
fn coerce_fn_result_column<'mcx>(
    mcx: Mcx<'mcx>,
    src_tle_node: Node<'mcx>,
    res_type: Oid,
    res_typmod: i32,
    tlist_is_modifiable: bool,
    upper_tlist: &mut NodeList<'mcx>,
    upper_tlist_nontrivial: &mut bool,
) -> PgResult<bool> {
    let src_tle = src_tle_node.as_target_entry().expect("tlist holds TargetEntries");
    let pstate = parser_small1::make_parsestate(mcx, None);
    let new_tle_expr: Node<'mcx>;
    if tlist_is_modifiable && src_tle.ressortgroupref == 0 {
        let cast = coerce::coerce_to_target_type(
            mcx,
            &pstate,
            src_tle.expr,
            expr_type(src_tle.expr),
            res_type,
            res_typmod,
            CoercionContext::COERCION_ASSIGNMENT,
            CoercionForm::COERCE_IMPLICIT_CAST,
            -1,
        )?;
        let Some(cast) = cast else { return Ok(false) };
        parse_collate::assign_expr_collations(mcx, &pstate, cast)?;
        // SAFETY: sole mutation of this parser-owned tree; no derived refs live.
        unsafe {
            src_tle_node.with_mut::<TargetEntry, _>(|t| t.expr = cast);
        }
        let tle = src_tle_node.as_target_entry().expect("tag unchanged");
        new_tle_expr = var_from_tle(mcx, tle)?;
    } else {
        let var = var_from_tle(mcx, src_tle)?;
        let cast = coerce::coerce_to_target_type(
            mcx,
            &pstate,
            var,
            expr_type(var),
            res_type,
            res_typmod,
            CoercionContext::COERCION_ASSIGNMENT,
            CoercionForm::COERCE_IMPLICIT_CAST,
            -1,
        )?;
        let Some(cast) = cast else { return Ok(false) };
        parse_collate::assign_expr_collations(mcx, &pstate, cast)?;
        if !cast.ptr_eq(var) {
            *upper_tlist_nontrivial = true;
        }
        new_tle_expr = cast;
    }
    let new_tle = Node::mk(
        mcx,
        TargetEntry {
            expr: new_tle_expr,
            resno: (upper_tlist.len() + 1) as i16,
            resname: src_tle.resname,
            ressortgroupref: 0,
            resorigtbl: InvalidOid,
            resorigcol: 0,
            resjunk: false,
        },
    )?;
    upper_tlist.lappend(mcx, new_tle)?;
    Ok(true)
}

// check_sql_stmt_retval (functions.c:2149). Returns true iff whole-tuple
// result; may coerce tlist entries in place or replace the final Query with
// an injected projection.
pub fn check_sql_stmt_retval<'mcx>(
    mcx: Mcx<'mcx>,
    query_list: &mut PgVec<'mcx, Query<'mcx>>,
    rettype: Oid,
    rettupdesc: Option<&TupleDescData<'_>>,
    prokind: i8,
    insert_dropped_cols: bool,
) -> PgResult<bool> {
    check_sql_stmt_retval_ext(mcx, query_list, rettype, rettupdesc, prokind, insert_dropped_cols)
        .map(|(t, _)| t)
}

fn check_sql_stmt_retval_ext<'mcx>(
    mcx: Mcx<'mcx>,
    query_list: &mut PgVec<'mcx, Query<'mcx>>,
    rettype: Oid,
    rettupdesc: Option<&TupleDescData<'_>>,
    prokind: i8,
    insert_dropped_cols: bool,
) -> PgResult<(bool, bool)> {
    if rettype == VOIDOID {
        return Ok((false, false));
    }
    let Some(parse_idx) = query_list.iter().rposition(|q| q.canSetTag) else {
        return Err(retval_mismatch_final_stmt(rettype));
    };
    let mut is_tuple_result = false;
    let mut upper_tlist: NodeList<'mcx> = NodeList::default();
    let mut upper_tlist_nontrivial = false;
    {
        let parse = &query_list[parse_idx];
        let (tlist, tlist_is_modifiable): (&NodeList<'mcx>, bool) = match parse.commandType {
            CmdType::CMD_SELECT => (&parse.targetList, parse.setOperations.is_none()),
            CmdType::CMD_INSERT | CmdType::CMD_UPDATE | CmdType::CMD_DELETE | CmdType::CMD_MERGE
                if !parse.returningList.is_nil() =>
            {
                (&parse.returningList, true)
            }
            _ => return Err(retval_mismatch_final_stmt(rettype)),
        };

        let tlistlen = tlist
            .iter()
            .filter(|n| n.as_target_entry().is_some_and(|t| !t.resjunk))
            .count();
        let fn_typtype = lsyscache::typ::get_typtype(rettype)?;

        if matches!(
            fn_typtype,
            TYPTYPE_BASE | TYPTYPE_DOMAIN | TYPTYPE_ENUM | TYPTYPE_RANGE | TYPTYPE_MULTIRANGE
        ) {
            if tlistlen != 1 {
                return Err(retval_mismatch(
                    rettype,
                    "Final statement must return exactly one column.".into(),
                ));
            }
            let tle_node = tlist.nth(0);
            let tle = tle_node.as_target_entry().expect("tlist holds TargetEntries");
            assert!(!tle.resjunk, "non-junk TLEs must come first");
            if !coerce_fn_result_column(
                mcx,
                tle_node,
                rettype,
                -1,
                tlist_is_modifiable,
                &mut upper_tlist,
                &mut upper_tlist_nontrivial,
            )? {
                let actual = format_type::format_type_be(expr_type(tle.expr))?;
                return Err(retval_mismatch(rettype, format!("Actual return type is {actual}.")));
            }
        } else if fn_typtype == TYPTYPE_COMPOSITE || rettype == RECORDOID {
            let mut coerced_single = false;
            if tlistlen == 1 && prokind != PROKIND_PROCEDURE {
                let tle_node = tlist.nth(0);
                coerced_single = coerce_fn_result_column(
                    mcx,
                    tle_node,
                    rettype,
                    -1,
                    tlist_is_modifiable,
                    &mut upper_tlist,
                    &mut upper_tlist_nontrivial,
                )?;
            }
            if !coerced_single {
                let Some(rettupdesc) = rettupdesc else {
                    return Ok((true, false));
                };
                let tupnatts = rettupdesc.natts as usize;
                let mut tuplogcols = 0usize;
                let mut colindex = 0usize;
                for tle_node in tlist.iter() {
                    let tle = tle_node.as_target_entry().expect("tlist holds TargetEntries");
                    if tle.resjunk {
                        continue;
                    }
                    let attr = loop {
                        colindex += 1;
                        if colindex > tupnatts {
                            return Err(retval_mismatch(
                                rettype,
                                "Final statement returns too many columns.".into(),
                            ));
                        }
                        let attr = &rettupdesc.attrs[colindex - 1];
                        if attr.attisdropped {
                            if insert_dropped_cols {
                                let ntle = null_const_tle(mcx, upper_tlist.len() + 1)?;
                                upper_tlist.lappend(mcx, ntle)?;
                                upper_tlist_nontrivial = true;
                            }
                            continue;
                        }
                        break attr;
                    };
                    tuplogcols += 1;
                    if !coerce_fn_result_column(
                        mcx,
                        tle_node,
                        attr.atttypid,
                        attr.atttypmod,
                        tlist_is_modifiable,
                        &mut upper_tlist,
                        &mut upper_tlist_nontrivial,
                    )? {
                        let actual = format_type::format_type_be(expr_type(tle.expr))?;
                        let want = format_type::format_type_be(attr.atttypid)?;
                        return Err(retval_mismatch(
                            rettype,
                            format!(
                                "Final statement returns {actual} instead of {want} at column \
                                 {tuplogcols}."
                            ),
                        ));
                    }
                }
                colindex += 1;
                while colindex <= tupnatts {
                    if !rettupdesc.attrs[colindex - 1].attisdropped {
                        return Err(retval_mismatch(
                            rettype,
                            "Final statement returns too few columns.".into(),
                        ));
                    }
                    if insert_dropped_cols {
                        let ntle = null_const_tle(mcx, upper_tlist.len() + 1)?;
                        upper_tlist.lappend(mcx, ntle)?;
                        upper_tlist_nontrivial = true;
                    }
                    colindex += 1;
                }
                is_tuple_result = true;
            }
        } else {
            let tn = format_type::format_type_be(rettype)?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .errmsg(format!("return type {tn} is not supported for SQL functions"))
                .into_error()
                .into());
        }
    }

    if upper_tlist_nontrivial {
        inject_projection(mcx, query_list, parse_idx, upper_tlist)?;
    }
    Ok((is_tuple_result, upper_tlist_nontrivial))
}

// The projection-Query injection leg of check_sql_stmt_retval
// (functions.c:2454-2504).
fn inject_projection<'mcx>(
    mcx: Mcx<'mcx>,
    query_list: &mut PgVec<'mcx, Query<'mcx>>,
    parse_idx: usize,
    upper_tlist: NodeList<'mcx>,
) -> PgResult<()> {
    assert_eq!(
        query_list[parse_idx].commandType,
        CmdType::CMD_SELECT,
        "injection only for SELECT"
    );
    let mut colnames: NodeList<'mcx> = NodeList::default();
    for tle_node in query_list[parse_idx].targetList.iter() {
        let tle = tle_node.as_target_entry().expect("tlist holds TargetEntries");
        if tle.resjunk {
            continue;
        }
        colnames.lappend(mcx, Node::mk_string(mcx, tle.resname.unwrap_or(""))?)?;
    }
    let sub: &'mcx Query<'mcx> =
        mcx::leak_in(mcx::alloc_in(mcx, crate::clone_query(&query_list[parse_idx]))?);
    let name_bytes = mcx::slice_borrow_in(mcx, "*SELECT*".as_bytes())?;
    // SAFETY: byte-for-byte copy of a &str.
    let name: &'mcx str = unsafe { core::str::from_utf8_unchecked(name_bytes) };
    let alias: &'mcx Alias<'mcx> =
        mcx::leak_in(mcx::alloc_in(mcx, Alias { aliasname: Some(name), colnames })?);
    let rte = Node::mk(
        mcx,
        RangeTblEntry {
            rtekind: RTEKind::RTE_SUBQUERY,
            subquery: Some(sub),
            eref: Some(alias),
            alias: Some(alias),
            inh: false,
            inFromCl: true,
            ..RangeTblEntry::default()
        },
    )?;
    let rtr = Node::mk(mcx, RangeTblRef { rtindex: 1 })?;
    let jointree: &'mcx FromExpr<'mcx> = mcx::leak_in(mcx::alloc_in(
        mcx,
        FromExpr { fromlist: NodeList::make1(mcx, rtr)?, quals: None },
    )?);
    let newquery = Query {
        commandType: CmdType::CMD_SELECT,
        querySource: query_list[parse_idx].querySource,
        canSetTag: true,
        targetList: upper_tlist,
        rtable: NodeList::make1(mcx, rte)?,
        jointree: Some(jointree),
        hasRowSecurity: query_list[parse_idx].hasRowSecurity,
        ..Query::default()
    };
    query_list[parse_idx] = newquery;
    Ok(())
}

// Inline-path leg: C inline_function passes the call's result tupdesc and
// declines on tuple results or an injected projection (clauses.c:4743-4753).
pub(crate) fn check_query_retval_inline<'mcx>(
    mcx: Mcx<'mcx>,
    q: Query<'mcx>,
    rettype: Oid,
) -> PgResult<Option<Query<'mcx>>> {
    let mut list: PgVec<'mcx, Query<'mcx>> = mcx::vec_with_capacity_in(mcx, 1)?;
    list.push(q);
    let rettupdesc = inline_rettupdesc(mcx, rettype)?;
    let (is_tuple, injected) =
        check_sql_stmt_retval_ext(mcx, &mut list, rettype, rettupdesc.as_ref(), b'f' as i8, false)?;
    if is_tuple || injected {
        return Ok(None);
    }
    Ok(Some(list.pop().expect("one query in, one out")))
}

fn inline_rettupdesc<'mcx>(
    mcx: Mcx<'mcx>,
    rettype: Oid,
) -> PgResult<Option<TupleDescData<'mcx>>> {
    if rettype == RECORDOID {
        return Ok(None);
    }
    if lsyscache::typ::get_typtype(rettype)? != TYPTYPE_COMPOSITE {
        return Ok(None);
    }
    Ok(Some(typcache_seams::lookup_rowtype_tupdesc_copy::call(mcx, rettype, -1)?))
}
