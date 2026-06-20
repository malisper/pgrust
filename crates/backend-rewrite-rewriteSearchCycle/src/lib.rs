#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// Every fallible function returns the shared `types_error::PgResult`; `PgError`'s
// size is fixed by `types-error`, so we accept the large-`Err` lint crate-wide,
// like every sibling rewrite/parser crate.
#![allow(clippy::result_large_err)]

//! Port of `src/backend/rewrite/rewriteSearchCycle.c` (PostgreSQL 18.3) — the
//! expansion of the `SEARCH` and `CYCLE` clauses of a recursive CTE.
//!
//! [`rewriteSearchAndCycle`] is the single public entry point (the C file's only
//! exported function). It is invoked from `fireRIRrules`
//! (rewriteHandler.c:2005), which calls it for every CTE that carries a
//! `search_clause` or `cycle_clause`. The three private helpers
//! ([`make_path_rowexpr`], [`make_path_initial_array`], [`make_path_cat_expr`])
//! mirror the C statics 1:1.
//!
//! # The owned node model
//!
//! C mutates the `CommonTableExpr` in place after a `copyObject`. Here the caller
//! hands us an owned `CommonTableExpr<'mcx>` by value (already a fresh copy), we
//! mutate it and return it, which is the same effect.
//!
//! The CTE's analyzed `ctequery` is a `Node::Query`; its top-level
//! `setOperations` is a `Node::SetOperationStmt` whose `larg`/`rarg` are
//! `Node::RangeTblRef`. We pull the `Query` out of the `Node` box, work on the
//! owned `Query`/`SetOperationStmt`, and box it back. The added expression nodes
//! (`RowExpr`/`ArrayExpr`/`FuncExpr`/`FieldSelect`/`ScalarArrayOpExpr`/
//! `CaseExpr`) are built over the trimmed `Expr` model (primnodes.rs); the
//! `cycle_mark_value`/`cycle_mark_default` carried as `Node::Expr` are unwrapped
//! to `Expr` where the C uses them directly as `Expr *`.

extern crate alloc;

use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;

use mcx::{alloc_in, Mcx, PgBox, PgString, PgVec};

use types_core::primitive::{AttrNumber, InvalidAttrNumber, Oid};
use types_core::InvalidOid;
use types_datum::Datum as ScalarWord;
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::{BOOLOID, INT8OID, RECORDARRAYOID, RECORDOID};

use types_nodes::copy_query::Query;
use types_nodes::nodes::{CmdType, Node, NodePtr};
use types_nodes::parsenodes::{RTEKind, RangeTblEntry};
use types_nodes::primnodes::{
    ArrayExpr, CaseExpr, CaseWhen, CoercionForm, Expr, FieldSelect, RowExpr, ScalarArrayOpExpr,
    TargetEntry,
};
use types_nodes::rawnodes::{Alias, FromExpr, RangeTblRef, SetOperation, SetOperationStmt};
use types_nodes::value::StringNode;

use backend_nodes_core::makefuncs::{make_const, make_func_expr, make_opclause, make_var};
use backend_parser_analyze::makeSortGroupClauseForSetOp;
use backend_rewrite_core::increment::IncrementVarSublevelsUp;
use backend_utils_error::ereport;

/// `F_ARRAY_CAT` (fmgroids.h) — the `array_cat(anyarray, anyarray)` function OID.
const F_ARRAY_CAT: Oid = 383;
/// `F_INT8INC` (fmgroids.h) — the `int8inc(int8)` function OID.
const F_INT8INC: Oid = 1219;
/// `RECORD_EQ_OP` (pg_operator_d.h) — the `record = record` operator OID.
const RECORD_EQ_OP: Oid = 2988;
/// `FLOAT8PASSBYVAL` (c.h) — true on 64-bit; controls the int8 const's byval.
const FLOAT8PASSBYVAL: bool = true;

// ===========================================================================
// Small owned-model helpers mirroring the C list/cast idioms.
// ===========================================================================

/// `strVal(node)` — a `String` value node's contents.
fn str_val<'a>(node: &'a Node<'_>) -> &'a str {
    node.as_string().map(|s| s.sval.as_str()).unwrap_or("")
}

/// `makeString(pstrdup(s))` — a `Node::String` for a colname list.
fn make_string_node<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<NodePtr<'mcx>> {
    alloc_in(
        mcx,
        Node::mk_string(mcx, StringNode {
            sval: PgString::from_str_in(s, mcx)?,
        })?,
    )
}

fn elog_error(msg: impl Into<String>) -> PgError {
    ereport(ERROR).errmsg_internal(msg.into()).into_error()
}

/// `makeTargetEntry((Expr *) expr, resno, resname, false)` over the owned model.
/// `resname` is cloned into `mcx`. C `makeTargetEntry` zeroes the
/// `ressortgroupref`/`resorigtbl`/`resorigcol` fields.
fn make_tle<'mcx>(
    mcx: Mcx<'mcx>,
    expr: Expr,
    resno: AttrNumber,
    resname: &str,
) -> PgResult<TargetEntry<'mcx>> {
    Ok(TargetEntry {
        expr: Some(alloc_in(mcx, expr)?),
        resno,
        resname: Some(PgString::from_str_in(resname, mcx)?),
        ressortgroupref: 0,
        resorigtbl: InvalidOid,
        resorigcol: 0,
        resjunk: false,
    })
}

// ===========================================================================
// rewriteSearchCycle.c statics.
// ===========================================================================

/// `make_path_rowexpr(cte, col_list)` (rewriteSearchCycle.c:116) — a `RowExpr`
/// over the named columns, which must be among the CTE's output columns.
fn make_path_rowexpr(cte: &types_nodes::rawnodes::CommonTableExpr<'_>, col_list: &PgVec<'_, NodePtr<'_>>) -> RowExpr {
    let mut args: Vec<Expr> = Vec::new();
    let mut colnames: Vec<String> = Vec::new();

    for lc in col_list.iter() {
        let colname = str_val(lc);

        for i in 0..cte.ctecolnames.len() {
            let colname2 = str_val(&cte.ctecolnames[i]);

            if colname == colname2 {
                let var = make_var(
                    1,
                    (i + 1) as AttrNumber,
                    cte.ctecoltypes[i],
                    cte.ctecoltypmods[i],
                    cte.ctecolcollations[i],
                    0,
                );
                args.push(Expr::Var(var));
                colnames.push(String::from(colname));
                break;
            }
        }
    }

    RowExpr {
        args,
        row_typeid: RECORDOID,
        row_format: CoercionForm::COERCE_IMPLICIT_CAST,
        colnames,
        location: -1,
    }
}

/// `make_path_initial_array(rowexpr)` (rewriteSearchCycle.c:158) — wrap a
/// `RowExpr` in `ARRAY[ ... ]` for the initial search/cycle row.
fn make_path_initial_array(rowexpr: RowExpr) -> Expr {
    Expr::ArrayExpr(ArrayExpr {
        array_typeid: RECORDARRAYOID,
        array_collid: InvalidOid,
        element_typeid: RECORDOID,
        elements: vec![Expr::RowExpr(rowexpr)],
        multidims: false,
        location: -1,
    })
}

/// `make_path_cat_expr(rowexpr, path_varattno)` (rewriteSearchCycle.c:179) — an
/// array-catenation `cpa || ARRAY[ROW(cols)]` (the underlying `array_cat` call).
fn make_path_cat_expr(rowexpr: RowExpr, path_varattno: AttrNumber) -> Expr {
    let arr = Expr::ArrayExpr(ArrayExpr {
        array_typeid: RECORDARRAYOID,
        array_collid: InvalidOid,
        element_typeid: RECORDOID,
        elements: vec![Expr::RowExpr(rowexpr)],
        multidims: false,
        location: -1,
    });

    make_func_expr(
        F_ARRAY_CAT,
        RECORDARRAYOID,
        vec![
            Expr::Var(make_var(1, path_varattno, RECORDARRAYOID, -1, InvalidOid, 0)),
            arr,
        ],
        InvalidOid,
        InvalidOid,
        CoercionForm::COERCE_EXPLICIT_CALL,
    )
}

// ===========================================================================
// rewriteSearchAndCycle.
// ===========================================================================

/// `rewriteSearchAndCycle(cte)` (rewriteSearchCycle.c:202) — expand a recursive
/// CTE's `SEARCH` and/or `CYCLE` clause into the extra computed columns
/// (ordering column for SEARCH, cycle-mark + path columns for CYCLE). The
/// passed-in `CommonTableExpr` is consumed (the C `copyObject` is the caller's
/// clone) and the rewritten one returned.
pub fn rewriteSearchAndCycle<'mcx>(
    mcx: Mcx<'mcx>,
    mut cte: types_nodes::rawnodes::CommonTableExpr<'mcx>,
) -> PgResult<types_nodes::rawnodes::CommonTableExpr<'mcx>> {
    debug_assert!(cte.search_clause.is_some() || cte.cycle_clause.is_some());

    // ctequery = castNode(Query, cte->ctequery)
    let ctequery_box = cte
        .ctequery
        .take()
        .ok_or_else(|| elog_error("rewriteSearchAndCycle: CTE has no ctequery"))?;
    let mut ctequery: Query<'mcx> = PgBox::into_inner(ctequery_box)
        .into_query()
        .ok_or_else(|| elog_error("rewriteSearchAndCycle: ctequery is not a Query"))?;

    // The top level of the CTE's query should be a UNION.  Find the two
    // subqueries.
    let sos_node = ctequery
        .setOperations
        .take()
        .ok_or_else(|| elog_error("rewriteSearchAndCycle: ctequery has no setOperations"))?;
    let mut sos: SetOperationStmt<'mcx> = PgBox::into_inner(sos_node)
        .into_setoperationstmt()
        .ok_or_else(|| elog_error("rewriteSearchAndCycle: setOperations is not a SetOperationStmt"))?;
    debug_assert!(sos.op == SetOperation::SETOP_UNION);

    let rti1 = sos
        .larg
        .as_deref()
        .and_then(|n| n.as_rangetblref())
        .ok_or_else(|| elog_error("rewriteSearchAndCycle: sos->larg is not a RangeTblRef"))?
        .rtindex;
    let rti2 = sos
        .rarg
        .as_deref()
        .and_then(|n| n.as_rangetblref())
        .ok_or_else(|| elog_error("rewriteSearchAndCycle: sos->rarg is not a RangeTblRef"))?
        .rtindex;

    debug_assert!(
        ctequery.rtable[(rti1 - 1) as usize].rtekind == RTEKind::RTE_SUBQUERY
    );
    debug_assert!(
        ctequery.rtable[(rti2 - 1) as usize].rtekind == RTEKind::RTE_SUBQUERY
    );

    // We'll need this a few times later.
    let mut search_seq_type = InvalidOid;
    if let Some(sc) = cte.search_clause.as_deref() {
        if sc.search_breadth_first {
            search_seq_type = RECORDOID;
        } else {
            search_seq_type = RECORDARRAYOID;
        }
    }

    // Attribute numbers of the added columns in the CTE's column list.
    let mut sqc_attno: AttrNumber = InvalidAttrNumber;
    let mut cmc_attno: AttrNumber = InvalidAttrNumber;
    let mut cpa_attno: AttrNumber = InvalidAttrNumber;
    let has_search = cte.search_clause.is_some();
    let has_cycle = cte.cycle_clause.is_some();
    if has_search {
        sqc_attno = (cte.ctecolnames.len() + 1) as AttrNumber;
    }
    if has_cycle {
        cmc_attno = (cte.ctecolnames.len() + 1) as AttrNumber;
        cpa_attno = (cte.ctecolnames.len() + 2) as AttrNumber;
        if has_search {
            cmc_attno += 1;
            cpa_attno += 1;
        }
    }

    // Decode the cycle clause's stored fields once (C reads them repeatedly via
    // cte->cycle_clause->...).  The cycle_clause is carried as Node::CTECycleClause.
    let cycle = match cte.cycle_clause.as_deref() {
        Some(n) => Some(
            n.as_ctecycleclause()
                .ok_or_else(|| elog_error("rewriteSearchAndCycle: cycle_clause is not a CTECycleClause"))?,
        ),
        None => None,
    };
    // Snapshot the scalar/by-value cycle fields and clone the value/default Exprs
    // (used several times; the Node::Expr arms are unwrapped to Expr).
    let cyc = match cycle {
        Some(c) => Some(CycleInfo {
            cycle_mark_column: c.cycle_mark_column.as_ref().map(|s| String::from(s.as_str())).unwrap_or_default(),
            cycle_path_column: c.cycle_path_column.as_ref().map(|s| String::from(s.as_str())).unwrap_or_default(),
            cycle_mark_type: c.cycle_mark_type,
            cycle_mark_typmod: c.cycle_mark_typmod,
            cycle_mark_collation: c.cycle_mark_collation,
            cycle_mark_neop: c.cycle_mark_neop,
            cycle_mark_value: clone_expr_node(mcx, c.cycle_mark_value.as_deref())?,
            cycle_mark_default: clone_expr_node(mcx, c.cycle_mark_default.as_deref())?,
            cycle_col_list: {
                let mut v: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
                for n in c.cycle_col_list.iter() {
                    v.push(alloc_in(mcx, n.clone_in(mcx)?)?);
                }
                v
            },
        }),
        None => None,
    };

    // search_seq_column / search_col_list snapshots.
    let search = match cte.search_clause.as_deref() {
        Some(sc) => Some(SearchInfo {
            search_breadth_first: sc.search_breadth_first,
            search_seq_column: sc.search_seq_column.as_ref().map(|s| String::from(s.as_str())).unwrap_or_default(),
            search_col_list: {
                let mut v: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
                for n in sc.search_col_list.iter() {
                    v.push(alloc_in(mcx, n.clone_in(mcx)?)?);
                }
                v
            },
        }),
        None => None,
    };

    // ----------------------------------------------------------------------
    // Make new left subquery (rewriteSearchCycle.c:279).
    // ----------------------------------------------------------------------
    let mut newq1 = Query::new(mcx);
    newq1.commandType = CmdType::CMD_SELECT;
    newq1.canSetTag = true;

    let mut newrte = RangeTblEntry::new_in(mcx);
    newrte.rtekind = RTEKind::RTE_SUBQUERY;
    // newrte->alias = makeAlias("*TLOCRN*", cte->ctecolnames)
    let alias1 = make_alias_node(mcx, "*TLOCRN*", &cte.ctecolnames)?;
    // newsubquery = copyObject(rte1->subquery); IncrementVarSublevelsUp(.., 1, 1)
    let mut newsubquery = clone_rte_subquery(mcx, &ctequery.rtable[(rti1 - 1) as usize])?;
    increment_query_sublevels(mcx, &mut newsubquery)?;
    newrte.subquery = Some(alloc_in(mcx, newsubquery)?);
    newrte.inFromCl = true;
    // eref = alias (separate copy)
    newrte.eref = Some(alloc_in(mcx, alias1.clone_in(mcx)?)?);
    newrte.alias = Some(alloc_in(mcx, alias1)?);
    newq1.rtable.push(newrte);

    // rtr->rtindex = 1; jointree = makeFromExpr(list_make1(rtr), NULL)
    newq1.jointree = Some(alloc_in(mcx, from_expr_with_rtr(mcx, 1, None)?)?);

    // Make target list: the original WITH columns.
    {
        let rte1_subq_tlist_meta = rte_subquery_tlist_resorig(&ctequery.rtable[(rti1 - 1) as usize]);
        for i in 0..cte.ctecolnames.len() {
            let var = make_var(
                1,
                (i + 1) as AttrNumber,
                cte.ctecoltypes[i],
                cte.ctecoltypmods[i],
                cte.ctecolcollations[i],
                0,
            );
            let mut tle = make_tle(mcx, Expr::Var(var), (i + 1) as AttrNumber, str_val(&cte.ctecolnames[i]))?;
            let (resorigtbl, resorigcol) = rte1_subq_tlist_meta[i];
            tle.resorigtbl = resorigtbl;
            tle.resorigcol = resorigcol;
            newq1.targetList.push(tle);
        }
    }

    // The added SEARCH column.
    let mut search_col_rowexpr: Option<RowExpr> = None;
    if let Some(s) = search.as_ref() {
        let mut rowexpr = make_path_rowexpr(&cte, &s.search_col_list);
        let texpr: Expr;
        if s.search_breadth_first {
            // lcons int8 const -1 as the *DEPTH* field.
            rowexpr.args.insert(
                0,
                Expr::Const(make_const(
                    mcx,
                    INT8OID,
                    -1,
                    InvalidOid,
                    core::mem::size_of::<i64>() as i32,
                    Datum::ByVal(ScalarWord::from_i64(0).as_usize()),
                    false,
                    FLOAT8PASSBYVAL,
                )?),
            );
            rowexpr.colnames.insert(0, String::from("*DEPTH*"));
            texpr = Expr::RowExpr(rowexpr.clone());
        } else {
            texpr = make_path_initial_array(rowexpr.clone());
        }
        search_col_rowexpr = Some(rowexpr);
        let resno = (newq1.targetList.len() + 1) as AttrNumber;
        let tle = make_tle(mcx, texpr, resno, &s.search_seq_column)?;
        newq1.targetList.push(tle);
    }

    // The added CYCLE columns.
    let mut cycle_col_rowexpr: Option<RowExpr> = None;
    if let Some(c) = cyc.as_ref() {
        // cycle_mark_default
        let resno = (newq1.targetList.len() + 1) as AttrNumber;
        let tle = make_tle(mcx, c.cycle_mark_default.clone(), resno, &c.cycle_mark_column)?;
        newq1.targetList.push(tle);

        let rowexpr = make_path_rowexpr(&cte, &c.cycle_col_list);
        let resno = (newq1.targetList.len() + 1) as AttrNumber;
        let tle = make_tle(mcx, make_path_initial_array(rowexpr.clone()), resno, &c.cycle_path_column)?;
        newq1.targetList.push(tle);
        cycle_col_rowexpr = Some(rowexpr);
    }

    // rte1->subquery = newq1; append the new column names to rte1->eref->colnames.
    {
        let rte1 = &mut ctequery.rtable[(rti1 - 1) as usize];
        rte1.subquery = Some(alloc_in(mcx, newq1)?);
        if let Some(eref) = rte1.eref.as_deref_mut() {
            if let Some(s) = search.as_ref() {
                eref.colnames.push(make_string_node(mcx, &s.search_seq_column)?);
            }
            if let Some(c) = cyc.as_ref() {
                eref.colnames.push(make_string_node(mcx, &c.cycle_mark_column)?);
                eref.colnames.push(make_string_node(mcx, &c.cycle_path_column)?);
            }
        }
    }

    // ----------------------------------------------------------------------
    // Make new right subquery (rewriteSearchCycle.c:366).
    // ----------------------------------------------------------------------
    let mut newq2 = Query::new(mcx);
    newq2.commandType = CmdType::CMD_SELECT;
    newq2.canSetTag = true;

    let mut newrte2 = RangeTblEntry::new_in(mcx);
    newrte2.rtekind = RTEKind::RTE_SUBQUERY;

    // ewcl = copyObject(cte->ctecolnames) + the new column names.
    let mut ewcl: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    for n in cte.ctecolnames.iter() {
        ewcl.push(alloc_in(mcx, n.clone_in(mcx)?)?);
    }
    if let Some(s) = search.as_ref() {
        ewcl.push(make_string_node(mcx, &s.search_seq_column)?);
    }
    if let Some(c) = cyc.as_ref() {
        ewcl.push(make_string_node(mcx, &c.cycle_mark_column)?);
        ewcl.push(make_string_node(mcx, &c.cycle_path_column)?);
    }
    let alias2 = make_alias_node(mcx, "*TROCRN*", &ewcl)?;

    // Find the reference to the recursive CTE in the right UNION subquery's
    // range table (must be exactly two levels up).
    let ctename = cte.ctename.as_ref().map(|s| s.as_str()).unwrap_or("");
    let mut cte_rtindex: i32 = -1;
    {
        let rte2_subq = ctequery.rtable[(rti2 - 1) as usize]
            .subquery
            .as_deref()
            .ok_or_else(|| elog_error("rewriteSearchAndCycle: rte2 has no subquery"))?;
        for rti in 1..=(rte2_subq.rtable.len() as i32) {
            let e = &rte2_subq.rtable[(rti - 1) as usize];
            if e.rtekind == RTEKind::RTE_CTE
                && e.ctename.as_ref().map(|s| s.as_str()).unwrap_or("") == ctename
                && e.ctelevelsup == 2
            {
                cte_rtindex = rti;
                break;
            }
        }
    }
    if cte_rtindex <= 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(alloc::format!(
                "with a SEARCH or CYCLE clause, the recursive reference to WITH query \"{}\" must be at the top level of its right-hand SELECT",
                ctename
            ))
            .into_error());
    }

    // newsubquery = copyObject(rte2->subquery); IncrementVarSublevelsUp(.., 1, 1)
    let mut newsubquery2 = clone_rte_subquery(mcx, &ctequery.rtable[(rti2 - 1) as usize])?;
    increment_query_sublevels(mcx, &mut newsubquery2)?;

    // Add extra columns to target list of subquery of right subquery.
    if let Some(s) = search.as_ref() {
        // ctename.sqc
        let var = make_var(cte_rtindex, sqc_attno, search_seq_type, -1, InvalidOid, 0);
        let resno = (newsubquery2.targetList.len() + 1) as AttrNumber;
        let tle = make_tle(mcx, Expr::Var(var), resno, &s.search_seq_column)?;
        newsubquery2.targetList.push(tle);
    }
    if let Some(c) = cyc.as_ref() {
        // ctename.cmc
        let var = make_var(
            cte_rtindex,
            cmc_attno,
            c.cycle_mark_type,
            c.cycle_mark_typmod,
            c.cycle_mark_collation,
            0,
        );
        let resno = (newsubquery2.targetList.len() + 1) as AttrNumber;
        let tle = make_tle(mcx, Expr::Var(var), resno, &c.cycle_mark_column)?;
        newsubquery2.targetList.push(tle);

        // ctename.cpa
        let var = make_var(cte_rtindex, cpa_attno, RECORDARRAYOID, -1, InvalidOid, 0);
        let resno = (newsubquery2.targetList.len() + 1) as AttrNumber;
        let tle = make_tle(mcx, Expr::Var(var), resno, &c.cycle_path_column)?;
        newsubquery2.targetList.push(tle);
    }

    newrte2.subquery = Some(alloc_in(mcx, newsubquery2)?);
    newrte2.inFromCl = true;
    newrte2.eref = Some(alloc_in(mcx, alias2.clone_in(mcx)?)?);
    newrte2.alias = Some(alloc_in(mcx, alias2)?);
    newq2.rtable.push(newrte2);

    // jointree: with the cmc <> cmv condition for a cycle clause.
    if let Some(c) = cyc.as_ref() {
        let expr = make_opclause(
            c.cycle_mark_neop,
            BOOLOID,
            false,
            Expr::Var(make_var(
                1,
                cmc_attno,
                c.cycle_mark_type,
                c.cycle_mark_typmod,
                c.cycle_mark_collation,
                0,
            )),
            Some(c.cycle_mark_value.clone()),
            InvalidOid,
            c.cycle_mark_collation,
        );
        let quals = alloc_in(mcx, Node::mk_expr(mcx, expr)?)?;
        newq2.jointree = Some(alloc_in(mcx, from_expr_with_rtr(mcx, 1, Some(quals))?)?);
    } else {
        newq2.jointree = Some(alloc_in(mcx, from_expr_with_rtr(mcx, 1, None)?)?);
    }

    // Make target list: the original WITH columns.
    {
        let rte2_subq_tlist_meta = rte_subquery_tlist_resorig(&ctequery.rtable[(rti2 - 1) as usize]);
        for i in 0..cte.ctecolnames.len() {
            let var = make_var(
                1,
                (i + 1) as AttrNumber,
                cte.ctecoltypes[i],
                cte.ctecoltypmods[i],
                cte.ctecolcollations[i],
                0,
            );
            let mut tle = make_tle(mcx, Expr::Var(var), (i + 1) as AttrNumber, str_val(&cte.ctecolnames[i]))?;
            let (resorigtbl, resorigcol) = rte2_subq_tlist_meta[i];
            tle.resorigtbl = resorigtbl;
            tle.resorigcol = resorigcol;
            newq2.targetList.push(tle);
        }
    }

    // The added SEARCH column expression.
    if let Some(s) = search.as_ref() {
        let texpr: Expr;
        if s.search_breadth_first {
            // ROW(sqc.depth + 1, cols)
            let mut rowexpr = search_col_rowexpr
                .clone()
                .ok_or_else(|| elog_error("rewriteSearchAndCycle: missing search_col_rowexpr"))?;

            let fs = FieldSelect {
                arg: Some(alloc::boxed::Box::new(Expr::Var(make_var(
                    1, sqc_attno, RECORDOID, -1, 0, 0,
                )))),
                fieldnum: 1,
                resulttype: INT8OID,
                resulttypmod: -1,
                resultcollid: InvalidOid,
            };

            let fexpr = make_func_expr(
                F_INT8INC,
                INT8OID,
                vec![Expr::FieldSelect(fs)],
                InvalidOid,
                InvalidOid,
                CoercionForm::COERCE_EXPLICIT_CALL,
            );

            // linitial(search_col_rowexpr->args) = fexpr
            if rowexpr.args.is_empty() {
                rowexpr.args.push(fexpr);
            } else {
                rowexpr.args[0] = fexpr;
            }
            texpr = Expr::RowExpr(rowexpr);
        } else {
            // sqc || ARRAY[ROW(cols)]
            let rowexpr = search_col_rowexpr
                .clone()
                .ok_or_else(|| elog_error("rewriteSearchAndCycle: missing search_col_rowexpr"))?;
            texpr = make_path_cat_expr(rowexpr, sqc_attno);
        }
        let resno = (newq2.targetList.len() + 1) as AttrNumber;
        let tle = make_tle(mcx, texpr, resno, &s.search_seq_column)?;
        newq2.targetList.push(tle);
    }

    // The added CYCLE column expressions.
    if let Some(c) = cyc.as_ref() {
        let rowexpr = cycle_col_rowexpr
            .clone()
            .ok_or_else(|| elog_error("rewriteSearchAndCycle: missing cycle_col_rowexpr"))?;

        // CASE WHEN ROW(cols) = ANY (ARRAY[cpa]) THEN cmv ELSE cmd END
        let saoe = ScalarArrayOpExpr {
            opno: RECORD_EQ_OP,
            opfuncid: InvalidOid,
            hashfuncid: InvalidOid,
            negfuncid: InvalidOid,
            useOr: true,
            inputcollid: InvalidOid,
            args: vec![
                Expr::RowExpr(rowexpr.clone()),
                Expr::Var(make_var(1, cpa_attno, RECORDARRAYOID, -1, 0, 0)),
            ],
            location: -1,
        };

        let casewhen = CaseWhen {
            expr: Some(alloc::boxed::Box::new(Expr::ScalarArrayOpExpr(saoe))),
            result: Some(alloc::boxed::Box::new(c.cycle_mark_value.clone())),
            location: -1,
        };
        let caseexpr = CaseExpr {
            casetype: c.cycle_mark_type,
            casecollid: c.cycle_mark_collation,
            arg: None,
            args: vec![casewhen],
            defresult: Some(alloc::boxed::Box::new(c.cycle_mark_default.clone())),
            location: -1,
        };

        let resno = (newq2.targetList.len() + 1) as AttrNumber;
        let tle = make_tle(mcx, Expr::CaseExpr(caseexpr), resno, &c.cycle_mark_column)?;
        newq2.targetList.push(tle);

        // cpa || ARRAY[ROW(cols)]
        let resno = (newq2.targetList.len() + 1) as AttrNumber;
        let tle = make_tle(mcx, make_path_cat_expr(rowexpr, cpa_attno), resno, &c.cycle_path_column)?;
        newq2.targetList.push(tle);
    }

    // rte2->subquery = newq2; append the new column names to rte2->eref->colnames.
    {
        let rte2 = &mut ctequery.rtable[(rti2 - 1) as usize];
        rte2.subquery = Some(alloc_in(mcx, newq2)?);
        if let Some(eref) = rte2.eref.as_deref_mut() {
            if let Some(s) = search.as_ref() {
                eref.colnames.push(make_string_node(mcx, &s.search_seq_column)?);
            }
            if let Some(c) = cyc.as_ref() {
                eref.colnames.push(make_string_node(mcx, &c.cycle_mark_column)?);
                eref.colnames.push(make_string_node(mcx, &c.cycle_path_column)?);
            }
        }
    }

    // ----------------------------------------------------------------------
    // Add the additional columns to the SetOperationStmt.
    // ----------------------------------------------------------------------
    if let Some(s) = search.as_ref() {
        sos.colTypes.push(search_seq_type);
        sos.colTypmods.push(-1);
        sos.colCollations.push(InvalidOid);
        if !sos.all {
            let sgc = makeSortGroupClauseForSetOp(search_seq_type, true)?;
            sos.groupClauses.push(alloc_in(mcx, Node::mk_sort_group_clause(mcx, sgc)?)?);
        }
        let _ = s;
    }
    if let Some(c) = cyc.as_ref() {
        sos.colTypes.push(c.cycle_mark_type);
        sos.colTypmods.push(c.cycle_mark_typmod);
        sos.colCollations.push(c.cycle_mark_collation);
        if !sos.all {
            let sgc = makeSortGroupClauseForSetOp(c.cycle_mark_type, true)?;
            sos.groupClauses.push(alloc_in(mcx, Node::mk_sort_group_clause(mcx, sgc)?)?);
        }

        sos.colTypes.push(RECORDARRAYOID);
        sos.colTypmods.push(-1);
        sos.colCollations.push(InvalidOid);
        if !sos.all {
            let sgc = makeSortGroupClauseForSetOp(RECORDARRAYOID, true)?;
            sos.groupClauses.push(alloc_in(mcx, Node::mk_sort_group_clause(mcx, sgc)?)?);
        }
    }

    // Put the (mutated) SetOperationStmt back.
    ctequery.setOperations = Some(alloc_in(mcx, Node::mk_set_operation_stmt(mcx, sos)?)?);

    // ----------------------------------------------------------------------
    // Add the additional columns to the CTE query's target list.
    // ----------------------------------------------------------------------
    if let Some(s) = search.as_ref() {
        let var = make_var(1, sqc_attno, search_seq_type, -1, InvalidOid, 0);
        let resno = (ctequery.targetList.len() + 1) as AttrNumber;
        let tle = make_tle(mcx, Expr::Var(var), resno, &s.search_seq_column)?;
        ctequery.targetList.push(tle);
    }
    if let Some(c) = cyc.as_ref() {
        let var = make_var(
            1,
            cmc_attno,
            c.cycle_mark_type,
            c.cycle_mark_typmod,
            c.cycle_mark_collation,
            0,
        );
        let resno = (ctequery.targetList.len() + 1) as AttrNumber;
        let tle = make_tle(mcx, Expr::Var(var), resno, &c.cycle_mark_column)?;
        ctequery.targetList.push(tle);

        let var = make_var(1, cpa_attno, RECORDARRAYOID, -1, InvalidOid, 0);
        let resno = (ctequery.targetList.len() + 1) as AttrNumber;
        let tle = make_tle(mcx, Expr::Var(var), resno, &c.cycle_path_column)?;
        ctequery.targetList.push(tle);
    }

    // ----------------------------------------------------------------------
    // Add the additional columns to the CTE's output columns.
    // ----------------------------------------------------------------------
    cte.ctecolnames = ewcl;
    if let Some(_s) = search.as_ref() {
        cte.ctecoltypes.push(search_seq_type);
        cte.ctecoltypmods.push(-1);
        cte.ctecolcollations.push(InvalidOid);
    }
    if let Some(c) = cyc.as_ref() {
        cte.ctecoltypes.push(c.cycle_mark_type);
        cte.ctecoltypmods.push(c.cycle_mark_typmod);
        cte.ctecolcollations.push(c.cycle_mark_collation);

        cte.ctecoltypes.push(RECORDARRAYOID);
        cte.ctecoltypmods.push(-1);
        cte.ctecolcollations.push(InvalidOid);
    }

    // Re-box the (mutated) ctequery back into the CTE.
    cte.ctequery = Some(alloc_in(mcx, Node::mk_query(mcx, ctequery)?)?);

    Ok(cte)
}

// ===========================================================================
// Snapshots of the cycle / search clause data (C reads the live clause fields
// repeatedly; we snapshot once to avoid re-borrowing `cte` while it is mutated).
// ===========================================================================

struct CycleInfo<'mcx> {
    cycle_mark_column: String,
    cycle_path_column: String,
    cycle_mark_type: Oid,
    cycle_mark_typmod: i32,
    cycle_mark_collation: Oid,
    cycle_mark_neop: Oid,
    cycle_mark_value: Expr,
    cycle_mark_default: Expr,
    cycle_col_list: PgVec<'mcx, NodePtr<'mcx>>,
}

struct SearchInfo<'mcx> {
    search_breadth_first: bool,
    search_seq_column: String,
    search_col_list: PgVec<'mcx, NodePtr<'mcx>>,
}

// ===========================================================================
// Owned-model glue helpers.
// ===========================================================================

/// Clone a `Node::Expr`-wrapped expression out to a bare `Expr` (deep copy into
/// `mcx`). Errors if the node is absent or not an `Expr`.
fn clone_expr_node<'mcx>(mcx: Mcx<'mcx>, node: Option<&Node<'_>>) -> PgResult<Expr> {
    let n = node.ok_or_else(|| elog_error("rewriteSearchAndCycle: missing cycle-mark expression"))?;
    let cloned = n.clone_in(mcx)?;
    cloned
        .into_expr()
        .ok_or_else(|| elog_error("rewriteSearchAndCycle: cycle-mark node is not an Expr"))
}

/// `makeAlias(name, colnames)` over the owned model, deep-copying the colnames
/// (String nodes) into a fresh list.
fn make_alias_node<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
    colnames: &PgVec<'_, NodePtr<'_>>,
) -> PgResult<Alias<'mcx>> {
    let mut cols: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    for n in colnames.iter() {
        cols.push(alloc_in(mcx, n.clone_in(mcx)?)?);
    }
    Ok(Alias {
        aliasname: Some(PgString::from_str_in(name, mcx)?),
        colnames: cols,
    })
}

/// `makeFromExpr(list_make1(makeNode(RangeTblRef){rtindex}), quals)`.
fn from_expr_with_rtr<'mcx>(
    mcx: Mcx<'mcx>,
    rtindex: i32,
    quals: Option<NodePtr<'mcx>>,
) -> PgResult<FromExpr<'mcx>> {
    let mut fromlist: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    fromlist.push(alloc_in(mcx, Node::mk_range_tbl_ref(mcx, RangeTblRef { rtindex })?)?);
    Ok(FromExpr { fromlist, quals })
}

/// `copyObject(rte->subquery)` — deep-copy a subquery RTE's `Query`.
fn clone_rte_subquery<'mcx>(mcx: Mcx<'mcx>, rte: &RangeTblEntry<'_>) -> PgResult<Query<'mcx>> {
    rte.subquery
        .as_deref()
        .ok_or_else(|| elog_error("rewriteSearchAndCycle: RTE_SUBQUERY has no subquery"))?
        .clone_in(mcx)
}

/// `IncrementVarSublevelsUp((Node *) query, 1, 1)` — wrap the owned `Query` as a
/// `Node`, bump, and unwrap it back.
fn increment_query_sublevels<'mcx>(mcx: Mcx<'mcx>, query: &mut Query<'mcx>) -> PgResult<()> {
    // Move the query into a Node, bump in place, move it back.
    let owned = core::mem::replace(query, Query::new(mcx));
    let mut node = Node::mk_query(mcx, owned)?;
    IncrementVarSublevelsUp(&mut node, 1, 1, mcx)?;
    *query = node
        .into_query()
        .ok_or_else(|| elog_error("rewriteSearchAndCycle: increment lost the Query"))?;
    Ok(())
}

/// Read the (resorigtbl, resorigcol) pair off each of a subquery RTE's
/// targetList entries (C: `list_nth_node(TargetEntry, rte->subquery->targetList,
/// i)->resorigtbl/resorigcol`).
fn rte_subquery_tlist_resorig(rte: &RangeTblEntry<'_>) -> Vec<(Oid, AttrNumber)> {
    match rte.subquery.as_deref() {
        Some(q) => q
            .targetList
            .iter()
            .map(|tle| (tle.resorigtbl, tle.resorigcol))
            .collect(),
        None => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::MemoryContext;
    use types_nodes::rawnodes::CommonTableExpr;

    /// Build a CTE shell with three output columns (a int4, b int4, c text) so
    /// make_path_rowexpr can resolve column names. typmod/coll left default.
    fn cte_with_cols<'mcx>(mcx: Mcx<'mcx>) -> CommonTableExpr<'mcx> {
        let mut ctecolnames: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
        for name in ["a", "b", "c"] {
            ctecolnames.push(make_string_node(mcx, name).unwrap());
        }
        let mut ctecoltypes: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
        ctecoltypes.push(23); // int4
        ctecoltypes.push(23);
        ctecoltypes.push(25); // text
        let mut ctecoltypmods: PgVec<'mcx, i32> = PgVec::new_in(mcx);
        let mut ctecolcollations: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
        for _ in 0..3 {
            ctecoltypmods.push(-1);
            ctecolcollations.push(InvalidOid);
        }
        CommonTableExpr {
            ctename: Some(PgString::from_str_in("t", mcx).unwrap()),
            aliascolnames: PgVec::new_in(mcx),
            ctematerialized: Default::default(),
            ctequery: None,
            search_clause: None,
            cycle_clause: None,
            location: -1,
            cterecursive: true,
            cterefcount: 1,
            ctecolnames,
            ctecoltypes,
            ctecoltypmods,
            ctecolcollations,
        }
    }

    fn col_list<'mcx>(mcx: Mcx<'mcx>, names: &[&str]) -> PgVec<'mcx, NodePtr<'mcx>> {
        let mut v: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
        for n in names {
            v.push(make_string_node(mcx, n).unwrap());
        }
        v
    }

    #[test]
    fn rowexpr_resolves_named_columns() {
        let ctx = MemoryContext::new("test");
        let mcx = ctx.mcx();
        let cte = cte_with_cols(mcx);
        let cl = col_list(mcx, &["a", "c"]);
        let row = make_path_rowexpr(&cte, &cl);

        // Two Vars: col 1 (int4) and col 3 (text), RECORDOID rowtype.
        assert_eq!(row.row_typeid, RECORDOID);
        assert_eq!(row.row_format, CoercionForm::COERCE_IMPLICIT_CAST);
        assert_eq!(row.args.len(), 2);
        assert_eq!(row.colnames, vec![String::from("a"), String::from("c")]);
        match &row.args[0] {
            Expr::Var(v) => {
                assert_eq!(v.varno, 1);
                assert_eq!(v.varattno, 1);
                assert_eq!(v.vartype, 23);
            }
            _ => panic!("expected Var"),
        }
        match &row.args[1] {
            Expr::Var(v) => {
                assert_eq!(v.varattno, 3); // "c" is the 3rd CTE column
                assert_eq!(v.vartype, 25);
            }
            _ => panic!("expected Var"),
        }
    }

    #[test]
    fn initial_array_wraps_rowexpr() {
        let ctx = MemoryContext::new("test");
        let mcx = ctx.mcx();
        let cte = cte_with_cols(mcx);
        let row = make_path_rowexpr(&cte, &col_list(mcx, &["a"]));
        match make_path_initial_array(row) {
            Expr::ArrayExpr(a) => {
                assert_eq!(a.array_typeid, RECORDARRAYOID);
                assert_eq!(a.element_typeid, RECORDOID);
                assert_eq!(a.elements.len(), 1);
                assert!(matches!(a.elements[0], Expr::RowExpr(_)));
            }
            _ => panic!("expected ArrayExpr"),
        }
    }

    #[test]
    fn cat_expr_is_array_cat_with_path_var() {
        let ctx = MemoryContext::new("test");
        let mcx = ctx.mcx();
        let cte = cte_with_cols(mcx);
        let row = make_path_rowexpr(&cte, &col_list(mcx, &["a"]));
        match make_path_cat_expr(row, 4) {
            Expr::FuncExpr(f) => {
                assert_eq!(f.funcid, F_ARRAY_CAT);
                assert_eq!(f.funcresulttype, RECORDARRAYOID);
                assert_eq!(f.args.len(), 2);
                // arg0 = Var(path_varattno=4, RECORDARRAYOID); arg1 = ARRAY[ROW(..)]
                match &f.args[0] {
                    Expr::Var(v) => {
                        assert_eq!(v.varattno, 4);
                        assert_eq!(v.vartype, RECORDARRAYOID);
                    }
                    _ => panic!("expected Var"),
                }
                assert!(matches!(f.args[1], Expr::ArrayExpr(_)));
            }
            _ => panic!("expected FuncExpr"),
        }
    }
}
