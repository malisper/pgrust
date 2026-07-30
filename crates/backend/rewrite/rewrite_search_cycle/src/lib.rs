// rewriteSearchCycle.c: expand SEARCH/CYCLE clauses of a recursive CTE.
// C copyObject's the CTE and replaces the cteList cell; the arena tree is
// uniquely owned by the rewriter here, so the CTE is mutated in place.
#![allow(non_snake_case)]

use mcx::{alloc_leak_in, Mcx};
use types_core::catalog::{BOOLOID, INT8OID, RECORDARRAYOID, RECORDOID};
use types_core::{AttrNumber, InvalidOid, Oid};
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use types_nodes::list::{IntList, OidList};
use types_nodes::parsenodes::{
    CTECycleClause, CTESearchClause, CommonTableExpr, Query, RTEKind, RangeTblEntry,
    SetOperation, SetOperationStmt,
};
use types_nodes::primnodes::{
    Alias, ArrayExpr, CaseExpr, CaseWhen, CoercionForm, FieldSelect, FromExpr, FuncExpr, OpExpr,
    RowExpr, ScalarArrayOpExpr, TargetEntry,
};
use types_nodes::{Node, NodeList};

const F_ARRAY_CAT: Oid = 383;
const F_INT8INC: Oid = 1219;
const RECORD_EQ_OP: Oid = 2988;
const FLOAT8PASSBYVAL: bool = true;

struct CteShape<'mcx> {
    colnames: &'mcx NodeList<'mcx>,
    coltypes: &'mcx OidList<'mcx>,
    coltypmods: &'mcx IntList<'mcx>,
    colcollations: &'mcx OidList<'mcx>,
    search: Option<&'mcx CTESearchClause<'mcx>>,
    cycle: Option<&'mcx CTECycleClause<'mcx>>,
}

fn make_path_rowexpr<'mcx>(
    mcx: Mcx<'mcx>,
    shape: &CteShape<'mcx>,
    col_list: &NodeList<'mcx>,
) -> PgResult<RowExpr<'mcx>> {
    let mut args = NodeList::nil();
    let mut colnames = NodeList::nil();
    for col in col_list {
        let colname = col.as_string().expect("column list cell").sval;
        for (i, name) in shape.colnames.iter().enumerate() {
            if name.as_string().expect("ctecolnames cell").sval == colname {
                args.lappend(
                    mcx,
                    Node::mk_var(
                        mcx,
                        1,
                        (i + 1) as AttrNumber,
                        shape.coltypes.nth(i),
                        shape.coltypmods.nth(i),
                        shape.colcollations.nth(i),
                        0,
                    )?,
                )?;
                colnames.lappend(mcx, Node::mk_string(mcx, colname)?)?;
                break;
            }
        }
    }
    Ok(RowExpr {
        args,
        row_typeid: RECORDOID,
        row_format: CoercionForm::COERCE_IMPLICIT_CAST,
        colnames,
        location: -1,
    })
}

fn make_path_initial_array<'mcx>(mcx: Mcx<'mcx>, rowexpr: Node<'mcx>) -> PgResult<Node<'mcx>> {
    Node::mk(
        mcx,
        ArrayExpr {
            array_typeid: RECORDARRAYOID,
            element_typeid: RECORDOID,
            elements: NodeList::make1(mcx, rowexpr)?,
            location: -1,
            ..Default::default()
        },
    )
}

fn make_path_cat_expr<'mcx>(
    mcx: Mcx<'mcx>,
    rowexpr: Node<'mcx>,
    path_varattno: AttrNumber,
) -> PgResult<Node<'mcx>> {
    let arr = Node::mk(
        mcx,
        ArrayExpr {
            array_typeid: RECORDARRAYOID,
            element_typeid: RECORDOID,
            elements: NodeList::make1(mcx, rowexpr)?,
            location: -1,
            ..Default::default()
        },
    )?;
    Node::mk(
        mcx,
        FuncExpr {
            funcid: F_ARRAY_CAT,
            funcresulttype: RECORDARRAYOID,
            funcformat: CoercionForm::COERCE_EXPLICIT_CALL,
            args: NodeList::make2(
                mcx,
                Node::mk_var(mcx, 1, path_varattno, RECORDARRAYOID, -1, 0, 0)?,
                arr,
            )?,
            location: -1,
            ..Default::default()
        },
    )
}

pub fn rewriteSearchAndCycle<'mcx>(mcx: Mcx<'mcx>, cte_node: Node<'mcx>) -> PgResult<()> {
    let cte = cte_node.as_common_table_expr().expect("CommonTableExpr");
    debug_assert!(cte.search_clause.is_some() || cte.cycle_clause.is_some());
    let shape = CteShape {
        colnames: &cte.ctecolnames,
        coltypes: &cte.ctecoltypes,
        coltypmods: &cte.ctecoltypmods,
        colcollations: &cte.ctecolcollations,
        search: cte.search_clause.map(|n| n.as_cte_search_clause().expect("search clause")),
        cycle: cte.cycle_clause.map(|n| n.as_cte_cycle_clause().expect("cycle clause")),
    };
    let ctename = cte.ctename.unwrap_or("");
    let ctequery_node = cte.ctequery.expect("analyzed CTE query");
    let ctequery = ctequery_node.as_query().expect("analyzed CTE query");

    let sos_node = ctequery.setOperations.expect("recursive CTE setOperations");
    let sos = sos_node.as_set_operation_stmt().expect("SetOperationStmt");
    debug_assert!(sos.op == SetOperation::SETOP_UNION);

    let rti1 = sos.larg.expect("larg").as_range_tbl_ref().expect("RangeTblRef").rtindex;
    let rti2 = sos.rarg.expect("rarg").as_range_tbl_ref().expect("RangeTblRef").rtindex;
    let rte1_node = ctequery.rtable.nth((rti1 - 1) as usize);
    let rte2_node = ctequery.rtable.nth((rti2 - 1) as usize);
    let rte1 = rte1_node.as_range_tbl_entry().expect("rte1");
    let rte2 = rte2_node.as_range_tbl_entry().expect("rte2");
    debug_assert!(rte1.rtekind == RTEKind::RTE_SUBQUERY);
    debug_assert!(rte2.rtekind == RTEKind::RTE_SUBQUERY);

    let search_seq_type = match shape.search {
        Some(sc) if sc.search_breadth_first => RECORDOID,
        Some(_) => RECORDARRAYOID,
        None => InvalidOid,
    };

    let ncols = shape.colnames.len();
    let sqc_attno = (ncols + 1) as AttrNumber;
    let (cmc_attno, cpa_attno) = if shape.search.is_some() {
        ((ncols + 2) as AttrNumber, (ncols + 3) as AttrNumber)
    } else {
        ((ncols + 1) as AttrNumber, (ncols + 2) as AttrNumber)
    };

    // Left subquery: wrap query1 as "*TLOCRN*"(cols) and project cols plus
    // the initial search/cycle rows.
    let orig_q1 = rte1.subquery.expect("rte1 subquery");
    rewrite_manip::IncrementVarSublevelsUp_query(orig_q1, 1, 1)?;

    let alias1 = alloc_leak_in(
        mcx,
        Alias { aliasname: Some("*TLOCRN*"), colnames: shape.colnames.clone_in(mcx)? },
    )?;
    let newrte1 = Node::mk(
        mcx,
        RangeTblEntry {
            rtekind: RTEKind::RTE_SUBQUERY,
            alias: Some(alias1),
            eref: Some(alias1),
            subquery: Some(orig_q1),
            inFromCl: true,
            ..Default::default()
        },
    )?;

    let mut newq1 = Query {
        commandType: types_nodes::nodes_enums::CmdType::CMD_SELECT,
        canSetTag: true,
        rtable: NodeList::make1(mcx, newrte1)?,
        jointree: Some(alloc_leak_in(
            mcx,
            FromExpr { fromlist: NodeList::make1(mcx, Node::mk_range_tbl_ref(mcx, 1)?)?, quals: None },
        )?),
        ..Default::default()
    };
    for i in 0..ncols {
        let var = Node::mk_var(
            mcx,
            1,
            (i + 1) as AttrNumber,
            shape.coltypes.nth(i),
            shape.coltypmods.nth(i),
            shape.colcollations.nth(i),
            0,
        )?;
        let orig_tle = orig_q1.targetList.nth(i).as_target_entry().expect("tlist cell");
        newq1.targetList.lappend(
            mcx,
            Node::mk(
                mcx,
                TargetEntry {
                    expr: var,
                    resno: (i + 1) as AttrNumber,
                    resname: Some(
                        shape.colnames.nth(i).as_string().expect("ctecolnames cell").sval,
                    ),
                    ressortgroupref: 0,
                    resorigtbl: orig_tle.resorigtbl,
                    resorigcol: orig_tle.resorigcol,
                    resjunk: false,
                },
            )?,
        )?;
    }

    let mut search_col_rowexpr: Option<Node<'mcx>> = None;
    if let Some(sc) = shape.search {
        let mut rowexpr = make_path_rowexpr(mcx, &shape, &sc.search_col_list)?;
        let texpr = if sc.search_breadth_first {
            let mut args = NodeList::make1(
                mcx,
                Node::mk_const(
                    mcx,
                    INT8OID,
                    -1,
                    InvalidOid,
                    8,
                    datum::Datum::from_i64(0),
                    false,
                    FLOAT8PASSBYVAL,
                )?,
            )?;
            args.concat(mcx, &rowexpr.args)?;
            let mut colnames = NodeList::make1(mcx, Node::mk_string(mcx, "*DEPTH*")?)?;
            colnames.concat(mcx, &rowexpr.colnames)?;
            rowexpr.args = args;
            rowexpr.colnames = colnames;
            let n = Node::mk(mcx, rowexpr)?;
            search_col_rowexpr = Some(n);
            n
        } else {
            let n = Node::mk(mcx, rowexpr)?;
            search_col_rowexpr = Some(n);
            make_path_initial_array(mcx, n)?
        };
        let resno = (newq1.targetList.len() + 1) as AttrNumber;
        newq1.targetList.lappend(
            mcx,
            Node::mk_target_entry(mcx, texpr, resno, sc.search_seq_column, false)?,
        )?;
    }
    let mut cycle_col_rowexpr: Option<Node<'mcx>> = None;
    if let Some(cc) = shape.cycle {
        let resno = (newq1.targetList.len() + 1) as AttrNumber;
        newq1.targetList.lappend(
            mcx,
            Node::mk_target_entry(
                mcx,
                cc.cycle_mark_default.expect("cycle_mark_default"),
                resno,
                cc.cycle_mark_column,
                false,
            )?,
        )?;
        let rowexpr = Node::mk(mcx, make_path_rowexpr(mcx, &shape, &cc.cycle_col_list)?)?;
        cycle_col_rowexpr = Some(rowexpr);
        let resno = (newq1.targetList.len() + 1) as AttrNumber;
        newq1.targetList.lappend(
            mcx,
            Node::mk_target_entry(
                mcx,
                make_path_initial_array(mcx, rowexpr)?,
                resno,
                cc.cycle_path_column,
                false,
            )?,
        )?;
    }

    let newq1_node = Node::mk(mcx, newq1)?;
    let mut eref1_colnames =
        rte1.eref.expect("rte1 eref").colnames.clone_in(mcx)?;
    append_new_colnames(mcx, &mut eref1_colnames, &shape)?;
    let eref1 = alloc_leak_in(
        mcx,
        Alias { aliasname: rte1.eref.expect("rte1 eref").aliasname, colnames: eref1_colnames },
    )?;
    // SAFETY: rewriter-owned tree; no derived refs of rte1 live across the write.
    unsafe {
        rte1_node.with_mut::<RangeTblEntry, _>(|r| {
            r.subquery = Some(newq1_node.as_query().expect("Query node"));
            r.eref = Some(eref1);
        })
    };

    // Right subquery: wrap query2 as "*TROCRN*"(cols + new cols); the wrapped
    // query grows tlist entries fetching the previous iteration's search/cycle
    // columns from the recursive self-reference.
    let mut ewcl = shape.colnames.clone_in(mcx)?;
    append_new_colnames(mcx, &mut ewcl, &shape)?;

    let mut cte_rtindex: i32 = -1;
    for (i, e_node) in rte2.subquery.expect("rte2 subquery").rtable.iter().enumerate() {
        let e = e_node.as_range_tbl_entry().expect("rtable cell");
        if e.rtekind == RTEKind::RTE_CTE && e.ctename == Some(ctename) && e.ctelevelsup == 2 {
            cte_rtindex = (i + 1) as i32;
            break;
        }
    }
    if cte_rtindex <= 0 {
        return Err(top_level_self_reference_required(ctename));
    }

    let orig_q2 = rte2.subquery.expect("rte2 subquery");
    rewrite_manip::IncrementVarSublevelsUp_query(orig_q2, 1, 1)?;
    // No Node handle on rte.subquery: re-issue the Query header with the
    // appended tlist entries (the old header goes unreferenced).
    // SAFETY: Query is !Drop arena data; bitwise-copied once, the source
    // reference is dead after the re-point below.
    let mut newsubquery: Query<'mcx> = unsafe { core::ptr::read(orig_q2) };
    if let Some(sc) = shape.search {
        let var = Node::mk_var(mcx, cte_rtindex, sqc_attno, search_seq_type, -1, InvalidOid, 0)?;
        let resno = (newsubquery.targetList.len() + 1) as AttrNumber;
        newsubquery.targetList.lappend(
            mcx,
            Node::mk_target_entry(mcx, var, resno, sc.search_seq_column, false)?,
        )?;
    }
    if let Some(cc) = shape.cycle {
        let var = Node::mk_var(
            mcx,
            cte_rtindex,
            cmc_attno,
            cc.cycle_mark_type,
            cc.cycle_mark_typmod,
            cc.cycle_mark_collation,
            0,
        )?;
        let resno = (newsubquery.targetList.len() + 1) as AttrNumber;
        newsubquery.targetList.lappend(
            mcx,
            Node::mk_target_entry(mcx, var, resno, cc.cycle_mark_column, false)?,
        )?;
        let var = Node::mk_var(mcx, cte_rtindex, cpa_attno, RECORDARRAYOID, -1, InvalidOid, 0)?;
        let resno = (newsubquery.targetList.len() + 1) as AttrNumber;
        newsubquery.targetList.lappend(
            mcx,
            Node::mk_target_entry(mcx, var, resno, cc.cycle_path_column, false)?,
        )?;
    }
    let newsubquery_node = Node::mk(mcx, newsubquery)?;

    let alias2 = alloc_leak_in(
        mcx,
        Alias { aliasname: Some("*TROCRN*"), colnames: ewcl.clone_in(mcx)? },
    )?;
    let newrte2 = Node::mk(
        mcx,
        RangeTblEntry {
            rtekind: RTEKind::RTE_SUBQUERY,
            alias: Some(alias2),
            eref: Some(alias2),
            subquery: Some(newsubquery_node.as_query().expect("Query node")),
            inFromCl: true,
            ..Default::default()
        },
    )?;

    let quals = match shape.cycle {
        Some(cc) => Some(Node::mk(
            mcx,
            OpExpr {
                opno: cc.cycle_mark_neop,
                opfuncid: InvalidOid,
                opresulttype: BOOLOID,
                opretset: false,
                opcollid: InvalidOid,
                inputcollid: cc.cycle_mark_collation,
                args: NodeList::make2(
                    mcx,
                    Node::mk_var(
                        mcx,
                        1,
                        cmc_attno,
                        cc.cycle_mark_type,
                        cc.cycle_mark_typmod,
                        cc.cycle_mark_collation,
                        0,
                    )?,
                    cc.cycle_mark_value.expect("cycle_mark_value"),
                )?,
                location: -1,
            },
        )?),
        None => None,
    };
    let mut newq2 = Query {
        commandType: types_nodes::nodes_enums::CmdType::CMD_SELECT,
        canSetTag: true,
        rtable: NodeList::make1(mcx, newrte2)?,
        jointree: Some(alloc_leak_in(
            mcx,
            FromExpr { fromlist: NodeList::make1(mcx, Node::mk_range_tbl_ref(mcx, 1)?)?, quals },
        )?),
        ..Default::default()
    };
    for i in 0..ncols {
        let var = Node::mk_var(
            mcx,
            1,
            (i + 1) as AttrNumber,
            shape.coltypes.nth(i),
            shape.coltypmods.nth(i),
            shape.colcollations.nth(i),
            0,
        )?;
        let orig_tle = newsubquery_node
            .as_query()
            .expect("Query node")
            .targetList
            .nth(i)
            .as_target_entry()
            .expect("tlist cell");
        newq2.targetList.lappend(
            mcx,
            Node::mk(
                mcx,
                TargetEntry {
                    expr: var,
                    resno: (i + 1) as AttrNumber,
                    resname: Some(
                        shape.colnames.nth(i).as_string().expect("ctecolnames cell").sval,
                    ),
                    ressortgroupref: 0,
                    resorigtbl: orig_tle.resorigtbl,
                    resorigcol: orig_tle.resorigcol,
                    resjunk: false,
                },
            )?,
        )?;
    }

    if let Some(sc) = shape.search {
        let texpr = if sc.search_breadth_first {
            // ROW(sqc.depth + 1, cols); C copyObject's the left-side rowexpr
            // then overwrites the depth arg — rebuilt fresh here.
            let mut rowexpr = make_path_rowexpr(mcx, &shape, &sc.search_col_list)?;
            let fs = Node::mk(
                mcx,
                FieldSelect {
                    arg: Node::mk_var(mcx, 1, sqc_attno, RECORDOID, -1, 0, 0)?,
                    fieldnum: 1,
                    resulttype: INT8OID,
                    resulttypmod: -1,
                    resultcollid: InvalidOid,
                },
            )?;
            let fexpr = Node::mk(
                mcx,
                FuncExpr {
                    funcid: F_INT8INC,
                    funcresulttype: INT8OID,
                    funcformat: CoercionForm::COERCE_EXPLICIT_CALL,
                    args: NodeList::make1(mcx, fs)?,
                    location: -1,
                    ..Default::default()
                },
            )?;
            let mut args = NodeList::make1(mcx, fexpr)?;
            args.concat(mcx, &rowexpr.args)?;
            let mut colnames = NodeList::make1(mcx, Node::mk_string(mcx, "*DEPTH*")?)?;
            colnames.concat(mcx, &rowexpr.colnames)?;
            rowexpr.args = args;
            rowexpr.colnames = colnames;
            Node::mk(mcx, rowexpr)?
        } else {
            make_path_cat_expr(mcx, search_col_rowexpr.expect("built above"), sqc_attno)?
        };
        let resno = (newq2.targetList.len() + 1) as AttrNumber;
        newq2.targetList.lappend(
            mcx,
            Node::mk_target_entry(mcx, texpr, resno, sc.search_seq_column, false)?,
        )?;
    }

    if let Some(cc) = shape.cycle {
        // CASE WHEN ROW(cols) = ANY (cpa) THEN cmv ELSE cmd END
        let saoe = Node::mk(
            mcx,
            ScalarArrayOpExpr {
                opno: RECORD_EQ_OP,
                useOr: true,
                args: NodeList::make2(
                    mcx,
                    cycle_col_rowexpr.expect("built above"),
                    Node::mk_var(mcx, 1, cpa_attno, RECORDARRAYOID, -1, 0, 0)?,
                )?,
                location: -1,
                ..Default::default()
            },
        )?;
        let casewhen = Node::mk(
            mcx,
            CaseWhen {
                expr: Some(saoe),
                result: cc.cycle_mark_value,
                location: -1,
            },
        )?;
        let caseexpr = Node::mk(
            mcx,
            CaseExpr {
                casetype: cc.cycle_mark_type,
                casecollid: cc.cycle_mark_collation,
                arg: None,
                args: NodeList::make1(mcx, casewhen)?,
                defresult: cc.cycle_mark_default,
                location: -1,
            },
        )?;
        let resno = (newq2.targetList.len() + 1) as AttrNumber;
        newq2.targetList.lappend(
            mcx,
            Node::mk_target_entry(mcx, caseexpr, resno, cc.cycle_mark_column, false)?,
        )?;
        let resno = (newq2.targetList.len() + 1) as AttrNumber;
        newq2.targetList.lappend(
            mcx,
            Node::mk_target_entry(
                mcx,
                make_path_cat_expr(mcx, cycle_col_rowexpr.expect("built above"), cpa_attno)?,
                resno,
                cc.cycle_path_column,
                false,
            )?,
        )?;
    }

    let newq2_node = Node::mk(mcx, newq2)?;
    let mut eref2_colnames = rte2.eref.expect("rte2 eref").colnames.clone_in(mcx)?;
    append_new_colnames(mcx, &mut eref2_colnames, &shape)?;
    let eref2 = alloc_leak_in(
        mcx,
        Alias { aliasname: rte2.eref.expect("rte2 eref").aliasname, colnames: eref2_colnames },
    )?;
    // SAFETY: rewriter-owned tree; no derived refs of rte2 live across the write.
    unsafe {
        rte2_node.with_mut::<RangeTblEntry, _>(|r| {
            r.subquery = Some(newq2_node.as_query().expect("Query node"));
            r.eref = Some(eref2);
        })
    };

    let all = sos.all;
    // SAFETY: rewriter-owned tree; `sos` is not read again after this write.
    unsafe {
        sos_node.with_mut::<SetOperationStmt, _>(|s| -> PgResult<()> {
            if shape.search.is_some() {
                s.colTypes.lappend(mcx, search_seq_type)?;
                s.colTypmods.lappend(mcx, -1)?;
                s.colCollations.lappend(mcx, InvalidOid)?;
                if !all {
                    s.groupClauses.lappend(
                        mcx,
                        parser_analyze::makeSortGroupClauseForSetOp(mcx, search_seq_type, true)?,
                    )?;
                }
            }
            if let Some(cc) = shape.cycle {
                s.colTypes.lappend(mcx, cc.cycle_mark_type)?;
                s.colTypmods.lappend(mcx, cc.cycle_mark_typmod)?;
                s.colCollations.lappend(mcx, cc.cycle_mark_collation)?;
                if !all {
                    s.groupClauses.lappend(
                        mcx,
                        parser_analyze::makeSortGroupClauseForSetOp(mcx, cc.cycle_mark_type, true)?,
                    )?;
                }
                s.colTypes.lappend(mcx, RECORDARRAYOID)?;
                s.colTypmods.lappend(mcx, -1)?;
                s.colCollations.lappend(mcx, InvalidOid)?;
                if !all {
                    s.groupClauses.lappend(
                        mcx,
                        parser_analyze::makeSortGroupClauseForSetOp(mcx, RECORDARRAYOID, true)?,
                    )?;
                }
            }
            Ok(())
        })
    }
    .expect("SetOperationStmt")?;

    // SAFETY: rewriter-owned tree; the ctequery ref is not read across this.
    unsafe {
        ctequery_node.with_mut::<Query, _>(|q| -> PgResult<()> {
            if let Some(sc) = shape.search {
                let resno = (q.targetList.len() + 1) as AttrNumber;
                q.targetList.lappend(
                    mcx,
                    Node::mk_target_entry(
                        mcx,
                        Node::mk_var(mcx, 1, sqc_attno, search_seq_type, -1, InvalidOid, 0)?,
                        resno,
                        sc.search_seq_column,
                        false,
                    )?,
                )?;
            }
            if let Some(cc) = shape.cycle {
                let resno = (q.targetList.len() + 1) as AttrNumber;
                q.targetList.lappend(
                    mcx,
                    Node::mk_target_entry(
                        mcx,
                        Node::mk_var(
                            mcx,
                            1,
                            cmc_attno,
                            cc.cycle_mark_type,
                            cc.cycle_mark_typmod,
                            cc.cycle_mark_collation,
                            0,
                        )?,
                        resno,
                        cc.cycle_mark_column,
                        false,
                    )?,
                )?;
                let resno = (q.targetList.len() + 1) as AttrNumber;
                q.targetList.lappend(
                    mcx,
                    Node::mk_target_entry(
                        mcx,
                        Node::mk_var(mcx, 1, cpa_attno, RECORDARRAYOID, -1, InvalidOid, 0)?,
                        resno,
                        cc.cycle_path_column,
                        false,
                    )?,
                )?;
            }
            Ok(())
        })
    }
    .expect("Query")?;

    let mut coltypes = shape.coltypes.clone_in(mcx)?;
    let mut coltypmods = shape.coltypmods.clone_in(mcx)?;
    let mut colcollations = shape.colcollations.clone_in(mcx)?;
    if shape.search.is_some() {
        coltypes.lappend(mcx, search_seq_type)?;
        coltypmods.lappend(mcx, -1)?;
        colcollations.lappend(mcx, InvalidOid)?;
    }
    if let Some(cc) = shape.cycle {
        coltypes.lappend(mcx, cc.cycle_mark_type)?;
        coltypmods.lappend(mcx, cc.cycle_mark_typmod)?;
        colcollations.lappend(mcx, cc.cycle_mark_collation)?;
        coltypes.lappend(mcx, RECORDARRAYOID)?;
        coltypmods.lappend(mcx, -1)?;
        colcollations.lappend(mcx, InvalidOid)?;
    }
    // SAFETY: rewriter-owned tree; the `cte`/`shape` borrows are dead after this.
    unsafe {
        cte_node.with_mut::<CommonTableExpr, _>(|c| {
            c.ctecolnames = ewcl;
            c.ctecoltypes = coltypes;
            c.ctecoltypmods = coltypmods;
            c.ctecolcollations = colcollations;
        })
    }
    .expect("CommonTableExpr");

    Ok(())
}

fn append_new_colnames<'mcx>(
    mcx: Mcx<'mcx>,
    list: &mut NodeList<'mcx>,
    shape: &CteShape<'mcx>,
) -> PgResult<()> {
    if let Some(sc) = shape.search {
        list.lappend(mcx, Node::mk_string(mcx, sc.search_seq_column.expect("SET column"))?)?;
    }
    if let Some(cc) = shape.cycle {
        list.lappend(mcx, Node::mk_string(mcx, cc.cycle_mark_column.expect("SET column"))?)?;
        list.lappend(mcx, Node::mk_string(mcx, cc.cycle_path_column.expect("USING column"))?)?;
    }
    Ok(())
}

#[track_caller]
#[cold]
#[inline(never)]
fn top_level_self_reference_required(ctename: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "with a SEARCH or CYCLE clause, the recursive reference to WITH query \"{ctename}\" \
             must be at the top level of its right-hand SELECT"
        ))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}
