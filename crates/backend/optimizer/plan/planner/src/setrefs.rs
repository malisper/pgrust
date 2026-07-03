use types_error::PgResult;
use types_nodes::list::NodeList;
use types_nodes::parsenodes::{RTEKind, RangeTblEntry};
use types_nodes::{Node, NodeTag};

use crate::run::PlannerRun;

const REGCLASSOID: u32 = 2205;
const FIRST_UNPINNED_OBJECT_ID: u32 = 12000;

// Trivial arm: no rowmarks, no appendrels, no AlternativeSubPlans.
pub fn set_plan_references<'mcx>(run: &mut PlannerRun<'mcx>, plan: Node<'mcx>) -> PgResult<Node<'mcx>> {
    let rtoffset = run.glob.finalrtable.len() as i32;
    add_rtes_to_flat_rtable(run)?;
    debug_assert!(run.root.rowMarks.is_empty());
    debug_assert!(run.root.append_rel_list.is_empty());
    debug_assert!(!run.root.hasAlternativeSubPlans);
    set_plan_refs(run, plan, rtoffset)
}

// Top-level flat copy with sub-structure zapped; alias/eref stay by ref.
fn add_rtes_to_flat_rtable(run: &mut PlannerRun<'_>) -> PgResult<()> {
    let mcx = run.mcx;
    let parse = run.parse();
    for rte_node in &parse.rtable {
        let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
        // C shares the RTEPermissionInfo node into glob->finalrteperminfos and
        // renumbers the copied RTE's index.
        let mut new_perminfoindex = 0;
        if rte.perminfoindex > 0 {
            let perminfo =
                parse_relation::getRTEPermissionInfo(&parse.rteperminfos, rte)?;
            run.glob.finalrteperminfos.lappend(mcx, perminfo)?;
            new_perminfoindex = run.glob.finalrteperminfos.len() as types_core::Index;
        }
        let newrte = Node::mk(
            mcx,
            RangeTblEntry {
                alias: rte.alias,
                eref: rte.eref,
                rtekind: rte.rtekind,
                relid: rte.relid,
                inh: rte.inh,
                relkind: rte.relkind,
                rellockmode: rte.rellockmode,
                perminfoindex: new_perminfoindex,
                tablesample: None,
                subquery: None,
                security_barrier: rte.security_barrier,
                jointype: rte.jointype,
                joinmergedcols: rte.joinmergedcols,
                joinaliasvars: NodeList::nil(),
                joinleftcols: types_nodes::list::IntList::nil(),
                joinrightcols: types_nodes::list::IntList::nil(),
                join_using_alias: None,
                functions: NodeList::nil(),
                funcordinality: rte.funcordinality,
                tablefunc: None,
                values_lists: NodeList::nil(),
                ctename: rte.ctename,
                ctelevelsup: rte.ctelevelsup,
                self_reference: rte.self_reference,
                coltypes: types_nodes::list::OidList::nil(),
                coltypmods: types_nodes::list::IntList::nil(),
                colcollations: types_nodes::list::OidList::nil(),
                enrname: rte.enrname,
                enrtuples: rte.enrtuples,
                groupexprs: NodeList::nil(),
                lateral: rte.lateral,
                inFromCl: rte.inFromCl,
                securityQuals: NodeList::nil(),
            },
        )?;
        run.glob.finalrtable.lappend(mcx, newrte)?;
        if rte.rtekind == RTEKind::RTE_RELATION {
            run.glob.relation_oids.lappend(mcx, rte.relid)?;
            let rti = run.glob.finalrtable.len() as i32;
            run.glob.all_relids.add_member(mcx, rti)?;
        }
        // Dead-subquery flattening unreachable: RTE_SUBQUERY panicked earlier.
    }
    Ok(())
}

// Childless Result arm only.
fn set_plan_refs<'mcx>(run: &mut PlannerRun<'mcx>, plan: Node<'mcx>, rtoffset: i32) -> PgResult<Node<'mcx>> {
    let plan_node_id = run.glob.last_plan_node_id;
    run.glob.last_plan_node_id += 1;
    // SAFETY: the plan tree was just built by createplan and is exclusively
    // ours until returned (C mutates it in place the same way).
    unsafe { plan.with_plan_mut(|p| p.plan_node_id = plan_node_id) }.expect("plan node");

    match plan.node_tag() {
        NodeTag::T_Result => {
            let r = plan.as_result().unwrap();
            if r.plan.lefttree.is_some() {
                panic!("set_upper_references (setrefs.c): Result with subplan; M2 lane");
            }
            debug_assert!(r.plan.qual.is_nil());
            fix_scan_list(run, &r.plan.targetlist, rtoffset)?;
            if r.resconstantqual.is_some() {
                panic!("fix_scan_expr (setrefs.c): resconstantqual; M2 qual lane");
            }
        }
        NodeTag::T_SeqScan => {
            let s = plan.as_seq_scan().unwrap();
            debug_assert!(s.scan.scanrelid as i32 + rtoffset > 0);
            fix_scan_list(run, &s.scan.plan.targetlist, rtoffset)?;
            fix_scan_list(run, &s.scan.plan.qual, rtoffset)?;
        }
        NodeTag::T_IndexScan => {
            let s = plan.as_index_scan().unwrap();
            debug_assert!(s.scan.scanrelid as i32 + rtoffset > 0);
            fix_scan_list(run, &s.scan.plan.targetlist, rtoffset)?;
            fix_scan_list(run, &s.scan.plan.qual, rtoffset)?;
            fix_scan_list(run, &s.indexqual, rtoffset)?;
            fix_scan_list(run, &s.indexqualorig, rtoffset)?;
            fix_scan_list(run, &s.indexorderby, rtoffset)?;
            fix_scan_list(run, &s.indexorderbyorig, rtoffset)?;
        }
        NodeTag::T_BitmapIndexScan => {
            let s = plan.as_bitmap_index_scan().unwrap();
            debug_assert!(s.scan.scanrelid as i32 + rtoffset > 0);
            debug_assert!(s.scan.plan.targetlist.is_nil() && s.scan.plan.qual.is_nil());
            fix_scan_list(run, &s.indexqual, rtoffset)?;
            fix_scan_list(run, &s.indexqualorig, rtoffset)?;
        }
        NodeTag::T_BitmapHeapScan => {
            let s = plan.as_bitmap_heap_scan().unwrap();
            debug_assert!(s.scan.scanrelid as i32 + rtoffset > 0);
            fix_scan_list(run, &s.scan.plan.targetlist, rtoffset)?;
            fix_scan_list(run, &s.scan.plan.qual, rtoffset)?;
            fix_scan_list(run, &s.bitmapqualorig, rtoffset)?;
        }
        NodeTag::T_Agg => {
            let a = plan.as_agg().unwrap();
            debug_assert!(a.groupingSets.is_nil() && a.chain.is_nil());
            set_upper_references(run, plan, rtoffset)?;
        }
        NodeTag::T_Sort => {
            // Sort never evaluates its tlist; fixed up for EXPLAIN only.
            set_dummy_tlist_references(run, plan, rtoffset)?;
            debug_assert!(plan.as_plan().unwrap().qual.is_nil());
        }
        NodeTag::T_Limit => {
            let l = plan.as_limit().unwrap();
            set_dummy_tlist_references(run, plan, rtoffset)?;
            debug_assert!(l.plan.qual.is_nil());
            if let Some(off) = l.limitOffset {
                fix_scan_expr_walker(run, off)?;
            }
            if let Some(cnt) = l.limitCount {
                fix_scan_expr_walker(run, cnt)?;
            }
            debug_assert!(l.uniqNumCols == 0);
        }
        NodeTag::T_ModifyTable => {
            let m = plan.as_modify_table().unwrap();
            debug_assert!(m.plan.targetlist.is_nil() && m.plan.qual.is_nil());
            debug_assert!(
                m.withCheckOptionLists.is_nil()
                    && m.returningLists.is_nil()
                    && m.onConflictSet.is_nil()
                    && m.mergeActionLists.is_nil()
            );
            debug_assert!(m.rootRelation == 0 && m.rowMarks.is_nil());
            assert_eq!(rtoffset, 0, "set_plan_refs (setrefs.c): ModifyTable rtoffset leg; M4 lane");
            for rti in m.resultRelations.iter() {
                run.glob.result_relations.lappend(run.mcx, rti)?;
            }
        }
        other => panic!("set_plan_refs (setrefs.c): {other:?}; M2 plan lane"),
    }

    let base = plan.as_plan().expect("plan node");
    assert!(base.righttree.is_none(), "set_plan_refs (setrefs.c): righttree; M2 join lane");
    if let Some(child) = base.lefttree {
        let new_child = set_plan_refs(run, child, rtoffset)?;
        // SAFETY: same exclusive plan-tree ownership as the prologue above.
        unsafe { plan.with_plan_mut(|p| p.lefttree = Some(new_child)) }.expect("plan node");
    }
    Ok(plan)
}

// set_upper_references (setrefs.c): retarget an upper node's tlist at its
// subplan's output; the sortgroupref fast path is the M3 grouping lane.
fn set_upper_references<'mcx>(
    run: &mut PlannerRun<'mcx>,
    plan: Node<'mcx>,
    rtoffset: i32,
) -> PgResult<()> {
    let mcx = run.mcx;
    let base = plan.as_plan().expect("plan node");
    let subplan = base.lefttree.expect("upper node has a subplan");
    let subplan_tlist = &subplan.as_plan().expect("plan node").targetlist;

    let mut output_targetlist = NodeList::nil();
    for tle_node in &base.targetlist {
        let tle = tle_node.as_target_entry().expect("TargetEntry");
        let mut newexpr = if tle.ressortgroupref != 0 {
            search_indexed_tlist_for_sortgroupref(run, tle.expr, tle.ressortgroupref, subplan_tlist)?
        } else {
            None
        };
        if newexpr.is_none() {
            newexpr = Some(fix_upper_expr(run, tle.expr, subplan_tlist, rtoffset)?);
        }
        let newexpr = newexpr.unwrap();
        // flatCopyTargetEntry + new expr.
        let new_tle = Node::mk(
            mcx,
            types_nodes::primnodes::TargetEntry {
                expr: newexpr,
                resno: tle.resno,
                resname: tle.resname,
                ressortgroupref: tle.ressortgroupref,
                resorigtbl: tle.resorigtbl,
                resorigcol: tle.resorigcol,
                resjunk: tle.resjunk,
            },
        )?;
        output_targetlist.lappend(mcx, new_tle)?;
    }
    assert!(base.qual.is_nil(), "set_upper_references (setrefs.c): quals; M3 having lane");
    // SAFETY: exclusive plan-tree ownership (C rewrites the same list in place).
    unsafe { plan.with_plan_mut(|p| p.targetlist = output_targetlist) }.expect("plan node");
    Ok(())
}

// search_indexed_tlist_for_sortgroupref (setrefs.c): sortgroupref + equal()
// match against the subplan tlist -> OUTER_VAR reference.
fn search_indexed_tlist_for_sortgroupref<'mcx>(
    run: &mut PlannerRun<'mcx>,
    node: Node<'mcx>,
    sortgroupref: u32,
    subplan_tlist: &NodeList<'mcx>,
) -> PgResult<Option<Node<'mcx>>> {
    for tle_node in subplan_tlist {
        let tle = tle_node.as_target_entry().expect("TargetEntry");
        if tle.ressortgroupref != sortgroupref || !types_nodes::equal(tle.expr, node) {
            continue;
        }
        let (vartype, vartypmod) = crate::costsize::expr_type_typmod(node);
        let newvar = types_nodes::primnodes::Var {
            varno: types_nodes::primnodes::OUTER_VAR,
            varattno: tle.resno,
            vartype,
            vartypmod,
            varcollid: exprs_collation(node),
            varnullingrels: types_nodes::bitmapset::Bitmapset::empty(),
            varlevelsup: 0,
            varreturningtype: types_nodes::primnodes::VarReturningType::VAR_RETURNING_DEFAULT,
            varnosyn: 0,
            varattnosyn: 0,
            location: expr_location(node),
        };
        return Ok(Some(Node::mk(run.mcx, newvar)?));
    }
    Ok(None)
}

fn exprs_collation(node: Node<'_>) -> u32 {
    match node.node_tag() {
        NodeTag::T_Var => node.as_var().unwrap().varcollid,
        NodeTag::T_Const => node.as_const().unwrap().constcollid,
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().funccollid,
        NodeTag::T_OpExpr => node.as_op_expr().unwrap().opcollid,
        tag => panic!("exprCollation (nodeFuncs.c): {tag:?} not ported here"),
    }
}

fn expr_location(node: Node<'_>) -> i32 {
    match node.node_tag() {
        NodeTag::T_Var => node.as_var().unwrap().location,
        NodeTag::T_Const => node.as_const().unwrap().location,
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().location,
        NodeTag::T_OpExpr => node.as_op_expr().unwrap().location,
        _ => -1,
    }
}

// fix_upper_expr_mutator (setrefs.c) over the plain-agg tlist shapes.
fn fix_upper_expr<'mcx>(
    run: &mut PlannerRun<'mcx>,
    node: Node<'mcx>,
    subplan_tlist: &NodeList<'mcx>,
    rtoffset: i32,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    match node.node_tag() {
        NodeTag::T_Var => {
            let var = node.as_var().expect("Var");
            search_indexed_tlist_for_var(run, var, subplan_tlist, rtoffset)
        }
        NodeTag::T_Const => {
            fix_scan_expr_walker(run, node)?;
            Ok(node)
        }
        NodeTag::T_Aggref => {
            let a = node.as_aggref().expect("Aggref");
            record_plan_function_dependency(a.aggfnoid);
            debug_assert!(a.aggdirectargs.is_nil() && a.aggfilter.is_none());
            debug_assert!(a.aggorder.is_nil() && a.aggdistinct.is_nil());
            let mut args = NodeList::nil();
            for arg_node in &a.args {
                let arg = arg_node.as_target_entry().expect("agg arg is a TLE");
                let newexpr = fix_upper_expr(run, arg.expr, subplan_tlist, rtoffset)?;
                let new_tle = Node::mk(
                    mcx,
                    types_nodes::primnodes::TargetEntry {
                        expr: newexpr,
                        resno: arg.resno,
                        resname: arg.resname,
                        ressortgroupref: arg.ressortgroupref,
                        resorigtbl: arg.resorigtbl,
                        resorigcol: arg.resorigcol,
                        resjunk: arg.resjunk,
                    },
                )?;
                args.lappend(mcx, new_tle)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::Aggref {
                    aggfnoid: a.aggfnoid,
                    aggtype: a.aggtype,
                    aggcollid: a.aggcollid,
                    inputcollid: a.inputcollid,
                    aggtranstype: a.aggtranstype,
                    aggargtypes: a.aggargtypes.clone_in(mcx)?,
                    aggdirectargs: NodeList::nil(),
                    args,
                    aggorder: NodeList::nil(),
                    aggdistinct: NodeList::nil(),
                    aggfilter: None,
                    aggstar: a.aggstar,
                    aggvariadic: a.aggvariadic,
                    aggkind: a.aggkind,
                    aggpresorted: a.aggpresorted,
                    agglevelsup: a.agglevelsup,
                    aggsplit: a.aggsplit,
                    aggno: a.aggno,
                    aggtransno: a.aggtransno,
                    location: a.location,
                },
            )
        }
        other => panic!("fix_upper_expr_mutator (setrefs.c): {other:?}; M3 expression lane"),
    }
}

// search_indexed_tlist_for_var (setrefs.c); a miss is C's elog(ERROR).
fn search_indexed_tlist_for_var<'mcx>(
    run: &mut PlannerRun<'mcx>,
    var: &types_nodes::primnodes::Var<'mcx>,
    subplan_tlist: &NodeList<'mcx>,
    rtoffset: i32,
) -> PgResult<Node<'mcx>> {
    debug_assert!(var.varlevelsup == 0 && var.varnullingrels.is_empty());
    for tle_node in subplan_tlist {
        let tle = tle_node.as_target_entry().expect("TargetEntry");
        let Some(sub) = tle.expr.as_var() else { continue };
        if sub.varno == var.varno && sub.varattno == var.varattno {
            let mut newvar = types_nodes::primnodes::Var {
                varno: types_nodes::primnodes::OUTER_VAR,
                varattno: tle.resno,
                vartype: var.vartype,
                vartypmod: var.vartypmod,
                varcollid: var.varcollid,
                varnullingrels: types_nodes::bitmapset::Bitmapset::empty(),
                varlevelsup: 0,
                varreturningtype: var.varreturningtype,
                varnosyn: var.varnosyn,
                varattnosyn: var.varattnosyn,
                location: var.location,
            };
            if newvar.varnosyn > 0 {
                newvar.varnosyn += rtoffset as u32;
            }
            return Node::mk(run.mcx, newvar);
        }
    }
    panic!("variable not found in subplan target list");
}

// fix_scan_expr, rtoffset==0 walker leg (fix_expr_common only). The mutator
// leg is unreachable while the flat rtable starts empty per statement.
fn fix_scan_list<'mcx>(run: &mut PlannerRun<'mcx>, list: &NodeList<'mcx>, rtoffset: i32) -> PgResult<()> {
    assert_eq!(rtoffset, 0, "fix_scan_expr mutator leg (setrefs.c): M2 lane");
    debug_assert!(run.root.multiexpr_params.is_empty());
    debug_assert!(run.glob.last_ph_id == 0);
    debug_assert!(run.root.minmax_aggs.is_empty());
    for node in list {
        fix_scan_expr_walker(run, node)?;
    }
    Ok(())
}

fn fix_scan_expr_walker<'mcx>(run: &mut PlannerRun<'mcx>, node: Node<'mcx>) -> PgResult<()> {
    match node.node_tag() {
        // fix_expr_common touches no Var fields; INDEX_VAR Vars pass through.
        NodeTag::T_Var => Ok(()),
        NodeTag::T_RelabelType => {
            fix_scan_expr_walker(run, node.as_relabel_type().unwrap().arg)
        }
        NodeTag::T_Const => {
            let c = node.as_const().unwrap();
            // fix_expr_common: a regclass Const is a plan dependency.
            if c.consttype == REGCLASSOID && !c.constisnull {
                run.glob
                    .relation_oids
                    .lappend(run.mcx, c.constvalue.as_u32())?;
            }
            Ok(())
        }
        NodeTag::T_TargetEntry => {
            fix_scan_expr_walker(run, node.as_target_entry().unwrap().expr)
        }
        NodeTag::T_OpExpr => {
            let o = node.as_op_expr().unwrap();
            // set_opfuncid memo write-back is unmodeled (walker.rs note);
            // eval_const_expressions already resolved reachable opfuncids.
            record_plan_function_dependency(o.opfuncid);
            for arg in &o.args {
                fix_scan_expr_walker(run, arg)?;
            }
            Ok(())
        }
        NodeTag::T_FuncExpr => {
            let f = node.as_func_expr().unwrap();
            record_plan_function_dependency(f.funcid);
            for arg in &f.args {
                fix_scan_expr_walker(run, arg)?;
            }
            Ok(())
        }
        other => panic!("fix_scan_expr_walker (setrefs.c): {other:?}; M2 expression lane"),
    }
}

fn record_plan_function_dependency(funcid: u32) {
    if funcid >= FIRST_UNPINNED_OBJECT_ID {
        panic!("record_plan_function_dependency (setrefs.c): PlanInvalItem; M2 inval lane");
    }
}

// set_dummy_tlist_references (setrefs.c).
fn set_dummy_tlist_references<'mcx>(
    run: &mut PlannerRun<'mcx>,
    plan: Node<'mcx>,
    rtoffset: i32,
) -> PgResult<()> {
    let mcx = run.mcx;
    let mut output_targetlist = NodeList::nil();
    for tle_node in &plan.as_plan().expect("plan node").targetlist {
        let tle = tle_node.as_target_entry().expect("TargetEntry");
        let oldexpr = tle.expr;
        // Consts stay Consts (cleaner EXPLAIN; C keeps the whole TLE).
        if oldexpr.node_tag() == NodeTag::T_Const {
            output_targetlist.lappend(mcx, tle_node)?;
            continue;
        }
        let (vartype, vartypmod) = crate::costsize::expr_type_typmod(oldexpr);
        let varcollid = crate::pathkeys::expr_collation(oldexpr);
        let mut newvar = types_nodes::primnodes::Var {
            varno: types_nodes::primnodes::OUTER_VAR,
            varattno: tle.resno,
            vartype,
            vartypmod,
            varcollid,
            varnullingrels: types_nodes::bitmapset::Bitmapset::empty(),
            varlevelsup: 0,
            varreturningtype: Default::default(),
            varnosyn: 0,
            varattnosyn: 0,
            location: -1,
        };
        if let Some(oldvar) = oldexpr.as_var() {
            if oldvar.varnosyn > 0 {
                newvar.varnosyn = oldvar.varnosyn + rtoffset as u32;
                newvar.varattnosyn = oldvar.varattnosyn;
            }
        }
        let new_tle = Node::mk(
            mcx,
            types_nodes::primnodes::TargetEntry {
                expr: Node::mk(mcx, newvar)?,
                resno: tle.resno,
                resname: tle.resname,
                ressortgroupref: tle.ressortgroupref,
                resorigtbl: tle.resorigtbl,
                resorigcol: tle.resorigcol,
                resjunk: tle.resjunk,
            },
        )?;
        output_targetlist.lappend(mcx, new_tle)?;
    }
    // SAFETY: exclusive plan-tree ownership (C rewrites the list in place).
    unsafe { plan.with_plan_mut(|p| p.targetlist = output_targetlist) }.expect("plan node");
    Ok(())
}
