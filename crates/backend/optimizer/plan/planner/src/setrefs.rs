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
        if rte.perminfoindex > 0 {
            panic!("addRTEPermissionInfo (parse_relation.c): M2 relation lane");
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
                perminfoindex: rte.perminfoindex,
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
        other => panic!("set_plan_refs (setrefs.c): {other:?}; M2 plan lane"),
    }
    Ok(plan)
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
