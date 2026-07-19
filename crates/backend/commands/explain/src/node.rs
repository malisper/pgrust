// explain.c plan display. Divergence: C walks the PlanState tree (it needs
// instrumentation and run-time subplan state); ANALYZE and SubPlan display are
// loud here, so the walk reads the sealed Plan tree directly.
#![allow(non_snake_case)]

use core::fmt::Write;

use mcx::{Mcx, PgString, PgVec};
use types_error::PgResult;
use types_nodes::bitmapset::Bitmapset;
use types_nodes::list::NodeList;
use types_nodes::parsenodes::{RTEKind, RangeTblEntry};
use types_nodes::plannodes::{Plan, PlannedStmt};
use types_nodes::primnodes::{BoolExpr, BoolExprType};
use types_nodes::{Node, NodeTag};

use crate::format::{
    append, ExplainCloseGroup, ExplainIndentText, ExplainOpenGroup, ExplainPropertyBool,
    ExplainPropertyFloat, ExplainPropertyInteger, ExplainPropertyList, ExplainPropertyText,
    ExplainPropertyUInteger,
};
use define::str_in;
use crate::state::{ExplainState, EXPLAIN_FORMAT_TEXT};

// SetOpCmd/SetOpStrategy wire values (nodes.h; canonical consts live in
// types_pathnodes, not a dep of this crate).
const SETOP_SORTED: u32 = 0;
const SETOP_HASHED: u32 = 1;
const SETOPCMD_INTERSECT: u32 = 0;
const SETOPCMD_INTERSECT_ALL: u32 = 1;
const SETOPCMD_EXCEPT: u32 = 2;
const SETOPCMD_EXCEPT_ALL: u32 = 3;

#[cold]
#[inline(never)]
fn node_gap(c_fn: &str, what: &str) -> ! {
    panic!("{c_fn} (explain.c): {what}")
}

fn plan_of(node: Node<'_>) -> &Plan<'_> {
    node.as_plan().unwrap_or_else(|| {
        node_gap(
            "ExplainNode",
            &format!("{:?} plan vocabulary unported (M2+ plan lanes)", node.node_tag()),
        )
    })
}

pub fn ExplainPrintPlan<'mcx>(
    mcx: Mcx<'mcx>,
    es: &mut ExplainState<'mcx>,
    pstmt: &'mcx PlannedStmt<'mcx>,
) -> PgResult<()> {
    es.pstmt = Some(pstmt);
    es.rtable = Some(&pstmt.rtable);
    let root = pstmt.planTree.expect("ExplainPrintPlan: PlannedStmt without planTree");
    let mut rels_used = Bitmapset::empty();
    let spctx =
        SubPlanScanCtx { analyze: es.analyze, part_prune_infos: &pstmt.partPruneInfos };
    ExplainPreScanNode(mcx, root, &pstmt.subplans, es.qd, spctx, &mut rels_used)?;
    let rtable_names = ruleutils::select_rtable_names_for_explain(mcx, &pstmt.rtable, &rels_used)?;
    let mut names: PgVec<'mcx, Option<&'mcx str>> = PgVec::new_in(mcx);
    for n in &rtable_names {
        names.push(match n {
            Some(s) => Some(str_in(mcx, s)?),
            None => None,
        });
    }
    es.rtable_names = names;
    es.deparse_cxt = Some(ruleutils::deparse_context_for_plan_tree(mcx, pstmt, rtable_names)?);
    // plan_ids are per-PlannedStmt: a rewrite product restarts the dedup set.
    es.printed_subplans = Bitmapset::empty();
    es.rtable_size = pstmt.rtable.len() as i32;
    for rte in pstmt.rtable.iter() {
        if rte.as_range_tbl_entry().expect("rtable holds RTEs").rtekind == RTEKind::RTE_GROUP {
            es.rtable_size -= 1;
            break;
        }
    }
    // debug_parallel_query=regress marks its top Gather invisible: skip it
    // and hide per-worker detail below (explain.c).
    let mut root = root;
    if let Some(g) = root.as_gather() {
        if g.invisible {
            root = g.plan.lefttree.expect("invisible Gather without an outer plan");
            es.hide_workers = true;
        }
    }
    ExplainNode(root, None, None, None, es)?;
    ExplainPrintSettings(es)?;
    if es.verbose
        && pstmt.queryId.get() != 0
        && guc_tables::backing::compute_query_id() != guc_tables::consts::COMPUTE_QUERY_ID_REGRESS
    {
        ExplainPropertyInteger("Query Identifier", None, pstmt.queryId.get(), es);
    }
    Ok(())
}

// explain.c ExplainPrintSettings.
fn ExplainPrintSettings(es: &mut ExplainState<'_>) -> PgResult<()> {
    if !es.settings {
        return Ok(());
    }
    let gucs = guc_seams::get_explain_guc_options::call()?;
    if es.format != EXPLAIN_FORMAT_TEXT {
        ExplainOpenGroup("Settings", Some("Settings"), true, es);
        for (name, setting) in &gucs {
            ExplainPropertyText(name, setting.as_deref().unwrap_or(""), es);
        }
        ExplainCloseGroup("Settings", Some("Settings"), true, es);
    } else {
        if gucs.is_empty() {
            return Ok(());
        }
        let mut line = String::new();
        for (i, (name, setting)) in gucs.iter().enumerate() {
            if i > 0 {
                line.push_str(", ");
            }
            match setting {
                Some(v) => {
                    write!(line, "{name} = '{v}'").expect("string write");
                }
                None => {
                    write!(line, "{name} = NULL").expect("string write");
                }
            }
        }
        ExplainPropertyText("Settings", &line, es);
    }
    Ok(())
}

// The slice of ExecInitNode context the Plan-based subPlan reconstruction
// needs: plain EXPLAIN runs under EXEC_FLAG_EXPLAIN_ONLY (index nodes bail
// before building runtime keys, so indexqual SubPlans never init), and
// Append/MergeAppend exec-pruning expressions init on the parent node.
#[derive(Clone, Copy)]
struct SubPlanScanCtx<'a, 'mcx> {
    analyze: bool,
    part_prune_infos: &'a NodeList<'mcx>,
}

fn ExplainPreScanNode<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    subplans: &types_nodes::list::OptNodeList<'mcx>,
    qd: types_portal::QueryDescHandle,
    ctx: SubPlanScanCtx<'_, 'mcx>,
    rels_used: &mut Bitmapset<'mcx>,
) -> PgResult<()> {
    match node.node_tag() {
        NodeTag::T_SeqScan => {
            rels_used.add_member(mcx, node.as_seq_scan().unwrap().scan.scanrelid as i32)?;
        }
        NodeTag::T_SampleScan => {
            rels_used.add_member(mcx, node.as_sample_scan().unwrap().scan.scanrelid as i32)?;
        }
        NodeTag::T_IndexScan => {
            rels_used.add_member(mcx, node.as_index_scan().unwrap().scan.scanrelid as i32)?;
        }
        NodeTag::T_TidScan => {
            rels_used.add_member(mcx, node.as_tid_scan().unwrap().scan.scanrelid as i32)?;
        }
        NodeTag::T_TidRangeScan => {
            rels_used
                .add_member(mcx, node.as_tid_range_scan().unwrap().scan.scanrelid as i32)?;
        }
        NodeTag::T_IndexOnlyScan => {
            rels_used
                .add_member(mcx, node.as_index_only_scan().unwrap().scan.scanrelid as i32)?;
        }
        NodeTag::T_BitmapHeapScan => {
            rels_used
                .add_member(mcx, node.as_bitmap_heap_scan().unwrap().scan.scanrelid as i32)?;
        }
        NodeTag::T_CteScan => {
            rels_used.add_member(mcx, node.as_cte_scan().unwrap().scan.scanrelid as i32)?;
        }
        NodeTag::T_NamedTuplestoreScan => {
            rels_used
                .add_member(mcx, node.as_named_tuplestore_scan().unwrap().scan.scanrelid as i32)?;
        }
        NodeTag::T_WorkTableScan => {
            rels_used
                .add_member(mcx, node.as_work_table_scan().unwrap().scan.scanrelid as i32)?;
        }
        NodeTag::T_ValuesScan => {
            rels_used.add_member(mcx, node.as_values_scan().unwrap().scan.scanrelid as i32)?;
        }
        NodeTag::T_ForeignScan => {
            rels_used.add_members(mcx, &node.as_foreign_scan().unwrap().fs_base_relids)?;
        }
        NodeTag::T_FunctionScan => {
            rels_used.add_member(mcx, node.as_function_scan().unwrap().scan.scanrelid as i32)?;
        }
        NodeTag::T_TableFuncScan => {
            rels_used
                .add_member(mcx, node.as_table_func_scan().unwrap().scan.scanrelid as i32)?;
        }
        NodeTag::T_SubqueryScan => {
            let sq = node.as_subquery_scan().unwrap();
            rels_used.add_member(mcx, sq.scan.scanrelid as i32)?;
            ExplainPreScanNode(mcx, sq.subplan.expect("SubqueryScan subplan"), subplans, qd, ctx, rels_used)?;
        }
        NodeTag::T_ModifyTable => {
            let mt = node.as_modify_table().unwrap();
            rels_used.add_member(mcx, mt.nominalRelation as i32)?;
            if mt.exclRelRTI != 0 {
                rels_used.add_member(mcx, mt.exclRelRTI as i32)?;
            }
            // Vars in RETURNING need refnames.
            if !mt.plan.targetlist.is_nil() {
                rels_used.add_member(mcx, mt.resultRelations.as_slice()[0])?;
            }
        }
        NodeTag::T_Append | NodeTag::T_MergeAppend => {
            // C walks the PLANSTATE tree: initially-pruned subplans have no
            // state and never reach rels_used (their RTEs get no name).
            let (apprelids, children, part_prune_index) = match node.as_append() {
                Some(a) => (&a.apprelids, &a.appendplans, a.part_prune_index),
                None => {
                    let m = node.as_merge_append().unwrap();
                    (&m.apprelids, &m.mergeplans, m.part_prune_index)
                }
            };
            rels_used.add_members(mcx, apprelids)?;
            let valid: Option<Vec<i32>> = if part_prune_index >= 0 && !qd.is_null() {
                execmain_seams::query_desc_prune_result::call(qd, part_prune_index)
            } else {
                None
            };
            match valid {
                Some(v) => {
                    for &i in &v {
                        ExplainPreScanNode(
                            mcx,
                            children.nth(i as usize),
                            subplans,
                            qd,
                            ctx,
                            rels_used,
                        )?;
                    }
                }
                None => {
                    for child in children {
                        ExplainPreScanNode(mcx, child, subplans, qd, ctx, rels_used)?;
                    }
                }
            }
        }
        // planstate_tree_walker's special-member leg for bitmap combiners.
        NodeTag::T_BitmapAnd => {
            for child in &node.as_bitmap_and().unwrap().bitmapplans {
                ExplainPreScanNode(mcx, child, subplans, qd, ctx, rels_used)?;
            }
        }
        NodeTag::T_BitmapOr => {
            for child in &node.as_bitmap_or().unwrap().bitmapplans {
                ExplainPreScanNode(mcx, child, subplans, qd, ctx, rels_used)?;
            }
        }
        _ => {}
    }
    let plan = plan_of(node);
    // planstate_tree_walker's initPlan + subPlan legs: reach each referenced
    // SubPlan's plan tree through PlannedStmt.subplans (the walk here is
    // Plan-based, not PlanState). Unreferenced alt-subplan losers stay out of
    // rels_used, as C's NULLed glob->subplans cells do.
    for sp_node in plan.initPlan.iter() {
        let sp = sp_node.as_sub_plan().expect("initPlan holds SubPlan nodes");
        let child =
            subplans.nth(sp.plan_id as usize - 1).expect("EXPLAIN pstmt has no subplan holes");
        ExplainPreScanNode(mcx, child, subplans, qd, ctx, rels_used)?;
    }
    for sp in collect_node_subplans(mcx, node, ctx)?.iter() {
        let child =
            subplans.nth(sp.plan_id as usize - 1).expect("EXPLAIN pstmt has no subplan holes");
        ExplainPreScanNode(mcx, child, subplans, qd, ctx, rels_used)?;
    }
    if let Some(l) = plan.lefttree {
        ExplainPreScanNode(mcx, l, subplans, qd, ctx, rels_used)?;
    }
    if let Some(r) = plan.righttree {
        ExplainPreScanNode(mcx, r, subplans, qd, ctx, rels_used)?;
    }
    Ok(())
}

fn plan_is_disabled(node: Node<'_>) -> bool {
    let plan = plan_of(node);
    if plan.disabled_nodes == 0 {
        return false;
    }
    let mut child_disabled = 0;
    if let Some(a) = node.as_append() {
        for child in &a.appendplans {
            child_disabled += plan_of(child).disabled_nodes;
        }
    } else if let Some(m) = node.as_merge_append() {
        for child in &m.mergeplans {
            child_disabled += plan_of(child).disabled_nodes;
        }
    } else if let Some(sq) = node.as_subquery_scan() {
        child_disabled += plan_of(sq.subplan.expect("SubqueryScan subplan")).disabled_nodes;
    } else {
        if let Some(l) = plan.lefttree {
            child_disabled += plan_of(l).disabled_nodes;
        }
        if let Some(r) = plan.righttree {
            child_disabled += plan_of(r).disabled_nodes;
        }
    }
    plan.disabled_nodes > child_disabled
}

pub use ruleutils::AncestorEntry;

pub struct Ancestors<'a, 'mcx> {
    entry: AncestorEntry<'mcx>,
    parent: Option<&'a Ancestors<'a, 'mcx>>,
}

fn collect_node_subplans<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    ctx: SubPlanScanCtx<'_, 'mcx>,
) -> PgResult<PgVec<'mcx, &'mcx types_nodes::primnodes::SubPlan<'mcx>>> {
    let plan = plan_of(node);
    let mut out: PgVec<'mcx, &'mcx types_nodes::primnodes::SubPlan<'mcx>> = PgVec::new_in(mcx);
    let mut walk_list = |out: &mut PgVec<'mcx, &'mcx types_nodes::primnodes::SubPlan<'mcx>>,
                         list: &NodeList<'mcx>| {
        for n in list {
            collect_subplans_expr(n, out);
        }
    };
    match node.node_tag() {
        NodeTag::T_NestLoop | NodeTag::T_MergeJoin => {
            walk_list(&mut out, &plan.qual);
            let joinqual = match node.node_tag() {
                NodeTag::T_NestLoop => &node.as_nest_loop().unwrap().join.joinqual,
                _ => &node.as_merge_join().unwrap().join.joinqual,
            };
            walk_list(&mut out, joinqual);
            walk_list(&mut out, &plan.targetlist);
        }
        // ExecInitHashJoin order: projection, outer-hashkey
        // ExecBuildHash32Expr, then qual/joinqual/hashclauses.
        NodeTag::T_HashJoin => {
            let hj = node.as_hash_join().unwrap();
            walk_list(&mut out, &plan.targetlist);
            walk_list(&mut out, &hj.hashkeys);
            walk_list(&mut out, &plan.qual);
            walk_list(&mut out, &hj.join.joinqual);
            walk_list(&mut out, &hj.hashclauses);
        }
        // The build-side hash expr is the Hash node's only compiled
        // expression (ExecInitHashJoin builds it against hashstate).
        NodeTag::T_Hash => {
            walk_list(&mut out, &node.as_hash().unwrap().hashkeys);
        }
        // ExecInitModifyTable order: WCO, RETURNING, ON CONFLICT, then
        // ExecInitMerge (per action: WHEN qual then projection); the per-rel
        // lists share plan_ids and the printer dedups.
        NodeTag::T_ModifyTable => {
            let mt = node.as_modify_table().unwrap();
            walk_list(&mut out, &plan.targetlist);
            walk_list(&mut out, &plan.qual);
            for wlist in mt.withCheckOptionLists.iter() {
                if let Some(l) = wlist.as_list() {
                    walk_list(&mut out, l);
                }
            }
            for rlist in mt.returningLists.iter() {
                if let Some(l) = rlist.as_list() {
                    walk_list(&mut out, l);
                }
            }
            walk_list(&mut out, &mt.onConflictSet);
            if let Some(w) = mt.onConflictWhere {
                collect_subplans_expr(w, &mut out);
            }
            for jc in mt.mergeJoinConditions.iter() {
                if let Some(l) = jc.as_list() {
                    walk_list(&mut out, l);
                }
            }
            for mal in mt.mergeActionLists.iter() {
                let Some(actions) = mal.as_list() else { continue };
                for action_node in actions {
                    let Some(action) = action_node.as_merge_action() else { continue };
                    if let Some(q) = action.qual {
                        // Preprocessed WHEN quals are implicit-AND Lists.
                        match q.as_list() {
                            Some(l) => walk_list(&mut out, l),
                            None => collect_subplans_expr(q, &mut out),
                        }
                    }
                    walk_list(&mut out, &action.targetList);
                }
            }
        }
        NodeTag::T_Result => {
            walk_list(&mut out, &plan.targetlist);
            walk_list(&mut out, &plan.qual);
            if let Some(q) = node.as_result().unwrap().resconstantqual {
                if let Some(l) = q.as_list() {
                    walk_list(&mut out, l);
                }
            }
        }
        NodeTag::T_ForeignScan => {
            walk_list(&mut out, &plan.targetlist);
            walk_list(&mut out, &plan.qual);
            walk_list(&mut out, &node.as_foreign_scan().unwrap().fdw_recheck_quals);
        }
        // C ExecInitValuesScan: projection, qual, then the per-row
        // exprstatelists — SubPlans inside VALUES rows land on the scan's
        // subPlan list.
        NodeTag::T_ValuesScan => {
            walk_list(&mut out, &plan.targetlist);
            walk_list(&mut out, &plan.qual);
            for row in &node.as_values_scan().unwrap().values_lists {
                if let Some(l) = row.as_list() {
                    walk_list(&mut out, l);
                }
            }
        }
        // ExecInitIndexScan: projection, qual, indexqualorig, indexorderbyorig.
        // Plain EXPLAIN runs under EXEC_FLAG_EXPLAIN_ONLY and exits before
        // ExecIndexBuildScanKeys, so indexqual/indexorderby runtime-key
        // SubPlans init only under ANALYZE.
        NodeTag::T_IndexScan => {
            let s = node.as_index_scan().unwrap();
            walk_list(&mut out, &plan.targetlist);
            walk_list(&mut out, &plan.qual);
            walk_list(&mut out, &s.indexqualorig);
            walk_list(&mut out, &s.indexorderbyorig);
            if ctx.analyze {
                walk_list(&mut out, &s.indexqual);
                walk_list(&mut out, &s.indexorderby);
            }
        }
        // ExecInitIndexOnlyScan: projection, qual, recheckqual, then the
        // runtime keys (ANALYZE only, as above).
        NodeTag::T_IndexOnlyScan => {
            let s = node.as_index_only_scan().unwrap();
            walk_list(&mut out, &plan.targetlist);
            walk_list(&mut out, &plan.qual);
            walk_list(&mut out, &s.recheckqual);
            if ctx.analyze {
                walk_list(&mut out, &s.indexqual);
                walk_list(&mut out, &s.indexorderby);
            }
        }
        // ExecInitBitmapIndexScan inits no quals; runtime keys as above.
        NodeTag::T_BitmapIndexScan => {
            if ctx.analyze {
                walk_list(&mut out, &node.as_bitmap_index_scan().unwrap().indexqual);
            }
        }
        NodeTag::T_BitmapHeapScan => {
            walk_list(&mut out, &plan.targetlist);
            walk_list(&mut out, &plan.qual);
            walk_list(&mut out, &node.as_bitmap_heap_scan().unwrap().bitmapqualorig);
        }
        // ExecInitPartitionExecPruning inits the exec-pruning step exprs on
        // the parent Append/MergeAppend node; initial steps init parentless
        // at ExecDoInitialPruning and cannot carry SubPlans. The Append's
        // own targetlist/qual are never compiled (no projection).
        NodeTag::T_Append | NodeTag::T_MergeAppend => {
            let ppi = match node.as_append() {
                Some(a) => a.part_prune_index,
                None => node.as_merge_append().unwrap().part_prune_index,
            };
            if ppi >= 0 {
                let pinfo = ctx
                    .part_prune_infos
                    .nth(ppi as usize)
                    .as_partition_prune_info()
                    .expect("PartitionPruneInfo node");
                for l in pinfo.prune_infos.iter() {
                    let prune_infos = l.as_list().expect("prune_infos cell is a List");
                    for prel_node in prune_infos.iter() {
                        let prel = prel_node
                            .as_partitioned_rel_prune_info()
                            .expect("PartitionedRelPruneInfo node");
                        for step in prel.exec_pruning_steps.iter() {
                            let Some(op) = step.as_partition_prune_step_op() else { continue };
                            walk_list(&mut out, &op.exprs);
                        }
                    }
                }
            }
        }
        // Scans: projection compiles before the qual (C ExecInitSeqScan).
        _ => {
            walk_list(&mut out, &plan.targetlist);
            walk_list(&mut out, &plan.qual);
        }
    }
    Ok(out)
}

fn collect_subplans_expr<'mcx>(
    node: Node<'mcx>,
    out: &mut PgVec<'mcx, &'mcx types_nodes::primnodes::SubPlan<'mcx>>,
) {
    if let Some(sp) = node.as_sub_plan() {
        out.push(sp);
        if let Some(te) = sp.testexpr {
            collect_subplans_expr(te, out);
        }
        // C ExecInitSubPlan inits args after testexpr; nested SubPlans there
        // (outer-agg arguments) land on the same node's subPlan list.
        for a in &sp.args {
            collect_subplans_expr(a, out);
        }
        return;
    }
    match node.node_tag() {
        // onConflictWhere carries an implicit-AND List (qual convention).
        NodeTag::T_List => {
            if let Some(l) = node.as_list() {
                for e in l {
                    collect_subplans_expr(e, out);
                }
            }
        }
        NodeTag::T_TargetEntry => {
            collect_subplans_expr(node.as_target_entry().unwrap().expr, out)
        }
        NodeTag::T_OpExpr => {
            for a in &node.as_op_expr().unwrap().args {
                collect_subplans_expr(a, out);
            }
        }
        NodeTag::T_FuncExpr => {
            for a in &node.as_func_expr().unwrap().args {
                collect_subplans_expr(a, out);
            }
        }
        NodeTag::T_BoolExpr => {
            for a in &node.as_bool_expr().unwrap().args {
                collect_subplans_expr(a, out);
            }
        }
        NodeTag::T_RelabelType => {
            collect_subplans_expr(node.as_relabel_type().unwrap().arg, out)
        }
        NodeTag::T_NullTest => {
            if let Some(a) = node.as_null_test().unwrap().arg {
                collect_subplans_expr(a, out);
            }
        }
        NodeTag::T_BooleanTest => {
            if let Some(a) = node.as_boolean_test().unwrap().arg {
                collect_subplans_expr(a, out);
            }
        }
        NodeTag::T_DistinctExpr => {
            for a in &node.as_distinct_expr().unwrap().args {
                collect_subplans_expr(a, out);
            }
        }
        NodeTag::T_Aggref => {
            for a in &node.as_aggref().unwrap().args {
                collect_subplans_expr(a, out);
            }
        }
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            if let Some(a) = c.arg {
                collect_subplans_expr(a, out);
            }
            for w in &c.args {
                collect_subplans_expr(w, out);
            }
            if let Some(d) = c.defresult {
                collect_subplans_expr(d, out);
            }
        }
        NodeTag::T_CaseWhen => {
            let w = node.as_case_when().unwrap();
            if let Some(e) = w.expr {
                collect_subplans_expr(e, out);
            }
            if let Some(r) = w.result {
                collect_subplans_expr(r, out);
            }
        }
        NodeTag::T_CoalesceExpr => {
            for a in &node.as_coalesce_expr().unwrap().args {
                collect_subplans_expr(a, out);
            }
        }
        NodeTag::T_MinMaxExpr => {
            for a in &node.as_min_max_expr().unwrap().args {
                collect_subplans_expr(a, out);
            }
        }
        NodeTag::T_ScalarArrayOpExpr => {
            for a in &node.as_scalar_array_op_expr().unwrap().args {
                collect_subplans_expr(a, out);
            }
        }
        NodeTag::T_ArrayExpr => {
            for e in &node.as_array_expr().unwrap().elements {
                collect_subplans_expr(e, out);
            }
        }
        NodeTag::T_RowExpr => {
            for a in &node.as_row_expr().unwrap().args {
                collect_subplans_expr(a, out);
            }
        }
        NodeTag::T_CoerceViaIO => {
            collect_subplans_expr(node.as_coerce_via_io().unwrap().arg, out)
        }
        NodeTag::T_CoerceToDomain => {
            collect_subplans_expr(node.as_coerce_to_domain().unwrap().arg, out)
        }
        NodeTag::T_WithCheckOption => {
            if let Some(q) = node.as_with_check_option().unwrap().qual {
                collect_subplans_expr(q, out);
            }
        }
        NodeTag::T_ReturningExpr => {
            collect_subplans_expr(node.as_returning_expr().unwrap().retexpr, out)
        }
        _ => {}
    }
}

pub fn ExplainNode<'mcx>(
    node: Node<'mcx>,
    relationship: Option<&str>,
    plan_name: Option<&str>,
    ancestors: Option<&Ancestors<'_, 'mcx>>,
    es: &mut ExplainState<'mcx>,
) -> PgResult<()> {
    let plan = plan_of(node);
    let save_indent = es.indent;

    let mut strategy: Option<&str> = None;
    let mut partialmode: Option<&str> = None;
    let mut operation: Option<&str> = None;
    let (pname, sname): (&str, &str) = match node.node_tag() {
        NodeTag::T_Result => ("Result", "Result"),
        NodeTag::T_ProjectSet => ("ProjectSet", "ProjectSet"),
        NodeTag::T_Append => ("Append", "Append"),
        NodeTag::T_MergeAppend => ("Merge Append", "Merge Append"),
        NodeTag::T_SubqueryScan => ("Subquery Scan", "Subquery Scan"),
        NodeTag::T_SetOp => {
            let p = match node.as_set_op().expect("SetOp plan node").strategy {
                SETOP_SORTED => {
                    strategy = Some("Sorted");
                    "SetOp"
                }
                SETOP_HASHED => {
                    strategy = Some("Hashed");
                    "HashSetOp"
                }
                other => {
                    node_gap("ExplainNode", &format!("SetOp strategy {other} unrecognized"))
                }
            };
            (p, "SetOp")
        }
        NodeTag::T_SeqScan => ("Seq Scan", "Seq Scan"),
        NodeTag::T_SampleScan => ("Sample Scan", "Sample Scan"),
        NodeTag::T_TidScan => ("Tid Scan", "Tid Scan"),
        NodeTag::T_TidRangeScan => ("Tid Range Scan", "Tid Range Scan"),
        NodeTag::T_IndexScan => ("Index Scan", "Index Scan"),
        NodeTag::T_IndexOnlyScan => ("Index Only Scan", "Index Only Scan"),
        NodeTag::T_BitmapIndexScan => ("Bitmap Index Scan", "Bitmap Index Scan"),
        NodeTag::T_BitmapHeapScan => ("Bitmap Heap Scan", "Bitmap Heap Scan"),
        NodeTag::T_BitmapAnd => ("BitmapAnd", "BitmapAnd"),
        NodeTag::T_BitmapOr => ("BitmapOr", "BitmapOr"),
        NodeTag::T_FunctionScan => ("Function Scan", "Function Scan"),
        NodeTag::T_TableFuncScan => ("Table Function Scan", "Table Function Scan"),
        NodeTag::T_ValuesScan => ("Values Scan", "Values Scan"),
        NodeTag::T_ForeignScan => {
            assert!(
                node.as_foreign_scan().unwrap().operation
                    == types_nodes::CmdType::CMD_SELECT,
                "ExplainNode (explain.c): ForeignScan direct modify unported"
            );
            ("Foreign Scan", "Foreign Scan")
        }
        NodeTag::T_CteScan => ("CTE Scan", "CTE Scan"),
        NodeTag::T_NamedTuplestoreScan => ("Named Tuplestore Scan", "Named Tuplestore Scan"),
        NodeTag::T_WorkTableScan => ("WorkTable Scan", "WorkTable Scan"),
        NodeTag::T_RecursiveUnion => ("Recursive Union", "Recursive Union"),
        NodeTag::T_NestLoop => ("Nested Loop", "Nested Loop"),
        NodeTag::T_Gather => ("Gather", "Gather"),
        NodeTag::T_GatherMerge => ("Gather Merge", "Gather Merge"),
        // C interpolates the join type into the TEXT node name: "Hash"/"Merge"
        // + " <Jointype> Join" (inner non-nestloop gets a bare " Join").
        NodeTag::T_HashJoin => ("Hash", "Hash Join"),
        NodeTag::T_MergeJoin => ("Merge", "Merge Join"),
        NodeTag::T_Hash => ("Hash", "Hash"),
        NodeTag::T_Material => ("Materialize", "Materialize"),
        NodeTag::T_Memoize => ("Memoize", "Memoize"),
        NodeTag::T_Agg => {
            let agg = node.as_agg().expect("Agg plan node");
            partialmode = Some(
                if agg.aggsplit & types_nodes::primnodes::AGGSPLITOP_SKIPFINAL != 0 {
                    "Partial"
                } else if agg.aggsplit & types_nodes::primnodes::AGGSPLITOP_COMBINE != 0 {
                    "Finalize"
                } else {
                    "Simple"
                },
            );
            let p = match agg.aggstrategy {
                0 => {
                    strategy = Some("Plain");
                    "Aggregate"
                }
                1 => {
                    strategy = Some("Sorted");
                    "GroupAggregate"
                }
                2 => {
                    strategy = Some("Hashed");
                    "HashAggregate"
                }
                3 => {
                    strategy = Some("Mixed");
                    "MixedAggregate"
                }
                other => node_gap("ExplainNode", &format!("Agg strategy {other} unrecognized")),
            };
            (p, "Aggregate")
        }
        NodeTag::T_Group => ("Group", "Group"),
        NodeTag::T_Unique => ("Unique", "Unique"),
        NodeTag::T_Sort => ("Sort", "Sort"),
        NodeTag::T_IncrementalSort => ("Incremental Sort", "Incremental Sort"),
        NodeTag::T_WindowAgg => ("WindowAgg", "WindowAgg"),
        NodeTag::T_Limit => ("Limit", "Limit"),
        NodeTag::T_LockRows => ("LockRows", "LockRows"),
        NodeTag::T_ModifyTable => {
            let mt = node.as_modify_table().expect("ModifyTable plan node");
            let p = match mt.operation {
                types_nodes::CmdType::CMD_INSERT => "Insert",
                types_nodes::CmdType::CMD_UPDATE => "Update",
                types_nodes::CmdType::CMD_DELETE => "Delete",
                types_nodes::CmdType::CMD_MERGE => "Merge",
                other => node_gap("ExplainNode", &format!("ModifyTable operation {other:?}")),
            };
            operation = Some(p);
            (p, "ModifyTable")
        }
        t => node_gap("ExplainNode", &format!("{t:?} display arm unported (M2+ plan lanes)")),
    };

    ExplainOpenGroup("Plan", if relationship.is_some() { None } else { Some("Plan") }, true, es);

    if es.format == EXPLAIN_FORMAT_TEXT {
        if let Some(name) = plan_name {
            crate::format::ExplainIndentText(es);
            append!(es, "{name}\n");
            es.indent += 1;
        }
        if es.indent != 0 {
            crate::format::ExplainIndentText(es);
            append!(es, "->  ");
            es.indent += 2;
        }
        if plan.parallel_aware {
            append!(es, "Parallel ");
        }
        if plan.async_capable {
            append!(es, "Async ");
        }
        if let Some(agg) = node.as_agg() {
            if agg.aggsplit & types_nodes::primnodes::AGGSPLITOP_SKIPFINAL != 0 {
                append!(es, "Partial ");
            } else if agg.aggsplit & types_nodes::primnodes::AGGSPLITOP_COMBINE != 0 {
                append!(es, "Finalize ");
            }
        }
        append!(es, "{pname}");
        es.indent += 1;
    } else {
        ExplainPropertyText("Node Type", sname, es);
        if let Some(s) = strategy {
            ExplainPropertyText("Strategy", s, es);
        }
        if let Some(p) = partialmode {
            ExplainPropertyText("Partial Mode", p, es);
        }
        if let Some(o) = operation {
            ExplainPropertyText("Operation", o, es);
        }
        if let Some(r) = relationship {
            ExplainPropertyText("Parent Relationship", r, es);
        }
        if let Some(name) = plan_name {
            ExplainPropertyText("Subplan Name", name, es);
        }
        ExplainPropertyBool("Parallel Aware", plan.parallel_aware, es);
        ExplainPropertyBool("Async Capable", plan.async_capable, es);
    }

    let join_type = match node.node_tag() {
        NodeTag::T_NestLoop => Some(node.as_nest_loop().unwrap().join.jointype),
        NodeTag::T_HashJoin => Some(node.as_hash_join().unwrap().join.jointype),
        NodeTag::T_MergeJoin => Some(node.as_merge_join().unwrap().join.jointype),
        _ => None,
    };
    if let Some(jt) = join_type {
        let jtname = match jt {
            types_nodes::JoinType::JOIN_INNER => "Inner",
            types_nodes::JoinType::JOIN_LEFT => "Left",
            types_nodes::JoinType::JOIN_FULL => "Full",
            types_nodes::JoinType::JOIN_RIGHT => "Right",
            types_nodes::JoinType::JOIN_SEMI => "Semi",
            types_nodes::JoinType::JOIN_RIGHT_SEMI => "Right Semi",
            types_nodes::JoinType::JOIN_ANTI => "Anti",
            types_nodes::JoinType::JOIN_RIGHT_ANTI => "Right Anti",
            other => panic!("unrecognized join type: {other:?}"),
        };
        if es.format == EXPLAIN_FORMAT_TEXT {
            if jt != types_nodes::JoinType::JOIN_INNER {
                append!(es, " {jtname} Join");
            } else if node.node_tag() != NodeTag::T_NestLoop {
                append!(es, " Join");
            }
        } else {
            ExplainPropertyText("Join Type", jtname, es);
        }
    }
    if let Some(so) = node.as_set_op() {
        let setopcmd = match so.cmd {
            SETOPCMD_INTERSECT => "Intersect",
            SETOPCMD_INTERSECT_ALL => "Intersect All",
            SETOPCMD_EXCEPT => "Except",
            SETOPCMD_EXCEPT_ALL => "Except All",
            other => node_gap("ExplainNode", &format!("SetOp command {other} unrecognized")),
        };
        if es.format == EXPLAIN_FORMAT_TEXT {
            append!(es, " {setopcmd}");
        } else {
            ExplainPropertyText("Command", setopcmd, es);
        }
    }

    if node.node_tag() == NodeTag::T_SeqScan {
        ExplainScanTarget(node.as_seq_scan().unwrap().scan.scanrelid, es)?;
    }
    if let Some(ss) = node.as_sample_scan() {
        ExplainScanTarget(ss.scan.scanrelid, es)?;
    }
    if let Some(ts) = node.as_tid_scan() {
        ExplainScanTarget(ts.scan.scanrelid, es)?;
    }
    if let Some(trs) = node.as_tid_range_scan() {
        ExplainScanTarget(trs.scan.scanrelid, es)?;
    }
    // ExplainModifyTarget.
    if let Some(mt) = node.as_modify_table() {
        ExplainTargetRel(mt.nominalRelation, es)?;
    }
    if let Some(is) = node.as_index_scan() {
        ExplainIndexScanDetails(is.indexid, is.indexorderdir, es)?;
        ExplainScanTarget(is.scan.scanrelid, es)?;
    }
    if let Some(ios) = node.as_index_only_scan() {
        ExplainIndexScanDetails(ios.indexid, ios.indexorderdir, es)?;
        ExplainScanTarget(ios.scan.scanrelid, es)?;
    }
    if let Some(bhs) = node.as_bitmap_heap_scan() {
        ExplainScanTarget(bhs.scan.scanrelid, es)?;
    }
    if let Some(bis) = node.as_bitmap_index_scan() {
        // explain_get_index_name arm: index name only.
        let mcx = es.str.allocator();
        let indexname = lsyscache::get_rel_name(mcx, bis.indexid)?
            .expect("explain_get_index_name: cache lookup failed");
        let indexname = str_in(mcx, indexname.as_str())?;
        if es.format == EXPLAIN_FORMAT_TEXT {
            append!(es, " on {}", quote_identifier(indexname));
        } else {
            ExplainPropertyText("Index Name", indexname, es);
        }
    }
    if node.node_tag() == NodeTag::T_CteScan {
        ExplainScanTarget(node.as_cte_scan().unwrap().scan.scanrelid, es)?;
    }
    // T_NamedTuplestoreScan is absent from C's scan-target dispatch
    // (explain.c:1655-1668): no " on <enrname>" suffix.
    if node.node_tag() == NodeTag::T_WorkTableScan {
        ExplainScanTarget(node.as_work_table_scan().unwrap().scan.scanrelid, es)?;
    }
    if node.node_tag() == NodeTag::T_ValuesScan {
        ExplainScanTarget(node.as_values_scan().unwrap().scan.scanrelid, es)?;
    }
    if let Some(fscan) = node.as_foreign_scan() {
        if fscan.scan.scanrelid > 0 {
            ExplainScanTarget(fscan.scan.scanrelid, es)?;
        }
    }
    if let Some(tfs) = node.as_table_func_scan() {
        ExplainTableFuncTarget(tfs, es)?;
    }
    if node.node_tag() == NodeTag::T_SubqueryScan {
        ExplainScanTarget(node.as_subquery_scan().unwrap().scan.scanrelid, es)?;
    }
    if let Some(fs) = node.as_function_scan() {
        ExplainFunctionTarget(fs, es)?;
    }

    if es.costs {
        if es.format == EXPLAIN_FORMAT_TEXT {
            append!(
                es,
                "  (cost={:.2}..{:.2} rows={:.0} width={})",
                plan.startup_cost,
                plan.total_cost,
                plan.plan_rows,
                plan.plan_width
            );
        } else {
            ExplainPropertyFloat("Startup Cost", None, plan.startup_cost, 2, es);
            ExplainPropertyFloat("Total Cost", None, plan.total_cost, 2, es);
            ExplainPropertyFloat("Plan Rows", None, plan.plan_rows, 0, es);
            ExplainPropertyInteger("Plan Width", None, plan.plan_width as i64, es);
        }
    }

    // C reads planstate->instrument; the walk here is over the sealed Plan
    // tree, so per-node Instrumentation comes from the executor keyed by
    // plan_node_id (the fetch also runs C's forced InstrEndLoop).
    let instrument = if es.qd.is_null() {
        None
    } else {
        execmain_seams::query_desc_instrument::call(es.qd, plan.plan_node_id)
    };
    // EA-on-morsels reports for this node (fetched once; the annotation
    // block below reuses it). A pipe whose root is NOT this node marks this
    // node as a bypassed MEMBER: its Instrumentation rows are exact but its
    // timing fields are deliberately zero — printing them would fake a time
    // (ea-morsels.md §7), so the actual-line drops its time= segment and
    // the member marker line explains where the time lives.
    let runtime_ea_pipes = if es.analyze && !es.qd.is_null() {
        execmain_seams::query_desc_runtime_ea_pipeline::call(es.qd, plan.plan_node_id)
    } else {
        None
    };
    let ea_member = runtime_ea_pipes
        .as_ref()
        .is_some_and(|ps| ps.iter().any(|p| p.root_node_id != plan.plan_node_id));
    let save_workers_state = es.workers_state.take();
    let worker_instrument = if es.analyze && !es.hide_workers && !es.qd.is_null() {
        execmain_seams::query_desc_worker_instrument::call(es.qd, plan.plan_node_id)
    } else {
        None
    };
    if let Some(w) = &worker_instrument {
        let mcx = es.str.allocator();
        let mut worker_inited = ::mcx::PgVec::new_in(mcx);
        worker_inited.resize(w.len(), false);
        let mut worker_str = ::mcx::PgVec::new_in(mcx);
        worker_str.resize_with(w.len(), || None);
        let mut worker_state_save = ::mcx::PgVec::new_in(mcx);
        worker_state_save.resize(w.len(), 0);
        es.workers_state = Some(crate::state::WorkersState {
            num_workers: w.len(),
            worker_inited,
            worker_str,
            worker_state_save,
        });
    }

    match instrument {
        Some(i) if es.analyze && i.nloops > 0.0 => {
            let nloops = i.nloops;
            let startup_ms = 1000.0 * i.startup / nloops;
            let total_ms = 1000.0 * i.total / nloops;
            let rows = i.ntuples / nloops;
            if es.format == EXPLAIN_FORMAT_TEXT {
                append!(es, " (actual ");
                if es.timing && !ea_member {
                    append!(es, "time={startup_ms:.3}..{total_ms:.3} ");
                }
                append!(es, "rows={rows:.2} loops={nloops:.0})");
            } else {
                if es.timing && !ea_member {
                    ExplainPropertyFloat("Actual Startup Time", Some("ms"), startup_ms, 3, es);
                    ExplainPropertyFloat("Actual Total Time", Some("ms"), total_ms, 3, es);
                }
                ExplainPropertyFloat("Actual Rows", None, rows, 2, es);
                ExplainPropertyFloat("Actual Loops", None, nloops, 0, es);
            }
        }
        _ if es.analyze => {
            if es.format == EXPLAIN_FORMAT_TEXT {
                append!(es, " (never executed)");
            } else {
                if es.timing {
                    ExplainPropertyFloat("Actual Startup Time", Some("ms"), 0.0, 3, es);
                    ExplainPropertyFloat("Actual Total Time", Some("ms"), 0.0, 3, es);
                }
                ExplainPropertyFloat("Actual Rows", None, 0.0, 0, es);
                ExplainPropertyFloat("Actual Loops", None, 0.0, 0, es);
            }
        }
        _ => {}
    }
    // in text format, first line ends here
    if es.format == EXPLAIN_FORMAT_TEXT {
        append!(es, "\n");
    }

    let isdisabled = plan_is_disabled(node);
    if es.format != EXPLAIN_FORMAT_TEXT || isdisabled {
        ExplainPropertyBool("Disabled", isdisabled, es);
    }

    // Per-worker general execution details.
    if es.workers_state.is_some() && es.verbose {
        let w = worker_instrument.as_ref().expect("workers_state implies worker data");
        for (n, i) in w.iter().enumerate() {
            let nloops = i.nloops;
            if nloops <= 0.0 {
                continue;
            }
            let startup_ms = 1000.0 * i.startup / nloops;
            let total_ms = 1000.0 * i.total / nloops;
            let rows = i.ntuples / nloops;
            explain_open_worker(n, es);
            if es.format == EXPLAIN_FORMAT_TEXT {
                ExplainIndentText(es);
                append!(es, "actual ");
                if es.timing {
                    append!(es, "time={startup_ms:.3}..{total_ms:.3} ");
                }
                append!(es, "rows={rows:.2} loops={nloops:.0}\n");
            } else {
                if es.timing {
                    ExplainPropertyFloat("Actual Startup Time", Some("ms"), startup_ms, 3, es);
                    ExplainPropertyFloat("Actual Total Time", Some("ms"), total_ms, 3, es);
                }
                ExplainPropertyFloat("Actual Rows", None, rows, 2, es);
                ExplainPropertyFloat("Actual Loops", None, nloops, 0, es);
            }
            explain_close_worker(n, es);
        }
    }

    if es.verbose {
        show_plan_tlist(node, ancestors, es)?;
    }

    // C: "try not to be too chatty about this in text mode".
    if let Some(inner_unique) = match node.node_tag() {
        NodeTag::T_NestLoop => Some(node.as_nest_loop().unwrap().join.inner_unique),
        NodeTag::T_MergeJoin => Some(node.as_merge_join().unwrap().join.inner_unique),
        NodeTag::T_HashJoin => Some(node.as_hash_join().unwrap().join.inner_unique),
        _ => None,
    } {
        if es.format != EXPLAIN_FORMAT_TEXT || (es.verbose && inner_unique) {
            ExplainPropertyBool("Inner Unique", inner_unique, es);
        }
    }

    match node.node_tag() {
        NodeTag::T_SampleScan => {
            show_tablesample(node, ancestors, es)?;
            show_scan_qual(&plan.qual, "Filter", node, ancestors, es)?;
            if !plan.qual.is_nil() {
                show_instrumentation_count("Rows Removed by Filter", 1, &instrument, es);
            }
        }
        NodeTag::T_SeqScan
        | NodeTag::T_CteScan
        | NodeTag::T_NamedTuplestoreScan
        | NodeTag::T_ValuesScan
        | NodeTag::T_WorkTableScan
        | NodeTag::T_TableFuncScan => {
            if node.node_tag() == NodeTag::T_TableFuncScan && es.verbose {
                let tablefunc = node
                    .as_table_func_scan()
                    .unwrap()
                    .tablefunc
                    .expect("TableFuncScan has a tablefunc");
                let mcx = es.str.allocator();
                let mut buf = PgString::new_in(mcx);
                deparse_expr(es, node, ancestors, tablefunc, true, false, &mut buf)?;
                crate::format::ExplainPropertyText("Table Function Call", buf.as_str(), es);
            }
            show_scan_qual(&plan.qual, "Filter", node, ancestors, es)?;
            if !plan.qual.is_nil() {
                show_instrumentation_count("Rows Removed by Filter", 1, &instrument, es);
            }
            if node.node_tag() == NodeTag::T_CteScan {
                show_ctescan_info(node, es);
            }
        }
        NodeTag::T_ForeignScan => {
            show_scan_qual(&plan.qual, "Filter", node, ancestors, es)?;
            if !plan.qual.is_nil() {
                show_instrumentation_count("Rows Removed by Filter", 1, &instrument, es);
            }
            show_foreignscan_info(plan.plan_node_id, es)?;
        }
        NodeTag::T_FunctionScan => {
            if es.verbose {
                let fs = node.as_function_scan().unwrap();
                let mcx = es.str.allocator();
                let mut buf = PgString::new_in(mcx);
                for (i, f) in fs.functions.iter().enumerate() {
                    if i > 0 {
                        buf.try_push_str(", ")?;
                    }
                    let fexpr = f
                        .as_range_tbl_function()
                        .expect("functions cell")
                        .funcexpr
                        .expect("RangeTblFunction has a funcexpr");
                    deparse_expr(es, node, ancestors, fexpr, true, false, &mut buf)?;
                }
                crate::format::ExplainPropertyText("Function Call", buf.as_str(), es);
            }
            show_scan_qual(&plan.qual, "Filter", node, ancestors, es)?;
            if !plan.qual.is_nil() {
                show_instrumentation_count("Rows Removed by Filter", 1, &instrument, es);
            }
        }
        NodeTag::T_BitmapIndexScan => {
            let s = node.as_bitmap_index_scan().unwrap();
            show_scan_qual(&s.indexqualorig, "Index Cond", node, ancestors, es)?;
            show_indexsearches_info(node, es);
        }
        NodeTag::T_TidScan => {
            // tidquals has OR semantics: multiple entries display as one OR.
            let s = node.as_tid_scan().unwrap();
            let tidquals = wrap_multi_quals(es, &s.tidquals, BoolExprType::OR_EXPR)?;
            show_scan_qual(&tidquals, "TID Cond", node, ancestors, es)?;
            show_scan_qual(&plan.qual, "Filter", node, ancestors, es)?;
            if !plan.qual.is_nil() {
                show_instrumentation_count("Rows Removed by Filter", 1, &instrument, es);
            }
        }
        NodeTag::T_TidRangeScan => {
            let s = node.as_tid_range_scan().unwrap();
            let tidquals = wrap_multi_quals(es, &s.tidrangequals, BoolExprType::AND_EXPR)?;
            show_scan_qual(&tidquals, "TID Cond", node, ancestors, es)?;
            show_scan_qual(&plan.qual, "Filter", node, ancestors, es)?;
            if !plan.qual.is_nil() {
                show_instrumentation_count("Rows Removed by Filter", 1, &instrument, es);
            }
        }
        NodeTag::T_BitmapHeapScan => {
            let s = node.as_bitmap_heap_scan().unwrap();
            show_scan_qual(&s.bitmapqualorig, "Recheck Cond", node, ancestors, es)?;
            if !s.bitmapqualorig.is_nil() {
                show_instrumentation_count("Rows Removed by Index Recheck", 2, &instrument, es);
            }
            show_scan_qual(&plan.qual, "Filter", node, ancestors, es)?;
            if !plan.qual.is_nil() {
                show_instrumentation_count("Rows Removed by Filter", 1, &instrument, es);
            }
            show_tidbitmap_info(node, es);
        }
        NodeTag::T_IndexScan => {
            let s = node.as_index_scan().unwrap();
            show_scan_qual(&s.indexqualorig, "Index Cond", node, ancestors, es)?;
            if !s.indexqualorig.is_nil() {
                show_instrumentation_count("Rows Removed by Index Recheck", 2, &instrument, es);
            }
            show_scan_qual(&s.indexorderbyorig, "Order By", node, ancestors, es)?;
            show_scan_qual(&plan.qual, "Filter", node, ancestors, es)?;
            if !plan.qual.is_nil() {
                show_instrumentation_count("Rows Removed by Filter", 1, &instrument, es);
            }
            show_indexsearches_info(node, es);
        }
        NodeTag::T_IndexOnlyScan => {
            let s = node.as_index_only_scan().unwrap();
            show_scan_qual(&s.indexqual, "Index Cond", node, ancestors, es)?;
            if !s.recheckqual.is_nil() {
                show_instrumentation_count("Rows Removed by Index Recheck", 2, &instrument, es);
            }
            show_scan_qual(&s.indexorderby, "Order By", node, ancestors, es)?;
            show_scan_qual(&plan.qual, "Filter", node, ancestors, es)?;
            if !plan.qual.is_nil() {
                show_instrumentation_count("Rows Removed by Filter", 1, &instrument, es);
            }
            if es.analyze {
                crate::format::ExplainPropertyFloat(
                    "Heap Fetches",
                    None,
                    instrument.as_ref().map_or(0.0, |i| i.ntuples2),
                    0,
                    es,
                );
            }
            show_indexsearches_info(node, es);
        }
        NodeTag::T_NestLoop => {
            let nl = node.as_nest_loop().unwrap();
            show_upper_qual(&nl.join.joinqual, "Join Filter", node, ancestors, es)?;
            if !nl.join.joinqual.is_nil() {
                show_instrumentation_count("Rows Removed by Join Filter", 1, &instrument, es);
            }
            show_upper_qual(&plan.qual, "Filter", node, ancestors, es)?;
            if !plan.qual.is_nil() {
                show_instrumentation_count("Rows Removed by Filter", 2, &instrument, es);
            }
        }
        NodeTag::T_HashJoin => {
            let hj = node.as_hash_join().unwrap();
            show_upper_qual(&hj.hashclauses, "Hash Cond", node, ancestors, es)?;
            show_upper_qual(&hj.join.joinqual, "Join Filter", node, ancestors, es)?;
            if !hj.join.joinqual.is_nil() {
                show_instrumentation_count("Rows Removed by Join Filter", 1, &instrument, es);
            }
            show_upper_qual(&plan.qual, "Filter", node, ancestors, es)?;
            if !plan.qual.is_nil() {
                show_instrumentation_count("Rows Removed by Filter", 2, &instrument, es);
            }
        }
        NodeTag::T_MergeJoin => {
            let mj = node.as_merge_join().unwrap();
            show_upper_qual(&mj.mergeclauses, "Merge Cond", node, ancestors, es)?;
            show_upper_qual(&mj.join.joinqual, "Join Filter", node, ancestors, es)?;
            if !mj.join.joinqual.is_nil() {
                show_instrumentation_count("Rows Removed by Join Filter", 1, &instrument, es);
            }
            show_upper_qual(&plan.qual, "Filter", node, ancestors, es)?;
            if !plan.qual.is_nil() {
                show_instrumentation_count("Rows Removed by Filter", 2, &instrument, es);
            }
        }
        NodeTag::T_Hash => {
            show_hash_info(node, es)?;
        }
        NodeTag::T_Gather => {
            let gather = node.as_gather().unwrap();
            show_scan_qual(&plan.qual, "Filter", node, ancestors, es)?;
            if !plan.qual.is_nil() {
                show_instrumentation_count("Rows Removed by Filter", 1, &instrument, es);
            }
            ExplainPropertyInteger("Workers Planned", None, gather.num_workers as i64, es);
            if es.analyze {
                let nworkers = if es.qd.is_null() {
                    0
                } else {
                    execmain_seams::query_desc_workers_launched::call(es.qd, plan.plan_node_id)
                        .unwrap_or(0)
                };
                ExplainPropertyInteger("Workers Launched", None, nworkers as i64, es);
            }
            if gather.single_copy || es.format != EXPLAIN_FORMAT_TEXT {
                ExplainPropertyBool("Single Copy", gather.single_copy, es);
            }
        }
        NodeTag::T_GatherMerge => {
            let gm = node.as_gather_merge().unwrap();
            show_scan_qual(&plan.qual, "Filter", node, ancestors, es)?;
            if !plan.qual.is_nil() {
                show_instrumentation_count("Rows Removed by Filter", 1, &instrument, es);
            }
            ExplainPropertyInteger("Workers Planned", None, gm.num_workers as i64, es);
            if es.analyze {
                let nworkers = if es.qd.is_null() {
                    0
                } else {
                    execmain_seams::query_desc_workers_launched::call(es.qd, plan.plan_node_id)
                        .unwrap_or(0)
                };
                ExplainPropertyInteger("Workers Launched", None, nworkers as i64, es);
            }
        }
        NodeTag::T_Result => {
            if let Some(q) = node.as_result().unwrap().resconstantqual {
                show_one_time_filter(q, node, ancestors, es)?;
            }
            show_scan_qual(&plan.qual, "Filter", node, ancestors, es)?;
            filtered_count_gap(&plan.qual, es);
        }
        NodeTag::T_Sort => {
            show_sort_keys(node, ancestors, es)?;
            show_sort_info(node, es)?;
        }
        NodeTag::T_MergeAppend => {
            show_merge_append_keys(node, ancestors, es)?;
        }
        NodeTag::T_IncrementalSort => {
            show_incremental_sort_keys(node, ancestors, es)?;
            show_incremental_sort_info(node, es)?;
        }
        NodeTag::T_WindowAgg => {
            show_window_def(node, ancestors, es)?;
            let w = node.as_window_agg().unwrap();
            show_upper_qual(&w.runConditionOrig, "Run Condition", node, ancestors, es)?;
            show_upper_qual(&plan.qual, "Filter", node, ancestors, es)?;
            filtered_count_gap(&plan.qual, es);
            show_windowagg_info(node, es);
        }
        NodeTag::T_Agg => {
            show_agg_keys(node, ancestors, es)?;
            show_upper_qual(&plan.qual, "Filter", node, ancestors, es)?;
            show_hashagg_info(node, es)?;
            if !plan.qual.is_nil() {
                show_instrumentation_count("Rows Removed by Filter", 1, &instrument, es);
            }
        }
        NodeTag::T_Group => {
            show_group_keys(node, ancestors, es)?;
            show_upper_qual(&plan.qual, "Filter", node, ancestors, es)?;
            filtered_count_gap(&plan.qual, es);
        }
        NodeTag::T_Material => {
            show_material_info(node, es);
        }
        NodeTag::T_Memoize => {
            show_memoize_info(node, ancestors, es)?;
        }
        NodeTag::T_SubqueryScan => {
            show_scan_qual(&plan.qual, "Filter", node, ancestors, es)?;
            if !plan.qual.is_nil() {
                show_instrumentation_count("Rows Removed by Filter", 1, &instrument, es);
            }
        }
        // Unique, Limit, Append, SetOp, LockRows, ProjectSet show nothing
        // extra without ANALYZE.
        NodeTag::T_Unique | NodeTag::T_Limit | NodeTag::T_Append | NodeTag::T_SetOp
        | NodeTag::T_LockRows | NodeTag::T_BitmapAnd | NodeTag::T_BitmapOr
        | NodeTag::T_ProjectSet | NodeTag::T_RecursiveUnion => {}
        // show_modifytable_info: FDW/ON CONFLICT legs absent (asserted at the
        // name arm). C reads mtstate->resultRelInfo; the filter below rebuilds
        // it from the plan list minus initially-pruned rels, keeping the first
        // if all were pruned (nodeModifyTable.c:4676).
        NodeTag::T_ModifyTable => {
            let mt = node.as_modify_table().unwrap();
            let unpruned = |rti: i32| -> bool {
                es.qd.is_null()
                    || execmain_seams::query_desc_rti_unpruned::call(es.qd, rti).unwrap_or(true)
            };
            let total_nrels = mt.resultRelations.len();
            let mut result_rtis: Vec<i32> = Vec::with_capacity(total_nrels);
            for (i, rti) in mt.resultRelations.iter().enumerate() {
                if unpruned(rti) {
                    result_rtis.push(rti);
                } else if i == total_nrels - 1 && result_rtis.is_empty() {
                    result_rtis.push(mt.resultRelations.nth(0));
                }
            }
            let nrels = result_rtis.len();
            let labeltargets = nrels > 1
                || (nrels == 1
                    && result_rtis[0] != mt.nominalRelation as i32
                    && unpruned(result_rtis[0]));
            if labeltargets {
                let opname = match mt.operation {
                    types_nodes::CmdType::CMD_INSERT => "Insert",
                    types_nodes::CmdType::CMD_UPDATE => "Update",
                    types_nodes::CmdType::CMD_DELETE => "Delete",
                    types_nodes::CmdType::CMD_MERGE => "Merge",
                    _ => "???",
                };
                for &rti in &result_rtis {
                    crate::format::ExplainIndentText(es);
                    append!(es, "{opname}");
                    ExplainTargetRel(rti as types_core::Index, es)?;
                    append!(es, "\n");
                }
            }
            // ON CONFLICT stanza (show_modifytable_info, explain.c:4632).
            if mt.onConflictAction != 0 {
                let resolution = if mt.onConflictAction
                    == types_nodes::OnConflictAction::ONCONFLICT_NOTHING as u32
                {
                    "NOTHING"
                } else {
                    "UPDATE"
                };
                ExplainPropertyText("Conflict Resolution", resolution, es);
                let mcx = es.str.allocator();
                let mut idx_names: Vec<std::string::String> = Vec::new();
                for oid in mt.arbiterIndexes.iter() {
                    let name = lsyscache::get_rel_name(mcx, oid)?
                        .map(|n| n.as_str().to_string())
                        .unwrap_or_default();
                    idx_names.push(name);
                }
                if !idx_names.is_empty() {
                    crate::format::ExplainPropertyList("Conflict Arbiter Indexes", &idx_names, es);
                }
                if let Some(w) = mt.onConflictWhere {
                    // onConflictWhere is an implicit-AND List after
                    // preprocessing (C casts it straight to List).
                    let qual = match w.as_list() {
                        Some(l) => l.clone_in(mcx)?,
                        None => {
                            let mut q = NodeList::nil();
                            q.lappend(mcx, w)?;
                            q
                        }
                    };
                    show_upper_qual(&qual, "Conflict Filter", node, ancestors, es)?;
                    // C also prints "Rows Removed by Conflict Filter" under
                    // ANALYZE; the executor doesn't count nfiltered there yet.
                    filtered_count_gap(&qual, es);
                }
                if es.analyze {
                    node_gap(
                        "show_modifytable_info",
                        "ON CONFLICT Tuples Inserted/Conflicting Tuples need ntuples2 \
                         accounting (nodeModifyTable instrument)",
                    );
                }
            }
            // MERGE Tuples: line (show_modifytable_info, explain.c:4681-4726);
            // total = outer plan's ntuples, skipped derived.
            if mt.operation == types_nodes::CmdType::CMD_MERGE
                && es.analyze
                && instrument.is_some()
                && !es.qd.is_null()
            {
                if let Some((inserted, updated, deleted)) =
                    execmain_seams::query_desc_merge_instrument::call(es.qd, plan.plan_node_id)
                {
                    let total = plan
                        .lefttree
                        .and_then(|l| {
                            execmain_seams::query_desc_instrument::call(
                                es.qd,
                                plan_of(l).plan_node_id,
                            )
                        })
                        .map_or(0.0, |i| i.ntuples);
                    let skipped = total - inserted - updated - deleted;
                    if es.format == EXPLAIN_FORMAT_TEXT {
                        if total > 0.0 {
                            crate::format::ExplainIndentText(es);
                            append!(es, "Tuples:");
                            if inserted > 0.0 {
                                append!(es, " inserted={inserted:.0}");
                            }
                            if updated > 0.0 {
                                append!(es, " updated={updated:.0}");
                            }
                            if deleted > 0.0 {
                                append!(es, " deleted={deleted:.0}");
                            }
                            if skipped > 0.0 {
                                append!(es, " skipped={skipped:.0}");
                            }
                            append!(es, "\n");
                        }
                    } else {
                        crate::format::ExplainPropertyFloat("Tuples Inserted", None, inserted, 0, es);
                        crate::format::ExplainPropertyFloat("Tuples Updated", None, updated, 0, es);
                        crate::format::ExplainPropertyFloat("Tuples Deleted", None, deleted, 0, es);
                        crate::format::ExplainPropertyFloat("Tuples Skipped", None, skipped, 0, es);
                    }
                }
            }
        }
        _ => unreachable!(),
    }

    if es.buffers {
        if let Some(i) = &instrument {
            crate::show_buffer_usage(es, &i.bufusage);
        }
    }

    // EA-on-morsels refusal transparency (docs/design/ea-morsels.md §6): the
    // runtime admission walk's verdict for a node that did not engage.
    // Records exist ONLY on armed + instrumented walks — unarmed sessions
    // (every existing gate: the C-diff explain e2e, regress) print nothing
    // and stay byte-identical.
    if es.analyze && !es.qd.is_null() {
        if let Some(refs) =
            execmain_seams::query_desc_runtime_ea_refusals::call(es.qd, plan.plan_node_id)
        {
            if es.format == EXPLAIN_FORMAT_TEXT {
                crate::format::ExplainIndentText(es);
                append!(es, "runtime: refused (");
                for (i, (arm, reason)) in refs.iter().enumerate() {
                    if i > 0 {
                        append!(es, "; ");
                    }
                    append!(es, "{arm}: {reason}");
                }
                append!(es, ")\n");
            } else {
                for (arm, reason) in &refs {
                    crate::format::ExplainPropertyText(
                        "Runtime Refused",
                        &format!("{arm}: {reason}"),
                        es,
                    );
                }
            }
        }
    }

    // EA-on-morsels pipeline blocks (docs/design/ea-morsels.md §4): the
    // engaged runtime pipeline reports at measured granularity — the full
    // block on the pipeline's breaker (root) node, a marker line on
    // bypassed member nodes. Reports exist ONLY on armed + instrumented
    // engagements (same emission-gate law as the refusal line); TIMING OFF
    // reports carry no time-derived lines (zero is not a time).
    if es.analyze && !es.qd.is_null() {
        if let Some(pipes) = &runtime_ea_pipes {
            let mut member_marked = false;
            for p in pipes {
                if p.root_node_id == plan.plan_node_id {
                    let role = p.role.to_ascii_uppercase();
                    if es.format == EXPLAIN_FORMAT_TEXT {
                        let (arm, ti, tc) = (p.arm, p.taskset_index, p.taskset_count);
                        crate::format::ExplainIndentText(es);
                        append!(es, "Runtime Pipeline: {arm} (task set {ti}/{tc} {role})\n");
                        let workers = p.workers;
                        crate::format::ExplainIndentText(es);
                        append!(es, "  Workers: {workers} participated\n");
                        let (claims, granules) = (p.claims, p.granules);
                        crate::format::ExplainIndentText(es);
                        append!(es, "  Morsels: {claims} claims, {granules} granules");
                        // TIMER mode only (zero is not a time — §7).
                        if p.last_end_ns != 0 && p.claims != 0 {
                            let avg_ms = p.busy_ns as f64 / p.claims as f64 / 1e6;
                            let spread_ms =
                                p.last_end_ns.saturating_sub(p.min_last_end_ns) as f64 / 1e6;
                            append!(es, ", avg {avg_ms:.3}ms, spread {spread_ms:.3}ms");
                        }
                        append!(es, "\n");
                        let (rs, rf, np) = (p.rows_scanned, p.rows_survived, p.partials);
                        crate::format::ExplainIndentText(es);
                        append!(es, "  Rows: {rs} scanned -> {rf} filtered -> {np} partials\n");
                        let (pruned, bloom, meta, windows) =
                            (p.prune[1], p.prune[2], p.prune[3], p.prune[6]);
                        crate::format::ExplainIndentText(es);
                        append!(
                            es,
                            "  Scan: {pruned} granules pruned ({bloom} bloom), \
                             {meta} meta-answered, {windows} windows staged\n"
                        );
                        // TIMER mode only: wall/cpu/DOP/stalls at PIPELINE
                        // granularity — the measured boundary (§4).
                        if p.last_end_ns != 0 && p.first_start_ns != 0 {
                            let wall_ns = p.last_end_ns.saturating_sub(p.first_start_ns);
                            if wall_ns != 0 {
                                let wall_ms = wall_ns as f64 / 1e6;
                                let cpu_ms = p.busy_ns as f64 / 1e6;
                                let dop = p.busy_ns as f64 / wall_ns as f64;
                                let stall_ms = ((p.workers as u64 * wall_ns)
                                    .saturating_sub(p.busy_ns))
                                    as f64
                                    / 1e6;
                                crate::format::ExplainIndentText(es);
                                append!(
                                    es,
                                    "  Time: wall {wall_ms:.3}ms, cpu {cpu_ms:.3}ms, \
                                     effective DOP {dop:.1}, stalls {stall_ms:.3}ms\n"
                                );
                            }
                        }
                    } else {
                        crate::format::ExplainOpenGroup(
                            "Runtime Pipeline",
                            Some("Runtime Pipeline"),
                            true,
                            es,
                        );
                        crate::format::ExplainPropertyText("Arm", p.arm, es);
                        crate::format::ExplainPropertyText("Role", &role, es);
                        ExplainPropertyUInteger("Task Set", None, p.taskset_index as u64, es);
                        ExplainPropertyUInteger("Task Sets", None, p.taskset_count as u64, es);
                        ExplainPropertyUInteger("Workers", None, p.workers as u64, es);
                        ExplainPropertyUInteger("Claims", None, p.claims, es);
                        ExplainPropertyUInteger("Granules", None, p.granules, es);
                        ExplainPropertyUInteger("Rows Scanned", None, p.rows_scanned, es);
                        ExplainPropertyUInteger("Rows Filtered", None, p.rows_survived, es);
                        ExplainPropertyUInteger("Partials", None, p.partials, es);
                        ExplainPropertyUInteger("Granules Pruned", None, p.prune[1], es);
                        ExplainPropertyUInteger("Bloom Pruned", None, p.prune[2], es);
                        ExplainPropertyUInteger("Meta Answered", None, p.prune[3], es);
                        ExplainPropertyUInteger("Windows Staged", None, p.prune[6], es);
                        // TIMER mode only (absent under TIMING OFF — zero
                        // is not a time, §7).
                        if p.last_end_ns != 0 && p.first_start_ns != 0 && p.claims != 0 {
                            let wall_ns = p.last_end_ns.saturating_sub(p.first_start_ns);
                            ExplainPropertyFloat(
                                "Avg Morsel Time",
                                Some("ms"),
                                p.busy_ns as f64 / p.claims as f64 / 1e6,
                                3,
                                es,
                            );
                            ExplainPropertyFloat(
                                "Finish Spread",
                                Some("ms"),
                                p.last_end_ns.saturating_sub(p.min_last_end_ns) as f64 / 1e6,
                                3,
                                es,
                            );
                            if wall_ns != 0 {
                                ExplainPropertyFloat(
                                    "Wall Time",
                                    Some("ms"),
                                    wall_ns as f64 / 1e6,
                                    3,
                                    es,
                                );
                                ExplainPropertyFloat(
                                    "CPU Time",
                                    Some("ms"),
                                    p.busy_ns as f64 / 1e6,
                                    3,
                                    es,
                                );
                                ExplainPropertyFloat(
                                    "Effective DOP",
                                    None,
                                    p.busy_ns as f64 / wall_ns as f64,
                                    1,
                                    es,
                                );
                                ExplainPropertyFloat(
                                    "Stall Time",
                                    Some("ms"),
                                    ((p.workers as u64 * wall_ns).saturating_sub(p.busy_ns))
                                        as f64
                                        / 1e6,
                                    3,
                                    es,
                                );
                            }
                        }
                        crate::format::ExplainCloseGroup(
                            "Runtime Pipeline",
                            Some("Runtime Pipeline"),
                            true,
                            es,
                        );
                    }
                } else if !member_marked {
                    member_marked = true;
                    if es.format == EXPLAIN_FORMAT_TEXT {
                        crate::format::ExplainIndentText(es);
                        let arm = p.arm;
                        append!(
                            es,
                            "runtime: member of {arm} pipeline \
                             (times at pipeline granularity)\n"
                        );
                    } else {
                        crate::format::ExplainPropertyText(
                            "Runtime Member",
                            &format!("{} pipeline (times at pipeline granularity)", p.arm),
                            es,
                        );
                    }
                }
            }
        }
    }

    // EXPLAIN (ENGINE) (single-executor migration Phase 0.2): per-node
    // engine attribution. Total by construction — an engaged runtime
    // pipeline is derived from the already-fetched runtime_ea_pipes
    // (root/member match), lane/spine verdicts come from the capture
    // records, and an event-less node is the row spine (the default truth,
    // zero extra capture). Double-gated: display needs the option AND the
    // records exist only under EXEC_FLAG_ENGINE_REPORT, so default output
    // stays byte-identical.
    if es.engine && !es.qd.is_null() {
        use ::types_core::instrument::EngineKindWire;
        let runtime = runtime_ea_pipes
            .as_ref()
            .and_then(|ps| ps.first())
            .map(|p| (p.arm, p.root_node_id == plan.plan_node_id));
        let (engine_txt, detail): (&str, String) = if let Some((arm, is_root)) = runtime {
            let d = if is_root {
                format!("{arm} arm")
            } else {
                format!("member of {arm} pipeline")
            };
            ("runtime", d)
        } else {
            let ev = execmain_seams::query_desc_engine_events::call(es.qd, plan.plan_node_id)
                .and_then(|v| v.first().copied());
            match ev {
                Some((EngineKindWire::Lane, class, _)) => ("lane", class.to_string()),
                Some((EngineKindWire::FusedArm, class, _)) => {
                    ("spine/fused-arm", class.to_string())
                }
                Some((EngineKindWire::Runtime, class, _)) => ("runtime", class.to_string()),
                // "instrumented" = RefuseReason::Instrumented.name()
                // (lanev2/stats.rs): the fused serial pipelines record their
                // observed refusal in inc-1; only this reason means "the
                // production verdict was not evaluated", hence the suffix.
                Some((EngineKindWire::Spine, class, "instrumented")) => (
                    "spine",
                    format!("{class}: refused instrumented; production engine may differ"),
                ),
                Some((EngineKindWire::Spine, class, d)) if !d.is_empty() => {
                    ("spine", format!("{class}: {d}"))
                }
                Some((EngineKindWire::Spine, class, _)) => ("spine", class.to_string()),
                None => ("spine", String::new()),
            }
        };
        if es.format == EXPLAIN_FORMAT_TEXT {
            crate::format::ExplainIndentText(es);
            if detail.is_empty() {
                append!(es, "Engine: {engine_txt}\n");
            } else {
                append!(es, "Engine: {engine_txt} ({detail})\n");
            }
        } else {
            crate::format::ExplainPropertyText("Engine", engine_txt, es);
            if !detail.is_empty() {
                crate::format::ExplainPropertyText("Engine Detail", &detail, es);
            }
        }
    }

    // Per-worker buffer usage, then flush the worker
    // sections and pop the set-aside state.
    if es.workers_state.is_some() && es.buffers && es.verbose {
        let w = worker_instrument.as_ref().expect("workers_state implies worker data");
        for (n, i) in w.iter().enumerate() {
            if i.nloops <= 0.0 {
                continue;
            }
            explain_open_worker(n, es);
            crate::show_buffer_usage(es, &i.bufusage);
            explain_close_worker(n, es);
        }
    }
    if es.workers_state.is_some() {
        explain_flush_workers_state(es);
    }
    es.workers_state = save_workers_state;

    if let Some(hook) = crate::state::explain_per_node_hook() {
        hook(node, relationship, plan_name, es)?;
    }

    // ExplainMissingMembers: subplans removed by initial runtime pruning.
    let append_children = match node.as_append() {
        Some(a) => Some((&a.appendplans, a.part_prune_index)),
        None => node.as_merge_append().map(|m| (&m.mergeplans, m.part_prune_index)),
    };
    let append_valid: Option<Vec<i32>> = match append_children {
        Some((_, ppi)) if ppi >= 0 && !es.qd.is_null() => {
            execmain_seams::query_desc_prune_result::call(es.qd, ppi)
        }
        _ => None,
    };
    if let Some((children, _)) = append_children {
        let nchildren = children.len() as i64;
        let nplans = append_valid.as_ref().map_or(nchildren, |v| v.len() as i64);
        if nplans < nchildren || es.format != EXPLAIN_FORMAT_TEXT {
            ExplainPropertyInteger("Subplans Removed", None, nchildren - nplans, es);
        }
    }

    let pushed = Ancestors { entry: AncestorEntry::Plan(node), parent: ancestors };
    let member_subplans = {
        let pstmt = es.pstmt.expect("ExplainNode before ExplainPrintPlan");
        let spctx =
            SubPlanScanCtx { analyze: es.analyze, part_prune_infos: &pstmt.partPruneInfos };
        collect_node_subplans(es.str.allocator(), node, spctx)?
    };
    let haschildren = !plan.initPlan.is_nil()
        || plan.lefttree.is_some()
        || plan.righttree.is_some()
        || node.node_tag() == NodeTag::T_Append
        || node.node_tag() == NodeTag::T_MergeAppend
        || node.node_tag() == NodeTag::T_SubqueryScan
        || node.node_tag() == NodeTag::T_BitmapAnd
        || node.node_tag() == NodeTag::T_BitmapOr
        || !member_subplans.is_empty();
    if haschildren {
        ExplainOpenGroup("Plans", Some("Plans"), false, es);
    }
    // ExplainSubPlans over initPlan.
    for sp_node in plan.initPlan.iter() {
        let sp = sp_node.as_sub_plan().expect("initPlan holds SubPlan nodes");
        explain_sub_plan(sp, "InitPlan", &pushed, es)?;
    }
    if let Some(l) = plan.lefttree {
        ExplainNode(l, Some("Outer"), None, Some(&pushed), es)?;
    }
    if let Some(r) = plan.righttree {
        ExplainNode(r, Some("Inner"), None, Some(&pushed), es)?;
    }
    if let Some((children, _)) = append_children {
        match &append_valid {
            Some(valid) => {
                for &i in valid {
                    ExplainNode(children.nth(i as usize), Some("Member"), None, Some(&pushed), es)?;
                }
            }
            None => {
                for child in children {
                    ExplainNode(child, Some("Member"), None, Some(&pushed), es)?;
                }
            }
        }
    }
    // ExplainMemberNodes over the bitmap combiners' member lists.
    if let Some(ba) = node.as_bitmap_and() {
        for child in &ba.bitmapplans {
            ExplainNode(child, Some("Member"), None, Some(&pushed), es)?;
        }
    }
    if let Some(bo) = node.as_bitmap_or() {
        for child in &bo.bitmapplans {
            ExplainNode(child, Some("Member"), None, Some(&pushed), es)?;
        }
    }
    if let Some(sq) = node.as_subquery_scan() {
        ExplainNode(sq.subplan.expect("SubqueryScan subplan"), Some("Subquery"), None, Some(&pushed), es)?;
    }
    // ExplainSubPlans over planstate->subPlan.
    for sp in member_subplans.iter() {
        explain_sub_plan(sp, "SubPlan", &pushed, es)?;
    }
    if haschildren {
        ExplainCloseGroup("Plans", Some("Plans"), false, es);
    }

    // in text format, undo whatever indentation we added
    if es.format == EXPLAIN_FORMAT_TEXT {
        es.indent = save_indent;
    }
    ExplainCloseGroup("Plan", if relationship.is_some() { None } else { Some("Plan") }, true, es);
    Ok(())
}

fn explain_sub_plan<'mcx>(
    sp: &'mcx types_nodes::primnodes::SubPlan<'mcx>,
    relationship: &str,
    pushed: &Ancestors<'_, 'mcx>,
    es: &mut ExplainState<'mcx>,
) -> PgResult<()> {
    if es.printed_subplans.is_member(sp.plan_id) {
        return Ok(());
    }
    es.printed_subplans
        .add_member(es.str.allocator(), sp.plan_id)?;
    let child = es
        .pstmt
        .expect("ExplainNode before ExplainPrintPlan")
        .subplans
        .nth(sp.plan_id as usize - 1)
        .expect("EXPLAIN pstmt has no subplan holes");
    let sub = Ancestors { entry: AncestorEntry::Sub(sp), parent: Some(pushed) };
    ExplainNode(child, Some(relationship), sp.plan_name, Some(&sub), es)
}

fn show_plan_tlist<'mcx>(node: Node<'mcx>, ancestors: Option<&Ancestors<'_, 'mcx>>, es: &mut ExplainState<'mcx>) -> PgResult<()> {
    let plan = plan_of(node);
    if plan.targetlist.is_nil() {
        return Ok(());
    }
    if matches!(
        node.node_tag(),
        NodeTag::T_Append | NodeTag::T_MergeAppend | NodeTag::T_RecursiveUnion
    ) {
        return Ok(());
    }
    let mcx = es.str.allocator();
    let useprefix = es.rtable_size > 1;
    let mut result: PgVec<'mcx, PgString<'mcx>> = PgVec::new_in(mcx);
    for tle_node in plan.targetlist.iter() {
        let tle = tle_node.as_target_entry().expect("targetlist holds TargetEntries");
        let mut buf = PgString::new_in(mcx);
        deparse_expr(es, node, ancestors, tle.expr, useprefix, false, &mut buf)?;
        result.push(buf);
    }
    ExplainPropertyList("Output", &result, es);
    Ok(())
}

// show_sort_keys -> show_sort_group_keys (explain.c).
fn show_sort_keys<'mcx>(
    node: Node<'mcx>,
    ancestors: Option<&Ancestors<'_, 'mcx>>,
    es: &mut ExplainState<'mcx>,
) -> PgResult<()> {
    let sort = node.as_sort().expect("Sort node");
    show_sort_node_keys(node, sort, 0, ancestors, es)
}

fn show_incremental_sort_keys<'mcx>(
    node: Node<'mcx>,
    ancestors: Option<&Ancestors<'_, 'mcx>>,
    es: &mut ExplainState<'mcx>,
) -> PgResult<()> {
    let isort = node.as_incremental_sort().expect("IncrementalSort node");
    show_sort_node_keys(node, &isort.sort, isort.nPresortedCols as usize, ancestors, es)
}

fn show_sort_node_keys<'mcx>(
    node: Node<'mcx>,
    sort: &types_nodes::plannodes::Sort<'mcx>,
    n_presorted_keys: usize,
    ancestors: Option<&Ancestors<'_, 'mcx>>,
    es: &mut ExplainState<'mcx>,
) -> PgResult<()> {
    show_sort_group_keys(
        node,
        &sort.plan.targetlist,
        sort.numCols,
        sort.sortColIdx,
        sort.sortOperators,
        sort.collations,
        sort.nullsFirst,
        n_presorted_keys,
        ancestors,
        es,
    )
}

fn show_merge_append_keys<'mcx>(
    node: Node<'mcx>,
    ancestors: Option<&Ancestors<'_, 'mcx>>,
    es: &mut ExplainState<'mcx>,
) -> PgResult<()> {
    let m = node.as_merge_append().expect("MergeAppend node");
    show_sort_group_keys(
        node,
        &m.plan.targetlist,
        m.numCols,
        m.sortColIdx,
        m.sortOperators,
        m.collations,
        m.nullsFirst,
        0,
        ancestors,
        es,
    )
}

#[allow(clippy::too_many_arguments)]
fn show_sort_group_keys<'mcx>(
    node: Node<'mcx>,
    targetlist: &types_nodes::list::NodeList<'mcx>,
    num_cols: i32,
    sort_col_idx: &[i16],
    sort_operators: &[u32],
    collations: &[u32],
    nulls_first: &[bool],
    n_presorted_keys: usize,
    ancestors: Option<&Ancestors<'_, 'mcx>>,
    es: &mut ExplainState<'mcx>,
) -> PgResult<()> {
    if num_cols <= 0 {
        return Ok(());
    }
    let mcx = es.str.allocator();
    let useprefix = es.rtable_size > 1 || es.verbose;
    let mut result: PgVec<'mcx, PgString<'mcx>> = PgVec::new_in(mcx);
    let mut presorted: PgVec<'mcx, PgString<'mcx>> = PgVec::new_in(mcx);
    for keyno in 0..num_cols as usize {
        let resno = sort_col_idx[keyno];
        let tle = get_tle_by_resno(targetlist, resno)
            .unwrap_or_else(|| node_gap("show_sort_group_keys", "no tlist entry for key column"));
        let mut buf = PgString::new_in(mcx);
        deparse_expr(es, node, ancestors, tle.expr, useprefix, true, &mut buf)?;
        if keyno < n_presorted_keys {
            presorted.push(PgString::from_str_in(buf.as_str(), mcx)?);
        }
        show_sortorder_options(
            &mut buf,
            tle.expr,
            sort_operators[keyno],
            collations[keyno],
            nulls_first[keyno],
        )?;
        result.push(buf);
    }
    ExplainPropertyList("Sort Key", &result, es);
    if n_presorted_keys > 0 {
        ExplainPropertyList("Presorted Key", &presorted, es);
    }
    Ok(())
}

// show_sort_info (explain.c). spaceUsed value diverges from C (arena vs
// palloc accounting) — notes/sort-explain-lane.md.
fn show_sort_info<'mcx>(node: Node<'mcx>, es: &mut ExplainState<'mcx>) -> PgResult<()> {
    if !es.analyze || es.qd.is_null() {
        return Ok(());
    }
    let id = plan_of(node).plan_node_id;
    if let Some(si) = execmain_seams::query_desc_sort_instrument::call(es.qd, id) {
        let sort_method = si.sortMethod.name();
        let space_type = si.spaceType.name();
        if es.format == EXPLAIN_FORMAT_TEXT {
            crate::format::ExplainIndentText(es);
            append!(es, "Sort Method: {}  {}: {}kB\n", sort_method, space_type, si.spaceUsed);
        } else {
            ExplainPropertyText("Sort Method", sort_method, es);
            ExplainPropertyInteger("Sort Space Used", Some("kB"), si.spaceUsed, es);
            ExplainPropertyText("Sort Space Type", space_type, es);
        }
    }
    // The shared_info worker stanza; a worker slot exists only once its sort
    // completed (C skips SORT_TYPE_STILL_IN_PROGRESS).
    if let Some(workers) = execmain_seams::query_desc_worker_sort_instrument::call(es.qd, id) {
        for (n, si) in workers {
            if es.workers_state.is_some() {
                explain_open_worker(n as usize, es);
            }
            if es.format == EXPLAIN_FORMAT_TEXT {
                crate::format::ExplainIndentText(es);
                append!(
                    es,
                    "Sort Method: {}  {}: {}kB\n",
                    si.sortMethod.name(),
                    si.spaceType.name(),
                    si.spaceUsed
                );
            } else {
                ExplainPropertyText("Sort Method", si.sortMethod.name(), es);
                ExplainPropertyInteger("Sort Space Used", Some("kB"), si.spaceUsed, es);
                ExplainPropertyText("Sort Space Type", si.spaceType.name(), es);
            }
            if es.workers_state.is_some() {
                explain_close_worker(n as usize, es);
            }
        }
    }
    Ok(())
}

// ExplainOpenWorker (explain.c): swap in this worker's set-aside buffer; the
// slot holds the main buffer until the matching close.
fn explain_open_worker(n: usize, es: &mut ExplainState<'_>) {
    let mcx = es.str.allocator();
    let (mut buf, first_time) = {
        let ws = es.workers_state.as_mut().expect("open_worker without workers_state");
        let first = ws.worker_str[n].is_none();
        if first {
            ws.worker_str[n] =
                Some(stringinfo::StringInfo::new_in(mcx).expect("explain output append"));
            ws.worker_inited[n] = true;
        }
        (ws.worker_str[n].take().expect("worker buffer present"), first)
    };
    core::mem::swap(&mut es.str, &mut buf);
    es.workers_state.as_mut().unwrap().worker_str[n] = Some(buf);
    if first_time {
        // One extra nesting level: the group eventually nests in "Workers".
        crate::format::ExplainOpenSetAsideGroup("Worker", None, true, 2, es);
        if es.format != EXPLAIN_FORMAT_TEXT {
            ExplainPropertyInteger("Worker Number", None, n as i64, es);
        }
    } else {
        let saved =
            es.workers_state.as_ref().expect("workers_state").worker_state_save[n];
        crate::format::ExplainRestoreGroup(es, 2, &saved);
    }
    if es.format == EXPLAIN_FORMAT_TEXT {
        if es.str.is_empty() {
            ExplainIndentText(es);
            append!(es, "Worker {n}:  ");
        }
        es.indent += 1;
    }
}

// ExplainCloseWorker (explain.c): in text, drop an output-less "Worker N:"
// prefix; restore the main buffer.
fn explain_close_worker(n: usize, es: &mut ExplainState<'_>) {
    let mut saved = 0;
    crate::format::ExplainSaveGroup(es, 2, &mut saved);
    es.workers_state.as_mut().expect("workers_state").worker_state_save[n] = saved;
    if es.format == EXPLAIN_FORMAT_TEXT {
        let bytes = es.str.as_bytes();
        let mut len = bytes.len();
        while len > 0 && bytes[len - 1] != b'\n' {
            len -= 1;
        }
        es.str.truncate(len);
        es.indent -= 1;
    }
    let mut buf = {
        let ws = es.workers_state.as_mut().expect("close_worker without workers_state");
        ws.worker_str[n].take().expect("worker slot holds the main buffer")
    };
    core::mem::swap(&mut es.str, &mut buf);
    es.workers_state.as_mut().unwrap().worker_str[n] = Some(buf);
}

// ExplainFlushWorkersState (explain.c).
fn explain_flush_workers_state(es: &mut ExplainState<'_>) {
    let ws = es.workers_state.take().expect("flush without workers_state");
    ExplainOpenGroup("Workers", Some("Workers"), false, es);
    for (inited, buf) in ws.worker_inited.iter().zip(ws.worker_str.iter()) {
        if *inited {
            ExplainOpenGroup("Worker", None, true, es);
            if let Some(b) = buf {
                es.str.append_bytes(b.as_bytes()).expect("explain output append");
            }
            ExplainCloseGroup("Worker", None, true, es);
        }
    }
    ExplainCloseGroup("Workers", Some("Workers"), false, es);
}

// show_incremental_sort_group_info (explain.c). Memory values inherit the
// sort-info divergence (arena vs palloc accounting) —
// notes/sort-explain-lane.md.
fn show_incremental_sort_group_info(
    group_info: &types_core::instrument::IncrementalSortGroupInfo,
    group_label: &str,
    indent: bool,
    es: &mut ExplainState<'_>,
) {
    use types_core::instrument::TuplesortMethod;
    const METHOD_BITS: [TuplesortMethod; 4] = [
        TuplesortMethod::TopNHeapsort,
        TuplesortMethod::Quicksort,
        TuplesortMethod::ExternalSort,
        TuplesortMethod::ExternalMerge,
    ];
    let methods: Vec<&TuplesortMethod> =
        METHOD_BITS.iter().filter(|m| group_info.sortMethods & m.bit() != 0).collect();
    if es.format == EXPLAIN_FORMAT_TEXT {
        if indent {
            es.str
                .append_spaces(es.indent as usize * 2)
                .expect("explain output append");
        }
        append!(es, "{} Groups: {}  Sort Method", group_label, group_info.groupCount);
        append!(es, "{}", if methods.len() > 1 { "s: " } else { ": " });
        for (i, m) in methods.iter().enumerate() {
            if i > 0 {
                append!(es, ", ");
            }
            append!(es, "{}", m.name());
        }
        if group_info.maxMemorySpaceUsed > 0 {
            let avg = group_info.totalMemorySpaceUsed / group_info.groupCount;
            append!(
                es,
                "  Average Memory: {}kB  Peak Memory: {}kB",
                avg,
                group_info.maxMemorySpaceUsed
            );
        }
        if group_info.maxDiskSpaceUsed > 0 {
            let avg = group_info.totalDiskSpaceUsed / group_info.groupCount;
            append!(
                es,
                "  Average Disk: {}kB  Peak Disk: {}kB",
                avg,
                group_info.maxDiskSpaceUsed
            );
        }
    } else {
        let group_name = format!("{group_label} Groups");
        let method_names: Vec<&str> = methods.iter().map(|m| m.name()).collect();
        ExplainOpenGroup("Incremental Sort Groups", Some(&group_name), true, es);
        ExplainPropertyInteger("Group Count", None, group_info.groupCount, es);
        ExplainPropertyList("Sort Methods Used", &method_names, es);
        if group_info.maxMemorySpaceUsed > 0 {
            let avg = group_info.totalMemorySpaceUsed / group_info.groupCount;
            ExplainOpenGroup("Sort Space", Some("Sort Space Memory"), true, es);
            ExplainPropertyInteger("Average Sort Space Used", Some("kB"), avg, es);
            ExplainPropertyInteger(
                "Peak Sort Space Used",
                Some("kB"),
                group_info.maxMemorySpaceUsed,
                es,
            );
            ExplainCloseGroup("Sort Space", Some("Sort Space Memory"), true, es);
        }
        if group_info.maxDiskSpaceUsed > 0 {
            let avg = group_info.totalDiskSpaceUsed / group_info.groupCount;
            ExplainOpenGroup("Sort Space", Some("Sort Space Disk"), true, es);
            ExplainPropertyInteger("Average Sort Space Used", Some("kB"), avg, es);
            ExplainPropertyInteger(
                "Peak Sort Space Used",
                Some("kB"),
                group_info.maxDiskSpaceUsed,
                es,
            );
            ExplainCloseGroup("Sort Space", Some("Sort Space Disk"), true, es);
        }
        ExplainCloseGroup("Incremental Sort Groups", Some(&group_name), true, es);
    }
}

// show_incremental_sort_info (explain.c). The group-info helper carries the
// trailing newline C's caller appends, so the stanza composition stays
// byte-identical.
fn show_incremental_sort_info<'mcx>(
    node: Node<'mcx>,
    es: &mut ExplainState<'mcx>,
) -> PgResult<()> {
    if !es.analyze || es.qd.is_null() {
        return Ok(());
    }
    let id = plan_of(node).plan_node_id;
    let Some(info) = execmain_seams::query_desc_incsort_instrument::call(es.qd, id) else {
        return Ok(());
    };
    if info.fullsortGroupInfo.groupCount > 0 {
        show_incremental_sort_group_info(&info.fullsortGroupInfo, "Full-sort", true, es);
        if info.prefixsortGroupInfo.groupCount > 0 {
            if es.format == EXPLAIN_FORMAT_TEXT {
                append!(es, "\n");
            }
            show_incremental_sort_group_info(&info.prefixsortGroupInfo, "Pre-sorted", true, es);
        }
        if es.format == EXPLAIN_FORMAT_TEXT {
            append!(es, "\n");
        }
    }
    // The shared_info worker stanza; workers with no full-sort groups either
    // didn't launch or didn't contribute (C skips them).
    if let Some(workers) = execmain_seams::query_desc_worker_incsort_instrument::call(es.qd, id) {
        for (n, info) in workers {
            if info.fullsortGroupInfo.groupCount == 0 {
                continue;
            }
            if es.workers_state.is_some() {
                explain_open_worker(n as usize, es);
            }
            let indent_first_line = es.workers_state.is_none() || es.verbose;
            show_incremental_sort_group_info(
                &info.fullsortGroupInfo,
                "Full-sort",
                indent_first_line,
                es,
            );
            if info.prefixsortGroupInfo.groupCount > 0 {
                if es.format == EXPLAIN_FORMAT_TEXT {
                    append!(es, "\n");
                }
                show_incremental_sort_group_info(&info.prefixsortGroupInfo, "Pre-sorted", true, es);
            }
            if es.format == EXPLAIN_FORMAT_TEXT {
                append!(es, "\n");
            }
            if es.workers_state.is_some() {
                explain_close_worker(n as usize, es);
            }
        }
    }
    Ok(())
}

// show_agg_keys -> show_sort_group_keys (explain.c): key columns resolve in
// the child plan's tlist; deparsed with showimplicit=true as C.
fn show_agg_keys<'mcx>(node: Node<'mcx>, ancestors: Option<&Ancestors<'_, 'mcx>>, es: &mut ExplainState<'mcx>) -> PgResult<()> {
    let agg = node.as_agg().expect("Agg plan node");
    if agg.numCols <= 0 && agg.groupingSets.is_nil() {
        return Ok(());
    }
    if !agg.groupingSets.is_nil() {
        return show_grouping_sets(node, ancestors, es);
    }
    let child = agg.plan.lefttree.expect("Agg has an outer plan");
    let child_tlist = &plan_of(child).targetlist;
    let mcx = es.str.allocator();
    let useprefix = es.rtable_size > 1 || es.verbose;
    let pushed = Ancestors { entry: AncestorEntry::Plan(node), parent: ancestors };
    let mut result: PgVec<'mcx, PgString<'mcx>> = PgVec::new_in(mcx);
    for &resno in agg.grpColIdx {
        let tle = get_tle_by_resno(child_tlist, resno)
            .unwrap_or_else(|| node_gap("show_sort_group_keys", "no tlist entry for key column"));
        let mut buf = PgString::new_in(mcx);
        deparse_expr(es, child, Some(&pushed), tle.expr, useprefix, true, &mut buf)?;
        result.push(buf);
    }
    ExplainPropertyList("Group Key", &result, es);
    Ok(())
}

// show_group_keys -> show_sort_group_keys (explain.c): key columns resolve in
// the child plan's tlist; deparsed with showimplicit=true as C.
fn show_group_keys<'mcx>(node: Node<'mcx>, ancestors: Option<&Ancestors<'_, 'mcx>>, es: &mut ExplainState<'mcx>) -> PgResult<()> {
    let grp = node.as_group().expect("Group plan node");
    let child = grp.plan.lefttree.expect("Group has an outer plan");
    let child_tlist = &plan_of(child).targetlist;
    let mcx = es.str.allocator();
    let useprefix = es.rtable_size > 1 || es.verbose;
    let pushed = Ancestors { entry: AncestorEntry::Plan(node), parent: ancestors };
    let mut result: PgVec<'mcx, PgString<'mcx>> = PgVec::new_in(mcx);
    for &resno in grp.grpColIdx {
        let tle = get_tle_by_resno(child_tlist, resno)
            .unwrap_or_else(|| node_gap("show_sort_group_keys", "no tlist entry for key column"));
        let mut buf = PgString::new_in(mcx);
        deparse_expr(es, child, Some(&pushed), tle.expr, useprefix, true, &mut buf)?;
        result.push(buf);
    }
    ExplainPropertyList("Group Key", &result, es);
    Ok(())
}

// show_hash_info (explain.c); the shared_info (parallel) merge has no
// parallel lane.
fn show_hash_info<'mcx>(node: Node<'mcx>, es: &mut ExplainState<'mcx>) -> PgResult<()> {
    if !es.analyze || es.qd.is_null() {
        return Ok(());
    }
    let plan = plan_of(node);
    let Some(hi) = execmain_seams::query_desc_hash_instrument::call(es.qd, plan.plan_node_id)
    else {
        return Ok(());
    };
    if hi.nbatch <= 0 {
        return Ok(());
    }
    let space_peak_kb = hi.space_peak.div_ceil(1024);
    if es.format != EXPLAIN_FORMAT_TEXT {
        ExplainPropertyInteger("Hash Buckets", None, hi.nbuckets as i64, es);
        ExplainPropertyInteger("Original Hash Buckets", None, hi.nbuckets_original as i64, es);
        ExplainPropertyInteger("Hash Batches", None, hi.nbatch as i64, es);
        ExplainPropertyInteger("Original Hash Batches", None, hi.nbatch_original as i64, es);
        crate::format::ExplainPropertyUInteger(
            "Peak Memory Usage",
            Some("kB"),
            space_peak_kb,
            es,
        );
    } else if hi.nbatch_original != hi.nbatch || hi.nbuckets_original != hi.nbuckets {
        crate::format::ExplainIndentText(es);
        append!(
            es,
            "Buckets: {} (originally {})  Batches: {} (originally {})  Memory Usage: {}kB\n",
            hi.nbuckets,
            hi.nbuckets_original,
            hi.nbatch,
            hi.nbatch_original,
            space_peak_kb
        );
    } else {
        crate::format::ExplainIndentText(es);
        append!(
            es,
            "Buckets: {}  Batches: {}  Memory Usage: {}kB\n",
            hi.nbuckets,
            hi.nbatch,
            space_peak_kb
        );
    }
    Ok(())
}

// show_grouping_sets + show_grouping_set_keys (explain.c): keys resolve in
// the outer child's tlist; the chain's vestigial Sort contributes a Sort Key
// line and one indent level.
fn show_grouping_sets<'mcx>(
    node: Node<'mcx>,
    ancestors: Option<&Ancestors<'_, 'mcx>>,
    es: &mut ExplainState<'mcx>,
) -> PgResult<()> {
    let agg = node.as_agg().expect("Agg plan node");
    ExplainOpenGroup("Grouping Sets", Some("Grouping Sets"), false, es);
    show_grouping_set_keys(node, agg, None, ancestors, es)?;
    for chain_node in agg.chain.iter() {
        let aggnode = chain_node.as_agg().expect("Agg.chain cell");
        let sortnode = aggnode.plan.lefttree.and_then(Node::as_sort);
        show_grouping_set_keys(node, aggnode, sortnode, ancestors, es)?;
    }
    ExplainCloseGroup("Grouping Sets", Some("Grouping Sets"), false, es);
    Ok(())
}

fn show_grouping_set_keys<'mcx>(
    node: Node<'mcx>,
    aggnode: &types_nodes::plannodes::Agg<'mcx>,
    sortnode: Option<&types_nodes::plannodes::Sort<'mcx>>,
    ancestors: Option<&Ancestors<'_, 'mcx>>,
    es: &mut ExplainState<'mcx>,
) -> PgResult<()> {
    let mcx = es.str.allocator();
    let useprefix = es.rtable_size > 1 || es.verbose;
    // C reads plan->targetlist (the Agg's own); our planner's Agg tlist is not
    // C's passthrough shape, and grpColIdx resnos address the child tlist.
    let child = node.as_agg().expect("Agg plan node").plan.lefttree.expect("Agg has an outer plan");
    let tlist = &plan_of(child).targetlist;
    // The child tlist's own INNER/OUTER refs resolve at the child's level:
    // push the Agg as an ancestor and deparse in the child's context (the
    // show_group_keys shape).
    let pushed = Ancestors { entry: AncestorEntry::Plan(node), parent: ancestors };
    let (keyname, keysetname) = if aggnode.aggstrategy == 2 || aggnode.aggstrategy == 3 {
        ("Hash Key", "Hash Keys")
    } else {
        ("Group Key", "Group Keys")
    };

    ExplainOpenGroup("Grouping Set", None, true, es);

    if let Some(sort) = sortnode {
        let mut result: PgVec<'mcx, PgString<'mcx>> = PgVec::new_in(mcx);
        for keyno in 0..sort.numCols as usize {
            let resno = sort.sortColIdx[keyno];
            let tle = get_tle_by_resno(tlist, resno).unwrap_or_else(|| {
                node_gap("show_sort_group_keys", "no tlist entry for key column")
            });
            let mut buf = PgString::new_in(mcx);
            deparse_expr(es, child, Some(&pushed), tle.expr, useprefix, true, &mut buf)?;
            show_sortorder_options(
                &mut buf,
                tle.expr,
                sort.sortOperators[keyno],
                sort.collations[keyno],
                sort.nullsFirst[keyno],
            )?;
            result.push(buf);
        }
        ExplainPropertyList("Sort Key", &result, es);
        if es.format == EXPLAIN_FORMAT_TEXT {
            es.indent += 1;
        }
    }

    ExplainOpenGroup(keysetname, Some(keysetname), false, es);

    for set in aggnode.groupingSets.iter() {
        let set = set
            .as_int_list()
            .unwrap_or_else(|| node_gap("show_grouping_set_keys", "groupingSets cell shape"));
        let mut result: PgVec<'mcx, PgString<'mcx>> = PgVec::new_in(mcx);
        for &i in set.as_slice() {
            let keyresno = aggnode.grpColIdx[i as usize];
            let tle = get_tle_by_resno(tlist, keyresno).unwrap_or_else(|| {
                node_gap("show_grouping_set_keys", "no tlist entry for key column")
            });
            let mut buf = PgString::new_in(mcx);
            deparse_expr(es, child, Some(&pushed), tle.expr, useprefix, true, &mut buf)?;
            result.push(buf);
        }
        if result.is_empty() && es.format == EXPLAIN_FORMAT_TEXT {
            crate::format::ExplainPropertyText(keyname, "()", es);
        } else {
            crate::format::ExplainPropertyListNested(keyname, &result, es);
        }
    }

    ExplainCloseGroup(keysetname, Some(keysetname), false, es);

    if sortnode.is_some() && es.format == EXPLAIN_FORMAT_TEXT {
        es.indent -= 1;
    }

    ExplainCloseGroup("Grouping Set", None, true, es);
    Ok(())
}

// show_hashagg_info (explain.c); the parallel-worker display has no parallel
// lane. AGG_HASHED/AGG_MIXED only.
fn show_hashagg_info<'mcx>(node: Node<'mcx>, es: &mut ExplainState<'mcx>) -> PgResult<()> {
    let agg = node.as_agg().expect("Agg plan node");
    if agg.aggstrategy != 2 && agg.aggstrategy != 3 {
        return Ok(());
    }
    let ai = if es.qd.is_null() {
        return Ok(());
    } else {
        match execmain_seams::query_desc_agg_instrument::call(es.qd, agg.plan.plan_node_id) {
            Some(ai) => ai,
            None => return Ok(()),
        }
    };
    if es.format != EXPLAIN_FORMAT_TEXT {
        if es.costs {
            ExplainPropertyInteger(
                "Planned Partitions",
                None,
                ai.hash_planned_partitions as i64,
                es,
            );
        }
        if es.analyze && ai.hash_mem_peak > 0 {
            ExplainPropertyInteger("HashAgg Batches", None, ai.hash_batches_used as i64, es);
            ExplainPropertyInteger(
                "Peak Memory Usage",
                Some("kB"),
                ai.hash_mem_peak.div_ceil(1024) as i64,
                es,
            );
            ExplainPropertyInteger("Disk Usage", Some("kB"), ai.hash_disk_used as i64, es);
        }
        return Ok(());
    }
    let mut gotone = false;
    if es.costs && ai.hash_planned_partitions > 0 {
        crate::format::ExplainIndentText(es);
        append!(es, "Planned Partitions: {}", ai.hash_planned_partitions);
        gotone = true;
    }
    if es.analyze && ai.hash_mem_peak > 0 {
        if !gotone {
            crate::format::ExplainIndentText(es);
        } else {
            append!(es, "  ");
        }
        append!(
            es,
            "Batches: {}  Memory Usage: {}kB",
            ai.hash_batches_used,
            ai.hash_mem_peak.div_ceil(1024)
        );
        gotone = true;
        if ai.hash_batches_used > 1 {
            append!(es, "  Disk Usage: {}kB", ai.hash_disk_used);
        }
    }
    if gotone {
        append!(es, "\n");
    }
    Ok(())
}

// show_window_def + show_window_keys (explain.c).
fn show_window_def<'mcx>(
    node: Node<'mcx>,
    ancestors: Option<&Ancestors<'_, 'mcx>>,
    es: &mut ExplainState<'mcx>,
) -> PgResult<()> {
    use types_nodes::rawnodes::FRAMEOPTION_NONDEFAULT;
    let wagg = node.as_window_agg().expect("WindowAgg node");
    let mcx = es.str.allocator();
    let mut buf = PgString::new_in(mcx);
    buf.try_push_str(&quote_identifier(wagg.winname.expect("named window (name_active_windows)")))?;
    buf.try_push_str(" AS (")?;
    let child = wagg.plan.lefttree.expect("WindowAgg has a child");
    let mut needspace = false;
    // show_window_keys: key columns refer to the child's tlist, deparsed in
    // the child's context with the WindowAgg pushed onto the ancestors.
    let pushed = Ancestors { entry: AncestorEntry::Plan(node), parent: ancestors };
    let mut keys = |buf: &mut PgString<'mcx>,
                    es: &mut ExplainState<'mcx>,
                    idx: &[i16]|
     -> PgResult<()> {
        let useprefix = es.rtable_size > 1 || es.verbose;
        let child_tlist = &plan_of(child).targetlist;
        for (i, &resno) in idx.iter().enumerate() {
            if i > 0 {
                buf.try_push_str(", ")?;
            }
            let tle = get_tle_by_resno(child_tlist, resno)
                .unwrap_or_else(|| node_gap("show_window_keys", "no tlist entry for key column"));
            deparse_expr(es, child, Some(&pushed), tle.expr, useprefix, true, buf)?;
        }
        Ok(())
    };
    if wagg.partNumCols > 0 {
        buf.try_push_str("PARTITION BY ")?;
        keys(&mut buf, es, wagg.partColIdx)?;
        needspace = true;
    }
    if wagg.ordNumCols > 0 {
        if needspace {
            buf.try_push(' ')?;
        }
        buf.try_push_str("ORDER BY ")?;
        keys(&mut buf, es, wagg.ordColIdx)?;
    }
    if wagg.frameOptions & FRAMEOPTION_NONDEFAULT != 0 {
        if needspace || wagg.ordNumCols > 0 {
            buf.try_push(' ')?;
        }
        get_window_frame_options(
            wagg.frameOptions,
            wagg.startOffset,
            wagg.endOffset,
            node,
            ancestors,
            es,
            &mut buf,
        )?;
    }
    buf.try_push(')')?;
    ExplainPropertyText("Window", buf.as_str(), es);
    Ok(())
}

// get_window_frame_options (ruleutils.c); offsets deparse through the
// Const/expr slice with the WindowAgg itself as deparse context.
#[allow(clippy::too_many_arguments)]
fn get_window_frame_options<'mcx>(
    frame_options: i32,
    start_offset: Option<Node<'mcx>>,
    end_offset: Option<Node<'mcx>>,
    plan_node: Node<'mcx>,
    ancestors: Option<&Ancestors<'_, 'mcx>>,
    es: &ExplainState<'mcx>,
    buf: &mut PgString<'mcx>,
) -> PgResult<()> {
    use types_nodes::rawnodes::{
        FRAMEOPTION_BETWEEN, FRAMEOPTION_END_CURRENT_ROW, FRAMEOPTION_END_OFFSET,
        FRAMEOPTION_END_OFFSET_FOLLOWING, FRAMEOPTION_END_OFFSET_PRECEDING,
        FRAMEOPTION_END_UNBOUNDED_FOLLOWING, FRAMEOPTION_EXCLUDE_CURRENT_ROW,
        FRAMEOPTION_EXCLUDE_GROUP, FRAMEOPTION_EXCLUDE_TIES, FRAMEOPTION_GROUPS,
        FRAMEOPTION_NONDEFAULT, FRAMEOPTION_RANGE, FRAMEOPTION_ROWS,
        FRAMEOPTION_START_CURRENT_ROW, FRAMEOPTION_START_OFFSET,
        FRAMEOPTION_START_OFFSET_FOLLOWING, FRAMEOPTION_START_OFFSET_PRECEDING,
        FRAMEOPTION_START_UNBOUNDED_PRECEDING,
    };
    debug_assert!(frame_options & FRAMEOPTION_NONDEFAULT != 0);
    let useprefix = es.rtable_size > 1 || es.verbose;
    if frame_options & FRAMEOPTION_RANGE != 0 {
        buf.try_push_str("RANGE ")?;
    } else if frame_options & FRAMEOPTION_ROWS != 0 {
        buf.try_push_str("ROWS ")?;
    } else if frame_options & FRAMEOPTION_GROUPS != 0 {
        buf.try_push_str("GROUPS ")?;
    } else {
        unreachable!()
    }
    if frame_options & FRAMEOPTION_BETWEEN != 0 {
        buf.try_push_str("BETWEEN ")?;
    }
    if frame_options & FRAMEOPTION_START_UNBOUNDED_PRECEDING != 0 {
        buf.try_push_str("UNBOUNDED PRECEDING ")?;
    } else if frame_options & FRAMEOPTION_START_CURRENT_ROW != 0 {
        buf.try_push_str("CURRENT ROW ")?;
    } else if frame_options & FRAMEOPTION_START_OFFSET != 0 {
        deparse_expr(
            es,
            plan_node,
            ancestors,
            start_offset.expect("startOffset"),
            useprefix,
            false,
            buf,
        )?;
        if frame_options & FRAMEOPTION_START_OFFSET_PRECEDING != 0 {
            buf.try_push_str(" PRECEDING ")?;
        } else if frame_options & FRAMEOPTION_START_OFFSET_FOLLOWING != 0 {
            buf.try_push_str(" FOLLOWING ")?;
        } else {
            unreachable!()
        }
    } else {
        unreachable!()
    }
    if frame_options & FRAMEOPTION_BETWEEN != 0 {
        buf.try_push_str("AND ")?;
        if frame_options & FRAMEOPTION_END_UNBOUNDED_FOLLOWING != 0 {
            buf.try_push_str("UNBOUNDED FOLLOWING ")?;
        } else if frame_options & FRAMEOPTION_END_CURRENT_ROW != 0 {
            buf.try_push_str("CURRENT ROW ")?;
        } else if frame_options & FRAMEOPTION_END_OFFSET != 0 {
            deparse_expr(
                es,
                plan_node,
                ancestors,
                end_offset.expect("endOffset"),
                useprefix,
                false,
                buf,
            )?;
            if frame_options & FRAMEOPTION_END_OFFSET_PRECEDING != 0 {
                buf.try_push_str(" PRECEDING ")?;
            } else if frame_options & FRAMEOPTION_END_OFFSET_FOLLOWING != 0 {
                buf.try_push_str(" FOLLOWING ")?;
            } else {
                unreachable!()
            }
        } else {
            unreachable!()
        }
    }
    if frame_options & FRAMEOPTION_EXCLUDE_CURRENT_ROW != 0 {
        buf.try_push_str("EXCLUDE CURRENT ROW ")?;
    } else if frame_options & FRAMEOPTION_EXCLUDE_GROUP != 0 {
        buf.try_push_str("EXCLUDE GROUP ")?;
    } else if frame_options & FRAMEOPTION_EXCLUDE_TIES != 0 {
        buf.try_push_str("EXCLUDE TIES ")?;
    }
    let len = buf.as_str().len();
    buf.truncate(len - 1);
    Ok(())
}

fn get_tle_by_resno<'a, 'mcx>(
    tlist: &'a NodeList<'mcx>,
    resno: i16,
) -> Option<&'mcx types_nodes::primnodes::TargetEntry<'mcx>> {
    tlist
        .iter()
        .map(|n| n.as_target_entry().expect("targetlist holds TargetEntries"))
        .find(|tle| tle.resno == resno)
}

fn show_sortorder_options(
    buf: &mut PgString<'_>,
    sortexpr: Node<'_>,
    sort_operator: types_core::primitive::Oid,
    collation: types_core::primitive::Oid,
    nulls_first: bool,
) -> PgResult<()> {
    let sortcoltype = execscan_expr_type(sortexpr);
    let typentry = typcache::lookup_type_cache(
        sortcoltype,
        typcache::TYPECACHE_LT_OPR | typcache::TYPECACHE_GT_OPR,
    )?;
    if collation != types_core::primitive::InvalidOid
        && collation != lsyscache::typ::get_typcollation(sortcoltype)?
    {
        let collname = ruleutils::generate_collation_name(buf.allocator(), collation)?;
        write!(buf, " COLLATE {collname}").expect("PgString write");
    }
    let reverse = if sort_operator == typentry.lt_opr() {
        false
    } else if sort_operator == typentry.gt_opr() {
        buf.try_push_str(" DESC")?;
        true
    } else {
        let scratch = mcx::MemoryContext::new("sortorder-opname");
        let opname = lsyscache::get_opname(scratch.mcx(), sort_operator)?
            .unwrap_or_else(|| node_gap("show_sortorder_options", "missing operator name"));
        buf.try_push_str(" USING ")?;
        buf.try_push_str(opname.as_str())?;
        match lsyscache::get_ordering_op_properties(sort_operator)? {
            Some((_, _, cmptype)) => cmptype == lsyscache::COMPARE_GT,
            None => false,
        }
    };
    if nulls_first != reverse {
        buf.try_push_str(if nulls_first { " NULLS FIRST" } else { " NULLS LAST" })?;
    }
    Ok(())
}

// exprType (nodeFuncs.c) over sort keys; non-Var keys (e.g. Const sort keys
// under a MergeAppend child Sort) resolve through the shared accessor.
fn execscan_expr_type(node: Node<'_>) -> types_core::primitive::Oid {
    nodes_core::node_funcs::expr_type(node)
}

// show_upper_qual on Result.resconstantqual: an implicit-AND List, deparsed
// via make_ands_explicit (single member prints bare, several as AND).
fn show_one_time_filter<'mcx>(
    qual: Node<'mcx>,
    node: Node<'mcx>,
    ancestors: Option<&Ancestors<'_, 'mcx>>,
    es: &mut ExplainState<'mcx>,
) -> PgResult<()> {
    let list = qual.as_list().expect("resconstantqual is a List");
    if list.is_nil() {
        return Ok(());
    }
    let mcx = es.str.allocator();
    let useprefix = es.rtable_size > 1 || es.verbose;
    let mut buf = PgString::new_in(mcx);
    if list.len() == 1 {
        deparse_expr(es, node, ancestors, list.nth(0), useprefix, false, &mut buf)?;
    } else {
        buf.try_push('(')?;
        for (i, item) in list.iter().enumerate() {
            if i > 0 {
                buf.try_push_str(" AND ")?;
            }
            deparse_expr(es, node, ancestors, item, useprefix, false, &mut buf)?;
        }
        buf.try_push(')')?;
    }
    crate::format::ExplainPropertyText("One-Time Filter", buf.as_str(), es);
    Ok(())
}

// ExplainIndexScanDetails (explain.c).
fn ExplainIndexScanDetails(
    indexid: types_core::Oid,
    indexorderdir: i32,
    es: &mut ExplainState<'_>,
) -> PgResult<()> {
    let mcx = es.str.allocator();
    let indexname = lsyscache::get_rel_name(mcx, indexid)?
        .unwrap_or_else(|| panic!("cache lookup failed for index {indexid}"));
    if es.format == EXPLAIN_FORMAT_TEXT {
        if indexorderdir < 0 {
            append!(es, " Backward");
        }
        append!(es, " using {}", quote_identifier(indexname.as_str()));
    } else {
        let scandir = match indexorderdir {
            d if d < 0 => "Backward",
            d if d > 0 => "Forward",
            _ => "???",
        };
        ExplainPropertyText("Scan Direction", scandir, es);
        ExplainPropertyText("Index Name", indexname.as_str(), es);
    }
    Ok(())
}

// show_instrumentation_count (explain.c). which=1 is genuine for every
// caller here: SampleScan/SeqScan-family/FunctionScan/TidScan/TidRangeScan/
// BitmapHeapScan/IndexScan/IndexOnlyScan/SubqueryScan are ExecScan-driven
// (execScan.h's generic InstrCountFiltered1 on qual failure, wired via
// ScanState.instr_idx); Gather/GatherMerge never carry a qual at all
// (planner never attaches one — C asserts `!plan->qual`), so their branch
// is dead code by construction, same as C. which=2 (index recheck) has no
// counting writer yet; callers of that arm rely on the recheck qual being
// empty in the covered tests, so the zero is coincidentally correct rather
// than counted.
fn show_instrumentation_count(
    qlabel: &str,
    which: i32,
    instrument: &Option<types_core::instrument::Instrumentation>,
    es: &mut ExplainState<'_>,
) {
    if !es.analyze {
        return;
    }
    let Some(i) = instrument else { return };
    let nfiltered = if which == 2 { i.nfiltered2 } else { i.nfiltered1 };
    if i.nloops > 0.0 && (nfiltered > 0.0 || es.format != EXPLAIN_FORMAT_TEXT) {
        crate::format::ExplainPropertyFloat(qlabel, None, nfiltered / i.nloops, 0, es);
    }
}

// show_indexsearches_info (explain.c); the SharedInfo worker sum has no
// parallel lane.
fn show_indexsearches_info<'mcx>(node: Node<'mcx>, es: &mut ExplainState<'mcx>) {
    if !es.analyze {
        return;
    }
    let nsearches = if es.qd.is_null() {
        0
    } else {
        execmain_seams::query_desc_index_searches::call(es.qd, plan_of(node).plan_node_id)
            .unwrap_or(0)
    };
    crate::format::ExplainPropertyUInteger("Index Searches", None, nsearches, es);
}

// show_tidbitmap_info (explain.c); parallel worker stats have no parallel
// lane.
fn show_tidbitmap_info<'mcx>(node: Node<'mcx>, es: &mut ExplainState<'mcx>) {
    if !es.analyze {
        return;
    }
    let id = plan_of(node).plan_node_id;
    let stats = if es.qd.is_null() {
        None
    } else {
        execmain_seams::query_desc_bitmap_instrument::call(es.qd, id)
    };
    let stats = stats.unwrap_or_default();
    if es.format != EXPLAIN_FORMAT_TEXT {
        crate::format::ExplainPropertyUInteger("Exact Heap Blocks", None, stats.exact_pages, es);
        crate::format::ExplainPropertyUInteger("Lossy Heap Blocks", None, stats.lossy_pages, es);
    } else if stats.exact_pages > 0 || stats.lossy_pages > 0 {
        crate::format::ExplainIndentText(es);
        append!(es, "Heap Blocks:");
        if stats.exact_pages > 0 {
            append!(es, " exact={}", stats.exact_pages);
        }
        if stats.lossy_pages > 0 {
            append!(es, " lossy={}", stats.lossy_pages);
        }
        append!(es, "\n");
    }
    // Per-worker stats (C reads planstate->sinstrument; all-zero worker rows
    // are skipped there and never reported here).
    if es.qd.is_null() {
        return;
    }
    if let Some(workers) = execmain_seams::query_desc_worker_bitmap_instrument::call(es.qd, id) {
        for (n, si) in workers {
            if si.exact_pages == 0 && si.lossy_pages == 0 {
                continue;
            }
            if es.workers_state.is_some() {
                explain_open_worker(n as usize, es);
            }
            crate::format::ExplainIndentText(es);
            append!(es, "Heap Blocks:");
            if si.exact_pages > 0 {
                append!(es, " exact={}", si.exact_pages);
            }
            if si.lossy_pages > 0 {
                append!(es, " lossy={}", si.lossy_pages);
            }
            append!(es, "\n");
            if es.workers_state.is_some() {
                explain_close_worker(n as usize, es);
            }
        }
    }
}

// show_storage_info (explain.c). maxSpace inherits the arena-vs-palloc
// accounting caveat only through tuplestore's chunk_space mirror (byte-exact
// vs C by construction; see tuplestore::chunk_space).
fn show_storage_info(stats: types_core::instrument::TuplestoreInstrumentation, es: &mut ExplainState<'_>) {
    let kb = (stats.max_space + 1023) / 1024;
    if es.format != EXPLAIN_FORMAT_TEXT {
        ExplainPropertyText("Storage", stats.space_type.name(), es);
        ExplainPropertyInteger("Maximum Storage", Some("kB"), kb as i64, es);
    } else {
        crate::format::ExplainIndentText(es);
        append!(es, "Storage: {}  Maximum Storage: {}kB\n", stats.space_type.name(), kb);
    }
}

fn tuplestore_stats<'mcx>(
    node: Node<'mcx>,
    es: &ExplainState<'mcx>,
) -> Option<types_core::instrument::TuplestoreInstrumentation> {
    if !es.analyze || es.qd.is_null() {
        return None;
    }
    execmain_seams::query_desc_tuplestore_instrument::call(es.qd, plan_of(node).plan_node_id)
}

fn show_material_info<'mcx>(node: Node<'mcx>, es: &mut ExplainState<'mcx>) {
    if let Some(stats) = tuplestore_stats(node, es) {
        show_storage_info(stats, es);
    }
}

// show_memoize_info (explain.c); the parallel-worker stanza has no lane.
fn show_memoize_info<'mcx>(
    node: Node<'mcx>,
    ancestors: Option<&Ancestors<'_, 'mcx>>,
    es: &mut ExplainState<'mcx>,
) -> PgResult<()> {
    use core::fmt::Write;
    let m = node.as_memoize().expect("Memoize plan node");
    let mcx = es.str.allocator();
    let useprefix = es.rtable_size > 1 || es.verbose;
    let mut keystr = PgString::new_in(mcx);
    let mut sep = "";
    for expr in &m.param_exprs {
        let mut buf = PgString::new_in(mcx);
        deparse_expr(es, node, ancestors, expr, useprefix, false, &mut buf)?;
        write!(keystr, "{sep}{}", buf.as_str()).expect("PgString write");
        sep = ", ";
    }
    ExplainPropertyText("Cache Key", keystr.as_str(), es);
    ExplainPropertyText("Cache Mode", if m.binary_mode { "binary" } else { "logical" }, es);

    if !es.analyze || es.qd.is_null() {
        return Ok(());
    }
    let id = plan_of(node).plan_node_id;
    let Some(si) = execmain_seams::query_desc_memoize_instrument::call(es.qd, id) else {
        return Ok(());
    };
    if si.cache_misses > 0 {
        let mem_peak_kb = (si.mem_peak + 1023) / 1024;
        if es.format != EXPLAIN_FORMAT_TEXT {
            ExplainPropertyInteger("Cache Hits", None, si.cache_hits as i64, es);
            ExplainPropertyInteger("Cache Misses", None, si.cache_misses as i64, es);
            ExplainPropertyInteger("Cache Evictions", None, si.cache_evictions as i64, es);
            ExplainPropertyInteger("Cache Overflows", None, si.cache_overflows as i64, es);
            ExplainPropertyInteger("Peak Memory Usage", Some("kB"), mem_peak_kb as i64, es);
        } else {
            crate::format::ExplainIndentText(es);
            append!(
                es,
                "Hits: {}  Misses: {}  Evictions: {}  Overflows: {}  Memory Usage: {}kB\n",
                si.cache_hits,
                si.cache_misses,
                si.cache_evictions,
                si.cache_overflows,
                mem_peak_kb
            );
        }
    }
    Ok(())
}

fn show_windowagg_info<'mcx>(node: Node<'mcx>, es: &mut ExplainState<'mcx>) {
    if let Some(stats) = tuplestore_stats(node, es) {
        show_storage_info(stats, es);
    }
}

// show_foreignscan_info (explain.c): ExplainForeignScan runs executor-side.
fn show_foreignscan_info<'mcx>(plan_node_id: i32, es: &mut ExplainState<'mcx>) -> PgResult<()> {
    if es.qd.is_null() {
        return Ok(());
    }
    let qd = es.qd;
    let flags = types_nodes::FdwExplainFlags { costs: es.costs, verbose: es.verbose };
    execmain_seams::query_desc_foreign_explain::call(qd, plan_node_id, flags, &mut |label, v| {
        match v {
            types_nodes::FdwExplainProp::Text(t) => ExplainPropertyText(label, t, es),
            types_nodes::FdwExplainProp::Integer { value, unit } => {
                ExplainPropertyInteger(label, Some(unit), value, es)
            }
        }
        Ok(())
    })
}

fn show_ctescan_info<'mcx>(node: Node<'mcx>, es: &mut ExplainState<'mcx>) {
    if let Some(stats) = tuplestore_stats(node, es) {
        show_storage_info(stats, es);
    }
}

// show_instrumentation_count's nfiltered read for join/upper nodes: unlike
// the ExecScan-driven scan family (execScan.h), these count via their own
// node-specific InstrCountFiltered1/2 calls (e.g. nodeNestloop.c:246,
// nodeHashjoin.c:596, nodeMergejoin.c:837, nodeAgg.c:1386, nodeGroup.c:96/149,
// nodeWindowAgg.c:2405), which aren't ported yet, so printing would be
// silently wrong whenever a filter removed rows.
fn filtered_count_gap(qual: &NodeList<'_>, es: &ExplainState<'_>) {
    if es.analyze && !qual.is_nil() {
        node_gap(
            "show_instrumentation_count",
            "Rows Removed by Filter needs nfiltered counting (InstrCountFiltered, execScan.c)",
        );
    }
}

// make_orclause/make_andclause over multi-entry tid quals (explain.c wraps
// them so the display carries the list's implicit OR/AND semantics).
fn wrap_multi_quals<'mcx>(
    es: &ExplainState<'mcx>,
    quals: &NodeList<'mcx>,
    boolop: BoolExprType,
) -> PgResult<NodeList<'mcx>> {
    let mcx = es.str.allocator();
    if quals.len() <= 1 {
        let mut out = NodeList::nil();
        for q in quals {
            out.lappend(mcx, q)?;
        }
        return Ok(out);
    }
    let mut args = NodeList::nil();
    for q in quals {
        args.lappend(mcx, q)?;
    }
    let wrapped = Node::mk(mcx, BoolExpr { boolop, args, location: -1 })?;
    let mut out = NodeList::nil();
    out.lappend(mcx, wrapped)?;
    Ok(out)
}

// C: scan quals prefix only under VERBOSE (or SubqueryScan, loud elsewhere).
fn show_scan_qual<'mcx>(
    qual: &NodeList<'mcx>,
    qlabel: &str,
    node: Node<'mcx>,
    ancestors: Option<&Ancestors<'_, 'mcx>>,
    es: &mut ExplainState<'mcx>,
) -> PgResult<()> {
    let useprefix = node.node_tag() == NodeTag::T_SubqueryScan || es.verbose;
    show_qual(qual, qlabel, node, ancestors, useprefix, es)
}

fn show_upper_qual<'mcx>(
    qual: &NodeList<'mcx>,
    qlabel: &str,
    node: Node<'mcx>,
    ancestors: Option<&Ancestors<'_, 'mcx>>,
    es: &mut ExplainState<'mcx>,
) -> PgResult<()> {
    let useprefix = es.rtable_size > 1 || es.verbose;
    show_qual(qual, qlabel, node, ancestors, useprefix, es)
}

// show_tablesample (explain.c).
fn show_tablesample<'mcx>(
    node: Node<'mcx>,
    ancestors: Option<&Ancestors<'_, 'mcx>>,
    es: &mut ExplainState<'mcx>,
) -> PgResult<()> {
    let ss = node.as_sample_scan().expect("SampleScan plan node");
    let tsc = ss
        .tablesample
        .expect("SampleScan has a tablesample clause")
        .as_table_sample_clause()
        .expect("tablesample is a TableSampleClause");
    let mcx = es.str.allocator();
    let useprefix = es.rtable_size > 1;

    let method_name = lsyscache::get_func_name(mcx, tsc.tsmhandler)?
        .expect("TSM handler has a pg_proc row");
    let mut params: PgVec<'mcx, PgString<'mcx>> = PgVec::new_in(mcx);
    for arg in tsc.args.iter() {
        let mut buf = PgString::new_in(mcx);
        deparse_expr(es, node, ancestors, arg, useprefix, false, &mut buf)?;
        params.push(buf);
    }
    let repeatable = match tsc.repeatable {
        Some(r) => {
            let mut buf = PgString::new_in(mcx);
            deparse_expr(es, node, ancestors, r, useprefix, false, &mut buf)?;
            Some(buf)
        }
        None => None,
    };

    if es.format == EXPLAIN_FORMAT_TEXT {
        crate::format::ExplainIndentText(es);
        append!(es, "Sampling: {} (", method_name.as_str());
        for (i, p) in params.iter().enumerate() {
            if i > 0 {
                append!(es, ", ");
            }
            append!(es, "{}", p.as_str());
        }
        append!(es, ")");
        if let Some(r) = repeatable {
            append!(es, " REPEATABLE ({})", r.as_str());
        }
        append!(es, "\n");
    } else {
        ExplainPropertyText("Sampling Method", method_name.as_str(), es);
        ExplainPropertyList("Sampling Parameters", &params, es);
        if let Some(r) = repeatable {
            ExplainPropertyText("Repeatable Seed", r.as_str(), es);
        }
    }
    Ok(())
}

fn show_qual<'mcx>(
    qual: &NodeList<'mcx>,
    qlabel: &str,
    node: Node<'mcx>,
    ancestors: Option<&Ancestors<'_, 'mcx>>,
    useprefix: bool,
    es: &mut ExplainState<'mcx>,
) -> PgResult<()> {
    if qual.is_nil() {
        return Ok(());
    }
    let mcx = es.str.allocator();
    let mut buf = PgString::new_in(mcx);
    // make_ands_explicit: a multi-item qual deparses as one AND expression.
    let expr = if qual.len() == 1 {
        qual.nth(0)
    } else {
        Node::mk(
            mcx,
            types_nodes::primnodes::BoolExpr {
                boolop: types_nodes::primnodes::BoolExprType::AND_EXPR,
                args: qual.clone_in(mcx)?,
                location: -1,
            },
        )?
    };
    deparse_expr(es, node, ancestors, expr, useprefix, false, &mut buf)?;
    crate::format::ExplainPropertyText(qlabel, buf.as_str(), es);
    Ok(())
}

// ruleutils.c deparse_expression over a plan-tree context: point the shared
// deparse context at plan_node + ancestors, deparse, append.
fn deparse_expr<'mcx>(
    es: &ExplainState<'mcx>,
    plan_node: Node<'mcx>,
    ancestors: Option<&Ancestors<'_, 'mcx>>,
    expr: Node<'mcx>,
    useprefix: bool,
    showimplicit: bool,
    buf: &mut PgString<'mcx>,
) -> PgResult<()> {
    let cxt = es.deparse_cxt.as_ref().expect("deparse before ExplainPrintPlan");
    ruleutils::set_deparse_context_plan(cxt, plan_node, ancestors_vec(ancestors));
    let s = ruleutils::deparse_expression(es.str.allocator(), expr, cxt, useprefix, showimplicit)?;
    buf.try_push_str(&s)?;
    Ok(())
}

fn ancestors_vec<'mcx>(
    ancestors: Option<&Ancestors<'_, 'mcx>>,
) -> Vec<ruleutils::AncestorEntry<'mcx>> {
    let mut v = Vec::new();
    let mut chain = ancestors;
    while let Some(a) = chain {
        v.push(a.entry);
        chain = a.parent;
    }
    v
}


fn ExplainScanTarget(scanrelid: types_core::Index, es: &mut ExplainState<'_>) -> PgResult<()> {
    ExplainTargetRel(scanrelid, es)
}

// ExplainTargetRel's T_TableFuncScan arm: objectname keys off functype.
fn ExplainTableFuncTarget<'mcx>(
    tfs: &types_nodes::plannodes::TableFuncScan<'mcx>,
    es: &mut ExplainState<'mcx>,
) -> PgResult<()> {
    let rti = tfs.scan.scanrelid;
    let rte: &RangeTblEntry<'_> = es
        .rtable
        .expect("ExplainTableFuncTarget before ExplainPrintPlan")
        .nth(rti as usize - 1)
        .as_range_tbl_entry()
        .expect("rtable holds RTEs");
    debug_assert_eq!(rte.rtekind, RTEKind::RTE_TABLEFUNC);
    let tf = tfs
        .tablefunc
        .and_then(|n| n.as_table_func())
        .expect("TableFuncScan has a TableFunc");
    let objectname = match tf.functype {
        types_nodes::TableFuncType::TFT_XMLTABLE => "xmltable",
        types_nodes::TableFuncType::TFT_JSON_TABLE => "json_table",
    };
    let refname = es.rtable_names[rti as usize - 1]
        .or_else(|| rte.eref.expect("RTE without eref").aliasname)
        .expect("scan RTE has a refname");
    if es.format == EXPLAIN_FORMAT_TEXT {
        append!(es, " on {}", quote_identifier(objectname));
        if objectname != refname {
            append!(es, " {}", quote_identifier(refname));
        }
    } else {
        ExplainPropertyText("Table Function Name", objectname, es);
        ExplainPropertyText("Alias", refname, es);
    }
    Ok(())
}

// ExplainTargetRel's T_FunctionScan arm: objectname is the function's name
// when the RTE holds exactly one FuncExpr item.
fn ExplainFunctionTarget<'mcx>(
    fs: &types_nodes::plannodes::FunctionScan<'mcx>,
    es: &mut ExplainState<'mcx>,
) -> PgResult<()> {
    let mcx = es.str.allocator();
    let rti = fs.scan.scanrelid;
    let rte: &RangeTblEntry<'_> = es
        .rtable
        .expect("ExplainFunctionTarget before ExplainPrintPlan")
        .nth(rti as usize - 1)
        .as_range_tbl_entry()
        .expect("rtable holds RTEs");
    debug_assert_eq!(rte.rtekind, RTEKind::RTE_FUNCTION);
    let mut objectname = None;
    // C reads the plan node's functions list (the flat rtable strips
    // rte->functions).
    if fs.functions.len() == 1 {
        let rtfunc = fs
            .functions
            .nth(0)
            .as_range_tbl_function()
            .expect("functions cell");
        if let Some(fe) = rtfunc.funcexpr.and_then(|n| n.as_func_expr()) {
            objectname = lsyscache::get_func_name(mcx, fe.funcid)?;
        }
    }
    let namespace = match (es.verbose, &objectname) {
        (true, Some(_)) => {
            let rtfunc = fs.functions.nth(0).as_range_tbl_function().expect("functions cell");
            let fe = rtfunc.funcexpr.and_then(|n| n.as_func_expr()).expect("FuncExpr");
            lsyscache::get_namespace_name_or_temp(mcx, lsyscache::get_func_namespace(fe.funcid)?)?
        }
        _ => None,
    };
    let objectname = objectname.as_ref().map(|s| s.as_str());
    let refname = es.rtable_names[rti as usize - 1]
        .or_else(|| rte.eref.expect("RTE without eref").aliasname)
        .expect("scan RTE has a refname");
    if es.format == EXPLAIN_FORMAT_TEXT {
        append!(es, " on");
        if let Some(ns) = &namespace {
            let obj = objectname.expect("namespace implies objectname");
            append!(es, " {}.{}", quote_identifier(ns.as_str()), quote_identifier(obj));
        } else if let Some(obj) = objectname {
            append!(es, " {}", quote_identifier(obj));
        }
        if objectname != Some(refname) {
            append!(es, " {}", quote_identifier(refname));
        }
    } else {
        if let Some(obj) = objectname {
            ExplainPropertyText("Function Name", obj, es);
        }
        if let Some(ns) = &namespace {
            ExplainPropertyText("Schema", ns.as_str(), es);
        }
        ExplainPropertyText("Alias", refname, es);
    }
    Ok(())
}

fn ExplainTargetRel<'mcx>(rti: types_core::Index, es: &mut ExplainState<'mcx>) -> PgResult<()> {
    let mcx = es.str.allocator();
    let rte: &RangeTblEntry<'_> = es
        .rtable
        .expect("ExplainTargetRel before ExplainPrintPlan")
        .nth(rti as usize - 1)
        .as_range_tbl_entry()
        .expect("rtable holds RTEs");
    let relname;
    let (objectname, objecttag) = match rte.rtekind {
        RTEKind::RTE_RELATION => {
            relname = lsyscache::get_rel_name(mcx, rte.relid)?;
            (relname.as_ref().map(|s| s.as_str()), Some("Relation Name"))
        }
        RTEKind::RTE_CTE => (rte.ctename, Some("CTE Name")),
        RTEKind::RTE_NAMEDTUPLESTORE => (rte.enrname, Some("Tuplestore Name")),
        RTEKind::RTE_SUBQUERY | RTEKind::RTE_VALUES => (None, None),
        other => node_gap(
            "ExplainTargetRel",
            &format!("{other:?} target arm unported (M2+ plan lanes)"),
        ),
    };
    let namespace = if es.verbose && rte.rtekind == RTEKind::RTE_RELATION {
        lsyscache::get_namespace_name_or_temp(mcx, lsyscache::get_rel_namespace(rte.relid)?)?
    } else {
        None
    };
    let refname = es.rtable_names[rti as usize - 1]
        .or_else(|| rte.eref.expect("RTE without eref").aliasname)
        .expect("scan RTE has a refname");
    if es.format == EXPLAIN_FORMAT_TEXT {
        append!(es, " on");
        if let Some(ns) = &namespace {
            let obj = objectname.expect("namespace implies objectname");
            append!(es, " {}.{}", quote_identifier(ns.as_str()), quote_identifier(obj));
        } else if let Some(obj) = objectname {
            append!(es, " {}", quote_identifier(obj));
        }
        if objectname != Some(refname) {
            append!(es, " {}", quote_identifier(refname));
        }
    } else {
        if let (Some(tag), Some(obj)) = (objecttag, objectname) {
            ExplainPropertyText(tag, obj, es);
        }
        if let Some(ns) = &namespace {
            ExplainPropertyText("Schema", ns.as_str(), es);
        }
        ExplainPropertyText("Alias", refname, es);
    }
    Ok(())
}

// ruleutils.c quote_identifier: bare-safe identifiers pass through, others
// come back double-quoted (format_type hosts the shared implementation).
fn quote_identifier(ident: &str) -> std::borrow::Cow<'_, str> {
    let bytes = ident.as_bytes();
    let safe = !bytes.is_empty()
        && (bytes[0].is_ascii_lowercase() || bytes[0] == b'_')
        && bytes
            .iter()
            .all(|&b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        && !crate::gucs::quote_all_identifiers()
        && {
            let kwnum = keywords::ScanKeywordLookup(bytes, &keywords::ScanKeywords);
            kwnum < 0
                || keywords::ScanKeywordCategories[kwnum as usize]
                    == keywords::KeywordCategory::Unreserved
        };
    if !safe {
        return format_type::quote_identifier(ident);
    }
    std::borrow::Cow::Borrowed(ident)
}
