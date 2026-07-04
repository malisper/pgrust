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
    append, ExplainCloseGroup, ExplainOpenGroup, ExplainPropertyBool, ExplainPropertyFloat,
    ExplainPropertyInteger, ExplainPropertyList, ExplainPropertyText,
};
use crate::options::str_in;
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
    ExplainPreScanNode(mcx, root, &pstmt.subplans, &mut rels_used)?;
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
    es.rtable_size = pstmt.rtable.len() as i32;
    for rte in pstmt.rtable.iter() {
        if rte.as_range_tbl_entry().expect("rtable holds RTEs").rtekind == RTEKind::RTE_GROUP {
            es.rtable_size -= 1;
            break;
        }
    }
    // Gather-invisible skip: Gather vocabulary is unported; plan_of is loud.
    ExplainNode(root, None, None, None, es)?;
    if es.settings {
        node_gap(
            "ExplainPrintSettings",
            "SETTINGS needs get_explain_guc_options (guc lane)",
        );
    }
    if es.verbose
        && pstmt.queryId != 0
        && guc_tables::backing::compute_query_id() != guc_tables::consts::COMPUTE_QUERY_ID_REGRESS
    {
        ExplainPropertyInteger("Query Identifier", None, pstmt.queryId, es);
    }
    Ok(())
}

fn ExplainPreScanNode<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    subplans: &NodeList<'mcx>,
    rels_used: &mut Bitmapset<'mcx>,
) -> PgResult<()> {
    match node.node_tag() {
        NodeTag::T_SeqScan => {
            rels_used.add_member(mcx, node.as_seq_scan().unwrap().scan.scanrelid as i32)?;
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
        NodeTag::T_WorkTableScan => {
            rels_used
                .add_member(mcx, node.as_work_table_scan().unwrap().scan.scanrelid as i32)?;
        }
        NodeTag::T_ValuesScan => {
            rels_used.add_member(mcx, node.as_values_scan().unwrap().scan.scanrelid as i32)?;
        }
        NodeTag::T_SubqueryScan => {
            let sq = node.as_subquery_scan().unwrap();
            rels_used.add_member(mcx, sq.scan.scanrelid as i32)?;
            ExplainPreScanNode(mcx, sq.subplan.expect("SubqueryScan subplan"), subplans, rels_used)?;
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
        NodeTag::T_Append => {
            let a = node.as_append().unwrap();
            rels_used.add_members(mcx, &a.apprelids)?;
            for child in &a.appendplans {
                ExplainPreScanNode(mcx, child, subplans, rels_used)?;
            }
        }
        // planstate_tree_walker's special-member leg for bitmap combiners.
        NodeTag::T_BitmapAnd => {
            for child in &node.as_bitmap_and().unwrap().bitmapplans {
                ExplainPreScanNode(mcx, child, subplans, rels_used)?;
            }
        }
        NodeTag::T_BitmapOr => {
            for child in &node.as_bitmap_or().unwrap().bitmapplans {
                ExplainPreScanNode(mcx, child, subplans, rels_used)?;
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
        let child = subplans.nth(sp.plan_id as usize - 1);
        ExplainPreScanNode(mcx, child, subplans, rels_used)?;
    }
    for sp in collect_node_subplans(mcx, node)?.iter() {
        let child = subplans.nth(sp.plan_id as usize - 1);
        ExplainPreScanNode(mcx, child, subplans, rels_used)?;
    }
    if let Some(l) = plan.lefttree {
        ExplainPreScanNode(mcx, l, subplans, rels_used)?;
    }
    if let Some(r) = plan.righttree {
        ExplainPreScanNode(mcx, r, subplans, rels_used)?;
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
        NodeTag::T_NestLoop | NodeTag::T_MergeJoin | NodeTag::T_HashJoin => {
            walk_list(&mut out, &plan.qual);
            let joinqual = match node.node_tag() {
                NodeTag::T_NestLoop => &node.as_nest_loop().unwrap().join.joinqual,
                NodeTag::T_MergeJoin => &node.as_merge_join().unwrap().join.joinqual,
                _ => &node.as_hash_join().unwrap().join.joinqual,
            };
            walk_list(&mut out, joinqual);
            walk_list(&mut out, &plan.targetlist);
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
        return;
    }
    match node.node_tag() {
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

    let pname = match node.node_tag() {
        NodeTag::T_Result => "Result",
        NodeTag::T_ProjectSet => "ProjectSet",
        NodeTag::T_Append => "Append",
        NodeTag::T_MergeAppend => {
            node_gap("ExplainNode", "MergeAppend display; MergeAppend lane unported (set-ops lane)")
        }
        NodeTag::T_SubqueryScan => "Subquery Scan",
        NodeTag::T_SetOp => match node.as_set_op().expect("SetOp plan node").strategy {
            SETOP_SORTED => "SetOp",
            SETOP_HASHED => "HashSetOp",
            other => node_gap("ExplainNode", &format!("SetOp strategy {other} unrecognized")),
        },
        NodeTag::T_SeqScan => "Seq Scan",
        NodeTag::T_TidScan => "Tid Scan",
        NodeTag::T_TidRangeScan => "Tid Range Scan",
        NodeTag::T_IndexScan => "Index Scan",
        NodeTag::T_IndexOnlyScan => "Index Only Scan",
        NodeTag::T_BitmapIndexScan => "Bitmap Index Scan",
        NodeTag::T_BitmapHeapScan => "Bitmap Heap Scan",
        NodeTag::T_BitmapAnd => "BitmapAnd",
        NodeTag::T_BitmapOr => "BitmapOr",
        NodeTag::T_FunctionScan => "Function Scan",
        NodeTag::T_TableFuncScan => "Table Function Scan",
        NodeTag::T_ValuesScan => "Values Scan",
        NodeTag::T_CteScan => "CTE Scan",
        NodeTag::T_WorkTableScan => "WorkTable Scan",
        NodeTag::T_RecursiveUnion => "Recursive Union",
        // C interpolates the join type into the node name in TEXT format:
        // "Hash"/"Merge" + " <Jointype> Join" (inner non-nestloop gets a bare
        // " Join"); see the jointype append below.
        NodeTag::T_NestLoop => "Nested Loop",
        NodeTag::T_HashJoin => "Hash",
        NodeTag::T_MergeJoin => "Merge",
        NodeTag::T_Hash => "Hash",
        NodeTag::T_Material => "Materialize",
        NodeTag::T_Memoize => "Memoize",
        NodeTag::T_Agg => {
            let agg = node.as_agg().expect("Agg plan node");
            assert!(
                agg.aggsplit == types_nodes::primnodes::AGGSPLIT_SIMPLE,
                "ExplainNode (explain.c): partial/finalize Agg display; parallel-agg lane"
            );
            match agg.aggstrategy {
                0 => "Aggregate",
                1 => "GroupAggregate",
                2 => "HashAggregate",
                3 => "MixedAggregate",
                other => node_gap("ExplainNode", &format!("Agg strategy {other} unrecognized")),
            }
        }
        NodeTag::T_Unique => "Unique",
        NodeTag::T_Sort => "Sort",
        NodeTag::T_IncrementalSort => "Incremental Sort",
        NodeTag::T_WindowAgg => "WindowAgg",
        NodeTag::T_Limit => "Limit",
        NodeTag::T_LockRows => "LockRows",
        NodeTag::T_ModifyTable => {
            let mt = node.as_modify_table().expect("ModifyTable plan node");
            assert!(
                mt.onConflictAction == 0,
                "ExplainNode (explain.c): ON CONFLICT display; upsert-explain lane"
            );
            match mt.operation {
                types_nodes::CmdType::CMD_INSERT => "Insert",
                types_nodes::CmdType::CMD_UPDATE => "Update",
                types_nodes::CmdType::CMD_DELETE => "Delete",
                types_nodes::CmdType::CMD_MERGE => "Merge",
                other => node_gap("ExplainNode", &format!("ModifyTable operation {other:?}")),
            }
        }
        t => node_gap("ExplainNode", &format!("{t:?} display arm unported (M2+ plan lanes)")),
    };

    ExplainOpenGroup("Plan", if relationship.is_some() { None } else { Some("Plan") }, true, es);

    if es.format != EXPLAIN_FORMAT_TEXT {
        crate::format::nontext_gap(es, "ExplainNode");
    }
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
    append!(es, "{pname}");
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
        if jt != types_nodes::JoinType::JOIN_INNER {
            append!(es, " {jtname} Join");
        } else if node.node_tag() != NodeTag::T_NestLoop {
            append!(es, " Join");
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
        append!(es, " {setopcmd}");
    }
    es.indent += 1;

    if node.node_tag() == NodeTag::T_SeqScan {
        ExplainScanTarget(node.as_seq_scan().unwrap().scan.scanrelid, es)?;
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
        // ExplainTargetRel's T_BitmapIndexScan arm: index name only.
        let mcx = es.str.allocator();
        let indexname = lsyscache::get_rel_name(mcx, bis.indexid)?
            .expect("explain_get_index_name: cache lookup failed");
        let indexname = str_in(mcx, indexname.as_str())?;
        append!(es, " on {}", quote_identifier(indexname));
    }
    if node.node_tag() == NodeTag::T_CteScan {
        ExplainScanTarget(node.as_cte_scan().unwrap().scan.scanrelid, es)?;
    }
    if node.node_tag() == NodeTag::T_WorkTableScan {
        ExplainScanTarget(node.as_work_table_scan().unwrap().scan.scanrelid, es)?;
    }
    if node.node_tag() == NodeTag::T_ValuesScan {
        ExplainScanTarget(node.as_values_scan().unwrap().scan.scanrelid, es)?;
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
        append!(
            es,
            "  (cost={:.2}..{:.2} rows={:.0} width={})",
            plan.startup_cost,
            plan.total_cost,
            plan.plan_rows,
            plan.plan_width
        );
    }

    // C reads planstate->instrument; the walk here is over the sealed Plan
    // tree, so per-node Instrumentation comes from the executor keyed by
    // plan_node_id (the fetch also runs C's forced InstrEndLoop).
    let instrument = if es.qd.is_null() {
        None
    } else {
        execmain_seams::query_desc_instrument::call(es.qd, plan.plan_node_id)
    };
    match instrument {
        Some(i) if es.analyze && i.nloops > 0.0 => {
            let nloops = i.nloops;
            let startup_ms = 1000.0 * i.startup / nloops;
            let total_ms = 1000.0 * i.total / nloops;
            let rows = i.ntuples / nloops;
            append!(es, " (actual ");
            if es.timing {
                append!(es, "time={startup_ms:.3}..{total_ms:.3} ");
            }
            append!(es, "rows={rows:.2} loops={nloops:.0})");
        }
        _ if es.analyze => append!(es, " (never executed)"),
        _ => {}
    }
    append!(es, "\n");

    let isdisabled = plan_is_disabled(node);
    if isdisabled {
        ExplainPropertyBool("Disabled", isdisabled, es);
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
        NodeTag::T_SeqScan
        | NodeTag::T_CteScan
        | NodeTag::T_ValuesScan
        | NodeTag::T_WorkTableScan
        | NodeTag::T_TableFuncScan => {
            if node.node_tag() == NodeTag::T_TableFuncScan && es.verbose {
                node_gap(
                    "show_table_func_scan_info",
                    "VERBOSE Table Function Call deparse (ruleutils get_tablefunc unported)",
                );
            }
            show_scan_qual(&plan.qual, "Filter", node, ancestors, es)?;
            if !plan.qual.is_nil() {
                show_instrumentation_count("Rows Removed by Filter", 1, &instrument, es);
            }
            if node.node_tag() == NodeTag::T_CteScan {
                show_ctescan_info(node, es);
            }
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
            filtered_count_gap(&nl.join.joinqual, es);
            show_upper_qual(&plan.qual, "Filter", node, ancestors, es)?;
            filtered_count_gap(&plan.qual, es);
        }
        NodeTag::T_HashJoin => {
            let hj = node.as_hash_join().unwrap();
            show_upper_qual(&hj.hashclauses, "Hash Cond", node, ancestors, es)?;
            show_upper_qual(&hj.join.joinqual, "Join Filter", node, ancestors, es)?;
            filtered_count_gap(&hj.join.joinqual, es);
            show_upper_qual(&plan.qual, "Filter", node, ancestors, es)?;
            filtered_count_gap(&plan.qual, es);
        }
        NodeTag::T_MergeJoin => {
            let mj = node.as_merge_join().unwrap();
            show_upper_qual(&mj.mergeclauses, "Merge Cond", node, ancestors, es)?;
            show_upper_qual(&mj.join.joinqual, "Join Filter", node, ancestors, es)?;
            filtered_count_gap(&mj.join.joinqual, es);
            show_upper_qual(&plan.qual, "Filter", node, ancestors, es)?;
            filtered_count_gap(&plan.qual, es);
        }
        NodeTag::T_Hash => {
            show_hash_info(node, es)?;
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
        NodeTag::T_IncrementalSort => {
            show_incremental_sort_keys(node, ancestors, es)?;
            show_incremental_sort_info(node, es)?;
        }
        NodeTag::T_WindowAgg => {
            show_window_def(node, ancestors, es)?;
            let w = node.as_window_agg().unwrap();
            debug_assert!(w.runCondition.is_nil());
            show_upper_qual(&plan.qual, "Filter", node, ancestors, es)?;
            filtered_count_gap(&plan.qual, es);
            show_windowagg_info(node, es);
        }
        NodeTag::T_Agg => {
            show_agg_keys(node, ancestors, es)?;
            show_upper_qual(&plan.qual, "Filter", node, ancestors, es)?;
            show_hashagg_info(node, es)?;
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
            filtered_count_gap(&plan.qual, es);
        }
        // Unique, Limit, Append, SetOp, LockRows, ProjectSet show nothing
        // extra without ANALYZE.
        NodeTag::T_Unique | NodeTag::T_Limit | NodeTag::T_Append | NodeTag::T_SetOp
        | NodeTag::T_LockRows | NodeTag::T_BitmapAnd | NodeTag::T_BitmapOr
        | NodeTag::T_ProjectSet | NodeTag::T_RecursiveUnion => {}
        // show_modifytable_info: FDW/ON CONFLICT/MERGE legs absent (asserted
        // at the name arm); nothing prints without ANALYZE.
        NodeTag::T_ModifyTable => {}
        _ => unreachable!(),
    }

    if es.buffers {
        if let Some(i) = &instrument {
            crate::show_buffer_usage(es, &i.bufusage);
        }
    }

    let pushed = Ancestors { entry: AncestorEntry::Plan(node), parent: ancestors };
    // ExplainSubPlans over initPlan.
    for sp_node in plan.initPlan.iter() {
        let sp = sp_node.as_sub_plan().expect("initPlan holds SubPlan nodes");
        explain_sub_plan(sp, "InitPlan", &pushed, es)?;
    }
    let haschildren = plan.lefttree.is_some()
        || plan.righttree.is_some()
        || node.node_tag() == NodeTag::T_Append
        || node.node_tag() == NodeTag::T_SubqueryScan
        || node.node_tag() == NodeTag::T_BitmapAnd
        || node.node_tag() == NodeTag::T_BitmapOr;
    if haschildren {
        ExplainOpenGroup("Plans", Some("Plans"), false, es);
    }
    if let Some(l) = plan.lefttree {
        ExplainNode(l, Some("Outer"), None, Some(&pushed), es)?;
    }
    if let Some(r) = plan.righttree {
        ExplainNode(r, Some("Inner"), None, Some(&pushed), es)?;
    }
    if let Some(a) = node.as_append() {
        for child in &a.appendplans {
            ExplainNode(child, Some("Member"), None, Some(&pushed), es)?;
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
    if haschildren {
        ExplainCloseGroup("Plans", Some("Plans"), false, es);
    }
    // ExplainSubPlans over planstate->subPlan.
    let member_subplans = collect_node_subplans(es.str.allocator(), node)?;
    for sp in member_subplans.iter() {
        explain_sub_plan(sp, "SubPlan", &pushed, es)?;
    }

    es.indent = save_indent;
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
        .nth(sp.plan_id as usize - 1);
    let sub = Ancestors { entry: AncestorEntry::Sub(sp), parent: Some(pushed) };
    ExplainNode(child, Some(relationship), sp.plan_name, Some(&sub), es)
}

fn show_plan_tlist<'mcx>(node: Node<'mcx>, ancestors: Option<&Ancestors<'_, 'mcx>>, es: &mut ExplainState<'mcx>) -> PgResult<()> {
    let plan = plan_of(node);
    if plan.targetlist.is_nil() {
        return Ok(());
    }
    if node.node_tag() == NodeTag::T_Append {
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
    if sort.numCols <= 0 {
        return Ok(());
    }
    let mcx = es.str.allocator();
    let useprefix = es.rtable_size > 1 || es.verbose;
    let mut result: PgVec<'mcx, PgString<'mcx>> = PgVec::new_in(mcx);
    let mut presorted: PgVec<'mcx, PgString<'mcx>> = PgVec::new_in(mcx);
    for keyno in 0..sort.numCols as usize {
        let resno = sort.sortColIdx[keyno];
        let tle = get_tle_by_resno(&sort.plan.targetlist, resno)
            .unwrap_or_else(|| node_gap("show_sort_group_keys", "no tlist entry for key column"));
        let mut buf = PgString::new_in(mcx);
        deparse_expr(es, node, ancestors, tle.expr, useprefix, true, &mut buf)?;
        if keyno < n_presorted_keys {
            presorted.push(PgString::from_str_in(buf.as_str(), mcx)?);
        }
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
    if n_presorted_keys > 0 {
        ExplainPropertyList("Presorted Key", &presorted, es);
    }
    Ok(())
}

// show_sort_info (explain.c); the shared_info worker stanza has no parallel
// lane. spaceUsed value diverges from C (arena vs palloc accounting) —
// notes/sort-explain-lane.md.
fn show_sort_info<'mcx>(node: Node<'mcx>, es: &mut ExplainState<'mcx>) -> PgResult<()> {
    if !es.analyze || es.qd.is_null() {
        return Ok(());
    }
    let id = plan_of(node).plan_node_id;
    let Some(si) = execmain_seams::query_desc_sort_instrument::call(es.qd, id) else {
        return Ok(());
    };
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
    Ok(())
}

// show_incremental_sort_group_info (explain.c), text format (non-text gaps
// upstream). Memory values inherit the sort-info divergence (arena vs palloc
// accounting) — notes/sort-explain-lane.md.
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
    let nmethods = METHOD_BITS.iter().filter(|m| group_info.sortMethods & m.bit() != 0).count();
    if indent {
        for _ in 0..es.indent * 2 {
            append!(es, " ");
        }
    }
    append!(es, "{} Groups: {}  Sort Method", group_label, group_info.groupCount);
    append!(es, "{}", if nmethods > 1 { "s: " } else { ": " });
    let mut emitted = 0;
    for m in METHOD_BITS.iter().filter(|m| group_info.sortMethods & m.bit() != 0) {
        if emitted > 0 {
            append!(es, ", ");
        }
        append!(es, "{}", m.name());
        emitted += 1;
    }
    if group_info.maxMemorySpaceUsed > 0 {
        let avg = group_info.totalMemorySpaceUsed / group_info.groupCount;
        append!(es, "  Average Memory: {}kB  Peak Memory: {}kB", avg, group_info.maxMemorySpaceUsed);
    }
    if group_info.maxDiskSpaceUsed > 0 {
        let avg = group_info.totalDiskSpaceUsed / group_info.groupCount;
        append!(es, "  Average Disk: {}kB  Peak Disk: {}kB", avg, group_info.maxDiskSpaceUsed);
    }
    append!(es, "\n");
}

// show_incremental_sort_info (explain.c); the shared_info worker stanza has
// no parallel lane.
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
            show_incremental_sort_group_info(&info.prefixsortGroupInfo, "Pre-sorted", true, es);
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

// show_hash_info (explain.c), text arm; the shared_info (parallel) merge has
// no parallel lane.
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
    if es.format != EXPLAIN_FORMAT_TEXT {
        crate::format::nontext_gap(es, "show_hash_info");
    }
    let space_peak_kb = hi.space_peak.div_ceil(1024);
    crate::format::ExplainIndentText(es);
    if hi.nbatch_original != hi.nbatch || hi.nbuckets_original != hi.nbuckets {
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
    show_grouping_set_keys(node, agg, None, ancestors, es)?;
    for chain_node in agg.chain.iter() {
        let aggnode = chain_node.as_agg().expect("Agg.chain cell");
        let sortnode = aggnode.plan.lefttree.and_then(Node::as_sort);
        show_grouping_set_keys(node, aggnode, sortnode, ancestors, es)?;
    }
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
    let keyname =
        if aggnode.aggstrategy == 2 || aggnode.aggstrategy == 3 { "Hash Key" } else { "Group Key" };

    if let Some(sort) = sortnode {
        let mut result: PgVec<'mcx, PgString<'mcx>> = PgVec::new_in(mcx);
        for keyno in 0..sort.numCols as usize {
            let resno = sort.sortColIdx[keyno];
            let tle = get_tle_by_resno(tlist, resno).unwrap_or_else(|| {
                node_gap("show_sort_group_keys", "no tlist entry for key column")
            });
            let mut buf = PgString::new_in(mcx);
            deparse_expr(es, node, ancestors, tle.expr, useprefix, true, &mut buf)?;
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
        es.indent += 1;
    }

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
            deparse_expr(es, node, ancestors, tle.expr, useprefix, true, &mut buf)?;
            result.push(buf);
        }
        if result.is_empty() {
            crate::format::ExplainPropertyText(keyname, "()", es);
        } else {
            ExplainPropertyList(keyname, &result, es);
        }
    }

    if sortnode.is_some() {
        es.indent -= 1;
    }
    Ok(())
}

// show_hashagg_info (explain.c), text arm; the parallel-worker display has no
// parallel lane. AGG_HASHED/AGG_MIXED only.
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
        node_gap("show_sortorder_options", "USING <op> needs get_opname + opfamily probe");
    };
    if nulls_first != reverse {
        buf.try_push_str(if nulls_first { " NULLS FIRST" } else { " NULLS LAST" })?;
    }
    Ok(())
}

// exprType over the sort-key shapes this display lane can carry.
fn execscan_expr_type(node: Node<'_>) -> types_core::primitive::Oid {
    match node.as_var() {
        Some(v) => v.vartype,
        None => node_gap("exprType", "non-Var sort key (nodeFuncs lane)"),
    }
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

// ExplainIndexScanDetails (explain.c), TEXT arm (nontext gapped upstream).
fn ExplainIndexScanDetails(
    indexid: types_core::Oid,
    indexorderdir: i32,
    es: &mut ExplainState<'_>,
) -> PgResult<()> {
    let mcx = es.str.allocator();
    let indexname = lsyscache::get_rel_name(mcx, indexid)?
        .unwrap_or_else(|| panic!("cache lookup failed for index {indexid}"));
    if indexorderdir < 0 {
        append!(es, " Backward");
    }
    append!(es, " using {}", quote_identifier(indexname.as_str()));
    Ok(())
}

// show_instrumentation_count (explain.c). Correct only where the executor
// counts nfiltered — index-recheck (which=2) on btree never filters, so the
// zero is genuine; the qual lane (which=1) keeps filtered_count_gap instead.
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

// show_tidbitmap_info (explain.c), text arm; parallel worker stats have no
// parallel lane.
fn show_tidbitmap_info<'mcx>(node: Node<'mcx>, es: &mut ExplainState<'mcx>) {
    if !es.analyze {
        return;
    }
    if es.format != EXPLAIN_FORMAT_TEXT {
        crate::format::nontext_gap(es, "show_tidbitmap_info");
    }
    let stats = if es.qd.is_null() {
        None
    } else {
        execmain_seams::query_desc_bitmap_instrument::call(es.qd, plan_of(node).plan_node_id)
    };
    let stats = stats.unwrap_or_default();
    if stats.exact_pages > 0 || stats.lossy_pages > 0 {
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
}

// show_storage_info (explain.c), text arm. maxSpace inherits the arena-vs-
// palloc accounting caveat only through tuplestore's chunk_space mirror
// (byte-exact vs C by construction; see tuplestore::chunk_space).
fn show_storage_info(stats: types_core::instrument::TuplestoreInstrumentation, es: &mut ExplainState<'_>) {
    if es.format != EXPLAIN_FORMAT_TEXT {
        crate::format::nontext_gap(es, "show_storage_info");
    }
    let kb = (stats.max_space + 1023) / 1024;
    crate::format::ExplainIndentText(es);
    append!(es, "Storage: {}  Maximum Storage: {}kB\n", stats.space_type.name(), kb);
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

fn show_ctescan_info<'mcx>(node: Node<'mcx>, es: &mut ExplainState<'mcx>) {
    if let Some(stats) = tuplestore_stats(node, es) {
        show_storage_info(stats, es);
    }
}

// show_instrumentation_count's nfiltered read: the executor never counts
// qual-filtered tuples (InstrCountFiltered, execScan.c), so printing would be
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
    append!(es, " on {}", quote_identifier(objectname));
    if objectname != refname {
        append!(es, " {}", quote_identifier(refname));
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
    let objectname = match rte.rtekind {
        RTEKind::RTE_RELATION => {
            relname = lsyscache::get_rel_name(mcx, rte.relid)?;
            relname.as_ref().map(|s| s.as_str())
        }
        RTEKind::RTE_CTE => rte.ctename,
        RTEKind::RTE_SUBQUERY | RTEKind::RTE_VALUES => None,
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
