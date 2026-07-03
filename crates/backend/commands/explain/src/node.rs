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
use types_nodes::primnodes::Const;
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

const BOOLOID: u32 = 16;
const INT4OID: u32 = 23;
const UNKNOWNOID: u32 = 705;
const NUMERICOID: u32 = 1700;

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
    es.rtable_names = select_rtable_names_for_explain(mcx, &pstmt.rtable, &rels_used)?;
    // deparse_context_for_plan_tree / printed_subplans: every consumer
    // (Var/subplan deparse) is loud below.
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
        NodeTag::T_SubqueryScan => {
            let sq = node.as_subquery_scan().unwrap();
            rels_used.add_member(mcx, sq.scan.scanrelid as i32)?;
            ExplainPreScanNode(mcx, sq.subplan.expect("SubqueryScan subplan"), subplans, rels_used)?;
        }
        NodeTag::T_Append => {
            let a = node.as_append().unwrap();
            rels_used.add_members(mcx, &a.apprelids)?;
            for child in &a.appendplans {
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

// ruleutils.c set_rtable_names slice, hosted here until ruleutils lands;
// its CATALOG row stays todo and points here.
fn select_rtable_names_for_explain<'mcx>(
    mcx: Mcx<'mcx>,
    rtable: &NodeList<'mcx>,
    rels_used: &Bitmapset<'mcx>,
) -> PgResult<PgVec<'mcx, Option<&'mcx str>>> {
    let mut names: PgVec<'mcx, Option<&'mcx str>> = PgVec::new_in(mcx);
    let mut counters: std::collections::HashMap<&'mcx str, i32> = std::collections::HashMap::new();
    for (i, rte_node) in rtable.iter().enumerate() {
        let rte = rte_node.as_range_tbl_entry().expect("rtable holds RTEs");
        let rtindex = i as i32 + 1;
        let refname: Option<&'mcx str> = if !rels_used.is_member(rtindex) {
            None
        } else if let Some(alias) = rte.alias {
            alias.aliasname
        } else if rte.rtekind == RTEKind::RTE_RELATION {
            match lsyscache::get_rel_name(mcx, rte.relid)? {
                Some(name) => Some(str_in(mcx, name.as_str())?),
                None => None,
            }
        } else if rte.rtekind == RTEKind::RTE_JOIN {
            None
        } else {
            rte.eref.expect("RTE without eref").aliasname
        };
        // Duplicate names take the C "_%d" unique-ifier (counter per base
        // name, resuming where the last collision left off). The NAMEDATALEN
        // clip leg is dead: identifiers are already truncated to 63 bytes and
        // "_%d" keeps modname under 64 only for >48-digit counters.
        let refname = match refname {
            Some(name) if names.iter().any(|n| *n == Some(name)) => {
                let counter = counters.entry(name).or_insert(0);
                loop {
                    *counter += 1;
                    let modname = format!("{name}_{counter}");
                    assert!(modname.len() < 64, "set_rtable_names: NAMEDATALEN clip leg");
                    if !names.iter().any(|n| *n == Some(modname.as_str())) {
                        break Some(str_in(mcx, &modname)?);
                    }
                }
            }
            other => other,
        };
        names.push(refname);
    }
    Ok(names)
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

#[derive(Clone, Copy)]
pub enum AncestorEntry<'mcx> {
    Plan(Node<'mcx>),
    Sub(&'mcx types_nodes::primnodes::SubPlan<'mcx>),
}

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
        NodeTag::T_IndexScan => "Index Scan",
        NodeTag::T_IndexOnlyScan => "Index Only Scan",
        NodeTag::T_BitmapIndexScan => "Bitmap Index Scan",
        NodeTag::T_BitmapHeapScan => "Bitmap Heap Scan",
        NodeTag::T_FunctionScan => "Function Scan",
        NodeTag::T_CteScan => "CTE Scan",
        // C interpolates the join type into the node name in TEXT format:
        // "Hash"/"Merge" + " <Jointype> Join" (inner non-nestloop gets a bare
        // " Join"); see the jointype append below.
        NodeTag::T_NestLoop => "Nested Loop",
        NodeTag::T_HashJoin => "Hash",
        NodeTag::T_MergeJoin => "Merge",
        NodeTag::T_Hash => "Hash",
        NodeTag::T_Material => "Materialize",
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
        NodeTag::T_SeqScan | NodeTag::T_CteScan => {
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
                    deparse_expr(es, node, ancestors, fexpr, true, &mut buf)?;
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
            show_sort_keys(node, es)?;
            show_sort_info(node, es)?;
        }
        NodeTag::T_IncrementalSort => {
            show_incremental_sort_keys(node, es)?;
            show_incremental_sort_info(node, es)?;
        }
        NodeTag::T_WindowAgg => {
            show_window_def(node, es)?;
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
        NodeTag::T_SubqueryScan => {
            show_scan_qual(&plan.qual, "Filter", node, ancestors, es)?;
            filtered_count_gap(&plan.qual, es);
        }
        // Unique, Limit, Append, SetOp, LockRows show nothing extra without ANALYZE.
        NodeTag::T_Unique | NodeTag::T_Limit | NodeTag::T_Append | NodeTag::T_SetOp
        | NodeTag::T_LockRows => {}
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
        || node.node_tag() == NodeTag::T_SubqueryScan;
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
        deparse_expr(es, node, ancestors, tle.expr, useprefix, &mut buf)?;
        result.push(buf);
    }
    ExplainPropertyList("Output", &result, es);
    Ok(())
}

// show_sort_keys -> show_sort_group_keys (explain.c), Var-only sort keys.
fn show_sort_keys<'mcx>(node: Node<'mcx>, es: &mut ExplainState<'mcx>) -> PgResult<()> {
    let sort = node.as_sort().expect("Sort node");
    show_sort_node_keys(sort, 0, es)
}

fn show_incremental_sort_keys<'mcx>(node: Node<'mcx>, es: &mut ExplainState<'mcx>) -> PgResult<()> {
    let isort = node.as_incremental_sort().expect("IncrementalSort node");
    show_sort_node_keys(&isort.sort, isort.nPresortedCols as usize, es)
}

fn show_sort_node_keys<'mcx>(
    sort: &types_nodes::plannodes::Sort<'mcx>,
    n_presorted_keys: usize,
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
        deparse_plan_var(sort.plan.lefttree, tle.expr, useprefix, es, &mut buf)?;
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
// the child plan's tlist. Divergence: C deparses with showimplicit=true; a
// top-level implicit cast on a group key prints without its ::type here.
fn show_agg_keys<'mcx>(node: Node<'mcx>, ancestors: Option<&Ancestors<'_, 'mcx>>, es: &mut ExplainState<'mcx>) -> PgResult<()> {
    let agg = node.as_agg().expect("Agg plan node");
    if agg.numCols <= 0 && agg.groupingSets.is_nil() {
        return Ok(());
    }
    if !agg.groupingSets.is_nil() {
        return show_grouping_sets(node, es);
    }
    let child = agg.plan.lefttree.expect("Agg has an outer plan");
    let child_tlist = &plan_of(child).targetlist;
    let mcx = es.str.allocator();
    let useprefix = es.rtable_size > 1 || es.verbose;
    let mut result: PgVec<'mcx, PgString<'mcx>> = PgVec::new_in(mcx);
    for &resno in agg.grpColIdx {
        let tle = get_tle_by_resno(child_tlist, resno)
            .unwrap_or_else(|| node_gap("show_sort_group_keys", "no tlist entry for key column"));
        let mut buf = PgString::new_in(mcx);
        deparse_expr(es, child, ancestors, tle.expr, useprefix, &mut buf)?;
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
fn show_grouping_sets<'mcx>(node: Node<'mcx>, es: &mut ExplainState<'mcx>) -> PgResult<()> {
    let agg = node.as_agg().expect("Agg plan node");
    let child = agg.plan.lefttree.expect("Agg has an outer plan");
    show_grouping_set_keys(child, agg, None, es)?;
    for chain_node in agg.chain.iter() {
        let aggnode = chain_node.as_agg().expect("Agg.chain cell");
        let sortnode = aggnode.plan.lefttree.and_then(Node::as_sort);
        show_grouping_set_keys(child, aggnode, sortnode, es)?;
    }
    Ok(())
}

fn show_grouping_set_keys<'mcx>(
    child: Node<'mcx>,
    aggnode: &types_nodes::plannodes::Agg<'mcx>,
    sortnode: Option<&types_nodes::plannodes::Sort<'mcx>>,
    es: &mut ExplainState<'mcx>,
) -> PgResult<()> {
    let mcx = es.str.allocator();
    let useprefix = es.rtable_size > 1 || es.verbose;
    let child_tlist = &plan_of(child).targetlist;
    let keyname =
        if aggnode.aggstrategy == 2 || aggnode.aggstrategy == 3 { "Hash Key" } else { "Group Key" };

    if let Some(sort) = sortnode {
        let mut result: PgVec<'mcx, PgString<'mcx>> = PgVec::new_in(mcx);
        for keyno in 0..sort.numCols as usize {
            let resno = sort.sortColIdx[keyno];
            let tle = get_tle_by_resno(child_tlist, resno).unwrap_or_else(|| {
                node_gap("show_sort_group_keys", "no tlist entry for key column")
            });
            let mut buf = PgString::new_in(mcx);
            deparse_expr(es, child, None, tle.expr, useprefix, &mut buf)?;
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
            let tle = get_tle_by_resno(child_tlist, keyresno).unwrap_or_else(|| {
                node_gap("show_grouping_set_keys", "no tlist entry for key column")
            });
            let mut buf = PgString::new_in(mcx);
            deparse_expr(es, child, None, tle.expr, useprefix, &mut buf)?;
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
fn show_window_def<'mcx>(node: Node<'mcx>, es: &mut ExplainState<'mcx>) -> PgResult<()> {
    use types_nodes::rawnodes::FRAMEOPTION_NONDEFAULT;
    let wagg = node.as_window_agg().expect("WindowAgg node");
    let mcx = es.str.allocator();
    let mut buf = PgString::new_in(mcx);
    buf.try_push_str(&quote_identifier(wagg.winname.expect("named window (name_active_windows)")))?;
    buf.try_push_str(" AS (")?;
    let child = wagg.plan.lefttree;
    let mut needspace = false;
    let mut keys = |buf: &mut PgString<'mcx>,
                    es: &mut ExplainState<'mcx>,
                    idx: &[i16]|
     -> PgResult<()> {
        let useprefix = es.rtable_size > 1 || es.verbose;
        let child_tlist = &plan_of(child.expect("WindowAgg has a child")).targetlist;
        for (i, &resno) in idx.iter().enumerate() {
            if i > 0 {
                buf.try_push_str(", ")?;
            }
            let tle = get_tle_by_resno(child_tlist, resno)
                .unwrap_or_else(|| node_gap("show_window_keys", "no tlist entry for key column"));
            deparse_plan_var(plan_of(child.unwrap()).lefttree, tle.expr, useprefix, es, buf)?;
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
fn get_window_frame_options<'mcx>(
    frame_options: i32,
    start_offset: Option<Node<'mcx>>,
    end_offset: Option<Node<'mcx>>,
    plan_node: Node<'mcx>,
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
        deparse_expr(es, plan_node, None, start_offset.expect("startOffset"), useprefix, buf)?;
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
            deparse_expr(es, plan_node, None, end_offset.expect("endOffset"), useprefix, buf)?;
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

// get_rule_expr/get_variable (ruleutils.c) reduced to the plan-tree Var walk:
// OUTER_VAR resolves through the child tlist, base Vars through eref colnames.
fn deparse_plan_var<'mcx>(
    child: Option<Node<'mcx>>,
    expr: Node<'mcx>,
    useprefix: bool,
    es: &ExplainState<'mcx>,
    buf: &mut PgString<'mcx>,
) -> PgResult<()> {
    let Some(var) = expr.as_var() else {
        node_gap(
            "deparse_expression",
            &format!("{:?} deparse unported (ruleutils lane)", expr.node_tag()),
        );
    };
    if var.varno == types_nodes::primnodes::OUTER_VAR {
        let child = child.unwrap_or_else(|| node_gap("get_variable", "OUTER_VAR without child"));
        let child_plan = plan_of(child);
        let tle = get_tle_by_resno(&child_plan.targetlist, var.varattno)
            .unwrap_or_else(|| node_gap("get_variable", "bogus varattno for OUTER_VAR"));
        if tle.expr.as_var().is_none() {
            buf.try_push('(')?;
            deparse_expr(es, child, None, tle.expr, useprefix, buf)?;
            buf.try_push(')')?;
            return Ok(());
        }
        return deparse_plan_var(outer_child(child), tle.expr, useprefix, es, buf);
    }
    if var.varno <= 0 || var.varno as usize > es.rtable_size as usize {
        node_gap("get_variable", "INNER_VAR/INDEX_VAR deparse unported (ruleutils lane)");
    }
    let rte: &RangeTblEntry<'_> = es
        .rtable
        .expect("deparse before ExplainPrintPlan")
        .nth(var.varno as usize - 1)
        .as_range_tbl_entry()
        .expect("rtable holds RTEs");
    let eref = rte.eref.expect("RTE without eref");
    if useprefix {
        let refname = es.rtable_names[var.varno as usize - 1]
            .or(eref.aliasname)
            .expect("deparsed Var's RTE has a refname");
        buf.try_push_str(&quote_identifier(refname))?;
        buf.try_push('.')?;
    }
    debug_assert!(var.varattno > 0, "system/whole-row Var deparse is a loud upstream lane");
    let colname = eref
        .colnames
        .nth(var.varattno as usize - 1)
        .as_string()
        .expect("eref colnames hold String nodes")
        .sval;
    buf.try_push_str(&quote_identifier(colname))?;
    Ok(())
}

// show_sortorder_options (explain.c); USING and COLLATE arms are loud.
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
        node_gap("show_sortorder_options", "COLLATE needs generate_collation_name");
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
        deparse_expr(es, node, ancestors, list.nth(0), useprefix, &mut buf)?;
    } else {
        buf.try_push('(')?;
        for (i, item) in list.iter().enumerate() {
            if i > 0 {
                buf.try_push_str(" AND ")?;
            }
            deparse_expr(es, node, ancestors, item, useprefix, &mut buf)?;
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
    deparse_expr(es, node, ancestors, expr, useprefix, &mut buf)?;
    crate::format::ExplainPropertyText(qlabel, buf.as_str(), es);
    Ok(())
}

// ruleutils.c deparse_expression slice: Const, Var (incl OUTER/INNER
// indirection through child tlists), binary OpExpr, implicit RelabelType;
// every other node tag is loud. Its CATALOG row stays todo and points here.
fn deparse_expr<'mcx>(
    es: &ExplainState<'mcx>,
    plan_node: Node<'mcx>,
    ancestors: Option<&Ancestors<'_, 'mcx>>,
    expr: Node<'mcx>,
    useprefix: bool,
    buf: &mut PgString<'mcx>,
) -> PgResult<()> {
    match expr.node_tag() {
        NodeTag::T_Const => get_const_expr(expr.as_const().unwrap(), buf, 0),
        NodeTag::T_Var => deparse_var(es, plan_node, ancestors, expr.as_var().unwrap(), useprefix, buf),
        NodeTag::T_OpExpr => {
            let o = expr.as_op_expr().unwrap();
            if o.args.len() != 2 {
                node_gap("get_oper_expr", "unary operator deparse (ruleutils lane)");
            }
            let opname = lsyscache::get_opname(es.str.allocator(), o.opno)?
                .expect("operator of a planned expression exists");
            buf.try_push('(')?;
            deparse_expr(es, plan_node, ancestors, o.args.nth(0), useprefix, buf)?;
            write!(buf, " {} ", opname.as_str()).expect("PgString write");
            deparse_expr(es, plan_node, ancestors, o.args.nth(1), useprefix, buf)?;
            buf.try_push(')')?;
            Ok(())
        }
        NodeTag::T_RelabelType => {
            let r = expr.as_relabel_type().unwrap();
            if r.relabelformat != types_nodes::CoercionForm::COERCE_IMPLICIT_CAST {
                node_gap("get_rule_expr", "explicit RelabelType deparse (ruleutils lane)");
            }
            deparse_expr(es, plan_node, ancestors, r.arg, useprefix, buf)
        }
        // get_rule_expr T_BoolExpr, non-pretty form: outer parens always.
        NodeTag::T_BoolExpr => {
            use types_nodes::primnodes::BoolExprType;
            let b = expr.as_bool_expr().unwrap();
            match b.boolop {
                BoolExprType::AND_EXPR | BoolExprType::OR_EXPR => {
                    let sep = if b.boolop == BoolExprType::AND_EXPR { " AND " } else { " OR " };
                    buf.try_push('(')?;
                    for (i, arg) in b.args.iter().enumerate() {
                        if i > 0 {
                            buf.try_push_str(sep)?;
                        }
                        deparse_expr(es, plan_node, ancestors, arg, useprefix, buf)?;
                    }
                    buf.try_push(')')?;
                    Ok(())
                }
                BoolExprType::NOT_EXPR => {
                    buf.try_push_str("(NOT ")?;
                    deparse_expr(es, plan_node, ancestors, b.args.nth(0), useprefix, buf)?;
                    buf.try_push(')')?;
                    Ok(())
                }
            }
        }
        // get_rule_expr T_NullTest, non-pretty form: outer parens always;
        // scalar tests only (a row-type arg deparses as IS [NOT] DISTINCT
        // FROM NULL in C and is loud here).
        NodeTag::T_NullTest => {
            use types_nodes::primnodes::NullTestType;
            let nt = expr.as_null_test().unwrap();
            let arg = nt.arg.expect("NullTest.arg");
            if !nt.argisrow && lsyscache::type_is_rowtype(deparse_expr_type(arg))? {
                node_gap("get_rule_expr", "row-type NullTest deparse (ruleutils lane)");
            }
            buf.try_push('(')?;
            deparse_expr(es, plan_node, ancestors, arg, useprefix, buf)?;
            buf.try_push_str(match nt.nulltesttype {
                NullTestType::IS_NULL => " IS NULL",
                NullTestType::IS_NOT_NULL => " IS NOT NULL",
            })?;
            buf.try_push(')')?;
            Ok(())
        }
        // get_agg_expr (ruleutils.c) plain-agg slice; the name prints
        // unqualified (generate_function_name's visibility probe unported —
        // a shadowed aggregate would deparse without C's schema prefix).
        NodeTag::T_Aggref => {
            let a = expr.as_aggref().unwrap();
            if !a.aggdistinct.is_nil()
                || !a.aggorder.is_nil()
                || a.aggfilter.is_some()
                || a.aggvariadic
                || !a.aggdirectargs.is_nil()
                || a.aggsplit != types_nodes::primnodes::AGGSPLIT_SIMPLE
            {
                node_gap(
                    "get_agg_expr",
                    "DISTINCT/ORDER BY/FILTER/variadic/ordered-set/partial \
                     aggregate deparse (ruleutils lane)",
                );
            }
            let name = lsyscache::get_func_name(es.str.allocator(), a.aggfnoid)?
                .expect("aggregate of a planned expression exists");
            write!(buf, "{}(", name.as_str()).expect("PgString write");
            if a.aggstar {
                buf.try_push('*')?;
            } else {
                let mut nargs = 0;
                for tle_node in a.args.iter() {
                    let tle =
                        tle_node.as_target_entry().expect("Aggref args hold TargetEntries");
                    if tle.resjunk {
                        continue;
                    }
                    if nargs > 0 {
                        buf.try_push_str(", ")?;
                    }
                    nargs += 1;
                    deparse_expr(es, plan_node, ancestors, tle.expr, useprefix, buf)?;
                }
            }
            buf.try_push(')')?;
            Ok(())
        }
        // get_rule_expr T_SQLValueFunction (datetime ops; name ops are loud
        // with their grammar arms).
        NodeTag::T_SQLValueFunction => {
            use types_nodes::primnodes::SQLValueFunctionOp as Op;
            let svf = expr.as_sql_value_function().unwrap();
            let kw = match svf.op {
                Op::SVFOP_CURRENT_DATE => "CURRENT_DATE",
                Op::SVFOP_CURRENT_TIME | Op::SVFOP_CURRENT_TIME_N => "CURRENT_TIME",
                Op::SVFOP_CURRENT_TIMESTAMP | Op::SVFOP_CURRENT_TIMESTAMP_N => {
                    "CURRENT_TIMESTAMP"
                }
                Op::SVFOP_LOCALTIME | Op::SVFOP_LOCALTIME_N => "LOCALTIME",
                Op::SVFOP_LOCALTIMESTAMP | Op::SVFOP_LOCALTIMESTAMP_N => "LOCALTIMESTAMP",
                other => node_gap(
                    "get_rule_expr",
                    &format!("SQLValueFunction {other:?} deparse (ruleutils lane)"),
                ),
            };
            buf.try_push_str(kw)?;
            if matches!(
                svf.op,
                Op::SVFOP_CURRENT_TIME_N
                    | Op::SVFOP_CURRENT_TIMESTAMP_N
                    | Op::SVFOP_LOCALTIME_N
                    | Op::SVFOP_LOCALTIMESTAMP_N
            ) {
                write!(buf, "({})", svf.typmod).expect("PgString write");
            }
            Ok(())
        }
        // get_rule_expr T_SubPlan: reference the subplan by name; a testexpr
        // shows the combining expression instead, with its output Params
        // rendered by the Param arm below.
        NodeTag::T_SubPlan => {
            use types_nodes::primnodes::SubLinkType;
            let sp = expr.as_sub_plan().unwrap();
            let prefix = match sp.subLinkType {
                SubLinkType::EXISTS_SUBLINK => "EXISTS(",
                SubLinkType::ALL_SUBLINK => "(ALL ",
                SubLinkType::ANY_SUBLINK => "(ANY ",
                SubLinkType::ROWCOMPARE_SUBLINK | SubLinkType::EXPR_SUBLINK => "(",
                SubLinkType::MULTIEXPR_SUBLINK => "(rescan ",
                SubLinkType::ARRAY_SUBLINK => "ARRAY(",
                SubLinkType::CTE_SUBLINK => "CTE(",
            };
            buf.try_push_str(prefix)?;
            if let Some(te) = sp.testexpr {
                let sub = Ancestors { entry: AncestorEntry::Sub(sp), parent: ancestors };
                deparse_expr(es, plan_node, Some(&sub), te, useprefix, buf)?;
                buf.try_push(')')?;
            } else {
                if sp.useHashTable {
                    buf.try_push_str("hashed ")?;
                }
                buf.try_push_str(sp.plan_name.expect("planned SubPlan has a name"))?;
                buf.try_push(')')?;
            }
            Ok(())
        }
        NodeTag::T_Param => {
            deparse_param(es, plan_node, ancestors, expr.as_param().unwrap(), buf)
        }
        // get_func_expr (ruleutils.c) plain-call slice; the name prints
        // unqualified (generate_function_name visibility divergence, as the
        // Aggref arm). Cast-form and variadic calls are loud.
        NodeTag::T_FuncExpr => {
            use types_nodes::CoercionForm;
            let f = expr.as_func_expr().unwrap();
            match f.funcformat {
                CoercionForm::COERCE_IMPLICIT_CAST => {
                    // showimplicit=false context: print the bare argument.
                    return deparse_expr(es, plan_node, ancestors, f.args.nth(0), useprefix, buf);
                }
                CoercionForm::COERCE_EXPLICIT_CAST => {
                    node_gap("get_func_expr", "explicit-cast FuncExpr deparse (ruleutils lane)")
                }
                _ => {}
            }
            if f.funcvariadic {
                node_gap("get_func_expr", "VARIADIC deparse (ruleutils lane)");
            }
            let name = lsyscache::get_func_name(es.str.allocator(), f.funcid)?
                .expect("function of a planned expression exists");
            write!(buf, "{}(", name.as_str()).expect("PgString write");
            for (i, arg) in f.args.iter().enumerate() {
                if i > 0 {
                    buf.try_push_str(", ")?;
                }
                deparse_expr(es, plan_node, ancestors, arg, useprefix, buf)?;
            }
            buf.try_push(')')?;
            Ok(())
        }
        // get_windowfunc_expr (ruleutils.c), EXPLAIN leg: OVER prints the
        // owning WindowAgg's winname. The name prints unqualified (same
        // generate_function_name visibility divergence as the Aggref arm).
        NodeTag::T_WindowFunc => {
            let w = expr.as_window_func().unwrap();
            if w.aggfilter.is_some() {
                node_gap("get_windowfunc_expr", "FILTER deparse (ruleutils lane)");
            }
            let name = lsyscache::get_func_name(es.str.allocator(), w.winfnoid)?
                .expect("window function of a planned expression exists");
            write!(buf, "{}(", name.as_str()).expect("PgString write");
            if w.winstar {
                buf.try_push('*')?;
            } else {
                for (i, arg) in w.args.iter().enumerate() {
                    if i > 0 {
                        buf.try_push_str(", ")?;
                    }
                    deparse_expr(es, plan_node, ancestors, arg, useprefix, buf)?;
                }
            }
            buf.try_push_str(") OVER ")?;
            let mut winname = plan_node
                .as_window_agg()
                .filter(|wagg| wagg.winref == w.winref)
                .map(|wagg| wagg.winname);
            let mut chain = ancestors;
            while winname.is_none() {
                let Some(a) = chain else {
                    node_gap(
                        "get_windowfunc_expr",
                        &format!("could not find window clause for winref {}", w.winref),
                    );
                };
                if let AncestorEntry::Plan(pn) = a.entry {
                    winname = pn
                        .as_window_agg()
                        .filter(|wagg| wagg.winref == w.winref)
                        .map(|wagg| wagg.winname);
                }
                chain = a.parent;
            }
            let winname = winname
                .flatten()
                .expect("planned WindowAgg has a winname (name_active_windows)");
            buf.try_push_str(&quote_identifier(winname))?;
            Ok(())
        }
        // get_rule_expr T_CaseExpr, non-pretty form; arg-form WHENs show only
        // the RHS of the parser-built "CaseTestExpr = RHS" (as C, punting to
        // the full expression when the shape is not recognized).
        NodeTag::T_CaseExpr => {
            let c = expr.as_case_expr().unwrap();
            buf.try_push_str("CASE")?;
            if let Some(arg) = c.arg {
                buf.try_push(' ')?;
                deparse_expr(es, plan_node, ancestors, arg, useprefix, buf)?;
            }
            for cell in c.args.iter() {
                let cw = cell.as_case_when().expect("CaseWhen");
                let mut w = cw.expr.expect("CaseWhen.expr");
                if c.arg.is_some() {
                    if let Some(o) = w.as_op_expr() {
                        if o.args.len() == 2
                            && nodes_core::strip_implicit_coercions(o.args.nth(0)).node_tag()
                                == NodeTag::T_CaseTestExpr
                        {
                            w = o.args.nth(1);
                        }
                    }
                }
                buf.try_push_str(" WHEN ")?;
                deparse_expr(es, plan_node, ancestors, w, useprefix, buf)?;
                buf.try_push_str(" THEN ")?;
                deparse_expr(
                    es,
                    plan_node,
                    ancestors,
                    cw.result.expect("CaseWhen.result"),
                    useprefix,
                    buf,
                )?;
            }
            buf.try_push_str(" ELSE ")?;
            deparse_expr(
                es,
                plan_node,
                ancestors,
                c.defresult.expect("transformCaseExpr always adds a default"),
                useprefix,
                buf,
            )?;
            buf.try_push_str(" END")?;
            Ok(())
        }
        // C: only reachable in optimized expressions (see CaseExpr comment).
        NodeTag::T_CaseTestExpr => {
            buf.try_push_str("CASE_TEST_EXPR")?;
            Ok(())
        }
        // get_rule_expr T_ScalarArrayOpExpr, non-pretty form.
        NodeTag::T_ScalarArrayOpExpr => {
            let sa = expr.as_scalar_array_op_expr().unwrap();
            let opname = lsyscache::get_opname(es.str.allocator(), sa.opno)?
                .expect("operator of a planned expression exists");
            buf.try_push('(')?;
            deparse_expr(es, plan_node, ancestors, sa.args.nth(0), useprefix, buf)?;
            write!(buf, " {} {} (", opname.as_str(), if sa.useOr { "ANY" } else { "ALL" })
                .expect("PgString write");
            deparse_expr(es, plan_node, ancestors, sa.args.nth(1), useprefix, buf)?;
            buf.try_push_str("))")?;
            Ok(())
        }
        other => node_gap(
            "deparse_expression",
            &format!("{other:?} deparse unported (ruleutils lane)"),
        ),
    }
}

// get_parameter (ruleutils.c): a PARAM_EXEC prints as its referent expression
// (input to the subplan being displayed) or as a subplan-output reference
// "(SubPlan n).colN"; anything unresolved prints "$n".
fn deparse_param<'mcx>(
    es: &ExplainState<'mcx>,
    plan_node: Node<'mcx>,
    ancestors: Option<&Ancestors<'_, 'mcx>>,
    p: &types_nodes::primnodes::Param,
    buf: &mut PgString<'mcx>,
) -> PgResult<()> {
    use types_nodes::primnodes::ParamKind;
    if p.paramkind == ParamKind::PARAM_EXEC {
        // find_param_referent: an ancestral SubPlan passing this param down.
        let mut chain = ancestors;
        while let Some(a) = chain {
            if let AncestorEntry::Sub(sp) = a.entry {
                if let Some(i) = sp.parParam.iter().position(|id| id == p.paramid) {
                    let arg = sp.args.nth(i);
                    // push_ancestor_plan: deparse in the SubPlan's owner
                    // node's context, Var prefixes forced.
                    let mut owner = a.parent;
                    let owner_plan = loop {
                        match owner {
                            Some(o) => match o.entry {
                                AncestorEntry::Plan(pn) => break pn,
                                AncestorEntry::Sub(_) => owner = o.parent,
                            },
                            None => node_gap("get_parameter", "SubPlan ancestor without a plan"),
                        }
                    };
                    let need_paren = !matches!(
                        arg.node_tag(),
                        NodeTag::T_Var | NodeTag::T_Aggref | NodeTag::T_Param
                    );
                    if need_paren {
                        buf.try_push('(')?;
                    }
                    deparse_expr(es, owner_plan, owner.and_then(|o| o.parent), arg, true, buf)?;
                    if need_paren {
                        buf.try_push(')')?;
                    }
                    return Ok(());
                }
            }
            chain = a.parent;
        }
        // find_param_generator: subplan/initplan output columns.
        if let Some((name, hashed, col)) = find_param_generator(plan_node, ancestors, p.paramid) {
            write!(
                buf,
                "({}{}).col{}",
                if hashed { "hashed " } else { "" },
                name,
                col + 1
            )
            .expect("PgString write");
            return Ok(());
        }
    }
    write!(buf, "${}", p.paramid).expect("PgString write");
    Ok(())
}

fn find_param_generator<'mcx>(
    plan_node: Node<'mcx>,
    ancestors: Option<&Ancestors<'_, 'mcx>>,
    paramid: i32,
) -> Option<(&'mcx str, bool, usize)> {
    let check_initplans = |n: Node<'mcx>| -> Option<(&'mcx str, bool, usize)> {
        for sp_node in plan_of(n).initPlan.iter() {
            let sp = sp_node.as_sub_plan().expect("initPlan holds SubPlan nodes");
            if let Some(i) = sp.setParam.iter().position(|id| id == paramid) {
                return Some((sp.plan_name.expect("named"), sp.useHashTable, i));
            }
        }
        None
    };
    if let Some(hit) = check_initplans(plan_node) {
        return Some(hit);
    }
    let mut chain = ancestors;
    while let Some(a) = chain {
        match a.entry {
            AncestorEntry::Sub(sp) => {
                if let Some(i) = sp.paramIds.iter().position(|id| id == paramid) {
                    return Some((sp.plan_name.expect("named"), sp.useHashTable, i));
                }
            }
            AncestorEntry::Plan(pn) => {
                if let Some(hit) = check_initplans(pn) {
                    return Some(hit);
                }
            }
        }
        chain = a.parent;
    }
    None
}

// exprType (nodeFuncs.c) over the tags deparse_expr accepts.
fn deparse_expr_type(node: Node<'_>) -> types_core::Oid {
    match node.node_tag() {
        NodeTag::T_Const => node.as_const().unwrap().consttype,
        NodeTag::T_Var => node.as_var().unwrap().vartype,
        NodeTag::T_OpExpr => node.as_op_expr().unwrap().opresulttype,
        NodeTag::T_Aggref => node.as_aggref().unwrap().aggtype,
        NodeTag::T_WindowFunc => node.as_window_func().unwrap().wintype,
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().funcresulttype,
        NodeTag::T_RelabelType => node.as_relabel_type().unwrap().resulttype,
        NodeTag::T_BoolExpr | NodeTag::T_NullTest | NodeTag::T_ScalarArrayOpExpr => {
            types_core::catalog::BOOLOID
        }
        NodeTag::T_CaseExpr => node.as_case_expr().unwrap().casetype,
        NodeTag::T_CaseTestExpr => node.as_case_test_expr().unwrap().typeId,
        other => node_gap("exprType", &format!("{other:?} (ruleutils deparse lane)")),
    }
}

// get_variable (ruleutils.c) slice: setrefs' OUTER_VAR/INNER_VAR retargets
// resolve through the child plan's tlist (C's deparse namespace ancestry);
// the base Var prints as [refname.]eref-colname.
fn deparse_var<'mcx>(
    es: &ExplainState<'mcx>,
    plan_node: Node<'mcx>,
    ancestors: Option<&Ancestors<'_, 'mcx>>,
    var: &types_nodes::Var<'mcx>,
    useprefix: bool,
    buf: &mut PgString<'mcx>,
) -> PgResult<()> {
    let (varno, varattno) = match resolve_plan_var(plan_node, var.varno, var.varattno) {
        ResolvedVar::Base(v, a) => (v, a),
        // C get_variable: a non-Var referent prints parenthesized.
        ResolvedVar::Expr(expr, ctx) => {
            buf.try_push('(')?;
            deparse_expr(es, ctx, ancestors, expr, useprefix, buf)?;
            buf.try_push(')')?;
            return Ok(());
        }
    };
    if varattno <= 0 {
        node_gap("get_variable", "whole-row/system column deparse (ruleutils lane)");
    }
    let rtable = es.rtable.expect("deparse before rtable capture");
    debug_assert!(varno >= 1 && varno as usize <= rtable.len());
    let rte = rtable.nth(varno as usize - 1).as_range_tbl_entry().expect("rtable cell");
    let eref = rte.eref.expect("analyzed RTE always has eref");
    if useprefix {
        let refname = es.rtable_names[varno as usize - 1]
            .or(eref.aliasname)
            .expect("relation RTE has a refname");
        push_identifier(buf, refname)?;
        buf.try_push('.')?;
    }
    let colname = eref
        .colnames
        .nth(varattno as usize - 1)
        .as_string()
        .expect("eref colnames are String nodes")
        .sval;
    push_identifier(buf, colname)
}

// ruleutils set_deparse_plan: Append's OUTER referent is its first member.
fn outer_child(plan_node: Node<'_>) -> Option<Node<'_>> {
    match plan_node.as_append() {
        Some(a) => Some(a.appendplans.nth(0)),
        None => plan_of(plan_node).lefttree,
    }
}

enum ResolvedVar<'mcx> {
    Base(i32, i16),
    Expr(Node<'mcx>, Node<'mcx>),
}

fn resolve_plan_var<'mcx>(plan_node: Node<'mcx>, varno: i32, varattno: i16) -> ResolvedVar<'mcx> {
    // INDEX_VAR: dpns->index_tlist (set_deparse_plan); entries are heap Vars.
    if varno == types_nodes::primnodes::INDEX_VAR {
        let Some(ios) = plan_node.as_index_only_scan() else {
            node_gap("get_variable", "INDEX_VAR outside IndexOnlyScan (ruleutils lane)");
        };
        let tle = find_tle_by_resno(&ios.indextlist, varattno);
        let Some(v) = tle.expr.as_var() else {
            node_gap("get_variable", "non-Var indextlist deparse (ruleutils lane)");
        };
        return ResolvedVar::Base(v.varno, v.varattno);
    }
    let child = match varno {
        types_nodes::primnodes::OUTER_VAR => outer_child(plan_node),
        types_nodes::primnodes::INNER_VAR => plan_of(plan_node).righttree,
        _ => return ResolvedVar::Base(varno, varattno),
    };
    let child = child.expect("OUTER/INNER var without child plan");
    let tle = plan_of(child)
        .targetlist
        .nth(varattno as usize - 1)
        .as_target_entry()
        .expect("tlist cell");
    debug_assert_eq!(tle.resno, varattno);
    match tle.expr.as_var() {
        Some(v) => resolve_plan_var(child, v.varno, v.varattno),
        None => ResolvedVar::Expr(tle.expr, child),
    }
}

// indexed_tlist probes match on resno, not list position (resjunk entries
// may be interspersed).
fn find_tle_by_resno<'a, 'mcx>(
    tlist: &'a NodeList<'mcx>,
    resno: i16,
) -> &'mcx types_nodes::primnodes::TargetEntry<'mcx> {
    for tle_node in tlist.iter() {
        let tle = tle_node.as_target_entry().expect("TargetEntry");
        if tle.resno == resno {
            return tle;
        }
    }
    node_gap("get_variable", "INDEX_VAR resno missing from indextlist");
}

fn push_identifier<'mcx>(buf: &mut PgString<'mcx>, name: &str) -> PgResult<()> {
    buf.try_push_str(&format_type::quote_identifier(name))?;
    Ok(())
}

pub(crate) fn get_const_expr(c: &Const, buf: &mut PgString<'_>, showtype: i32) -> PgResult<()> {
    if c.constisnull {
        buf.try_push_str("NULL")?;
        if showtype >= 0 {
            let t = format_type::format_type_with_typemod(c.consttype, c.consttypmod)?;
            write!(buf, "::{t}").expect("PgString write");
            get_const_collation(c, buf)?;
        }
        return Ok(());
    }

    let (typoutput, _typisvarlena) = lsyscache::typ::getTypeOutputInfo(c.consttype)?;
    let mut finfo = fmgr_seams::fmgr_info::call(typoutput)?;
    let mut fcinfo = types_fmgr::LocalFcinfo::<1>::fresh(types_core::primitive::InvalidOid);
    // SAFETY: buf's arena outlives this single output-function call.
    unsafe { fcinfo.set_result_mcx(buf.allocator()) };
    fcinfo.set_arg(0, c.constvalue);
    let out = finfo.invoke(&mut fcinfo)?;
    // SAFETY: text output fns return a NUL-terminated cstring datum (the
    // contract C's DatumGetCString trusts).
    let extval = unsafe { core::ffi::CStr::from_ptr(out.as_usize() as *const core::ffi::c_char) };
    let extval = core::str::from_utf8(extval.to_bytes()).expect("typoutput yields server encoding");

    let mut needlabel = false;
    match c.consttype {
        // Negative INT4 deparses as '-nnn'::integer so it re-parses as a
        // constant, not constant-plus-operator (INT_MIN breaks the paren
        // form).
        INT4OID => {
            if !extval.starts_with('-') {
                buf.try_push_str(extval)?;
            } else {
                write!(buf, "'{extval}'").expect("PgString write");
                needlabel = true;
            }
        }
        NUMERICOID => {
            if extval.as_bytes()[0].is_ascii_digit() && extval.contains(['e', 'E', '.']) {
                buf.try_push_str(extval)?;
            } else {
                write!(buf, "'{extval}'").expect("PgString write");
                needlabel = true;
            }
        }
        BOOLOID => buf.try_push_str(if extval == "t" { "true" } else { "false" })?,
        _ => simple_quote_literal(buf, extval)?,
    }

    if showtype < 0 {
        return Ok(());
    }

    match c.consttype {
        BOOLOID | UNKNOWNOID => needlabel = false,
        INT4OID => {}
        NUMERICOID => needlabel |= c.consttypmod >= 0,
        _ => needlabel = true,
    }
    if needlabel || showtype > 0 {
        let t = format_type::format_type_with_typemod(c.consttype, c.consttypmod)?;
        write!(buf, "::{t}").expect("PgString write");
    }
    get_const_collation(c, buf)?;
    Ok(())
}

fn get_const_collation(c: &Const, _buf: &mut PgString<'_>) -> PgResult<()> {
    if c.constcollid != types_core::primitive::InvalidOid {
        let typcollation = lsyscache::typ::get_typcollation(c.consttype)?;
        if c.constcollid != typcollation {
            node_gap(
                "get_const_collation",
                "COLLATE deparse needs generate_collation_name (ruleutils lane)",
            );
        }
    }
    Ok(())
}

fn simple_quote_literal(buf: &mut PgString<'_>, val: &str) -> PgResult<()> {
    let std_strings = guc_tables::vars::standard_conforming_strings.read();
    buf.try_push('\'')?;
    for ch in val.chars() {
        if ch == '\'' || (ch == '\\' && !std_strings) {
            buf.try_push(ch)?;
        }
        buf.try_push(ch)?;
    }
    buf.try_push('\'')?;
    Ok(())
}

fn ExplainScanTarget(scanrelid: types_core::Index, es: &mut ExplainState<'_>) -> PgResult<()> {
    ExplainTargetRel(scanrelid, es)
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
        RTEKind::RTE_SUBQUERY => None,
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
