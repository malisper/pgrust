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
    append, ExplainCloseGroup, ExplainOpenGroup, ExplainPropertyBool, ExplainPropertyInteger,
    ExplainPropertyList,
};
use crate::options::str_in;
use crate::state::{ExplainState, EXPLAIN_FORMAT_TEXT};

const BOOLOID: u32 = 16;
const INT4OID: u32 = 23;

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
    ExplainPreScanNode(mcx, root, &mut rels_used)?;
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
    ExplainNode(root, None, None, es)?;
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
    rels_used: &mut Bitmapset<'mcx>,
) -> PgResult<()> {
    if node.node_tag() == NodeTag::T_SeqScan {
        rels_used.add_member(mcx, node.as_seq_scan().unwrap().scan.scanrelid as i32)?;
    }
    let plan = plan_of(node);
    if !plan.initPlan.is_nil() {
        node_gap("ExplainPreScanNode", "initPlan walk needs PlanState (SubPlan lane)");
    }
    if let Some(l) = plan.lefttree {
        ExplainPreScanNode(mcx, l, rels_used)?;
    }
    if let Some(r) = plan.righttree {
        ExplainPreScanNode(mcx, r, rels_used)?;
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
        if let Some(name) = refname {
            if names.iter().any(|n| *n == Some(name)) {
                node_gap(
                    "set_rtable_names",
                    "duplicate refname needs the \"_%d\" unique-ifier (ruleutils lane)",
                );
            }
        }
        names.push(refname);
    }
    Ok(names)
}

fn plan_is_disabled(node: Node<'_>) -> bool {
    let plan = plan_of(node);
    if plan.disabled_nodes == 0 {
        return false;
    }
    // Append/MergeAppend/SubqueryScan/CustomScan child sums: vocabulary
    // unported, plan_of already panicked on those tags.
    let mut child_disabled = 0;
    if let Some(l) = plan.lefttree {
        child_disabled += plan_of(l).disabled_nodes;
    }
    if let Some(r) = plan.righttree {
        child_disabled += plan_of(r).disabled_nodes;
    }
    plan.disabled_nodes > child_disabled
}

pub fn ExplainNode<'mcx>(
    node: Node<'mcx>,
    relationship: Option<&str>,
    plan_name: Option<&str>,
    es: &mut ExplainState<'mcx>,
) -> PgResult<()> {
    let plan = plan_of(node);
    let save_indent = es.indent;

    let pname = match node.node_tag() {
        NodeTag::T_Result => "Result",
        NodeTag::T_SeqScan => "Seq Scan",
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
    es.indent += 1;

    if node.node_tag() == NodeTag::T_SeqScan {
        ExplainScanTarget(node.as_seq_scan().unwrap().scan.scanrelid, es)?;
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

    if es.analyze {
        node_gap("ExplainNode", "ANALYZE needs Instrumentation (instrument lane)");
    }
    append!(es, "\n");

    let isdisabled = plan_is_disabled(node);
    if isdisabled {
        ExplainPropertyBool("Disabled", isdisabled, es);
    }

    if es.verbose {
        show_plan_tlist(node, es)?;
    }

    match node.node_tag() {
        NodeTag::T_SeqScan => {
            show_scan_qual(&plan.qual, "Filter", es)?;
        }
        NodeTag::T_Result => {
            if let Some(q) = node.as_result().unwrap().resconstantqual {
                show_one_time_filter(q, es)?;
            }
            show_scan_qual(&plan.qual, "Filter", es)?;
        }
        _ => unreachable!(),
    }

    if !plan.initPlan.is_nil() {
        node_gap("ExplainNode", "InitPlan display needs PlanState (SubPlan lane)");
    }
    let haschildren = plan.lefttree.is_some() || plan.righttree.is_some();
    if haschildren {
        ExplainOpenGroup("Plans", Some("Plans"), false, es);
    }
    if let Some(l) = plan.lefttree {
        ExplainNode(l, Some("Outer"), None, es)?;
    }
    if let Some(r) = plan.righttree {
        ExplainNode(r, Some("Inner"), None, es)?;
    }
    if haschildren {
        ExplainCloseGroup("Plans", Some("Plans"), false, es);
    }

    es.indent = save_indent;
    ExplainCloseGroup("Plan", if relationship.is_some() { None } else { Some("Plan") }, true, es);
    Ok(())
}

fn show_plan_tlist<'mcx>(node: Node<'mcx>, es: &mut ExplainState<'mcx>) -> PgResult<()> {
    let plan = plan_of(node);
    if plan.targetlist.is_nil() {
        return Ok(());
    }
    // Append/MergeAppend/RecursiveUnion/ForeignScan suppression arms: those
    // tags already panic in plan_of.
    let mcx = es.str.allocator();
    let useprefix = es.rtable_size > 1;
    let mut result: Vec<PgString<'mcx>> = Vec::new();
    for tle_node in plan.targetlist.iter() {
        let tle = tle_node.as_target_entry().expect("targetlist holds TargetEntries");
        result.push(deparse_expression_minimal(mcx, tle.expr, useprefix)?);
    }
    ExplainPropertyList("Output", &result, es);
    Ok(())
}

// show_upper_qual on Result.resconstantqual: C stores it as a bare expression
// (not an implicit-AND list).
fn show_one_time_filter<'mcx>(qual: Node<'mcx>, es: &mut ExplainState<'mcx>) -> PgResult<()> {
    let mcx = es.str.allocator();
    let useprefix = es.rtable_size > 1 || es.verbose;
    let s = deparse_expression_minimal(mcx, qual, useprefix)?;
    crate::format::ExplainPropertyText("One-Time Filter", s.as_str(), es);
    Ok(())
}

fn show_scan_qual<'mcx>(
    qual: &NodeList<'mcx>,
    qlabel: &str,
    es: &mut ExplainState<'mcx>,
) -> PgResult<()> {
    if qual.is_nil() {
        return Ok(());
    }
    if qual.len() > 1 {
        node_gap("show_qual", "multi-item qual needs make_ands_explicit (ruleutils lane)");
    }
    let mcx = es.str.allocator();
    let useprefix = es.rtable_size > 1 || es.verbose;
    let s = deparse_expression_minimal(mcx, qual.nth(0), useprefix)?;
    crate::format::ExplainPropertyText(qlabel, s.as_str(), es);
    Ok(())
}

// ruleutils.c deparse_expression slice: only the Const shapes the M1 planner
// lane emits; everything else is loud. Its CATALOG row stays todo and points
// here.
fn deparse_expression_minimal<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    _useprefix: bool,
) -> PgResult<PgString<'mcx>> {
    let Some(c) = node.as_const() else {
        node_gap(
            "deparse_expression",
            &format!("{:?} deparse unported (ruleutils lane)", node.node_tag()),
        );
    };
    deparse_const_minimal(mcx, c)
}

fn deparse_const_minimal<'mcx>(mcx: Mcx<'mcx>, c: &Const) -> PgResult<PgString<'mcx>> {
    if c.constisnull {
        node_gap("get_const_expr", "NULL const needs the ::type cast form (ruleutils lane)");
    }
    let mut out = PgString::new_in(mcx);
    match c.consttype {
        BOOLOID => out.try_push_str(if c.constvalue.as_bool() { "true" } else { "false" })?,
        INT4OID => {
            let v = c.constvalue.as_i32();
            if v < 0 {
                node_gap(
                    "get_const_expr",
                    "negative int const needs the quoted-cast form (ruleutils lane)",
                );
            }
            write!(out, "{v}").expect("PgString write");
        }
        other => node_gap(
            "get_const_expr",
            &format!("const type {other} needs typoutput deparse (ruleutils lane)"),
        ),
    }
    Ok(out)
}

fn ExplainScanTarget(scanrelid: types_core::Index, es: &mut ExplainState<'_>) -> PgResult<()> {
    ExplainTargetRel(scanrelid, es)
}

fn ExplainTargetRel<'mcx>(rti: types_core::Index, es: &mut ExplainState<'mcx>) -> PgResult<()> {
    let mcx = es.str.allocator();
    let rte: &RangeTblEntry<'_> = es
        .rtable
        .expect("ExplainTargetRel before ExplainPrintPlan")
        .nth(rti as usize - 1)
        .as_range_tbl_entry()
        .expect("rtable holds RTEs");
    debug_assert_eq!(rte.rtekind, RTEKind::RTE_RELATION);
    let objectname = lsyscache::get_rel_name(mcx, rte.relid)?;
    let objectname = objectname.as_ref().map(|s| s.as_str());
    if es.verbose {
        node_gap(
            "ExplainTargetRel",
            "VERBOSE schema qualification needs get_namespace_name_or_temp (lsyscache lane)",
        );
    }
    let refname = es.rtable_names[rti as usize - 1]
        .or_else(|| rte.eref.expect("RTE without eref").aliasname)
        .expect("scan RTE has a refname");
    append!(es, " on");
    if let Some(obj) = objectname {
        append!(es, " {}", quote_identifier(obj));
    }
    if objectname != Some(refname) {
        append!(es, " {}", quote_identifier(refname));
    }
    Ok(())
}

// ruleutils.c quote_identifier slice: bare-safe identifiers pass through,
// anything that C would quote is loud.
fn quote_identifier(ident: &str) -> &str {
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
        node_gap("quote_identifier", "quoted-identifier form unported (ruleutils lane)");
    }
    ident
}
