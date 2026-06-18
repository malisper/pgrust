//! The EXPLAIN plan-tree walk (`commands/explain.c`): `ExplainNode`,
//! `ExplainPrintPlan`, and the scan/target helpers.
//!
//! This is the structural slice the architect scoped: the `nodeTag(plan)` name
//! switch, the cost/rows/width block, the Parallel-Aware / Async-Capable /
//! Disabled flags, and the child recursion. The per-node *detail* switch
//! (`show_*`, `ExplainScanTarget`/`ExplainTargetRel`) deparses expressions and
//! looks up relation names through ruleutils / lsyscache, which are unported;
//! those calls route through the K2 deparse seams (and `explain_get_index_name`
//! / relation-name lookups), which panic loudly when actually reached on a
//! VERBOSE / qual / named-scan plan. A structural EXPLAIN of a no-qual,
//! no-named-relation plan (e.g. `EXPLAIN SELECT 1`) never reaches them.

extern crate alloc;

use alloc::format;

use mcx::{vec_with_capacity_in, Mcx, PgBox, PgVec};
use types_error::PgResult;
use types_explain::{ExplainFormat, ExplainState};
use types_nodes::nodeindexscan::{Plan, PlannedStmt};
use types_nodes::nodes::{CmdType, Node};
use types_nodes::parsenodes::RTEKind;
use types_nodes::planstate::PlanStateNode;

use backend_commands_explain_format as fmt;
use backend_utils_adt_ruleutils_seams as ruleutils_s;

/// `ExplainPrintPlan(es, queryDesc)` (explain.c:759). Sets the `ExplainState`
/// plan-tree fields from the started query, applies the Gather-invisible skip,
/// and walks the plan-state tree with [`ExplainNode`].
///
/// `pstmt` is the (copy of the) running `PlannedStmt`; `planstate` is the top
/// plan-state node (already past any invisible Gather skip is handled here).
///
/// `select_rtable_names_for_explain` / `deparse_context_for_plan_tree`
/// (ruleutils) are unported. For the non-verbose structural path the C output
/// does not consult `rtable_names` / `deparse_cxt`, so they are left empty (the
/// architect's "or empty for no-verbose path"); a VERBOSE plan would need the
/// real deparse context and reaches the K2 seams (which panic).
pub fn ExplainPrintPlan<'es, 'p>(
    es: &mut ExplainState<'es>,
    mcx: Mcx<'es>,
    pstmt: &PlannedStmt<'p>,
    planstate: &PlanStateNode<'p>,
    invisible_gather_skipped: bool,
) -> PgResult<()> {
    // es->pstmt = queryDesc->plannedstmt; es->rtable = ...->rtable;
    es.pstmt = Some(mcx::alloc_in(mcx, pstmt.clone_in(mcx)?)?);
    es.rtable = match &pstmt.rtable {
        Some(rt) => {
            let mut out = vec_with_capacity_in(mcx, rt.len())?;
            for rte in rt.iter() {
                out.push(rte.clone_in(mcx)?);
            }
            Some(out)
        }
        None => None,
    };

    // ExplainPreScanNode(planstate, &rels_used);
    // es->rtable_names = select_rtable_names_for_explain(es->rtable, rels_used);
    // es->deparse_cxt = deparse_context_for_plan_tree(pstmt, es->rtable_names);
    //
    // Both ruleutils functions are unported. The non-verbose structural output
    // never reads rtable_names/deparse_cxt, so leave them empty on that path;
    // a VERBOSE plan needs the real names + deparse context and goes through the
    // K2 deparse seams (which panic until ruleutils.c lands).
    if es.verbose {
        // ExplainPreScanNode would populate `rels_used`; the unported scan-pre
        // walk hands an empty set to the (panicking) name selector. The deparse
        // seams take values at the formatting lifetime, so operate on the `'es`
        // copies just stored into `es` (`es.pstmt`/`es.rtable`).
        let rels_used = types_nodes::bitmapset::Bitmapset {
            words: PgVec::new_in(mcx),
        };
        let rtable = es
            .rtable
            .as_ref()
            .expect("VERBOSE EXPLAIN: rtable must be set before name selection");
        let names = ruleutils_s::select_rtable_names_for_explain::call(mcx, rtable, &rels_used)?;
        let es_pstmt = es
            .pstmt
            .as_deref()
            .expect("VERBOSE EXPLAIN: es->pstmt must be set before deparse context");
        let cxt = ruleutils_s::deparse_context_for_plan_tree::call(mcx, es_pstmt, &names)?;
        es.rtable_names = names;
        es.deparse_cxt = Some(cxt);
    }
    // es->printed_subplans = NULL; (already None)
    es.printed_subplans = None;

    // es->rtable_size = list_length(es->rtable); minus an RTE_GROUP entry.
    let mut rtable_size = es.rtable.as_ref().map(|rt| rt.len() as i32).unwrap_or(0);
    if let Some(rt) = es.rtable.as_ref() {
        for rte in rt.iter() {
            if rte.rtekind == RTEKind::RTE_GROUP {
                rtable_size -= 1;
                break;
            }
        }
    }
    es.rtable_size = rtable_size;

    // The Gather-invisible skip (and es->hide_workers) is applied by the caller
    // before passing `planstate` in; `invisible_gather_skipped` records whether
    // it happened so we can mirror C's es->hide_workers = true.
    if invisible_gather_skipped {
        es.hide_workers = true;
    }

    // ExplainNode(ps, NIL, NULL, NULL, es);
    let ancestors: PgVec<'es, PgBox<'es, Node<'es>>> = PgVec::new_in(mcx);
    ExplainNode(es, mcx, planstate, &ancestors, None, None)?;

    // ExplainPrintSettings(es): only emits when es->settings (GUC source list,
    // unported). Skipped for the structural slice; a SETTINGS plan would need
    // the GUC machinery.
    if es.settings {
        panic!(
            "ExplainPrintPlan: SETTINGS option needs get_explain_guc_options (GUC unported)"
        );
    }

    // The es->verbose queryId block reads pstmt->queryId, a field the trimmed
    // PlannedStmt does not carry; it is verbose-only and already gated out above.
    Ok(())
}

/// `ExplainNode(planstate, ancestors, relationship, plan_name, es)`
/// (explain.c:1349) — the structural slice (name switch, generic details, cost
/// block, child recursion).
pub fn ExplainNode<'es, 'p>(
    es: &mut ExplainState<'es>,
    mcx: Mcx<'es>,
    planstate: &PlanStateNode<'p>,
    ancestors: &PgVec<'es, PgBox<'es, Node<'es>>>,
    relationship: Option<&str>,
    plan_name: Option<&str>,
) -> PgResult<()> {
    // Plan *plan = planstate->plan;
    let plan_node: &Node<'p> = planstate
        .ps_head()
        .plan
        .expect("ExplainNode: planstate->plan is NULL");
    let plan: &Plan<'p> = plan_node.plan_head();

    let save_indent = es.indent;

    // Per-worker output buffers (ANALYZE parallel): only when
    // planstate->worker_instrument && es->analyze && !es->hide_workers. The
    // trimmed PlanState carries no worker_instrument, so this is always the
    // else-branch (workers_state = NULL).
    es.workers_state = None;

    // Identify plan node type, and print generic details.
    let pname: alloc::string::String;
    let sname: &str;
    let mut strategy: Option<&str> = None;
    // partialmode is set only by the Agg name case, which is not reachable until
    // the `Agg` plan-node variant is modelled; kept for faithful emission order.
    let partialmode: Option<&str> = None;
    let mut operation: Option<&str> = None;
    let custom_name: Option<&str> = None;

    match plan_node {
        Node::Result(_) => {
            sname = "Result";
            pname = sname.into();
        }
        Node::ProjectSet(_) => {
            sname = "ProjectSet";
            pname = sname.into();
        }
        Node::ModifyTable(m) => {
            sname = "ModifyTable";
            let op = match m.operation {
                CmdType::CMD_INSERT => "Insert",
                CmdType::CMD_UPDATE => "Update",
                CmdType::CMD_DELETE => "Delete",
                CmdType::CMD_MERGE => "Merge",
                _ => "???",
            };
            if op != "???" {
                operation = Some(op);
            }
            pname = op.into();
        }
        Node::Append(_) => {
            sname = "Append";
            pname = sname.into();
        }
        Node::MergeAppend(_) => {
            sname = "Merge Append";
            pname = sname.into();
        }
        Node::RecursiveUnion(_) => {
            sname = "Recursive Union";
            pname = sname.into();
        }
        Node::BitmapAnd(_) => {
            sname = "BitmapAnd";
            pname = sname.into();
        }
        Node::NestLoop(_) => {
            sname = "Nested Loop";
            pname = sname.into();
        }
        Node::MergeJoin(_) => {
            // pname "Merge"; "Join" added by jointype switch (gated detail).
            sname = "Merge Join";
            pname = "Merge".into();
        }
        Node::HashJoin(_) => {
            sname = "Hash Join";
            pname = "Hash".into();
        }
        Node::SeqScan(_) => {
            sname = "Seq Scan";
            pname = sname.into();
        }
        Node::Gather(_) => {
            sname = "Gather";
            pname = sname.into();
        }
        Node::GatherMerge(_) => {
            sname = "Gather Merge";
            pname = sname.into();
        }
        Node::IndexScan(_) => {
            sname = "Index Scan";
            pname = sname.into();
        }
        Node::IndexOnlyScan(_) => {
            sname = "Index Only Scan";
            pname = sname.into();
        }
        Node::BitmapIndexScan(_) => {
            sname = "Bitmap Index Scan";
            pname = sname.into();
        }
        Node::TidRangeScan(_) => {
            sname = "Tid Range Scan";
            pname = sname.into();
        }
        Node::SubqueryScan(_) => {
            sname = "Subquery Scan";
            pname = sname.into();
        }
        Node::TableFuncScan(_) => {
            sname = "Table Function Scan";
            pname = sname.into();
        }
        Node::ValuesScan(_) => {
            sname = "Values Scan";
            pname = sname.into();
        }
        Node::CteScan(_) => {
            sname = "CTE Scan";
            pname = sname.into();
        }
        Node::NamedTuplestoreScan(_) => {
            sname = "Named Tuplestore Scan";
            pname = sname.into();
        }
        Node::ForeignScan(_) => {
            // ForeignScan operation switch (Select/Insert/...) reaches into
            // unported FDW detail; structural Select is "Foreign Scan".
            sname = "Foreign Scan";
            pname = sname.into();
        }
        Node::CustomScan(_) => {
            sname = "Custom Scan";
            pname = sname.into();
        }
        Node::Material(_) => {
            sname = "Materialize";
            pname = sname.into();
        }
        Node::Memoize(_) => {
            sname = "Memoize";
            pname = sname.into();
        }
        Node::Sort(_) => {
            sname = "Sort";
            pname = sname.into();
        }
        Node::Group(_) => {
            sname = "Group";
            pname = sname.into();
        }
        // NOTE: the `Agg` / `WindowAgg` / `LockRows` / `IncrementalSort` plan
        // nodes are not modelled in the `Node` enum yet, so they cannot reach
        // here; their explain.c name cases (Aggregate strategy/partialmode,
        // WindowAgg, ...) land when those plan-node variants do. Anything not
        // matched below falls through to the C default "???".
        Node::Unique(_) => {
            sname = "Unique";
            pname = sname.into();
        }
        Node::SetOp(s) => {
            sname = "SetOp";
            let (pn, st) = match s.strategy {
                types_nodes::nodesetop::SETOP_SORTED => ("SetOp", "Sorted"),
                types_nodes::nodesetop::SETOP_HASHED => ("HashSetOp", "Hashed"),
                _ => ("SetOp ???", "???"),
            };
            strategy = Some(st);
            pname = pn.into();
        }
        Node::Limit(_) => {
            sname = "Limit";
            pname = sname.into();
        }
        Node::Hash(_) => {
            sname = "Hash";
            pname = sname.into();
        }
        other => {
            // C default: pname = sname = "???". A node type the enum models but
            // explain has no name for would land here; mirror C exactly.
            let _ = other;
            sname = "???";
            pname = sname.into();
        }
    }

    fmt::ExplainOpenGroup(
        "Plan",
        if relationship.is_some() { None } else { Some("Plan") },
        true,
        es,
    )?;

    if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
        if let Some(pn) = plan_name {
            fmt::ExplainIndentText(es)?;
            es.str.try_push_str(pn)?;
            es.str.try_push('\n')?;
            es.indent += 1;
        }
        if es.indent != 0 {
            fmt::ExplainIndentText(es)?;
            es.str.try_push_str("->  ")?;
            es.indent += 2;
        }
        if plan.parallel_aware {
            es.str.try_push_str("Parallel ")?;
        }
        if plan.async_capable {
            es.str.try_push_str("Async ")?;
        }
        es.str.try_push_str(&pname)?;
        es.indent += 1;
    } else {
        fmt::ExplainPropertyText("Node Type", sname, es)?;
        if let Some(st) = strategy {
            fmt::ExplainPropertyText("Strategy", st, es)?;
        }
        if let Some(pm) = partialmode {
            fmt::ExplainPropertyText("Partial Mode", pm, es)?;
        }
        if let Some(op) = operation {
            fmt::ExplainPropertyText("Operation", op, es)?;
        }
        if let Some(rel) = relationship {
            fmt::ExplainPropertyText("Parent Relationship", rel, es)?;
        }
        if let Some(pn) = plan_name {
            fmt::ExplainPropertyText("Subplan Name", pn, es)?;
        }
        if let Some(cn) = custom_name {
            fmt::ExplainPropertyText("Custom Plan Provider", cn, es)?;
        }
        fmt::ExplainPropertyBool("Parallel Aware", plan.parallel_aware, es)?;
        fmt::ExplainPropertyBool("Async Capable", plan.async_capable, es)?;
    }

    // Scan/target switch (explain.c:1655): show the relation/index name of the
    // scan or modify node. These resolve names through the catalog and quote
    // them through ruleutils `quote_identifier` — no expression deparse — so
    // they are part of the structural slice.
    explain_scan_target_switch(es, plan_node)?;

    // Cost block: if (es->costs) { ... }.
    if es.costs {
        if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
            es.str.try_push_str(&format!(
                "  (cost={:.2}..{:.2} rows={:.0} width={})",
                plan.startup_cost, plan.total_cost, plan.plan_rows, plan.plan_width
            ))?;
        } else {
            fmt::ExplainPropertyFloat("Startup Cost", None, plan.startup_cost, 2, es)?;
            fmt::ExplainPropertyFloat("Total Cost", None, plan.total_cost, 2, es)?;
            fmt::ExplainPropertyFloat("Plan Rows", None, plan.plan_rows, 0, es)?;
            fmt::ExplainPropertyInteger("Plan Width", None, plan.plan_width as i64, es)?;
        }
    }

    // We have to forcibly clean up the instrumentation state because we haven't
    // done ExecutorEnd yet. `if (planstate->instrument) InstrEndLoop(...)`.
    //
    // InstrEndLoop folds an in-progress loop into the totals; it is a no-op once
    // `running == false`. In the EXPLAIN ANALYZE flow ExplainNode runs only after
    // ExecutorRun + ExecutorFinish have completed, so every node's instrument has
    // `running == false` (looped or never-executed) and InstrEndLoop has nothing
    // left to fold. The `instrument` slot is reached through the immutable
    // `ps_head()` borrow, so the still-running case (which cannot occur here)
    // cannot be finalized in place; surface it loudly rather than print stale
    // totals.
    if let Some(instr) = planstate.ps_head().instrument.as_deref() {
        if instr.running {
            panic!(
                "ExplainNode: InstrEndLoop on a still-running node needs &mut instrument \
                 (cannot occur after ExecutorFinish in EXPLAIN ANALYZE)"
            );
        }
    }

    // ANALYZE actual-rows/timing block:
    //   if (es->analyze && instrument && instrument->nloops > 0) { ... }
    //   else if (es->analyze) { " (never executed)" / zeroed properties }
    let instr_totals = planstate
        .ps_head()
        .instrument
        .as_deref()
        .filter(|_| es.analyze)
        .map(|i| (i.nloops, i.startup, i.total, i.ntuples));
    if let Some((nloops_raw, startup, total, ntuples)) = instr_totals {
        if nloops_raw > 0.0 {
            let nloops = nloops_raw;
            let startup_ms = 1000.0 * startup / nloops;
            let total_ms = 1000.0 * total / nloops;
            let rows = ntuples / nloops;

            if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
                es.str.try_push_str(" (actual ")?;
                if es.timing {
                    es.str
                        .try_push_str(&format!("time={startup_ms:.3}..{total_ms:.3} "))?;
                }
                es.str
                    .try_push_str(&format!("rows={rows:.2} loops={nloops:.0})"))?;
            } else {
                if es.timing {
                    fmt::ExplainPropertyFloat("Actual Startup Time", Some("ms"), startup_ms, 3, es)?;
                    fmt::ExplainPropertyFloat("Actual Total Time", Some("ms"), total_ms, 3, es)?;
                }
                fmt::ExplainPropertyFloat("Actual Rows", None, rows, 2, es)?;
                fmt::ExplainPropertyFloat("Actual Loops", None, nloops, 0, es)?;
            }
        } else {
            // never executed
            if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
                es.str.try_push_str(" (never executed)")?;
            } else {
                if es.timing {
                    fmt::ExplainPropertyFloat("Actual Startup Time", Some("ms"), 0.0, 3, es)?;
                    fmt::ExplainPropertyFloat("Actual Total Time", Some("ms"), 0.0, 3, es)?;
                }
                fmt::ExplainPropertyFloat("Actual Rows", None, 0.0, 0, es)?;
                fmt::ExplainPropertyFloat("Actual Loops", None, 0.0, 0, es)?;
            }
        }
    }

    // Per-worker general execution details:
    //   if (es->workers_state && es->verbose) { ... worker_instrument ... }
    // The trimmed PlanState carries no `worker_instrument`, and `workers_state`
    // is forced to None above, so this leg is never entered on the modelled path;
    // it is a genuinely-trimmed sub-leg. Guard loudly if it is ever reached.
    if es.workers_state.is_some() && es.verbose {
        panic!(
            "ExplainNode: per-worker actual-rows detail needs PlanState.worker_instrument \
             (trimmed from PlanState) — single-loop EXPLAIN ANALYZE only"
        );
    }

    // Disabled flag: plan_is_disabled reads plan->disabled_nodes, a field the
    // trimmed Plan does not carry. With no disabled_nodes the node is never
    // disabled (C returns false when disabled_nodes == 0), so isdisabled =
    // false; the property is emitted only for non-text format.
    let isdisabled = false;
    if es.format != ExplainFormat::EXPLAIN_FORMAT_TEXT || isdisabled {
        fmt::ExplainPropertyBool("Disabled", isdisabled, es)?;
    }

    // End the line in text format.
    if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
        es.str.try_push('\n')?;
    }

    // The per-node detail switch (show_plan_tlist / show_*_qual / show_sort_keys
    // / instrumentation counts) deparses expressions through ruleutils and reads
    // ANALYZE instrumentation. None of it is reached for a no-qual,
    // non-verbose, non-analyze plan: show_scan_qual(plan->qual, ...) is a no-op
    // when plan->qual is NIL, and the verbose-only tlist/keys are gated. Guard
    // loudly if a qual is actually present.
    if let Some(qual) = plan.qual.as_ref().filter(|q| !q.is_empty()) {
        // show_scan_qual(qual, "Filter", planstate, ancestors, es):
        //   context = set_deparse_context_plan(es->deparse_cxt, plan, ancestors);
        //   exprstr = deparse_expression(node, context, useprefix, false);
        // ruleutils is unported, so both seams panic; calling them here keeps the
        // EXPLAIN deparse contract live and surfaces the exact boundary.
        // The deparse context is es->deparse_cxt; set_deparse_context_plan
        // panics (ruleutils unported) before consuming it, so an empty context
        // at the formatting lifetime suffices for the call.
        let dpcontext: PgVec<'es, PgBox<'es, Node<'es>>> = PgVec::new_in(mcx);
        let context =
            ruleutils_s::set_deparse_context_plan::call(mcx, &dpcontext, planstate, ancestors)?;
        // make_ands_explicit(qual): the qual list deparses as an AND of its
        // members; the structural slice never reaches this, so wrap the first
        // member as the representative node for the (panicking) deparse call.
        let node = mcx::alloc_in(mcx, Node::Expr(qual[0].clone()))?;
        let _ = ruleutils_s::deparse_expression::call(mcx, &node, &context, false, false)?;
        // Unreachable on a structural plan (the seam panicked); mirror C by not
        // emitting anything further here.
        unreachable!("deparse_expression returns only via panic until ruleutils lands");
    }

    // Children. haschildren over initPlan / outer / inner / member nodes /
    // subPlan. The trimmed PlanState carries initPlan/subPlan as Option<PgVec>;
    // member-node nodes (Append/BitmapAnd/...) recurse through their own state.
    let head = planstate.ps_head();
    let has_init = head.initPlan.as_ref().map(|v| !v.is_empty()).unwrap_or(false);
    let has_sub = head.subPlan.as_ref().map(|v| !v.is_empty()).unwrap_or(false);
    let has_outer = planstate.outer_plan_state().is_some();
    let has_inner = head.righttree.is_some();
    let haschildren = has_init || has_outer || has_inner || has_sub;

    if haschildren {
        fmt::ExplainOpenGroup("Plans", Some("Plans"), false, es)?;
        // ancestors = lcons(plan, ancestors): prepend this Plan node. The
        // ancestor list is consumed only by deparse (PARAM_EXEC resolution),
        // which the structural slice never reaches; carry it forward unchanged.
    }

    // initPlan-s: SubPlanState detail reaches deparse; gate loudly if present.
    if has_init {
        panic!(
            "ExplainNode: initPlan-s need ExplainSubPlans (SubPlan deparse, ruleutils \
             unported) — structural EXPLAIN only"
        );
    }

    // lefttree (Outer).
    if let Some(outer) = planstate.outer_plan_state() {
        ExplainNode(es, mcx, outer, ancestors, Some("Outer"), None)?;
    }
    // righttree (Inner).
    if let Some(inner) = head.righttree.as_deref() {
        ExplainNode(es, mcx, inner, ancestors, Some("Inner"), None)?;
    }

    // Special member-node children (Append/MergeAppend/BitmapAnd/BitmapOr/
    // SubqueryScan/CustomScan): the trimmed PlanState does not yet thread those
    // member-node vectors into the enum (append_input_states returns None), so a
    // member-bearing node reaches no children here. Guard loudly if such a node
    // appears with children to display.
    if matches!(
        plan_node,
        Node::Append(_) | Node::MergeAppend(_) | Node::BitmapAnd(_)
    ) {
        panic!(
            "ExplainNode: Append/MergeAppend/BitmapAnd member-node recursion needs the \
             member plan-state vectors (not threaded into PlanStateNode yet)"
        );
    }

    // subPlan-s.
    if has_sub {
        panic!(
            "ExplainNode: subPlan-s need ExplainSubPlans (SubPlan deparse, ruleutils \
             unported) — structural EXPLAIN only"
        );
    }

    if haschildren {
        fmt::ExplainCloseGroup("Plans", Some("Plans"), false, es)?;
    }

    // In text format, undo whatever indentation we added.
    if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
        es.indent = save_indent;
    }

    fmt::ExplainCloseGroup(
        "Plan",
        if relationship.is_some() { None } else { Some("Plan") },
        true,
        es,
    )?;
    Ok(())
}

/// The `ExplainScanTarget`/`ExplainModifyTarget`/`ExplainIndexScanDetails`
/// branch of `ExplainNode` (explain.c:1655) — show the relation / index name of
/// the scan or modify node. Mirrors the C `nodeTag(plan)` switch exactly.
fn explain_scan_target_switch<'es, 'p>(
    es: &mut ExplainState<'es>,
    plan_node: &Node<'p>,
) -> PgResult<()> {
    use crate::scantarget;

    match plan_node {
        Node::SeqScan(s) => scantarget::ExplainScanTarget(es, plan_node, s.scan.scanrelid),
        Node::SampleScan(s) => scantarget::ExplainScanTarget(es, plan_node, s.scan.scanrelid),
        Node::TidScan(s) => scantarget::ExplainScanTarget(es, plan_node, s.scan.scanrelid),
        Node::TidRangeScan(s) => scantarget::ExplainScanTarget(es, plan_node, s.scan.scanrelid),
        Node::SubqueryScan(s) => scantarget::ExplainScanTarget(es, plan_node, s.scan.scanrelid),
        Node::FunctionScan(s) => scantarget::ExplainScanTarget(es, plan_node, s.scan.scanrelid),
        Node::TableFuncScan(s) => scantarget::ExplainScanTarget(es, plan_node, s.scan.scanrelid),
        Node::ValuesScan(s) => scantarget::ExplainScanTarget(es, plan_node, s.scan.scanrelid),
        Node::CteScan(s) => scantarget::ExplainScanTarget(es, plan_node, s.scan.scanrelid),
        Node::WorkTableScan(s) => scantarget::ExplainScanTarget(es, plan_node, s.scan.scanrelid),
        Node::ForeignScan(s) => {
            // if (((Scan *) plan)->scanrelid > 0) ExplainScanTarget(...)
            if s.scan.scanrelid > 0 {
                scantarget::ExplainScanTarget(es, plan_node, s.scan.scanrelid)
            } else {
                Ok(())
            }
        }
        Node::CustomScan(s) => {
            if s.scan.scanrelid > 0 {
                scantarget::ExplainScanTarget(es, plan_node, s.scan.scanrelid)
            } else {
                Ok(())
            }
        }
        Node::IndexScan(s) => {
            scantarget::ExplainIndexScanDetails(es, s.indexid, s.indexorderdir)?;
            scantarget::ExplainScanTarget(es, plan_node, s.scan.scanrelid)
        }
        Node::IndexOnlyScan(s) => {
            scantarget::ExplainIndexScanDetails(es, s.indexid, s.indexorderdir)?;
            scantarget::ExplainScanTarget(es, plan_node, s.scan.scanrelid)
        }
        Node::BitmapIndexScan(s) => {
            // explain_get_index_name + quote_identifier — no ExplainTargetRel.
            let mcx = es.str.allocator();
            let indexname = scantarget::explain_get_index_name(mcx, s.indexid)?;
            if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
                let quoted =
                    backend_utils_adt_ruleutils::quote_identifier(mcx, indexname.as_str())?;
                es.str.try_push_str(" on ")?;
                es.str.try_push_str(quoted.as_str())?;
            } else {
                fmt::ExplainPropertyText("Index Name", indexname.as_str(), es)?;
            }
            Ok(())
        }
        Node::ModifyTable(m) => {
            scantarget::ExplainModifyTarget(es, plan_node, m.nominalRelation)
        }
        _ => Ok(()),
    }
}
