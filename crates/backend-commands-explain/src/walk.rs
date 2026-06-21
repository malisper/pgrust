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
use types_core::primitive::{AttrNumber, Oid};
use types_error::PgResult;
use types_explain::{ExplainFormat, ExplainState};
use types_nodes::nodeindexscan::{Plan, PlannedStmt};
use types_nodes::nodes::{ntag, CmdType, Node};
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
    // The deparse context itself is built on demand inside ruleutils' folded
    // `deparse_expr_for_plan` seam (from es->pstmt + es->rtable_names), so we keep
    // only the per-RTE display names here. `ExplainPreScanNode` walks the
    // plan-state tree marking the RTE indexes actually referenced by scan /
    // ModifyTable / Append nodes; `select_rtable_names_for_explain` then assigns
    // display names only to those (and suppresses unreferenced RTEs), so a Var
    // resolved to a referenced RTE gets its `alias.` prefix.
    let rels_used = explain_pre_scan_node(mcx, planstate, None)?;
    let rels_used = match rels_used {
        Some(b) => PgBox::into_inner(b),
        None => types_nodes::bitmapset::Bitmapset {
            words: PgVec::new_in(mcx),
        },
    };
    if let Some(rtable) = es.rtable.as_ref() {
        let names = ruleutils_s::select_rtable_names_for_explain::call(mcx, rtable, &rels_used)?;
        es.rtable_names = names;
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

/// `ExplainPreScanNode(planstate, &rels_used)` (explain.c:1182) — walk the
/// plan-state tree and accumulate the set of RTE indexes (`scanrelid` /
/// `nominalRelation` / `apprelids` / ...) that are actually referenced, so the
/// deparser assigns display names only to those (and gives Vars resolving to
/// them an `alias.` prefix). Returns the accumulated `Bitmapset` (the C
/// `*rels_used`), threaded by value (`acc`) down the recursion as the C `**`
/// out-parameter would be mutated.
fn explain_pre_scan_node<'es, 'p>(
    mcx: Mcx<'es>,
    planstate: &PlanStateNode<'p>,
    acc: Option<PgBox<'es, types_nodes::bitmapset::Bitmapset<'es>>>,
) -> PgResult<Option<PgBox<'es, types_nodes::bitmapset::Bitmapset<'es>>>> {
    use backend_nodes_core::bitmapset::{bms_add_member, bms_add_members};

    let plan_node: &Node<'p> = match planstate.ps_head().plan {
        Some(p) => p,
        None => return Ok(acc),
    };

    let mut acc = acc;
    match plan_node.node_tag() {
        // Plain Scan-family: bms_add_member(*rels_used, ((Scan*)plan)->scanrelid).
        ntag::T_SeqScan => {
            acc = Some(bms_add_member(mcx, acc, plan_node.expect_seqscan().scan.scanrelid as i32)?);
        }
        ntag::T_SampleScan => {
            acc = Some(bms_add_member(mcx, acc, plan_node.expect_samplescan().scan.scanrelid as i32)?);
        }
        ntag::T_IndexScan => {
            acc = Some(bms_add_member(mcx, acc, plan_node.expect_indexscan().scan.scanrelid as i32)?);
        }
        ntag::T_IndexOnlyScan => {
            acc = Some(bms_add_member(
                mcx,
                acc,
                plan_node.expect_indexonlyscan().scan.scanrelid as i32,
            )?);
        }
        ntag::T_BitmapHeapScan => {
            acc = Some(bms_add_member(
                mcx,
                acc,
                plan_node.expect_bitmapheapscan().scan.scanrelid as i32,
            )?);
        }
        ntag::T_TidScan => {
            acc = Some(bms_add_member(mcx, acc, plan_node.expect_tidscan().scan.scanrelid as i32)?);
        }
        ntag::T_TidRangeScan => {
            acc = Some(bms_add_member(
                mcx,
                acc,
                plan_node.expect_tidrangescan().scan.scanrelid as i32,
            )?);
        }
        ntag::T_SubqueryScan => {
            acc = Some(bms_add_member(
                mcx,
                acc,
                plan_node.expect_subqueryscan().scan.scanrelid as i32,
            )?);
        }
        ntag::T_FunctionScan => {
            acc = Some(bms_add_member(
                mcx,
                acc,
                plan_node.expect_functionscan().scan.scanrelid as i32,
            )?);
        }
        ntag::T_TableFuncScan => {
            acc = Some(bms_add_member(
                mcx,
                acc,
                plan_node.expect_tablefuncscan().scan.scanrelid as i32,
            )?);
        }
        ntag::T_ValuesScan => {
            acc = Some(bms_add_member(mcx, acc, plan_node.expect_valuesscan().scan.scanrelid as i32)?);
        }
        ntag::T_CteScan => {
            acc = Some(bms_add_member(mcx, acc, plan_node.expect_ctescan().scan.scanrelid as i32)?);
        }
        ntag::T_NamedTuplestoreScan => {
            acc = Some(bms_add_member(
                mcx,
                acc,
                plan_node.expect_namedtuplestorescan().scan.scanrelid as i32,
            )?);
        }
        ntag::T_WorkTableScan => {
            acc = Some(bms_add_member(
                mcx,
                acc,
                plan_node.expect_worktablescan().scan.scanrelid as i32,
            )?);
        }
        ntag::T_ForeignScan => {
            // bms_add_members(*rels_used, ((ForeignScan*)plan)->fs_base_relids).
            let fs = plan_node.expect_foreignscan();
            if let Some(relids) = fs.fs_base_relids.as_ref() {
                acc = bms_add_members(mcx, acc, Some(&**relids))?;
            }
        }
        ntag::T_ModifyTable => {
            let m = plan_node.expect_modifytable();
            acc = Some(bms_add_member(mcx, acc, m.nominalRelation as i32)?);
            if m.exclRelRTI != 0 {
                acc = Some(bms_add_member(mcx, acc, m.exclRelRTI as i32)?);
            }
            // Ensure Vars used in RETURNING will have refnames.
            if plan_node.plan_head().targetlist.is_some() {
                if let Some(rrs) = m.resultRelations.as_ref() {
                    if let Some(&first) = rrs.first() {
                        acc = Some(bms_add_member(mcx, acc, first as i32)?);
                    }
                }
            }
        }
        ntag::T_Append => {
            if let Some(relids) = plan_node.expect_append().apprelids.as_ref() {
                acc = bms_add_members(mcx, acc, Some(&**relids))?;
            }
        }
        ntag::T_MergeAppend => {
            if let Some(relids) = plan_node.expect_mergeappend().apprelids.as_ref() {
                acc = bms_add_members(mcx, acc, Some(&**relids))?;
            }
        }
        _ => {}
    }

    // return planstate_tree_walker(planstate, ExplainPreScanNode, rels_used).
    // The owned PlanStateNode threads outer (lefttree) and inner (righttree),
    // plus the member-node children (Append/MergeAppend/BitmapAnd/BitmapOr) via
    // `member_input_states()` — so partition Seq Scans under an Append
    // contribute their scanrelids (and thus get display names).
    if let Some(outer) = planstate.outer_plan_state() {
        acc = explain_pre_scan_node(mcx, outer, acc)?;
    }
    if let Some(inner) = planstate.ps_head().righttree.as_deref() {
        acc = explain_pre_scan_node(mcx, inner, acc)?;
    }
    if let Some(members) = planstate.member_input_states() {
        for child in members {
            acc = explain_pre_scan_node(mcx, child, acc)?;
        }
    }

    Ok(acc)
}

/// `plan_is_disabled(Plan *plan)` (explain.c:1245) — a node was disabled during
/// planning iff its `disabled_nodes` count exceeds the sum of its immediate
/// children's. Append/MergeAppend/SubqueryScan/CustomScan carry their child
/// plans in special fields (children of BitmapAnd/BitmapOr can't be disabled);
/// everything else uses outer (`lefttree`) + inner (`righttree`).
fn plan_is_disabled(plan_node: &Node<'_>) -> bool {
    let plan = plan_node.plan_head();
    // The node is certainly not disabled if this is zero.
    if plan.disabled_nodes == 0 {
        return false;
    }

    let mut child_disabled_nodes: i32 = 0;
    match plan_node.node_tag() {
        ntag::T_Append => {
            // Purposefully includes any run-time pruned children.
            for subplan in plan_node.expect_append().appendplans.iter() {
                child_disabled_nodes += subplan.plan_head().disabled_nodes;
            }
        }
        ntag::T_MergeAppend => {
            for subplan in plan_node.expect_mergeappend().mergeplans.iter() {
                child_disabled_nodes += subplan.plan_head().disabled_nodes;
            }
        }
        ntag::T_SubqueryScan => {
            if let Some(subplan) = plan_node.expect_subqueryscan().subplan.as_deref() {
                child_disabled_nodes += subplan.plan_head().disabled_nodes;
            }
        }
        ntag::T_CustomScan => {
            if let Some(custom_plans) = plan_node.expect_customscan().custom_plans.as_ref() {
                for subplan in custom_plans.iter() {
                    child_disabled_nodes += subplan.plan_head().disabled_nodes;
                }
            }
        }
        _ => {
            // Else, sum up disabled_nodes from the inner and outer side.
            if let Some(outer) = plan.lefttree.as_deref() {
                child_disabled_nodes += outer.plan_head().disabled_nodes;
            }
            if let Some(inner) = plan.righttree.as_deref() {
                child_disabled_nodes += inner.plan_head().disabled_nodes;
            }
        }
    }

    // It's disabled if the plan's disabled_nodes is higher than the sum of its
    // children's.
    plan.disabled_nodes > child_disabled_nodes
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
    // partialmode is set by the Agg name case (DO_AGGSPLIT_* of agg->aggsplit).
    let mut partialmode: Option<&str> = None;
    let mut operation: Option<&str> = None;
    let custom_name: Option<&str> = None;

    match plan_node.node_tag() {
        ntag::T_Result => {
            sname = "Result";
            pname = sname.into();
        }
        ntag::T_ProjectSet => {
            sname = "ProjectSet";
            pname = sname.into();
        }
        ntag::T_ModifyTable => {
            let m = plan_node.expect_modifytable();
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
        ntag::T_Append => {
            sname = "Append";
            pname = sname.into();
        }
        ntag::T_MergeAppend => {
            sname = "Merge Append";
            pname = sname.into();
        }
        ntag::T_RecursiveUnion => {
            sname = "Recursive Union";
            pname = sname.into();
        }
        ntag::T_BitmapAnd => {
            sname = "BitmapAnd";
            pname = sname.into();
        }
        ntag::T_BitmapOr => {
            sname = "BitmapOr";
            pname = sname.into();
        }
        ntag::T_NestLoop => {
            sname = "Nested Loop";
            pname = sname.into();
        }
        ntag::T_MergeJoin => {
            // pname "Merge"; "Join" added by jointype switch (gated detail).
            sname = "Merge Join";
            pname = "Merge".into();
        }
        ntag::T_HashJoin => {
            sname = "Hash Join";
            pname = "Hash".into();
        }
        ntag::T_SeqScan => {
            sname = "Seq Scan";
            pname = sname.into();
        }
        ntag::T_SampleScan => {
            sname = "Sample Scan";
            pname = sname.into();
        }
        ntag::T_Gather => {
            sname = "Gather";
            pname = sname.into();
        }
        ntag::T_GatherMerge => {
            sname = "Gather Merge";
            pname = sname.into();
        }
        ntag::T_IndexScan => {
            sname = "Index Scan";
            pname = sname.into();
        }
        ntag::T_IndexOnlyScan => {
            sname = "Index Only Scan";
            pname = sname.into();
        }
        ntag::T_BitmapIndexScan => {
            sname = "Bitmap Index Scan";
            pname = sname.into();
        }
        ntag::T_BitmapHeapScan => {
            sname = "Bitmap Heap Scan";
            pname = sname.into();
        }
        ntag::T_TidScan => {
            sname = "Tid Scan";
            pname = sname.into();
        }
        ntag::T_TidRangeScan => {
            sname = "Tid Range Scan";
            pname = sname.into();
        }
        ntag::T_SubqueryScan => {
            sname = "Subquery Scan";
            pname = sname.into();
        }
        ntag::T_FunctionScan => {
            sname = "Function Scan";
            pname = sname.into();
        }
        ntag::T_TableFuncScan => {
            sname = "Table Function Scan";
            pname = sname.into();
        }
        ntag::T_ValuesScan => {
            sname = "Values Scan";
            pname = sname.into();
        }
        ntag::T_CteScan => {
            sname = "CTE Scan";
            pname = sname.into();
        }
        ntag::T_NamedTuplestoreScan => {
            sname = "Named Tuplestore Scan";
            pname = sname.into();
        }
        ntag::T_WorkTableScan => {
            sname = "WorkTable Scan";
            pname = sname.into();
        }
        ntag::T_ForeignScan => {
            // ForeignScan operation switch (Select/Insert/...) reaches into
            // unported FDW detail; structural Select is "Foreign Scan".
            sname = "Foreign Scan";
            pname = sname.into();
        }
        ntag::T_CustomScan => {
            sname = "Custom Scan";
            pname = sname.into();
        }
        ntag::T_Material => {
            sname = "Materialize";
            pname = sname.into();
        }
        ntag::T_Memoize => {
            sname = "Memoize";
            pname = sname.into();
        }
        ntag::T_Sort => {
            sname = "Sort";
            pname = sname.into();
        }
        ntag::T_IncrementalSort => {
            sname = "Incremental Sort";
            pname = sname.into();
        }
        ntag::T_Group => {
            sname = "Group";
            pname = sname.into();
        }
        ntag::T_Agg => {
            // C explain.c T_Agg: name/strategy from agg->aggstrategy, and the
            // "Partial"/"Finalize" prefix from DO_AGGSPLIT_* of agg->aggsplit.
            // The verbose per-node Agg detail (show_agg_keys / show_hashagg_info)
            // is emitted later in the detail pass; here we only set the name.
            use types_nodes::nodeagg::{
                do_aggsplit_combine, do_aggsplit_skipfinal, AGG_HASHED, AGG_MIXED, AGG_PLAIN,
                AGG_SORTED,
            };
            let agg = plan_node.expect_agg();
            sname = "Aggregate";
            let (pn, st): (&str, &str) = match agg.aggstrategy {
                AGG_PLAIN => ("Aggregate", "Plain"),
                AGG_SORTED => ("GroupAggregate", "Sorted"),
                AGG_HASHED => ("HashAggregate", "Hashed"),
                AGG_MIXED => ("MixedAggregate", "Mixed"),
                _ => ("Aggregate ???", "???"),
            };
            strategy = Some(st);
            if do_aggsplit_skipfinal(agg.aggsplit) {
                partialmode = Some("Partial");
                pname = alloc::format!("Partial {pn}");
            } else if do_aggsplit_combine(agg.aggsplit) {
                partialmode = Some("Finalize");
                pname = alloc::format!("Finalize {pn}");
            } else {
                partialmode = Some("Simple");
                pname = pn.into();
            }
        }
        ntag::T_WindowAgg => {
            sname = "WindowAgg";
            pname = sname.into();
        }
        ntag::T_Unique => {
            sname = "Unique";
            pname = sname.into();
        }
        ntag::T_SetOp => {
            let s = plan_node.expect_setop();
            sname = "SetOp";
            let (pn, st) = match s.strategy {
                types_nodes::nodesetop::SETOP_SORTED => ("SetOp", "Sorted"),
                types_nodes::nodesetop::SETOP_HASHED => ("HashSetOp", "Hashed"),
                _ => ("SetOp ???", "???"),
            };
            strategy = Some(st);
            pname = pn.into();
        }
        ntag::T_LockRows => {
            sname = "LockRows";
            pname = sname.into();
        }
        ntag::T_Limit => {
            sname = "Limit";
            pname = sname.into();
        }
        ntag::T_Hash => {
            sname = "Hash";
            pname = sname.into();
        }
        _ => {
            // C default: pname = sname = "???". A node type the enum models but
            // explain has no name for would land here; mirror C exactly.
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
    // InstrEndLoop (instrument.c) folds the current in-progress cycle into the
    // accumulated totals: a node that produced rows but was never loop-ended
    // (the common single-scan EXPLAIN ANALYZE case — the scan stops at EOF, which
    // sets `starttime = 0` via InstrStopNode but leaves `running = true` until a
    // rescan or this finalize) still has `nloops == 0` and its per-cycle counters
    // in `firsttuple`/`counter`/`tuplecount`. C mutates the instrument in place;
    // the owned `PlanStateNode` is borrowed immutably here, so we replicate
    // InstrEndLoop's arithmetic locally to produce the same folded
    // `(nloops, startup, total, ntuples)` the ANALYZE block reads — the values
    // are deterministic from the current fields and the instrument is not read
    // again, so the local fold is behaviorally identical to the in-place mutate.
    //
    // C precondition: `InstrEndLoop` elogs ERROR if `starttime` is non-zero
    // ("InstrEndLoop called on running node"); after ExecutorFinish every node
    // has been stopped (InstrStopNode zeroes `starttime`), so a non-zero
    // `starttime` is a genuine internal inconsistency — surface it.
    let instr_totals = match planstate.ps_head().instrument.as_deref() {
        Some(i) if es.analyze => {
            if i.running {
                if !i.starttime.is_zero() {
                    return Err(backend_utils_error::ereport(types_error::ERROR)
                        .errmsg_internal("InstrEndLoop called on running node")
                        .into_error());
                }
                // Accumulate this cycle's per-cycle counters into the totals
                // (the `instr->startup += firsttuple; total += counter; ntuples
                // += tuplecount; nloops += 1` of InstrEndLoop).
                let totaltime = i.counter.get_double();
                Some((
                    i.nloops + 1.0,
                    i.startup + i.firsttuple,
                    i.total + totaltime,
                    i.ntuples + i.tuplecount,
                ))
            } else {
                // Already loop-ended (or never executed): the totals are final.
                Some((i.nloops, i.startup, i.total, i.ntuples))
            }
        }
        _ => None,
    };
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

    // In text format, the first line ends here (explain.c:1877) — BEFORE the
    // Disabled property, so the property lands on its own indented line.
    if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
        es.str.try_push('\n')?;
    }

    // Disabled flag (explain.c plan_is_disabled): a node is "disabled" when its
    // own `disabled_nodes` count exceeds the sum of its children's — i.e. the
    // planner applied an `enable_*`-GUC penalty at this node. The trimmed Plan
    // DOES carry `disabled_nodes` (the planner's accumulator), so reproduce
    // plan_is_disabled() faithfully: total the immediate children's counts and
    // compare. The property is emitted in text format only when true.
    let isdisabled = plan_is_disabled(plan_node);
    if es.format != ExplainFormat::EXPLAIN_FORMAT_TEXT || isdisabled {
        fmt::ExplainPropertyBool("Disabled", isdisabled, es)?;
    }

    // target list (explain.c:1932): `if (es->verbose) show_plan_tlist(...)`.
    // The VERBOSE-only `Output:` line deparses the node's targetlist.
    if es.verbose {
        show_plan_tlist(es, mcx, plan_node, ancestors)?;
    }

    // T_Result: `show_upper_qual(resconstantqual, "One-Time Filter")` runs
    // BEFORE the generic `Filter:` line (explain.c:2234). `show_upper_qual`
    // uses `useprefix = list_length(es->rtable) > 1 || es->verbose`.
    if plan_node.node_tag() == ntag::T_Result {
        if let Some(rcq) = plan_node
            .as_result()
            .and_then(|r| r.resconstantqual.as_ref())
            .filter(|q| !q.is_empty())
        {
            let mut exprs: alloc::vec::Vec<types_nodes::primnodes::Expr> =
                alloc::vec::Vec::with_capacity(rcq.len());
            for e in rcq.iter() {
                exprs.push(e.clone_in(mcx)?);
            }
            let anded = backend_nodes_core::makefuncs::make_ands_explicit(exprs);
            let node = Node::mk_expr(mcx, anded)?;

            let useprefix = es.rtable_names.len() > 1 || es.verbose;

            let plan_owned: PgBox<'es, Node<'es>> = mcx::alloc_in(mcx, plan_node.clone_in(mcx)?)?;
            let es_pstmt = es
                .pstmt
                .as_deref()
                .expect("EXPLAIN: es->pstmt must be set before deparse");
            let exprstr = ruleutils_s::deparse_expr_for_plan::call(
                mcx,
                es_pstmt,
                &es.rtable_names,
                &plan_owned,
                ancestors,
                &node,
                useprefix,
                false,
            )?;
            fmt::ExplainPropertyText("One-Time Filter", exprstr.as_str(), es)?;
        }
    }

    // T_WindowAgg: `show_window_def(...)` then `show_upper_qual(runConditionOrig,
    // "Run Condition")` run BEFORE the generic `Filter:` line (explain.c:2203-
    // 2208), so the `Window:` and `Run Condition:` detail lines precede `Filter`.
    if plan_node.node_tag() == ntag::T_WindowAgg {
        show_window_def(es, mcx, plan_node, planstate, ancestors)?;

        // show_upper_qual(((WindowAgg *) plan)->runConditionOrig, "Run Condition",
        //                 planstate, ancestors, es). useprefix = rtable>1||verbose.
        let wagg = plan_node.expect_windowagg();
        if let Some(rc) = wagg.runConditionOrig.as_ref().filter(|q| !q.is_empty()) {
            let mut exprs: alloc::vec::Vec<types_nodes::primnodes::Expr> =
                alloc::vec::Vec::with_capacity(rc.len());
            for e in rc.iter() {
                exprs.push(e.clone_in(mcx)?);
            }
            let anded = backend_nodes_core::makefuncs::make_ands_explicit(exprs);
            let node = Node::mk_expr(mcx, anded)?;

            let useprefix = es.rtable_names.len() > 1 || es.verbose;
            let plan_owned: PgBox<'es, Node<'es>> = mcx::alloc_in(mcx, plan_node.clone_in(mcx)?)?;
            let es_pstmt = es
                .pstmt
                .as_deref()
                .expect("EXPLAIN: es->pstmt must be set before deparse");
            let exprstr = ruleutils_s::deparse_expr_for_plan::call(
                mcx,
                es_pstmt,
                &es.rtable_names,
                &plan_owned,
                ancestors,
                &node,
                useprefix,
                false,
            )?;
            fmt::ExplainPropertyText("Run Condition", exprstr.as_str(), es)?;
        }
    }

    // The per-node "quals, sort keys, etc" switch (explain.c:1952). C emits the
    // scan-specific quals (`Index Cond` / `Recheck Cond` / `Order By`) BEFORE the
    // generic `Filter` line, each in its node's case. The `Filter` itself
    // (`show_scan_qual(plan->qual, "Filter", ...)`) prints for index/bitmap/seq/
    // values/cte/etc scans and Gather; the index nodes additionally print their
    // index condition first. (The verbose-only tlist (`Output:`) /
    // instrumentation counts stay gated.) `show_scan_qual` uses
    // `useprefix = IsA(plan, SubqueryScan) || es->verbose`.
    match plan_node.node_tag() {
        ntag::T_IndexScan => {
            // indexqualorig -> "Index Cond"; indexorderbyorig -> "Order By";
            // plan->qual -> "Filter".
            let is = plan_node.expect_indexscan();
            show_scan_qual(es, mcx, plan_node, ancestors, is.indexqualorig.as_ref(), "Index Cond")?;
            show_scan_qual(es, mcx, plan_node, ancestors, is.indexorderbyorig.as_ref(), "Order By")?;
            show_scan_qual(es, mcx, plan_node, ancestors, plan.qual.as_ref(), "Filter")?;
            show_indexsearches_info(es, planstate)?;
        }
        ntag::T_IndexOnlyScan => {
            // indexqual -> "Index Cond"; indexorderby -> "Order By";
            // plan->qual -> "Filter".
            let ios = plan_node.expect_indexonlyscan();
            show_scan_qual(es, mcx, plan_node, ancestors, ios.indexqual.as_ref(), "Index Cond")?;
            show_scan_qual(es, mcx, plan_node, ancestors, ios.indexorderby.as_ref(), "Order By")?;
            show_scan_qual(es, mcx, plan_node, ancestors, plan.qual.as_ref(), "Filter")?;
            show_indexsearches_info(es, planstate)?;
        }
        ntag::T_BitmapIndexScan => {
            // indexqualorig -> "Index Cond" (no Filter — the heap node carries it).
            let bis = plan_node.expect_bitmapindexscan();
            show_scan_qual(es, mcx, plan_node, ancestors, bis.indexqualorig.as_ref(), "Index Cond")?;
            show_indexsearches_info(es, planstate)?;
        }
        ntag::T_BitmapHeapScan => {
            // bitmapqualorig -> "Recheck Cond"; plan->qual -> "Filter".
            let bhs = plan_node.expect_bitmapheapscan();
            show_scan_qual(es, mcx, plan_node, ancestors, Some(&bhs.bitmapqualorig), "Recheck Cond")?;
            show_scan_qual(es, mcx, plan_node, ancestors, plan.qual.as_ref(), "Filter")?;
        }
        ntag::T_TableFuncScan => {
            // explain.c:2089-2095: verbose-only show_expression((Node *)
            //   scan->tablefunc, "Table Function Call", ...); then Filter.
            if es.verbose {
                let tfs = plan_node.expect_tablefuncscan();
                let tf_node = Node::mk_table_func(mcx, tfs.tablefunc.clone_in(mcx)?)?;
                show_expression(
                    es,
                    mcx,
                    plan_node,
                    ancestors,
                    &tf_node,
                    "Table Function Call",
                )?;
            }
            show_scan_qual(es, mcx, plan_node, ancestors, plan.qual.as_ref(), "Filter")?;
        }
        ntag::T_NestLoop => {
            // explain.c:2158: join.joinqual -> "Join Filter"; plan->qual -> "Filter".
            let nl = plan_node.expect_nestloop();
            let jq = clone_expr_qual(mcx, nl.join.joinqual.as_ref())?;
            show_upper_qual(es, mcx, plan_node, ancestors, jq, "Join Filter")?;
            let q = clone_expr_qual(mcx, plan.qual.as_ref())?;
            show_upper_qual(es, mcx, plan_node, ancestors, q, "Filter")?;
        }
        ntag::T_MergeJoin => {
            // explain.c:2169: mergeclauses -> "Merge Cond"; join.joinqual ->
            // "Join Filter"; plan->qual -> "Filter".
            let mj = plan_node.expect_mergejoin();
            let mut mc = alloc::vec::Vec::with_capacity(mj.mergeclauses.len());
            for e in mj.mergeclauses.iter() {
                mc.push(e.clone_in(mcx)?);
            }
            show_upper_qual(es, mcx, plan_node, ancestors, mc, "Merge Cond")?;
            let jq = clone_expr_qual(mcx, mj.join.joinqual.as_ref())?;
            show_upper_qual(es, mcx, plan_node, ancestors, jq, "Join Filter")?;
            let q = clone_expr_qual(mcx, plan.qual.as_ref())?;
            show_upper_qual(es, mcx, plan_node, ancestors, q, "Filter")?;
        }
        ntag::T_HashJoin => {
            // explain.c:2182: hashclauses -> "Hash Cond"; join.joinqual ->
            // "Join Filter"; plan->qual -> "Filter".
            let hj = plan_node.expect_hashjoin();
            let mut hc = alloc::vec::Vec::new();
            if let Some(hcl) = hj.hashclauses.as_ref() {
                hc.reserve(hcl.len());
                for n in hcl.iter() {
                    let e = n
                        .as_expr()
                        .expect("EXPLAIN: hashclause node is not an Expr")
                        .clone_in(mcx)?;
                    hc.push(e);
                }
            }
            show_upper_qual(es, mcx, plan_node, ancestors, hc, "Hash Cond")?;
            let jq = clone_expr_qual(mcx, hj.join.joinqual.as_ref())?;
            show_upper_qual(es, mcx, plan_node, ancestors, jq, "Join Filter")?;
            let q = clone_expr_qual(mcx, plan.qual.as_ref())?;
            show_upper_qual(es, mcx, plan_node, ancestors, q, "Filter")?;
        }
        ntag::T_TidScan => {
            // explain.c:2105: tidquals has OR semantics, so wrap a multi-element
            // list in a single make_orclause before showing it as "TID Cond";
            // then plan->qual -> "Filter".
            let ts = plan_node.expect_tidscan();
            let tidcond = build_cond_list(mcx, ts.tidquals.as_ref(), false)?;
            show_scan_qual_owned(es, mcx, plan_node, ancestors, tidcond, "TID Cond")?;
            show_scan_qual(es, mcx, plan_node, ancestors, plan.qual.as_ref(), "Filter")?;
        }
        ntag::T_TidRangeScan => {
            // explain.c:2127: tidrangequals has AND semantics, so wrap a
            // multi-element list in a single make_andclause; then Filter.
            let trs = plan_node.expect_tidrangescan();
            let tidcond = build_cond_list(mcx, trs.tidrangequals.as_ref(), true)?;
            show_scan_qual_owned(es, mcx, plan_node, ancestors, tidcond, "TID Cond")?;
            show_scan_qual(es, mcx, plan_node, ancestors, plan.qual.as_ref(), "Filter")?;
        }
        ntag::T_SampleScan => {
            // explain.c:2004: show_tablesample(((SampleScan *) plan)->tablesample,
            //   ...); then FALLTHROUGH to SeqScan to print the `Filter` qual.
            let ss = plan_node.expect_samplescan();
            if let Some(tsc) = ss.tablesample.as_deref() {
                show_tablesample(es, mcx, plan_node, ancestors, tsc)?;
            }
            show_scan_qual(es, mcx, plan_node, ancestors, plan.qual.as_ref(), "Filter")?;
        }
        ntag::T_Agg | ntag::T_WindowAgg | ntag::T_Group | ntag::T_Result => {
            // explain.c:2197/2208/2215/2237: these upper plan nodes show their
            // plan->qual via show_upper_qual (useprefix = list_length(rtable) > 1
            // || verbose), NOT show_scan_qual. Their `Filter` line must follow
            // their node-specific detail lines (Group Key / Run Condition /
            // One-Time Filter) to match C's single-case ordering, so it is emitted
            // in the sort-/group-key switch below — nothing here.
        }
        _ => {
            // The generic `Filter` leg (SeqScan / ValuesScan / CteScan /
            // NamedTuplestoreScan / WorkTableScan / SubqueryScan / Gather / etc).
            show_scan_qual(es, mcx, plan_node, ancestors, plan.qual.as_ref(), "Filter")?;
        }
    }

    // The sort-/group-key detail (`show_sort_keys` / `show_agg_keys` /
    // `show_group_keys`). These are NOT verbose-only in C — they always print
    // for Sort/IncrementalSort/Agg/Group nodes. The key columns refer to a
    // target list, deparsed against a plan context (the node itself for sort
    // keys, the *outer child* plan for group keys).
    match plan_node.node_tag() {
        ntag::T_Sort => {
            // show_sort_keys: show_sort_group_keys((PlanState*)sortstate,
            //   "Sort Key", numCols, 0, sortColIdx, sortOperators, collations,
            //   nullsFirst, ...). Context plan = the sort node itself.
            let s = plan_node.expect_sort();
            show_sort_group_keys(
                es,
                mcx,
                plan_node,
                plan,
                ancestors,
                "Sort Key",
                s.numCols,
                0,
                &s.sortColIdx,
                Some(&s.sortOperators),
                Some(&s.collations),
                Some(&s.nullsFirst),
            )?;
        }
        ntag::T_IncrementalSort => {
            // show_incremental_sort_keys (explain.c:2583): the full "Sort Key"
            // list plus a "Presorted Key" list of the first nPresortedCols keys.
            let s = plan_node.expect_incrementalsort();
            show_sort_group_keys(
                es,
                mcx,
                plan_node,
                plan,
                ancestors,
                "Sort Key",
                s.sort.numCols,
                s.nPresortedCols,
                &s.sort.sortColIdx,
                Some(&s.sort.sortOperators),
                Some(&s.sort.collations),
                Some(&s.sort.nullsFirst),
            )?;
            // show_incremental_sort_info(castNode(IncrementalSortState, planstate), es)
            // — the ANALYZE-time "Full-sort Groups:" / "Pre-sorted Groups:" sort
            // method + space (Memory/Disk) instrumentation lines (explain.c:2226).
            if let PlanStateNode::IncrementalSort(incrsortstate) = planstate {
                crate::details::show_incremental_sort_info(incrsortstate, es)?;
            }
        }
        ntag::T_Agg => {
            // show_agg_keys (explain.c:2477): when numCols > 0 OR grouping sets
            // are present, the key columns refer to the *child* plan's tlist.
            let agg = plan_node.expect_agg();
            let has_gsets = agg
                .grouping_sets
                .as_ref()
                .map(|g| !g.is_empty())
                .unwrap_or(false);
            if agg.num_cols > 0 || has_gsets {
                // ancestors = lcons(plan, ancestors): prepend this Agg node
                // (cloned into the 'es formatting arena) so the children's
                // deparse can resolve OUTER_VAR/PARAM through it.
                let mut agg_ancestors: PgVec<'es, PgBox<'es, Node<'es>>> = PgVec::new_in(mcx);
                agg_ancestors
                    .try_reserve(ancestors.len() + 1)
                    .map_err(|_| mcx.oom(0))?;
                agg_ancestors.push(mcx::alloc_in(mcx, plan_node.clone_in(mcx)?)?);
                for a in ancestors.iter() {
                    agg_ancestors.push(mcx::alloc_in(mcx, a.clone_in(mcx)?)?);
                }

                let child_plan = planstate
                    .outer_plan_state()
                    .and_then(|c| c.ps_head().plan)
                    .expect("show_agg_keys: outerPlanState(astate)->plan");
                let child = child_plan.plan_head();

                if has_gsets {
                    show_grouping_sets(es, mcx, child_plan, child, agg, &agg_ancestors)?;
                } else {
                    let grp = agg
                        .grp_col_idx
                        .as_ref()
                        .expect("show_agg_keys: grpColIdx with numCols>0");
                    show_sort_group_keys(
                        es,
                        mcx,
                        child_plan,
                        child,
                        &agg_ancestors,
                        "Group Key",
                        agg.num_cols,
                        0,
                        grp,
                        None,
                        None,
                        None,
                    )?;
                }
            }
            // explain.c:2198: show_upper_qual(plan->qual, "Filter") AFTER the
            // Group Key detail.
            let q = clone_expr_qual(mcx, plan.qual.as_ref())?;
            show_upper_qual(es, mcx, plan_node, ancestors, q, "Filter")?;
        }
        ntag::T_MergeAppend => {
            // show_merge_append_keys (explain.c:2600): the merge sort keys refer
            // to the MergeAppend node's own targetlist (context plan = the node
            // itself), printed as "Sort Key".
            let ma = plan_node.expect_mergeappend();
            // The MergeAppend's sort-key arrays are plain Vecs; show_sort_group_keys
            // takes mcx-allocated PgVecs (like the Sort node's), so copy them in.
            let mut col_idx: PgVec<'es, AttrNumber> = PgVec::new_in(mcx);
            col_idx.try_reserve(ma.sortColIdx.len()).map_err(|_| mcx.oom(0))?;
            for &v in ma.sortColIdx.iter() {
                col_idx.push(v);
            }
            let mut sort_ops: PgVec<'es, Oid> = PgVec::new_in(mcx);
            sort_ops.try_reserve(ma.sortOperators.len()).map_err(|_| mcx.oom(0))?;
            for &v in ma.sortOperators.iter() {
                sort_ops.push(v);
            }
            let mut colls: PgVec<'es, Oid> = PgVec::new_in(mcx);
            colls.try_reserve(ma.collations.len()).map_err(|_| mcx.oom(0))?;
            for &v in ma.collations.iter() {
                colls.push(v);
            }
            let mut nulls_first: PgVec<'es, bool> = PgVec::new_in(mcx);
            nulls_first.try_reserve(ma.nullsFirst.len()).map_err(|_| mcx.oom(0))?;
            for &v in ma.nullsFirst.iter() {
                nulls_first.push(v);
            }
            let numcols = ma.numCols;
            // Clone the MergeAppend node into the 'es arena so the context plan
            // and the 'es-allocated key arrays share one lifetime.
            let ma_node: PgBox<'es, Node<'es>> = mcx::alloc_in(mcx, plan_node.clone_in(mcx)?)?;
            let ma_head = ma_node.plan_head();
            show_sort_group_keys(
                es,
                mcx,
                &ma_node,
                ma_head,
                ancestors,
                "Sort Key",
                numcols,
                0,
                &col_idx,
                Some(&sort_ops),
                Some(&colls),
                Some(&nulls_first),
            )?;
        }
        ntag::T_Group => {
            // show_group_keys: keys refer to the *child* plan's tlist (no
            // sort-order arrays — Group Key).
            let g = plan_node.expect_group();
            let child_plan = planstate
                .outer_plan_state()
                .and_then(|c| c.ps_head().plan)
                .expect("show_group_keys: outerPlanState(gstate)->plan");
            let child = child_plan.plan_head();
            show_sort_group_keys(
                es,
                mcx,
                child_plan,
                child,
                ancestors,
                "Group Key",
                g.numCols,
                0,
                &g.grpColIdx,
                None,
                None,
                None,
            )?;
            // explain.c:2216: show_upper_qual(plan->qual, "Filter") AFTER Group Key.
            let q = clone_expr_qual(mcx, plan.qual.as_ref())?;
            show_upper_qual(es, mcx, plan_node, ancestors, q, "Filter")?;
        }
        ntag::T_Memoize => {
            // show_memoize_info (explain.c:2253): the "Cache Key" / "Cache Mode"
            // detail (and ANALYZE-time cache instrumentation).
            show_memoize_info(es, mcx, plan_node, ancestors, planstate)?;
        }
        ntag::T_WindowAgg | ntag::T_Result => {
            // explain.c:2208/2238: the WindowAgg `Run Condition` / Result
            // `One-Time Filter` detail lines were emitted earlier (before the
            // scan-qual switch); their plan->qual `Filter` follows via
            // show_upper_qual.
            let q = clone_expr_qual(mcx, plan.qual.as_ref())?;
            show_upper_qual(es, mcx, plan_node, ancestors, q, "Filter")?;
        }
        ntag::T_ModifyTable => {
            // explain.c:2242 / show_modifytable_info (explain.c:4520): the
            // Target Tables / FDW labeling (labeltargets) and EXPLAIN ANALYZE
            // path counts are runtime/FDW-only; the ON CONFLICT block (Conflict
            // Resolution / Conflict Arbiter Indexes / Conflict Filter) is the
            // plan-shape detail exercised by EXPLAIN of an INSERT ... ON CONFLICT.
            show_modifytable_info(es, mcx, plan_node, ancestors)?;
        }
        _ => {}
    }

    // Show buffer/WAL usage (explain.c:2284-2288). Per-worker buffer/WAL prep
    // (es->workers_state && verbose) needs PlanState.worker_instrument, which
    // the trimmed PlanState does not carry; the single-process path emits the
    // node's own accumulated counters.
    if let Some(instr) = planstate.ps_head().instrument.as_deref() {
        if es.buffers {
            crate::show_buffer_usage(es, &instr.bufusage)?;
        }
        if es.wal {
            crate::details::show_wal_usage(es, &instr.walusage)?;
        }
    }

    // If partition pruning was done during executor initialization, the number
    // of child plans we'll display below will be less than the number of
    // subplans specified in the plan. Emit "Subplans Removed" to make that less
    // mysterious. This field is a property of the parent node and *cannot* be
    // emitted within the Plans sub-node opened below. (explain.c:2330-2344)
    match plan_node.node_tag() {
        ntag::T_Append => {
            if let PlanStateNode::Append(a) = planstate {
                ExplainMissingMembers(
                    a.as_nplans,
                    plan_node.expect_append().appendplans.len() as i32,
                    es,
                )?;
            }
        }
        ntag::T_MergeAppend => {
            if let PlanStateNode::MergeAppend(m) = planstate {
                ExplainMissingMembers(
                    m.ms_nplans,
                    plan_node.expect_mergeappend().mergeplans.len() as i32,
                    es,
                )?;
            }
        }
        _ => {}
    }

    // Children. haschildren over initPlan / outer / inner / member nodes /
    // subPlan. The trimmed PlanState carries initPlan/subPlan as Option<PgVec>;
    // member-node nodes (Append/BitmapAnd/...) recurse through their own state.
    let head = planstate.ps_head();
    let has_init = head.initPlan.as_ref().map(|v| !v.is_empty()).unwrap_or(false);
    let has_sub = head.subPlan.as_ref().map(|v| !v.is_empty()).unwrap_or(false);
    let has_outer = planstate.outer_plan_state().is_some();
    let has_inner = head.righttree.is_some();
    // Member-node children: Append/MergeAppend appendplans/mergeplans,
    // BitmapAnd/BitmapOr bitmapplans (explain.c's `haschildren` member legs).
    let has_members = matches!(
        plan_node.node_tag(),
        ntag::T_Append | ntag::T_MergeAppend | ntag::T_BitmapAnd | ntag::T_BitmapOr
    ) && planstate
        .member_input_states()
        .map(|m| !m.is_empty())
        .unwrap_or(false);
    let haschildren = has_init || has_outer || has_inner || has_members || has_sub;

    // ancestors = lcons(plan, ancestors): prepend this Plan node (cloned into
    // the 'es formatting arena, matching es->pstmt) for the children's deparse,
    // so PARAM_EXEC / OUTER_VAR resolution can reach this node as an ancestor.
    // The owned model rebuilds the list (the C list_delete_first at block end is
    // implicit: `child_ancestors` is simply dropped and `ancestors` is reused).
    let child_ancestors: PgVec<'es, PgBox<'es, Node<'es>>> = if haschildren {
        fmt::ExplainOpenGroup("Plans", Some("Plans"), false, es)?;
        let mut v: PgVec<'es, PgBox<'es, Node<'es>>> = PgVec::new_in(mcx);
        v.try_reserve(ancestors.len() + 1).map_err(|_| mcx.oom(0))?;
        v.push(mcx::alloc_in(mcx, plan_node.clone_in(mcx)?)?);
        for a in ancestors.iter() {
            v.push(mcx::alloc_in(mcx, a.clone_in(mcx)?)?);
        }
        v
    } else {
        PgVec::new_in(mcx)
    };

    // initPlan-s.
    if has_init {
        if let Some(initplans) = head.initPlan.as_ref() {
            ExplainSubPlans(es, mcx, initplans, &child_ancestors, "InitPlan")?;
        }
    }

    // lefttree (Outer).
    if let Some(outer) = planstate.outer_plan_state() {
        ExplainNode(es, mcx, outer, &child_ancestors, Some("Outer"), None)?;
    }
    // righttree (Inner).
    if let Some(inner) = head.righttree.as_deref() {
        ExplainNode(es, mcx, inner, &child_ancestors, Some("Inner"), None)?;
    }

    // Special member-node children (explain.c:2042-2065): Append/MergeAppend
    // recurse into appendplans/mergeplans, BitmapAnd/BitmapOr into bitmapplans,
    // each via ExplainMemberNodes (relationship "Member"). The member plan-state
    // vectors are threaded onto the owned state structs and exposed through
    // `member_input_states()`. (SubqueryScan recurses through its subplan, and
    // CustomScan through custom_ps; those legs land with those nodes.)
    if let ntag::T_Append | ntag::T_MergeAppend | ntag::T_BitmapAnd | ntag::T_BitmapOr =
        plan_node.node_tag()
    {
        if let Some(members) = planstate.member_input_states() {
            ExplainMemberNodes(es, mcx, &members, &child_ancestors)?;
        }
    }

    // subPlan-s.
    if has_sub {
        if let Some(subplans) = head.subPlan.as_ref() {
            ExplainSubPlans(es, mcx, subplans, &child_ancestors, "SubPlan")?;
        }
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

/// `show_plan_tlist(planstate, ancestors, es)` (explain.c:2438): emit the
/// VERBOSE-only `Output:` line — the node's target list deparsed, one entry per
/// `TargetEntry` (resjunk ones included, per the C comment "we now include
/// resjunk ones"). Several node kinds suppress it (empty tlist, Append,
/// MergeAppend, RecursiveUnion, and a direct-modify ForeignScan). The deparse
/// runs against the running plan node via the `deparse_expr_for_plan` seam
/// (= `set_deparse_context_plan` + `deparse_expression`), with
/// `useprefix = es->rtable_size > 1` (note: NOT `|| es->verbose`, unlike the
/// qual/key helpers — show_plan_tlist already only runs under VERBOSE).
fn show_plan_tlist<'es, 'p>(
    es: &mut ExplainState<'es>,
    mcx: Mcx<'es>,
    plan_node: &Node<'p>,
    ancestors: &PgVec<'es, PgBox<'es, Node<'es>>>,
) -> PgResult<()> {
    let plan: &Plan<'p> = plan_node.plan_head();

    // No work if empty tlist (this occurs eg in bitmap indexscans).
    let Some(tlist) = plan.targetlist.as_ref().filter(|t| !t.is_empty()) else {
        return Ok(());
    };

    // The tlist of an Append isn't real helpful, so suppress it; likewise for
    // MergeAppend and RecursiveUnion.
    match plan_node.node_tag() {
        ntag::T_Append | ntag::T_MergeAppend | ntag::T_RecursiveUnion => return Ok(()),
        _ => {}
    }

    // Likewise for a ForeignScan that executes a direct INSERT/UPDATE/DELETE:
    // its tlist contains subplan-output / row-identity junk columns confusing
    // in this context. `IsA(plan, ForeignScan) && operation != CMD_SELECT`.
    if let Some(fs) = plan_node.as_foreignscan() {
        if fs.operation != CmdType::CMD_SELECT {
            return Ok(());
        }
    }

    // Set up deparsing context + deparse each result column. The
    // deparse_namespace is owner-private to ruleutils, so
    // set_deparse_context_plan + deparse_expression are folded into the one
    // `deparse_expr_for_plan` seam (same as show_scan_qual / show_sort_group_keys).
    let useprefix = es.rtable_size > 1;

    let plan_owned: PgBox<'es, Node<'es>> = mcx::alloc_in(mcx, plan_node.clone_in(mcx)?)?;
    let es_pstmt = es
        .pstmt
        .as_deref()
        .expect("EXPLAIN: es->pstmt must be set before deparse");

    let mut result: alloc::vec::Vec<alloc::string::String> =
        alloc::vec::Vec::with_capacity(tlist.len());
    for tle in tlist.iter() {
        let expr = tle
            .expr
            .as_deref()
            .expect("show_plan_tlist: TargetEntry has no expr");
        // node = (Node *) tle->expr.
        let expr_node = Node::mk_expr(mcx, expr.clone_in(mcx)?)?;
        let exprstr = ruleutils_s::deparse_expr_for_plan::call(
            mcx,
            es_pstmt,
            &es.rtable_names,
            &plan_owned,
            ancestors,
            &expr_node,
            useprefix,
            false,
        )?;
        result.push(alloc::string::String::from(exprstr.as_str()));
    }

    // Print results: ExplainPropertyList("Output", result, es).
    let view: alloc::vec::Vec<&str> = result.iter().map(|s| s.as_str()).collect();
    fmt::ExplainPropertyList("Output", &view, es)?;
    Ok(())
}

/// `show_scan_qual(qual, qlabel, planstate, ancestors, es)` (explain.c:2470):
/// deparse the AND of `qual`'s conditions and emit them as a `<qlabel>:` line.
/// A NULL/empty qual prints nothing. C: `useprefix = IsA(plan, SubqueryScan) ||
/// es->verbose`. The deparse runs against the running plan node (cloned into the
/// `'es` formatting arena so it matches `es->pstmt`, the `'es` plan-tree copy).
fn show_scan_qual<'es, 'p>(
    es: &mut ExplainState<'es>,
    mcx: Mcx<'es>,
    plan_node: &Node<'p>,
    ancestors: &PgVec<'es, PgBox<'es, Node<'es>>>,
    qual: Option<&PgVec<'p, types_nodes::primnodes::Expr>>,
    qlabel: &str,
) -> PgResult<()> {
    let Some(qual) = qual.filter(|q| !q.is_empty()) else {
        return Ok(());
    };
    // node = (Node *) make_ands_explicit(qual);
    // Deep-clone via clone_in: a qual may carry a SubPlan / Aggref child, on
    // which a bare derived `Expr::clone()` panics (clone-in convention).
    let mut exprs: alloc::vec::Vec<types_nodes::primnodes::Expr> =
        alloc::vec::Vec::with_capacity(qual.len());
    for e in qual.iter() {
        exprs.push(e.clone_in(mcx)?);
    }
    let anded = backend_nodes_core::makefuncs::make_ands_explicit(exprs);
    let node = Node::mk_expr(mcx, anded)?;

    let useprefix = matches!(plan_node.node_tag(), ntag::T_SubqueryScan) || es.verbose;

    // context = set_deparse_context_plan(es->deparse_cxt, planstate->plan,
    //                                    ancestors); exprstr =
    // deparse_expression(node, context, useprefix, false). Folded into one
    // ruleutils seam (the deparse_namespace is owner-private).
    let plan_owned: PgBox<'es, Node<'es>> = mcx::alloc_in(mcx, plan_node.clone_in(mcx)?)?;
    let es_pstmt = es
        .pstmt
        .as_deref()
        .expect("EXPLAIN: es->pstmt must be set before deparse");
    let exprstr = ruleutils_s::deparse_expr_for_plan::call(
        mcx,
        es_pstmt,
        &es.rtable_names,
        &plan_owned,
        ancestors,
        &node,
        useprefix,
        false,
    )?;
    // ExplainPropertyText(qlabel, exprstr, es);
    fmt::ExplainPropertyText(qlabel, exprstr.as_str(), es)?;
    Ok(())
}

/// `show_tablesample(tsc, planstate, ancestors, es)` (explain.c:3018) — show the
/// TABLESAMPLE method, its parameters, and the REPEATABLE seed for a SampleScan.
fn show_tablesample<'es, 'p>(
    es: &mut ExplainState<'es>,
    mcx: Mcx<'es>,
    plan_node: &Node<'p>,
    ancestors: &PgVec<'es, PgBox<'es, Node<'es>>>,
    tsc: &types_nodes::nodesamplescan::TableSampleClause<'p>,
) -> PgResult<()> {
    // useprefix = es->rtable_size > 1;
    let useprefix = es.rtable_size > 1;

    // method_name = get_func_name(tsc->tsmhandler);
    let method_name = backend_utils_cache_lsyscache::function::get_func_name(mcx, tsc.tsmhandler)?
        .map(|s| s.as_str().to_string())
        .unwrap_or_default();

    let plan_owned: PgBox<'es, Node<'es>> = mcx::alloc_in(mcx, plan_node.clone_in(mcx)?)?;
    let es_pstmt = es
        .pstmt
        .as_deref()
        .expect("EXPLAIN: es->pstmt must be set before deparse");

    // Deparse parameter expressions: params = lappend(params,
    //   deparse_expression(arg, context, useprefix, false)).
    let mut params: alloc::vec::Vec<alloc::string::String> = alloc::vec::Vec::new();
    if let Some(args) = tsc.args.as_ref() {
        for arg in args.iter() {
            let node = Node::mk_expr(mcx, arg.clone_in(mcx)?)?;
            let s = ruleutils_s::deparse_expr_for_plan::call(
                mcx,
                es_pstmt,
                &es.rtable_names,
                &plan_owned,
                ancestors,
                &node,
                useprefix,
                false,
            )?;
            params.push(s.as_str().to_string());
        }
    }

    // repeatable = tsc->repeatable ? deparse_expression(...) : NULL;
    let repeatable: Option<alloc::string::String> = match tsc.repeatable.as_deref() {
        Some(rep) => {
            let node = Node::mk_expr(mcx, rep.clone_in(mcx)?)?;
            let s = ruleutils_s::deparse_expr_for_plan::call(
                mcx,
                es_pstmt,
                &es.rtable_names,
                &plan_owned,
                ancestors,
                &node,
                useprefix,
                false,
            )?;
            Some(s.as_str().to_string())
        }
        None => None,
    };

    if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
        // ExplainIndentText(es);
        // appendStringInfo(es->str, "Sampling: %s (", method_name);
        fmt::ExplainIndentText(es)?;
        es.str.try_push_str("Sampling: ")?;
        es.str.try_push_str(&method_name)?;
        es.str.try_push_str(" (")?;
        let mut first = true;
        for p in params.iter() {
            if !first {
                es.str.try_push_str(", ")?;
            }
            es.str.try_push_str(p)?;
            first = false;
        }
        es.str.try_push(')')?;
        if let Some(rep) = repeatable.as_ref() {
            es.str.try_push_str(" REPEATABLE (")?;
            es.str.try_push_str(rep)?;
            es.str.try_push(')')?;
        }
        es.str.try_push('\n')?;
    } else {
        fmt::ExplainPropertyText("Sampling Method", &method_name, es)?;
        let refs: alloc::vec::Vec<&str> = params.iter().map(|s| s.as_str()).collect();
        fmt::ExplainPropertyList("Sampling Parameters", &refs, es)?;
        if let Some(rep) = repeatable.as_ref() {
            fmt::ExplainPropertyText("Repeatable Seed", rep, es)?;
        }
    }
    Ok(())
}

/// `show_modifytable_info(mtstate, ancestors, es)` (explain.c:4520) — the
/// ON CONFLICT detail block for a ModifyTable node. The Target Tables labeling,
/// FDW `ExplainForeignModify`, and EXPLAIN ANALYZE path-count instrumentation
/// (which require live `ModifyTableState`/`ResultRelInfo`/`FdwRoutine` runtime
/// state) are not part of this plan-shape slice; this ports the
/// `node->onConflictAction != ONCONFLICT_NONE` arm — the Conflict Resolution
/// string, the Conflict Arbiter Indexes list (suppressed for the DO NOTHING
/// implicit-ignore variant), and the Conflict Filter qual.
fn show_modifytable_info<'es, 'p>(
    es: &mut ExplainState<'es>,
    mcx: Mcx<'es>,
    plan_node: &Node<'p>,
    ancestors: &PgVec<'es, PgBox<'es, Node<'es>>>,
) -> PgResult<()> {
    use types_nodes::nodes::OnConflictAction;

    let node = plan_node.expect_modifytable();

    // explain.c:4632 — gather arbiter index names via get_rel_name.
    let mut idx_names: alloc::vec::Vec<alloc::string::String> = alloc::vec::Vec::new();
    if let Some(arbiters) = node.arbiterIndexes.as_ref() {
        for &idx_oid in arbiters.iter() {
            // C uses the bare get_rel_name (no "?" placeholder fallback here).
            if let Some(name) = backend_utils_cache_lsyscache::relation::get_rel_name(mcx, idx_oid)? {
                idx_names.push(alloc::string::String::from(name.as_str()));
            }
        }
    }

    if node.onConflictAction != OnConflictAction::ONCONFLICT_NONE {
        fmt::ExplainPropertyText(
            "Conflict Resolution",
            if node.onConflictAction == OnConflictAction::ONCONFLICT_NOTHING {
                "NOTHING"
            } else {
                "UPDATE"
            },
            es,
        )?;

        // Don't display arbiter indexes at all when the DO NOTHING variant
        // implicitly ignores all conflicts (idxNames == NIL).
        if !idx_names.is_empty() {
            let refs: alloc::vec::Vec<&str> = idx_names.iter().map(|s| s.as_str()).collect();
            fmt::ExplainPropertyList("Conflict Arbiter Indexes", &refs, es)?;
        }

        // ON CONFLICT DO UPDATE WHERE qual is specially displayed.
        if let Some(where_qual) = node.onConflictWhere.as_ref() {
            if !where_qual.is_empty() {
                let q = clone_expr_qual(mcx, Some(where_qual))?;
                show_upper_qual(es, mcx, plan_node, ancestors, q, "Conflict Filter")?;
                // show_instrumentation_count("Rows Removed by Conflict Filter", ...)
                // is EXPLAIN-ANALYZE-only instrumentation, not part of this slice.
            }
        }
    }
    Ok(())
}

/// `show_upper_qual(qual, qlabel, ...)` (explain.c:2554) — show a qualifier
/// expression for an upper-level (join / agg / result / window) plan node. Same
/// make_ands_explicit + deparse as `show_scan_qual`, but with the upper-node
/// prefix rule `useprefix = es->rtable_size > 1 || es->verbose`.
fn show_upper_qual<'es, 'p>(
    es: &mut ExplainState<'es>,
    mcx: Mcx<'es>,
    plan_node: &Node<'p>,
    ancestors: &PgVec<'es, PgBox<'es, Node<'es>>>,
    exprs: alloc::vec::Vec<types_nodes::primnodes::Expr>,
    qlabel: &str,
) -> PgResult<()> {
    if exprs.is_empty() {
        return Ok(());
    }
    let anded = backend_nodes_core::makefuncs::make_ands_explicit(exprs);
    let node = Node::mk_expr(mcx, anded)?;

    let useprefix = es.rtable_size > 1 || es.verbose;

    let plan_owned: PgBox<'es, Node<'es>> = mcx::alloc_in(mcx, plan_node.clone_in(mcx)?)?;
    let es_pstmt = es
        .pstmt
        .as_deref()
        .expect("EXPLAIN: es->pstmt must be set before deparse");
    let exprstr = ruleutils_s::deparse_expr_for_plan::call(
        mcx,
        es_pstmt,
        &es.rtable_names,
        &plan_owned,
        ancestors,
        &node,
        useprefix,
        false,
    )?;
    fmt::ExplainPropertyText(qlabel, exprstr.as_str(), es)?;
    Ok(())
}

/// Clone a `PgVec<Expr>` qual list into the deparse arena (clone_in for SubPlan/
/// Aggref children). Returns an empty vec for `None`.
fn clone_expr_qual<'es>(
    mcx: Mcx<'es>,
    qual: Option<&PgVec<'_, types_nodes::primnodes::Expr>>,
) -> PgResult<alloc::vec::Vec<types_nodes::primnodes::Expr>> {
    let mut out = alloc::vec::Vec::new();
    if let Some(q) = qual {
        out.reserve(q.len());
        for e in q.iter() {
            out.push(e.clone_in(mcx)?);
        }
    }
    Ok(out)
}

/// Build the single-clause TID condition list explain.c constructs for a
/// TidScan/TidRangeScan: an empty/None list yields an empty Vec; a single clause
/// is cloned as-is; a multi-clause list is wrapped in one `make_orclause`
/// (`is_and = false`, OR semantics for tidquals) or `make_andclause`
/// (`is_and = true`, AND semantics for tidrangequals). The result is then
/// rendered by `show_scan_qual_owned` (which `make_ands_explicit`s a 1-element
/// list to that very clause).
fn build_cond_list<'es>(
    mcx: Mcx<'es>,
    quals: Option<&PgVec<'_, types_nodes::primnodes::Expr>>,
    is_and: bool,
) -> PgResult<alloc::vec::Vec<types_nodes::primnodes::Expr>> {
    let Some(quals) = quals.filter(|q| !q.is_empty()) else {
        return Ok(alloc::vec::Vec::new());
    };
    let mut cloned: alloc::vec::Vec<types_nodes::primnodes::Expr> =
        alloc::vec::Vec::with_capacity(quals.len());
    for e in quals.iter() {
        cloned.push(e.clone_in(mcx)?);
    }
    if cloned.len() > 1 {
        let wrapped = if is_and {
            backend_nodes_core::makefuncs::make_andclause(cloned)
        } else {
            backend_nodes_core::makefuncs::make_orclause(cloned)
        };
        Ok(alloc::vec![wrapped])
    } else {
        Ok(cloned)
    }
}

/// Like `show_scan_qual` but over an owned `Vec` (the caller pre-wrapped the
/// list). Uses the scan-qual prefix rule `useprefix = IsA(plan, SubqueryScan)
/// || es->verbose` (explain.c:2470), which for a TidScan/TidRangeScan reduces to
/// `es->verbose` — so the scan's own Var is unqualified while an outer
/// (join-inner) Var keeps its alias, matching pg_regress's `TID Cond` output.
fn show_scan_qual_owned<'es, 'p>(
    es: &mut ExplainState<'es>,
    mcx: Mcx<'es>,
    plan_node: &Node<'p>,
    ancestors: &PgVec<'es, PgBox<'es, Node<'es>>>,
    exprs: alloc::vec::Vec<types_nodes::primnodes::Expr>,
    qlabel: &str,
) -> PgResult<()> {
    if exprs.is_empty() {
        return Ok(());
    }
    let anded = backend_nodes_core::makefuncs::make_ands_explicit(exprs);
    let node = Node::mk_expr(mcx, anded)?;

    let useprefix = matches!(plan_node.node_tag(), ntag::T_SubqueryScan) || es.verbose;

    let plan_owned: PgBox<'es, Node<'es>> = mcx::alloc_in(mcx, plan_node.clone_in(mcx)?)?;
    let es_pstmt = es
        .pstmt
        .as_deref()
        .expect("EXPLAIN: es->pstmt must be set before deparse");
    let exprstr = ruleutils_s::deparse_expr_for_plan::call(
        mcx,
        es_pstmt,
        &es.rtable_names,
        &plan_owned,
        ancestors,
        &node,
        useprefix,
        false,
    )?;
    fmt::ExplainPropertyText(qlabel, exprstr.as_str(), es)?;
    Ok(())
}

/// `show_indexsearches_info(planstate, es)` (explain.c:3837) — show the total
/// number of index searches for an IndexScan/IndexOnlyScan/BitmapIndexScan node.
/// Sums the local-process counter with each parallel worker's counter (when a
/// SharedInfo is present); the `--single` / non-parallel path has no SharedInfo
/// and reports only the local count.
fn show_indexsearches_info<'es, 'p>(
    es: &mut ExplainState<'es>,
    planstate: &PlanStateNode<'p>,
) -> PgResult<()> {
    if !es.analyze {
        return Ok(());
    }

    // Initialize counters with stats from the local process first, then add the
    // sum of the per-worker counters (explain.c:3848-3888).
    let (mut nsearches, shared) = match planstate {
        PlanStateNode::IndexScan(m) => (m.iss_Instrument.nsearches, m.iss_SharedInfo.as_deref()),
        PlanStateNode::IndexOnlyScan(m) => {
            (m.ioss_Instrument.nsearches, m.ioss_SharedInfo.as_deref())
        }
        PlanStateNode::BitmapIndexScan(m) => {
            (m.biss_Instrument.nsearches, m.biss_SharedInfo.as_deref())
        }
        // C's `default: break;` — only the three index node types reach here.
        _ => return Ok(()),
    };

    if let Some(shared) = shared {
        for w in shared.winstrument.iter() {
            nsearches += w.nsearches;
        }
    }

    fmt::ExplainPropertyUInteger("Index Searches", None, nsearches, es)
}

/// `show_expression(node, qlabel, planstate, ancestors, useprefix, es)`
/// (explain.c:2870) — deparse and print a single expression `node` under
/// `qlabel`. Used by the TableFuncScan "Table Function Call" leg.
fn show_expression<'es, 'p>(
    es: &mut ExplainState<'es>,
    mcx: Mcx<'es>,
    plan_node: &Node<'p>,
    ancestors: &PgVec<'es, PgBox<'es, Node<'es>>>,
    node: &Node<'es>,
    qlabel: &str,
) -> PgResult<()> {
    // useprefix = es->verbose (the only caller, TableFuncScan, passes
    // es->verbose).
    let useprefix = es.verbose;
    let plan_owned: PgBox<'es, Node<'es>> = mcx::alloc_in(mcx, plan_node.clone_in(mcx)?)?;
    let es_pstmt = es
        .pstmt
        .as_deref()
        .expect("EXPLAIN: es->pstmt must be set before deparse");
    let exprstr = ruleutils_s::deparse_expr_for_plan::call(
        mcx,
        es_pstmt,
        &es.rtable_names,
        &plan_owned,
        ancestors,
        node,
        useprefix,
        false,
    )?;
    fmt::ExplainPropertyText(qlabel, exprstr.as_str(), es)?;
    Ok(())
}

/// `ExplainMissingMembers(nplans, nchildren, es)` (explain.c:4783) — report on
/// any subplans removed by run-time partition pruning. `nplans` is the number
/// of live subplans, `nchildren` the original number specified in the plan;
/// some of the latter may have been pruned during executor initialization. Only
/// emitted in text format when something was actually removed, but always in
/// the structured formats.
#[allow(non_snake_case)]
fn ExplainMissingMembers(nplans: i32, nchildren: i32, es: &mut ExplainState<'_>) -> PgResult<()> {
    if nplans < nchildren || es.format != ExplainFormat::EXPLAIN_FORMAT_TEXT {
        fmt::ExplainPropertyInteger("Subplans Removed", None, (nchildren - nplans) as i64, es)?;
    }
    Ok(())
}

/// `ExplainMemberNodes(planstates, nplans, ancestors, es)` (explain.c:4537) —
/// explain a list of the member-node child plan states of an Append /
/// MergeAppend / BitmapAnd / BitmapOr node, each as a "Member" child.
#[allow(non_snake_case)]
fn ExplainMemberNodes<'es, 'p>(
    es: &mut ExplainState<'es>,
    mcx: Mcx<'es>,
    planstates: &[&PlanStateNode<'p>],
    ancestors: &PgVec<'es, PgBox<'es, Node<'es>>>,
) -> PgResult<()> {
    // for (j = 0; j < nplans; j++)
    //     ExplainNode(planstates[j], ancestors, "Member", NULL, es);
    for child in planstates {
        ExplainNode(es, mcx, child, ancestors, Some("Member"), None)?;
    }
    Ok(())
}

/// `ExplainSubPlans(plans, ancestors, relationship, es)` (explain.c:4561) —
/// explain a list of `SubPlanState`s (a plan node's initPlan or subPlan list).
/// Each physical subplan (`plan_id`) is printed only once across the whole plan
/// tree (`es->printed_subplans`). The `SubPlan` node is treated as an ancestor
/// of the plan node(s) within it, so ruleutils.c can resolve the referents of
/// subplan parameters (`find_param_referent` / `find_param_generator`).
#[allow(non_snake_case)]
fn ExplainSubPlans<'es, 'p>(
    es: &mut ExplainState<'es>,
    mcx: Mcx<'es>,
    plans: &PgVec<'p, types_nodes::execexpr::SubPlanState<'p>>,
    ancestors: &PgVec<'es, PgBox<'es, Node<'es>>>,
    relationship: &str,
) -> PgResult<()> {
    use backend_nodes_core::bitmapset::{bms_add_member, bms_is_member};

    for sps in plans.iter() {
        let Some(sp) = sps.subplan.as_deref() else {
            continue;
        };

        // Print a subplan only once (track plan_id across the plan tree).
        if bms_is_member(sp.plan_id, es.printed_subplans.as_deref()) {
            continue;
        }
        es.printed_subplans =
            Some(bms_add_member(mcx, es.printed_subplans.take(), sp.plan_id)?);

        // ancestors = lcons(sp, ancestors): treat the SubPlan node as an
        // ancestor so ruleutils can find subplan-parameter referents.
        let sub_node = Node::mk_expr(
            mcx,
            types_nodes::primnodes::Expr::SubPlan(
                types_nodes::primnodes::SubPlanExpr::from_subplan(mcx, sp)?,
            ),
        )?;
        let mut child_ancestors: PgVec<'es, PgBox<'es, Node<'es>>> = PgVec::new_in(mcx);
        child_ancestors
            .try_reserve(ancestors.len() + 1)
            .map_err(|_| mcx.oom(0))?;
        child_ancestors.push(mcx::alloc_in(mcx, sub_node)?);
        for a in ancestors.iter() {
            child_ancestors.push(mcx::alloc_in(mcx, a.clone_in(mcx)?)?);
        }

        // ExplainNode(sps->planstate, ancestors, relationship, sp->plan_name, es).
        let plan_name = sp.plan_name.as_ref().map(|s| s.as_str());
        if let Some(child_ps) = sps.planstate.as_deref() {
            ExplainNode(es, mcx, child_ps, &child_ancestors, Some(relationship), plan_name)?;
        }
    }
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

    match plan_node.node_tag() {
        ntag::T_SeqScan => {
            scantarget::ExplainScanTarget(es, plan_node, plan_node.expect_seqscan().scan.scanrelid)
        }
        ntag::T_SampleScan => scantarget::ExplainScanTarget(
            es,
            plan_node,
            plan_node.expect_samplescan().scan.scanrelid,
        ),
        ntag::T_BitmapHeapScan => scantarget::ExplainScanTarget(
            es,
            plan_node,
            plan_node.expect_bitmapheapscan().scan.scanrelid,
        ),
        ntag::T_TidScan => {
            scantarget::ExplainScanTarget(es, plan_node, plan_node.expect_tidscan().scan.scanrelid)
        }
        ntag::T_TidRangeScan => scantarget::ExplainScanTarget(
            es,
            plan_node,
            plan_node.expect_tidrangescan().scan.scanrelid,
        ),
        ntag::T_SubqueryScan => scantarget::ExplainScanTarget(
            es,
            plan_node,
            plan_node.expect_subqueryscan().scan.scanrelid,
        ),
        ntag::T_FunctionScan => scantarget::ExplainScanTarget(
            es,
            plan_node,
            plan_node.expect_functionscan().scan.scanrelid,
        ),
        ntag::T_TableFuncScan => scantarget::ExplainScanTarget(
            es,
            plan_node,
            plan_node.expect_tablefuncscan().scan.scanrelid,
        ),
        ntag::T_ValuesScan => scantarget::ExplainScanTarget(
            es,
            plan_node,
            plan_node.expect_valuesscan().scan.scanrelid,
        ),
        ntag::T_CteScan => {
            scantarget::ExplainScanTarget(es, plan_node, plan_node.expect_ctescan().scan.scanrelid)
        }
        ntag::T_WorkTableScan => scantarget::ExplainScanTarget(
            es,
            plan_node,
            plan_node.expect_worktablescan().scan.scanrelid,
        ),
        ntag::T_ForeignScan => {
            // if (((Scan *) plan)->scanrelid > 0) ExplainScanTarget(...)
            let s = plan_node.expect_foreignscan();
            if s.scan.scanrelid > 0 {
                scantarget::ExplainScanTarget(es, plan_node, s.scan.scanrelid)
            } else {
                Ok(())
            }
        }
        ntag::T_CustomScan => {
            let s = plan_node.expect_customscan();
            if s.scan.scanrelid > 0 {
                scantarget::ExplainScanTarget(es, plan_node, s.scan.scanrelid)
            } else {
                Ok(())
            }
        }
        ntag::T_IndexScan => {
            let s = plan_node.expect_indexscan();
            scantarget::ExplainIndexScanDetails(es, s.indexid, s.indexorderdir)?;
            scantarget::ExplainScanTarget(es, plan_node, s.scan.scanrelid)
        }
        ntag::T_IndexOnlyScan => {
            let s = plan_node.expect_indexonlyscan();
            scantarget::ExplainIndexScanDetails(es, s.indexid, s.indexorderdir)?;
            scantarget::ExplainScanTarget(es, plan_node, s.scan.scanrelid)
        }
        ntag::T_BitmapIndexScan => {
            let s = plan_node.expect_bitmapindexscan();
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
        ntag::T_ModifyTable => scantarget::ExplainModifyTarget(
            es,
            plan_node,
            plan_node.expect_modifytable().nominalRelation,
        ),
        ntag::T_NestLoop | ntag::T_MergeJoin | ntag::T_HashJoin => {
            // explain.c:1716 — interpolate the join type into the node name.
            use types_nodes::jointype::JoinType;
            let tag = plan_node.node_tag();
            let jointype = match tag {
                ntag::T_NestLoop => plan_node.expect_nestloop().join.jointype,
                ntag::T_MergeJoin => plan_node.expect_mergejoin().join.jointype,
                _ => plan_node.expect_hashjoin().join.jointype,
            };
            let jointype_str = match jointype {
                JoinType::JOIN_INNER => "Inner",
                JoinType::JOIN_LEFT => "Left",
                JoinType::JOIN_FULL => "Full",
                JoinType::JOIN_RIGHT => "Right",
                JoinType::JOIN_SEMI => "Semi",
                JoinType::JOIN_ANTI => "Anti",
                JoinType::JOIN_RIGHT_SEMI => "Right Semi",
                JoinType::JOIN_RIGHT_ANTI => "Right Anti",
                _ => "???",
            };
            if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
                // For historical reasons, the join type is interpolated into the
                // node type name in TEXT format.
                if jointype != JoinType::JOIN_INNER {
                    es.str.try_push_str(" ")?;
                    es.str.try_push_str(jointype_str)?;
                    es.str.try_push_str(" Join")?;
                } else if tag != ntag::T_NestLoop {
                    es.str.try_push_str(" Join")?;
                }
                Ok(())
            } else {
                fmt::ExplainPropertyText("Join Type", jointype_str, es)
            }
        }
        _ => Ok(()),
    }
}

/// `show_sort_group_keys` (explain.c:2768). Deparse each key column's tlist
/// expression against `context_plan` and emit them as a `qlabel:` property
/// list (e.g. `Sort Key:` / `Group Key:`). When `sort_operators` is `Some`
/// (sort keys), append per-key `COLLATE`/`DESC`/`USING`/`NULLS` options via
/// [`show_sortorder_options`].
///
/// `context_plan`/`context_plan_head` are the plan whose target list holds the
/// key expressions (the node itself for sort keys, the outer child for group
/// keys); both refer to the same node, passed split so we needn't re-`plan_head`.
#[allow(clippy::too_many_arguments)]
/// `show_window_def(planstate, ancestors, es)` (explain.c:2888). Render the
/// window definition (`winname AS (...)`) for a WindowAgg node: the PARTITION BY
/// and ORDER BY key columns (deparsed against the child plan's tlist) plus the
/// non-default frame clause.
fn show_window_def<'es, 'p>(
    es: &mut ExplainState<'es>,
    mcx: Mcx<'es>,
    plan_node: &Node<'p>,
    planstate: &PlanStateNode<'p>,
    ancestors: &PgVec<'es, PgBox<'es, Node<'es>>>,
) -> PgResult<()> {
    let wagg = plan_node.expect_windowagg();

    // appendStringInfo(&wbuf, "%s AS (", quote_identifier(wagg->winname));
    let winname = wagg
        .winname
        .as_ref()
        .map(|s| s.as_str())
        .expect("show_window_def: WindowAgg has no winname");
    let quoted = backend_utils_adt_ruleutils::quote_identifier(mcx, winname)?;
    let mut wbuf = alloc::string::String::new();
    wbuf.push_str(quoted.as_str());
    wbuf.push_str(" AS (");

    // ancestors = lcons(wagg, ancestors): prepend the WindowAgg plan node for the
    // key/frame deparse (so PARAM_EXEC/OUTER_VAR resolution reaches it).
    let mut child_ancestors: PgVec<'es, PgBox<'es, Node<'es>>> = PgVec::new_in(mcx);
    child_ancestors
        .try_reserve(ancestors.len() + 1)
        .map_err(|_| mcx.oom(0))?;
    child_ancestors.push(mcx::alloc_in(mcx, plan_node.clone_in(mcx)?)?);
    for a in ancestors.iter() {
        child_ancestors.push(mcx::alloc_in(mcx, a.clone_in(mcx)?)?);
    }

    // The key columns refer to the tlist of the child plan.
    let child_plan = planstate
        .outer_plan_state()
        .and_then(|c| c.ps_head().plan)
        .expect("show_window_def: outerPlanState(planstate)->plan");
    let child = child_plan.plan_head();

    let mut needspace = false;
    if wagg.partNumCols > 0 {
        wbuf.push_str("PARTITION BY ");
        let cols = wagg
            .partColIdx
            .as_ref()
            .expect("show_window_def: partColIdx with partNumCols>0");
        show_window_keys(
            es,
            mcx,
            &mut wbuf,
            child_plan,
            child,
            wagg.partNumCols,
            cols,
            &child_ancestors,
        )?;
        needspace = true;
    }
    if wagg.ordNumCols > 0 {
        if needspace {
            wbuf.push(' ');
        }
        wbuf.push_str("ORDER BY ");
        let cols = wagg
            .ordColIdx
            .as_ref()
            .expect("show_window_def: ordColIdx with ordNumCols>0");
        show_window_keys(
            es,
            mcx,
            &mut wbuf,
            child_plan,
            child,
            wagg.ordNumCols,
            cols,
            &child_ancestors,
        )?;
        needspace = true;
    }

    // ancestors = list_delete_first(ancestors): the frame deparse uses the
    // original ancestor list (without the WindowAgg prepended).
    const FRAMEOPTION_NONDEFAULT: i32 = 0x00001;
    if wagg.frameOptions & FRAMEOPTION_NONDEFAULT != 0 {
        let useprefix = es.rtable_names.len() > 1 || es.verbose;
        // Clone pstmt and the WindowAgg plan node into the 'es formatting arena
        // so all deparse-seam arguments share one lifetime (the seam's `'p`
        // unifies to `'es`, matching the `deparse_expr_for_plan` callers).
        let es_pstmt = es
            .pstmt
            .as_deref()
            .expect("EXPLAIN: es->pstmt must be set before deparse")
            .clone_in(mcx)?;
        let es_pstmt: PgBox<'es, PlannedStmt<'es>> = mcx::alloc_in(mcx, es_pstmt)?;
        let plan_owned: PgBox<'es, Node<'es>> = mcx::alloc_in(mcx, plan_node.clone_in(mcx)?)?;
        // Wrap the offset Exprs as Nodes for the deparse seam.
        let start_node = match wagg.startOffset.as_deref() {
            Some(e) => Some(Node::mk_expr(mcx, e.clone_in(mcx)?)?),
            None => None,
        };
        let end_node = match wagg.endOffset.as_deref() {
            Some(e) => Some(Node::mk_expr(mcx, e.clone_in(mcx)?)?),
            None => None,
        };
        let framestr = ruleutils_s::deparse_window_frame_for_plan::call(
            mcx,
            &es_pstmt,
            &es.rtable_names,
            &plan_owned,
            ancestors,
            wagg.frameOptions,
            start_node.as_ref(),
            end_node.as_ref(),
            useprefix,
        )?;
        if needspace {
            wbuf.push(' ');
        }
        wbuf.push_str(framestr.as_str());
    }

    wbuf.push(')');
    fmt::ExplainPropertyText("Window", wbuf.as_str(), es)?;
    Ok(())
}

/// `show_window_keys(buf, planstate, nkeys, keycols, ancestors, es)`
/// (explain.c:2950). Append a window's PARTITION BY / ORDER BY key expressions
/// (deparsed against the child plan's tlist), comma-separated, to `buf`. Unlike
/// `show_sort_group_keys` no sort-order is shown (WindowAgg carries equality
/// operators, not comparison operators).
#[allow(clippy::too_many_arguments)]
fn show_window_keys<'es, 'p>(
    es: &mut ExplainState<'es>,
    mcx: Mcx<'es>,
    buf: &mut alloc::string::String,
    context_plan: &Node<'p>,
    context_plan_head: &Plan<'p>,
    nkeys: i32,
    keycols: &PgVec<'p, AttrNumber>,
    ancestors: &PgVec<'es, PgBox<'es, Node<'es>>>,
) -> PgResult<()> {
    let useprefix = es.rtable_names.len() > 1 || es.verbose;

    let plan_owned: PgBox<'es, Node<'es>> = mcx::alloc_in(mcx, context_plan.clone_in(mcx)?)?;
    let es_pstmt = es
        .pstmt
        .as_deref()
        .expect("EXPLAIN: es->pstmt must be set before deparse")
        .clone_in(mcx)?;
    let es_pstmt: PgBox<'es, PlannedStmt<'es>> = mcx::alloc_in(mcx, es_pstmt)?;

    let tlist = context_plan_head.targetlist.as_ref();

    for keyno in 0..nkeys as usize {
        // AttrNumber keyresno = keycols[keyno];
        let keyresno = keycols[keyno];
        // TargetEntry *target = get_tle_by_resno(plan->targetlist, keyresno);
        let target = tlist
            .and_then(|tl| tl.iter().find(|tle| tle.resno == keyresno))
            .unwrap_or_else(|| panic!("no tlist entry for key {keyresno}"));
        let target_expr = target
            .expr
            .as_deref()
            .expect("show_window_keys: TargetEntry has no expr");

        // exprstr = deparse_expression(target->expr, context, useprefix, true);
        let expr_node = Node::mk_expr(mcx, target_expr.clone_in(mcx)?)?;
        let exprstr = ruleutils_s::deparse_expr_for_plan::call(
            mcx,
            &es_pstmt,
            &es.rtable_names,
            &plan_owned,
            ancestors,
            &expr_node,
            useprefix,
            true,
        )?;

        if keyno > 0 {
            buf.push_str(", ");
        }
        buf.push_str(exprstr.as_str());
    }
    Ok(())
}

fn show_sort_group_keys<'es, 'p>(
    es: &mut ExplainState<'es>,
    mcx: Mcx<'es>,
    context_plan: &Node<'p>,
    context_plan_head: &Plan<'p>,
    ancestors: &PgVec<'es, PgBox<'es, Node<'es>>>,
    qlabel: &str,
    nkeys: i32,
    n_presorted_keys: i32,
    keycols: &PgVec<'p, AttrNumber>,
    sort_operators: Option<&PgVec<'p, Oid>>,
    collations: Option<&PgVec<'p, Oid>>,
    nulls_first: Option<&PgVec<'p, bool>>,
) -> PgResult<()> {
    // if (nkeys <= 0) return;
    if nkeys <= 0 {
        return Ok(());
    }

    let tlist = context_plan_head.targetlist.as_ref();

    // Set up deparsing context. useprefix = (es->rtable_size > 1 || es->verbose).
    let useprefix = es.rtable_size > 1 || es.verbose;

    // Clone the context plan into the formatting arena (matches es->pstmt's
    // 'es plan-tree copy), as the qual/Filter deparse does above.
    let plan_owned: PgBox<'es, Node<'es>> = mcx::alloc_in(mcx, context_plan.clone_in(mcx)?)?;
    let es_pstmt = es
        .pstmt
        .as_deref()
        .expect("EXPLAIN: es->pstmt must be set before deparse")
        .clone_in(mcx)?;
    let es_pstmt: PgBox<'es, PlannedStmt<'es>> = mcx::alloc_in(mcx, es_pstmt)?;

    let mut result: alloc::vec::Vec<alloc::string::String> = alloc::vec::Vec::new();
    // resultPresorted: the first nPresortedKeys keys (Incremental Sort).
    let mut result_presorted: alloc::vec::Vec<alloc::string::String> = alloc::vec::Vec::new();

    for keyno in 0..nkeys as usize {
        // AttrNumber keyresno = keycols[keyno];
        let keyresno = keycols[keyno];
        // TargetEntry *target = get_tle_by_resno(plan->targetlist, keyresno);
        let target = tlist
            .and_then(|tl| tl.iter().find(|tle| tle.resno == keyresno))
            .unwrap_or_else(|| panic!("no tlist entry for key {keyresno}"));
        let target_expr = target
            .expr
            .as_deref()
            .expect("show_sort_group_keys: TargetEntry has no expr");

        // Deparse the expression, showing any top-level cast (showImplicit=true).
        let expr_node = Node::mk_expr(mcx, target_expr.clone_in(mcx)?)?;
        let exprstr = ruleutils_s::deparse_expr_for_plan::call(
            mcx,
            &es_pstmt,
            &es.rtable_names,
            &plan_owned,
            ancestors,
            &expr_node,
            useprefix,
            true,
        )?;

        let mut sortkeybuf = alloc::string::String::from(exprstr.as_str());

        // Append sort order information, if relevant.
        if let (Some(ops), Some(colls), Some(nf)) = (sort_operators, collations, nulls_first) {
            show_sortorder_options(
                mcx,
                &mut sortkeybuf,
                target_expr,
                ops[keyno],
                colls[keyno],
                nf[keyno],
            )?;
        }

        // if (keyno < nPresortedKeys) resultPresorted = lappend(...).
        if (keyno as i32) < n_presorted_keys {
            result_presorted.push(sortkeybuf.clone());
        }
        result.push(sortkeybuf);
    }

    let view: alloc::vec::Vec<&str> = result.iter().map(|s| s.as_str()).collect();
    fmt::ExplainPropertyList(qlabel, &view, es)?;
    // if (nPresortedKeys > 0) ExplainPropertyList("Presorted Key", ...).
    if n_presorted_keys > 0 {
        let pview: alloc::vec::Vec<&str> =
            result_presorted.iter().map(|s| s.as_str()).collect();
        fmt::ExplainPropertyList("Presorted Key", &pview, es)?;
    }
    Ok(())
}

/// `BYTES_TO_KILOBYTES(b)` — `(b + 1023) / 1024` (memutils.h).
fn bytes_to_kilobytes(b: i64) -> i64 {
    (b + (1024 - 1)) / 1024
}

/// `show_memoize_info(mstate, ancestors, es)` (explain.c:3582). Deparse the
/// Memoize node's `param_exprs` as the "Cache Key" list, print the "Cache Mode"
/// (binary/logical), and — for EXPLAIN ANALYZE — the cache hit/miss/eviction
/// instrumentation (serial line plus any per-worker `shared_info` lines).
fn show_memoize_info<'es>(
    es: &mut ExplainState<'es>,
    mcx: Mcx<'es>,
    plan_node: &Node<'_>,
    ancestors: &PgVec<'es, PgBox<'es, Node<'es>>>,
    planstate: &PlanStateNode<'_>,
) -> PgResult<()> {
    let memo = plan_node.expect_memoize();

    // useprefix = es->rtable_size > 1 || es->verbose.
    let useprefix = es.rtable_size > 1 || es.verbose;

    // Set up deparsing context: context plan = the Memoize node itself
    // (set_deparse_context_plan(es->deparse_cxt, plan, ancestors)). Clone into
    // the formatting arena, matching show_sort_group_keys.
    let plan_owned: PgBox<'es, Node<'es>> = mcx::alloc_in(mcx, plan_node.clone_in(mcx)?)?;
    let es_pstmt = es
        .pstmt
        .as_deref()
        .expect("EXPLAIN: es->pstmt must be set before deparse")
        .clone_in(mcx)?;
    let es_pstmt: PgBox<'es, PlannedStmt<'es>> = mcx::alloc_in(mcx, es_pstmt)?;

    // Build the comma-separated "Cache Key" string from param_exprs.
    let mut keystr = alloc::string::String::new();
    let mut separator = "";
    for expr in memo.param_exprs.iter() {
        keystr.push_str(separator);
        let expr_node = Node::mk_expr(mcx, expr.clone_in(mcx)?)?;
        let exprstr = ruleutils_s::deparse_expr_for_plan::call(
            mcx,
            &es_pstmt,
            &es.rtable_names,
            &plan_owned,
            ancestors,
            &expr_node,
            useprefix,
            false,
        )?;
        keystr.push_str(exprstr.as_str());
        separator = ", ";
    }

    fmt::ExplainPropertyText("Cache Key", keystr.as_str(), es)?;
    fmt::ExplainPropertyText(
        "Cache Mode",
        if memo.binary_mode { "binary" } else { "logical" },
        es,
    )?;

    if !es.analyze {
        return Ok(());
    }

    // Pull the runtime MemoizeState stats. On the structural slice this is
    // available only when the executing PlanState is a Memoize node.
    let mstate = match planstate {
        PlanStateNode::Memoize(m) => m,
        // EXPLAIN without a started executor (e.g. EXPLAIN ANALYZE on a plan that
        // never ran a Memoize state) carries no stats — nothing more to print.
        _ => return Ok(()),
    };

    if mstate.stats.cache_misses > 0 {
        // mem_peak is only set when we freed memory, so use mem_used when 0.
        let mem_peak_kb = if mstate.stats.mem_peak > 0 {
            bytes_to_kilobytes(mstate.stats.mem_peak as i64)
        } else {
            bytes_to_kilobytes(mstate.mem_used as i64)
        };

        if es.format != ExplainFormat::EXPLAIN_FORMAT_TEXT {
            fmt::ExplainPropertyInteger("Cache Hits", None, mstate.stats.cache_hits as i64, es)?;
            fmt::ExplainPropertyInteger(
                "Cache Misses",
                None,
                mstate.stats.cache_misses as i64,
                es,
            )?;
            fmt::ExplainPropertyInteger(
                "Cache Evictions",
                None,
                mstate.stats.cache_evictions as i64,
                es,
            )?;
            fmt::ExplainPropertyInteger(
                "Cache Overflows",
                None,
                mstate.stats.cache_overflows as i64,
                es,
            )?;
            fmt::ExplainPropertyInteger("Peak Memory Usage", Some("kB"), mem_peak_kb, es)?;
        } else {
            fmt::ExplainIndentText(es)?;
            es.str.try_push_str(&format!(
                "Hits: {}  Misses: {}  Evictions: {}  Overflows: {}  Memory Usage: {}kB\n",
                mstate.stats.cache_hits,
                mstate.stats.cache_misses,
                mstate.stats.cache_evictions,
                mstate.stats.cache_overflows,
                mem_peak_kb,
            ))?;
        }
    }

    // Show details from parallel workers (shared_info). On the structural slice
    // this is populated only on the worker-DSM round-trip (blocked, like the
    // IncrementalSort shared_info path); mirror C's loop faithfully.
    let shared = match mstate.shared_info.as_deref() {
        Some(s) => s,
        None => return Ok(()),
    };

    for n in 0..shared.num_workers {
        let si = match shared.sinstrument.get(n as usize) {
            Some(s) => s,
            None => break,
        };

        // Skip workers that didn't do any work (a miss always precedes a hit).
        if si.cache_misses == 0 {
            continue;
        }

        // es->workers_state is unmodelled on the structural slice; ExplainOpenWorker
        // is a no-op there. mem_peak is set by ExecEndMemoize, no zero check needed.
        let mem_peak_kb = bytes_to_kilobytes(si.mem_peak as i64);

        if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
            fmt::ExplainIndentText(es)?;
            es.str.try_push_str(&format!(
                "Hits: {}  Misses: {}  Evictions: {}  Overflows: {}  Memory Usage: {}kB\n",
                si.cache_hits,
                si.cache_misses,
                si.cache_evictions,
                si.cache_overflows,
                mem_peak_kb,
            ))?;
        } else {
            fmt::ExplainPropertyInteger("Cache Hits", None, si.cache_hits as i64, es)?;
            fmt::ExplainPropertyInteger("Cache Misses", None, si.cache_misses as i64, es)?;
            fmt::ExplainPropertyInteger("Cache Evictions", None, si.cache_evictions as i64, es)?;
            fmt::ExplainPropertyInteger("Cache Overflows", None, si.cache_overflows as i64, es)?;
            fmt::ExplainPropertyInteger("Peak Memory Usage", Some("kB"), mem_peak_kb, es)?;
        }
    }

    Ok(())
}

/// `show_grouping_sets` (explain.c:2509). Render the grouping-set key lists for
/// an Agg plan that carries `groupingSets` (GROUPING SETS / ROLLUP / CUBE).
/// `context_plan`/`context_plan_head` are the *child* (outer) plan whose tlist
/// the key indexes reference — mirroring C's `planstate = outerPlanState(astate)`.
fn show_grouping_sets<'es, 'p>(
    es: &mut ExplainState<'es>,
    mcx: Mcx<'es>,
    context_plan: &Node<'p>,
    context_plan_head: &Plan<'p>,
    agg: &types_nodes::nodeagg::Agg<'p>,
    ancestors: &PgVec<'es, PgBox<'es, Node<'es>>>,
) -> PgResult<()> {
    // C sets up the deparse context once here; in the seam model the context is
    // re-derived per key inside show_grouping_set_keys (via deparse_expr_for_plan),
    // so there is nothing to precompute. useprefix is computed there as well.

    fmt::ExplainOpenGroup("Grouping Sets", Some("Grouping Sets"), false, es)?;

    // First the top Agg node's own grouping sets (no sort node).
    show_grouping_set_keys(
        es,
        mcx,
        context_plan,
        context_plan_head,
        agg,
        None,
        ancestors,
    )?;

    // Then each chained Agg/Sort node.
    if let Some(chain) = agg.chain.as_ref() {
        for aggnode in chain.iter() {
            // Sort *sortnode = (Sort *) aggnode->plan.lefttree;
            let sortnode = aggnode
                .plan
                .lefttree
                .as_deref()
                .and_then(|n| n.as_sort());
            show_grouping_set_keys(
                es,
                mcx,
                context_plan,
                context_plan_head,
                aggnode,
                sortnode,
                ancestors,
            )?;
        }
    }

    fmt::ExplainCloseGroup("Grouping Sets", Some("Grouping Sets"), false, es)?;
    Ok(())
}

/// `show_grouping_set_keys` (explain.c:2542). Render one grouping-set node's key
/// lists: optionally a "Sort Key" (when a `sortnode` is present), then the
/// "Group Keys"/"Hash Keys" group holding each grouping set's key list (deparsed
/// against the child plan's targetlist).
#[allow(clippy::too_many_arguments)]
fn show_grouping_set_keys<'es, 'p>(
    es: &mut ExplainState<'es>,
    mcx: Mcx<'es>,
    context_plan: &Node<'p>,
    context_plan_head: &Plan<'p>,
    aggnode: &types_nodes::nodeagg::Agg<'p>,
    sortnode: Option<&types_nodes::nodesort::Sort<'p>>,
    ancestors: &PgVec<'es, PgBox<'es, Node<'es>>>,
) -> PgResult<()> {
    use types_nodes::nodeagg::AggStrategy;

    let gsets = aggnode
        .grouping_sets
        .as_ref()
        .expect("show_grouping_set_keys: aggnode->groupingSets");
    // keycols may be absent (grpColIdx == NULL when numCols == 0); in that case
    // every grouping set is empty so it is never indexed.
    let keycols = aggnode.grp_col_idx.as_ref();

    let (keyname, keysetname) = match aggnode.aggstrategy {
        AggStrategy::AggHashed | AggStrategy::AggMixed => ("Hash Key", "Hash Keys"),
        _ => ("Group Key", "Group Keys"),
    };

    fmt::ExplainOpenGroup("Grouping Set", None, true, es)?;

    if let Some(sort) = sortnode {
        show_sort_group_keys(
            es,
            mcx,
            context_plan,
            context_plan_head,
            ancestors,
            "Sort Key",
            sort.numCols,
            0,
            &sort.sortColIdx,
            Some(&sort.sortOperators),
            Some(&sort.collations),
            Some(&sort.nullsFirst),
        )?;
        if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
            es.indent += 1;
        }
    }

    fmt::ExplainOpenGroup(keysetname, Some(keysetname), false, es)?;

    // Deparse context: the child plan, cloned into the 'es formatting arena
    // (matching es->pstmt), as show_sort_group_keys / show_qual do.
    let useprefix = es.rtable_size > 1 || es.verbose;
    let plan_owned: PgBox<'es, Node<'es>> = mcx::alloc_in(mcx, context_plan.clone_in(mcx)?)?;
    let es_pstmt = es
        .pstmt
        .as_deref()
        .expect("EXPLAIN: es->pstmt must be set before deparse")
        .clone_in(mcx)?;
    let es_pstmt: PgBox<'es, PlannedStmt<'es>> = mcx::alloc_in(mcx, es_pstmt)?;
    let tlist = context_plan_head.targetlist.as_ref();

    for gset in gsets.iter() {
        let mut result: alloc::vec::Vec<alloc::string::String> = alloc::vec::Vec::new();

        for &i in gset.iter() {
            // AttrNumber keyresno = keycols[i];
            let keyresno = keycols
                .expect("show_grouping_set_keys: grpColIdx indexed with NULL")
                [i as usize];
            // TargetEntry *target = get_tle_by_resno(plan->targetlist, keyresno);
            let target = tlist
                .and_then(|tl| tl.iter().find(|tle| tle.resno == keyresno))
                .unwrap_or_else(|| {
                    panic!("no tlist entry for key {keyresno}")
                });
            let target_expr = target
                .expr
                .as_deref()
                .expect("show_grouping_set_keys: TargetEntry has no expr");

            // Deparse the expression, showing any top-level cast (showImplicit=true).
            let expr_node = Node::mk_expr(mcx, target_expr.clone_in(mcx)?)?;
            let exprstr = ruleutils_s::deparse_expr_for_plan::call(
                mcx,
                &es_pstmt,
                &es.rtable_names,
                &plan_owned,
                ancestors,
                &expr_node,
                useprefix,
                true,
            )?;
            result.push(alloc::string::String::from(exprstr.as_str()));
        }

        // if (!result && es->format == TEXT) ExplainPropertyText(keyname, "()")
        // else ExplainPropertyListNested(keyname, result).
        if result.is_empty() && es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
            fmt::ExplainPropertyText(keyname, "()", es)?;
        } else {
            let view: alloc::vec::Vec<&str> = result.iter().map(|s| s.as_str()).collect();
            fmt::ExplainPropertyListNested(keyname, &view, es)?;
        }
    }

    fmt::ExplainCloseGroup(keysetname, Some(keysetname), false, es)?;

    if sortnode.is_some() && es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
        es.indent -= 1;
    }

    fmt::ExplainCloseGroup("Grouping Set", None, true, es)?;
    Ok(())
}

/// `show_sortorder_options` (explain.c:2830). Append the nondefault sort-order
/// characteristics (COLLATE / DESC / USING / NULLS FIRST|LAST) of one key to
/// `buf`.
fn show_sortorder_options<'p>(
    mcx: Mcx<'_>,
    buf: &mut alloc::string::String,
    sortexpr: &types_nodes::primnodes::Expr,
    sort_operator: types_core::primitive::Oid,
    collation: types_core::primitive::Oid,
    nulls_first: bool,
) -> PgResult<()> {
    use types_core::primitive::InvalidOid;

    // Oid sortcoltype = exprType(sortexpr);
    let sortcoltype = backend_nodes_core::nodefuncs::expr_type(Some(sortexpr))?;

    // typentry = lookup_type_cache(sortcoltype, TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);
    let (lt_opr, gt_opr) =
        ruleutils_s::lookup_type_cache_lt_gt_opr::call(sortcoltype)?;

    let mut reverse = false;

    // Print COLLATE if it's not default for the column's type.
    if collation != InvalidOid
        && collation != backend_utils_cache_lsyscache::type_::get_typcollation(sortcoltype)?
    {
        let collname = backend_utils_cache_lsyscache::collation_constraint_language_cast::get_collation_name(mcx, collation)?
            .unwrap_or_else(|| panic!("cache lookup failed for collation {collation}"));
        let quoted = backend_utils_adt_ruleutils::quote_identifier(mcx, collname.as_str())?;
        buf.push_str(" COLLATE ");
        buf.push_str(quoted.as_str());
    }

    // Print direction if not ASC, or USING if non-default sort operator.
    if sort_operator == gt_opr {
        buf.push_str(" DESC");
        reverse = true;
    } else if sort_operator != lt_opr {
        let opname = backend_utils_cache_lsyscache::opfamily_operator::get_opname(mcx, sort_operator)?
            .unwrap_or_else(|| panic!("cache lookup failed for operator {sort_operator}"));
        buf.push_str(" USING ");
        buf.push_str(opname.as_str());
        // Determine whether operator would be considered ASC or DESC.
        if let Some((_eq_op, rev)) =
            backend_utils_cache_lsyscache::opfamily_operator::get_equality_op_for_ordering_op(sort_operator)?
        {
            reverse = rev;
        }
    }

    // Add NULLS FIRST/LAST only if it wouldn't be default.
    if nulls_first && !reverse {
        buf.push_str(" NULLS FIRST");
    } else if !nulls_first && reverse {
        buf.push_str(" NULLS LAST");
    }

    Ok(())
}
