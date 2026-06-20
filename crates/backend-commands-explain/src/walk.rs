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
        }
        ntag::T_IndexOnlyScan => {
            // indexqual -> "Index Cond"; indexorderby -> "Order By";
            // plan->qual -> "Filter".
            let ios = plan_node.expect_indexonlyscan();
            show_scan_qual(es, mcx, plan_node, ancestors, ios.indexqual.as_ref(), "Index Cond")?;
            show_scan_qual(es, mcx, plan_node, ancestors, ios.indexorderby.as_ref(), "Order By")?;
            show_scan_qual(es, mcx, plan_node, ancestors, plan.qual.as_ref(), "Filter")?;
        }
        ntag::T_BitmapIndexScan => {
            // indexqualorig -> "Index Cond" (no Filter — the heap node carries it).
            let bis = plan_node.expect_bitmapindexscan();
            show_scan_qual(es, mcx, plan_node, ancestors, bis.indexqualorig.as_ref(), "Index Cond")?;
        }
        ntag::T_BitmapHeapScan => {
            // bitmapqualorig -> "Recheck Cond"; plan->qual -> "Filter".
            let bhs = plan_node.expect_bitmapheapscan();
            show_scan_qual(es, mcx, plan_node, ancestors, Some(&bhs.bitmapqualorig), "Recheck Cond")?;
            show_scan_qual(es, mcx, plan_node, ancestors, plan.qual.as_ref(), "Filter")?;
        }
        _ => {
            // The generic `Filter` leg (SeqScan / SampleScan / ValuesScan /
            // CteScan / NamedTuplestoreScan / WorkTableScan / SubqueryScan /
            // Gather / joins / Result / etc).
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
                &s.sortColIdx,
                Some(&s.sortOperators),
                Some(&s.collations),
                Some(&s.nullsFirst),
            )?;
        }
        ntag::T_IncrementalSort => {
            // show_incremental_sort_keys: same as Sort Key. The trimmed
            // IncrementalSort carries the Sort base plus nPresortedCols, but the
            // "Presorted Key" list is verbose-only detail we don't reach in the
            // structural slice; emit the full Sort Key here.
            let s = plan_node.expect_incrementalsort();
            show_sort_group_keys(
                es,
                mcx,
                plan_node,
                plan,
                ancestors,
                "Sort Key",
                s.sort.numCols,
                &s.sort.sortColIdx,
                Some(&s.sort.sortOperators),
                Some(&s.sort.collations),
                Some(&s.sort.nullsFirst),
            )?;
        }
        ntag::T_Agg => {
            // show_agg_keys: when numCols > 0 (and no grouping sets), the key
            // columns refer to the *child* plan's tlist. Group Key has no
            // sort-order arrays.
            let agg = plan_node.expect_agg();
            if agg.grouping_sets.as_ref().map(|g| !g.is_empty()).unwrap_or(false) {
                panic!(
                    "ExplainNode: show_grouping_sets (GROUPING SETS / ROLLUP / CUBE) \
                     detail unported — structural EXPLAIN only"
                );
            }
            if agg.num_cols > 0 {
                let child_plan = planstate
                    .outer_plan_state()
                    .and_then(|c| c.ps_head().plan)
                    .expect("show_agg_keys: outerPlanState(astate)->plan");
                let child = child_plan.plan_head();
                let grp = agg
                    .grp_col_idx
                    .as_ref()
                    .expect("show_agg_keys: grpColIdx with numCols>0");
                show_sort_group_keys(
                    es,
                    mcx,
                    child_plan,
                    child,
                    ancestors,
                    "Group Key",
                    agg.num_cols,
                    grp,
                    None,
                    None,
                    None,
                )?;
            }
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
                &g.grpColIdx,
                None,
                None,
                None,
            )?;
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
fn show_sort_group_keys<'es, 'p>(
    es: &mut ExplainState<'es>,
    mcx: Mcx<'es>,
    context_plan: &Node<'p>,
    context_plan_head: &Plan<'p>,
    ancestors: &PgVec<'es, PgBox<'es, Node<'es>>>,
    qlabel: &str,
    nkeys: i32,
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

        result.push(sortkeybuf);
    }

    let view: alloc::vec::Vec<&str> = result.iter().map(|s| s.as_str()).collect();
    fmt::ExplainPropertyList(qlabel, &view, es)?;
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
