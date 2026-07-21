use ::executils::EStateData;
use ::types_error::PgResult;
use ::types_nodes::node_tree::Node;
use ::types_nodes::NodeTag;

use crate::noderesult::ResultState;
use crate::procnode::PlanStateNode;

/// C's `ExecSupportsBackwardScan` (execAmi.c:503+), DEMOTED to a
/// scroll-POLICY oracle (backward-execution wave B10, cursors inc-2 §6
/// rider row 3 "execami demotion + rename").
///
/// What changed: in C this predicate answers "can the EXECUTOR run this
/// plan backwards?" and gates both the planner's Material wrap (deleted,
/// B3) and the implicit-SCROLL default for cursors declared without
/// SCROLL/NO SCROLL. Our executor NEVER runs backwards (the run seam
/// refuses backward entry - deletion-prep B1; backward cursor reads are
/// served by the portal tuplestore). What remains is the POLICY use: which
/// cursors get CURSOR_OPT_SCROLL by default - a user-visible SQL contract
/// (whether FETCH BACKWARD on an undeclared cursor succeeds or raises the
/// no-scroll error) that must stay byte-identical to C. So the plan-shape
/// walk below mirrors C's answer set EXACTLY and is consulted only by the
/// two cursor-open policy probes (portalcmds PerformCursorOpen, SPI
/// SPI_cursor_open_internal), never by the executor.
pub fn plan_implicit_scroll_ok(node: Option<Node<'_>>) -> bool {
    let Some(node) = node else { return false };
    let plan = node.as_plan().expect("plan-tree node has a Plan prefix");
    if plan.parallel_aware {
        return false;
    }
    match node.node_tag() {
        NodeTag::T_Result => match plan.lefttree {
            Some(outer) => plan_implicit_scroll_ok(Some(outer)),
            None => false,
        },
        // amcanbackward: the only live index AM is btree (plancat.c port
        // loud-panics on any other relam before a plan can carry it).
        NodeTag::T_IndexScan | NodeTag::T_IndexOnlyScan => true,
        NodeTag::T_SeqScan
        | NodeTag::T_TidScan
        | NodeTag::T_TidRangeScan
        | NodeTag::T_FunctionScan
        | NodeTag::T_ValuesScan
        | NodeTag::T_CteScan
        | NodeTag::T_Material
        | NodeTag::T_Sort => true,
        NodeTag::T_Append => {
            let a = node.as_append().expect("T_Append");
            // With async, tuples may be interleaved, so can't back up.
            a.nasyncplans == 0
                && a.appendplans.iter().all(|p| plan_implicit_scroll_ok(Some(p)))
        }
        NodeTag::T_SubqueryScan => {
            plan_implicit_scroll_ok(node.as_subquery_scan().expect("T_SubqueryScan").subplan)
        }
        NodeTag::T_LockRows | NodeTag::T_Limit => plan_implicit_scroll_ok(plan.lefttree),
        _ => false,
    }
}

/// `ExecReScan` (execAmi.c). The chgParam/initPlan/subPlan propagation block
/// is dead until the Param lanes land (their construction panics loudly).
pub fn exec_re_scan<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if let Some(id) = node.ps_expr_context() {
        estate.ecxt_mut(id).rescan();
    }
    match node {
        // C ExecReScan's InstrEndLoop: close the finished cycle, then the
        // recursion runs inner's ecxt reset + node rescan.
        PlanStateNode::Instrumented(w) => {
            ::instrument::instr_end_loop(
                &mut estate.es_instrumentation[w.instr_idx as usize],
            );
            exec_re_scan(&mut w.inner, estate)
        }
        PlanStateNode::Result(rs) => exec_re_scan_result(rs, estate),
        // ExecReScanProjectSet: outer child rescanned when chgParam is NULL
        // (always, until the Param lanes land).
        PlanStateNode::ProjectSet(ps) => {
            crate::nodeprojectset::exec_re_scan_project_set_local(ps)?;
            exec_re_scan(&mut ps.outer, estate)
        }
        PlanStateNode::SeqScan(ss) => ::nodeseqscan::exec_rescan_seq_scan(ss, estate),
        PlanStateNode::SampleScan(ss) => ::nodesamplescan::exec_rescan_sample_scan(ss, estate),
        PlanStateNode::FunctionScan(fs) => {
            ::nodefunctionscan::exec_rescan_function_scan(fs, estate)
        }
        PlanStateNode::ValuesScan(vs) => ::nodevaluesscan::exec_rescan_values_scan(vs, estate),
        PlanStateNode::ForeignScan(fs) => {
            ::nodeforeignscan::exec_rescan_foreign_scan(fs, estate)
        }
        PlanStateNode::TableFuncScan(ts) => {
            ::nodetablefuncscan::exec_rescan_table_func_scan(ts, estate)
        }
        PlanStateNode::CteScan(cs) => ::nodectescan::exec_rescan_cte_scan(cs, estate),
        PlanStateNode::WorkTableScan(wts) => {
            ::nodeworktablescan::exec_rescan_work_table_scan(wts, estate);
            Ok(())
        }
        PlanStateNode::NamedTuplestoreScan(nts) => {
            ::nodenamedtuplestorescan::exec_rescan_named_tuplestore_scan(nts, estate)
        }
        // The inner term takes C's chgParam={wtParam} deferred rescan, eagerly.
        PlanStateNode::RecursiveUnion(ru) => {
            let ru = &mut **ru;
            ::noderecursiveunion::exec_rescan_recursive_union(&mut ru.state, estate);
            exec_re_scan(&mut ru.outer, estate)?;
            exec_re_scan_with_chg(&mut ru.inner, ru.state.inner_plan, estate, &ru.state.wt_chg)
        }
        PlanStateNode::IndexScan(is) => ::nodeindexscan::exec_rescan_index_scan(is, estate),
        PlanStateNode::TidScan(ts) => ::nodetidscan::exec_rescan_tid_scan(ts, estate),
        PlanStateNode::TidRangeScan(ts) => {
            ::nodetidrangescan::exec_rescan_tid_range_scan(ts, estate)
        }
        PlanStateNode::IndexOnlyScan(ios) => {
            ::nodeindexonlyscan::exec_rescan_index_only_scan(ios, estate)
        }
        // ExecReScanAgg: outer child rescanned when chgParam is NULL (always,
        // until the Param lanes land).
        PlanStateNode::Agg(aps) => {
            ::nodeagg::exec_rescan_agg(&mut aps.agg, estate);
            exec_re_scan(&mut aps.outer, estate)
        }
        // ExecReScanWindowAgg: outer child rescanned when chgParam is NULL
        // (always, until the Param lanes land).
        PlanStateNode::WindowAgg(w) => {
            ::nodewindowagg::exec_rescan_window_agg(&mut w.state, estate);
            // Lane-v2 sticky drive: forget the partition machine (the memoized
            // admission verdict stands — ownership is per-(re)scan-life).
            if let Some(d) = w.lane.as_mut() {
                ::nodewindowagg::lane::lane_window_reset(d);
            }
            // --- WS-R T2-B (wave-3): forget the framed drive's phase (the
            // node-side machine was reset by exec_rescan_window_agg above,
            // except more_partitions — lane_framed_reset clears it; the
            // memoized framed verdict stands). ---
            {
                let w = &mut **w;
                if let Some(d) = w.lane_framed.as_mut() {
                    ::nodewindowagg::lane::lane_framed_reset(&mut w.state, d);
                }
            }
            // --- end WS-R T2-B ---
            exec_re_scan(&mut w.outer, estate)
        }
        PlanStateNode::Material(m) => {
            let m = &mut **m;
            if ::nodematerial::exec_rescan_material(&mut m.state, estate)? {
                exec_re_scan(&mut m.outer, estate)?;
            }
            Ok(())
        }
        // ExecReScanMemoize: while outer_chg (C outerPlan->chgParam) is
        // pending, the child rescan stays deferred to the next pull and the
        // purge test runs on the accumulated set.
        PlanStateNode::Memoize(m) => {
            let m = &mut **m;
            ::nodememoize::exec_rescan_memoize(&mut m.state);
            if m.outer_chg.is_empty() {
                exec_re_scan(&mut m.outer, estate)?;
            } else if m
                .outer_chg
                .nonempty_difference(::nodememoize::keyparamids(&m.state))
            {
                ::nodememoize::exec_rescan_memoize_purge(&mut m.state);
            }
            Ok(())
        }
        // ExecReScanSort: child rescanned only when the sort must be redone
        // (chgParam NULL until the Param lanes land).
        PlanStateNode::Sort(s) => {
            if ::nodesort::exec_rescan_sort(&mut s.state, estate)? {
                exec_re_scan(&mut s.outer, estate)?;
            }
            Ok(())
        }
        // ExecReScanIncrementalSort: no efficient rescan (single batch in
        // memory); the outer child is always rescanned (chgParam NULL until
        // the Param lanes land).
        PlanStateNode::IncrementalSort(s) => {
            let s = &mut **s;
            ::nodeincrementalsort::exec_rescan_incremental_sort(&mut s.state, estate);
            exec_re_scan(&mut s.outer, estate)
        }
        // ExecReScanUnique: outer child rescanned when chgParam is NULL
        // (always, until the Param lanes land).
        PlanStateNode::Unique(u) => {
            ::nodeunique::exec_rescan_unique(&mut u.state, estate);
            exec_re_scan(&mut u.outer, estate)
        }
        // ExecReScanGroup: outer child rescanned when chgParam is NULL
        // (always, until the Param lanes land).
        PlanStateNode::Group(g) => {
            ::nodegroup::exec_rescan_group(&mut g.state, estate);
            exec_re_scan(&mut g.outer, estate)
        }
        PlanStateNode::Limit(l) => {
            let crate::procnode::LimitNode { state, outer } = l;
            ::nodelimit::exec_rescan_limit(state, &mut **outer, estate)?;
            exec_re_scan(outer, estate)
        }
        // ExecReScanLockRows: child rescanned when its chgParam is NULL
        // (always, until the Param lanes land).
        PlanStateNode::LockRows(l) => exec_re_scan(&mut l.outer, estate),
        // ExecReScanBitmapHeapScan: bitmapqual rescanned when chgParam is
        // NULL (always, until the Param lanes land).
        PlanStateNode::BitmapHeapScan(b) => {
            let b = &mut **b;
            ::nodebitmapheapscan::exec_rescan_bitmap_heap_scan(&mut b.scan, estate)?;
            exec_re_scan(&mut b.bitmapqual, estate)
        }
        PlanStateNode::BitmapIndexScan(biss) => {
            ::nodebitmapindexscan::exec_rescan_bitmap_index_scan(biss, estate)
        }
        PlanStateNode::BitmapAnd(bc) | PlanStateNode::BitmapOr(bc) => {
            for sub in bc.substates.iter_mut() {
                exec_re_scan(sub, estate)?;
            }
            Ok(())
        }
        // ExecReScanNestLoop: outer rescanned when its chgParam is NULL
        // (always, until the Param lanes land); the inner is NOT rescanned
        // here -- ExecNestLoop rescans it per outer tuple.
        PlanStateNode::NestLoop(nl) => {
            exec_re_scan(&mut nl.outer, estate)?;
            ::nodenestloop::exec_rescan_nest_loop(&mut nl.state);
            Ok(())
        }
        // ExecReScanHashJoin: single-batch reuse keeps the built table and
        // jumps to HJ_NEED_NEW_OUTER; a multi-batch table is destroyed and
        // the Hash sub-node's child rescanned for the rebuild. The outer
        // child is rescanned either way (chgParam NULL until the Param lanes
        // land).
        PlanStateNode::HashJoin(hj) => {
            let hj = &mut **hj;
            hj.probe_batch.reset_staged();
            // An EPQ recheck swaps the target rel's test tuple under the
            // built hash; C forces the rebuild via the epqParam landing in
            // the Hash child's chgParam (EvalPlanQualBegin) — mirror that.
            if estate.es_epq_active {
                ::nodehashjoin::exec_rescan_hash_join_chg(
                    &mut hj.state,
                    &mut hj.hash.state,
                    estate,
                )?;
                exec_re_scan(&mut hj.hash.child, estate)?;
                exec_re_scan(&mut hj.outer, estate)?;
                return Ok(());
            }
            let inner = ::nodehashjoin::exec_rescan_hash_join(
                &mut hj.state,
                &mut hj.hash.state,
                estate,
            )?;
            if inner == ::nodehashjoin::RescanInner::Rescan {
                exec_re_scan(&mut hj.hash.child, estate)?;
            }
            exec_re_scan(&mut hj.outer, estate)?;
            Ok(())
        }
        // ExecReScanMergeJoin: both children rescanned (chgParam NULL until the
        // Param lanes land); node-local half clears the marked slot + state.
        PlanStateNode::MergeJoin(mj) => {
            let mj = &mut **mj;
            // MJSORT adopted state: a rescan restarts the stream — drop
            // the adopted result and re-allow one probe (params refuse at
            // admission, so a re-probe re-derives the same content).
            mj.mjsort = None;
            mj.mjsort_probed = false;
            exec_re_scan(&mut mj.outer, estate)?;
            exec_re_scan(&mut mj.inner, estate)?;
            ::nodemergejoin::exec_rescan_merge_join(&mut mj.state, estate);
            Ok(())
        }
        // ExecReScanAppend: every subplan rescanned (chgParam always NULL).
        PlanStateNode::Append(a) => {
            let a = &mut **a;
            for sub in a.substates.iter_mut() {
                exec_re_scan(sub, estate)?;
            }
            ::nodeappend::exec_rescan_append(&mut a.state);
            Ok(())
        }
        // ExecReScanMergeAppend: every subplan rescanned (chgParam always NULL).
        PlanStateNode::MergeAppend(m) => {
            let m = &mut **m;
            for sub in m.substates.iter_mut() {
                exec_re_scan(sub, estate)?;
            }
            ::nodemergeappend::exec_rescan_merge_append(&mut m.state);
            Ok(())
        }
        // ExecReScanSubqueryScan: subplan rescanned (chgParam always NULL).
        PlanStateNode::SubqueryScan(s) => {
            let s = &mut **s;
            ::execscan::exec_scan_rescan(&mut s.ss, estate);
            exec_re_scan(&mut s.subplan, estate)
        }
        // ExecReScanSetOp: hashed re-walks the table; sorted re-reads both.
        PlanStateNode::SetOp(s) => {
            let s = &mut **s;
            if ::nodesetop::exec_rescan_set_op(&mut s.state, estate) {
                exec_re_scan(&mut s.outer, estate)?;
                exec_re_scan(&mut s.inner, estate)?;
            }
            Ok(())
        }
        PlanStateNode::Gather(g) => {
            let g = &mut **g;
            crate::nodegather::exec_rescan_gather(&mut g.state, &mut g.outer, estate)
        }
        PlanStateNode::GatherMerge(gm) => {
            let gm = &mut **gm;
            crate::nodegathermerge::exec_rescan_gather_merge(&mut gm.state, &mut gm.outer, estate)
        }
        // execAmi.c has no ModifyTable rescan arm ("node type not supported").
        PlanStateNode::ModifyTable(_) => {
            panic!("ExecReScan (execAmi.c): node type 232 does not support ExecReScan")
        }
    }
}

/// `ExecReScan` with a non-NULL chgParam (execAmi.c): the SubPlan scan lane's
/// per-call rescan. `chg` is the un-intersected changed-param set; each node
/// tests overlap against its plan's allParam (allParam sets nest, so the
/// per-edge intersection C materializes is equivalent). C defers a changed
/// child's rescan to its next ExecProcNode; the values are already bound, so
/// the eager recursion here is the same rescan one call earlier.
#[cold]
#[inline(never)]
fn rescan_mark_initplans<'mcx>(
    base: &'mcx types_nodes::plannodes::Plan<'mcx>,
    estate: &mut EStateData<'mcx>,
    chg: &types_nodes::bitmapset::Bitmapset<'mcx>,
    chg_owned: &mut Option<types_nodes::bitmapset::Bitmapset<'mcx>>,
) -> PgResult<()> {
    for sp_node in base.initPlan.iter() {
        let sp = sp_node.as_sub_plan().expect("initPlan cell is a SubPlan");
        let init_plan = estate
            .es_plannedstmt
            .expect("es_plannedstmt set before rescan")
            .subplans
            .nth((sp.plan_id - 1) as usize)
            // A shipped tree's initPlan SubPlans are parallel-safe: never a
            // NULL hole (an unsafe reference errors at ExecInitSubPlan).
            .expect("initPlan references a transferred subplan");
        let ext = &init_plan.as_plan().expect("plan node").extParam;
        // C tests against node->chgParam mid-walk, so an initplan that reads
        // an earlier sibling's output param sees that param as changed (the
        // one-pass ordering caveat in ExecReScan's comment).
        if !chg_owned.as_ref().unwrap_or(chg).overlap(ext) {
            continue;
        }
        // C: UpdateChangedParamSet(splan, chgParam) sets splan->chgParam; the
        // rescan itself is deferred to ExecSetParamPlan's (or nodeCtescan's)
        // first ExecProcNode. The param values are already bound here, so the
        // eager rescan is the same rescan one call earlier. C snapshots
        // splan->chgParam BEFORE this initplan's own setParams join
        // node->chgParam, hence rescan-then-mark order.
        {
            let cell = estate.es_subplanstates[(sp.plan_id - 1) as usize];
            // SAFETY: cell installed by InitPlan on this estate.
            let slot = unsafe { &mut *cell.0.cast::<Option<PlanStateNode<'mcx>>>().as_ptr() };
            let mut ps = slot
                .take()
                .unwrap_or_else(|| panic!("recursive initplan execution (nodeSubplan.c)"));
            let r = exec_re_scan_with_chg(
                &mut ps,
                init_plan,
                estate,
                chg_owned.as_ref().unwrap_or(chg),
            );
            *slot = Some(ps);
            r?;
        }
        let mcx = estate.es_query_cxt;
        let owned = match chg_owned.as_mut() {
            Some(o) => o,
            None => {
                *chg_owned = Some(chg.clone_in(mcx)?);
                chg_owned.as_mut().unwrap()
            }
        };
        // ExecReScanSetParamPlan: setParams join chgParam; the execPlan mark
        // skips CTE_SUBLINK (nodeCtescan runs those, not param recalc).
        let is_cte = sp.subLinkType == ::types_nodes::primnodes::SubLinkType::CTE_SUBLINK;
        for pid in sp.setParam.iter() {
            if !is_cte {
                estate.es_param_exec_vals[pid as usize].exec_plan = true;
                debug_assert!(estate.es_param_subplans[pid as usize].is_some());
            }
            owned.add_member(mcx, pid)?;
        }
    }
    Ok(())
}

pub fn exec_re_scan_with_chg<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    plan: Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    chg: &types_nodes::bitmapset::Bitmapset<'mcx>,
) -> PgResult<()> {
    let base = plan.as_plan().expect("plan-tree node");
    if !chg.overlap(&base.allParam) {
        return exec_re_scan(node, estate);
    }
    exec_re_scan_chg_forced(node, plan, estate, chg)
}

/// The chg arms without the allParam-overlap early-out. Gather's deferred
/// rescan sets the child's chgParam to {rescan_param} directly (C
/// bms_add_member, no UpdateChangedParamSet intersection), so the child's
/// node-local chg behavior must run even when rescan_param is absent from
/// its allParam.
pub(crate) fn exec_re_scan_chg_forced<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    plan: Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    chg: &types_nodes::bitmapset::Bitmapset<'mcx>,
) -> PgResult<()> {
    let base = plan.as_plan().expect("plan-tree node");

    // The initplan mark/rescan walk and the hashed-SubPlan stale sweep are
    // outlined cold: this function runs per Memoize probe (200k/q on
    // memoize_lat) and the common shape has no initplans and no SubPlans.
    let mut chg_owned: Option<types_nodes::bitmapset::Bitmapset<'mcx>> = None;
    if !base.initPlan.is_nil() {
        rescan_mark_initplans(base, estate, chg, &mut chg_owned)?;
    }
    let chg: &types_nodes::bitmapset::Bitmapset<'mcx> = chg_owned.as_ref().unwrap_or(chg);
    if !estate.es_subplan_expr_states.is_empty() {
        crate::nodesubplan::mark_hashed_subplans_stale(estate, chg)?;
    }

    if let Some(id) = node.ps_expr_context() {
        estate.ecxt_mut(id).rescan();
    }
    match node {
        PlanStateNode::Instrumented(w) => {
            ::instrument::instr_end_loop(&mut estate.es_instrumentation[w.instr_idx as usize]);
            return exec_re_scan_with_chg(&mut w.inner, plan, estate, chg);
        }
        PlanStateNode::Result(rs) => {
            rs.rs_done = false;
            rs.rs_checkqual = rs.resconstantqual.is_some();
            if let Some(outer) = rs.outer.as_deref_mut() {
                exec_re_scan_with_chg(outer, base.lefttree.expect("Result outer plan"), estate, chg)?;
            }
        }
        PlanStateNode::ProjectSet(ps) => {
            crate::nodeprojectset::exec_re_scan_project_set_local(ps)?;
            exec_re_scan_with_chg(
                &mut ps.outer,
                base.lefttree.expect("ProjectSet outer plan"),
                estate,
                chg,
            )?;
        }
        PlanStateNode::SeqScan(ss) => ::nodeseqscan::exec_rescan_seq_scan(ss, estate)?,
        PlanStateNode::SampleScan(ss) => ::nodesamplescan::exec_rescan_sample_scan(ss, estate)?,
        PlanStateNode::FunctionScan(fs) => {
            ::nodefunctionscan::exec_rescan_function_scan_chg(fs, estate, chg)?
        }
        PlanStateNode::ValuesScan(vs) => ::nodevaluesscan::exec_rescan_values_scan(vs, estate)?,
        PlanStateNode::ForeignScan(fs) => {
            ::nodeforeignscan::exec_rescan_foreign_scan(fs, estate)?
        }
        // C drops the tuplestore whenever chgParam is non-NULL.
        PlanStateNode::TableFuncScan(ts) => {
            ::nodetablefuncscan::exec_rescan_table_func_scan_chg(ts, estate)?
        }
        PlanStateNode::CteScan(cs) => {
            ::nodectescan::exec_rescan_cte_scan_chg(cs, estate, chg)?
        }
        PlanStateNode::WorkTableScan(wts) => {
            ::nodeworktablescan::exec_rescan_work_table_scan(wts, estate)
        }
        PlanStateNode::NamedTuplestoreScan(nts) => {
            ::nodenamedtuplestorescan::exec_rescan_named_tuplestore_scan(nts, estate)?;
        }
        // Inner gets chg + wtParam (C: bms_add_member onto the deferred set).
        PlanStateNode::RecursiveUnion(ru) => {
            let ru = &mut **ru;
            ::noderecursiveunion::exec_rescan_recursive_union(&mut ru.state, estate);
            exec_re_scan_with_chg(
                &mut ru.outer,
                base.lefttree.expect("RecursiveUnion outer plan"),
                estate,
                chg,
            )?;
            let mcx = estate.es_query_cxt;
            let mut inner_chg = chg.clone_in(mcx)?;
            inner_chg.add_member(mcx, ru.state.plan.wtParam)?;
            exec_re_scan_with_chg(&mut ru.inner, ru.state.inner_plan, estate, &inner_chg)?;
        }
        PlanStateNode::IndexScan(is) => ::nodeindexscan::exec_rescan_index_scan(is, estate)?,
        PlanStateNode::TidScan(ts) => ::nodetidscan::exec_rescan_tid_scan(ts, estate)?,
        PlanStateNode::TidRangeScan(ts) => {
            ::nodetidrangescan::exec_rescan_tid_range_scan(ts, estate)?
        }
        PlanStateNode::IndexOnlyScan(ios) => {
            ::nodeindexonlyscan::exec_rescan_index_only_scan(ios, estate)?
        }
        PlanStateNode::Agg(aps) => {
            ::nodeagg::exec_rescan_agg_chg(&mut aps.agg, estate);
            exec_re_scan_with_chg(&mut aps.outer, base.lefttree.expect("Agg outer plan"), estate, chg)?;
        }
        PlanStateNode::WindowAgg(w) => {
            ::nodewindowagg::exec_rescan_window_agg(&mut w.state, estate);
            // Lane-v2 sticky drive: forget the partition machine (see the
            // chgParam-free arm).
            if let Some(d) = w.lane.as_mut() {
                ::nodewindowagg::lane::lane_window_reset(d);
            }
            // --- WS-R T2-B (wave-3): forget the framed drive's phase (see
            // the chgParam-free arm; lane_framed_reset also clears the
            // node's more_partitions). ---
            {
                let w = &mut **w;
                if let Some(d) = w.lane_framed.as_mut() {
                    ::nodewindowagg::lane::lane_framed_reset(&mut w.state, d);
                }
            }
            // --- end WS-R T2-B ---
            exec_re_scan_with_chg(&mut w.outer, base.lefttree.expect("WindowAgg outer plan"), estate, chg)?;
        }
        PlanStateNode::Material(m) => {
            let m = &mut **m;
            ::nodematerial::exec_rescan_material_chg(&mut m.state, estate);
            exec_re_scan_with_chg(&mut m.outer, base.lefttree.expect("Material outer plan"), estate, chg)?;
        }
        PlanStateNode::Memoize(m) => {
            let m = &mut **m;
            ::nodememoize::exec_rescan_memoize(&mut m.state);
            let outer_plan = base.lefttree.expect("Memoize outer plan");
            // UpdateChangedParamSet: chg ∩ outer allParam accumulates into
            // outer_chg (C outerPlan->chgParam); the child rescan is DEFERRED
            // to the next pull so cache hits never walk the child subtree
            // (this arm runs per outer tuple — C ExecReScanMemoize skips
            // ExecReScan(outerPlan) whenever chgParam is pending).
            let outer_allparam = &outer_plan.as_plan().expect("plan node").allParam;
            let mcx = estate.es_query_cxt;
            let mut x = chg.next_member(-1);
            while x >= 0 {
                if outer_allparam.is_member(x) {
                    m.outer_chg.add_member(mcx, x)?;
                }
                x = chg.next_member(x);
            }
            if m.outer_chg.is_empty() {
                exec_re_scan(&mut m.outer, estate)?;
            } else if m
                .outer_chg
                .nonempty_difference(::nodememoize::keyparamids(&m.state))
            {
                ::nodememoize::exec_rescan_memoize_purge(&mut m.state);
            }
        }
        PlanStateNode::Sort(s) => {
            ::nodesort::exec_rescan_sort_chg(&mut s.state, estate);
            exec_re_scan_with_chg(&mut s.outer, base.lefttree.expect("Sort outer plan"), estate, chg)?;
        }
        PlanStateNode::IncrementalSort(s) => {
            let s = &mut **s;
            ::nodeincrementalsort::exec_rescan_incremental_sort(&mut s.state, estate);
            exec_re_scan_with_chg(
                &mut s.outer,
                base.lefttree.expect("IncrementalSort outer plan"),
                estate,
                chg,
            )?;
        }
        PlanStateNode::Unique(u) => {
            ::nodeunique::exec_rescan_unique(&mut u.state, estate);
            exec_re_scan_with_chg(&mut u.outer, base.lefttree.expect("Unique outer plan"), estate, chg)?;
        }
        PlanStateNode::Group(g) => {
            ::nodegroup::exec_rescan_group(&mut g.state, estate);
            exec_re_scan_with_chg(&mut g.outer, base.lefttree.expect("Group outer plan"), estate, chg)?;
        }
        PlanStateNode::Limit(l) => {
            let crate::procnode::LimitNode { state, outer } = l;
            ::nodelimit::exec_rescan_limit(state, &mut **outer, estate)?;
            exec_re_scan_with_chg(outer, base.lefttree.expect("Limit outer plan"), estate, chg)?;
        }
        // ExecReScanLockRows: child rescanned when its chgParam is NULL.
        PlanStateNode::LockRows(l) => {
            let l = &mut **l;
            exec_re_scan_with_chg(
                &mut l.outer,
                base.lefttree.expect("LockRows outer plan"),
                estate,
                chg,
            )?;
        }
        PlanStateNode::BitmapHeapScan(b) => {
            let b = &mut **b;
            ::nodebitmapheapscan::exec_rescan_bitmap_heap_scan(&mut b.scan, estate)?;
            exec_re_scan_with_chg(
                &mut b.bitmapqual,
                base.lefttree.expect("BitmapHeapScan bitmapqual plan"),
                estate,
                chg,
            )?;
        }
        PlanStateNode::BitmapIndexScan(biss) => {
            ::nodebitmapindexscan::exec_rescan_bitmap_index_scan(biss, estate)?
        }
        PlanStateNode::BitmapAnd(bc) => {
            let subplans = &plan.as_bitmap_and().expect("BitmapAnd plan").bitmapplans;
            for (sub, sub_plan) in bc.substates.iter_mut().zip(subplans.iter()) {
                exec_re_scan_with_chg(sub, sub_plan, estate, chg)?;
            }
        }
        PlanStateNode::BitmapOr(bc) => {
            let subplans = &plan.as_bitmap_or().expect("BitmapOr plan").bitmapplans;
            for (sub, sub_plan) in bc.substates.iter_mut().zip(subplans.iter()) {
                exec_re_scan_with_chg(sub, sub_plan, estate, chg)?;
            }
        }
        PlanStateNode::NestLoop(nl) => {
            exec_re_scan_with_chg(&mut nl.outer, base.lefttree.expect("NestLoop outer plan"), estate, chg)?;
            exec_re_scan_with_chg(&mut nl.inner, base.righttree.expect("NestLoop inner plan"), estate, chg)?;
            ::nodenestloop::exec_rescan_nest_loop(&mut nl.state);
        }
        PlanStateNode::HashJoin(hj) => {
            let hj = &mut **hj;
            hj.probe_batch.reset_staged();
            let inner_plan = base.righttree.expect("HashJoin Hash plan");
            let inner_chg = chg.overlap(&inner_plan.as_plan().expect("plan node").allParam);
            exec_re_scan_with_chg(&mut hj.outer, base.lefttree.expect("HashJoin outer plan"), estate, chg)?;
            if inner_chg {
                ::nodehashjoin::exec_rescan_hash_join_chg(&mut hj.state, &mut hj.hash.state, estate)?;
                let hash_child_plan = inner_plan
                    .as_plan()
                    .unwrap()
                    .lefttree
                    .expect("Hash child plan");
                exec_re_scan_with_chg(&mut hj.hash.child, hash_child_plan, estate, chg)?;
            } else {
                let inner = ::nodehashjoin::exec_rescan_hash_join(
                    &mut hj.state,
                    &mut hj.hash.state,
                    estate,
                )?;
                if inner == ::nodehashjoin::RescanInner::Rescan {
                    exec_re_scan(&mut hj.hash.child, estate)?;
                }
            }
        }
        PlanStateNode::MergeJoin(mj) => {
            let mj = &mut **mj;
            // MJSORT adopted state: see the exec_re_scan arm.
            mj.mjsort = None;
            mj.mjsort_probed = false;
            exec_re_scan_with_chg(&mut mj.outer, base.lefttree.expect("MergeJoin outer plan"), estate, chg)?;
            exec_re_scan_with_chg(&mut mj.inner, base.righttree.expect("MergeJoin inner plan"), estate, chg)?;
            ::nodemergejoin::exec_rescan_merge_join(&mut mj.state, estate);
        }
        PlanStateNode::Append(a) => {
            let a = &mut **a;
            let subplans = &plan.as_append().expect("Append plan").appendplans;
            for (sub, &origin) in a.substates.iter_mut().zip(a.subplan_origin.iter()) {
                exec_re_scan_with_chg(sub, subplans.nth(origin as usize), estate, chg)?;
            }
            ::nodeappend::exec_rescan_append_chg(&mut a.state, chg);
        }
        PlanStateNode::MergeAppend(m) => {
            let m = &mut **m;
            let subplans = &plan.as_merge_append().expect("MergeAppend plan").mergeplans;
            for (sub, &origin) in m.substates.iter_mut().zip(m.subplan_origin.iter()) {
                exec_re_scan_with_chg(sub, subplans.nth(origin as usize), estate, chg)?;
            }
            ::nodemergeappend::exec_rescan_merge_append_chg(&mut m.state, chg);
        }
        PlanStateNode::SubqueryScan(s) => {
            let s = &mut **s;
            ::execscan::exec_scan_rescan(&mut s.ss, estate);
            let sub_plan = plan
                .as_subquery_scan()
                .expect("SubqueryScan plan")
                .subplan
                .expect("SubqueryScan subplan");
            exec_re_scan_with_chg(&mut s.subplan, sub_plan, estate, chg)?;
        }
        // Changed params force the full SetOp rebuild (C's chgParam-nonnull arm).
        PlanStateNode::SetOp(s) => {
            let s = &mut **s;
            ::nodesetop::exec_rescan_set_op_chg(&mut s.state, estate);
            exec_re_scan_with_chg(&mut s.outer, base.lefttree.expect("SetOp outer plan"), estate, chg)?;
            exec_re_scan_with_chg(&mut s.inner, base.righttree.expect("SetOp inner plan"), estate, chg)?;
        }
        // ExecReScanGather: chg lands on the child via UpdateChangedParamSet
        // (∩ its allParam) plus rescan_param (no intersection); a non-empty
        // pending set defers the child rescan to the next leader pull, after
        // ExecParallelReinitialize has re-created the DSM.
        PlanStateNode::Gather(g) => {
            let g = &mut **g;
            crate::nodegather::exec_shutdown_gather_workers(&mut g.state)?;
            g.state.initialized = false;
            let outer_plan = base.lefttree.expect("Gather outer plan");
            accumulate_outer_chg(
                &mut g.state.outer_chg,
                outer_plan,
                estate,
                chg,
                g.state.plan.rescan_param,
            )?;
            if g.state.outer_chg.is_empty() {
                exec_re_scan_with_chg(&mut g.outer, outer_plan, estate, chg)?;
            }
        }
        PlanStateNode::GatherMerge(gm) => {
            let gm = &mut **gm;
            crate::nodegathermerge::exec_rescan_gather_merge_pre(&mut gm.state, estate)?;
            let outer_plan = base.lefttree.expect("GatherMerge outer plan");
            accumulate_outer_chg(
                &mut gm.state.outer_chg,
                outer_plan,
                estate,
                chg,
                gm.state.plan.rescan_param,
            )?;
            if gm.state.outer_chg.is_empty() {
                exec_re_scan_with_chg(&mut gm.outer, outer_plan, estate, chg)?;
            }
        }
        PlanStateNode::ModifyTable(_) => {
            panic!("ExecReScan (execAmi.c): node type 232 does not support ExecReScan")
        }
    }
    Ok(())
}

// The Gather/GatherMerge rescan chgParam build: chg ∩ child allParam
// (UpdateChangedParamSet) plus rescan_param when assigned (direct
// bms_add_member in ExecReScanGather/ExecReScanGatherMerge).
fn accumulate_outer_chg<'mcx>(
    outer_chg: &mut types_nodes::bitmapset::Bitmapset<'mcx>,
    outer_plan: Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    chg: &types_nodes::bitmapset::Bitmapset<'mcx>,
    rescan_param: i32,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let outer_allparam = &outer_plan.as_plan().expect("plan node").allParam;
    let mut x = chg.next_member(-1);
    while x >= 0 {
        if outer_allparam.is_member(x) {
            outer_chg.add_member(mcx, x)?;
        }
        x = chg.next_member(x);
    }
    if rescan_param >= 0 {
        outer_chg.add_member(mcx, rescan_param)?;
    }
    Ok(())
}

/// `ExecMarkPos` (execAmi.c): remember `node`'s current scan position. Only the
/// mark-capable ported nodes have arms; the planner routes an unmarkable merge
/// inner through a Sort/Material, so anything else is a loud panic.
// ExecIndexMarkPos/RestrPos EPQ arm: with a test tuple for the scan's rel the
// index is never touched, so mark/restore are no-ops (relsubs_done must
// already be set — no caller marks before the first fetch).
fn epq_markrestore_noop(estate: &EStateData<'_>, scanrelid: u32, what: &str) -> bool {
    if !estate.es_epq_active {
        return false;
    }
    assert!(scanrelid > 0);
    let subs = estate.es_epq.as_ref().expect("EPQ active with installed relsubs");
    let idx = (scanrelid - 1) as usize;
    if subs.relsubs_slot[idx].is_some() {
        assert!(subs.relsubs_done[idx], "unexpected {what} call in EPQ recheck");
        return true;
    }
    false
}

pub fn exec_mark_pos<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    match node {
        PlanStateNode::Instrumented(w) => exec_mark_pos(&mut w.inner, estate),
        PlanStateNode::IndexScan(is) => {
            if epq_markrestore_noop(estate, is.ss.scanrelid, "ExecIndexMarkPos") {
                return Ok(());
            }
            ::nodeindexscan::exec_index_mark_pos(is)
        }
        PlanStateNode::IndexOnlyScan(ios) => {
            if epq_markrestore_noop(estate, ios.ss.scanrelid, "ExecIndexOnlyMarkPos") {
                return Ok(());
            }
            ::nodeindexonlyscan::exec_index_only_mark_pos(ios)
        }
        PlanStateNode::Sort(s) => ::nodesort::exec_sort_mark_pos(&mut s.state),
        PlanStateNode::Material(m) => ::nodematerial::exec_material_mark_pos(&mut m.state),
        _ => panic!("ExecMarkPos (execAmi.c): node type does not support mark/restore"),
    }
}

/// `ExecRestrPos` (execAmi.c): restore `node` to its last marked position.
pub fn exec_restr_pos<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    match node {
        PlanStateNode::Instrumented(w) => exec_restr_pos(&mut w.inner, estate),
        PlanStateNode::IndexScan(is) => {
            if epq_markrestore_noop(estate, is.ss.scanrelid, "ExecIndexRestrPos") {
                return Ok(());
            }
            ::nodeindexscan::exec_index_restr_pos(is)
        }
        PlanStateNode::IndexOnlyScan(ios) => {
            if epq_markrestore_noop(estate, ios.ss.scanrelid, "ExecIndexOnlyRestrPos") {
                return Ok(());
            }
            ::nodeindexonlyscan::exec_index_only_restr_pos(ios)
        }
        PlanStateNode::Sort(s) => ::nodesort::exec_sort_restr_pos(&mut s.state),
        PlanStateNode::Material(m) => ::nodematerial::exec_material_restr_pos(&mut m.state),
        _ => panic!("ExecRestrPos (execAmi.c): node type does not support mark/restore"),
    }
}

/// `ExecReScanResult` (nodeResult.c).
pub fn exec_re_scan_result<'mcx>(
    node: &mut ResultState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    node.rs_done = false;
    node.rs_checkqual = node.resconstantqual.is_some();
    match node.outer.as_deref_mut() {
        Some(outer) => exec_re_scan(outer, estate),
        None => Ok(()),
    }
}
