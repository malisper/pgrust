use ::executils::EStateData;
use ::types_error::PgResult;
use ::types_nodes::node_tree::Node;
use ::types_nodes::NodeTag;

use crate::noderesult::ResultState;
use crate::procnode::PlanStateNode;

/// `ExecSupportsBackwardScan` (execAmi.c). Unlanded node types take C's
/// default-false arms; the true-returning unlanded ones (Append, TidScan,
/// SubqueryScan…) cannot appear in a plan today. Material/CteScan match C's
/// `true`; their runtime backward gaps are loud panics, never silent reads.
pub fn exec_supports_backward_scan(node: Option<Node<'_>>) -> bool {
    let Some(node) = node else { return false };
    let plan = node.as_plan().expect("plan-tree node has a Plan prefix");
    if plan.parallel_aware {
        return false;
    }
    match node.node_tag() {
        NodeTag::T_Result => match plan.lefttree {
            Some(outer) => exec_supports_backward_scan(Some(outer)),
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
        NodeTag::T_Limit => exec_supports_backward_scan(plan.lefttree),
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
        PlanStateNode::FunctionScan(fs) => {
            ::nodefunctionscan::exec_rescan_function_scan(fs, estate)
        }
        PlanStateNode::ValuesScan(vs) => ::nodevaluesscan::exec_rescan_values_scan(vs, estate),
        PlanStateNode::TableFuncScan(ts) => {
            ::nodetablefuncscan::exec_rescan_table_func_scan(ts, estate)
        }
        PlanStateNode::CteScan(cs) => ::nodectescan::exec_rescan_cte_scan(cs, estate),
        PlanStateNode::WorkTableScan(wts) => {
            ::nodeworktablescan::exec_rescan_work_table_scan(wts, estate);
            Ok(())
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
            exec_re_scan(&mut w.outer, estate)
        }
        PlanStateNode::Material(m) => {
            let m = &mut **m;
            if ::nodematerial::exec_rescan_material(&mut m.state, estate) {
                exec_re_scan(&mut m.outer, estate)?;
            }
            Ok(())
        }
        // ExecReScanMemoize, chgParam-NULL arm: no purge (an empty chgParam
        // has no members outside keyparamids).
        PlanStateNode::Memoize(m) => {
            let m = &mut **m;
            ::nodememoize::exec_rescan_memoize(&mut m.state);
            exec_re_scan(&mut m.outer, estate)
        }
        // ExecReScanSort: child rescanned only when the sort must be redone
        // (chgParam NULL until the Param lanes land).
        PlanStateNode::Sort(s) => {
            if ::nodesort::exec_rescan_sort(&mut s.state, estate) {
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

    let mut chg_owned: Option<types_nodes::bitmapset::Bitmapset<'mcx>> = None;
    for sp_node in base.initPlan.iter() {
        let sp = sp_node.as_sub_plan().expect("initPlan cell is a SubPlan");
        let init_plan = estate
            .es_plannedstmt
            .expect("es_plannedstmt set before rescan")
            .subplans
            .nth((sp.plan_id - 1) as usize);
        let ext = &init_plan.as_plan().expect("plan node").extParam;
        if sp.subLinkType == ::types_nodes::primnodes::SubLinkType::CTE_SUBLINK {
            assert!(
                !chg.overlap(ext),
                "ExecReScan (execAmi.c): CTE initplan under a changed-param rescan not ported"
            );
            continue;
        }
        if chg.overlap(ext) {
            let mcx = estate.es_query_cxt;
            let owned = match chg_owned.as_mut() {
                Some(o) => o,
                None => {
                    chg_owned = Some(chg.clone_in(mcx)?);
                    chg_owned.as_mut().unwrap()
                }
            };
            for pid in sp.setParam.iter() {
                estate.es_param_exec_vals[pid as usize].exec_plan = true;
                debug_assert!(estate.es_param_subplans[pid as usize].is_some());
                owned.add_member(mcx, pid)?;
            }
        }
    }
    let chg: &types_nodes::bitmapset::Bitmapset<'mcx> = chg_owned.as_ref().unwrap_or(chg);

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
        PlanStateNode::FunctionScan(fs) => {
            ::nodefunctionscan::exec_rescan_function_scan_chg(fs, estate, chg)?
        }
        PlanStateNode::ValuesScan(vs) => ::nodevaluesscan::exec_rescan_values_scan(vs, estate)?,
        // C drops the tuplestore whenever chgParam is non-NULL.
        PlanStateNode::TableFuncScan(ts) => {
            ::nodetablefuncscan::exec_rescan_table_func_scan_chg(ts, estate)?
        }
        PlanStateNode::CteScan(_) => {
            panic!("ExecReScanCteScan (nodeCtescan.c): changed-param rescan not ported")
        }
        PlanStateNode::WorkTableScan(wts) => {
            ::nodeworktablescan::exec_rescan_work_table_scan(wts, estate)
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
            // C purges when outerPlan->chgParam (= chg ∩ outer allParam) has
            // members outside the cache keys; alloc-free member walk (this
            // runs per outer tuple).
            let outer_allparam = &outer_plan.as_plan().expect("plan node").allParam;
            let keyparamids = ::nodememoize::keyparamids(&m.state);
            let mut x = chg.next_member(-1);
            while x >= 0 {
                if outer_allparam.is_member(x) && !keyparamids.is_member(x) {
                    ::nodememoize::exec_rescan_memoize_purge(&mut m.state);
                    break;
                }
                x = chg.next_member(x);
            }
            exec_re_scan_with_chg(&mut m.outer, outer_plan, estate, chg)?;
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
            ::nodesetop::exec_rescan_set_op(&mut s.state, estate);
            exec_re_scan_with_chg(&mut s.outer, base.lefttree.expect("SetOp outer plan"), estate, chg)?;
            exec_re_scan_with_chg(&mut s.inner, base.righttree.expect("SetOp inner plan"), estate, chg)?;
        }
        PlanStateNode::ModifyTable(_) => {
            panic!("ExecReScan (execAmi.c): node type 232 does not support ExecReScan")
        }
    }
    Ok(())
}

/// `ExecMarkPos` (execAmi.c): remember `node`'s current scan position. Only the
/// mark-capable ported nodes have arms; the planner routes an unmarkable merge
/// inner through a Sort/Material, so anything else is a loud panic.
pub fn exec_mark_pos<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    match node {
        PlanStateNode::Instrumented(w) => exec_mark_pos(&mut w.inner, estate),
        PlanStateNode::IndexScan(is) => ::nodeindexscan::exec_index_mark_pos(is),
        PlanStateNode::IndexOnlyScan(ios) => ::nodeindexonlyscan::exec_index_only_mark_pos(ios),
        PlanStateNode::Sort(s) => {
            ::nodesort::exec_sort_mark_pos(&mut s.state);
            Ok(())
        }
        PlanStateNode::Material(m) => {
            ::nodematerial::exec_material_mark_pos(&mut m.state);
            Ok(())
        }
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
        PlanStateNode::IndexScan(is) => ::nodeindexscan::exec_index_restr_pos(is),
        PlanStateNode::IndexOnlyScan(ios) => ::nodeindexonlyscan::exec_index_only_restr_pos(ios),
        PlanStateNode::Sort(s) => {
            ::nodesort::exec_sort_restr_pos(&mut s.state);
            Ok(())
        }
        PlanStateNode::Material(m) => {
            ::nodematerial::exec_material_restr_pos(&mut m.state);
            Ok(())
        }
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
