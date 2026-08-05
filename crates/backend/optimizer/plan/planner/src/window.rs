//! grouping_planner's window lane: optimize_window_clauses /
//! select_active_windows / make_window_input_target / create_window_paths
//! (planner.c), including the WindowFuncRunCondition -> OpExpr conversion.

use clauses::classify::WindowFuncLists;
use mcx::{Mcx, PgVec};
use types_error::PgResult;
use types_nodes::list::NodeList;
use types_nodes::parsenodes::{SortGroupClause, WindowClause};
use types_nodes::{Node, NodeTag};
use types_pathnodes::{NodeId, PathKey, PtId, RelId, UPPERREL_WINDOW};

use crate::run::PlannerRun;

fn mcx_str<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let b = mcx::slice_borrow_in(mcx, s.as_bytes())?;
    // SAFETY: bytes copied from a valid &str.
    Ok(unsafe { core::str::from_utf8_unchecked(b) })
}

// optimize_window_clauses (planner.c): prosupport frame-option rewrite +
// duplicate-WindowClause merge.
pub(crate) fn optimize_window_clauses<'mcx>(
    run: &mut PlannerRun<'mcx>,
    wflists: &mut WindowFuncLists<'mcx>,
) -> PgResult<()> {
    use types_nodes::equal::{equal_opt, NodeEqual};
    use types_nodes::primnodes::SupportRequestOptimizeWindowClause;

    let window_clause = &run.parse().windowClause;
    for wc_node in window_clause {
        let wc = wc_node.as_window_clause().expect("windowClause cell");
        let winref = wc.winref as usize;
        debug_assert!(wc.winref <= wflists.max_win_ref);
        if wflists.window_funcs[winref].is_empty() {
            continue;
        }
        let mut optimized_frame_options = 0;
        let mut all_agree = true;
        for (i, wfunc_node) in wflists.window_funcs[winref].iter().enumerate() {
            let wfunc = wfunc_node.as_window_func().expect("WindowFunc");
            let prosupport = lsyscache::get_func_support(wfunc.winfnoid)?;
            if prosupport == 0 {
                all_agree = false;
                break;
            }
            let mut req = SupportRequestOptimizeWindowClause {
                tag: NodeTag::T_SupportRequestOptimizeWindowClause,
                frame_options: wc.frameOptions,
            };
            let res = fmgr_core::oid_function_call1_coll(
                prosupport,
                0,
                datum::Datum::from_usize(&mut req as *mut _ as usize),
            )?;
            if res.as_usize() == 0 {
                all_agree = false;
                break;
            }
            if i == 0 {
                optimized_frame_options = req.frame_options;
            } else if optimized_frame_options != req.frame_options {
                all_agree = false;
                break;
            }
        }
        if !all_agree || wc.frameOptions == optimized_frame_options {
            continue;
        }
        // SAFETY: parse tree is planner-owned; no derived refs live.
        unsafe {
            wc_node
                .with_mut::<WindowClause, _>(|w| w.frameOptions = optimized_frame_options)
                .expect("WindowClause");
        }
        if window_clause.len() == 1 {
            continue;
        }
        let wc = wc_node.as_window_clause().expect("windowClause cell");
        for existing_node in window_clause {
            if existing_node.ptr_eq(wc_node) {
                continue;
            }
            let existing = existing_node.as_window_clause().expect("windowClause cell");
            if wc.partitionClause.node_equal(&existing.partitionClause)
                && wc.orderClause.node_equal(&existing.orderClause)
                && wc.frameOptions == existing.frameOptions
                && equal_opt(wc.startOffset, existing.startOffset)
                && equal_opt(wc.endOffset, existing.endOffset)
            {
                let existing_winref = existing.winref as usize;
                for wfunc_node in &wflists.window_funcs[winref] {
                    // SAFETY: parse tree is planner-owned; no derived refs live.
                    unsafe {
                        wfunc_node
                            .with_mut::<types_nodes::primnodes::WindowFunc, _>(|f| {
                                f.winref = existing.winref;
                            })
                            .expect("WindowFunc");
                    }
                }
                let moved = core::mem::replace(
                    &mut wflists.window_funcs[winref],
                    PgVec::new_in(run.mcx),
                );
                for n in moved {
                    wflists.window_funcs[existing_winref].push(n);
                }
                break;
            }
        }
    }
    Ok(())
}

pub(crate) fn select_active_windows<'mcx>(
    run: &mut PlannerRun<'mcx>,
    wflists: &WindowFuncLists<'mcx>,
) -> PgResult<PgVec<'mcx, Node<'mcx>>> {
    let mcx = run.mcx;
    struct Active<'mcx> {
        wc: Node<'mcx>,
        unique_order: PgVec<'mcx, &'mcx SortGroupClause>,
    }
    let mut actives: PgVec<'_, Active<'mcx>> = PgVec::new_in(mcx);
    for wc_node in &run.parse().windowClause {
        let wc = wc_node.as_window_clause().expect("windowClause cell");
        debug_assert!(wc.winref <= wflists.max_win_ref);
        if wflists.window_funcs[wc.winref as usize].is_empty() {
            continue;
        }
        // list_concat_unique(list_copy(partitionClause), orderClause).
        let mut unique_order: PgVec<'_, &'mcx SortGroupClause> = PgVec::new_in(mcx);
        for n in &wc.partitionClause {
            unique_order.push(n.as_sort_group_clause().expect("SortGroupClause"));
        }
        for n in &wc.orderClause {
            let sc = n.as_sort_group_clause().expect("SortGroupClause");
            if !unique_order.iter().any(|u| **u == *sc) {
                unique_order.push(sc);
            }
        }
        actives.push(Active { wc: wc_node, unique_order });
    }
    // common_prefix_cmp; stable sort where C's pg_qsort is not — equal-order
    // windows keep clause order (results identical, EXPLAIN order may differ).
    actives.sort_by(|a, b| {
        for (sca, scb) in a.unique_order.iter().zip(b.unique_order.iter()) {
            let ord = scb
                .tleSortGroupRef
                .cmp(&sca.tleSortGroupRef)
                .then(scb.sortop.cmp(&sca.sortop))
                .then(scb.nulls_first.cmp(&sca.nulls_first));
            if ord != core::cmp::Ordering::Equal {
                return ord;
            }
        }
        b.unique_order.len().cmp(&a.unique_order.len())
    });
    let mut result: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
    for a in actives.iter() {
        result.push(a.wc);
    }
    Ok(result)
}

pub(crate) fn name_active_windows(
    mcx: Mcx<'_>,
    active_windows: &[Node<'_>],
) -> PgResult<()> {
    let mut next_n = 1;
    for i in 0..active_windows.len() {
        let wc_node = active_windows[i];
        if wc_node.as_window_clause().expect("WindowClause").name.is_some() {
            continue;
        }
        let newname = loop {
            let candidate = format!("w{next_n}");
            next_n += 1;
            let taken = active_windows.iter().any(|w| {
                w.as_window_clause().expect("WindowClause").name == Some(candidate.as_str())
            });
            if !taken {
                break candidate;
            }
        };
        let name = mcx_str(mcx, &newname)?;
        // SAFETY: planner-owned query tree; no derived refs live.
        unsafe { wc_node.with_mut::<WindowClause, _>(|w| w.name = Some(name)) }
            .expect("WindowClause");
    }
    Ok(())
}

pub(crate) fn make_window_input_target<'mcx>(
    run: &mut PlannerRun<'mcx>,
    final_target: PtId,
) -> PgResult<PtId> {
    let mcx = run.mcx;
    debug_assert!(run.parse().hasWindowFuncs);
    let mut sgrefs: PgVec<'_, u32> = PgVec::new_in(mcx);
    for wc_node in run.active_windows.iter() {
        let wc = wc_node.as_window_clause().expect("WindowClause");
        for n in wc.partitionClause.iter().chain(wc.orderClause.iter()) {
            sgrefs.push(n.as_sort_group_clause().expect("SortGroupClause").tleSortGroupRef);
        }
    }
    for &id in run.root.processed_groupClause.iter() {
        let gc = *run.root.expr_node(id);
        sgrefs.push(gc.as_sort_group_clause().expect("SortGroupClause").tleSortGroupRef);
    }

    let mut tlist = NodeList::nil();
    let mut kept_exprs: PgVec<'_, Node<'mcx>> = PgVec::new_in(mcx);
    let mut vars: PgVec<'_, Node<'mcx>> = PgVec::new_in(mcx);
    let n = run.root.pathtarget(final_target).exprs.len();
    for i in 0..n {
        let ft = run.root.pathtarget(final_target);
        let expr = *run.root.expr_node(ft.exprs[i]);
        let sgref = ft.sortgrouprefs.get(i).copied().unwrap_or(0);
        if sgref != 0 && sgrefs.contains(&sgref) {
            let tle = Node::mk(
                mcx,
                types_nodes::primnodes::TargetEntry {
                    expr,
                    resno: (tlist.len() + 1) as i16,
                    resname: None,
                    ressortgroupref: sgref,
                    resorigtbl: 0,
                    resorigcol: 0,
                    resjunk: false,
                },
            )?;
            tlist.lappend(mcx, tle)?;
            kept_exprs.push(expr);
        } else {
            pull_window_input_vars(expr, &mut vars);
        }
    }

    // add_new_columns_to_pathtarget: dedupe by equal().
    let mut uniq: PgVec<'_, Node<'mcx>> = PgVec::new_in(mcx);
    for &v in vars.iter() {
        if kept_exprs.iter().chain(uniq.iter()).any(|&u| types_nodes::equal(u, v)) {
            continue;
        }
        uniq.push(v);
        let tle =
            Node::mk_target_entry(mcx, v, (tlist.len() + 1) as i16, None, false)?;
        tlist.lappend(mcx, tle)?;
    }
    crate::pathnode::create_pathtarget(run, &tlist)
}

// pull_var_clause with PVC_INCLUDE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS |
// PVC_INCLUDE_PLACEHOLDERS over the window-lane shapes.
fn pull_window_input_vars<'mcx>(node: Node<'mcx>, out: &mut PgVec<'_, Node<'mcx>>) {
    match node.node_tag() {
        NodeTag::T_Var => out.push(node),
        NodeTag::T_Aggref => out.push(node),
        // PVC_INCLUDE_PLACEHOLDERS (var.c:371): take the PHV whole, without
        // looking into the contained expression.
        NodeTag::T_PlaceHolderVar => out.push(node),
        NodeTag::T_Const => {}
        NodeTag::T_WindowFunc => {
            let wf = node.as_window_func().unwrap();
            for arg in &wf.args {
                pull_window_input_vars(arg, out);
            }
            if let Some(f) = wf.aggfilter {
                pull_window_input_vars(f, out);
            }
        }
        NodeTag::T_TargetEntry => {
            pull_window_input_vars(node.as_target_entry().unwrap().expr, out)
        }
        NodeTag::T_OpExpr => {
            for a in &node.as_op_expr().unwrap().args {
                pull_window_input_vars(a, out);
            }
        }
        NodeTag::T_FuncExpr => {
            for a in &node.as_func_expr().unwrap().args {
                pull_window_input_vars(a, out);
            }
        }
        NodeTag::T_RelabelType => {
            pull_window_input_vars(node.as_relabel_type().unwrap().arg, out)
        }
        NodeTag::T_FieldSelect => {
            pull_window_input_vars(node.as_field_select().unwrap().arg, out)
        }
        NodeTag::T_SubscriptingRef => {
            let sr = node.as_subscripting_ref().unwrap();
            for a in sr.refupperindexpr.iter().flatten() {
                pull_window_input_vars(a, out);
            }
            for a in sr.reflowerindexpr.iter().flatten() {
                pull_window_input_vars(a, out);
            }
            if let Some(a) = sr.refexpr {
                pull_window_input_vars(a, out);
            }
            if let Some(a) = sr.refassgnexpr {
                pull_window_input_vars(a, out);
            }
        }
        NodeTag::T_Param => {}
        NodeTag::T_AlternativeSubPlan => {
            for a in &node.as_alternative_sub_plan().unwrap().subplans {
                pull_window_input_vars(a, out);
            }
        }
        NodeTag::T_SubPlan => {
            let sp = node.as_sub_plan().unwrap();
            if let Some(te) = sp.testexpr {
                pull_window_input_vars(te, out);
            }
            for a in &sp.args {
                pull_window_input_vars(a, out);
            }
        }
        // PVC_INCLUDE_AGGREGATES treats GroupingFunc exactly like Aggref.
        NodeTag::T_GroupingFunc => out.push(node),
        NodeTag::T_CaseTestExpr
        | NodeTag::T_SQLValueFunction
        | NodeTag::T_NextValueExpr
        | NodeTag::T_CoerceToDomainValue => {}
        NodeTag::T_List => {
            for a in node.as_list().unwrap() {
                pull_window_input_vars(a, out);
            }
        }
        NodeTag::T_BoolExpr => {
            for a in &node.as_bool_expr().unwrap().args {
                pull_window_input_vars(a, out);
            }
        }
        NodeTag::T_NullTest => {
            if let Some(arg) = node.as_null_test().unwrap().arg {
                pull_window_input_vars(arg, out);
            }
        }
        NodeTag::T_BooleanTest => {
            if let Some(arg) = node.as_boolean_test().unwrap().arg {
                pull_window_input_vars(arg, out);
            }
        }
        NodeTag::T_DistinctExpr => {
            for a in &node.as_distinct_expr().unwrap().args {
                pull_window_input_vars(a, out);
            }
        }
        NodeTag::T_NullIfExpr => {
            for a in &node.as_null_if_expr().unwrap().args {
                pull_window_input_vars(a, out);
            }
        }
        NodeTag::T_FieldStore => {
            let fs = node.as_field_store().unwrap();
            pull_window_input_vars(fs.arg, out);
            for a in &fs.newvals {
                pull_window_input_vars(a, out);
            }
        }
        NodeTag::T_RowExpr => {
            for a in &node.as_row_expr().unwrap().args {
                pull_window_input_vars(a, out);
            }
        }
        NodeTag::T_RowCompareExpr => {
            let rc = node.as_row_compare_expr().unwrap();
            for a in rc.largs.iter().chain(rc.rargs.iter()) {
                pull_window_input_vars(a, out);
            }
        }
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            if let Some(arg) = c.arg {
                pull_window_input_vars(arg, out);
            }
            for w in &c.args {
                let cw = w.as_case_when().expect("CaseWhen");
                pull_window_input_vars(cw.expr.expect("CaseWhen.expr"), out);
                pull_window_input_vars(cw.result.expect("CaseWhen.result"), out);
            }
            if let Some(d) = c.defresult {
                pull_window_input_vars(d, out);
            }
        }
        NodeTag::T_CoalesceExpr => {
            for a in &node.as_coalesce_expr().unwrap().args {
                pull_window_input_vars(a, out);
            }
        }
        NodeTag::T_MinMaxExpr => {
            for a in &node.as_min_max_expr().unwrap().args {
                pull_window_input_vars(a, out);
            }
        }
        NodeTag::T_ArrayExpr => {
            for a in &node.as_array_expr().unwrap().elements {
                pull_window_input_vars(a, out);
            }
        }
        NodeTag::T_ScalarArrayOpExpr => {
            for a in &node.as_scalar_array_op_expr().unwrap().args {
                pull_window_input_vars(a, out);
            }
        }
        NodeTag::T_CoerceViaIO => {
            pull_window_input_vars(node.as_coerce_via_io().unwrap().arg, out)
        }
        NodeTag::T_ArrayCoerceExpr => {
            let a = node.as_array_coerce_expr().unwrap();
            pull_window_input_vars(a.arg, out);
            if let Some(e) = a.elemexpr {
                pull_window_input_vars(e, out);
            }
        }
        NodeTag::T_ConvertRowtypeExpr => {
            pull_window_input_vars(node.as_convert_rowtype_expr().unwrap().arg, out)
        }
        NodeTag::T_CoerceToDomain => {
            pull_window_input_vars(node.as_coerce_to_domain().unwrap().arg, out)
        }
        NodeTag::T_JsonValueExpr => {
            let j = node.as_json_value_expr().unwrap();
            for e in [j.raw_expr, j.formatted_expr].into_iter().flatten() {
                pull_window_input_vars(e, out);
            }
        }
        NodeTag::T_JsonConstructorExpr => {
            let c = node.as_json_constructor_expr().unwrap();
            for a in &c.args {
                pull_window_input_vars(a, out);
            }
            for e in [c.func, c.coercion].into_iter().flatten() {
                pull_window_input_vars(e, out);
            }
        }
        NodeTag::T_JsonIsPredicate => {
            if let Some(e) = node.as_json_is_predicate().unwrap().expr {
                pull_window_input_vars(e, out);
            }
        }
        NodeTag::T_JsonBehavior => {
            if let Some(e) = node.as_json_behavior().unwrap().expr {
                pull_window_input_vars(e, out);
            }
        }
        NodeTag::T_JsonExpr => {
            let j = node.as_json_expr().unwrap();
            for e in [j.formatted_expr, j.path_spec, j.on_empty, j.on_error]
                .into_iter()
                .flatten()
            {
                pull_window_input_vars(e, out);
            }
            for v in &j.passing_values {
                pull_window_input_vars(v, out);
            }
        }
        other => panic!("pull_var_clause (var.c): {other:?}; window-input lane"),
    }
}

pub(crate) fn make_pathkeys_for_window<'mcx>(
    run: &mut PlannerRun<'mcx>,
    wc_node: Node<'mcx>,
    tlist: &NodeList<'mcx>,
) -> PgResult<PgVec<'mcx, PathKey>> {
    let mcx = run.mcx;
    let wc = wc_node.as_window_clause().expect("WindowClause");
    let sortable = |clause: &NodeList<'mcx>| {
        clause
            .iter()
            .all(|n| n.as_sort_group_clause().expect("SortGroupClause").sortop != 0)
    };
    if !sortable(&wc.partitionClause) {
        return Err(err_0a000("could not implement window PARTITION BY",
            "Window partitioning columns must be of sortable datatypes."));
    }
    if !sortable(&wc.orderClause) {
        return Err(err_0a000("could not implement window ORDER BY",
            "Window ordering columns must be of sortable datatypes."));
    }

    let mut window_pathkeys: PgVec<'mcx, PathKey> = PgVec::new_in(mcx);
    if !wc.partitionClause.is_nil() {
        let mut clause_ids: PgVec<'mcx, NodeId> = PgVec::new_in(mcx);
        for n in &wc.partitionClause {
            clause_ids.push(run.intern_expr(n));
        }
        let before = clause_ids.len();
        let (pathkeys, ok) = crate::pathkeys::make_pathkeys_for_sortclauses_extended(
            run,
            &mut clause_ids,
            tlist,
            true,
            false,
            false,
        )?;
        debug_assert!(ok);
        if clause_ids.len() != before {
            // C prunes wc->partitionClause in place (redundant pathkeys).
            let mut pruned = NodeList::nil();
            for &id in clause_ids.iter() {
                pruned.lappend(mcx, *run.root.expr_node(id))?;
            }
            // SAFETY: planner-owned query tree; no derived refs live.
            unsafe { wc_node.with_mut::<WindowClause, _>(|w| w.partitionClause = pruned) }
                .expect("WindowClause");
        }
        window_pathkeys = pathkeys;
    }
    let wc = wc_node.as_window_clause().expect("WindowClause");
    if !wc.orderClause.is_nil() {
        let orderby =
            crate::pathkeys::make_pathkeys_for_sortclauses(run, &wc.orderClause, tlist)?;
        // append_pathkeys: skip entries already present (canonical identity).
        for pk in orderby.iter() {
            if !window_pathkeys.iter().any(|p| p == pk) {
                window_pathkeys.push(*pk);
            }
        }
    }
    Ok(window_pathkeys)
}

#[cold]
fn err_0a000(msg: &str, detail: &str) -> Box<types_error::PgError> {
    Box::new(
        types_error::PgError::error(msg)
            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .with_detail(detail),
    )
}

pub(crate) fn create_window_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    input_rel: RelId,
    input_target: PtId,
    output_target: PtId,
    output_target_parallel_safe: bool,
    wflists: &WindowFuncLists<'mcx>,
) -> PgResult<RelId> {
    let window_rel = crate::relnode::fetch_upper_rel(&mut run.root, UPPERREL_WINDOW);
    {
        let (serverid, userid, useridiscurrent, has_fdw, in_parallel) = {
            let input = run.root.rel(input_rel);
            (
                input.serverid,
                input.userid,
                input.useridiscurrent,
                input.fdwroutine,
                input.consider_parallel,
            )
        };
        let w = run.root.rel_mut(window_rel);
        // is_parallel_safe(activeWindows) is vacuous: frame offsets are
        // Var-free Consts of builtin types after preprocessing (parser
        // rejects variables; C divergence recorded).
        w.consider_parallel = in_parallel && output_target_parallel_safe;
        w.serverid = serverid;
        w.userid = userid;
        w.useridiscurrent = useridiscurrent;
        w.fdwroutine = has_fdw;
        w.pathtarget_id = Some(output_target);
    }

    let cheapest = run
        .root
        .rel(input_rel)
        .cheapest_total_path
        .expect("input rel has a cheapest path");
    let paths = crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(input_rel).pathlist);
    for &path_id in paths.iter() {
        let (is_sorted, presorted) = crate::pathkeys::pathkeys_count_contained_in(
            &run.root.window_pathkeys,
            &run.root.path(path_id).base().pathkeys,
        );
        if path_id == cheapest || is_sorted || presorted > 0 {
            create_one_window_path(
                run,
                window_rel,
                path_id,
                input_target,
                output_target,
                wflists,
            )?;
        }
    }
    crate::pathnode::set_cheapest(run, window_rel)?;
    Ok(window_rel)
}

fn create_one_window_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    window_rel: RelId,
    mut path_id: types_pathnodes::PathId,
    input_target: PtId,
    output_target: PtId,
    wflists: &WindowFuncLists<'mcx>,
) -> PgResult<()> {
    let mcx = run.mcx;
    let mut window_target = input_target;
    let active = crate::relnode::pgvec_clone_shallow(mcx, &run.active_windows);
    let nactive = active.len();
    let mut topqual: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
    for (i, &wc_node) in active.iter().enumerate() {
        let tlist = run.processed_tlist();
        let window_pathkeys = make_pathkeys_for_window(run, wc_node, tlist)?;
        let (is_sorted, presorted) = crate::pathkeys::pathkeys_count_contained_in(
            &window_pathkeys,
            &run.root.path(path_id).base().pathkeys,
        );
        if !is_sorted {
            let keys = crate::relnode::pgvec_clone_shallow(mcx, &window_pathkeys);
            path_id = if presorted == 0 || !crate::gucs::enable_incremental_sort() {
                crate::pathnode::create_sort_path(run, window_rel, path_id, keys, -1.0)
            } else {
                crate::pathnode::create_incremental_sort_path(
                    run, window_rel, path_id, keys, presorted, -1.0,
                )?
            };
        }

        let winref = wc_node.as_window_clause().expect("WindowClause").winref;
        let wfuncs = &wflists.window_funcs[winref as usize];
        let topwindow = i == nactive - 1;
        if !topwindow {
            // copy_pathtarget + add_column_to_pathtarget(wfunc, 0) per C; a
            // WindowFunc adds width but no eval cost at this level.
            let src = run.root.pathtarget(window_target);
            let mut t = types_pathnodes::PathTarget::new(mcx);
            let mut tuple_width = src.width as i64;
            let had_refs = !src.sortgrouprefs.is_empty();
            for &e in src.exprs.iter() {
                t.exprs.push(e);
            }
            for &r in src.sortgrouprefs.iter() {
                t.sortgrouprefs.push(r);
            }
            t.cost = src.cost;
            let mut new_ids: PgVec<'_, NodeId> = PgVec::new_in(mcx);
            for wf_node in wfuncs.iter() {
                new_ids.push(run.intern_expr(*wf_node));
            }
            for id in new_ids.iter() {
                tuple_width += crate::costsize::get_expr_width(run, *id)? as i64;
            }
            let t = {
                let mut t = t;
                for &id in new_ids.iter() {
                    t.exprs.push(id);
                    if had_refs {
                        t.sortgrouprefs.push(0);
                    }
                }
                t.width = crate::costsize::clamp_width_est(tuple_width);
                t
            };
            window_target = run.root.alloc_pathtarget(t);
        } else {
            window_target = output_target;
        }

        // WindowFuncRunConditions become OpExprs; non-top conditions also
        // feed the top window's qual. C copyObject's both operands; the
        // shared subtrees are read-only from here.
        let mut runcondition: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
        for wf_node in wfuncs.iter() {
            let wf = wf_node.as_window_func().expect("WindowFunc");
            for rc_node in &wf.runCondition {
                let rc = rc_node
                    .as_window_func_run_condition()
                    .expect("runCondition cell is a WindowFuncRunCondition");
                let (leftop, rightop) =
                    if rc.wfunc_left { (*wf_node, rc.arg) } else { (rc.arg, *wf_node) };
                let opexpr = crate::like_support::make_opclause(
                    mcx,
                    rc.opno,
                    leftop,
                    rightop,
                    rc.inputcollid,
                )?;
                runcondition.push(opexpr);
                if !topwindow {
                    topqual.push(opexpr);
                }
            }
        }

        path_id = crate::pathnode::create_windowagg_path(
            run,
            window_rel,
            path_id,
            window_target,
            wfuncs,
            wc_node,
            &runcondition,
            if topwindow { &topqual } else { &[] },
            topwindow,
        )?;
    }
    crate::pathnode::add_path(run, window_rel, path_id);
    Ok(())
}
