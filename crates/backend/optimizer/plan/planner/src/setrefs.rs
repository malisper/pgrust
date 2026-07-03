use types_error::PgResult;
use types_nodes::list::NodeList;
use types_nodes::parsenodes::{RTEKind, RangeTblEntry};
use types_nodes::{Node, NodeTag};

use crate::run::PlannerRun;

const REGCLASSOID: u32 = 2205;
const FIRST_UNPINNED_OBJECT_ID: u32 = 12000;
// syscache.h: PROCOID. record_plan_function_dependency keys PlanInvalItems on
// it because plancache.c registers PlanCacheObjectCallback for PROCOID.
const PROCOID: i32 = 47;

// No appendrels, no AlternativeSubPlans.
pub fn set_plan_references<'mcx>(run: &mut PlannerRun<'mcx>, plan: Node<'mcx>) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let rtoffset = run.glob.finalrtable.len() as i32;
    add_rtes_to_flat_rtable(run)?;
    // Flat PlanRowMark copies, RT indexes adjusted, rowmarkId untouched.
    for i in 0..run.root.rowMarks.len() {
        let mut rc = *run.rowmark(run.root.rowMarks[i]);
        rc.rti += rtoffset as u32;
        rc.prti += rtoffset as u32;
        run.glob.finalrowmarks.lappend(mcx, Node::mk(mcx, rc)?)?;
    }
    debug_assert!(run.root.append_rel_list.is_empty());
    set_plan_refs(run, plan, rtoffset)
}

// Top-level flat copy with sub-structure zapped; alias/eref stay by ref.
fn add_rtes_to_flat_rtable(run: &mut PlannerRun<'_>) -> PgResult<()> {
    let mcx = run.mcx;
    let parse = run.parse();
    for rte_node in &parse.rtable {
        let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
        // C shares the RTEPermissionInfo node into glob->finalrteperminfos and
        // renumbers the copied RTE's index.
        let mut new_perminfoindex = 0;
        if rte.perminfoindex > 0 {
            let perminfo =
                parse_relation::getRTEPermissionInfo(&parse.rteperminfos, rte)?;
            run.glob.finalrteperminfos.lappend(mcx, perminfo)?;
            new_perminfoindex = run.glob.finalrteperminfos.len() as types_core::Index;
        }
        let newrte = Node::mk(
            mcx,
            RangeTblEntry {
                alias: rte.alias,
                eref: rte.eref,
                rtekind: rte.rtekind,
                relid: rte.relid,
                inh: rte.inh,
                relkind: rte.relkind,
                rellockmode: rte.rellockmode,
                perminfoindex: new_perminfoindex,
                tablesample: None,
                subquery: None,
                security_barrier: rte.security_barrier,
                jointype: rte.jointype,
                joinmergedcols: rte.joinmergedcols,
                joinaliasvars: NodeList::nil(),
                joinleftcols: types_nodes::list::IntList::nil(),
                joinrightcols: types_nodes::list::IntList::nil(),
                join_using_alias: None,
                functions: NodeList::nil(),
                funcordinality: rte.funcordinality,
                tablefunc: None,
                values_lists: NodeList::nil(),
                ctename: rte.ctename,
                ctelevelsup: rte.ctelevelsup,
                self_reference: rte.self_reference,
                coltypes: types_nodes::list::OidList::nil(),
                coltypmods: types_nodes::list::IntList::nil(),
                colcollations: types_nodes::list::OidList::nil(),
                enrname: rte.enrname,
                enrtuples: rte.enrtuples,
                groupexprs: NodeList::nil(),
                lateral: rte.lateral,
                inFromCl: rte.inFromCl,
                securityQuals: NodeList::nil(),
            },
        )?;
        run.glob.finalrtable.lappend(mcx, newrte)?;
        if rte.rtekind == RTEKind::RTE_RELATION
            || (rte.rtekind == RTEKind::RTE_SUBQUERY && rte.relid != 0)
        {
            run.glob.relation_oids.lappend(mcx, rte.relid)?;
            let rti = run.glob.finalrtable.len() as i32;
            run.glob.all_relids.add_member(mcx, rti)?;
        }
    }
    // C's dead-subquery pass: planned subqueries not referenced by the plan
    // tree must contribute their RTEs anyway. Live setop leaves are always
    // scanned; a subquery rel without a subroot means it was never planned.
    for (i, rte_node) in parse.rtable.iter().enumerate() {
        let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
        let rti = (i + 1) as i32;
        if rte.rtekind == RTEKind::RTE_SUBQUERY
            && !rte.inh
            && rti < run.root.simple_rel_array_size
        {
            if let Some(rel) = run.root.simple_rel_array[rti as usize] {
                assert!(
                    run.root.rel(rel).subroot_idx.is_some(),
                    "flatten_unplanned_rtes (setrefs.c): unplanned subquery RTE; \
                     constraint-exclusion lane unported"
                );
                // IS_DUMMY_REL recursion arm: dummy children still surface as
                // SubqueryScan(Result) plans here, so their rtables flatten
                // through the plan walk.
            }
        }
    }
    Ok(())
}

fn set_plan_refs<'mcx>(run: &mut PlannerRun<'mcx>, plan: Node<'mcx>, rtoffset: i32) -> PgResult<Node<'mcx>> {
    let plan_node_id = run.glob.last_plan_node_id;
    run.glob.last_plan_node_id += 1;
    // SAFETY: the plan tree was just built by createplan and is exclusively
    // ours until returned (C mutates it in place the same way).
    unsafe { plan.with_plan_mut(|p| p.plan_node_id = plan_node_id) }.expect("plan node");

    match plan.node_tag() {
        NodeTag::T_Result => {
            let r = plan.as_result().unwrap();
            if r.plan.lefttree.is_some() {
                set_upper_references(run, plan, rtoffset)?;
            } else {
                debug_assert!(r.plan.qual.is_nil());
                if let Some(tl) = fix_scan_list(run, &r.plan.targetlist, rtoffset, r.plan.plan_rows)? {
                    // SAFETY: exclusive plan-tree ownership (prologue note).
                    unsafe { plan.with_plan_mut(|p| p.targetlist = tl) }.expect("plan node");
                }
            }
            if let Some(rcq) = plan.as_result().unwrap().resconstantqual {
                let list = rcq.as_list().expect("resconstantqual is a List");
                if let Some(fixed) = fix_scan_list(run, list, rtoffset, 1.0)? {
                    let fixed = Node::mk_list(run.mcx, fixed)?;
                    // SAFETY: exclusive plan-tree ownership (prologue note).
                    unsafe {
                        plan.with_mut::<types_nodes::plannodes::Result, _>(|r| {
                            r.resconstantqual = Some(fixed)
                        })
                    }
                    .expect("Result node");
                }
            }
        }
        NodeTag::T_SeqScan => {
            let s = plan.as_seq_scan().unwrap();
            debug_assert!(s.scan.scanrelid as i32 + rtoffset > 0);
            let tl = fix_scan_list(run, &s.scan.plan.targetlist, rtoffset, s.scan.plan.plan_rows)?;
            let qual = fix_scan_list(run, &s.scan.plan.qual, rtoffset, 2.0 * s.scan.plan.plan_rows)?;
            if rtoffset != 0 || tl.is_some() || qual.is_some() {
                // SAFETY: exclusive plan-tree ownership (prologue note).
                unsafe {
                    plan.with_mut::<types_nodes::plannodes::SeqScan, _>(|s| {
                        if let Some(tl) = tl {
                            s.scan.plan.targetlist = tl;
                        }
                        if let Some(q) = qual {
                            s.scan.plan.qual = q;
                        }
                        s.scan.scanrelid += rtoffset as u32;
                    })
                }
                .expect("SeqScan node");
            }
        }
        NodeTag::T_IndexScan => {
            let s = plan.as_index_scan().unwrap();
            debug_assert!(s.scan.scanrelid as i32 + rtoffset > 0);
            let nr = s.scan.plan.plan_rows;
            let tl = fix_scan_list(run, &s.scan.plan.targetlist, rtoffset, nr)?;
            let qual = fix_scan_list(run, &s.scan.plan.qual, rtoffset, 2.0 * nr)?;
            let iq = fix_scan_list(run, &s.indexqual, rtoffset, 2.0 * nr)?;
            let iqo = fix_scan_list(run, &s.indexqualorig, rtoffset, 2.0 * nr)?;
            let iob = fix_scan_list(run, &s.indexorderby, rtoffset, 2.0 * nr)?;
            let iobo = fix_scan_list(run, &s.indexorderbyorig, rtoffset, 2.0 * nr)?;
            if rtoffset != 0 {
                // SAFETY: exclusive plan-tree ownership (prologue note).
                unsafe {
                    plan.with_mut::<types_nodes::plannodes::IndexScan, _>(|p| {
                        p.scan.scanrelid += rtoffset as u32;
                        if let Some(v) = tl { p.scan.plan.targetlist = v; }
                        if let Some(v) = qual { p.scan.plan.qual = v; }
                        if let Some(v) = iq { p.indexqual = v; }
                        if let Some(v) = iqo { p.indexqualorig = v; }
                        if let Some(v) = iob { p.indexorderby = v; }
                        if let Some(v) = iobo { p.indexorderbyorig = v; }
                    })
                }
                .expect("IndexScan node");
            }
        }
        NodeTag::T_IndexOnlyScan => set_indexonlyscan_references(run, plan, rtoffset)?,
        NodeTag::T_BitmapIndexScan => {
            let s = plan.as_bitmap_index_scan().unwrap();
            debug_assert!(s.scan.scanrelid as i32 + rtoffset > 0);
            assert_eq!(rtoffset, 0, "set_plan_refs (setrefs.c): bitmap scan in a subplan; M2 lane");
            debug_assert!(s.scan.plan.targetlist.is_nil() && s.scan.plan.qual.is_nil());
            fix_scan_list(run, &s.indexqual, rtoffset, 2.0 * s.scan.plan.plan_rows)?;
            fix_scan_list(run, &s.indexqualorig, rtoffset, 2.0 * s.scan.plan.plan_rows)?;
        }
        NodeTag::T_BitmapHeapScan => {
            let s = plan.as_bitmap_heap_scan().unwrap();
            debug_assert!(s.scan.scanrelid as i32 + rtoffset > 0);
            assert_eq!(rtoffset, 0, "set_plan_refs (setrefs.c): bitmap scan in a subplan; M2 lane");
            fix_scan_list(run, &s.scan.plan.targetlist, rtoffset, s.scan.plan.plan_rows)?;
            fix_scan_list(run, &s.scan.plan.qual, rtoffset, 2.0 * s.scan.plan.plan_rows)?;
            fix_scan_list(run, &s.bitmapqualorig, rtoffset, 2.0 * s.scan.plan.plan_rows)?;
        }
        NodeTag::T_FunctionScan => {
            let s = plan.as_function_scan().unwrap();
            debug_assert!(s.scan.scanrelid as i32 + rtoffset > 0);
            fix_scan_list(run, &s.scan.plan.targetlist, rtoffset, s.scan.plan.plan_rows)?;
            fix_scan_list(run, &s.scan.plan.qual, rtoffset, 2.0 * s.scan.plan.plan_rows)?;
            for rtfunc_node in &s.functions {
                let rtfunc = rtfunc_node.as_range_tbl_function().expect("functions cell");
                if let Some(fexpr) = rtfunc.funcexpr {
                    fix_scan_expr_walker(run, fexpr)?;
                }
            }
        }
        NodeTag::T_ValuesScan => {
            let s = plan.as_values_scan().unwrap();
            debug_assert!(s.scan.scanrelid as i32 + rtoffset > 0);
            let tl = fix_scan_list(run, &s.scan.plan.targetlist, rtoffset, s.scan.plan.plan_rows)?;
            let qual = fix_scan_list(run, &s.scan.plan.qual, rtoffset, 2.0 * s.scan.plan.plan_rows)?;
            let vls = fix_scan_list(run, &s.values_lists, rtoffset, 2.0 * s.scan.plan.plan_rows)?;
            if rtoffset != 0 || tl.is_some() || qual.is_some() || vls.is_some() {
                // SAFETY: exclusive plan-tree ownership (prologue note).
                unsafe {
                    plan.with_mut::<types_nodes::plannodes::ValuesScan, _>(|s| {
                        if let Some(tl) = tl {
                            s.scan.plan.targetlist = tl;
                        }
                        if let Some(q) = qual {
                            s.scan.plan.qual = q;
                        }
                        if let Some(v) = vls {
                            s.values_lists = v;
                        }
                        s.scan.scanrelid += rtoffset as u32;
                    })
                }
                .expect("ValuesScan node");
            }
        }
        NodeTag::T_CteScan => {
            let s = plan.as_cte_scan().unwrap();
            debug_assert!(s.scan.scanrelid as i32 + rtoffset > 0);
            let tl = fix_scan_list(run, &s.scan.plan.targetlist, rtoffset, s.scan.plan.plan_rows)?;
            let qual = fix_scan_list(run, &s.scan.plan.qual, rtoffset, 2.0 * s.scan.plan.plan_rows)?;
            if rtoffset != 0 || tl.is_some() || qual.is_some() {
                // SAFETY: exclusive plan-tree ownership (prologue note).
                unsafe {
                    plan.with_mut::<types_nodes::plannodes::CteScan, _>(|s| {
                        if let Some(tl) = tl {
                            s.scan.plan.targetlist = tl;
                        }
                        if let Some(q) = qual {
                            s.scan.plan.qual = q;
                        }
                        s.scan.scanrelid += rtoffset as u32;
                    })
                }
                .expect("CteScan node");
            }
        }
        NodeTag::T_Agg => {
            // C's set_plan_refs never walks agg->chain: the chain Aggs
            // carry NIL tlists/quals and stripped Sorts; nothing to fix.
            let a = plan.as_agg().unwrap();
            debug_assert!(a.chain.is_nil() || !a.groupingSets.is_nil());
            for chain_node in &a.chain {
                let c = chain_node.as_agg().expect("chain cell is an Agg");
                debug_assert!(c.plan.targetlist.is_nil() && c.plan.qual.is_nil());
            }
            set_upper_references(run, plan, rtoffset)?;
        }
        NodeTag::T_WindowAgg => {
            let w = plan.as_window_agg().unwrap();
            debug_assert!(w.runCondition.is_nil() && w.runConditionOrig.is_nil());
            if let Some(off) = w.startOffset {
                fix_frame_offset(run, off, rtoffset)?;
            }
            if let Some(off) = w.endOffset {
                fix_frame_offset(run, off, rtoffset)?;
            }
            set_upper_references(run, plan, rtoffset)?;
        }
        NodeTag::T_Sort | NodeTag::T_IncrementalSort | NodeTag::T_Unique
        | NodeTag::T_Material => {
            // Neither evaluates its tlist; fixed up for EXPLAIN only.
            set_dummy_tlist_references(run, plan, rtoffset)?;
            debug_assert!(plan.as_plan().unwrap().qual.is_nil());
        }
        NodeTag::T_LockRows => {
            let l = plan.as_lock_rows().unwrap();
            set_dummy_tlist_references(run, plan, rtoffset)?;
            debug_assert!(l.plan.qual.is_nil());
            if rtoffset != 0 {
                for rc_node in &plan.as_lock_rows().unwrap().rowMarks {
                    // SAFETY: exclusive plan-tree ownership (prologue note).
                    unsafe {
                        rc_node.with_mut::<types_nodes::plannodes::PlanRowMark, _>(|rc| {
                            rc.rti += rtoffset as u32;
                            rc.prti += rtoffset as u32;
                        })
                    }
                    .expect("PlanRowMark");
                }
            }
        }
        NodeTag::T_Limit => {
            let l = plan.as_limit().unwrap();
            set_dummy_tlist_references(run, plan, rtoffset)?;
            debug_assert!(l.plan.qual.is_nil());
            if let Some(off) = l.limitOffset {
                fix_scan_expr_walker(run, off)?;
            }
            if let Some(cnt) = l.limitCount {
                fix_scan_expr_walker(run, cnt)?;
            }
            debug_assert!(l.uniqNumCols == 0);
        }
        NodeTag::T_NestLoop => {
            let nl = plan.as_nest_loop().unwrap();
            debug_assert!(nl.nestParams.is_nil());
            set_join_references(run, plan, rtoffset)?;
        }
        NodeTag::T_MergeJoin | NodeTag::T_HashJoin => {
            set_join_references(run, plan, rtoffset)?;
        }
        NodeTag::T_Hash => {
            set_hash_references(run, plan, rtoffset)?;
        }
        NodeTag::T_ModifyTable => {
            let m = plan.as_modify_table().unwrap();
            debug_assert!(m.plan.targetlist.is_nil() && m.plan.qual.is_nil());
            debug_assert!(
                m.withCheckOptionLists.is_nil()
                    && m.onConflictSet.is_nil()
                    && m.mergeActionLists.is_nil()
            );
            debug_assert!(m.rootRelation == 0 && m.rowMarks.is_nil());
            assert_eq!(rtoffset, 0, "set_plan_refs (setrefs.c): ModifyTable rtoffset leg; M4 lane");
            // set_returning_clause_references: the other-relations index over
            // the subplan tlist is empty on this lane (join DML loud upstream;
            // preprocess_targetlist checked every RETURNING Var references the
            // result relation) and rtoffset is 0, so C's fix_join_expr reduces
            // to the fix_expr_common walk over unchanged Vars.
            let has_returning = !m.returningLists.is_nil();
            if has_returning {
                debug_assert_eq!(m.returningLists.len(), m.resultRelations.len());
                for rlist_node in &m.returningLists {
                    let rlist = rlist_node.as_list().expect("returningLists cell is a List");
                    let fixed = fix_scan_list(run, rlist, rtoffset, m.plan.plan_rows)?;
                    debug_assert!(fixed.is_none());
                }
            }
            for rti in m.resultRelations.iter() {
                run.glob.result_relations.lappend(run.mcx, rti)?;
            }
            if has_returning {
                // C copyObject's the first RETURNING list into the visible
                // tlist (EXPLAIN + the node's result slot descriptor); the
                // cells are shared here — the executor never mutates them.
                let first = m
                    .returningLists
                    .nth(0)
                    .as_list()
                    .expect("returningLists cell is a List")
                    .clone_in(run.mcx)?;
                // SAFETY: exclusive plan-tree ownership (prologue note).
                unsafe { plan.with_plan_mut(|p| p.targetlist = first) }.expect("plan node");
            }
        }
        NodeTag::T_Append => {
            return set_append_references(run, plan, rtoffset);
        }
        NodeTag::T_SubqueryScan => {
            return set_subqueryscan_references(run, plan, rtoffset);
        }
        NodeTag::T_SetOp => {
            // SetOp returns its input tuples unmodified; dummy tlist for EXPLAIN.
            set_dummy_tlist_references(run, plan, rtoffset)?;
            debug_assert!(plan.as_plan().unwrap().qual.is_nil());
        }
        other => panic!("set_plan_refs (setrefs.c): {other:?}; M2 plan lane"),
    }

    let base = plan.as_plan().expect("plan node");
    if let Some(child) = base.lefttree {
        let new_child = set_plan_refs(run, child, rtoffset)?;
        // SAFETY: same exclusive plan-tree ownership as the prologue above.
        unsafe { plan.with_plan_mut(|p| p.lefttree = Some(new_child)) }.expect("plan node");
    }
    let base = plan.as_plan().expect("plan node");
    if let Some(child) = base.righttree {
        debug_assert!(matches!(
            plan.node_tag(),
            NodeTag::T_NestLoop | NodeTag::T_MergeJoin | NodeTag::T_HashJoin | NodeTag::T_SetOp
        ));
        let new_child = set_plan_refs(run, child, rtoffset)?;
        // SAFETY: same exclusive plan-tree ownership as the prologue above.
        unsafe { plan.with_plan_mut(|p| p.righttree = Some(new_child)) }.expect("plan node");
    }
    Ok(plan)
}

// set_upper_references (setrefs.c): retarget an upper node's tlist at its
// subplan's output; the sortgroupref fast path is the M3 grouping lane.
fn set_upper_references<'mcx>(
    run: &mut PlannerRun<'mcx>,
    plan: Node<'mcx>,
    rtoffset: i32,
) -> PgResult<()> {
    let mcx = run.mcx;
    let base = plan.as_plan().expect("plan node");
    let subplan = base.lefttree.expect("upper node has a subplan");
    let subplan_tlist = &subplan.as_plan().expect("plan node").targetlist;

    let mut output_targetlist = NodeList::nil();
    for tle_node in &base.targetlist {
        let tle = tle_node.as_target_entry().expect("TargetEntry");
        let mut newexpr = if tle.ressortgroupref != 0 {
            search_indexed_tlist_for_sortgroupref(run, tle.expr, tle.ressortgroupref, subplan_tlist)?
        } else {
            None
        };
        if newexpr.is_none() {
            newexpr = Some(fix_upper_expr(run, tle.expr, subplan_tlist, rtoffset, types_nodes::primnodes::OUTER_VAR, base.plan_rows)?);
        }
        let newexpr = newexpr.unwrap();
        // flatCopyTargetEntry + new expr.
        let new_tle = Node::mk(
            mcx,
            types_nodes::primnodes::TargetEntry {
                expr: newexpr,
                resno: tle.resno,
                resname: tle.resname,
                ressortgroupref: tle.ressortgroupref,
                resorigtbl: tle.resorigtbl,
                resorigcol: tle.resorigcol,
                resjunk: tle.resjunk,
            },
        )?;
        output_targetlist.lappend(mcx, new_tle)?;
    }
    let mut output_qual = NodeList::nil();
    for qual_node in &base.qual {
        output_qual.lappend(mcx, fix_upper_expr(run, qual_node, subplan_tlist, rtoffset, types_nodes::primnodes::OUTER_VAR, 2.0 * base.plan_rows)?)?;
    }
    // SAFETY: exclusive plan-tree ownership (C rewrites the same lists in place).
    unsafe {
        plan.with_plan_mut(|p| {
            p.targetlist = output_targetlist;
            p.qual = output_qual;
        })
    }
    .expect("plan node");
    Ok(())
}

// set_indexonlyscan_references (setrefs.c): heap Vars in tlist/qual/
// recheckqual retarget to INDEX_VAR positions through the stripped indextlist.
fn set_indexonlyscan_references<'mcx>(
    run: &mut PlannerRun<'mcx>,
    plan: Node<'mcx>,
    rtoffset: i32,
) -> PgResult<()> {
    let mcx = run.mcx;
    let (stripped, tlist, qual, recheckqual) = {
        let s = plan.as_index_only_scan().expect("IndexOnlyScan node");
        debug_assert!(s.scan.scanrelid as i32 + rtoffset > 0);
        debug_assert!(s.indexorderby.is_nil());
        let mut stripped = NodeList::nil();
        for tle_node in &s.indextlist {
            if !tle_node.as_target_entry().expect("TargetEntry").resjunk {
                stripped.lappend(mcx, tle_node)?;
            }
        }
        (
            stripped,
            s.scan.plan.targetlist.clone_in(mcx)?,
            s.scan.plan.qual.clone_in(mcx)?,
            s.recheckqual.clone_in(mcx)?,
        )
    };
    const INDEX_VAR: i32 = types_nodes::primnodes::INDEX_VAR;
    let plan_rows = plan.as_plan().expect("plan node").plan_rows;
    let mut new_tlist = NodeList::nil();
    for tle_node in &tlist {
        let tle = tle_node.as_target_entry().expect("TargetEntry");
        let newexpr = fix_upper_expr(run, tle.expr, &stripped, rtoffset, INDEX_VAR, plan_rows)?;
        new_tlist.lappend(
            mcx,
            Node::mk(
                mcx,
                types_nodes::primnodes::TargetEntry {
                    expr: newexpr,
                    resno: tle.resno,
                    resname: tle.resname,
                    ressortgroupref: tle.ressortgroupref,
                    resorigtbl: tle.resorigtbl,
                    resorigcol: tle.resorigcol,
                    resjunk: tle.resjunk,
                },
            )?,
        )?;
    }
    let mut new_qual = NodeList::nil();
    for qual_node in &qual {
        new_qual.lappend(mcx, fix_upper_expr(run, qual_node, &stripped, rtoffset, INDEX_VAR, 2.0 * plan_rows)?)?;
    }
    let mut new_recheck = NodeList::nil();
    for qual_node in &recheckqual {
        new_recheck.lappend(mcx, fix_upper_expr(run, qual_node, &stripped, rtoffset, INDEX_VAR, 2.0 * plan_rows)?)?;
    }
    let (iq, itl) = {
        let s = plan.as_index_only_scan().unwrap();
        (
            fix_scan_list(run, &s.indexqual, rtoffset, 2.0 * s.scan.plan.plan_rows)?,
            fix_scan_list(run, &s.indextlist, rtoffset, 2.0 * s.scan.plan.plan_rows)?,
        )
    };
    // SAFETY: exclusive plan-tree ownership (prologue note).
    unsafe {
        plan.with_mut::<types_nodes::plannodes::IndexOnlyScan, _>(|s| {
            s.scan.scanrelid += rtoffset as u32;
            s.scan.plan.targetlist = new_tlist;
            s.scan.plan.qual = new_qual;
            s.recheckqual = new_recheck;
            if let Some(v) = iq {
                s.indexqual = v;
            }
            if let Some(v) = itl {
                s.indextlist = v;
            }
        })
    }
    .expect("IndexOnlyScan node");
    Ok(())
}

// search_indexed_tlist_for_sortgroupref (setrefs.c).
fn search_indexed_tlist_for_sortgroupref<'mcx>(
    run: &mut PlannerRun<'mcx>,
    node: Node<'mcx>,
    sortgroupref: u32,
    subplan_tlist: &NodeList<'mcx>,
) -> PgResult<Option<Node<'mcx>>> {
    for tle_node in subplan_tlist {
        let tle = tle_node.as_target_entry().expect("TargetEntry");
        if tle.ressortgroupref != sortgroupref || !types_nodes::equal(tle.expr, node) {
            continue;
        }
        let (vartype, vartypmod) = crate::costsize::expr_type_typmod(node);
        let newvar = types_nodes::primnodes::Var {
            varno: types_nodes::primnodes::OUTER_VAR,
            varattno: tle.resno,
            vartype,
            vartypmod,
            varcollid: exprs_collation(node),
            varnullingrels: types_nodes::bitmapset::Bitmapset::empty(),
            varlevelsup: 0,
            varreturningtype: types_nodes::primnodes::VarReturningType::VAR_RETURNING_DEFAULT,
            varnosyn: 0,
            varattnosyn: 0,
            location: expr_location(node),
        };
        return Ok(Some(Node::mk(run.mcx, newvar)?));
    }
    Ok(None)
}

fn exprs_collation(node: Node<'_>) -> u32 {
    match node.node_tag() {
        NodeTag::T_Var => node.as_var().unwrap().varcollid,
        NodeTag::T_Const => node.as_const().unwrap().constcollid,
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().funccollid,
        NodeTag::T_OpExpr => node.as_op_expr().unwrap().opcollid,
        NodeTag::T_WindowFunc => node.as_window_func().unwrap().wincollid,
        NodeTag::T_CaseExpr => node.as_case_expr().unwrap().casecollid,
        tag => panic!("exprCollation (nodeFuncs.c): {tag:?} not ported here"),
    }
}

fn expr_location(node: Node<'_>) -> i32 {
    match node.node_tag() {
        NodeTag::T_Var => node.as_var().unwrap().location,
        NodeTag::T_Const => node.as_const().unwrap().location,
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().location,
        NodeTag::T_OpExpr => node.as_op_expr().unwrap().location,
        _ => -1,
    }
}

// fix_upper_expr_mutator (setrefs.c) over the plain-agg tlist shapes.
fn fix_upper_expr<'mcx>(
    run: &mut PlannerRun<'mcx>,
    node: Node<'mcx>,
    subplan_tlist: &NodeList<'mcx>,
    rtoffset: i32,
    newvarno: i32,
    num_exec: f64,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    // search_indexed_tlist_for_non_var: an upper node consuming a value the
    // subplan already computed (Aggref/WindowFunc in a lower tlist) reads it
    // as an OUTER Var instead of re-evaluating.
    if node.node_tag() != NodeTag::T_Var {
        for tle_node in subplan_tlist {
            let tle = tle_node.as_target_entry().expect("TargetEntry");
            if types_nodes::equal(tle.expr, node) {
                let (vartype, vartypmod) = crate::costsize::expr_type_typmod(node);
                return Node::mk(
                    mcx,
                    types_nodes::primnodes::Var {
                        varno: newvarno,
                        varattno: tle.resno,
                        vartype,
                        vartypmod,
                        varcollid: exprs_collation(node),
                        varnullingrels: types_nodes::bitmapset::Bitmapset::empty(),
                        varlevelsup: 0,
                        varreturningtype:
                            types_nodes::primnodes::VarReturningType::VAR_RETURNING_DEFAULT,
                        varnosyn: 0,
                        varattnosyn: 0,
                        location: expr_location(node),
                    },
                );
            }
        }
    }
    match node.node_tag() {
        NodeTag::T_Var => {
            let var = node.as_var().expect("Var");
            search_indexed_tlist_for_var(run, var, subplan_tlist, rtoffset, newvarno)
        }
        NodeTag::T_Const | NodeTag::T_Param | NodeTag::T_SQLValueFunction => {
            fix_scan_expr_walker(run, node)?;
            Ok(node)
        }
        NodeTag::T_Aggref => {
            if let Some(prm) = find_minmax_agg_replacement_param(run, node) {
                return Ok(*run.root.expr_node(prm));
            }
            let a = node.as_aggref().expect("Aggref");
            record_plan_function_dependency(run, a.aggfnoid)?;
            debug_assert!(a.aggdirectargs.is_nil() && a.aggfilter.is_none());
            debug_assert!(a.aggorder.is_nil() && a.aggdistinct.is_nil());
            let mut args = NodeList::nil();
            for arg_node in &a.args {
                let arg = arg_node.as_target_entry().expect("agg arg is a TLE");
                let newexpr = fix_upper_expr(run, arg.expr, subplan_tlist, rtoffset, newvarno, num_exec)?;
                let new_tle = Node::mk(
                    mcx,
                    types_nodes::primnodes::TargetEntry {
                        expr: newexpr,
                        resno: arg.resno,
                        resname: arg.resname,
                        ressortgroupref: arg.ressortgroupref,
                        resorigtbl: arg.resorigtbl,
                        resorigcol: arg.resorigcol,
                        resjunk: arg.resjunk,
                    },
                )?;
                args.lappend(mcx, new_tle)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::Aggref {
                    aggfnoid: a.aggfnoid,
                    aggtype: a.aggtype,
                    aggcollid: a.aggcollid,
                    inputcollid: a.inputcollid,
                    aggtranstype: a.aggtranstype,
                    aggargtypes: a.aggargtypes.clone_in(mcx)?,
                    aggdirectargs: NodeList::nil(),
                    args,
                    aggorder: NodeList::nil(),
                    aggdistinct: NodeList::nil(),
                    aggfilter: None,
                    aggstar: a.aggstar,
                    aggvariadic: a.aggvariadic,
                    aggkind: a.aggkind,
                    aggpresorted: a.aggpresorted,
                    agglevelsup: a.agglevelsup,
                    aggsplit: a.aggsplit,
                    aggno: a.aggno,
                    aggtransno: a.aggtransno,
                    location: a.location,
                },
            )
        }
        NodeTag::T_GroupingFunc => {
            // fix_expr_common (setrefs.c): cols built from refs through
            // root->grouping_map; args mutated like any other expression.
            let g = node.as_grouping_func().expect("GroupingFunc");
            let grouping_map = &run.root.grouping_map;
            debug_assert!(!grouping_map.is_empty() || g.cols.is_nil());
            let cols = if !run.root.grouping_map.is_empty() {
                let mut cols = types_nodes::list::IntList::nil();
                for r in &g.refs {
                    cols.lappend(mcx, run.root.grouping_map[r as usize] as i32)?;
                }
                debug_assert!(
                    g.cols.is_nil() || g.cols.iter().eq(cols.iter()),
                    "GroupingFunc cols disagree with grouping_map"
                );
                cols
            } else {
                g.cols.clone_in(mcx)?
            };
            let mut args = NodeList::nil();
            for arg in &g.args {
                args.lappend(mcx, fix_upper_expr(run, arg, subplan_tlist, rtoffset, newvarno)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::GroupingFunc {
                    args,
                    refs: g.refs.clone_in(mcx)?,
                    cols,
                    agglevelsup: g.agglevelsup,
                    location: g.location,
                },
            )
        }
        NodeTag::T_WindowFunc => {
            let wf = node.as_window_func().expect("WindowFunc");
            record_plan_function_dependency(run, wf.winfnoid)?;
            debug_assert!(wf.aggfilter.is_none() && wf.runCondition.is_nil());
            let mut args = NodeList::nil();
            for arg in &wf.args {
                args.lappend(mcx, fix_upper_expr(run, arg, subplan_tlist, rtoffset, newvarno, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::WindowFunc {
                    winfnoid: wf.winfnoid,
                    wintype: wf.wintype,
                    wincollid: wf.wincollid,
                    inputcollid: wf.inputcollid,
                    args,
                    aggfilter: None,
                    runCondition: NodeList::nil(),
                    winref: wf.winref,
                    winstar: wf.winstar,
                    winagg: wf.winagg,
                    location: wf.location,
                },
            )
        }
        NodeTag::T_OpExpr => {
            let o = node.as_op_expr().expect("OpExpr");
            let opfuncid = set_opfuncid(o)?;
            record_plan_function_dependency(run, opfuncid)?;
            let mut args = NodeList::nil();
            for arg in &o.args {
                args.lappend(mcx, fix_upper_expr(run, arg, subplan_tlist, rtoffset, newvarno, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::OpExpr {
                    opno: o.opno,
                    opfuncid,
                    opresulttype: o.opresulttype,
                    opretset: o.opretset,
                    opcollid: o.opcollid,
                    inputcollid: o.inputcollid,
                    args,
                    location: o.location,
                },
            )
        }
        NodeTag::T_FuncExpr => {
            let f = node.as_func_expr().expect("FuncExpr");
            record_plan_function_dependency(run, f.funcid)?;
            let mut args = NodeList::nil();
            for arg in &f.args {
                args.lappend(mcx, fix_upper_expr(run, arg, subplan_tlist, rtoffset, newvarno, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::FuncExpr {
                    funcid: f.funcid,
                    funcresulttype: f.funcresulttype,
                    funcretset: f.funcretset,
                    funcvariadic: f.funcvariadic,
                    funcformat: f.funcformat,
                    funccollid: f.funccollid,
                    inputcollid: f.inputcollid,
                    args,
                    location: f.location,
                },
            )
        }
        NodeTag::T_RelabelType => {
            let r = node.as_relabel_type().expect("RelabelType");
            let arg = fix_upper_expr(run, r.arg, subplan_tlist, rtoffset, newvarno, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::primnodes::RelabelType {
                    arg,
                    resulttype: r.resulttype,
                    resulttypmod: r.resulttypmod,
                    resultcollid: r.resultcollid,
                    relabelformat: r.relabelformat,
                    location: r.location,
                },
            )
        }
        NodeTag::T_BoolExpr => {
            let b = node.as_bool_expr().expect("BoolExpr");
            let mut args = NodeList::nil();
            for arg in &b.args {
                args.lappend(mcx, fix_upper_expr(run, arg, subplan_tlist, rtoffset, newvarno, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::BoolExpr { boolop: b.boolop, args, location: b.location },
            )
        }
        NodeTag::T_NullTest => {
            let nt = node.as_null_test().unwrap();
            let arg = fix_upper_expr(run, nt.arg.expect("NullTest.arg"), subplan_tlist, rtoffset, newvarno, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::primnodes::NullTest {
                    arg: Some(arg),
                    nulltesttype: nt.nulltesttype,
                    argisrow: nt.argisrow,
                    location: nt.location,
                },
            )
        }
        NodeTag::T_SubPlan => {
            let sp = node.as_sub_plan().unwrap();
            let testexpr = match sp.testexpr {
                None => None,
                Some(te) => Some(fix_upper_expr(run, te, subplan_tlist, rtoffset, newvarno, num_exec)?),
            };
            let mut args = NodeList::nil();
            for arg in &sp.args {
                args.lappend(mcx, fix_upper_expr(run, arg, subplan_tlist, rtoffset, newvarno, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::SubPlan {
                    subLinkType: sp.subLinkType,
                    testexpr,
                    paramIds: sp.paramIds.clone_in(mcx)?,
                    plan_id: sp.plan_id,
                    plan_name: sp.plan_name,
                    firstColType: sp.firstColType,
                    firstColTypmod: sp.firstColTypmod,
                    firstColCollation: sp.firstColCollation,
                    useHashTable: sp.useHashTable,
                    unknownEqFalse: sp.unknownEqFalse,
                    parallel_safe: sp.parallel_safe,
                    setParam: sp.setParam.clone_in(mcx)?,
                    parParam: sp.parParam.clone_in(mcx)?,
                    args,
                    startup_cost: sp.startup_cost,
                    per_call_cost: sp.per_call_cost,
                },
            )
        }
        NodeTag::T_AlternativeSubPlan => {
            let chosen =
                fix_alternative_subplan(run, node.as_alternative_sub_plan().unwrap(), num_exec);
            fix_upper_expr(run, chosen, subplan_tlist, rtoffset, newvarno, num_exec)
        }
        other => panic!("fix_upper_expr_mutator (setrefs.c): {other:?}; M3 expression lane"),
    }
}

// search_indexed_tlist_for_var (setrefs.c); a miss is C's elog(ERROR).
fn search_indexed_tlist_for_var<'mcx>(
    run: &mut PlannerRun<'mcx>,
    var: &types_nodes::primnodes::Var<'mcx>,
    subplan_tlist: &NodeList<'mcx>,
    rtoffset: i32,
    newvarno: i32,
) -> PgResult<Node<'mcx>> {
    debug_assert!(var.varlevelsup == 0 && var.varnullingrels.is_empty());
    for tle_node in subplan_tlist {
        let tle = tle_node.as_target_entry().expect("TargetEntry");
        let Some(sub) = tle.expr.as_var() else { continue };
        if sub.varno == var.varno && sub.varattno == var.varattno {
            let mut newvar = types_nodes::primnodes::Var {
                varno: newvarno,
                varattno: tle.resno,
                vartype: var.vartype,
                vartypmod: var.vartypmod,
                varcollid: var.varcollid,
                varnullingrels: types_nodes::bitmapset::Bitmapset::empty(),
                varlevelsup: 0,
                varreturningtype: var.varreturningtype,
                varnosyn: var.varnosyn,
                varattnosyn: var.varattnosyn,
                location: var.location,
            };
            if newvar.varnosyn > 0 {
                newvar.varnosyn += rtoffset as u32;
            }
            return Node::mk(run.mcx, newvar);
        }
    }
    panic!("variable not found in subplan target list");
}

// fix_scan_expr over a WindowAgg frame offset: offsets are Var-free (parser
// enforced), so C's mutator leg is the identity copy; the walker leg covers
// fix_expr_common bookkeeping.
fn fix_frame_offset<'mcx>(
    run: &mut PlannerRun<'mcx>,
    off: Node<'mcx>,
    rtoffset: i32,
) -> PgResult<()> {
    let _ = rtoffset;
    fix_scan_expr_walker(run, off)
}

// fix_scan_expr (setrefs.c): rtoffset==0 walks in place (fix_expr_common
// only, returns None); rtoffset>0 is the subplan pass and takes C's mutator
// leg, rebuilding the expressions with renumbered varnos.
fn fix_scan_list<'mcx>(
    run: &mut PlannerRun<'mcx>,
    list: &NodeList<'mcx>,
    rtoffset: i32,
    num_exec: f64,
) -> PgResult<Option<NodeList<'mcx>>> {
    debug_assert!(run.root.multiexpr_params.is_empty());
    if rtoffset == 0 && !run.glob.has_alternative_subplans && run.root.minmax_aggs.is_empty() {
        for node in list {
            fix_scan_expr_walker(run, node)?;
        }
        return Ok(None);
    }
    let mut out = NodeList::nil();
    for node in list {
        out.lappend(run.mcx, fix_scan_expr_mutator(run, node, rtoffset, num_exec)?)?;
    }
    Ok(Some(out))
}

// fix_scan_expr_mutator (setrefs.c) over the shapes subplan trees carry.
fn fix_scan_expr_mutator<'mcx>(
    run: &mut PlannerRun<'mcx>,
    node: Node<'mcx>,
    rtoffset: i32,
    num_exec: f64,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    match node.node_tag() {
        NodeTag::T_Var => {
            let var = node.as_var().unwrap();
            debug_assert!(var.varlevelsup == 0);
            let mut newvar = types_nodes::primnodes::Var {
                varno: var.varno,
                varattno: var.varattno,
                vartype: var.vartype,
                vartypmod: var.vartypmod,
                varcollid: var.varcollid,
                varnullingrels: types_nodes::bitmapset::Bitmapset::empty(),
                varlevelsup: 0,
                varreturningtype: var.varreturningtype,
                varnosyn: var.varnosyn,
                varattnosyn: var.varattnosyn,
                location: var.location,
            };
            if newvar.varno > 0 {
                newvar.varno += rtoffset;
            }
            if newvar.varnosyn > 0 {
                newvar.varnosyn += rtoffset as u32;
            }
            Node::mk(mcx, newvar)
        }
        NodeTag::T_Param | NodeTag::T_Const | NodeTag::T_SQLValueFunction => {
            fix_scan_expr_walker(run, node)?;
            Ok(node)
        }
        NodeTag::T_Aggref => {
            let prm = find_minmax_agg_replacement_param(run, node)
                .expect("Aggref outside a minmax Result reaches fix_upper_expr");
            Ok(*run.root.expr_node(prm))
        }
        NodeTag::T_TargetEntry => {
            let tle = node.as_target_entry().unwrap();
            let newexpr = fix_scan_expr_mutator(run, tle.expr, rtoffset, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::primnodes::TargetEntry {
                    expr: newexpr,
                    resno: tle.resno,
                    resname: tle.resname,
                    ressortgroupref: tle.ressortgroupref,
                    resorigtbl: tle.resorigtbl,
                    resorigcol: tle.resorigcol,
                    resjunk: tle.resjunk,
                },
            )
        }
        NodeTag::T_OpExpr => {
            let o = node.as_op_expr().unwrap();
            let opfuncid = set_opfuncid(o)?;
            record_plan_function_dependency(run, opfuncid)?;
            let mut args = NodeList::nil();
            for arg in &o.args {
                args.lappend(mcx, fix_scan_expr_mutator(run, arg, rtoffset, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::OpExpr {
                    opno: o.opno,
                    opfuncid,
                    opresulttype: o.opresulttype,
                    opretset: o.opretset,
                    opcollid: o.opcollid,
                    inputcollid: o.inputcollid,
                    args,
                    location: o.location,
                },
            )
        }
        NodeTag::T_FuncExpr => {
            let f = node.as_func_expr().unwrap();
            record_plan_function_dependency(run, f.funcid)?;
            let mut args = NodeList::nil();
            for arg in &f.args {
                args.lappend(mcx, fix_scan_expr_mutator(run, arg, rtoffset, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::FuncExpr {
                    funcid: f.funcid,
                    funcresulttype: f.funcresulttype,
                    funcretset: f.funcretset,
                    funcvariadic: f.funcvariadic,
                    funcformat: f.funcformat,
                    funccollid: f.funccollid,
                    inputcollid: f.inputcollid,
                    args,
                    location: f.location,
                },
            )
        }
        NodeTag::T_RelabelType => {
            let r = node.as_relabel_type().unwrap();
            let arg = fix_scan_expr_mutator(run, r.arg, rtoffset, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::primnodes::RelabelType {
                    arg,
                    resulttype: r.resulttype,
                    resulttypmod: r.resulttypmod,
                    resultcollid: r.resultcollid,
                    relabelformat: r.relabelformat,
                    location: r.location,
                },
            )
        }
        NodeTag::T_BoolExpr => {
            let b = node.as_bool_expr().unwrap();
            let mut args = NodeList::nil();
            for arg in &b.args {
                args.lappend(mcx, fix_scan_expr_mutator(run, arg, rtoffset, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::BoolExpr { boolop: b.boolop, args, location: b.location },
            )
        }
        NodeTag::T_NullTest => {
            let nt = node.as_null_test().unwrap();
            let arg = fix_scan_expr_mutator(run, nt.arg.expect("NullTest.arg"), rtoffset, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::primnodes::NullTest {
                    arg: Some(arg),
                    nulltesttype: nt.nulltesttype,
                    argisrow: nt.argisrow,
                    location: nt.location,
                },
            )
        }
        NodeTag::T_SubPlan => {
            let sp = node.as_sub_plan().unwrap();
            let testexpr = match sp.testexpr {
                None => None,
                Some(te) => Some(fix_scan_expr_mutator(run, te, rtoffset, num_exec)?),
            };
            let mut args = NodeList::nil();
            for arg in &sp.args {
                args.lappend(mcx, fix_scan_expr_mutator(run, arg, rtoffset, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::SubPlan {
                    subLinkType: sp.subLinkType,
                    testexpr,
                    paramIds: sp.paramIds.clone_in(mcx)?,
                    plan_id: sp.plan_id,
                    plan_name: sp.plan_name,
                    firstColType: sp.firstColType,
                    firstColTypmod: sp.firstColTypmod,
                    firstColCollation: sp.firstColCollation,
                    useHashTable: sp.useHashTable,
                    unknownEqFalse: sp.unknownEqFalse,
                    parallel_safe: sp.parallel_safe,
                    setParam: sp.setParam.clone_in(mcx)?,
                    parParam: sp.parParam.clone_in(mcx)?,
                    args,
                    startup_cost: sp.startup_cost,
                    per_call_cost: sp.per_call_cost,
                },
            )
        }
        NodeTag::T_AlternativeSubPlan => {
            let chosen =
                fix_alternative_subplan(run, node.as_alternative_sub_plan().unwrap(), num_exec);
            fix_scan_expr_mutator(run, chosen, rtoffset, num_exec)
        }
        NodeTag::T_CaseTestExpr => Ok(node),
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            let arg = match c.arg {
                Some(a) => Some(fix_scan_expr_mutator(run, a, rtoffset, num_exec)?),
                None => None,
            };
            let mut args = NodeList::nil();
            for w in &c.args {
                let cw = w.as_case_when().expect("CaseWhen");
                let expr = fix_scan_expr_mutator(run, cw.expr.expect("CaseWhen.expr"), rtoffset, num_exec)?;
                let result =
                    fix_scan_expr_mutator(run, cw.result.expect("CaseWhen.result"), rtoffset, num_exec)?;
                args.lappend(
                    mcx,
                    Node::mk(
                        mcx,
                        types_nodes::primnodes::CaseWhen {
                            expr: Some(expr),
                            result: Some(result),
                            location: cw.location,
                        },
                    )?,
                )?;
            }
            let defresult = match c.defresult {
                Some(d) => Some(fix_scan_expr_mutator(run, d, rtoffset, num_exec)?),
                None => None,
            };
            Node::mk(
                mcx,
                types_nodes::primnodes::CaseExpr {
                    casetype: c.casetype,
                    casecollid: c.casecollid,
                    arg,
                    args,
                    defresult,
                    location: c.location,
                },
            )
        }
        other => panic!("fix_scan_expr_mutator (setrefs.c): {other:?}; M2 expression lane"),
    }
}

fn fix_scan_expr_walker<'mcx>(run: &mut PlannerRun<'mcx>, node: Node<'mcx>) -> PgResult<()> {
    match node.node_tag() {
        // fix_expr_common touches no Var fields; INDEX_VAR Vars pass through.
        NodeTag::T_Var => Ok(()),
        // fix_param_node: only PARAM_MULTIEXPR is rewritten (multiexpr_params
        // asserted empty in fix_scan_list); PARAM_EXEC passes through.
        NodeTag::T_Param => Ok(()),
        // fix_expr_common has nothing to record for a SQLValueFunction.
        NodeTag::T_SQLValueFunction => Ok(()),
        NodeTag::T_RelabelType => {
            fix_scan_expr_walker(run, node.as_relabel_type().unwrap().arg)
        }
        NodeTag::T_Const => {
            let c = node.as_const().unwrap();
            // fix_expr_common: a regclass Const is a plan dependency.
            if c.consttype == REGCLASSOID && !c.constisnull {
                run.glob
                    .relation_oids
                    .lappend(run.mcx, c.constvalue.as_u32())?;
            }
            Ok(())
        }
        NodeTag::T_TargetEntry => {
            fix_scan_expr_walker(run, node.as_target_entry().unwrap().expr)
        }
        NodeTag::T_OpExpr => {
            let o = node.as_op_expr().unwrap();
            // The walker cannot write the set_opfuncid memo back into the
            // shared node; assert it was already resolved (the mutator arms
            // handle the switched-clause InvalidOid case).
            debug_assert!(o.opfuncid != 0, "fix_scan_expr_walker: unresolved opfuncid");
            record_plan_function_dependency(run, o.opfuncid)?;
            for arg in &o.args {
                fix_scan_expr_walker(run, arg)?;
            }
            Ok(())
        }
        NodeTag::T_FuncExpr => {
            let f = node.as_func_expr().unwrap();
            record_plan_function_dependency(run, f.funcid)?;
            for arg in &f.args {
                fix_scan_expr_walker(run, arg)?;
            }
            Ok(())
        }
        NodeTag::T_List => {
            for cell in node.as_list().unwrap() {
                fix_scan_expr_walker(run, cell)?;
            }
            Ok(())
        }
        NodeTag::T_BoolExpr => {
            for arg in &node.as_bool_expr().unwrap().args {
                fix_scan_expr_walker(run, arg)?;
            }
            Ok(())
        }
        NodeTag::T_NullTest => match node.as_null_test().unwrap().arg {
            Some(arg) => fix_scan_expr_walker(run, arg),
            None => Ok(()),
        },
        NodeTag::T_SubPlan => {
            let sp = node.as_sub_plan().unwrap();
            if let Some(te) = sp.testexpr {
                fix_scan_expr_walker(run, te)?;
            }
            for arg in &sp.args {
                fix_scan_expr_walker(run, arg)?;
            }
            Ok(())
        }
        NodeTag::T_CaseTestExpr => Ok(()),
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            if let Some(a) = c.arg {
                fix_scan_expr_walker(run, a)?;
            }
            for w in &c.args {
                let cw = w.as_case_when().expect("CaseWhen");
                fix_scan_expr_walker(run, cw.expr.expect("CaseWhen.expr"))?;
                fix_scan_expr_walker(run, cw.result.expect("CaseWhen.result"))?;
            }
            match c.defresult {
                Some(d) => fix_scan_expr_walker(run, d),
                None => Ok(()),
            }
        }
        NodeTag::T_CoerceViaIO => fix_scan_expr_walker(run, node.as_coerce_via_io().unwrap().arg),
        other => panic!("fix_scan_expr_walker (setrefs.c): {other:?}; M2 expression lane"),
    }
}

// fix_alternative_subplan (setrefs.c): keep the cheapest member for the
// expected execution count. Divergence: C NULLs the losers out of
// glob->subplans so the executor never initializes them; here they stay
// (initialized but never executed or displayed).
fn fix_alternative_subplan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    asplan: &'mcx types_nodes::primnodes::AlternativeSubPlan<'mcx>,
    num_exec: f64,
) -> Node<'mcx> {
    let _ = run;
    let mut best: Option<(Node<'mcx>, f64)> = None;
    for sub_node in &asplan.subplans {
        let sp = sub_node.as_sub_plan().expect("AlternativeSubPlan member");
        let curcost = sp.startup_cost + num_exec * sp.per_call_cost;
        // Ties prefer the later plan (bias against fast-start), as C.
        if best.as_ref().is_none_or(|(_, c)| curcost <= *c) {
            best = Some((sub_node, curcost));
        }
    }
    best.expect("AlternativeSubPlan has members").0
}

// set_opfuncid (nodeFuncs.c): get_switched_clauses hands over commuted
// OpExprs with opfuncid = InvalidOid; C's fix_expr_common resolves them here.
fn set_opfuncid(o: &types_nodes::primnodes::OpExpr<'_>) -> PgResult<u32> {
    if o.opfuncid != 0 {
        return Ok(o.opfuncid);
    }
    lsyscache::get_opcode(o.opno)
}

// Built-ins (OID < FirstUnpinnedObjectId) are assumed immutable and untracked.
fn record_plan_function_dependency<'mcx>(run: &mut PlannerRun<'mcx>, funcid: u32) -> PgResult<()> {
    if funcid < FIRST_UNPINNED_OBJECT_ID {
        return Ok(());
    }
    let hash_value = syscache_seams::syscache_hash_value_procoid::call(funcid)?;
    let item = Node::mk(
        run.mcx,
        types_nodes::plannodes::PlanInvalItem { cacheId: PROCOID, hashValue: hash_value },
    )?;
    run.glob.inval_items.lappend(run.mcx, item)?;
    Ok(())
}

fn set_dummy_tlist_references<'mcx>(
    run: &mut PlannerRun<'mcx>,
    plan: Node<'mcx>,
    rtoffset: i32,
) -> PgResult<()> {
    let mcx = run.mcx;
    let mut output_targetlist = NodeList::nil();
    for tle_node in &plan.as_plan().expect("plan node").targetlist {
        let tle = tle_node.as_target_entry().expect("TargetEntry");
        let oldexpr = tle.expr;
        // Consts stay Consts (cleaner EXPLAIN; C keeps the whole TLE).
        if oldexpr.node_tag() == NodeTag::T_Const {
            output_targetlist.lappend(mcx, tle_node)?;
            continue;
        }
        let (vartype, vartypmod) = crate::costsize::expr_type_typmod(oldexpr);
        let varcollid = crate::pathkeys::expr_collation(oldexpr);
        let mut newvar = types_nodes::primnodes::Var {
            varno: types_nodes::primnodes::OUTER_VAR,
            varattno: tle.resno,
            vartype,
            vartypmod,
            varcollid,
            varnullingrels: types_nodes::bitmapset::Bitmapset::empty(),
            varlevelsup: 0,
            varreturningtype: Default::default(),
            varnosyn: 0,
            varattnosyn: 0,
            location: -1,
        };
        if let Some(oldvar) = oldexpr.as_var() {
            if oldvar.varnosyn > 0 {
                newvar.varnosyn = oldvar.varnosyn + rtoffset as u32;
                newvar.varattnosyn = oldvar.varattnosyn;
            }
        }
        let new_tle = Node::mk(
            mcx,
            types_nodes::primnodes::TargetEntry {
                expr: Node::mk(mcx, newvar)?,
                resno: tle.resno,
                resname: tle.resname,
                ressortgroupref: tle.ressortgroupref,
                resorigtbl: tle.resorigtbl,
                resorigcol: tle.resorigcol,
                resjunk: tle.resjunk,
            },
        )?;
        output_targetlist.lappend(mcx, new_tle)?;
    }
    // SAFETY: exclusive plan-tree ownership (C rewrites the list in place).
    unsafe { plan.with_plan_mut(|p| p.targetlist = output_targetlist) }.expect("plan node");
    Ok(())
}

// set_join_references (setrefs.c), inner-nestloop arm: joinqual and tlist
// Vars retarget onto the child tlists as OUTER_VAR/INNER_VAR. C builds
// indexed_tlists; the linear probe is the set_upper_references divergence
// (cold, tlists tiny). nestParams/merge/hash legs are dead or loud upstream.
fn set_join_references<'mcx>(
    run: &mut PlannerRun<'mcx>,
    plan: Node<'mcx>,
    rtoffset: i32,
) -> PgResult<()> {
    let base = plan.as_plan().expect("join plan");
    let outer_tlist = &base.lefttree.expect("join outer plan").as_plan().unwrap().targetlist;
    let inner_tlist = &base.righttree.expect("join inner plan").as_plan().unwrap().targetlist;

    let is_hash = plan.node_tag() == NodeTag::T_HashJoin;
    let is_merge = plan.node_tag() == NodeTag::T_MergeJoin;
    let (joinqual_src, jointype) = if is_hash {
        let j = plan.as_hash_join().unwrap();
        (&j.join.joinqual, j.join.jointype)
    } else if is_merge {
        let j = plan.as_merge_join().unwrap();
        (&j.join.joinqual, j.join.jointype)
    } else {
        let j = plan.as_nest_loop().unwrap();
        (&j.join.joinqual, j.join.jointype)
    };
    let above_nrm = if jointype == types_nodes::JoinType::JOIN_INNER {
        NrmMatch::Equal
    } else {
        NrmMatch::Superset
    };
    let joinqual = fix_join_expr_list(
        run, joinqual_src, outer_tlist, inner_tlist, rtoffset, NrmMatch::Equal,
        2.0 * base.plan_rows,
    )?;
    let targetlist = fix_join_expr_list(
        run, &base.targetlist, outer_tlist, inner_tlist, rtoffset, above_nrm, base.plan_rows,
    )?;
    let qual = fix_join_expr_list(
        run, &base.qual, outer_tlist, inner_tlist, rtoffset, above_nrm, 2.0 * base.plan_rows,
    )?;

    if is_hash {
        let hj = plan.as_hash_join().unwrap();
        let hashclauses = fix_join_expr_list(
            run,
            &hj.hashclauses,
            outer_tlist,
            inner_tlist,
            rtoffset,
            NrmMatch::Equal,
            2.0 * base.plan_rows,
        )?;
        // HashJoin's hashkeys look up outer tuples: outer itlist -> OUTER_VAR.
        let empty = NodeList::nil();
        let hashkeys = fix_join_expr_list(
            run, &hj.hashkeys, outer_tlist, &empty, rtoffset, NrmMatch::Equal,
            2.0 * base.plan_rows,
        )?;
        // SAFETY: exclusive plan-tree ownership (C rewrites in place).
        unsafe {
            plan.with_mut::<types_nodes::plannodes::HashJoin, _>(|p| {
                p.join.joinqual = joinqual;
                p.join.plan.targetlist = targetlist;
                p.join.plan.qual = qual;
                p.hashclauses = hashclauses;
                p.hashkeys = hashkeys;
            })
        }
        .expect("HashJoin node");
    } else if is_merge {
        let mj = plan.as_merge_join().unwrap();
        let mergeclauses = fix_join_expr_list(
            run,
            &mj.mergeclauses,
            outer_tlist,
            inner_tlist,
            rtoffset,
            NrmMatch::Equal,
            2.0 * base.plan_rows,
        )?;
        // SAFETY: exclusive plan-tree ownership (C rewrites in place).
        unsafe {
            plan.with_mut::<types_nodes::plannodes::MergeJoin, _>(|p| {
                p.join.joinqual = joinqual;
                p.join.plan.targetlist = targetlist;
                p.join.plan.qual = qual;
                p.mergeclauses = mergeclauses;
            })
        }
        .expect("MergeJoin node");
    } else {
        // SAFETY: exclusive plan-tree ownership (C rewrites in place).
        unsafe {
            plan.with_mut::<types_nodes::plannodes::NestLoop, _>(|p| {
                p.join.joinqual = joinqual;
                p.join.plan.targetlist = targetlist;
                p.join.plan.qual = qual;
            })
        }
        .expect("NestLoop node");
    }
    Ok(())
}

// setrefs.c NullingRelsMatch; NRM_SUBSET has no consumer on this lane.
#[derive(Clone, Copy, PartialEq)]
enum NrmMatch {
    Equal,
    Superset,
}

// set_hash_references (setrefs.c): the Hash node's hashkeys reference its own
// outer plan (the HashJoin inner) mapped to OUTER_VAR; the tlist is a dummy
// passthrough (Hash doesn't project).
fn set_hash_references<'mcx>(
    run: &mut PlannerRun<'mcx>,
    plan: Node<'mcx>,
    rtoffset: i32,
) -> PgResult<()> {
    let hash = plan.as_hash().expect("Hash");
    let outer_tlist =
        &hash.plan.lefttree.expect("Hash outer plan").as_plan().unwrap().targetlist;
    let empty = NodeList::nil();
    let hashkeys =
        fix_join_expr_list(run, &hash.hashkeys, outer_tlist, &empty, rtoffset, NrmMatch::Equal, 2.0 * hash.plan.plan_rows)?;
    // SAFETY: exclusive plan-tree ownership (C rewrites in place).
    unsafe { plan.with_mut::<types_nodes::plannodes::Hash, _>(|p| p.hashkeys = hashkeys) }
        .expect("Hash node");
    set_dummy_tlist_references(run, plan, rtoffset)?;
    debug_assert!(plan.as_plan().unwrap().qual.is_nil());
    Ok(())
}

fn fix_join_expr_list<'mcx>(
    run: &mut PlannerRun<'mcx>,
    list: &NodeList<'mcx>,
    outer_tlist: &NodeList<'mcx>,
    inner_tlist: &NodeList<'mcx>,
    rtoffset: i32,
    nrm_match: NrmMatch,
    num_exec: f64,
) -> PgResult<NodeList<'mcx>> {
    let mut out = NodeList::nil();
    for node in list {
        out.lappend(
            run.mcx,
            fix_join_expr_mutator(run, node, outer_tlist, inner_tlist, rtoffset, nrm_match, num_exec)?,
        )?;
    }
    Ok(out)
}

// fix_join_expr_mutator (setrefs.c) over the shapes this lane can carry.
fn fix_join_expr_mutator<'mcx>(
    run: &mut PlannerRun<'mcx>,
    node: Node<'mcx>,
    outer_tlist: &NodeList<'mcx>,
    inner_tlist: &NodeList<'mcx>,
    rtoffset: i32,
    nrm_match: NrmMatch,
    num_exec: f64,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    match node.node_tag() {
        NodeTag::T_Var => {
            let var = node.as_var().unwrap();
            if let Some(new) = search_join_tlist_for_var(
                run,
                var,
                outer_tlist,
                types_nodes::primnodes::OUTER_VAR,
                rtoffset,
                nrm_match,
            )? {
                return Ok(new);
            }
            if let Some(new) = search_join_tlist_for_var(
                run,
                var,
                inner_tlist,
                types_nodes::primnodes::INNER_VAR,
                rtoffset,
                nrm_match,
            )? {
                return Ok(new);
            }
            panic!("variable not found in subplan target lists");
        }
        NodeTag::T_Const | NodeTag::T_SQLValueFunction => {
            fix_scan_expr_walker(run, node)?;
            Ok(node)
        }
        NodeTag::T_TargetEntry => {
            let tle = node.as_target_entry().unwrap();
            let newexpr =
                fix_join_expr_mutator(run, tle.expr, outer_tlist, inner_tlist, rtoffset, nrm_match, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::primnodes::TargetEntry {
                    expr: newexpr,
                    resno: tle.resno,
                    resname: tle.resname,
                    ressortgroupref: tle.ressortgroupref,
                    resorigtbl: tle.resorigtbl,
                    resorigcol: tle.resorigcol,
                    resjunk: tle.resjunk,
                },
            )
        }
        NodeTag::T_OpExpr => {
            let o = node.as_op_expr().unwrap();
            let opfuncid = set_opfuncid(o)?;
            record_plan_function_dependency(run, opfuncid)?;
            let mut args = NodeList::nil();
            for arg in &o.args {
                args.lappend(
                    mcx,
                    fix_join_expr_mutator(run, arg, outer_tlist, inner_tlist, rtoffset, nrm_match, num_exec)?,
                )?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::OpExpr {
                    opno: o.opno,
                    opfuncid,
                    opresulttype: o.opresulttype,
                    opretset: o.opretset,
                    opcollid: o.opcollid,
                    inputcollid: o.inputcollid,
                    args,
                    location: o.location,
                },
            )
        }
        NodeTag::T_RelabelType => {
            let r = node.as_relabel_type().unwrap();
            let arg = fix_join_expr_mutator(run, r.arg, outer_tlist, inner_tlist, rtoffset, nrm_match, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::primnodes::RelabelType {
                    arg,
                    resulttype: r.resulttype,
                    resulttypmod: r.resulttypmod,
                    resultcollid: r.resultcollid,
                    relabelformat: r.relabelformat,
                    location: r.location,
                },
            )
        }
        NodeTag::T_BoolExpr => {
            let b = node.as_bool_expr().unwrap();
            let mut args = NodeList::nil();
            for arg in &b.args {
                args.lappend(
                    mcx,
                    fix_join_expr_mutator(run, arg, outer_tlist, inner_tlist, rtoffset, nrm_match, num_exec)?,
                )?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::BoolExpr { boolop: b.boolop, args, location: b.location },
            )
        }
        NodeTag::T_FuncExpr => {
            let f = node.as_func_expr().unwrap();
            record_plan_function_dependency(run, f.funcid)?;
            let mut args = NodeList::nil();
            for arg in &f.args {
                args.lappend(
                    mcx,
                    fix_join_expr_mutator(run, arg, outer_tlist, inner_tlist, rtoffset, nrm_match, num_exec)?,
                )?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::FuncExpr {
                    funcid: f.funcid,
                    funcresulttype: f.funcresulttype,
                    funcretset: f.funcretset,
                    funcvariadic: f.funcvariadic,
                    funcformat: f.funcformat,
                    funccollid: f.funccollid,
                    inputcollid: f.inputcollid,
                    args,
                    location: f.location,
                },
            )
        }
        NodeTag::T_NullTest => {
            let nt = node.as_null_test().unwrap();
            let arg = match nt.arg {
                None => None,
                Some(a) => Some(fix_join_expr_mutator(
                    run, a, outer_tlist, inner_tlist, rtoffset, nrm_match, num_exec,
                )?),
            };
            Node::mk(
                mcx,
                types_nodes::primnodes::NullTest {
                    arg,
                    nulltesttype: nt.nulltesttype,
                    argisrow: nt.argisrow,
                    location: nt.location,
                },
            )
        }
        NodeTag::T_Param => {
            fix_scan_expr_walker(run, node)?;
            Ok(node)
        }
        NodeTag::T_SubPlan => {
            let sp = node.as_sub_plan().unwrap();
            let testexpr = match sp.testexpr {
                None => None,
                Some(te) => Some(fix_join_expr_mutator(
                    run, te, outer_tlist, inner_tlist, rtoffset, nrm_match, num_exec,
                )?),
            };
            let mut args = NodeList::nil();
            for arg in &sp.args {
                args.lappend(
                    mcx,
                    fix_join_expr_mutator(run, arg, outer_tlist, inner_tlist, rtoffset, nrm_match, num_exec)?,
                )?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::SubPlan {
                    subLinkType: sp.subLinkType,
                    testexpr,
                    paramIds: sp.paramIds.clone_in(mcx)?,
                    plan_id: sp.plan_id,
                    plan_name: sp.plan_name,
                    firstColType: sp.firstColType,
                    firstColTypmod: sp.firstColTypmod,
                    firstColCollation: sp.firstColCollation,
                    useHashTable: sp.useHashTable,
                    unknownEqFalse: sp.unknownEqFalse,
                    parallel_safe: sp.parallel_safe,
                    setParam: sp.setParam.clone_in(mcx)?,
                    parParam: sp.parParam.clone_in(mcx)?,
                    args,
                    startup_cost: sp.startup_cost,
                    per_call_cost: sp.per_call_cost,
                },
            )
        }
        NodeTag::T_AlternativeSubPlan => {
            let chosen =
                fix_alternative_subplan(run, node.as_alternative_sub_plan().unwrap(), num_exec);
            fix_join_expr_mutator(run, chosen, outer_tlist, inner_tlist, rtoffset, nrm_match, num_exec)
        }
        other => panic!("fix_join_expr_mutator (setrefs.c): {other:?}; M2 expression lane"),
    }
}

// search_indexed_tlist_for_var, join leg: miss returns None so the caller can
// probe the other side. The nullingrels cross-check mirrors C's elog guard;
// the emitted Var keeps the reference Var's nullingrels (C copyVar).
fn search_join_tlist_for_var<'mcx>(
    run: &mut PlannerRun<'mcx>,
    var: &types_nodes::primnodes::Var<'mcx>,
    tlist: &NodeList<'mcx>,
    newvarno: i32,
    rtoffset: i32,
    nrm_match: NrmMatch,
) -> PgResult<Option<Node<'mcx>>> {
    debug_assert!(var.varlevelsup == 0);
    for tle_node in tlist {
        let tle = tle_node.as_target_entry().expect("TargetEntry");
        let Some(sub) = tle.expr.as_var() else { continue };
        if sub.varno == var.varno && sub.varattno == var.varattno {
            assert!(
                var.varattno <= 0
                    || match nrm_match {
                        NrmMatch::Superset => sub.varnullingrels.is_subset(&var.varnullingrels),
                        NrmMatch::Equal => sub.varnullingrels.equal(&var.varnullingrels),
                    },
                "wrong varnullingrels for Var {}/{}",
                var.varno,
                var.varattno
            );
            let mut newvar = types_nodes::primnodes::Var {
                varno: newvarno,
                varattno: tle.resno,
                vartype: var.vartype,
                vartypmod: var.vartypmod,
                varcollid: var.varcollid,
                varnullingrels: var.varnullingrels.clone_in(run.mcx)?,
                varlevelsup: 0,
                varreturningtype: var.varreturningtype,
                varnosyn: var.varnosyn,
                varattnosyn: var.varattnosyn,
                location: var.location,
            };
            if newvar.varnosyn > 0 {
                newvar.varnosyn += rtoffset as u32;
            }
            return Ok(Some(Node::mk(run.mcx, newvar)?));
        }
    }
    Ok(None)
}

// set_append_references (setrefs.c); part_prune_index and parallel-aware legs
// have no lane.
fn set_append_references<'mcx>(
    run: &mut PlannerRun<'mcx>,
    plan: Node<'mcx>,
    rtoffset: i32,
) -> PgResult<Node<'mcx>> {
    let aplan = plan.as_append().expect("Append node");
    debug_assert!(aplan.plan.qual.is_nil());
    assert!(aplan.part_prune_index < 0, "set_append_references (setrefs.c): partition pruning lane");

    let mut new_children = NodeList::nil();
    for child in &aplan.appendplans {
        new_children.lappend(run.mcx, set_plan_refs(run, child, rtoffset)?)?;
    }
    let single = (new_children.len() == 1).then(|| new_children.nth(0));
    // SAFETY: exclusive plan-tree ownership (prologue note in set_plan_refs).
    unsafe {
        plan.with_mut::<types_nodes::plannodes::Append, _>(|p| p.appendplans = new_children)
    }
    .expect("Append node");

    if let Some(child) = single {
        if child.as_plan().expect("plan node").parallel_aware
            == plan.as_plan().unwrap().parallel_aware
        {
            return clean_up_removed_plan_level(run, plan, child);
        }
    }

    set_dummy_tlist_references(run, plan, rtoffset)?;
    if rtoffset != 0 {
        let old = &plan.as_append().unwrap().apprelids;
        let mut shifted = types_nodes::bitmapset::Bitmapset::empty();
        let mut m = old.next_member(-1);
        while m >= 0 {
            shifted.add_member(run.mcx, m + rtoffset)?;
            m = old.next_member(m);
        }
        // SAFETY: exclusive plan-tree ownership (prologue note).
        unsafe {
            plan.with_mut::<types_nodes::plannodes::Append, _>(|p| p.apprelids = shifted)
        }
        .expect("Append node");
    }
    debug_assert!(plan.as_plan().unwrap().lefttree.is_none());
    debug_assert!(plan.as_plan().unwrap().righttree.is_none());
    Ok(plan)
}

// set_subqueryscan_references (setrefs.c): the subplan is processed under the
// rel's subroot; trivial scans are elided.
fn set_subqueryscan_references<'mcx>(
    run: &mut PlannerRun<'mcx>,
    plan: Node<'mcx>,
    rtoffset: i32,
) -> PgResult<Node<'mcx>> {
    let s = plan.as_subquery_scan().expect("SubqueryScan node");
    let rel = crate::relnode::find_base_rel(&run.root, s.scan.scanrelid as i32);
    let idx = run.root.rel(rel).subroot_idx.expect("subquery rel has a subroot");
    let subplan = s.subplan.expect("SubqueryScan has a subplan");

    run.swap_with_rel_subroot(idx);
    let new_subplan = set_plan_references(run, subplan);
    run.swap_with_rel_subroot(idx);
    let new_subplan = new_subplan?;
    // SAFETY: exclusive plan-tree ownership (prologue note).
    unsafe {
        plan.with_mut::<types_nodes::plannodes::SubqueryScan, _>(|p| {
            p.subplan = Some(new_subplan)
        })
    }
    .expect("SubqueryScan node");

    if trivial_subqueryscan(plan) {
        return clean_up_removed_plan_level(run, plan, new_subplan);
    }

    let s = plan.as_subquery_scan().unwrap();
    let tl = fix_scan_list(run, &s.scan.plan.targetlist, rtoffset, s.scan.plan.plan_rows)?;
    let qual = fix_scan_list(run, &s.scan.plan.qual, rtoffset, 2.0 * s.scan.plan.plan_rows)?;
    // SAFETY: exclusive plan-tree ownership (prologue note).
    unsafe {
        plan.with_mut::<types_nodes::plannodes::SubqueryScan, _>(|p| {
            if let Some(tl) = tl {
                p.scan.plan.targetlist = tl;
            }
            if let Some(q) = qual {
                p.scan.plan.qual = q;
            }
            p.scan.scanrelid += rtoffset as u32;
        })
    }
    .expect("SubqueryScan node");
    Ok(plan)
}

const SUBQUERY_SCAN_TRIVIAL: u32 = 1;
const SUBQUERY_SCAN_NONTRIVIAL: u32 = 2;

// trivial_subqueryscan (setrefs.c), scanstatus memo included.
fn trivial_subqueryscan(plan: Node<'_>) -> bool {
    let s = plan.as_subquery_scan().expect("SubqueryScan node");
    match s.scanstatus {
        SUBQUERY_SCAN_TRIVIAL => return true,
        SUBQUERY_SCAN_NONTRIVIAL => return false,
        _ => {}
    }
    let set_status = |v: u32| {
        // SAFETY: exclusive plan-tree ownership (prologue note); scanstatus is
        // a memo the executor also reads.
        unsafe {
            plan.with_mut::<types_nodes::plannodes::SubqueryScan, _>(|p| p.scanstatus = v)
        }
        .expect("SubqueryScan node");
    };
    set_status(SUBQUERY_SCAN_NONTRIVIAL);

    if !s.scan.plan.qual.is_nil() {
        return false;
    }
    let sub_tlist = &s.subplan.expect("SubqueryScan has a subplan").as_plan().unwrap().targetlist;
    if s.scan.plan.targetlist.len() != sub_tlist.len() {
        return false;
    }
    for (attrno, (p_node, c_node)) in
        s.scan.plan.targetlist.iter().zip(sub_tlist.iter()).enumerate()
    {
        let ptle = p_node.as_target_entry().expect("tlist cell");
        let ctle = c_node.as_target_entry().expect("tlist cell");
        if ptle.resjunk != ctle.resjunk {
            return false;
        }
        if let Some(var) = ptle.expr.as_var() {
            debug_assert!(var.varno as u32 == s.scan.scanrelid && var.varlevelsup == 0);
            if var.varattno != (attrno + 1) as i16 {
                return false;
            }
        } else if ptle.expr.node_tag() == NodeTag::T_Const {
            if !types_nodes::equal(ptle.expr, ctle.expr) {
                return false;
            }
        } else {
            return false;
        }
    }
    set_status(SUBQUERY_SCAN_TRIVIAL);
    true
}

// clean_up_removed_plan_level (setrefs.c).
fn clean_up_removed_plan_level<'mcx>(
    run: &PlannerRun<'mcx>,
    parent: Node<'mcx>,
    child: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let pplan = parent.as_plan().expect("plan node");
    if !pplan.initPlan.is_nil() {
        // SS_compute_initplan_cost: move the initplans and their run costs.
        let mut initplan_cost = 0.0;
        let mut unsafe_initplans = false;
        for sp_node in &pplan.initPlan {
            let sp = sp_node.as_sub_plan().expect("initPlan holds SubPlan nodes");
            initplan_cost += sp.startup_cost + sp.per_call_cost;
            if !sp.parallel_safe {
                unsafe_initplans = true;
            }
        }
        let mcx = run.mcx;
        let mut merged = pplan.initPlan.clone_in(mcx)?;
        merged.concat(mcx, &child.as_plan().unwrap().initPlan)?;
        // SAFETY: exclusive plan-tree ownership (prologue note).
        unsafe {
            child.with_plan_mut(|c| {
                c.startup_cost += initplan_cost;
                c.total_cost += initplan_cost;
                if unsafe_initplans {
                    c.parallel_safe = false;
                }
                c.initPlan = merged;
            })
        }
        .expect("plan node");
    }
    crate::createplan::apply_tlist_labeling(child, &pplan.targetlist);
    Ok(child)
}

/// find_minmax_agg_replacement_param (setrefs.c); the returned NodeId is the
/// InitPlan output Param in the current root's arena.
pub(crate) fn find_minmax_agg_replacement_param<'mcx>(
    run: &PlannerRun<'mcx>,
    node: Node<'mcx>,
) -> Option<types_pathnodes::NodeId> {
    let aggref = node.as_aggref()?;
    if run.root.minmax_aggs.is_empty() || aggref.args.len() != 1 {
        return None;
    }
    let cur_target = aggref
        .args
        .nth(0)
        .as_target_entry()
        .expect("Aggref.args holds TargetEntries")
        .expr;
    for i in 0..run.root.minmax_aggs.len() {
        let mm = *run.root.minmax_agg_info(run.root.minmax_aggs[i]);
        if mm.aggfnoid == aggref.aggfnoid
            && types_nodes::equal(*run.root.expr_node(mm.target), cur_target)
        {
            return Some(mm.param);
        }
    }
    None
}
