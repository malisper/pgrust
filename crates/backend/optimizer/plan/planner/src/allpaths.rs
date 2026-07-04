//! allpaths.c slice: make_one_rel over a single plain baserel (seqscan +
//! index paths); other RTE kinds and parallel paths are loud or dead.

use types_error::PgResult;
use types_nodes::parsenodes::RTEKind;
use types_pathnodes::{JoinlistNode, RelId, RELOPT_BASEREL};

use crate::pathnode::{add_path, set_cheapest};
use crate::run::PlannerRun;
pub fn make_one_rel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinlist: &[JoinlistNode<'mcx>],
) -> PgResult<RelId> {
    // set_base_rel_consider_startup (allpaths.c): a singleton SEMI/ANTI RHS
    // may benefit from fast-start parameterized plans.
    for i in 0..run.root.join_info_list.len() {
        let sj = &run.root.join_info_list[i];
        if !matches!(
            sj.jointype,
            types_pathnodes::JOIN_SEMI | types_pathnodes::JOIN_ANTI
        ) {
            continue;
        }
        if let Some(relid) = crate::relnode::relids_singleton_member(&sj.min_righthand) {
            let rel = crate::relnode::find_base_rel(&run.root, relid);
            run.root.rel_mut(rel).consider_param_startup = true;
        }
    }

    set_base_rel_sizes(run)?;

    let mut total_pages = 0.0f64;
    for rti in 1..run.root.simple_rel_array_size as usize {
        let Some(brel) = run.root.simple_rel_array[rti] else { continue };
        debug_assert_eq!(run.root.rel(brel).relid as usize, rti);
        if (run.root.rel(brel).reloptkind == RELOPT_BASEREL
            || run.root.rel(brel).reloptkind == types_pathnodes::RELOPT_OTHER_MEMBER_REL)
            && !crate::joinrels::is_dummy_rel(&run.root, brel)
        {
            total_pages += run.root.rel(brel).pages as f64;
        }
    }
    run.root.total_table_pages = total_pages;

    set_base_rel_pathlists(run)?;

    crate::joinrels::make_rel_from_joinlist(run, joinlist)
}

fn set_base_rel_sizes(run: &mut PlannerRun<'_>) -> PgResult<()> {
    for rti in 1..run.root.simple_rel_array_size as usize {
        let Some(rel) = run.root.simple_rel_array[rti] else { continue };
        debug_assert_eq!(run.root.rel(rel).relid as usize, rti);
        if run.root.rel(rel).reloptkind != RELOPT_BASEREL {
            continue;
        }
        if run.glob.parallel_mode_ok {
            set_rel_consider_parallel(run, rel, rti)?;
        }
        set_rel_size(run, rel, rti)?;
    }
    Ok(())
}

fn set_base_rel_pathlists(run: &mut PlannerRun<'_>) -> PgResult<()> {
    for rti in 1..run.root.simple_rel_array_size as usize {
        let Some(rel) = run.root.simple_rel_array[rti] else { continue };
        if run.root.rel(rel).reloptkind != RELOPT_BASEREL {
            continue;
        }
        set_rel_pathlist(run, rel, rti)?;
    }
    Ok(())
}

fn set_rel_size(run: &mut PlannerRun<'_>, rel: RelId, rti: usize) -> PgResult<()> {
    if run.root.rel(rel).reloptkind == RELOPT_BASEREL
        && relation_excluded_by_constraints(run, rel, rti)?
    {
        set_dummy_rel_pathlist(run, rel)?;
        return Ok(());
    }
    let rte = run.rte(rti);
    if rte.inh {
        return set_append_rel_size(run, rel, rti);
    }
    match rte.rtekind {
        RTEKind::RTE_RELATION => {
            // Toast relations are plain heaps in C's set_plain_rel_size arm
            // (direct SELECT from pg_toast.* is legal).
            if rte.relkind == types_rel::RELKIND_PARTITIONED_TABLE {
                // allpaths.c:394: ONLY on a partitioned table, or a
                // zero-partition parent whose stale relhassubclass cleared
                // inh -- storageless, always empty.
                set_dummy_rel_pathlist(run, rel)?;
                return Ok(());
            }
            assert!(
                rte.relkind == types_rel::RELKIND_RELATION
                    || rte.relkind == types_rel::RELKIND_TOASTVALUE
                    || rte.relkind == types_rel::RELKIND_SEQUENCE
                    || rte.relkind == types_rel::RELKIND_MATVIEW,
                "set_rel_size relkind {}",
                rte.relkind
            );
            debug_assert!(rte.tablesample.is_none());
            set_plain_rel_size(run, rel)?;
        }
        RTEKind::RTE_FUNCTION => {
            crate::costsize::set_function_size_estimates(run, rel)?;
        }
        RTEKind::RTE_VALUES => {
            crate::costsize::set_values_size_estimates(run, rel)?;
        }
        RTEKind::RTE_SUBQUERY => {
            set_subquery_pathlist(run, rel, rti)?;
        }
        RTEKind::RTE_CTE => {
            if rte.self_reference {
                crate::cte::set_worktable_pathlist(run, rel, rti)?;
            } else {
                crate::cte::set_cte_pathlist(run, rel, rti)?;
            }
        }
        RTEKind::RTE_RESULT => {
            crate::costsize::set_result_size_estimates(run, rel)?;
        }
        other => panic!("set_rel_size (allpaths.c): {other:?}; M2 scan lane"),
    }
    debug_assert!(
        run.root.rel(rel).rows > 0.0 || crate::joinrels::is_dummy_rel(&run.root, rel)
    );
    Ok(())
}

// set_subquery_pathlist (allpaths.c): qual pushdown is loud (baserestrictinfo
// empty on this lane), remove_unused_subquery_outputs skipped (plan-shape
// optimization only), pathkeys empty (convert_subquery_pathkeys unported).
fn set_subquery_pathlist(run: &mut PlannerRun<'_>, rel: RelId, rti: usize) -> PgResult<()> {
    let rte = run.rte(rti);
    let required_outer = crate::relnode::relids_copy(run.mcx, &run.root.rel(rel).lateral_relids);
    assert!(
        run.root.rel(rel).baserestrictinfo.is_empty(),
        "set_subquery_pathlist (allpaths.c): qual pushdown \
         (subquery_is_pushdown_safe) unported"
    );

    let parse = run.parse();
    let mut n_baserels = 0;
    for i in 1..run.root.simple_rel_array_size as usize {
        if let Some(r) = run.root.simple_rel_array[i] {
            if run.root.rel(r).reloptkind == RELOPT_BASEREL {
                n_baserels += 1;
            }
        }
    }
    let tuple_fraction = if parse.hasAggs
        || !parse.groupClause.is_nil()
        || !parse.groupingSets.is_nil()
        || run.root.hasHavingQual
        || !parse.distinctClause.is_nil()
        || !parse.sortClause.is_nil()
        || n_baserels > 1
    {
        0.0
    } else {
        run.root.tuple_fraction
    };

    debug_assert!(run.root.plan_params.is_empty());
    let sub_parse = crate::subselect::query_cells_copy(
        run.mcx,
        rte.subquery.expect("RTE_SUBQUERY has a subquery"),
    )?;
    run.push_root()?;
    crate::subquery::subquery_planner(run, sub_parse, false, tuple_fraction, None)?;
    let idx = run.pop_root_to_rel_subroot();
    run.root.rel_mut(rel).subroot_idx = Some(idx);
    // Isolate the params needed by this specific subplan.
    let sp = core::mem::replace(&mut run.root.plan_params, mcx::PgVec::new_in(run.mcx));
    run.root.rel_mut(rel).subplan_params = sp;

    run.swap_with_rel_subroot(idx);
    let sub_dummy = {
        let final_rel = crate::planmain::fetch_final_rel(run);
        crate::joinrels::is_dummy_rel(&run.root, final_rel)
    };
    run.swap_with_rel_subroot(idx);
    if sub_dummy {
        return set_dummy_rel_pathlist(run, rel);
    }

    crate::costsize::set_subquery_size_estimates(run, rel)?;

    let sub = run.rte(rti).subquery.expect("RTE_SUBQUERY has a subquery");
    let trivial_pathtarget = {
        let rt = run.root.rel_reltarget(rel);
        if rt.exprs.len() != sub.targetList.len() {
            false
        } else {
            let mut ok = true;
            for (i, &eid) in rt.exprs.iter().enumerate() {
                match run.root.expr_node(eid).as_var() {
                    Some(v) if v.varno == rti as i32 && v.varattno as usize == i + 1 => {}
                    _ => {
                        ok = false;
                        break;
                    }
                }
            }
            ok
        }
    };

    run.swap_with_rel_subroot(idx);
    let mut candidates: mcx::PgVec<
        '_,
        (types_pathnodes::PathId, crate::pathnode::SubqueryScanInfo),
    > = mcx::PgVec::new_in(run.mcx);
    {
        let final_rel = crate::planmain::fetch_final_rel(run);
        debug_assert!(run.root.rel(final_rel).partial_pathlist.is_empty());
        let paths =
            crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(final_rel).pathlist);
        for &sp in paths.iter() {
            candidates.push((sp, crate::prepunion::child_info(run, sp)));
        }
    }
    run.swap_with_rel_subroot(idx);

    for c in candidates.iter() {
        let id = crate::pathnode::create_subqueryscan_path(
            run,
            rel,
            c.0,
            trivial_pathtarget,
            mcx::PgVec::new_in(run.mcx),
            &required_outer,
            &c.1,
        )?;
        add_path(run, rel, id);
    }
    Ok(())
}

// relation_excluded_by_constraints (plancat.c); hosted here with its only
// callers. Fallible: the refutation legs probe catalogs and may evaluate
// cross-type comparison operators.
fn relation_excluded_by_constraints(
    run: &mut PlannerRun<'_>,
    rel: RelId,
    rti: usize,
) -> PgResult<bool> {
    let mcx = run.mcx;
    if run.root.rel(rel).baserestrictinfo.is_empty() {
        return Ok(false);
    }
    // Regardless of constraint_exclusion, detect constant-FALSE-or-NULL
    // restrictions (qual pushdown can leave other members beside the FALSE).
    for i in 0..run.root.rel(rel).baserestrictinfo.len() {
        let rid = run.root.rel(rel).baserestrictinfo[i];
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        if let Some(c) = clause.as_const() {
            if c.constisnull || !c.constvalue.as_bool() {
                return Ok(true);
            }
        }
    }

    let mut include_partition = false;
    match crate::gucs::constraint_exclusion() {
        guc_tables::consts::CONSTRAINT_EXCLUSION_OFF => return Ok(false),
        guc_tables::consts::CONSTRAINT_EXCLUSION_PARTITION => {
            // Only appendrel members; partition pruning already ran.
            if run.root.rel(rel).reloptkind != types_pathnodes::RELOPT_OTHER_MEMBER_REL {
                return Ok(false);
            }
        }
        _ => {
            // 'on': a directly named partition's constraint is not yet applied.
            if run.root.rel(rel).reloptkind == types_pathnodes::RELOPT_BASEREL {
                include_partition = true;
            }
        }
    }

    // Self-contradictory immutable restrictions exclude the scan; weak
    // refutation suffices (restrictions vs restrictions).
    let mut safe_restrictions: mcx::PgVec<'_, types_nodes::Node<'_>> = mcx::PgVec::new_in(mcx);
    let mut baserestrict_clauses: mcx::PgVec<'_, types_nodes::Node<'_>> = mcx::PgVec::new_in(mcx);
    for i in 0..run.root.rel(rel).baserestrictinfo.len() {
        let rid = run.root.rel(rel).baserestrictinfo[i];
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        baserestrict_clauses.push(clause);
        if !clauses::contain_mutable_functions(clause)? {
            safe_restrictions.push(clause);
        }
    }
    if crate::predtest::predicate_refuted_by(mcx, &safe_restrictions, &safe_restrictions, true)? {
        return Ok(true);
    }

    let rte = run.rte(rti);
    if rte.rtekind != RTEKind::RTE_RELATION {
        return Ok(false);
    }

    // NO INHERIT constraints apply only when not scanning children too;
    // attnotnull is NO INHERIT unless the table is partitioned.
    let include_noinherit = !rte.inh;
    let include_notnull = !rte.inh || rte.relkind == types_rel::RELKIND_PARTITIONED_TABLE;
    let rte_relid = rte.relid;

    let constraint_pred = crate::plancat::get_relation_constraints(
        run,
        rte_relid,
        rel,
        include_noinherit,
        include_notnull,
        include_partition,
    )?;

    // CHECK constraints may contain mutable functions; ignore those members.
    let mut safe_constraints: mcx::PgVec<'_, types_nodes::Node<'_>> = mcx::PgVec::new_in(mcx);
    for &pred in constraint_pred.iter() {
        if !clauses::contain_mutable_functions(pred)? {
            safe_constraints.push(pred);
        }
    }

    // Strong refutation of the ANDed constraints by the full restriction list
    // (volatile OR subclauses are still usable for deduction, hence not
    // safe_restrictions here).
    if crate::predtest::predicate_refuted_by(mcx, &safe_constraints, &baserestrict_clauses, false)?
    {
        return Ok(true);
    }

    Ok(false)
}

// set_dummy_rel_pathlist (allpaths.c). C marks a dummy with a childless
// Append that create_append_plan turns into a gated Result; Append is
// unported, so the marker is a zero-cost GroupResultPath whose single
// constant-FALSE qual creates the identical Result plan.
pub fn set_dummy_rel_pathlist(run: &mut PlannerRun<'_>, rel: RelId) -> PgResult<()> {
    run.root.rel_mut(rel).rows = 0.0;
    run.root.rel_reltarget_mut(rel).width = 0;
    run.root.rel_mut(rel).pathlist.clear();
    run.root.rel_mut(rel).partial_pathlist.clear();

    let konst = clauses::make_bool_const(run.mcx, false, false)?;
    let mut quals: mcx::PgVec<'_, types_pathnodes::NodeId> = mcx::PgVec::new_in(run.mcx);
    quals.push(run.intern_expr(konst));
    let target_id = run.rel_reltarget_id(rel);
    let parallel_safe = run.root.rel(rel).consider_parallel;
    let path = types_pathnodes::PathNode::GroupResultPath(types_pathnodes::GroupResultPath {
        path: types_pathnodes::Path {
            type_: crate::pathnode::tag16(types_nodes::NodeTag::T_GroupResultPath),
            pathtype: crate::pathnode::tag16(types_nodes::NodeTag::T_Result),
            parent: rel,
            pathtarget_id: Some(target_id),
            param_info: None,
            parallel_aware: false,
            parallel_safe,
            parallel_workers: 0,
            rows: 0.0,
            disabled_nodes: 0,
            startup_cost: 0.0,
            total_cost: 0.0,
            pathkeys: mcx::PgVec::new_in(run.mcx),
        },
        quals,
    });
    let pid = run.root.alloc_path(path);
    add_path(run, rel, pid);
    set_cheapest(run, rel)?;
    Ok(())
}

fn set_rel_pathlist(run: &mut PlannerRun<'_>, rel: RelId, rti: usize) -> PgResult<()> {
    if crate::joinrels::is_dummy_rel(&run.root, rel) {
        return set_cheapest(run, rel);
    }
    let rte = run.rte(rti);
    if rte.inh {
        set_append_rel_pathlist(run, rel, rti)?;
    } else {
        match rte.rtekind {
            RTEKind::RTE_RELATION => set_plain_rel_pathlist(run, rel)?,
            RTEKind::RTE_FUNCTION => set_function_pathlist(run, rel, rti)?,
            RTEKind::RTE_VALUES => set_values_pathlist(run, rel)?,
            RTEKind::RTE_SUBQUERY => {} // fully handled during set_rel_size
            RTEKind::RTE_CTE => {} // fully handled during set_rel_size
            RTEKind::RTE_RESULT => set_result_pathlist(run, rel)?,
            other => panic!("set_rel_pathlist (allpaths.c): {other:?}; M2 scan lane"),
        }
    }

    debug_assert!(run.root.rel(rel).partial_pathlist.is_empty());
    set_cheapest(run, rel)?;
    Ok(())
}

// set_append_rel_size (allpaths.c): size each live child, then aggregate.
// Partitionwise joins and child ECs stay dead (no ECs exist on this lane).
fn set_append_rel_size(run: &mut PlannerRun<'_>, rel: RelId, rti: usize) -> PgResult<()> {
    let mcx = run.mcx;
    debug_assert!(
        run.root.rel(rel).reloptkind == RELOPT_BASEREL
            || run.root.rel(rel).reloptkind == types_pathnodes::RELOPT_OTHER_MEMBER_REL
    );
    assert!(
        run.root.rel(rel).joininfo.is_empty(),
        "set_append_rel_size (allpaths.c): joininfo translation \
         (adjust_appendrel_attrs over RestrictInfo); inherited-join lane"
    );
    // C divergence: add_child_rel_equivalences is unported (appendrel EC
    // lane) — child EC members only feed child parameterized index paths and
    // MergeAppend orderings; the indexlist gate in add_paths_to_append_rel
    // stays loud where those could change the chosen plan. Join enforcement
    // is unaffected: the parent appendrel's members drive
    // generate_join_implied_equalities at the join level.

    let mut has_live_children = false;
    let mut parent_tuples = 0.0f64;
    let mut parent_rows = 0.0f64;
    let mut parent_size = 0.0f64;
    let (min_attr, max_attr) = {
        let r = run.root.rel(rel);
        (r.min_attr, r.max_attr)
    };
    let nattrs = (max_attr - min_attr + 1) as usize;
    let mut parent_attrsizes = mcx::vec_from_elem_in(mcx, 0.0f64, nattrs);

    for ai in 0..run.root.append_rel_list.len() {
        let (parent_relid, child_rti) = {
            let a = &run.root.append_rel_list[ai];
            (a.parent_relid, a.child_relid)
        };
        if parent_relid != rti as u32 {
            continue;
        }
        let childrel = crate::relnode::find_base_rel(&run.root, child_rti as i32);
        debug_assert!(
            run.root.rel(childrel).reloptkind == types_pathnodes::RELOPT_OTHER_MEMBER_REL
        );
        if crate::joinrels::is_dummy_rel(&run.root, childrel) {
            continue;
        }
        if relation_excluded_by_constraints(run, childrel, child_rti as usize)? {
            set_dummy_rel_pathlist(run, childrel)?;
            continue;
        }

        // Child reltarget = parent reltarget translated.
        let appinfo = run.root.append_rel_array[child_rti as usize]
            .clone()
            .expect("child AppendRelInfo");
        let parent_exprs =
            crate::relnode::pgvec_clone_shallow(mcx, &run.root.rel_reltarget(rel).exprs);
        let mut child_exprs: mcx::PgVec<'_, types_pathnodes::NodeId> = mcx::PgVec::new_in(mcx);
        for &eid in parent_exprs.iter() {
            let e = *run.root.expr_node(eid);
            let translated = crate::inherit::adjust_appendrel_attrs(run, e, &appinfo)?;
            child_exprs.push(run.intern_expr(translated));
        }
        let child_target = run.rel_reltarget_id(childrel);
        run.root.pathtarget_mut(child_target).exprs = child_exprs;

        if run.glob.parallel_mode_ok && run.root.rel(rel).consider_parallel {
            set_rel_consider_parallel(run, childrel, child_rti as usize)?;
        }

        set_rel_size(run, childrel, child_rti as usize)?;

        if crate::joinrels::is_dummy_rel(&run.root, childrel) {
            continue;
        }
        has_live_children = true;
        if !run.root.rel(childrel).consider_parallel {
            run.root.rel_mut(rel).consider_parallel = false;
        }

        debug_assert!(run.root.rel(childrel).rows > 0.0);
        let child_rows = run.root.rel(childrel).rows;
        parent_tuples += run.root.rel(childrel).tuples;
        parent_rows += child_rows;
        parent_size += run.root.rel_reltarget(childrel).width as f64 * child_rows;

        let n = run.root.rel_reltarget(rel).exprs.len();
        debug_assert_eq!(n, run.root.rel_reltarget(childrel).exprs.len());
        for i in 0..n {
            let pid = run.root.rel_reltarget(rel).exprs[i];
            let parentvar = *run.root.expr_node(pid);
            let cid = run.root.rel_reltarget(childrel).exprs[i];
            let childvar = *run.root.expr_node(cid);
            let Some(pv) = parentvar.as_var() else { continue };
            if pv.varno != rti as i32 {
                continue;
            }
            let pndx = (pv.varattno - min_attr) as usize;
            let mut child_width = 0i32;
            if let Some(cv) = childvar.as_var() {
                if cv.varno == run.root.rel(childrel).relid as i32 {
                    let cndx = (cv.varattno - run.root.rel(childrel).min_attr) as usize;
                    child_width = run.root.rel(childrel).attr_widths[cndx];
                }
            }
            if child_width <= 0 {
                let (typid, typmod) = crate::costsize::expr_type_typmod(childvar);
                child_width = lsyscache::get_typavgwidth(typid, typmod)?;
            }
            debug_assert!(child_width > 0);
            parent_attrsizes[pndx] += child_width as f64 * child_rows;
        }
    }

    if has_live_children {
        debug_assert!(parent_rows > 0.0);
        {
            let r = run.root.rel_mut(rel);
            r.tuples = parent_tuples;
            r.rows = parent_rows;
        }
        run.root.rel_reltarget_mut(rel).width = (parent_size / parent_rows).round_ties_even() as i32;
        for i in 0..nattrs {
            run.root.rel_mut(rel).attr_widths[i] =
                (parent_attrsizes[i] / parent_rows).round_ties_even() as i32;
        }
        // rel->pages stays zero: appendrels must not double-count in
        // total_table_pages.
    } else {
        set_dummy_rel_pathlist(run, rel)?;
    }
    Ok(())
}

// set_append_rel_pathlist + add_paths_to_append_rel (allpaths.c), serial
// unparameterized arm; ordered/parameterized appends are loud below.
fn set_append_rel_pathlist(run: &mut PlannerRun<'_>, rel: RelId, rti: usize) -> PgResult<()> {
    let mcx = run.mcx;
    let mut live_childrels: mcx::PgVec<'_, RelId> = mcx::PgVec::new_in(mcx);
    for ai in 0..run.root.append_rel_list.len() {
        let (parent_relid, child_rti) = {
            let a = &run.root.append_rel_list[ai];
            (a.parent_relid, a.child_relid)
        };
        if parent_relid != rti as u32 {
            continue;
        }
        let childrel = crate::relnode::find_base_rel(&run.root, child_rti as i32);
        if !run.root.rel(rel).consider_parallel {
            run.root.rel_mut(childrel).consider_parallel = false;
        }
        set_rel_pathlist(run, childrel, child_rti as usize)?;
        if crate::joinrels::is_dummy_rel(&run.root, childrel) {
            continue;
        }
        live_childrels.push(childrel);
    }
    add_paths_to_append_rel(run, rel, &live_childrels)
}

pub(crate) fn add_paths_to_append_rel(
    run: &mut PlannerRun<'_>,
    rel: RelId,
    live_childrels: &[RelId],
) -> PgResult<()> {
    let mcx = run.mcx;
    let mut subpaths: mcx::PgVec<'_, types_pathnodes::PathId> = mcx::PgVec::new_in(mcx);
    let mut startup_subpaths: mcx::PgVec<'_, types_pathnodes::PathId> = mcx::PgVec::new_in(mcx);
    let mut startup_valid = run.root.rel(rel).consider_startup;
    for &childrel in live_childrels {
        debug_assert!(run.root.rel(childrel).partial_pathlist.is_empty());
        let cheapest_total = run.root.rel(childrel).cheapest_total_path;
        match cheapest_total {
            Some(p) if run.root.path(p).base().param_info.is_none() => {
                accumulate_append_subpath(&run.root, p, &mut subpaths);
            }
            _ => panic!(
                "add_paths_to_append_rel (allpaths.c): parameterized-only child; \
                 parameterized-append lane"
            ),
        }
        if startup_valid {
            match run.root.rel(childrel).cheapest_startup_path {
                Some(p) => {
                    let chosen = if run.root.tuple_fraction > 0.0 {
                        crate::pathnode::get_cheapest_fractional_path(
                            run,
                            childrel,
                            run.root.tuple_fraction,
                        )
                    } else {
                        p
                    };
                    debug_assert!(run.root.path(chosen).base().param_info.is_none());
                    accumulate_append_subpath(&run.root, chosen, &mut startup_subpaths);
                }
                None => startup_valid = false,
            }
        }
        // generate_orderedappend_paths / parameterized appends: without
        // add_child_rel_equivalences child index paths never carry pathkeys,
        // so a MergeAppend C might pick is invisible here — stay loud when
        // ordering is requested and a child has indexes to order by.
        if !run.root.query_pathkeys.is_empty() {
            assert!(
                run.root.rel(childrel).indexlist.is_empty(),
                "generate_orderedappend_paths (allpaths.c): indexed child under \
                 query_pathkeys; MergeAppend/child-EC lane"
            );
        }
    }

    let pid = crate::pathnode::create_append_path(run, rel, subpaths, -1.0)?;
    add_path(run, rel, pid);
    if startup_valid {
        let pid = crate::pathnode::create_append_path(run, rel, startup_subpaths, -1.0)?;
        add_path(run, rel, pid);
    }
    Ok(())
}

// accumulate_append_subpath (allpaths.c), non-parallel arm: flatten nested
// serial Appends (multi-level partitioning); MergeAppend children can't
// exist (ordered append is loud above).
fn accumulate_append_subpath(
    root: &types_pathnodes::PlannerInfo<'_>,
    path: types_pathnodes::PathId,
    subpaths: &mut mcx::PgVec<'_, types_pathnodes::PathId>,
) {
    if let types_pathnodes::PathNode::AppendPath(a) = root.path(path) {
        if !a.path.parallel_aware {
            debug_assert!(a.first_partial_path as usize == a.subpaths.len());
            for &sp in a.subpaths.iter() {
                subpaths.push(sp);
            }
            return;
        }
    }
    subpaths.push(path);
}

// set_function_pathlist (allpaths.c); the ORDINALITY pathkey leg is dead
// (funcordinality is loud in the parser).
fn set_function_pathlist(run: &mut PlannerRun<'_>, rel: RelId, rti: usize) -> PgResult<()> {
    debug_assert!(!run.rte(rti).funcordinality);
    let required_outer = crate::relnode::relids_copy(run.mcx, &run.root.rel(rel).lateral_relids);
    let path = crate::pathnode::create_functionscan_path(run, rel, &required_outer)?;
    add_path(run, rel, path);
    Ok(())
}
// set_result_pathlist (allpaths.c): one Result path, parameterized only by
// lateral refs (join quals never push into a Result scan).
fn set_result_pathlist(run: &mut PlannerRun<'_>, rel: RelId) -> PgResult<()> {
    let required_outer = crate::relnode::relids_copy(run.mcx, &run.root.rel(rel).lateral_relids);
    let path = crate::pathnode::create_resultscan_path(run, rel, &required_outer)?;
    add_path(run, rel, path);
    Ok(())
}
// set_values_pathlist (allpaths.c).
fn set_values_pathlist(run: &mut PlannerRun<'_>, rel: RelId) -> PgResult<()> {
    let required_outer = crate::relnode::relids_copy(run.mcx, &run.root.rel(rel).lateral_relids);
    let path = crate::pathnode::create_valuesscan_path(run, rel, &required_outer)?;
    add_path(run, rel, path);
    Ok(())
}

fn set_plain_rel_size(run: &mut PlannerRun<'_>, rel: RelId) -> PgResult<()> {
    crate::indxpath::check_index_predicates(run, rel)?;
    crate::costsize::set_baserel_size_estimates(run, rel)?;
    Ok(())
}

// set_rel_consider_parallel (allpaths.c), RTE_RELATION arm.
fn set_rel_consider_parallel(run: &mut PlannerRun<'_>, rel: RelId, rti: usize) -> PgResult<()> {
    debug_assert!(!run.root.rel(rel).consider_parallel);
    let rte = run.rte(rti);
    match rte.rtekind {
        RTEKind::RTE_RELATION => {
            if lsyscache::get_rel_persistence(rte.relid)? != b'p' as i8 {
                return Ok(());
            }
            debug_assert!(rte.tablesample.is_none());
        }
        RTEKind::RTE_FUNCTION | RTEKind::RTE_VALUES | RTEKind::RTE_SUBQUERY => {
            // C tests is_parallel_safe over the funcexprs/values_lists (and
            // security_barrier for subqueries); parallel plans are loud on
            // this lane, so the flag stays conservatively false.
            return Ok(());
        }
        RTEKind::RTE_CTE => {
            return Ok(()); // tuplestores aren't shared among workers
        }
        RTEKind::RTE_RESULT => {
            // RESULT RTEs, in themselves, are no problem.
        }
        other => panic!("set_rel_consider_parallel (allpaths.c): {other:?}; M2 lane"),
    }

    // is_parallel_safe over baserestrictinfo and the reltarget exprs.
    for i in 0..run.root.rel(rel).baserestrictinfo.len() {
        let rid = run.root.rel(rel).baserestrictinfo[i];
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        if !crate::is_parallel_safe_opt(run, Some(clause))? {
            return Ok(());
        }
    }
    let reltarget = run.rel_reltarget_id(rel);
    if !crate::is_parallel_safe_exprs(run, reltarget)? {
        return Ok(());
    }

    run.root.rel_mut(rel).consider_parallel = true;
    Ok(())
}

// set_plain_rel_pathlist (allpaths.c).
fn set_plain_rel_pathlist(run: &mut PlannerRun<'_>, rel: RelId) -> PgResult<()> {
    debug_assert!(run.root.rel(rel).lateral_relids.is_none());

    // A CurrentOfExpr qual forces the TID path: the executor handles no other.
    if crate::tidpath::create_tidscan_paths(run, rel)? {
        return Ok(());
    }

    // create_plain_partial_paths: M3 parallel lane (Gather is loud).
    let seqscan = crate::pathnode::create_seqscan_path(run, rel, 0)?;
    add_path(run, rel, seqscan);

    crate::indxpath::create_index_paths(run, rel)?;
    Ok(())
}
