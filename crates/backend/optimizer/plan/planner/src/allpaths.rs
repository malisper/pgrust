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
        if run.root.rel(brel).reloptkind == RELOPT_BASEREL
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
    if relation_excluded_by_constraints(run, rel) {
        set_dummy_rel_pathlist(run, rel)?;
        return Ok(());
    }
    let rte = run.rte(rti);
    assert!(!rte.inh, "set_append_rel_size (allpaths.c): M2 partition lane");
    match rte.rtekind {
        RTEKind::RTE_RELATION => {
            // Toast relations are plain heaps in C's set_plain_rel_size arm
            // (direct SELECT from pg_toast.* is legal).
            debug_assert!(
                rte.relkind == types_rel::RELKIND_RELATION
                    || rte.relkind == types_rel::RELKIND_TOASTVALUE,
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
            crate::cte::set_cte_pathlist(run, rel, rti)?;
        }
        RTEKind::RTE_RESULT => {
            unreachable!("RTE_RESULT is handled by query_planner's trivial arm");
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
    assert!(!rte.lateral, "set_subquery_pathlist (allpaths.c): LATERAL; M2 lateral lane");
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
    crate::subquery::subquery_planner(run, sub_parse, tuple_fraction, None)?;
    let idx = run.pop_root_to_rel_subroot();
    run.root.rel_mut(rel).subroot_idx = Some(idx);
    assert!(
        run.root.plan_params.is_empty(),
        "set_subquery_pathlist (allpaths.c): subplan_params isolation unported"
    );

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
            &c.1,
        )?;
        add_path(run, rel, id);
    }
    Ok(())
}

// relation_excluded_by_constraints (plancat.c): the unconditional
// constant-FALSE-or-NULL restriction scan; predicate proofs beyond it only
// run under constraint_exclusion=on (loud) or for otherrels (inh is loud).
fn relation_excluded_by_constraints(run: &mut PlannerRun<'_>, rel: RelId) -> bool {
    if run.root.rel(rel).baserestrictinfo.is_empty() {
        return false;
    }
    for i in 0..run.root.rel(rel).baserestrictinfo.len() {
        let rid = run.root.rel(rel).baserestrictinfo[i];
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        if let Some(c) = clause.as_const() {
            if c.constisnull || !c.constvalue.as_bool() {
                return true;
            }
        }
    }
    if crate::gucs::constraint_exclusion() == guc_tables::consts::CONSTRAINT_EXCLUSION_ON {
        panic!(
            "relation_excluded_by_constraints (plancat.c): constraint_exclusion=on \
             needs predicate_refuted_by; constraint-exclusion lane unported"
        );
    }
    false
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
    debug_assert!(!rte.inh);
    match rte.rtekind {
        RTEKind::RTE_RELATION => set_plain_rel_pathlist(run, rel)?,
        RTEKind::RTE_FUNCTION => set_function_pathlist(run, rel, rti)?,
        RTEKind::RTE_VALUES => set_values_pathlist(run, rel)?,
        RTEKind::RTE_SUBQUERY => {} // fully handled during set_rel_size
        RTEKind::RTE_CTE => {} // fully handled during set_rel_size
        other => panic!("set_rel_pathlist (allpaths.c): {other:?}; M2 scan lane"),
    }

    debug_assert!(run.root.rel(rel).partial_pathlist.is_empty());
    set_cheapest(run, rel)?;
    Ok(())
}

// set_function_pathlist (allpaths.c); the ORDINALITY pathkey leg is dead
// (funcordinality is loud in the parser).
fn set_function_pathlist(run: &mut PlannerRun<'_>, rel: RelId, rti: usize) -> PgResult<()> {
    debug_assert!(run.root.rel(rel).lateral_relids.is_none());
    debug_assert!(!run.rte(rti).funcordinality);
    let path = crate::pathnode::create_functionscan_path(run, rel)?;
    add_path(run, rel, path);
    Ok(())
}
// set_values_pathlist (allpaths.c); required_outer empty (LATERAL loud upstream).
fn set_values_pathlist(run: &mut PlannerRun<'_>, rel: RelId) -> PgResult<()> {
    debug_assert!(run.root.rel(rel).lateral_relids.is_none());
    let path = crate::pathnode::create_valuesscan_path(run, rel)?;
    add_path(run, rel, path);
    Ok(())
}

fn set_plain_rel_size(run: &mut PlannerRun<'_>, rel: RelId) -> PgResult<()> {
    crate::indxpath::check_index_predicates(run, rel);
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

    // create_tidscan_paths: TID quals can't exist on this lane (M2 tidscan
    // lane); create_plain_partial_paths: M3 parallel lane (Gather is loud).
    let seqscan = crate::pathnode::create_seqscan_path(run, rel, 0)?;
    add_path(run, rel, seqscan);

    crate::indxpath::create_index_paths(run, rel)?;
    Ok(())
}
