//! allpaths.c slice: make_one_rel over a single plain baserel (seqscan +
//! index paths); other RTE kinds and parallel paths are loud or dead.

use types_error::PgResult;
use types_nodes::parsenodes::RTEKind;
use types_pathnodes::{JoinlistNode, PathId, PathKey, RelId, RELOPT_BASEREL, RELOPT_OTHER_MEMBER_REL};

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
            if rte.relkind == types_rel::RELKIND_FOREIGN_TABLE {
                set_foreign_size(run, rel, rti)?;
            } else {
                assert!(
                    rte.relkind == types_rel::RELKIND_RELATION
                        || rte.relkind == types_rel::RELKIND_TOASTVALUE
                        || rte.relkind == types_rel::RELKIND_SEQUENCE
                        || rte.relkind == types_rel::RELKIND_MATVIEW,
                    "set_rel_size relkind {}",
                    rte.relkind
                );
                if rte.tablesample.is_some() {
                    set_tablesample_rel_size(run, rel, rti)?;
                } else {
                    set_plain_rel_size(run, rel)?;
                }
            }
        }
        RTEKind::RTE_FUNCTION => {
            crate::costsize::set_function_size_estimates(run, rel)?;
        }
        RTEKind::RTE_VALUES => {
            crate::costsize::set_values_size_estimates(run, rel)?;
        }
        RTEKind::RTE_TABLEFUNC => {
            crate::costsize::set_tablefunc_size_estimates(run, rel)?;
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
        RTEKind::RTE_NAMEDTUPLESTORE => {
            crate::costsize::set_namedtuplestore_size_estimates(run, rel)?;
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

// set_subquery_pathlist (allpaths.c).
fn set_subquery_pathlist(run: &mut PlannerRun<'_>, rel: RelId, rti: usize) -> PgResult<()> {
    let rte = run.rte(rti);
    let orig = rte.subquery.expect("RTE_SUBQUERY has a subquery");
    let required_outer = crate::relnode::relids_copy(run.mcx, &run.root.rel(rel).lateral_relids);

    // The copy keeps planning (and qual pushdown) off the RTE contents.
    let sub_parse =
        mcx::leak_in(mcx::alloc_in(run.mcx, crate::subselect::query_cells_copy(run.mcx, orig)?)?);
    let mut run_cond_attrs = types_nodes::Bitmapset::empty();
    crate::pushdown::pushdown_quals_into_subquery(
        run,
        rel,
        rti,
        rte,
        orig,
        sub_parse,
        &mut run_cond_attrs,
    )?;
    crate::pushdown::remove_unused_subquery_outputs(run, rel, sub_parse, run_cond_attrs)?;

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

    let outer_consider_parallel = run.root.rel(rel).consider_parallel;
    run.swap_with_rel_subroot(idx);
    type SubCand<'a> = (
        types_pathnodes::PathId,
        crate::pathnode::SubqueryScanInfo,
        mcx::PgVec<'a, crate::pathkeys::SubPathKeyDesc<'a>>,
        mcx::PgVec<'a, crate::pathkeys::SubTle<'a>>,
    );
    let mut candidates: mcx::PgVec<'_, SubCand<'_>> = mcx::PgVec::new_in(run.mcx);
    let mut partial_candidates: mcx::PgVec<'_, SubCand<'_>> = mcx::PgVec::new_in(run.mcx);
    {
        let final_rel = crate::planmain::fetch_final_rel(run);
        let paths =
            crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(final_rel).pathlist);
        for &sp in paths.iter() {
            candidates.push((
                sp,
                crate::prepunion::child_info(run, sp),
                crate::pathkeys::extract_subquery_pathkey_descs(run, sp),
                crate::pathkeys::extract_subquery_tlist(run, sp),
            ));
        }
        // allpaths.c: the sub-rel's partial paths become partial SubqueryScan
        // paths when the outer rel allows parallelism.
        if outer_consider_parallel
            && types_pathnodes::relids::relids_is_empty(&required_outer)
        {
            let partials = crate::relnode::pgvec_clone_shallow(
                run.mcx,
                &run.root.rel(final_rel).partial_pathlist,
            );
            for &sp in partials.iter() {
                partial_candidates.push((
                    sp,
                    crate::prepunion::child_info(run, sp),
                    crate::pathkeys::extract_subquery_pathkey_descs(run, sp),
                    crate::pathkeys::extract_subquery_tlist(run, sp),
                ));
            }
        }
    }
    run.swap_with_rel_subroot(idx);

    for c in candidates.iter() {
        let pathkeys = crate::pathkeys::convert_subquery_pathkeys(run, rel, &c.2, &c.3)?;
        let id = crate::pathnode::create_subqueryscan_path(
            run,
            rel,
            c.0,
            trivial_pathtarget,
            pathkeys,
            &required_outer,
            &c.1,
        )?;
        add_path(run, rel, id);
    }
    for c in partial_candidates.iter() {
        let pathkeys = crate::pathkeys::convert_subquery_pathkeys(run, rel, &c.2, &c.3)?;
        let id = crate::pathnode::create_subqueryscan_path(
            run,
            rel,
            c.0,
            trivial_pathtarget,
            pathkeys,
            &required_outer,
            &c.1,
        )?;
        crate::pathnode::add_partial_path(run, rel, id);
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
// constant-FALSE qual creates the identical Result plan. C parameterizes
// the dummy path by rel->lateral_relids so lateral refs in the tlist become
// nestloop Params.
pub fn set_dummy_rel_pathlist(run: &mut PlannerRun<'_>, rel: RelId) -> PgResult<()> {
    run.root.rel_reltarget_mut(rel).width = 0;
    let required_outer =
        crate::relnode::relids_copy(run.mcx, &run.root.rel(rel).lateral_relids);
    add_dummy_path(run, rel, &required_outer)
}

// The shared body of set_dummy_rel_pathlist (allpaths.c) and mark_dummy_rel
// (joinrels.c) — the latter leaves reltarget width alone; both parameterize
// by rel->lateral_relids (C create_append_path calls).
pub(crate) fn add_dummy_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    required_outer: &types_pathnodes::Relids<'mcx>,
) -> PgResult<()> {
    run.root.rel_mut(rel).rows = 0.0;
    run.root.rel_mut(rel).pathlist.clear();
    run.root.rel_mut(rel).partial_pathlist.clear();

    let param_info = crate::pathnode::get_appendrel_parampathinfo(run, rel, required_outer);
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
            param_info,
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
            RTEKind::RTE_RELATION => {
                if rte.relkind == types_rel::RELKIND_FOREIGN_TABLE {
                    set_foreign_pathlist(run, rel, rti)?;
                } else if rte.tablesample.is_some() {
                    set_tablesample_rel_pathlist(run, rel, rti)?;
                } else {
                    set_plain_rel_pathlist(run, rel)?;
                }
            }
            RTEKind::RTE_FUNCTION => set_function_pathlist(run, rel, rti)?,
            RTEKind::RTE_VALUES => set_values_pathlist(run, rel)?,
            RTEKind::RTE_TABLEFUNC => set_tablefunc_pathlist(run, rel)?,
            RTEKind::RTE_SUBQUERY => {} // fully handled during set_rel_size
            RTEKind::RTE_CTE => {} // fully handled during set_rel_size
            RTEKind::RTE_NAMEDTUPLESTORE => set_namedtuplestore_pathlist(run, rel)?,
            RTEKind::RTE_RESULT => set_result_pathlist(run, rel)?,
            other => panic!("set_rel_pathlist (allpaths.c): {other:?}; M2 scan lane"),
        }
    }

    // Gather partial paths for every baserel except an inheritance child
    // (the parent appendrel gathers) and the topmost scan/join rel (it waits
    // for the final tlist; see grouping_planner).
    if run.root.rel(rel).reloptkind == RELOPT_BASEREL
        && !crate::relnode::relids_equal(&run.root.rel(rel).relids, &run.root.all_query_rels)
    {
        generate_useful_gather_paths(run, rel, false)?;
    }

    set_cheapest(run, rel)?;
    Ok(())
}

// set_append_rel_size (allpaths.c): size each live child, then aggregate.
// Child ECs stay dead (no ECs exist on this lane).
fn set_append_rel_size(run: &mut PlannerRun<'_>, rel: RelId, rti: usize) -> PgResult<()> {
    let mcx = run.mcx;
    debug_assert!(
        run.root.rel(rel).reloptkind == RELOPT_BASEREL
            || run.root.rel(rel).reloptkind == types_pathnodes::RELOPT_OTHER_MEMBER_REL
    );

    if crate::gucs::enable_partitionwise_join()
        && run.root.rel(rel).reloptkind == RELOPT_BASEREL
        && run.rte(rti).relkind == types_rel::RELKIND_PARTITIONED_TABLE
    {
        let wholerow_idx = (0 - run.root.rel(rel).min_attr) as usize;
        if crate::relnode::relids_is_empty(&run.root.rel(rel).attr_needed[wholerow_idx]) {
            run.root.rel_mut(rel).consider_partitionwise_join = true;
        }
    }

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

        let appinfo = run.root.append_rel_array[child_rti as usize]
            .clone()
            .expect("child AppendRelInfo");

        // Child joininfo = parent joininfo translated, skipping quals from
        // above outer joins that can null this rel (C's nullingrels-on-non-Var
        // translation restriction).
        {
            let parent_joininfo =
                crate::relnode::pgvec_clone_shallow(mcx, &run.root.rel(rel).joininfo);
            let nulling = crate::relnode::relids_copy(mcx, &run.root.rel(rel).nulling_relids);
            let mut childrinfos: mcx::PgVec<'_, types_pathnodes::RinfoId> =
                mcx::PgVec::new_in(mcx);
            for &rid in parent_joininfo.iter() {
                if !crate::relnode::relids_overlap(
                    &run.root.rinfo(rid).clause_relids,
                    &nulling,
                ) {
                    childrinfos.push(crate::inherit::adjust_child_rinfo(
                        run,
                        rid,
                        core::slice::from_ref(&appinfo),
                    )?);
                }
            }
            run.root.rel_mut(childrel).joininfo = childrinfos;
        }

        // Child reltarget = parent reltarget translated.
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

        if run.root.rel(rel).has_eclass_joins || crate::pathkeys::has_useful_pathkeys(run, rel) {
            crate::equivclass::add_child_rel_equivalences(run, &appinfo, rel, childrel)?;
        }
        let parent_has_ec_joins = run.root.rel(rel).has_eclass_joins;
        run.root.rel_mut(childrel).has_eclass_joins = parent_has_ec_joins;

        // C abuses the flag on unpartitioned children to mark them valid
        // per-partition join inputs (tlist set up above).
        if run.root.rel(rel).consider_partitionwise_join {
            run.root.rel_mut(childrel).consider_partitionwise_join = true;
        }

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
    use types_pathnodes::relids::{relids_copy, relids_equal, relids_is_empty};
    let mcx = run.mcx;
    let mut subpaths: mcx::PgVec<'_, types_pathnodes::PathId> = mcx::PgVec::new_in(mcx);
    let mut subpaths_valid = true;
    let mut startup_subpaths: mcx::PgVec<'_, types_pathnodes::PathId> = mcx::PgVec::new_in(mcx);
    let mut startup_valid = run.root.rel(rel).consider_startup;
    let mut all_child_pathkeys: mcx::PgVec<'_, mcx::PgVec<'_, PathKey>> =
        mcx::PgVec::new_in(mcx);
    let mut all_child_outers: mcx::PgVec<'_, types_pathnodes::Relids<'_>> =
        mcx::PgVec::new_in(mcx);
    let mut partial_subpaths: mcx::PgVec<'_, types_pathnodes::PathId> = mcx::PgVec::new_in(mcx);
    let mut partial_subpaths_valid = true;
    let mut pa_partial_subpaths: mcx::PgVec<'_, types_pathnodes::PathId> =
        mcx::PgVec::new_in(mcx);
    let mut pa_nonpartial_subpaths: mcx::PgVec<'_, types_pathnodes::PathId> =
        mcx::PgVec::new_in(mcx);
    let mut pa_subpaths_valid =
        crate::gucs::enable_parallel_append() && run.root.rel(rel).consider_parallel;
    for &childrel in live_childrels {
        let cheapest_total = run.root.rel(childrel).cheapest_total_path;
        match cheapest_total {
            Some(p) if run.root.path(p).base().param_info.is_none() => {
                accumulate_append_subpath(&run.root, p, &mut subpaths, None);
            }
            // A child with only parameterized paths: no unparameterized
            // Append, but the per-parameterization loop below still applies.
            _ => subpaths_valid = false,
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
                    accumulate_append_subpath(&run.root, chosen, &mut startup_subpaths, None);
                }
                None => startup_valid = false,
            }
        }
        let cheapest_partial_path = run.root.rel(childrel).partial_pathlist.first().copied();
        match cheapest_partial_path {
            Some(p) => accumulate_append_subpath(&run.root, p, &mut partial_subpaths, None),
            None => partial_subpaths_valid = false,
        }
        if pa_subpaths_valid {
            let pl = crate::relnode::pgvec_clone_shallow(mcx, &run.root.rel(childrel).pathlist);
            let nppath = crate::pathkeys::get_cheapest_parallel_safe_total_inner(run, &pl);
            match (cheapest_partial_path, nppath) {
                (None, None) => pa_subpaths_valid = false,
                (Some(pp), np)
                    if np.is_none()
                        || run.root.path(pp).base().total_cost
                            < run.root.path(np.unwrap()).base().total_cost =>
                {
                    accumulate_append_subpath(
                        &run.root,
                        pp,
                        &mut pa_partial_subpaths,
                        Some(&mut pa_nonpartial_subpaths),
                    );
                }
                (_, np) => {
                    // Only a non-partial path, or a single backend running it
                    // beats the partial path's all-workers estimate.
                    accumulate_append_subpath(
                        &run.root,
                        np.expect("non-partial parallel-safe path"),
                        &mut pa_nonpartial_subpaths,
                        None,
                    );
                }
            }
        }
        for pi in 0..run.root.rel(childrel).pathlist.len() {
            let childpath = run.root.rel(childrel).pathlist[pi];
            let childkeys = &run.root.path(childpath).base().pathkeys;
            if !childkeys.is_empty() {
                let found = all_child_pathkeys.iter().any(|existing| {
                    crate::pathkeys::compare_pathkeys(existing, childkeys)
                        == crate::pathkeys::PathKeysComparison::Equal
                });
                if !found {
                    all_child_pathkeys
                        .push(crate::relnode::pgvec_clone_shallow(mcx, childkeys));
                }
            }
            let childouter =
                crate::pathnode::path_req_outer(run.root.path(childpath).base());
            if !relids_is_empty(childouter) {
                let found =
                    all_child_outers.iter().any(|existing| relids_equal(existing, childouter));
                if !found {
                    all_child_outers.push(relids_copy(mcx, childouter));
                }
            }
        }
    }

    if subpaths_valid {
        let pid = crate::pathnode::create_append_path(
            run,
            rel,
            subpaths,
            mcx::PgVec::new_in(mcx),
            mcx::PgVec::new_in(mcx),
            &crate::relnode::RELIDS_UNSET,
            0,
            false,
            -1.0,
        )?;
        add_path(run, rel, pid);
        if startup_valid {
            let pid = crate::pathnode::create_append_path(
                run,
                rel,
                startup_subpaths,
                mcx::PgVec::new_in(mcx),
                mcx::PgVec::new_in(mcx),
                &crate::relnode::RELIDS_UNSET,
                0,
                false,
                -1.0,
            )?;
            add_path(run, rel, pid);
        }
    }

    // Unordered, unparameterized append of the child partial paths;
    // parallel-aware when permitted.
    let mut partial_rows = -1.0;
    if partial_subpaths_valid && !partial_subpaths.is_empty() {
        let mut parallel_workers = 0;
        for &sp in partial_subpaths.iter() {
            parallel_workers = parallel_workers.max(run.root.path(sp).base().parallel_workers);
        }
        debug_assert!(parallel_workers > 0);
        let pa_enabled = crate::gucs::enable_parallel_append();
        if pa_enabled {
            // At least log2(# children) workers: spread across the children.
            parallel_workers =
                parallel_workers.max((live_childrels.len() as u32).ilog2() as i32 + 1);
            parallel_workers =
                parallel_workers.min(crate::gucs::max_parallel_workers_per_gather());
        }
        debug_assert!(parallel_workers > 0);
        let pid = crate::pathnode::create_append_path(
            run,
            rel,
            mcx::PgVec::new_in(mcx),
            partial_subpaths,
            mcx::PgVec::new_in(mcx),
            &crate::relnode::RELIDS_UNSET,
            parallel_workers,
            pa_enabled,
            -1.0,
        )?;
        partial_rows = run.root.path(pid).base().rows;
        crate::pathnode::add_partial_path(run, rel, pid);
    }

    // Parallel-aware append mixing partial and non-partial subpaths: only
    // worthwhile when some child has a substantially cheaper non-partial path.
    if pa_subpaths_valid && !pa_nonpartial_subpaths.is_empty() {
        let mut parallel_workers = 0;
        for &sp in pa_partial_subpaths.iter() {
            parallel_workers = parallel_workers.max(run.root.path(sp).base().parallel_workers);
        }
        parallel_workers =
            parallel_workers.max((live_childrels.len() as u32).ilog2() as i32 + 1);
        parallel_workers = parallel_workers.min(crate::gucs::max_parallel_workers_per_gather());
        debug_assert!(parallel_workers > 0);
        let pid = crate::pathnode::create_append_path(
            run,
            rel,
            pa_nonpartial_subpaths,
            pa_partial_subpaths,
            mcx::PgVec::new_in(mcx),
            &crate::relnode::RELIDS_UNSET,
            parallel_workers,
            true,
            partial_rows,
        )?;
        crate::pathnode::add_partial_path(run, rel, pid);
    }

    if subpaths_valid && !all_child_pathkeys.is_empty() {
        generate_orderedappend_paths(run, rel, live_childrels, &all_child_pathkeys)?;
    }

    // Build unordered, parameterized Append paths for each parameterization
    // seen among the child rels (lateral references force these).
    for oi in 0..all_child_outers.len() {
        let required_outer = relids_copy(mcx, &all_child_outers[oi]);
        let mut par_subpaths: mcx::PgVec<'_, types_pathnodes::PathId> = mcx::PgVec::new_in(mcx);
        let mut par_valid = true;
        for &childrel in live_childrels {
            if run.root.rel(childrel).pathlist.is_empty() {
                par_valid = false;
                break;
            }
            match get_cheapest_parameterized_child_path(run, childrel, &required_outer)? {
                Some(subpath) => {
                    accumulate_append_subpath(&run.root, subpath, &mut par_subpaths, None);
                }
                None => {
                    par_valid = false;
                    break;
                }
            }
        }
        if par_valid {
            let pid = crate::pathnode::create_append_path(
                run,
                rel,
                par_subpaths,
                mcx::PgVec::new_in(mcx),
                mcx::PgVec::new_in(mcx),
                &required_outer,
                0,
                false,
                -1.0,
            )?;
            add_path(run, rel, pid);
        }
    }
    Ok(())
}

// get_cheapest_parameterized_child_path (allpaths.c).
fn get_cheapest_parameterized_child_path<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    required_outer: &types_pathnodes::Relids<'mcx>,
) -> PgResult<Option<types_pathnodes::PathId>> {
    use types_pathnodes::relids::{relids_equal, relids_is_subset};
    let mcx = run.mcx;
    let paths = crate::relnode::pgvec_clone_shallow(mcx, &run.root.rel(rel).pathlist);
    let cheapest = crate::pathkeys::get_cheapest_path_for_pathkeys(
        run,
        &paths,
        &[],
        required_outer,
        crate::pathnode::CostSelector::Total,
        false,
    )
    .expect("cheapest path with no more than the needed parameterization");
    if relids_equal(
        crate::pathnode::path_req_outer(run.root.path(cheapest).base()),
        required_outer,
    ) {
        return Ok(Some(cheapest));
    }
    // No exact match: reparameterize candidates and keep the cheapest.
    let mut best: Option<types_pathnodes::PathId> = None;
    for &pid in paths.iter() {
        {
            let p = run.root.path(pid).base();
            if !relids_is_subset(crate::pathnode::path_req_outer(p), required_outer) {
                continue;
            }
            if let Some(b) = best {
                if crate::pathnode::compare_path_costs(
                    run.root.path(b).base(),
                    p,
                    crate::pathnode::CostSelector::Total,
                ) <= 0
                {
                    continue;
                }
            }
        }
        let candidate = if relids_equal(
            crate::pathnode::path_req_outer(run.root.path(pid).base()),
            required_outer,
        ) {
            pid
        } else {
            match crate::pathnode::reparameterize_path(run, pid, required_outer, 1.0)? {
                Some(np) => {
                    debug_assert!(relids_equal(
                        crate::pathnode::path_req_outer(run.root.path(np).base()),
                        required_outer
                    ));
                    if let Some(b) = best {
                        if crate::pathnode::compare_path_costs(
                            run.root.path(b).base(),
                            run.root.path(np).base(),
                            crate::pathnode::CostSelector::Total,
                        ) <= 0
                        {
                            continue;
                        }
                    }
                    np
                }
                None => continue,
            }
        };
        best = Some(candidate);
    }
    Ok(best)
}

// generate_orderedappend_paths (allpaths.c): one ordered path set per child
// ordering; plain Append when the ordering matches the (fwd/rev) partition
// order, MergeAppend otherwise. No parameterized ordered paths, as C.
fn generate_orderedappend_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    live_childrels: &[RelId],
    all_child_pathkeys: &[mcx::PgVec<'mcx, PathKey>],
) -> PgResult<()> {
    let mcx = run.mcx;
    let mut partition_pathkeys: mcx::PgVec<'mcx, PathKey> = mcx::PgVec::new_in(mcx);
    let mut partition_pathkeys_desc: mcx::PgVec<'mcx, PathKey> = mcx::PgVec::new_in(mcx);
    let mut partition_pathkeys_partial = true;
    let mut partition_pathkeys_desc_partial = true;

    let is_simple_rel = matches!(
        run.root.rel(rel).reloptkind,
        RELOPT_BASEREL | RELOPT_OTHER_MEMBER_REL
    );
    let ordered = run.root.rel(rel).part_scheme.is_some()
        && is_simple_rel
        && match &run.root.rel(rel).boundinfo {
            Some(bi) => {
                crate::partprune::partitions_are_ordered(bi, &run.root.rel(rel).live_parts)
            }
            None => false,
        };
    if ordered {
        (partition_pathkeys, partition_pathkeys_partial) =
            crate::pathkeys::build_partition_pathkeys(run, rel, false)?;
        (partition_pathkeys_desc, partition_pathkeys_desc_partial) =
            crate::pathkeys::build_partition_pathkeys(run, rel, true)?;
        // Partition keys that are a subset of the query pathkeys still help
        // (children supply the lower-order ordering), so no truncation here.
    }

    for pathkeys in all_child_pathkeys {
        let mut startup_subpaths: mcx::PgVec<'mcx, PathId> = mcx::PgVec::new_in(mcx);
        let mut total_subpaths: mcx::PgVec<'mcx, PathId> = mcx::PgVec::new_in(mcx);
        let mut fractional_subpaths: mcx::PgVec<'mcx, PathId> = mcx::PgVec::new_in(mcx);
        let mut startup_neq_total = false;

        let mut match_partition_order =
            crate::pathkeys::pathkeys_contained_in(pathkeys, &partition_pathkeys)
                || (!partition_pathkeys_partial
                    && crate::pathkeys::pathkeys_contained_in(&partition_pathkeys, pathkeys));
        let match_partition_order_desc = !match_partition_order
            && (crate::pathkeys::pathkeys_contained_in(pathkeys, &partition_pathkeys_desc)
                || (!partition_pathkeys_desc_partial
                    && crate::pathkeys::pathkeys_contained_in(
                        &partition_pathkeys_desc,
                        pathkeys,
                    )));

        // Reversed partition order: build the subpath lists back-to-front.
        let (first_index, end_index, direction) = if match_partition_order_desc {
            match_partition_order = true;
            (live_childrels.len() as i32 - 1, -1i32, -1i32)
        } else {
            (0i32, live_childrels.len() as i32, 1i32)
        };

        let mut i = first_index;
        while i != end_index {
            let childrel = live_childrels[i as usize];
            let child_paths =
                crate::relnode::pgvec_clone_shallow(mcx, &run.root.rel(childrel).pathlist);
            let mut cheapest_startup = crate::pathkeys::get_cheapest_path_for_pathkeys(
                run,
                &child_paths,
                pathkeys,
                &crate::relnode::RELIDS_UNSET,
                crate::pathnode::CostSelector::Startup,
                false,
            );
            let mut cheapest_total = crate::pathkeys::get_cheapest_path_for_pathkeys(
                run,
                &child_paths,
                pathkeys,
                &crate::relnode::RELIDS_UNSET,
                crate::pathnode::CostSelector::Total,
                false,
            );
            if cheapest_startup.is_none() || cheapest_total.is_none() {
                let fallback = run
                    .root
                    .rel(childrel)
                    .cheapest_total_path
                    .expect("append child has a cheapest total path");
                debug_assert!(run.root.path(fallback).base().param_info.is_none());
                cheapest_startup = Some(fallback);
                cheapest_total = Some(fallback);
            }
            let cheapest_startup = cheapest_startup.unwrap();
            let cheapest_total = cheapest_total.unwrap();

            let cheapest_fractional = if run.root.tuple_fraction > 0.0 {
                let total_rows = run.root.path(cheapest_total).base().rows;
                debug_assert!(total_rows > 0.0);
                let mut path_fraction = run.root.tuple_fraction;
                if path_fraction >= 1.0 {
                    path_fraction /= total_rows;
                }
                Some(
                    crate::pathkeys::get_cheapest_fractional_path_for_pathkeys(
                        run,
                        &child_paths,
                        pathkeys,
                        path_fraction,
                    )
                    .unwrap_or(cheapest_total),
                )
            } else {
                None
            };

            if cheapest_startup != cheapest_total {
                startup_neq_total = true;
            }

            if match_partition_order {
                startup_subpaths
                    .push(get_singleton_append_subpath(&run.root, cheapest_startup));
                total_subpaths.push(get_singleton_append_subpath(&run.root, cheapest_total));
                if let Some(f) = cheapest_fractional {
                    fractional_subpaths.push(get_singleton_append_subpath(&run.root, f));
                }
            } else {
                accumulate_append_subpath(&run.root, cheapest_startup, &mut startup_subpaths, None);
                accumulate_append_subpath(&run.root, cheapest_total, &mut total_subpaths, None);
                if let Some(f) = cheapest_fractional {
                    accumulate_append_subpath(&run.root, f, &mut fractional_subpaths, None);
                }
            }
            i += direction;
        }

        if match_partition_order {
            let pid = crate::pathnode::create_append_path(
                run,
                rel,
                startup_subpaths,
                mcx::PgVec::new_in(mcx),
                crate::relnode::pgvec_clone_shallow(mcx, pathkeys),
                &crate::relnode::RELIDS_UNSET,
                0,
                false,
                -1.0,
            )?;
            add_path(run, rel, pid);
            if startup_neq_total {
                let pid = crate::pathnode::create_append_path(
                    run,
                    rel,
                    total_subpaths,
                    mcx::PgVec::new_in(mcx),
                    crate::relnode::pgvec_clone_shallow(mcx, pathkeys),
                    &crate::relnode::RELIDS_UNSET,
                    0,
                    false,
                    -1.0,
                )?;
                add_path(run, rel, pid);
            }
            if !fractional_subpaths.is_empty() {
                let pid = crate::pathnode::create_append_path(
                    run,
                    rel,
                    fractional_subpaths,
                    mcx::PgVec::new_in(mcx),
                    crate::relnode::pgvec_clone_shallow(mcx, pathkeys),
                    &crate::relnode::RELIDS_UNSET,
                    0,
                    false,
                    -1.0,
                )?;
                add_path(run, rel, pid);
            }
        } else {
            let pid = crate::pathnode::create_merge_append_path(
                run,
                rel,
                startup_subpaths,
                crate::relnode::pgvec_clone_shallow(mcx, pathkeys),
            )?;
            add_path(run, rel, pid);
            if startup_neq_total {
                let pid = crate::pathnode::create_merge_append_path(
                    run,
                    rel,
                    total_subpaths,
                    crate::relnode::pgvec_clone_shallow(mcx, pathkeys),
                )?;
                add_path(run, rel, pid);
            }
            if !fractional_subpaths.is_empty() {
                let pid = crate::pathnode::create_merge_append_path(
                    run,
                    rel,
                    fractional_subpaths,
                    crate::relnode::pgvec_clone_shallow(mcx, pathkeys),
                )?;
                add_path(run, rel, pid);
            }
        }
    }
    Ok(())
}

// accumulate_append_subpath (allpaths.c): flatten nested Appends and
// MergeAppends (multi-level partitioning); a parallel-aware child Append with
// non-partial subpaths splits into (partial -> subpaths, non-partial ->
// special_subpaths) when the caller can accept that.
fn accumulate_append_subpath(
    root: &types_pathnodes::PlannerInfo<'_>,
    path: types_pathnodes::PathId,
    subpaths: &mut mcx::PgVec<'_, types_pathnodes::PathId>,
    special_subpaths: Option<&mut mcx::PgVec<'_, types_pathnodes::PathId>>,
) {
    match root.path(path) {
        types_pathnodes::PathNode::AppendPath(a)
            if !a.path.parallel_aware || a.first_partial_path == 0 =>
        {
            for &sp in a.subpaths.iter() {
                subpaths.push(sp);
            }
        }
        types_pathnodes::PathNode::AppendPath(a) if special_subpaths.is_some() => {
            let special = special_subpaths.expect("checked above");
            for &sp in a.subpaths.iter().skip(a.first_partial_path as usize) {
                subpaths.push(sp);
            }
            for &sp in a.subpaths.iter().take(a.first_partial_path as usize) {
                special.push(sp);
            }
        }
        types_pathnodes::PathNode::MergeAppendPath(m) => {
            for &sp in m.subpaths.iter() {
                subpaths.push(sp);
            }
        }
        _ => subpaths.push(path),
    }
}

// get_singleton_append_subpath (allpaths.c): strip single-child (non-
// parallel-aware) Appends/MergeAppends.
fn get_singleton_append_subpath(
    root: &types_pathnodes::PlannerInfo<'_>,
    path: types_pathnodes::PathId,
) -> types_pathnodes::PathId {
    debug_assert!(!root.path(path).base().parallel_aware);
    match root.path(path) {
        types_pathnodes::PathNode::AppendPath(a) if a.subpaths.len() == 1 => a.subpaths[0],
        types_pathnodes::PathNode::MergeAppendPath(m) if m.subpaths.len() == 1 => m.subpaths[0],
        _ => path,
    }
}

// set_function_pathlist (allpaths.c).
fn set_function_pathlist(run: &mut PlannerRun<'_>, rel: RelId, rti: usize) -> PgResult<()> {
    let mut pathkeys = mcx::PgVec::new_in(run.mcx);
    if run.rte(rti).funcordinality {
        // Ordered by the ordinal (last) column when some EC already cares.
        let ordattno = run.root.rel(rel).max_attr;
        let mut ordvar = None;
        for &eid in run.root.rel_reltarget(rel).exprs.iter() {
            let node = *run.root.expr_node(eid);
            if let Some(v) = node.as_var() {
                if v.varattno == ordattno && v.varno == rti as i32 && v.varlevelsup == 0 {
                    ordvar = Some(node);
                    break;
                }
            }
        }
        if let Some(var) = ordvar {
            const INT8_LESS_OPERATOR: u32 = 412;
            let fn_relids =
                types_pathnodes::relids::relids_copy(run.mcx, &run.root.rel(rel).relids);
            pathkeys = crate::pathkeys::build_expression_pathkey(
                run, var, INT8_LESS_OPERATOR, &fn_relids, false,
            )?;
        }
    }
    let required_outer = crate::relnode::relids_copy(run.mcx, &run.root.rel(rel).lateral_relids);
    let path = crate::pathnode::create_functionscan_path(run, rel, pathkeys, &required_outer)?;
    add_path(run, rel, path);
    Ok(())
}
// set_result_pathlist (allpaths.c): one Result path, parameterized only by
// lateral refs (join quals never push into a Result scan).
// set_namedtuplestore_pathlist (allpaths.c); sizing ran in set_rel_size (the
// RTE_RESULT split here), required_outer empty on this lane.
fn set_namedtuplestore_pathlist(run: &mut PlannerRun<'_>, rel: RelId) -> PgResult<()> {
    debug_assert!(crate::relnode::relids_is_unset(&run.root.rel(rel).lateral_relids));
    let path = crate::pathnode::create_namedtuplestorescan_path(run, rel)?;
    add_path(run, rel, path);
    Ok(())
}

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
// set_tablefunc_pathlist (allpaths.c).
fn set_tablefunc_pathlist(run: &mut PlannerRun<'_>, rel: RelId) -> PgResult<()> {
    let required_outer = crate::relnode::relids_copy(run.mcx, &run.root.rel(rel).lateral_relids);
    let path = crate::pathnode::create_tablefuncscan_path(run, rel, &required_outer)?;
    add_path(run, rel, path);
    Ok(())
}

fn set_plain_rel_size(run: &mut PlannerRun<'_>, rel: RelId) -> PgResult<()> {
    crate::indxpath::check_index_predicates(run, rel)?;
    crate::costsize::set_baserel_size_estimates(run, rel)?;
    Ok(())
}

// set_foreign_size (allpaths.c): the FDW adjusts the default estimates, but
// may not zero rows or leave tuples insane relative to rows (reltuples -1).
fn set_foreign_size(run: &mut PlannerRun<'_>, rel: RelId, rti: usize) -> PgResult<()> {
    crate::costsize::set_foreign_size_estimates(run, rel)?;
    let relid = run.rte(rti).relid;
    let kind = run.root.rel(rel).fdwroutine.expect("foreign rel has fdwroutine");
    (crate::fdwplan::fdw_plan_routine(kind).get_foreign_rel_size)(run, rel, relid)?;
    let rows = crate::costsize::clamp_row_est(run.root.rel(rel).rows);
    let r = run.root.rel_mut(rel);
    r.rows = rows;
    r.tuples = r.tuples.max(rows);
    Ok(())
}

// set_foreign_pathlist (allpaths.c).
fn set_foreign_pathlist(run: &mut PlannerRun<'_>, rel: RelId, rti: usize) -> PgResult<()> {
    let relid = run.rte(rti).relid;
    let kind = run.root.rel(rel).fdwroutine.expect("foreign rel has fdwroutine");
    (crate::fdwplan::fdw_plan_routine(kind).get_foreign_paths)(run, rel, relid)?;
    Ok(())
}

// set_tablesample_rel_size (allpaths.c): the TSM's size estimate overwrites
// the whole-rel pages/tuples (SampleScan is the only path considered).
fn set_tablesample_rel_size(run: &mut PlannerRun<'_>, rel: RelId, rti: usize) -> PgResult<()> {
    crate::indxpath::check_index_predicates(run, rel)?;
    let tsc = run
        .rte(rti)
        .tablesample
        .expect("sampled rel has a tablesample clause")
        .as_table_sample_clause()
        .expect("tablesample is a TableSampleClause");
    let tsm = ::tablesample::Tsm::get(run.mcx, tsc.tsmhandler)?;
    let (pages, tuples) = {
        let r = run.root.rel(rel);
        let (spc_random_page_cost, _) =
            crate::costsize::get_tablespace_page_costs(r.reltablespace);
        tsm.sample_scan_get_sample_size(
            run.mcx,
            &tsc.args,
            r.pages,
            r.tuples,
            spc_random_page_cost,
        )?
    };
    {
        let r = run.root.rel_mut(rel);
        r.pages = pages;
        r.tuples = tuples;
    }
    crate::costsize::set_baserel_size_estimates(run, rel)?;
    Ok(())
}

// set_tablesample_rel_pathlist (allpaths.c).
fn set_tablesample_rel_pathlist(run: &mut PlannerRun<'_>, rel: RelId, rti: usize) -> PgResult<()> {
    let required_outer = crate::relnode::relids_copy(run.mcx, &run.root.rel(rel).lateral_relids);
    let mut path = crate::pathnode::create_samplescan_path(run, rel, &required_outer)?;

    // A TSM without repeatable_across_scans must not be scanned twice: if the
    // query could join (not a singleton rel set) or we are in a subquery
    // (outer join unknowable), wrap the SampleScan in Material. The rel-count
    // tests run first — GetTsmRoutine is the expensive check (as C).
    // bms_membership != BMS_SINGLETON.
    let join_possible = run.root.query_level > 1
        || crate::relnode::relids_num_members(&run.root.all_query_rels) != 1;
    if join_possible {
        let tsc = run
            .rte(rti)
            .tablesample
            .expect("sampled rel has a tablesample clause")
            .as_table_sample_clause()
            .expect("tablesample is a TableSampleClause");
        let tsm = ::tablesample::Tsm::get(run.mcx, tsc.tsmhandler)?;
        if !tsm.repeatable_across_scans() {
            path = crate::pathnode::create_material_path(run, rel, path);
        }
    }

    add_path(run, rel, path);
    Ok(())
}

// set_rel_consider_parallel (allpaths.c).
fn set_rel_consider_parallel(run: &mut PlannerRun<'_>, rel: RelId, rti: usize) -> PgResult<()> {
    debug_assert!(!run.root.rel(rel).consider_parallel);
    let rte = run.rte(rti);
    match rte.rtekind {
        RTEKind::RTE_RELATION => {
            // Workers can't read the leader's temp buffers.
            if lsyscache::get_rel_persistence(rte.relid)? != b'p' as i8 {
                return Ok(());
            }
            if let Some(ts) = rte.tablesample {
                let tsc = ts.as_table_sample_clause().expect("TableSampleClause");
                const PROPARALLEL_SAFE: i8 = b's' as i8;
                if lsyscache::func_parallel(tsc.tsmhandler)? != PROPARALLEL_SAFE {
                    return Ok(());
                }
                for arg in tsc.args.iter() {
                    if !crate::is_parallel_safe_opt(run, Some(arg))? {
                        return Ok(());
                    }
                }
            }
            // Foreign tables are parallel-unsafe by construction: the
            // parallel-FDW ABI was deleted (2026-07-20 FDW-ABI ruling — UX
            // frozen, ABI ours; the worker lane was never implemented, so
            // IsForeignScanParallelSafe had nothing to admit into). A future
            // parallel foreign scan arrives as a morsel-native source.
            if rte.relkind == types_rel::RELKIND_FOREIGN_TABLE {
                return Ok(());
            }
        }
        RTEKind::RTE_SUBQUERY => {
            // LIMIT/OFFSET in a subquery gives nondeterministic row order
            // across workers.
            let subquery = rte.subquery.expect("RTE_SUBQUERY has a subquery");
            if crate::grouping::limit_needed(subquery) {
                return Ok(());
            }
        }
        RTEKind::RTE_FUNCTION => {
            let node = types_nodes::Node::mk_list(run.mcx, rte.functions.clone_in(run.mcx)?)?;
            if !crate::is_parallel_safe_opt(run, Some(node))? {
                return Ok(());
            }
        }
        RTEKind::RTE_VALUES => {
            let node = types_nodes::Node::mk_list(run.mcx, rte.values_lists.clone_in(run.mcx)?)?;
            if !crate::is_parallel_safe_opt(run, Some(node))? {
                return Ok(());
            }
        }
        RTEKind::RTE_TABLEFUNC => {
            return Ok(()); // not parallel safe
        }
        RTEKind::RTE_CTE | RTEKind::RTE_NAMEDTUPLESTORE => {
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
    // Join clauses never push into a seqscan, but lateral refs in the tlist
    // (a PHV evaluated at the scan) still force parameterization.
    let required_outer = crate::relnode::relids_copy(run.mcx, &run.root.rel(rel).lateral_relids);

    // A CurrentOfExpr qual forces the TID path: the executor handles no other.
    if crate::tidpath::create_tidscan_paths(run, rel)? {
        return Ok(());
    }

    let seqscan = crate::pathnode::create_seqscan_path(run, rel, &required_outer, 0)?;
    add_path(run, rel, seqscan);

    create_pgrcolumnar_sorted_paths(run, rel, &required_outer)?;

    // C: partial paths only when required_outer == NULL.
    if run.root.rel(rel).consider_parallel
        && crate::relnode::relids_is_empty(&required_outer)
    {
        create_plain_partial_paths(run, rel)?;
    }

    crate::indxpath::create_index_paths(run, rel)?;
    Ok(())
}

// pgrcolumnar v5 sorted-column pathkeys (sorted-groupexec inc2): one extra
// serial seqscan path per useful ordering of the footer-proven sorted
// columns. Each claimed column is independently non-decreasing over the
// whole part, so ANY permutation of the claimed columns is a valid
// lexicographic ordering; candidates lead with each column in turn so a
// GROUP BY/ORDER BY on any single proven column can match. Exprs come from
// the rel's reltarget (canonical Vars; an unreferenced column can serve no
// pathkey), ECs are looked up create_it=false (the index-path discipline),
// and useless tails are truncated — no useful prefix means no extra path.
// Partial paths claim nothing (per-worker RG dispatch order is not proven).
fn create_pgrcolumnar_sorted_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    required_outer: &types_pathnodes::Relids<'mcx>,
) -> PgResult<()> {
    let nsorted = run.root.rel(rel).pgrcolumnar_sorted_attnos.len();
    if nsorted == 0 {
        return Ok(());
    }
    let relid_index = run.root.rel(rel).relid;
    let mut vars: Vec<(i16, types_nodes::node_tree::Node<'mcx>)> = Vec::with_capacity(nsorted);
    for i in 0..nsorted {
        let attno = run.root.rel(rel).pgrcolumnar_sorted_attnos[i];
        let found = run.root.rel_reltarget(rel).exprs.iter().find_map(|&id| {
            let n = *run.root.expr_node(id);
            let v = n.as_var()?;
            (v.varno == relid_index as i32 && v.varlevelsup == 0 && v.varattno == attno)
                .then_some(n)
        });
        if let Some(n) = found {
            vars.push((attno, n));
        }
    }
    let mut added: Vec<mcx::PgVec<'mcx, types_pathnodes::PathKey>> = Vec::new();
    for lead in 0..vars.len() {
        let relids = crate::relnode::relids_copy(run.mcx, &run.root.rel(rel).relids);
        let mut cand: mcx::PgVec<'mcx, types_pathnodes::PathKey> = mcx::PgVec::new_in(run.mcx);
        for j in std::iter::once(lead).chain((0..vars.len()).filter(|&j| j != lead)) {
            let (_, expr) = vars[j];
            let Some(pk) = crate::pathkeys::make_pathkey_from_sortinfo_existing(
                run, expr, &relids,
            )?
            else {
                break;
            };
            if !crate::pathkeys::pathkey_is_redundant(run, pk, &cand) {
                cand.push(pk);
            }
        }
        let cand = crate::pathkeys::truncate_useless_pathkeys(run, rel, &cand)?;
        if cand.is_empty() || added.iter().any(|a| a.as_slice() == cand.as_slice()) {
            continue;
        }
        let id = crate::pathnode::create_seqscan_path(run, rel, required_outer, 0)?;
        run.root.path_mut(id).base_mut().pathkeys =
            types_pathnodes::relids::pgvec_clone_shallow(run.mcx, &cand);
        add_path(run, rel, id);
        added.push(cand);
    }
    Ok(())
}

// create_plain_partial_paths (allpaths.c).
fn create_plain_partial_paths(run: &mut PlannerRun<'_>, rel: RelId) -> PgResult<()> {
    let parallel_workers = {
        let r = run.root.rel(rel);
        let max = crate::gucs::max_parallel_workers_per_gather();
        // Stage-4 pool arming (guc_tables::lane_pool): a pgrcolumnar baserel in
        // an armed session plans exactly the requested DOP (clamped to the
        // gather GUC; the helper already clamped to available cores) — the
        // plan's forced-plans posture, no shape rules. Unarmed pgrcolumnar rels
        // size from their own scan geometry (row-group count; see
        // compute_pgrcolumnar_parallel_worker), honoring the parallel_workers
        // reloption first. Heap rels keep C's page-ladder sizing untouched.
        if r.amflags & types_pathnodes::AMFLAG_PGRCOLUMNAR != 0 {
            match ::guc_tables::lane_pool::lane_parallel_pool_dop() {
                dop if dop > 0 => dop.min(max),
                _ => ::allpaths::compute_pgrcolumnar_parallel_worker(r, r.tuples, max),
            }
        } else {
            ::allpaths::compute_parallel_worker(r, r.pages as f64, -1.0, max)
        }
    };
    if parallel_workers <= 0 {
        return Ok(());
    }
    let p = crate::pathnode::create_seqscan_path(run, rel, &crate::relnode::RELIDS_UNSET, parallel_workers)?;
    crate::pathnode::add_partial_path(run, rel, p);
    Ok(())
}

// generate_gather_paths (allpaths.c): only call once all partial paths for
// the rel exist (add_partial_path may drop paths a Gather references).
pub(crate) fn generate_gather_paths(
    run: &mut PlannerRun<'_>,
    rel: RelId,
    override_rows: bool,
) -> PgResult<()> {
    if run.root.rel(rel).partial_pathlist.is_empty() {
        return Ok(());
    }
    let target = run.rel_reltarget_id(rel);

    // Gather output is unordered: only the cheapest partial path (kept in
    // front by add_partial_path) is interesting for a plain Gather.
    let cheapest = run.root.rel(rel).partial_pathlist[0];
    let rows = {
        let cb = run.root.path(cheapest).base();
        override_rows.then(|| ::costsize::compute_gather_rows(cb.rows, cb.parallel_workers))
    };
    let gather = crate::pathnode::create_gather_path(run, rel, cheapest, Some(target), rows);
    add_path(run, rel, gather);

    let mut i = 0usize;
    while i < run.root.rel(rel).partial_pathlist.len() {
        let subpath = run.root.rel(rel).partial_pathlist[i];
        i += 1;
        let sb = run.root.path(subpath).base();
        if sb.pathkeys.is_empty() {
            continue;
        }
        let rows =
            override_rows.then(|| ::costsize::compute_gather_rows(sb.rows, sb.parallel_workers));
        let keys = crate::relnode::pgvec_clone_shallow(run.mcx, &sb.pathkeys);
        let gm =
            crate::pathnode::create_gather_merge_path(run, rel, subpath, Some(target), keys, rows);
        add_path(run, rel, gm);
    }
    Ok(())
}

// get_useful_pathkeys_for_relation (allpaths.c): today only query_pathkeys
// prefixes qualify, so at most one entry comes back.
fn get_useful_pathkeys_for_relation<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    require_parallel_safe: bool,
) -> PgResult<Vec<mcx::PgVec<'mcx, PathKey>>> {
    let mut useful_pathkeys_list = Vec::new();
    if run.root.query_pathkeys.is_empty() {
        return Ok(useful_pathkeys_list);
    }
    let mut npathkeys = 0usize;
    while npathkeys < run.root.query_pathkeys.len() {
        let pathkey = run.root.query_pathkeys[npathkeys];
        let pathkey_ec = pathkey.pk_eclass.expect("pathkey has an eclass");
        // A sortable prefix is still useful (incremental sort); stop at the
        // first pathkey with no early-computable member.
        if !relation_can_be_sorted_early(run, rel, pathkey_ec, require_parallel_safe)? {
            break;
        }
        npathkeys += 1;
    }
    if npathkeys > 0 {
        let mut keys: mcx::PgVec<'mcx, PathKey> = mcx::PgVec::new_in(run.mcx);
        keys.extend(run.root.query_pathkeys.iter().take(npathkeys).copied());
        useful_pathkeys_list.push(keys);
    }
    Ok(useful_pathkeys_list)
}

// generate_useful_gather_paths (allpaths.c): plain gathers plus Gather
// Merges over (incrementally) sorted partial paths for useful orderings.
pub(crate) fn generate_useful_gather_paths(
    run: &mut PlannerRun<'_>,
    rel: RelId,
    override_rows: bool,
) -> PgResult<()> {
    if run.root.rel(rel).partial_pathlist.is_empty() {
        return Ok(());
    }

    // M5-3 coverage-keyed suppression (m5_suppress, design §2.3): under
    // pgrust.parallel_engine=runtime a COVERED shape gets no Gather/Gather
    // Merge anywhere in its plan — the serial-shaped plan reaches the
    // executor and the runtime router engages it. Uncovered shapes (and
    // every query under the default legacy engine) fall through unchanged.
    if crate::m5_suppress::m5_suppress_gather(run)? {
        return Ok(());
    }
    // NLIDX (GL-NLIDX-2, rel-aware): the final joinrel's serial election is
    // itself the NL-inner-index shape the morsel arm engages — no Gather of
    // any form; uncovered shapes and knob-OFF fall through unchanged.
    if crate::m5_suppress::m5_suppress_gather_nlidx(run, rel)? {
        return Ok(());
    }

    generate_gather_paths(run, rel, override_rows)?;

    let useful_pathkeys_list = get_useful_pathkeys_for_relation(run, rel, true)?;
    let cheapest_partial_path = run.root.rel(rel).partial_pathlist[0];
    let target = run.rel_reltarget_id(rel);

    for useful_pathkeys in useful_pathkeys_list {
        let mut i = 0usize;
        while i < run.root.rel(rel).partial_pathlist.len() {
            let subpath = run.root.rel(rel).partial_pathlist[i];
            i += 1;
            let (is_sorted, presorted_keys) = crate::pathkeys::pathkeys_count_contained_in(
                &useful_pathkeys,
                &run.root.path(subpath).base().pathkeys,
            );
            // Fully sorted subpaths already got their Gather Merge in
            // generate_gather_paths; no sort to add.
            if is_sorted {
                continue;
            }
            if subpath != cheapest_partial_path
                && (presorted_keys == 0 || !crate::gucs::enable_incremental_sort())
            {
                continue;
            }
            let keys = crate::relnode::pgvec_clone_shallow(run.mcx, &useful_pathkeys);
            let sorted = if presorted_keys == 0 || !crate::gucs::enable_incremental_sort() {
                crate::pathnode::create_sort_path(run, rel, subpath, keys, -1.0)
            } else {
                crate::pathnode::create_incremental_sort_path(
                    run,
                    rel,
                    subpath,
                    keys,
                    presorted_keys,
                    -1.0,
                )?
            };
            let (rows, gm_keys) = {
                let sb = run.root.path(sorted).base();
                (
                    ::costsize::compute_gather_rows(sb.rows, sb.parallel_workers),
                    crate::relnode::pgvec_clone_shallow(run.mcx, &sb.pathkeys),
                )
            };
            let gm = crate::pathnode::create_gather_merge_path(
                run,
                rel,
                sorted,
                Some(target),
                gm_keys,
                override_rows.then_some(rows),
            );
            add_path(run, rel, gm);
        }
    }
    Ok(())
}

// relation_can_be_sorted_early (equivclass.c).
fn relation_can_be_sorted_early(
    run: &mut PlannerRun<'_>,
    rel: RelId,
    ec: types_pathnodes::EcId,
    require_parallel_safe: bool,
) -> PgResult<bool> {
    // Volatile-EC sorts must always wait for the final output step.
    if run.root.ec(ec).ec_has_volatile {
        return Ok(false);
    }
    let relids = crate::relnode::relids_copy(run.mcx, &run.root.rel(rel).relids);
    let target = run.rel_reltarget_id(rel);

    let nexprs = run.root.pathtarget(target).exprs.len();
    for i in 0..nexprs {
        let texpr = *run.root.expr_node(run.root.pathtarget(target).exprs[i]);
        let Some(em_id) = crate::createplan::find_ec_member_matching_expr(run, ec, texpr, &relids)
        else {
            continue;
        };
        let em_expr = *run.root.expr_node(run.root.em(em_id).em_expr);
        // SRF results can't be computed early either.
        if coerce::expression_returns_set(em_expr) {
            continue;
        }
        if require_parallel_safe && !crate::is_parallel_safe_opt(run, Some(em_expr))? {
            continue;
        }
        return Ok(true);
    }

    let Some(em_id) = find_computable_ec_member(run, ec, target, &relids, require_parallel_safe)?
    else {
        return Ok(false);
    };
    let em_expr = *run.root.expr_node(run.root.em(em_id).em_expr);
    // SRFs can't appear in WHERE, so no other member could do better.
    if coerce::expression_returns_set(em_expr) {
        return Ok(false);
    }
    Ok(true)
}

// find_computable_ec_member (equivclass.c). The reltarget stands in for C's
// "exprs" list (the only ported call site); PVC_INCLUDE_CONVERTROWTYPES is
// dead until appendrel wholerow children exist (the var walker is loud there).
fn find_computable_ec_member(
    run: &mut PlannerRun<'_>,
    ec: types_pathnodes::EcId,
    target: types_pathnodes::PtId,
    relids: &types_pathnodes::Relids<'_>,
    require_parallel_safe: bool,
) -> PgResult<Option<types_pathnodes::EmId>> {
    use vars::{PVC_INCLUDE_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS, PVC_INCLUDE_WINDOWFUNCS};
    let mcx = run.mcx;
    let flags = PVC_INCLUDE_AGGREGATES | PVC_INCLUDE_WINDOWFUNCS | PVC_INCLUDE_PLACEHOLDERS;

    let mut exprvars: Vec<types_nodes::Node<'_>> = Vec::new();
    let nexprs = run.root.pathtarget(target).exprs.len();
    for i in 0..nexprs {
        let e = *run.root.expr_node(run.root.pathtarget(target).exprs[i]);
        for v in &vars::pull_var_clause(mcx, e, flags)? {
            exprvars.push(v);
        }
    }

    let candidates = {
        use crate::relnode::relids_members;
        let e = run.root.ec(ec);
        let mut out: Vec<types_pathnodes::EmId> = Vec::new();
        out.extend(e.ec_members.iter().copied());
        if !e.ec_childmembers.is_empty() {
            for r in relids_members(relids) {
                if let Some(list) = e.ec_childmembers.get(r as usize) {
                    out.extend(list.iter().copied());
                }
            }
        }
        out
    };

    'candidate: for em_id in candidates {
        let em = run.root.em(em_id);
        if em.em_is_const {
            continue;
        }
        if em.em_is_child && !crate::relnode::relids_is_subset(&em.em_relids, relids) {
            continue;
        }
        let em_expr = *run.root.expr_node(em.em_expr);
        for emv in &vars::pull_var_clause(mcx, em_expr, flags)? {
            if !exprvars.iter().any(|&x| types_nodes::equal(x, emv)) {
                continue 'candidate;
            }
        }
        if require_parallel_safe && !crate::is_parallel_safe_opt(run, Some(em_expr))? {
            continue;
        }
        return Ok(Some(em_id));
    }
    Ok(None)
}

// generate_partitionwise_join_paths (allpaths.c).
pub(crate) fn generate_partitionwise_join_paths(
    run: &mut PlannerRun<'_>,
    rel: RelId,
) -> PgResult<()> {
    if !matches!(
        run.root.rel(rel).reloptkind,
        types_pathnodes::RELOPT_JOINREL | types_pathnodes::RELOPT_OTHER_JOINREL
    ) {
        return Ok(());
    }
    if !crate::relnode::rel_is_partitioned(&run.root, rel) {
        return Ok(());
    }
    debug_assert!(run.root.rel(rel).consider_partitionwise_join);

    let num_parts = run.root.rel(rel).nparts;
    let mut live_children: mcx::PgVec<'_, RelId> = mcx::PgVec::new_in(run.mcx);
    for cnt_parts in 0..num_parts as usize {
        let Some(child_rel) = run.root.rel(rel).part_rels[cnt_parts] else {
            continue; // pruned entirely, certainly dummy
        };
        generate_partitionwise_join_paths(run, child_rel)?;
        if run.root.rel(child_rel).pathlist.is_empty() {
            // No path for this child: the parent joinrel is unpartitioned.
            run.root.rel_mut(rel).nparts = 0;
            return Ok(());
        }
        set_cheapest(run, child_rel)?;
        if crate::joinrels::is_dummy_rel(&run.root, child_rel) {
            continue;
        }
        live_children.push(child_rel);
    }

    if live_children.is_empty() {
        crate::joinrels::mark_dummy_rel(run, rel)?;
        return Ok(());
    }
    add_paths_to_append_rel(run, rel, &live_children)
}
