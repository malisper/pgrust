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
    // set_base_rel_consider_startup: consider_param_startup only flips for
    // SEMI/ANTI RHS singletons, which cannot exist here.
    debug_assert!(run
        .root
        .join_info_list
        .iter()
        .all(|sj| sj.jointype != types_pathnodes::JOIN_SEMI
            && sj.jointype != types_pathnodes::JOIN_ANTI));

    set_base_rel_sizes(run)?;

    let mut total_pages = 0.0f64;
    for rti in 1..run.root.simple_rel_array_size as usize {
        let Some(brel) = run.root.simple_rel_array[rti] else { continue };
        debug_assert_eq!(run.root.rel(brel).relid as usize, rti);
        // IS_SIMPLE_REL && not dummy (dummies can't be built on this lane).
        if run.root.rel(brel).reloptkind == RELOPT_BASEREL {
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
    // relation_excluded_by_constraints: its inputs (constraint_exclusion on,
    // partition quals, constant-FALSE quals) are all loud upstream.
    let rte = run.rte(rti);
    assert!(!rte.inh, "set_append_rel_size (allpaths.c): M2 partition lane");
    match rte.rtekind {
        RTEKind::RTE_RELATION => {
            debug_assert_eq!(rte.relkind, types_rel::RELKIND_RELATION);
            debug_assert!(rte.tablesample.is_none());
            set_plain_rel_size(run, rel)?;
        }
        RTEKind::RTE_FUNCTION => {
            crate::costsize::set_function_size_estimates(run, rel)?;
        }
        RTEKind::RTE_VALUES => {
            crate::costsize::set_values_size_estimates(run, rel)?;
        }
        RTEKind::RTE_CTE => {
            crate::cte::set_cte_pathlist(run, rel, rti)?;
        }
        RTEKind::RTE_RESULT => {
            unreachable!("RTE_RESULT is handled by query_planner's trivial arm");
        }
        other => panic!("set_rel_size (allpaths.c): {other:?}; M2 scan lane"),
    }
    debug_assert!(run.root.rel(rel).rows > 0.0);
    Ok(())
}

fn set_rel_pathlist(run: &mut PlannerRun<'_>, rel: RelId, rti: usize) -> PgResult<()> {
    let rte = run.rte(rti);
    debug_assert!(!rte.inh);
    match rte.rtekind {
        RTEKind::RTE_RELATION => set_plain_rel_pathlist(run, rel)?,
        RTEKind::RTE_FUNCTION => set_function_pathlist(run, rel, rti)?,
        RTEKind::RTE_VALUES => set_values_pathlist(run, rel)?,
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
        RTEKind::RTE_FUNCTION | RTEKind::RTE_VALUES => {
            // C tests is_parallel_safe over the funcexprs/values_lists;
            // parallel plans are loud on this lane, so the flag stays
            // conservatively false.
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
