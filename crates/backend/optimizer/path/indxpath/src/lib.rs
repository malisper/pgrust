//! indxpath.c: index path generation over restriction, join, and
//! EC-derived clauses; SAOP/boolean/RowCompare matching included.

use mcx::PgVec;
use types_error::PgResult;
use types_nodes::{Node, NodeTag};
use types_pathnodes::{IndexClause, IndexOptInfo, PathId, RelId, RinfoId};

use pathnode::add_path;
use types_pathnodes::relids::{relids_empty, relids_is_member, relids_is_subset};
use types_pathnodes::run::PlannerRun;

pub struct IndexClauseSet<'mcx> {
    pub nonempty: bool,
    pub indexclauses: PgVec<'mcx, PgVec<'mcx, IndexClause<'mcx>>>,
}

impl<'mcx> IndexClauseSet<'mcx> {
    fn new(mcx: mcx::Mcx<'mcx>, ncols: usize) -> Self {
        let mut indexclauses = PgVec::new_in(mcx);
        for _ in 0..ncols {
            indexclauses.push(PgVec::new_in(mcx));
        }
        IndexClauseSet {
            nonempty: false,
            indexclauses,
        }
    }
}

pub fn check_index_predicates<'mcx>(run: &mut PlannerRun<'mcx>, rel: RelId) -> PgResult<()> {
    let mcx = run.mcx;
    let nindexes = run.root.rel(rel).indexlist.len();
    let mut have_partial = false;
    for i in 0..nindexes {
        let index = run.root.rel(rel).indexlist[i];
        let mut clauses = PgVec::new_in(mcx);
        clauses.extend(run.root.rel(rel).baserestrictinfo.iter().copied());
        *index.indrestrictinfo.borrow_mut() = clauses;
        if !index.indpred.is_empty() {
            have_partial = true;
        }
    }
    if !have_partial {
        return Ok(());
    }

    let mut clause_rids: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
    clause_rids.extend(run.root.rel(rel).baserestrictinfo.iter().copied());
    for i in 0..run.root.rel(rel).joininfo.len() {
        let rid = run.root.rel(rel).joininfo[i];
        if join_clause_is_movable_to(run, rid, rel) {
            clause_rids.push(rid);
        }
    }
    // A child rel subtracts its parents' relids from all_query_rels, since
    // ECs and join clauses are stated in terms of the parents.
    let subtract = if run.root.rel(rel).reloptkind == types_pathnodes::RELOPT_OTHER_MEMBER_REL {
        types_pathnodes::relids::find_childrel_parents(&run.root, rel)
    } else {
        types_pathnodes::relids::relids_copy(mcx, &run.root.rel(rel).relids)
    };
    let mut otherrels =
        types_pathnodes::relids::relids_difference(mcx, &run.root.all_query_rels, &subtract);
    otherrels =
        types_pathnodes::relids::relids_difference(mcx, &otherrels, &run.root.rel(rel).nulling_relids);
    if !types_pathnodes::relids::relids_is_empty(&otherrels) {
        let join_relids = types_pathnodes::relids::relids_union(mcx, &run.root.rel(rel).relids, &otherrels);
        let derived = equivclass::generate_join_implied_equalities(
            run,
            &join_relids,
            &otherrels,
            rel,
            None,
        )?;
        clause_rids.extend(derived.iter().copied());
    }
    let mut clauselist: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
    for &rid in clause_rids.iter() {
        clauselist.push(*run.root.expr_node(run.root.rinfo(rid).clause));
    }

    let relid = run.root.rel(rel).relid;
    let is_target_rel = relids_is_member(relid as i32, &run.root.all_result_relids)
        || run
            .root
            .rowMarks
            .iter()
            .any(|&rm| run.rowmark(rm).rti == relid);

    for i in 0..nindexes {
        let index = run.root.rel(rel).indexlist[i];
        if index.indpred.is_empty() {
            continue;
        }
        let mut indpred: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
        for &pid in index.indpred.iter() {
            indpred.push(*run.root.expr_node(pid));
        }
        if !index.predOK.get() {
            index.predOK.set(planner_seams::predicate_implied_by::call(
                mcx,
                &indpred,
                &clauselist,
                false,
            )?);
        }
        // Target rels keep implied quals for EvalPlanQual rechecks; a
        // !amoptionalkey index must keep first-column quals to stay scannable.
        if is_target_rel || !index.amoptionalkey {
            continue;
        }
        let mut kept: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
        for j in 0..run.root.rel(rel).baserestrictinfo.len() {
            let rid = run.root.rel(rel).baserestrictinfo[j];
            let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
            if clauses::contain_mutable_functions(clause)?
                || !planner_seams::predicate_implied_by::call(mcx, &[clause], &indpred, false)?
            {
                kept.push(rid);
            }
        }
        *index.indrestrictinfo.borrow_mut() = kept;
    }
    Ok(())
}

// join_clause_is_movable_to (restrictinfo.c).
pub fn join_clause_is_movable_to(run: &PlannerRun<'_>, rid: RinfoId, rel: RelId) -> bool {
    let rinfo = run.root.rinfo(rid);
    let baserel = run.root.rel(rel);
    if !relids_is_member(baserel.relid as i32, &rinfo.clause_relids) {
        return false;
    }
    if relids_is_member(baserel.relid as i32, &rinfo.outer_relids) {
        return false;
    }
    if types_pathnodes::relids::relids_overlap(&rinfo.clause_relids, &baserel.nulling_relids) {
        return false;
    }
    if types_pathnodes::relids::relids_overlap(&baserel.lateral_referencers, &rinfo.clause_relids) {
        return false;
    }
    if rinfo.is_clone {
        return false;
    }
    true
}

// create_index_paths (indxpath.c).
pub fn create_index_paths<'mcx>(run: &mut PlannerRun<'mcx>, rel: RelId) -> PgResult<()> {
    let mcx = run.mcx;
    if run.root.rel(rel).indexlist.is_empty() {
        return Ok(());
    }

    let mut bitindexpaths: PgVec<'mcx, PathId> = PgVec::new_in(mcx);
    let mut bitjoinpaths: PgVec<'mcx, PathId> = PgVec::new_in(mcx);
    let mut joinorclauses: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
    let nindexes = run.root.rel(rel).indexlist.len();
    for idx in 0..nindexes {
        let index = run.root.rel(rel).indexlist[idx];
        if !index.indpred.is_empty() && !index.predOK.get() {
            continue;
        }
        let mut rclauseset = IndexClauseSet::new(mcx, index.nkeycolumns as usize);
        match_restriction_clauses_to_index(run, &index, &mut rclauseset)?;
        get_index_paths(run, rel, &index, &rclauseset, &mut bitindexpaths)?;

        // Without join or EC-join clauses both match passes are no-ops; skip
        // their clause-set builds (strictly less work than C's stack MemSets).
        if run.root.rel(rel).joininfo.is_empty() && !run.root.rel(rel).has_eclass_joins {
            continue;
        }

        // "Loose" join clauses not absorbed into ECs.
        let mut jclauseset = IndexClauseSet::new(mcx, index.nkeycolumns as usize);
        match_join_clauses_to_index(run, rel, &index, &mut jclauseset, &mut joinorclauses)?;

        let mut eclauseset = IndexClauseSet::new(mcx, index.nkeycolumns as usize);
        match_eclass_clauses_to_index(run, &index, &mut eclauseset)?;

        if jclauseset.nonempty || eclauseset.nonempty {
            consider_index_join_clauses(
                run,
                rel,
                &index,
                &rclauseset,
                &jclauseset,
                &eclauseset,
                &mut bitjoinpaths,
            )?;
        }
    }

    // C calls generate_bitmap_or_paths unconditionally; the OR pre-scan skips
    // its two list copies on the OR-free common path (strictly less work).
    let has_or = (0..run.root.rel(rel).baserestrictinfo.len()).any(|i| {
        let rid = run.root.rel(rel).baserestrictinfo[i];
        clauses::is_orclause(*run.root.expr_node(run.root.rinfo(rid).clause))
    });
    if has_or {
        let mut baserestrict: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
        baserestrict.extend(run.root.rel(rel).baserestrictinfo.iter().copied());
        let orpaths = generate_bitmap_or_paths(run, rel, &baserestrict, &[])?;
        bitindexpaths.extend(orpaths.iter().copied());
    }
    if !joinorclauses.is_empty() {
        let mut baserestrict: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
        baserestrict.extend(run.root.rel(rel).baserestrictinfo.iter().copied());
        let orpaths = generate_bitmap_or_paths(run, rel, &joinorclauses, &baserestrict)?;
        bitjoinpaths.extend(orpaths.iter().copied());
    }

    if !bitindexpaths.is_empty() {
        let bitmapqual = choose_bitmap_and(run, rel, &bitindexpaths)?;
        let lateral_relids = types_pathnodes::relids::relids_copy(mcx, &run.root.rel(rel).lateral_relids);
        let bpath =
            pathnode::create_bitmap_heap_path(run, rel, bitmapqual, &lateral_relids, 1.0, 0)?;
        add_path(run, rel, bpath);
        if run.root.rel(rel).consider_parallel
            && types_pathnodes::relids::relids_is_empty(&run.root.rel(rel).lateral_relids)
        {
            create_partial_bitmap_paths(run, rel, bitmapqual)?;
        }
    }

    if !bitjoinpaths.is_empty() {
        // One BitmapHeapPath per distinct parameterization seen among the
        // join bitmap index paths.
        let mut all_path_outers: PgVec<'mcx, types_pathnodes::Relids<'mcx>> = PgVec::new_in(mcx);
        for &p in bitjoinpaths.iter() {
            let req = types_pathnodes::relids::relids_copy(
                mcx,
                pathnode::path_req_outer(run.root.path(p).base()),
            );
            if !all_path_outers
                .iter()
                .any(|o| types_pathnodes::relids::relids_equal(o, &req))
            {
                all_path_outers.push(req);
            }
        }
        for max_outers in all_path_outers.iter() {
            let mut this_path_set: PgVec<'mcx, PathId> = PgVec::new_in(mcx);
            for &p in bitjoinpaths.iter() {
                if relids_is_subset(
                    pathnode::path_req_outer(run.root.path(p).base()),
                    max_outers,
                ) {
                    this_path_set.push(p);
                }
            }
            this_path_set.extend(bitindexpaths.iter().copied());
            let bitmapqual = choose_bitmap_and(run, rel, &this_path_set)?;
            let required_outer = types_pathnodes::relids::relids_copy(
                mcx,
                pathnode::path_req_outer(run.root.path(bitmapqual).base()),
            );
            let cur_relid = run.root.rel(rel).relid;
            let loop_count = get_loop_count(run, cur_relid, &required_outer)?;
            let bpath = pathnode::create_bitmap_heap_path(
                run,
                rel,
                bitmapqual,
                &required_outer,
                loop_count,
                0,
            )?;
            add_path(run, rel, bpath);
        }
    }
    Ok(())
}

// create_partial_bitmap_paths (allpaths.c).
fn create_partial_bitmap_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    bitmapqual: PathId,
) -> PgResult<()> {
    let (pages_fetched, _, _) = costsize::compute_bitmap_pages(run, rel, bitmapqual, 1.0);
    let parallel_workers = ::allpaths::compute_parallel_worker(
        run.root.rel(rel),
        pages_fetched,
        -1.0,
        guc_tables::vars::max_parallel_workers_per_gather.read(),
    );
    if parallel_workers <= 0 {
        return Ok(());
    }
    let lateral_relids =
        types_pathnodes::relids::relids_copy(run.mcx, &run.root.rel(rel).lateral_relids);
    let bpath = pathnode::create_bitmap_heap_path(
        run,
        rel,
        bitmapqual,
        &lateral_relids,
        1.0,
        parallel_workers,
    )?;
    pathnode::add_partial_path(run, rel, bpath);
    Ok(())
}

// consider_index_join_clauses (indxpath.c).
#[allow(clippy::too_many_arguments)]
fn consider_index_join_clauses<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    index: &'mcx IndexOptInfo<'mcx>,
    rclauseset: &IndexClauseSet<'mcx>,
    jclauseset: &IndexClauseSet<'mcx>,
    eclauseset: &IndexClauseSet<'mcx>,
    bitindexpaths: &mut PgVec<'mcx, PathId>,
) -> PgResult<()> {
    let mut considered_clauses = 0usize;
    let mut considered_relids: PgVec<'mcx, types_pathnodes::Relids<'mcx>> = PgVec::new_in(run.mcx);
    for indexcol in 0..index.nkeycolumns as usize {
        considered_clauses += jclauseset.indexclauses[indexcol].len();
        consider_index_join_outer_rels(
            run,
            rel,
            index,
            rclauseset,
            jclauseset,
            eclauseset,
            bitindexpaths,
            &jclauseset.indexclauses[indexcol],
            considered_clauses,
            &mut considered_relids,
        )?;
        considered_clauses += eclauseset.indexclauses[indexcol].len();
        consider_index_join_outer_rels(
            run,
            rel,
            index,
            rclauseset,
            jclauseset,
            eclauseset,
            bitindexpaths,
            &eclauseset.indexclauses[indexcol],
            considered_clauses,
            &mut considered_relids,
        )?;
    }
    Ok(())
}

// consider_index_join_outer_rels (indxpath.c).
#[allow(clippy::too_many_arguments)]
fn consider_index_join_outer_rels<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    index: &'mcx IndexOptInfo<'mcx>,
    rclauseset: &IndexClauseSet<'mcx>,
    jclauseset: &IndexClauseSet<'mcx>,
    eclauseset: &IndexClauseSet<'mcx>,
    bitindexpaths: &mut PgVec<'mcx, PathId>,
    indexjoinclauses: &[IndexClause<'mcx>],
    considered_clauses: usize,
    considered_relids: &mut PgVec<'mcx, types_pathnodes::Relids<'mcx>>,
) -> PgResult<()> {
    let mcx = run.mcx;
    for iclause in indexjoinclauses {
        let rid = iclause.rinfo.expect("IndexClause rinfo");
        let clause_relids = types_pathnodes::relids::relids_copy(mcx, &run.root.rinfo(rid).clause_relids);
        let parent_ec = run.root.rinfo(rid).parent_ec;
        if considered_relids
            .iter()
            .any(|r| types_pathnodes::relids::relids_equal(r, &clause_relids))
        {
            continue;
        }
        // Union with each previously-tried set, capped at
        // 10 * considered_clauses relid sets.
        let num_considered_relids = considered_relids.len();
        for pos in 0..num_considered_relids {
            let oldrelids = types_pathnodes::relids::relids_copy(mcx, &considered_relids[pos]);
            if types_pathnodes::relids::relids_subset_compare(&clause_relids, &oldrelids)
                != types_pathnodes::relids::SubsetCmp::Different
            {
                continue;
            }
            if parent_ec.is_some()
                && eclass_already_used(run, parent_ec, &oldrelids, indexjoinclauses)
            {
                continue;
            }
            if considered_relids.len() >= 10 * considered_clauses {
                break;
            }
            let union = types_pathnodes::relids::relids_union(mcx, &clause_relids, &oldrelids);
            get_join_index_paths(
                run,
                rel,
                index,
                rclauseset,
                jclauseset,
                eclauseset,
                bitindexpaths,
                &union,
                considered_relids,
            )?;
        }
        get_join_index_paths(
            run,
            rel,
            index,
            rclauseset,
            jclauseset,
            eclauseset,
            bitindexpaths,
            &clause_relids,
            considered_relids,
        )?;
    }
    Ok(())
}

// get_join_index_paths (indxpath.c).
#[allow(clippy::too_many_arguments)]
fn get_join_index_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    index: &'mcx IndexOptInfo<'mcx>,
    rclauseset: &IndexClauseSet<'mcx>,
    jclauseset: &IndexClauseSet<'mcx>,
    eclauseset: &IndexClauseSet<'mcx>,
    bitindexpaths: &mut PgVec<'mcx, PathId>,
    relids: &types_pathnodes::Relids<'mcx>,
    considered_relids: &mut PgVec<'mcx, types_pathnodes::Relids<'mcx>>,
) -> PgResult<()> {
    let mcx = run.mcx;
    if considered_relids
        .iter()
        .any(|r| types_pathnodes::relids::relids_equal(r, relids))
    {
        return Ok(());
    }
    let mut clauseset = IndexClauseSet::new(mcx, index.nkeycolumns as usize);
    for indexcol in 0..index.nkeycolumns as usize {
        for ic in jclauseset.indexclauses[indexcol].iter() {
            let rid = ic.rinfo.expect("IndexClause rinfo");
            if relids_is_subset(&run.root.rinfo(rid).clause_relids, relids) {
                clauseset.indexclauses[indexcol].push(ic.clone());
            }
        }
        // EC clauses per column are mutually redundant: use at most one.
        for ic in eclauseset.indexclauses[indexcol].iter() {
            let rid = ic.rinfo.expect("IndexClause rinfo");
            if relids_is_subset(&run.root.rinfo(rid).clause_relids, relids) {
                clauseset.indexclauses[indexcol].push(ic.clone());
                break;
            }
        }
        for ic in rclauseset.indexclauses[indexcol].iter() {
            clauseset.indexclauses[indexcol].push(ic.clone());
        }
        if !clauseset.indexclauses[indexcol].is_empty() {
            clauseset.nonempty = true;
        }
    }
    debug_assert!(clauseset.nonempty);
    get_index_paths(run, rel, index, &clauseset, bitindexpaths)?;
    considered_relids.push(types_pathnodes::relids::relids_copy(mcx, relids));
    Ok(())
}

// eclass_already_used (indxpath.c).
fn eclass_already_used(
    run: &PlannerRun<'_>,
    parent_ec: Option<types_pathnodes::EcId>,
    oldrelids: &types_pathnodes::Relids<'_>,
    indexjoinclauses: &[IndexClause<'_>],
) -> bool {
    for iclause in indexjoinclauses {
        let rid = iclause.rinfo.expect("IndexClause rinfo");
        let ri = run.root.rinfo(rid);
        if ri.parent_ec == parent_ec && relids_is_subset(&ri.clause_relids, oldrelids) {
            return true;
        }
    }
    false
}

// get_loop_count (indxpath.c).
pub(crate) fn get_loop_count(
    run: &mut PlannerRun<'_>,
    cur_relid: u32,
    outer_relids: &types_pathnodes::Relids<'_>,
) -> PgResult<f64> {
    if types_pathnodes::relids::relids_is_unset(outer_relids) {
        return Ok(1.0);
    }
    let mut members: PgVec<'_, i32> = PgVec::new_in(run.mcx);
    members.extend(types_pathnodes::relids::relids_members(outer_relids));
    let mut result = 0.0f64;
    for outer_relid in members {
        if outer_relid >= run.root.simple_rel_array_size {
            continue;
        }
        let Some(outer_rel) = run.root.simple_rel_array[outer_relid as usize] else {
            continue;
        };
        debug_assert_eq!(run.root.rel(outer_rel).relid, outer_relid as u32);
        if planner_seams::is_dummy_rel::call(&run.root, outer_rel) {
            continue;
        }
        let outer_rows = run.root.rel(outer_rel).rows;
        debug_assert!(outer_rows > 0.0);
        let rowcount =
            adjust_rowcount_for_semijoins(run, cur_relid, outer_relid as u32, outer_rows)?;
        if result == 0.0 || result > rowcount {
            result = rowcount;
        }
    }
    Ok(if result > 0.0 { result } else { 1.0 })
}

// adjust_rowcount_for_semijoins (indxpath.c).
fn adjust_rowcount_for_semijoins(
    run: &mut PlannerRun<'_>,
    cur_relid: u32,
    outer_relid: u32,
    mut rowcount: f64,
) -> PgResult<f64> {
    let mcx = run.mcx;
    for i in 0..run.root.join_info_list.len() {
        let (is_semi, in_left, in_right) = {
            let sj = &run.root.join_info_list[i];
            (
                sj.jointype == types_pathnodes::JOIN_SEMI,
                relids_is_member(cur_relid as i32, &sj.syn_lefthand),
                relids_is_member(outer_relid as i32, &sj.syn_righthand),
            )
        };
        if is_semi && in_left && in_right {
            let (syn_righthand, rhs_exprs) = {
                let sj = &run.root.join_info_list[i];
                (
                    types_pathnodes::relids::relids_copy(mcx, &sj.syn_righthand),
                    types_pathnodes::relids::pgvec_clone_shallow(mcx, &sj.semi_rhs_exprs),
                )
            };
            let nraw = approximate_joinrel_size(run, &syn_righthand);
            let mut exprs: PgVec<'_, (types_pathnodes::NodeId, Node<'_>)> = PgVec::new_in(mcx);
            for &id in rhs_exprs.iter() {
                exprs.push((id, *run.root.expr_node(id)));
            }
            let nunique = planner_seams::estimate_num_groups::call(run, &exprs, nraw)?;
            if rowcount > nunique {
                rowcount = nunique;
            }
        }
    }
    Ok(rowcount)
}

// approximate_joinrel_size (indxpath.c).
fn approximate_joinrel_size(run: &PlannerRun<'_>, relids: &types_pathnodes::Relids<'_>) -> f64 {
    let mut rowcount = 1.0f64;
    for relid in types_pathnodes::relids::relids_members(relids) {
        if relid >= run.root.simple_rel_array_size {
            continue;
        }
        let Some(rel) = run.root.simple_rel_array[relid as usize] else {
            continue;
        };
        debug_assert_eq!(run.root.rel(rel).relid, relid as u32);
        if planner_seams::is_dummy_rel::call(&run.root, rel) {
            continue;
        }
        debug_assert!(run.root.rel(rel).rows > 0.0);
        rowcount *= run.root.rel(rel).rows;
    }
    rowcount
}

// match_join_clauses_to_index (indxpath.c).
fn match_join_clauses_to_index<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    index: &IndexOptInfo<'mcx>,
    clauseset: &mut IndexClauseSet<'mcx>,
    joinorclauses: &mut PgVec<'mcx, RinfoId>,
) -> PgResult<()> {
    let joininfo = types_pathnodes::relids::pgvec_clone_shallow(run.mcx, &run.root.rel(rel).joininfo);
    for &rid in joininfo.iter() {
        if !join_clause_is_movable_to(run, rid, rel) {
            continue;
        }
        if clauses::is_orclause(*run.root.expr_node(run.root.rinfo(rid).clause))
            && !joinorclauses.iter().any(|&x| x == rid)
        {
            joinorclauses.push(rid);
        }
        match_clause_to_index(run, rid, index, clauseset)?;
    }
    Ok(())
}

// match_eclass_clauses_to_index (indxpath.c).
fn match_eclass_clauses_to_index<'mcx>(
    run: &mut PlannerRun<'mcx>,
    index: &IndexOptInfo<'mcx>,
    clauseset: &mut IndexClauseSet<'mcx>,
) -> PgResult<()> {
    let mcx = run.mcx;
    let rel = index.rel.expect("index rel set");
    if !run.root.rel(rel).has_eclass_joins {
        return Ok(());
    }
    for indexcol in 0..index.nkeycolumns as usize {
        let lateral_referencers =
            types_pathnodes::relids::relids_copy(mcx, &run.root.rel(rel).lateral_referencers);
        let clauses = equivclass::generate_implied_equalities_for_column(
            run,
            rel,
            |run, _rel, ec, em| ec_member_matches_indexcol(run, ec, em, index, indexcol),
            &lateral_referencers,
        )?;
        // Recheck against the index: non-btree EC operators may not be in
        // the index opclass (cf ec_member_matches_indexcol).
        for &rid in clauses.iter() {
            match_clause_to_index(run, rid, index, clauseset)?;
        }
    }
    Ok(())
}

// ec_member_matches_indexcol (indxpath.c).
fn ec_member_matches_indexcol(
    run: &PlannerRun<'_>,
    ec: types_pathnodes::EcId,
    em: types_pathnodes::EmId,
    index: &IndexOptInfo<'_>,
    indexcol: usize,
) -> bool {
    use types_core::BTREE_AM_OID;
    debug_assert!(indexcol < index.nkeycolumns as usize);
    let cur_family = index.opfamily[indexcol];
    let cur_collation = index.indexcollations[indexcol];
    if index.relam == BTREE_AM_OID && !run.root.ec(ec).ec_opfamilies.contains(&cur_family) {
        return false;
    }
    if !index_coll_matches_expr_coll(cur_collation, run.root.ec(ec).ec_collation) {
        return false;
    }
    match_index_to_operand(
        run,
        *run.root.expr_node(run.root.em(em).em_expr),
        indexcol,
        index,
    )
}

fn match_restriction_clauses_to_index<'mcx>(
    run: &mut PlannerRun<'mcx>,
    index: &IndexOptInfo<'mcx>,
    clauseset: &mut IndexClauseSet<'mcx>,
) -> PgResult<()> {
    let clauses = index.indrestrictinfo.borrow().clone();
    for &rinfo in clauses.iter() {
        match_clause_to_index(run, rinfo, index, clauseset)?;
    }
    Ok(())
}

fn match_clause_to_index<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rinfo: RinfoId,
    index: &IndexOptInfo<'mcx>,
    clauseset: &mut IndexClauseSet<'mcx>,
) -> PgResult<()> {
    if run.root.rinfo(rinfo).pseudoconstant {
        return Ok(());
    }
    // restriction_is_securely_promotable.
    {
        let r = run.root.rinfo(rinfo);
        let index_rel = index.rel.expect("index rel set");
        if !(r.security_level <= run.root.rel(index_rel).baserestrict_min_security || r.leakproof) {
            return Ok(());
        }
    }
    for indexcol in 0..index.nkeycolumns as usize {
        if clauseset.indexclauses[indexcol]
            .iter()
            .any(|ic| ic.rinfo == Some(rinfo))
        {
            return Ok(());
        }
        if let Some(iclause) = match_clause_to_indexcol(run, rinfo, indexcol, index)? {
            clauseset.indexclauses[indexcol].push(iclause);
            clauseset.nonempty = true;
            return Ok(());
        }
    }
    Ok(())
}

const BOOL_BTREE_FAM_OID: u32 = 424;
const BOOL_HASH_FAM_OID: u32 = 2222;
const BOOLEAN_EQUAL_OPERATOR: u32 = 91;
const FIRST_NORMAL_OBJECT_ID: u32 = 16384;

fn is_boolean_opfamily(opfamily: u32) -> PgResult<bool> {
    if opfamily < FIRST_NORMAL_OBJECT_ID {
        Ok(opfamily == BOOL_BTREE_FAM_OID || opfamily == BOOL_HASH_FAM_OID)
    } else {
        lsyscache::op_in_opfamily(BOOLEAN_EQUAL_OPERATOR, opfamily)
    }
}

fn match_boolean_index_clause<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rinfo: RinfoId,
    indexcol: usize,
    index: &IndexOptInfo<'mcx>,
) -> PgResult<Option<IndexClause<'mcx>>> {
    let mcx = run.mcx;
    let clause = *run.root.expr_node(run.root.rinfo(rinfo).clause);
    let mut op = None;
    if match_index_to_operand(run, clause, indexcol, index) {
        op = Some(planner_seams::make_opclause::call(
            mcx,
            BOOLEAN_EQUAL_OPERATOR,
            clause,
            clauses::make_bool_const(mcx, true, false)?,
            0,
        )?);
    } else if clauses::is_notclause(clause) {
        let arg = clause
            .as_bool_expr()
            .unwrap()
            .args
            .first()
            .expect("NOT has one arg");
        if match_index_to_operand(run, arg, indexcol, index) {
            op = Some(planner_seams::make_opclause::call(
                mcx,
                BOOLEAN_EQUAL_OPERATOR,
                arg,
                clauses::make_bool_const(mcx, false, false)?,
                0,
            )?);
        }
    } else if clause.node_tag() == NodeTag::T_BooleanTest {
        use types_nodes::primnodes::BoolTestType;
        let btest = clause.as_boolean_test().unwrap();
        let arg = btest.arg.expect("BooleanTest carries its arg");
        let wanted = match btest.booltesttype {
            BoolTestType::IS_TRUE => Some(true),
            BoolTestType::IS_FALSE => Some(false),
            _ => None,
        };
        if let Some(v) = wanted {
            if match_index_to_operand(run, arg, indexcol, index) {
                op = Some(planner_seams::make_opclause::call(
                    mcx,
                    BOOLEAN_EQUAL_OPERATOR,
                    arg,
                    clauses::make_bool_const(mcx, v, false)?,
                    0,
                )?);
            }
        }
    }
    let Some(op) = op else { return Ok(None) };
    let mut indexquals = PgVec::new_in(mcx);
    indexquals.push(planner_seams::make_restrictinfo::call(
        run, op, true, false, false, false, 0, relids_empty(), relids_empty(), relids_empty(),
    )?);
    Ok(Some(IndexClause {
        rinfo: Some(rinfo),
        indexquals,
        lossy: false,
        indexcol: indexcol as i16,
        indexcols: PgVec::new_in(mcx),
    }))
}

pub fn indexcol_is_bool_constant_for_query<'mcx>(
    run: &mut PlannerRun<'mcx>,
    index: &IndexOptInfo<'mcx>,
    indexcol: usize,
) -> PgResult<bool> {
    if !is_boolean_opfamily(index.opfamily[indexcol])? {
        return Ok(false);
    }
    let rel = index.rel.expect("index carries its rel");
    for i in 0..run.root.rel(rel).baserestrictinfo.len() {
        let rid = run.root.rel(rel).baserestrictinfo[i];
        if run.root.rinfo(rid).pseudoconstant {
            continue;
        }
        if match_boolean_index_clause(run, rid, indexcol, index)?.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

// match_clause_to_indexcol (indxpath.c).
fn match_clause_to_indexcol<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rinfo: RinfoId,
    indexcol: usize,
    index: &IndexOptInfo<'mcx>,
) -> PgResult<Option<IndexClause<'mcx>>> {
    debug_assert!(indexcol < index.nkeycolumns as usize);
    let opfamily = index.opfamily[indexcol];
    if is_boolean_opfamily(opfamily)? {
        let iclause = match_boolean_index_clause(run, rinfo, indexcol, index)?;
        if iclause.is_some() {
            return Ok(iclause);
        }
    }

    let clause = *run.root.expr_node(run.root.rinfo(rinfo).clause);
    match clause.node_tag() {
        NodeTag::T_OpExpr => match_opclause_to_indexcol(run, rinfo, indexcol, index),
        // match_funcclause_to_indexcol (indxpath.c).
        NodeTag::T_FuncExpr => {
            let f = clause.as_func_expr().unwrap();
            let funcid = f.funcid;
            for (indexarg, op) in f.args.iter().enumerate() {
                if match_index_to_operand(run, op, indexcol, index) {
                    return get_index_clause_from_support(
                        run,
                        rinfo,
                        funcid,
                        indexarg as i32,
                        indexcol,
                        index,
                    );
                }
            }
            Ok(None)
        }
        NodeTag::T_RelabelType => {
            panic!("match_clause_to_indexcol (indxpath.c): RelabelType clause; M2 lane")
        }
        NodeTag::T_NullTest if index.amsearchnulls => {
            let nt = clause.as_null_test().unwrap();
            if !nt.argisrow
                && match_index_to_operand(run, nt.arg.expect("NullTest.arg"), indexcol, index)
            {
                return Ok(Some(IndexClause {
                    rinfo: Some(rinfo),
                    indexquals: {
                        let mut v = PgVec::new_in(run.mcx);
                        v.push(rinfo);
                        v
                    },
                    lossy: false,
                    indexcol: indexcol as i16,
                    indexcols: PgVec::new_in(run.mcx),
                }));
            }
            Ok(None)
        }
        NodeTag::T_ScalarArrayOpExpr if index.amsearcharray => {
            match_saopclause_to_indexcol(run, rinfo, indexcol, index)
        }
        NodeTag::T_ScalarArrayOpExpr => Ok(None),
        NodeTag::T_BoolExpr if clauses::is_orclause(clause) => {
            match_orclause_to_indexcol(run, rinfo, indexcol, index)
        }
        NodeTag::T_RowCompareExpr => match_rowcompare_to_indexcol(run, rinfo, indexcol, index),
        _ => Ok(None),
    }
}

// match_rowcompare_to_indexcol (indxpath.c): the first row member must match
// this index column (btree only); expand_indexqual_rowcompare does the rest.
fn match_rowcompare_to_indexcol<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rinfo: RinfoId,
    indexcol: usize,
    index: &IndexOptInfo<'mcx>,
) -> PgResult<Option<IndexClause<'mcx>>> {
    if index.relam != types_core::BTREE_AM_OID {
        return Ok(None);
    }
    let index_relid = run.root.rel(index.rel.expect("index rel set")).relid;
    let opfamily = index.opfamily[indexcol];
    let idxcollation = index.indexcollations[indexcol];

    let clause = *run.root.expr_node(run.root.rinfo(rinfo).clause);
    let rc = clause.as_row_compare_expr().expect("RowCompareExpr");
    let leftop = rc.largs.nth(0);
    let rightop = rc.rargs.nth(0);
    let mut expr_op = rc.opnos.nth(0);
    let expr_coll = rc.inputcollids.nth(0);

    if !index_coll_matches_expr_coll(idxcollation, expr_coll) {
        return Ok(None);
    }

    // Match on operator opfamily membership, not the RowCompareExpr's own
    // opfamilies (reverse-sort families make those a matter of chance).
    let var_on_left;
    if match_index_to_operand(run, leftop, indexcol, index)
        && !vars::pull_varnos(run.mcx, rightop)?.is_member(index_relid as i32)
        && !clauses::contain_volatile_functions(rightop)?
    {
        var_on_left = true;
    } else if match_index_to_operand(run, rightop, indexcol, index)
        && !vars::pull_varnos(run.mcx, leftop)?.is_member(index_relid as i32)
        && !clauses::contain_volatile_functions(leftop)?
    {
        expr_op = lsyscache::get_commutator(expr_op)?;
        if expr_op == 0 {
            return Ok(None);
        }
        var_on_left = false;
    } else {
        return Ok(None);
    }

    match lsyscache::run_memo::get_op_opfamily_strategy(run, expr_op, opfamily)? {
        1 | 2 | 4 | 5 => {
            // BTLess/BTLessEqual/BTGreaterEqual/BTGreater
            expand_indexqual_rowcompare(run, rinfo, indexcol, index, expr_op, var_on_left)
                .map(Some)
        }
        _ => Ok(None),
    }
}

// expand_indexqual_rowcompare (indxpath.c): keep the longest prefix of row
// members matching index columns in the same direction; a lossy prefix keeps
// all matchable rows by flipping </> to <=/>=.
fn expand_indexqual_rowcompare<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rinfo: RinfoId,
    indexcol: usize,
    index: &IndexOptInfo<'mcx>,
    first_op: types_core::Oid,
    var_on_left: bool,
) -> PgResult<IndexClause<'mcx>> {
    use lsyscache::{
        BTGreaterEqualStrategyNumber, BTGreaterStrategyNumber, BTLessEqualStrategyNumber,
        BTLessStrategyNumber,
    };
    let mcx = run.mcx;
    let index_relid = run.root.rel(index.rel.expect("index rel set")).relid;
    let clause = *run.root.expr_node(run.root.rinfo(rinfo).clause);
    let rc = clause.as_row_compare_expr().expect("RowCompareExpr");
    let (var_args, non_var_args) =
        if var_on_left { (&rc.largs, &rc.rargs) } else { (&rc.rargs, &rc.largs) };

    let (mut op_strategy, op_lefttype, op_righttype) =
        lsyscache::run_memo::get_op_opfamily_properties(run, first_op, index.opfamily[indexcol], false)?;

    let mut indexcols: PgVec<'mcx, i16> = PgVec::new_in(mcx);
    indexcols.push(indexcol as i16);
    let mut expr_ops: PgVec<'mcx, types_core::Oid> = PgVec::new_in(mcx);
    expr_ops.push(first_op);
    let mut opfamilies: PgVec<'mcx, types_core::Oid> = PgVec::new_in(mcx);
    opfamilies.push(index.opfamily[indexcol]);
    let mut lefttypes: PgVec<'mcx, types_core::Oid> = PgVec::new_in(mcx);
    lefttypes.push(op_lefttype);
    let mut righttypes: PgVec<'mcx, types_core::Oid> = PgVec::new_in(mcx);
    righttypes.push(op_righttype);

    let mut matching_cols = 1usize;
    while matching_cols < var_args.len() {
        let varop = var_args.nth(matching_cols);
        let constop = non_var_args.nth(matching_cols);
        let mut expr_op = rc.opnos.nth(matching_cols);
        if !var_on_left {
            expr_op = lsyscache::get_commutator(expr_op)?;
            if expr_op == 0 {
                break;
            }
        }
        if vars::pull_varnos(mcx, constop)?.is_member(index_relid as i32) {
            break;
        }
        if clauses::contain_volatile_functions(constop)? {
            break;
        }

        // The Var side can match any key column of the index.
        let mut matched = None;
        for i in 0..index.nkeycolumns as usize {
            if match_index_to_operand(run, varop, i, index)
                && lsyscache::run_memo::get_op_opfamily_strategy(run, expr_op, index.opfamily[i])?
                    == op_strategy
                && index_coll_matches_expr_coll(
                    index.indexcollations[i],
                    rc.inputcollids.nth(matching_cols),
                )
            {
                matched = Some(i);
                break;
            }
        }
        let Some(i) = matched else { break };

        indexcols.push(i as i16);
        let (strat, lefttype, righttype) =
            lsyscache::run_memo::get_op_opfamily_properties(run, expr_op, index.opfamily[i], false)?;
        op_strategy = strat;
        expr_ops.push(expr_op);
        opfamilies.push(index.opfamily[i]);
        lefttypes.push(lefttype);
        righttypes.push(righttype);
        matching_cols += 1;
    }

    let lossy = matching_cols != rc.opnos.len();

    if var_on_left && !lossy {
        let mut indexquals = PgVec::new_in(mcx);
        indexquals.push(rinfo);
        return Ok(IndexClause {
            rinfo: Some(rinfo),
            indexquals,
            lossy: false,
            indexcol: indexcol as i16,
            indexcols,
        });
    }

    let new_ops: PgVec<'mcx, types_core::Oid> = if !lossy
        || op_strategy == BTLessEqualStrategyNumber as i32
        || op_strategy == BTGreaterEqualStrategyNumber as i32
    {
        expr_ops
    } else {
        op_strategy = match op_strategy {
            s if s == BTLessStrategyNumber as i32 => BTLessEqualStrategyNumber as i32,
            s if s == BTGreaterStrategyNumber as i32 => BTGreaterEqualStrategyNumber as i32,
            other => panic!("unexpected strategy number {other}"),
        };
        let mut ops = PgVec::new_in(mcx);
        for k in 0..matching_cols {
            let expr_op = lsyscache::get_opfamily_member(
                opfamilies[k],
                lefttypes[k],
                righttypes[k],
                op_strategy as i16,
            )?;
            assert!(
                expr_op != 0,
                "missing operator {}({},{}) in opfamily {}",
                op_strategy,
                lefttypes[k],
                righttypes[k],
                opfamilies[k]
            );
            ops.push(expr_op);
        }
        ops
    };

    let new_rinfo = if matching_cols > 1 {
        let mut opnos = types_nodes::list::OidList::nil();
        let mut new_opfamilies = types_nodes::list::OidList::nil();
        let mut inputcollids = types_nodes::list::OidList::nil();
        let mut largs = types_nodes::NodeList::nil();
        let mut rargs = types_nodes::NodeList::nil();
        for k in 0..matching_cols {
            opnos.lappend(mcx, new_ops[k])?;
            new_opfamilies.lappend(mcx, rc.opfamilies.nth(k))?;
            inputcollids.lappend(mcx, rc.inputcollids.nth(k))?;
            largs.lappend(mcx, var_args.nth(k))?;
            rargs.lappend(mcx, non_var_args.nth(k))?;
        }
        let new_rc = Node::mk(
            mcx,
            types_nodes::RowCompareExpr {
                cmptype: op_strategy,
                opnos,
                opfamilies: new_opfamilies,
                inputcollids,
                largs,
                rargs,
            },
        )?;
        planner_seams::make_restrictinfo::call(
            run, new_rc, true, false, false, false, 0, relids_empty(), relids_empty(), relids_empty(),
        )?
    } else {
        indexcols.clear();
        let op = planner_seams::make_opclause::call(
            mcx,
            new_ops[0],
            var_args.nth(0),
            non_var_args.nth(0),
            rc.inputcollids.nth(0),
        )?;
        planner_seams::make_restrictinfo::call(
            run, op, true, false, false, false, 0, relids_empty(), relids_empty(), relids_empty(),
        )?
    };

    let mut indexquals = PgVec::new_in(mcx);
    indexquals.push(new_rinfo);
    Ok(IndexClause {
        rinfo: Some(rinfo),
        indexquals,
        lossy,
        indexcol: indexcol as i16,
        indexcols,
    })
}

// match_opclause_to_indexcol (indxpath.c), indexkey-op-const arm.
fn match_opclause_to_indexcol<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rinfo: RinfoId,
    indexcol: usize,
    index: &IndexOptInfo<'mcx>,
) -> PgResult<Option<IndexClause<'mcx>>> {
    let index_relid = run.root.rel(index.rel.expect("index rel set")).relid;
    let opfamily = index.opfamily[indexcol];
    let idxcollation = index.indexcollations[indexcol];

    let clause = *run.root.expr_node(run.root.rinfo(rinfo).clause);
    let op = clause.as_op_expr().expect("OpExpr");
    if op.args.len() != 2 {
        return Ok(None);
    }
    let leftop = op.args.nth(0);
    let rightop = op.args.nth(1);
    let left_matches = match_index_to_operand(run, leftop, indexcol, index);
    let right_matches = match_index_to_operand(run, rightop, indexcol, index);

    if left_matches
        && !relids_is_member(index_relid as i32, &run.root.rinfo(rinfo).right_relids)
        && !clauses::contain_volatile_functions(rightop)?
    {
        if index_coll_matches_expr_coll(idxcollation, op.inputcollid)
            && lsyscache::run_memo::op_in_opfamily(run, op.opno, opfamily)?
        {
            return Ok(Some(IndexClause {
                rinfo: Some(rinfo),
                indexquals: {
                    let mut v = PgVec::new_in(run.mcx);
                    v.push(rinfo);
                    v
                },
                lossy: false,
                indexcol: indexcol as i16,
                indexcols: PgVec::new_in(run.mcx),
            }));
        }
        let opfuncid = lsyscache::run_memo::get_opcode(run, op.opno)?;
        if let Some(ic) = get_index_clause_from_support(run, rinfo, opfuncid, 0, indexcol, index)? {
            return Ok(Some(ic));
        }
    }

    if right_matches
        && !relids_is_member(index_relid as i32, &run.root.rinfo(rinfo).left_relids)
        && !clauses::contain_volatile_functions(leftop)?
    {
        if index_coll_matches_expr_coll(idxcollation, op.inputcollid) {
            let comm_op = lsyscache::run_memo::get_commutator(run, op.opno)?;
            if comm_op != 0 && lsyscache::run_memo::op_in_opfamily(run, comm_op, opfamily)? {
                let commrinfo = planner_seams::commute_restrictinfo::call(run, rinfo, comm_op)?;
                return Ok(Some(IndexClause {
                    rinfo: Some(rinfo),
                    indexquals: {
                        let mut v = PgVec::new_in(run.mcx);
                        v.push(commrinfo);
                        v
                    },
                    lossy: false,
                    indexcol: indexcol as i16,
                    indexcols: PgVec::new_in(run.mcx),
                }));
            }
        }
        let opfuncid = lsyscache::get_opcode(op.opno)?;
        if let Some(ic) = get_index_clause_from_support(run, rinfo, opfuncid, 1, indexcol, index)? {
            return Ok(Some(ic));
        }
    }

    Ok(None)
}

// match_saopclause_to_indexcol (indxpath.c).
fn match_saopclause_to_indexcol<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rinfo: RinfoId,
    indexcol: usize,
    index: &IndexOptInfo<'mcx>,
) -> PgResult<Option<IndexClause<'mcx>>> {
    let index_relid = run.root.rel(index.rel.expect("index rel set")).relid;
    let clause = *run.root.expr_node(run.root.rinfo(rinfo).clause);
    let saop = clause.as_scalar_array_op_expr().expect("ScalarArrayOpExpr");

    if !saop.useOr || saop.args.len() != 2 {
        return Ok(None);
    }
    let leftop = saop.args.nth(0);
    let rightop = saop.args.nth(1);
    let right_relids = vars::pull_varnos(run.mcx, rightop)?;

    if match_index_to_operand(run, leftop, indexcol, index)
        && !right_relids.is_member(index_relid as i32)
        && !clauses::contain_volatile_functions(rightop)?
        && index_coll_matches_expr_coll(index.indexcollations[indexcol], saop.inputcollid)
        && lsyscache::run_memo::op_in_opfamily(run, saop.opno, index.opfamily[indexcol])?
    {
        return Ok(Some(IndexClause {
            rinfo: Some(rinfo),
            indexquals: {
                let mut v = PgVec::new_in(run.mcx);
                v.push(rinfo);
                v
            },
            lossy: false,
            indexcol: indexcol as i16,
            indexcols: PgVec::new_in(run.mcx),
        }));
    }
    Ok(None)
}

// match_orclause_to_indexcol (indxpath.c): an OR of "indexkey op constant"
// arms sharing one operator/collation folds to a non-lossy SAOP indexqual.
fn match_orclause_to_indexcol<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rinfo: RinfoId,
    indexcol: usize,
    index: &IndexOptInfo<'mcx>,
) -> PgResult<Option<IndexClause<'mcx>>> {
    use nodes_core::node_funcs::expr_type;
    use types_core::catalog::RECORDOID;
    let mcx = run.mcx;
    if !index.amsearcharray {
        return Ok(None);
    }
    let index_relid = run.root.rel(index.rel.expect("index rel set")).relid as i32;
    let orclause = *run.root.expr_node(run.root.rinfo(rinfo).clause);

    let mut consts = types_nodes::NodeList::nil();
    let mut index_expr: Option<Node<'mcx>> = None;
    let mut match_opno = 0;
    let mut consttype = 0;
    let mut inputcollid = 0;
    let mut first_time = true;
    let mut have_non_const = false;
    let mut complete = true;
    for arg in &orclause.as_bool_expr().expect("OR clause").args {
        let Some(sub) = arg.as_op_expr() else {
            complete = false;
            break;
        };
        let mut opno = sub.opno;
        if sub.args.len() != 2 {
            complete = false;
            break;
        }
        let leftop = sub.args.nth(0);
        let rightop = sub.args.nth(1);
        let const_expr;
        if match_index_to_operand(run, leftop, indexcol, index)
            && !vars::pull_varnos(mcx, rightop)?.is_member(index_relid)
            && !clauses::contain_volatile_functions(rightop)?
        {
            index_expr = Some(leftop);
            const_expr = rightop;
        } else if match_index_to_operand(run, rightop, indexcol, index)
            && !vars::pull_varnos(mcx, leftop)?.is_member(index_relid)
            && !clauses::contain_volatile_functions(leftop)?
        {
            opno = lsyscache::get_commutator(opno)?;
            if opno == 0 {
                complete = false;
                break;
            }
            index_expr = Some(rightop);
            const_expr = leftop;
        } else {
            complete = false;
            break;
        }

        if first_time {
            match_opno = opno;
            consttype = expr_type(const_expr);
            let arraytype = lsyscache::get_array_type(consttype)?;
            inputcollid = sub.inputcollid;
            if !index_coll_matches_expr_coll(index.indexcollations[indexcol], inputcollid)
                || !lsyscache::run_memo::op_in_opfamily(run, match_opno, index.opfamily[indexcol])?
                || arraytype == 0
                || consttype == RECORDOID
                || expr_type(index_expr.unwrap()) == RECORDOID
            {
                complete = false;
                break;
            }
            first_time = false;
        } else if match_opno != opno
            || inputcollid != sub.inputcollid
            || consttype != expr_type(const_expr)
        {
            complete = false;
            break;
        }

        if const_expr.as_const().is_none() {
            have_non_const = true;
        }
        consts.lappend(mcx, const_expr)?;
    }
    if !complete {
        return Ok(None);
    }
    let Some(index_expr) = index_expr else {
        return Ok(None);
    };

    let saop = clauses::make_saop_expr(
        mcx,
        match_opno,
        index_expr,
        consttype,
        inputcollid,
        inputcollid,
        consts,
        have_non_const,
    )?
    .expect("array type verified on the first arm");
    let mut indexquals = PgVec::new_in(mcx);
    indexquals.push(planner_seams::make_restrictinfo::call(
        run, saop, true, false, false, false, 0, relids_empty(), relids_empty(), relids_empty(),
    )?);
    Ok(Some(IndexClause {
        rinfo: Some(rinfo),
        indexquals,
        lossy: false,
        indexcol: indexcol as i16,
        indexcols: PgVec::new_in(mcx),
    }))
}

// get_index_clause_from_support (indxpath.c): the in-core providers keep
// their native closed-set dispatch; other prosupport functions get the
// SupportRequestIndexCondition through fmgr, C's protocol — the returned
// datum is a List of bare index clauses (NIL declines), lossiness rides
// back on the request.
fn get_index_clause_from_support<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rinfo: RinfoId,
    funcid: u32,
    indexarg: i32,
    indexcol: usize,
    index: &IndexOptInfo<'mcx>,
) -> PgResult<Option<IndexClause<'mcx>>> {
    use planner_seams::PatternType;
    let shape = syscache_seams::pg_proc_cost_shape::call(funcid)?
        .unwrap_or_else(|| panic!("cache lookup failed for function {funcid}"));
    if shape.prosupport == 0 {
        return Ok(None);
    }
    let clause = *run.root.expr_node(run.root.rinfo(rinfo).clause);
    let mut lossy = true;
    let exprs = match shape.prosupport {
        1023 | 1025 | 1364 | 1024 | 6242 => {
            let ptype = match shape.prosupport {
                1023 => PatternType::Like,
                1025 => PatternType::LikeIc,
                1364 => PatternType::Regex,
                1024 => PatternType::RegexIc,
                _ => PatternType::Prefix,
            };
            // like_regex_support: no reverse-match operators, indexkey-on-left
            // only; C accepts the OpExpr and FuncExpr clause forms.
            if indexarg != 0 {
                return Ok(None);
            }
            let (args, inputcollid) = if let Some(op) = clause.as_op_expr() {
                (&op.args, op.inputcollid)
            } else if let Some(f) = clause.as_func_expr() {
                (&f.args, f.inputcollid)
            } else {
                return Ok(None);
            };
            planner_seams::match_pattern_prefix::call(
                run,
                args.nth(0),
                args.nth(1),
                ptype,
                inputcollid,
                index.opfamily[indexcol],
                index.indexcollations[indexcol],
            )?
        }
        // network_subset_support (network.c): SupportRequestIndexCondition.
        1173 => {
            let op = clause.as_op_expr().expect("support request over an OpExpr");
            match_network_function(
                run,
                op.args.nth(0),
                op.args.nth(1),
                indexarg,
                funcid,
                index.opfamily[indexcol],
            )?
        }
        // range_contains_elem_support / elem_contained_by_range_support
        // (rangetypes.c) handle only SupportRequestSimplify: an
        // index-condition request returns NULL.
        6345 | 6346 => return Ok(None),
        prosupport => {
            let mut req = types_nodes::supportnodes::SupportRequestIndexCondition::new(
                funcid,
                Some(clause),
                indexarg,
                indexcol as i32,
                index.opfamily[indexcol],
                index.indexcollations[indexcol],
            );
            let addr = core::ptr::from_mut(&mut req) as usize;
            let result = fmgr_core::oid_function_call1_coll(
                prosupport,
                0,
                datum::Datum::from_usize(addr),
            )?;
            match core::ptr::NonNull::new(result.as_usize() as *mut ()) {
                None => None,
                Some(p) => {
                    // SAFETY: prosupport contract — a non-NIL result is a live
                    // List node of bare clauses built by the support function.
                    let node = unsafe { types_nodes::Node::from_raw(p) };
                    let list = node.as_list().expect("support function returns a List");
                    let mut v = PgVec::new_in(run.mcx);
                    for e in list.iter() {
                        v.push(e);
                    }
                    lossy = req.lossy;
                    Some(v)
                }
            }
        }
    };
    let Some(exprs) = exprs else {
        return Ok(None);
    };
    let mut indexquals = PgVec::new_in(run.mcx);
    for expr in exprs.iter() {
        // make_simple_restrictinfo (restrictinfo.h).
        indexquals.push(planner_seams::make_restrictinfo::call(
            run, *expr, true, false, false, false, 0, relids_empty(), relids_empty(), relids_empty(),
        )?);
    }
    Ok(Some(IndexClause {
        rinfo: Some(rinfo),
        indexquals,
        lossy,
        indexcol: indexcol as i16,
        indexcols: PgVec::new_in(run.mcx),
    }))
}

// match_network_function (network.c).
fn match_network_function<'mcx>(
    run: &mut PlannerRun<'mcx>,
    leftop: Node<'mcx>,
    rightop: Node<'mcx>,
    indexarg: i32,
    funcid: u32,
    opfamily: u32,
) -> PgResult<Option<PgVec<'mcx, Node<'mcx>>>> {
    const F_NETWORK_SUB: u32 = 927;
    const F_NETWORK_SUBEQ: u32 = 928;
    const F_NETWORK_SUP: u32 = 929;
    const F_NETWORK_SUPEQ: u32 = 930;
    match funcid {
        F_NETWORK_SUB if indexarg == 0 => {
            match_network_subset(run, leftop, rightop, false, opfamily)
        }
        F_NETWORK_SUBEQ if indexarg == 0 => {
            match_network_subset(run, leftop, rightop, true, opfamily)
        }
        F_NETWORK_SUP if indexarg == 1 => {
            match_network_subset(run, rightop, leftop, false, opfamily)
        }
        F_NETWORK_SUPEQ if indexarg == 1 => {
            match_network_subset(run, rightop, leftop, true, opfamily)
        }
        _ => Ok(None),
    }
}

// match_network_subset (network.c): key >= scan_first AND key <= scan_last.
fn match_network_subset<'mcx>(
    run: &mut PlannerRun<'mcx>,
    leftop: Node<'mcx>,
    rightop: Node<'mcx>,
    is_eq: bool,
    opfamily: u32,
) -> PgResult<Option<PgVec<'mcx, Node<'mcx>>>> {
    const INETOID: u32 = 869;
    let Some(c) = rightop.as_const() else {
        return Ok(None);
    };
    if c.constisnull {
        return Ok(None);
    }
    let rightopval = c.constvalue;

    let inet_const =
        |run: &mut PlannerRun<'mcx>, v: adt_network::InetValue| -> PgResult<Node<'mcx>> {
            let (img, len) = v.image();
            let copy = mcx::slice_borrow_in(run.mcx, &img[..len])?;
            types_nodes::Node::mk(
                run.mcx,
                types_nodes::primnodes::Const {
                    consttype: INETOID,
                    consttypmod: -1,
                    constcollid: 0,
                    constlen: -1,
                    constvalue: datum::Datum::from_usize(copy.as_ptr() as usize),
                    constisnull: false,
                    constbyval: false,
                    location: -1,
                },
            )
        };

    let cmp1 = if is_eq {
        types_pathnodes::COMPARE_GE
    } else {
        types_pathnodes::COMPARE_GT
    };
    let opr1oid = lsyscache::get_opfamily_member_for_cmptype(opfamily, INETOID, INETOID, cmp1)?;
    if opr1oid == 0 {
        return Ok(None);
    }
    let opr1right = adt_network::network_scan_first(planner_seams::inet_ref::call(rightopval));

    let opr2oid = lsyscache::get_opfamily_member_for_cmptype(
        opfamily,
        INETOID,
        INETOID,
        types_pathnodes::COMPARE_LE,
    )?;
    if opr2oid == 0 {
        return Ok(None);
    }
    let opr2right = adt_network::network_scan_last(planner_seams::inet_ref::call(rightopval))?;

    let mut result: PgVec<'mcx, Node<'mcx>> = mcx::vec_with_capacity_in(run.mcx, 2)?;
    let c1 = inet_const(run, opr1right)?;
    result.push(planner_seams::make_opclause::call(
        run.mcx, opr1oid, leftop, c1, 0,
    )?);
    let c2 = inet_const(run, opr2right)?;
    result.push(planner_seams::make_opclause::call(
        run.mcx, opr2oid, leftop, c2, 0,
    )?);
    Ok(Some(result))
}

// IndexCollMatchesExprColl (indxpath.c).
fn index_coll_matches_expr_coll(idxcollation: u32, exprcollation: u32) -> bool {
    idxcollation == 0 || idxcollation == exprcollation
}

// match_index_to_operand (indxpath.c). strip_noop_phvs runs before Relabel
// stripping; PHV removal can bring RelabelTypes into adjacency
// (indxpath.c:4425-4442). The strip is deep (nested PHVs in the operand),
// gated on a strippable PHV existing so the common case never copies.
pub fn match_index_to_operand<'mcx>(
    run: &PlannerRun<'mcx>,
    mut operand: Node<'mcx>,
    indexcol: usize,
    index: &IndexOptInfo<'_>,
) -> bool {
    operand = vars::strip_noop_phvs(run.mcx, operand)
        .expect("strip_noop_phvs: arena allocation failed");
    while operand.node_tag() == NodeTag::T_RelabelType {
        operand = operand.as_relabel_type().unwrap().arg;
    }
    let indkey = index.indexkeys[indexcol];
    if indkey != 0 {
        let index_relid = run.root.rel(index.rel.expect("index rel set")).relid;
        if let Some(var) = operand.as_var() {
            if var.varno as u32 == index_relid
                && indkey == var.varattno as i32
                && var.varnullingrels.is_empty()
            {
                return true;
            }
        }
    } else {
        let mut pos = 0usize;
        for i in 0..indexcol {
            if index.indexkeys[i] == 0 {
                pos += 1;
            }
        }
        let id = *index
            .indexprs
            .get(pos)
            .expect("wrong number of index expressions");
        let mut indexkey = *run.root.expr_node(id);
        if indexkey.node_tag() == NodeTag::T_RelabelType {
            indexkey = indexkey.as_relabel_type().unwrap().arg;
        }
        if types_nodes::equal(indexkey, operand) {
            return true;
        }
    }
    false
}

// match_pathkeys_to_index (indxpath.c): ORDER BY expressions of the form
// "indexedcol operator pseudoconstant" for a prefix of query_pathkeys, plus
// the zero-based index columns each one uses.
fn match_pathkeys_to_index<'mcx>(
    run: &mut PlannerRun<'mcx>,
    index: &IndexOptInfo<'mcx>,
) -> PgResult<(PgVec<'mcx, types_pathnodes::NodeId>, PgVec<'mcx, i32>)> {
    let mcx = run.mcx;
    let mut orderby_clauses: PgVec<'mcx, types_pathnodes::NodeId> = PgVec::new_in(mcx);
    let mut clause_columns: PgVec<'mcx, i32> = PgVec::new_in(mcx);
    debug_assert!(index.amcanorderbyop);
    let rel = index.rel.expect("index rel set");
    let rel_relids = types_pathnodes::relids::relids_copy(mcx, &run.root.rel(rel).relids);
    let mut pathkeys: PgVec<'mcx, types_pathnodes::PathKey> = PgVec::new_in(mcx);
    pathkeys.extend(run.root.query_pathkeys.iter().copied());
    for pathkey in pathkeys.iter() {
        if pathkey.pk_cmptype != types_pathnodes::COMPARE_LT || pathkey.pk_nulls_first {
            return Ok((orderby_clauses, clause_columns));
        }
        let ec = pathkey.pk_eclass.expect("pathkey eclass set");
        if run.root.ec(ec).ec_has_volatile {
            return Ok((orderby_clauses, clause_columns));
        }
        // Any index column may match each pathkey, not just left-to-right:
        // correct for GiST, moot for single-column SP-GiST.
        let mut found = false;
        for em in equivclass::ec_members_for_relids(run, ec, &rel_relids).iter() {
            if !types_pathnodes::relids::relids_equal(&run.root.em(*em).em_relids, &rel_relids) {
                continue;
            }
            let member_expr = *run.root.expr_node(run.root.em(*em).em_expr);
            for indexcol in 0..index.nkeycolumns as usize {
                if let Some(expr) = match_clause_to_ordering_op(
                    run,
                    index,
                    indexcol,
                    member_expr,
                    pathkey.pk_opfamily,
                )? {
                    let expr_id = run.intern_expr(expr);
                    orderby_clauses.push(expr_id);
                    clause_columns.push(indexcol as i32);
                    found = true;
                    break;
                }
            }
            if found {
                break;
            }
        }
        if !found {
            return Ok((orderby_clauses, clause_columns));
        }
    }
    Ok((orderby_clauses, clause_columns))
}

// match_clause_to_ordering_op (indxpath.c): (indexkey op const) or the
// commuted form, where op is an ordering operator of the column's opfamily
// whose sortfamily is the pathkey's opfamily.
fn match_clause_to_ordering_op<'mcx>(
    run: &mut PlannerRun<'mcx>,
    index: &IndexOptInfo<'mcx>,
    indexcol: usize,
    clause: Node<'mcx>,
    pk_opfamily: u32,
) -> PgResult<Option<Node<'mcx>>> {
    debug_assert!(indexcol < index.nkeycolumns as usize);
    let opfamily = index.opfamily[indexcol];
    let idxcollation = index.indexcollations[indexcol];
    let Some(op) = clause.as_op_expr() else {
        return Ok(None);
    };
    if op.args.len() != 2 {
        return Ok(None);
    }
    let leftop = op.args.nth(0);
    let rightop = op.args.nth(1);
    let mut expr_op = op.opno;
    if !index_coll_matches_expr_coll(idxcollation, op.inputcollid) {
        return Ok(None);
    }

    let commuted;
    if match_index_to_operand(run, leftop, indexcol, index)
        && !vars::contain_var_clause(rightop)?
        && !clauses::contain_volatile_functions(rightop)?
    {
        commuted = false;
    } else if match_index_to_operand(run, rightop, indexcol, index)
        && !vars::contain_var_clause(leftop)?
        && !clauses::contain_volatile_functions(leftop)?
    {
        expr_op = lsyscache::get_commutator(expr_op)?;
        if expr_op == 0 {
            return Ok(None);
        }
        commuted = true;
    } else {
        return Ok(None);
    }

    let sortfamily = lsyscache::run_memo::get_op_opfamily_sortfamily(run, expr_op, opfamily)?;
    if sortfamily != pk_opfamily {
        return Ok(None);
    }

    if !commuted {
        return Ok(Some(clause));
    }
    let newclause = types_nodes::Node::mk(
        run.mcx,
        types_nodes::primnodes::OpExpr {
            opno: expr_op,
            opfuncid: 0,
            opresulttype: op.opresulttype,
            opretset: op.opretset,
            opcollid: op.opcollid,
            inputcollid: op.inputcollid,
            args: types_nodes::list::NodeList::make2(run.mcx, rightop, leftop)?,
            location: op.location,
        },
    )?;
    Ok(Some(newclause))
}

// get_index_paths (indxpath.c). btree has amhasgettuple; the bitmap
// collection feeds create_index_paths' (deferred) bitmap arm.
fn get_index_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    index: &'mcx IndexOptInfo<'mcx>,
    clauses: &IndexClauseSet<'mcx>,
    bitindexpaths: &mut PgVec<'mcx, PathId>,
) -> PgResult<()> {
    let indexpaths = build_index_paths(run, rel, index, clauses, index.predOK.get(), false)?;
    for &ipath in indexpaths.iter() {
        if index.amhasgettuple {
            add_path(run, rel, ipath);
        }
        if index.amhasgetbitmap {
            let (no_pathkeys, selec) = {
                let p = run.root.path(ipath);
                let sel = match p {
                    types_pathnodes::PathNode::IndexPath(ip) => ip.indexselectivity,
                    _ => 1.0,
                };
                (p.base().pathkeys.is_empty(), sel)
            };
            if no_pathkeys || selec < 1.0 {
                bitindexpaths.push(ipath);
            }
        }
    }
    Ok(())
}

// Decide-once mirror of compute_parallel_worker's zero-worker outcomes: the
// partial leg's cost_index inputs are bounded (Mackert-Lohman clamps
// rand_heap_pages <= rel.pages; genericcostestimate clamps index_pages <=
// index.pages), so when these upper bounds already trip the size gates the
// leg's parallel_workers is 0 and build_index_paths would drop the path —
// skipping the second cost_index entirely is plan-identical. C runs the full
// costing every time; the serial point/range lanes must not pay for it.
fn partial_leg_can_get_workers(
    run: &PlannerRun<'_>,
    rel: RelId,
    index: &IndexOptInfo<'_>,
    index_only_scan: bool,
) -> bool {
    if guc_tables::vars::max_parallel_workers_per_gather.read() <= 0 {
        return false;
    }
    let r = run.root.rel(rel);
    // Explicit reloption bypasses the size gates (allpaths.c).
    if r.rel_parallel_workers != -1 {
        return r.rel_parallel_workers > 0;
    }
    if r.reloptkind != types_pathnodes::RELOPT_BASEREL {
        return true;
    }
    // genericcostestimate floors numIndexPages at 1 and clamps it to
    // index.pages, so max(pages, 1) bounds the leg's index_pages from above.
    if (index.pages.max(1) as i64) < ::allpaths::gucs::min_parallel_index_scan_size() as i64 {
        return false;
    }
    // Index-only legs pass rand_heap_pages = -1: the heap gate never applies.
    // Mackert-Lohman's T = max(pages, 1): a 0-page rel still fetches 1 page.
    if !index_only_scan
        && (r.pages.max(1) as i64) < ::allpaths::gucs::min_parallel_table_scan_size() as i64
    {
        return false;
    }
    true
}

// build_index_paths (indxpath.c), ST_ANYSCAN (bitmap=false) and ST_BITMAPSCAN
// (bitmap=true) arms.
fn build_index_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    index: &'mcx IndexOptInfo<'mcx>,
    clauses: &IndexClauseSet<'mcx>,
    useful_predicate: bool,
    bitmap: bool,
) -> PgResult<PgVec<'mcx, PathId>> {
    let mcx = run.mcx;
    let mut result: PgVec<'mcx, PathId> = PgVec::new_in(mcx);

    let mut index_clauses: PgVec<'mcx, IndexClause<'mcx>> = PgVec::new_in(mcx);
    let mut outer_relids = types_pathnodes::relids::relids_copy(mcx, &run.root.rel(rel).lateral_relids);
    for indexcol in 0..index.nkeycolumns as usize {
        for ic in clauses.indexclauses[indexcol].iter() {
            let rid = ic.rinfo.expect("IndexClause rinfo");
            outer_relids = types_pathnodes::relids::relids_union(
                mcx,
                &outer_relids,
                &run.root.rinfo(rid).clause_relids,
            );
            index_clauses.push(ic.clone());
        }
        if index_clauses.is_empty() && !index.amoptionalkey {
            return Ok(result);
        }
    }
    outer_relids =
        types_pathnodes::relids::relids_del_member(mcx, &outer_relids, run.root.rel(rel).relid as i32);

    let cur_relid = run.root.rel(rel).relid;
    let loop_count = get_loop_count(run, cur_relid, &outer_relids)?;

    // has_useful_pathkeys (allpaths.c). Bitmap scans never provide
    // ordering (C ST_BITMAPSCAN: useful_pathkeys = NIL).
    let pathkeys_possibly_useful = !bitmap
        && (!run.root.rel(rel).joininfo.is_empty()
            || run.root.rel(rel).has_eclass_joins
            || !run.root.group_pathkeys.is_empty()
            || !run.root.query_pathkeys.is_empty());
    let index_is_ordered = !index.sortopfamily.is_empty();
    let mut orderbyclauses: PgVec<'mcx, types_pathnodes::NodeId> = PgVec::new_in(mcx);
    let mut orderbyclausecols: PgVec<'mcx, i32> = PgVec::new_in(mcx);
    let useful_pathkeys: PgVec<'mcx, types_pathnodes::PathKey> =
        if index_is_ordered && pathkeys_possibly_useful {
            let index_pathkeys = planner_seams::build_index_pathkeys::call(
                run,
                index,
                types_pathnodes::ForwardScanDirection,
            )?;
            planner_seams::truncate_useless_pathkeys::call(run, rel, &index_pathkeys)?
        } else if index.amcanorderbyop && pathkeys_possibly_useful {
            // A prefix match of query_pathkeys still allows incremental sort
            // over the partially sorted output (C build_index_paths step 2).
            (orderbyclauses, orderbyclausecols) = match_pathkeys_to_index(run, index)?;
            let mut v: PgVec<'mcx, types_pathnodes::PathKey> = PgVec::new_in(mcx);
            v.extend(run.root.query_pathkeys.iter().take(orderbyclauses.len()).copied());
            v
        } else {
            PgVec::new_in(mcx)
        };

    let index_only_scan = !bitmap && check_index_only(run, rel, index);

    let backward_arm = index_is_ordered && pathkeys_possibly_useful;
    // Parallel index scans are never built for bitmap collection (C's
    // ST_BITMAPSCAN exclusion) or parameterized scans (outer_relids != NULL).
    let parallel_arm = index.amcanparallel
        && run.root.rel(rel).consider_parallel
        && types_pathnodes::relids::relids_is_empty(&outer_relids)
        && !bitmap
        && partial_leg_can_get_workers(run, rel, index, index_only_scan);
    let clone_clauses = |src: &PgVec<'mcx, IndexClause<'mcx>>| {
        let mut v: PgVec<'mcx, IndexClause<'mcx>> = PgVec::new_in(mcx);
        v.extend(src.iter().cloned());
        v
    };
    if !index_clauses.is_empty()
        || !useful_pathkeys.is_empty()
        || useful_predicate
        || index_only_scan
    {
        // C shares one clause list across all scan arms; clone only if a
        // later arm still needs it.
        let forward_clauses = if backward_arm || parallel_arm {
            clone_clauses(&index_clauses)
        } else {
            core::mem::replace(&mut index_clauses, PgVec::new_in(mcx))
        };
        let parallel_pathkeys = parallel_arm.then(|| {
            let mut v: PgVec<'mcx, types_pathnodes::PathKey> = PgVec::new_in(mcx);
            v.extend(useful_pathkeys.iter().copied());
            v
        });
        let clone_ints = |src: &PgVec<'mcx, i32>| {
            let mut v: PgVec<'mcx, i32> = PgVec::new_in(mcx);
            v.extend(src.iter().copied());
            v
        };
        let clone_orderbys = |src: &PgVec<'mcx, types_pathnodes::NodeId>| {
            let mut v: PgVec<'mcx, types_pathnodes::NodeId> = PgVec::new_in(mcx);
            v.extend(src.iter().copied());
            v
        };
        let (forward_orderbys, forward_orderbycols) = if parallel_arm {
            (clone_orderbys(&orderbyclauses), clone_ints(&orderbyclausecols))
        } else {
            (
                core::mem::replace(&mut orderbyclauses, PgVec::new_in(mcx)),
                core::mem::replace(&mut orderbyclausecols, PgVec::new_in(mcx)),
            )
        };
        let ipath = pathnode::create_index_path(
            run,
            index,
            forward_clauses,
            forward_orderbys,
            forward_orderbycols,
            useful_pathkeys,
            types_pathnodes::ForwardScanDirection,
            index_only_scan,
            &outer_relids,
            loop_count,
            false,
        )?;
        result.push(ipath);

        if let Some(pathkeys) = parallel_pathkeys {
            let ipath = pathnode::create_index_path(
                run,
                index,
                clone_clauses(&index_clauses),
                orderbyclauses,
                orderbyclausecols,
                pathkeys,
                types_pathnodes::ForwardScanDirection,
                index_only_scan,
                &outer_relids,
                loop_count,
                true,
            )?;
            // Not worth using workers: drop the path (C pfrees it).
            if run.root.path(ipath).base().parallel_workers > 0 {
                pathnode::add_partial_path(run, rel, ipath);
            }
        }
    }

    if backward_arm {
        let index_pathkeys = planner_seams::build_index_pathkeys::call(
            run,
            index,
            types_pathnodes::BackwardScanDirection,
        )?;
        let useful_pathkeys =
            planner_seams::truncate_useless_pathkeys::call(run, rel, &index_pathkeys)?;
        if !useful_pathkeys.is_empty() {
            let backward_clauses = if parallel_arm {
                clone_clauses(&index_clauses)
            } else {
                core::mem::replace(&mut index_clauses, PgVec::new_in(mcx))
            };
            let parallel_pathkeys = parallel_arm.then(|| {
                let mut v: PgVec<'mcx, types_pathnodes::PathKey> = PgVec::new_in(mcx);
                v.extend(useful_pathkeys.iter().copied());
                v
            });
            let ipath = pathnode::create_index_path(
                run,
                index,
                backward_clauses,
                PgVec::new_in(mcx),
                PgVec::new_in(mcx),
                useful_pathkeys,
                types_pathnodes::BackwardScanDirection,
                index_only_scan,
                &outer_relids,
                loop_count,
                false,
            )?;
            result.push(ipath);

            if let Some(pathkeys) = parallel_pathkeys {
                let ipath = pathnode::create_index_path(
                    run,
                    index,
                    index_clauses,
                    PgVec::new_in(mcx),
                    PgVec::new_in(mcx),
                    pathkeys,
                    types_pathnodes::BackwardScanDirection,
                    index_only_scan,
                    &outer_relids,
                    loop_count,
                    true,
                )?;
                if run.root.path(ipath).base().parallel_workers > 0 {
                    pathnode::add_partial_path(run, rel, ipath);
                }
            }
        }
    }

    Ok(result)
}

// check_index_only (indxpath.c).
fn check_index_only(run: &PlannerRun<'_>, rel: RelId, index: &IndexOptInfo<'_>) -> bool {
    if !costsize::gucs::enable_indexonlyscan() {
        return false;
    }
    // Attrs needed above the scan plus indrestrictinfo Vars (quals implied by
    // the index predicate need no recheck, hence not baserestrictinfo), each
    // checked against returnable index columns.
    // C reads the reltarget, not attr_needed: attr_needed is never computed
    // for inheritance child rels.
    let r = run.root.rel(rel);
    let mut needed: mcx::PgVec<'_, i16> = mcx::PgVec::new_in(run.mcx);
    let target = r.pathtarget_id.expect("baserel has a reltarget");
    let exprs =
        types_pathnodes::relids::pgvec_clone_shallow(run.mcx, &run.root.pathtarget(target).exprs);
    let relid = r.relid as i32;
    for &eid in exprs.iter() {
        collect_varattnos(run, *run.root.expr_node(eid), relid, &mut needed);
    }
    let r = run.root.rel(rel);
    for &rid in index.indrestrictinfo.borrow().iter() {
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        collect_varattnos(run, clause, r.relid as i32, &mut needed);
    }
    needed.sort_unstable();
    needed.dedup();

    for attno in needed {
        if attno == 0 {
            return false;
        }
        let mut found = false;
        for c in 0..index.ncolumns as usize {
            if index.indexkeys[c] == attno as i32 && index.canreturn[c] {
                found = true;
                break;
            }
        }
        if !found {
            return false;
        }
    }
    true
}

// pull_varattnos_walker (var.c) over a flat attno vec instead of a Bitmapset
// (check_index_only sorts/dedups after the walk).
struct CollectVarattnos<'a, 'v> {
    relid: i32,
    out: &'a mut mcx::PgVec<'v, i16>,
}

impl<'mcx> nodes_core::NodeWalker<'mcx> for CollectVarattnos<'_, '_> {
    fn visit(&mut self, node: Node<'mcx>) -> types_error::PgResult<bool> {
        if let Some(v) = node.as_var() {
            if v.varno == self.relid && v.varlevelsup == 0 {
                self.out.push(v.varattno);
            }
            return Ok(false);
        }
        assert!(
            node.node_tag() != NodeTag::T_Query,
            "pull_varattnos: unexpected unplanned Query subtree"
        );
        nodes_core::expression_tree_walker(node, self)
    }
}

fn collect_varattnos(
    _run: &PlannerRun<'_>,
    node: Node<'_>,
    relid: i32,
    out: &mut mcx::PgVec<'_, i16>,
) {
    use nodes_core::NodeWalker;
    let mut w = CollectVarattnos { relid, out };
    w.visit(node).expect("collect_varattnos walk is infallible");
}

// Sub-RestrictInfo for one OR arm. C divergence: make_restrictinfo here never
// runs make_sub_restrictinfos (orclause stays None), so the arm rinfos are
// built on first use; the per-arm selectivity memo is scoped to this planning
// pass, the numerics are C's.
pub fn or_arm_rinfo<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parent: RinfoId,
    arm: Node<'mcx>,
) -> PgResult<RinfoId> {
    let mcx = run.mcx;
    let (is_pushed_down, has_clone, is_clone, pseudoconstant, security_level, req, incompat, outer) = {
        let p = run.root.rinfo(parent);
        (
            p.is_pushed_down,
            p.has_clone,
            p.is_clone,
            p.pseudoconstant,
            p.security_level,
            types_pathnodes::relids::relids_copy(mcx, &p.required_relids),
            types_pathnodes::relids::relids_copy(mcx, &p.incompatible_relids),
            types_pathnodes::relids::relids_copy(mcx, &p.outer_relids),
        )
    };
    planner_seams::make_restrictinfo::call(
        run,
        arm,
        is_pushed_down,
        has_clone,
        is_clone,
        pseudoconstant,
        security_level,
        req,
        incompat,
        outer,
    )
}

enum OrArm<'mcx> {
    Simple(RinfoId),
    And(PgVec<'mcx, RinfoId>),
    Group {
        rinfo: RinfoId,
        arm_rids: PgVec<'mcx, RinfoId>,
    },
}

// group_similar_or_args (indxpath.c) over pre-built arm rinfos; the bool is
// C's "some arm matched an index" (groupedArgs != orargs), which gates the
// caller's inner_other_clauses rebuild.
fn group_similar_or_args<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    parent: RinfoId,
    mut arms: PgVec<'mcx, Option<OrArm<'mcx>>>,
) -> PgResult<(PgVec<'mcx, OrArm<'mcx>>, bool)> {
    let mcx = run.mcx;
    let relid = run.root.rel(rel).relid as i32;
    let n = arms.len();

    #[derive(Clone, Copy)]
    struct OrArgIndexMatch {
        indexnum: i32,
        colnum: i32,
        opno: u32,
        inputcollid: u32,
        argindex: i32,
        groupindex: i32,
    }
    let mut matches: PgVec<'mcx, OrArgIndexMatch> = PgVec::new_in(mcx);
    let mut matched = false;
    for i in 0..n {
        let mut m = OrArgIndexMatch {
            indexnum: -1,
            colnum: -1,
            opno: 0,
            inputcollid: 0,
            argindex: i as i32,
            groupindex: i as i32,
        };
        matches.push(m);
        let Some(OrArm::Simple(rid)) = &arms[i] else {
            continue;
        };
        let rid = *rid;
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        let Some(op) = clause.as_op_expr() else {
            continue;
        };
        if op.args.len() != 2 {
            continue;
        }
        let strip = |mut n: Node<'mcx>| {
            while let Some(r) = n.as_relabel_type() {
                n = r.arg;
            }
            n
        };
        let leftop = strip(op.args.nth(0));
        let rightop = strip(op.args.nth(1));
        let (in_left, in_right) = {
            let r = run.root.rinfo(rid);
            (
                relids_is_member(relid, &r.left_relids),
                relids_is_member(relid, &r.right_relids),
            )
        };
        let (opno, nonconst) =
            if in_right && !in_left && !clauses::contain_volatile_functions(leftop)? {
                let comm = lsyscache::get_commutator(op.opno)?;
                if comm == 0 {
                    continue;
                }
                (comm, rightop)
            } else if in_left && !in_right && !clauses::contain_volatile_functions(rightop)? {
                (op.opno, leftop)
            } else {
                continue;
            };
        let nindexes = run.root.rel(rel).indexlist.len();
        'indexes: for indexnum in 0..nindexes {
            let index = run.root.rel(rel).indexlist[indexnum];
            if !index.amhasgetbitmap || !index.amsearcharray {
                continue;
            }
            for colnum in 0..index.nkeycolumns as usize {
                if match_index_to_operand(run, nonconst, colnum, index) {
                    m.indexnum = indexnum as i32;
                    m.colnum = colnum as i32;
                    m.opno = opno;
                    m.inputcollid = op.inputcollid;
                    matched = true;
                    break 'indexes;
                }
            }
        }
        matches[i] = m;
    }

    if !matched {
        let mut result: PgVec<'mcx, OrArm<'mcx>> = PgVec::new_in(mcx);
        for slot in arms.iter_mut() {
            result.push(slot.take().expect("arm present"));
        }
        return Ok((result, false));
    }

    // C's index-only loop counts indexnum over amhasgetbitmap+amsearcharray
    // indexes only when none matched earlier; the numbering above counts all
    // indexes, which is an equally consistent grouping key.
    matches.sort_unstable_by_key(|m| (m.indexnum, m.colnum, m.opno, m.inputcollid, m.argindex));
    for i in 1..n {
        if matches[i].indexnum == matches[i - 1].indexnum
            && matches[i].colnum == matches[i - 1].colnum
            && matches[i].opno == matches[i - 1].opno
            && matches[i].inputcollid == matches[i - 1].inputcollid
            && matches[i].indexnum != -1
        {
            let g = matches[i - 1].groupindex;
            matches[i].groupindex = g;
        }
    }
    matches.sort_unstable_by_key(|m| (m.groupindex, m.argindex));

    let mut result: PgVec<'mcx, OrArm<'mcx>> = PgVec::new_in(mcx);
    let mut group_start = 0usize;
    for i in 1..=n {
        let boundary = i == n
            || matches[i].indexnum != matches[group_start].indexnum
            || matches[i].colnum != matches[group_start].colnum
            || matches[i].opno != matches[group_start].opno
            || matches[i].inputcollid != matches[group_start].inputcollid
            || matches[i].indexnum == -1;
        if !boundary {
            continue;
        }
        if i - group_start == 1 {
            let arm = arms[matches[group_start].argindex as usize]
                .take()
                .expect("arm consumed once");
            result.push(arm);
        } else {
            let mut or_args = types_nodes::NodeList::nil();
            let mut arm_rids: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
            for j in group_start..i {
                let arm = arms[matches[j].argindex as usize]
                    .take()
                    .expect("arm consumed once");
                let OrArm::Simple(arid) = arm else {
                    unreachable!("grouped arms are simple op arms")
                };
                or_args.lappend(mcx, *run.root.expr_node(run.root.rinfo(arid).clause))?;
                arm_rids.push(arid);
            }
            let or_node = clauses::make_orclause(mcx, or_args)?;
            let sub = or_arm_rinfo(run, parent, or_node)?;
            result.push(OrArm::Group {
                rinfo: sub,
                arm_rids,
            });
        }
        group_start = i;
    }
    Ok((result, true))
}

// make_bitmap_paths_for_or_group (indxpath.c).
fn make_bitmap_paths_for_or_group<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    ri: RinfoId,
    arm_rids: &[RinfoId],
    other_clauses: &[RinfoId],
) -> PgResult<PgVec<'mcx, PathId>> {
    let mcx = run.mcx;
    let mut jointlist: PgVec<'mcx, PathId> = PgVec::new_in(mcx);
    let mut jointcost = 0.0f64;
    let indlist = build_paths_for_or(run, rel, core::slice::from_ref(&ri), other_clauses)?;
    if !indlist.is_empty() {
        let bq = choose_bitmap_and(run, rel, &indlist)?;
        jointcost = run.root.path(bq).base().total_cost;
        jointlist.push(bq);
    }
    if !jointlist.is_empty() && other_clauses.is_empty() {
        return Ok(jointlist);
    }
    let mut splitlist: PgVec<'mcx, PathId> = PgVec::new_in(mcx);
    let mut splitcost = 0.0f64;
    let mut split_ok = true;
    for &arid in arm_rids {
        let indlist = build_paths_for_or(run, rel, core::slice::from_ref(&arid), other_clauses)?;
        if indlist.is_empty() {
            split_ok = false;
            break;
        }
        let bq = choose_bitmap_and(run, rel, &indlist)?;
        splitcost += run.root.path(bq).base().total_cost;
        splitlist.push(bq);
    }
    if !split_ok || splitlist.is_empty() {
        Ok(jointlist)
    } else if !jointlist.is_empty() && jointcost < splitcost {
        Ok(jointlist)
    } else {
        Ok(splitlist)
    }
}

// build_paths_for_OR (indxpath.c).
fn build_paths_for_or<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    clauses: &[RinfoId],
    other_clauses: &[RinfoId],
) -> PgResult<PgVec<'mcx, PathId>> {
    let mcx = run.mcx;
    let mut result: PgVec<'mcx, PathId> = PgVec::new_in(mcx);
    let mut all_clause_nodes: Option<PgVec<'mcx, Node<'mcx>>> = None;
    let nindexes = run.root.rel(rel).indexlist.len();
    for i in 0..nindexes {
        let index = run.root.rel(rel).indexlist[i];
        if !index.amhasgetbitmap {
            continue;
        }
        let mut useful_predicate = false;
        if !index.indpred.is_empty() {
            if !index.predOK.get() {
                if all_clause_nodes.is_none() {
                    let mut v: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
                    for &r in clauses.iter().chain(other_clauses.iter()) {
                        v.push(*run.root.expr_node(run.root.rinfo(r).clause));
                    }
                    all_clause_nodes = Some(v);
                }
                let mut indpred: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
                for &pid in index.indpred.iter() {
                    indpred.push(*run.root.expr_node(pid));
                }
                if !planner_seams::predicate_implied_by::call(
                    mcx,
                    &indpred,
                    all_clause_nodes.as_ref().unwrap(),
                    false,
                )? {
                    continue;
                }
                let mut other_nodes: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
                for &r in other_clauses.iter() {
                    other_nodes.push(*run.root.expr_node(run.root.rinfo(r).clause));
                }
                if !planner_seams::predicate_implied_by::call(mcx, &indpred, &other_nodes, false)? {
                    useful_predicate = true;
                }
            }
        }
        let mut clauseset = IndexClauseSet::new(mcx, index.nkeycolumns as usize);
        for &r in clauses {
            match_clause_to_index(run, r, index, &mut clauseset)?;
        }
        // C keeps a clause-less index when its predicate covers the arm.
        if !clauseset.nonempty && !useful_predicate {
            continue;
        }
        for &r in other_clauses {
            match_clause_to_index(run, r, index, &mut clauseset)?;
        }
        let paths = build_index_paths(run, rel, index, &clauseset, useful_predicate, true)?;
        result.extend(paths.iter().copied());
    }
    Ok(result)
}

// generate_bitmap_or_paths (indxpath.c).
pub fn generate_bitmap_or_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    clauses: &[RinfoId],
    other_clauses: &[RinfoId],
) -> PgResult<PgVec<'mcx, PathId>> {
    let mcx = run.mcx;
    let mut result: PgVec<'mcx, PathId> = PgVec::new_in(mcx);
    let mut all_clauses: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
    all_clauses.extend(clauses.iter().copied());
    all_clauses.extend(other_clauses.iter().copied());

    for &rid in clauses {
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        if !clauses::is_orclause(clause) {
            continue;
        }

        let mut prearms: PgVec<'mcx, Option<OrArm<'mcx>>> = PgVec::new_in(mcx);
        for arg in &clause.as_bool_expr().expect("OR clause").args {
            if clauses::is_andclause(arg) {
                let mut andargs: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
                for a in &arg.as_bool_expr().expect("AND clause").args {
                    debug_assert!(!clauses::is_andclause(a), "unflattened AND");
                    andargs.push(or_arm_rinfo(run, rid, a)?);
                }
                prearms.push(Some(OrArm::And(andargs)));
            } else {
                prearms.push(Some(OrArm::Simple(or_arm_rinfo(run, rid, arg)?)));
            }
        }
        let (arms, grouped) = group_similar_or_args(run, rel, rid, prearms)?;
        // Grouped sub-rinfos duplicate rinfo; drop it from the context list so
        // match_clauses_to_index doesn't build de-facto duplicate iclauses.
        let mut inner_other_clauses: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
        if grouped {
            inner_other_clauses.extend(all_clauses.iter().copied().filter(|&x| x != rid));
        }

        let mut pathlist: PgVec<'mcx, PathId> = PgVec::new_in(mcx);
        let mut matched_all = true;
        for arm in arms.iter() {
            let indlist = match arm {
                OrArm::And(andargs) => {
                    let mut il = build_paths_for_or(run, rel, andargs, &all_clauses)?;
                    let sub = generate_bitmap_or_paths(run, rel, andargs, &all_clauses)?;
                    il.extend(sub.iter().copied());
                    il
                }
                OrArm::Simple(arid) => {
                    build_paths_for_or(run, rel, core::slice::from_ref(arid), &all_clauses)?
                }
                OrArm::Group { rinfo, arm_rids } => {
                    let indlist = make_bitmap_paths_for_or_group(
                        run,
                        rel,
                        *rinfo,
                        arm_rids,
                        &inner_other_clauses,
                    )?;
                    if indlist.is_empty() {
                        matched_all = false;
                        break;
                    }
                    pathlist.extend(indlist.iter().copied());
                    continue;
                }
            };
            if indlist.is_empty() {
                matched_all = false;
                break;
            }
            pathlist.push(choose_bitmap_and(run, rel, &indlist)?);
        }
        if matched_all && !pathlist.is_empty() {
            result.push(pathnode::create_bitmap_or_path(run, rel, pathlist)?);
        }
    }
    Ok(result)
}

struct PathClauseUsage<'mcx> {
    path: PathId,
    quals: PgVec<'mcx, Node<'mcx>>,
    preds: PgVec<'mcx, Node<'mcx>>,
    clauseids: types_nodes::bitmapset::Bitmapset<'mcx>,
    unclassifiable: bool,
}

// choose_bitmap_and (indxpath.c): O(N^2) AND-group search over the
// clause-usage-deduplicated candidates.
pub fn choose_bitmap_and<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    paths: &[PathId],
) -> PgResult<PathId> {
    let mcx = run.mcx;
    debug_assert!(!paths.is_empty());
    if paths.len() == 1 {
        return Ok(paths[0]);
    }

    let mut clauselist: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
    let mut infos: PgVec<'mcx, PathClauseUsage<'mcx>> = PgVec::new_in(mcx);
    for &p in paths {
        let info = classify_index_clause_usage(run, p, &mut clauselist)?;
        if info.unclassifiable {
            infos.push(info);
            continue;
        }
        let dup = infos
            .iter()
            .position(|e| !e.unclassifiable && info.clauseids.equal(&e.clauseids));
        match dup {
            Some(i) => {
                let (ncost, _) = costsize::cost_bitmap_tree_node(run, info.path);
                let (ocost, _) = costsize::cost_bitmap_tree_node(run, infos[i].path);
                if ncost < ocost {
                    infos[i] = info;
                }
            }
            None => infos.push(info),
        }
    }
    if infos.len() == 1 {
        return Ok(infos[0].path);
    }

    // path_usage_comparator; sort_by is stable where C's qsort is not — a
    // difference only on exact (cost, selectivity) ties.
    infos.sort_by(|a, b| {
        let (ac, asel) = costsize::cost_bitmap_tree_node(run, a.path);
        let (bc, bsel) = costsize::cost_bitmap_tree_node(run, b.path);
        ac.partial_cmp(&bc)
            .expect("bitmap path cost is not NaN")
            .then(
                asel.partial_cmp(&bsel)
                    .expect("bitmap selectivity is not NaN"),
            )
    });

    let mut bestpaths: PgVec<'mcx, PathId> = PgVec::new_in(mcx);
    let mut bestcost = 0.0;
    for i in 0..infos.len() {
        let mut curpaths: PgVec<'mcx, PathId> = PgVec::new_in(mcx);
        curpaths.push(infos[i].path);
        let mut costsofar = bitmap_scan_cost_est(run, rel, infos[i].path)?;
        let mut qualsofar: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
        qualsofar.extend(infos[i].quals.iter().copied());
        qualsofar.extend(infos[i].preds.iter().copied());
        let mut clauseidsofar = types_nodes::bitmapset::Bitmapset::empty();
        clauseidsofar.add_members(mcx, &infos[i].clauseids)?;
        for j in i + 1..infos.len() {
            if infos[j].clauseids.overlap(&clauseidsofar) {
                continue;
            }
            // A partial index's predicate implied by quals already enforced
            // means it adds no selectivity (choose_bitmap_and preds check).
            let mut redundant = false;
            for k in 0..infos[j].preds.len() {
                let np = infos[j].preds[k];
                if planner_seams::predicate_implied_by::call(mcx, &[np], &qualsofar, false)? {
                    redundant = true;
                    break;
                }
            }
            if redundant {
                continue;
            }
            curpaths.push(infos[j].path);
            let newcost = bitmap_and_cost_est(run, rel, &curpaths)?;
            if newcost < costsofar {
                costsofar = newcost;
                qualsofar.extend(infos[j].quals.iter().copied());
                qualsofar.extend(infos[j].preds.iter().copied());
                clauseidsofar.add_members(mcx, &infos[j].clauseids)?;
            } else {
                curpaths.pop();
            }
        }
        if i == 0 || costsofar < bestcost {
            bestpaths = curpaths;
            bestcost = costsofar;
        }
    }
    if bestpaths.len() == 1 {
        return Ok(bestpaths[0]);
    }
    pathnode::create_bitmap_and_path(run, rel, bestpaths)
}

fn classify_index_clause_usage<'mcx>(
    run: &PlannerRun<'mcx>,
    path: PathId,
    clauselist: &mut PgVec<'mcx, Node<'mcx>>,
) -> PgResult<PathClauseUsage<'mcx>> {
    let mcx = run.mcx;
    let mut quals: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
    let mut preds: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
    find_indexpath_quals(run, path, &mut quals, &mut preds);
    if quals.len() + preds.len() > 100 {
        return Ok(PathClauseUsage {
            path,
            quals,
            preds,
            clauseids: types_nodes::bitmapset::Bitmapset::empty(),
            unclassifiable: true,
        });
    }
    let mut clauseids = types_nodes::bitmapset::Bitmapset::empty();
    for i in 0..quals.len() {
        let pos = find_list_position(quals[i], clauselist);
        clauseids.add_member(mcx, pos as i32)?;
    }
    for i in 0..preds.len() {
        let pos = find_list_position(preds[i], clauselist);
        clauseids.add_member(mcx, pos as i32)?;
    }
    Ok(PathClauseUsage {
        path,
        quals,
        preds,
        clauseids,
        unclassifiable: false,
    })
}

fn find_indexpath_quals<'mcx>(
    run: &PlannerRun<'mcx>,
    path: PathId,
    quals: &mut PgVec<'mcx, Node<'mcx>>,
    preds: &mut PgVec<'mcx, Node<'mcx>>,
) {
    match run.root.path(path) {
        types_pathnodes::PathNode::BitmapAndPath(p) => {
            for i in 0..p.bitmapquals.len() {
                find_indexpath_quals(run, p.bitmapquals[i], quals, preds);
            }
        }
        types_pathnodes::PathNode::BitmapOrPath(p) => {
            for i in 0..p.bitmapquals.len() {
                find_indexpath_quals(run, p.bitmapquals[i], quals, preds);
            }
        }
        types_pathnodes::PathNode::IndexPath(ip) => {
            for ic in ip.indexclauses.iter() {
                let rid = ic.rinfo.expect("IndexClause rinfo");
                quals.push(*run.root.expr_node(run.root.rinfo(rid).clause));
            }
            for &pid in ip.indexinfo.expect("indexinfo set").indpred.iter() {
                preds.push(*run.root.expr_node(pid));
            }
        }
        other => panic!(
            "find_indexpath_quals (indxpath.c): pathtype {}",
            other.base().pathtype
        ),
    }
}

fn find_list_position<'mcx>(node: Node<'mcx>, list: &mut PgVec<'mcx, Node<'mcx>>) -> usize {
    for (i, old) in list.iter().enumerate() {
        if types_nodes::equal(node, *old) {
            return i;
        }
    }
    list.push(node);
    list.len() - 1
}

// bitmap_scan_cost_est (indxpath.c). C costs a throwaway stack BitmapHeapPath;
// the arena copy here is same-lifetime garbage.
fn bitmap_scan_cost_est<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    ipath: PathId,
) -> PgResult<f64> {
    let required_outer = types_pathnodes::relids::relids_copy(
        run.mcx,
        pathnode::path_req_outer(run.root.path(ipath).base()),
    );
    let cur_relid = run.root.rel(rel).relid;
    let loop_count = get_loop_count(run, cur_relid, &required_outer)?;
    let bpath =
        pathnode::create_bitmap_heap_path(run, rel, ipath, &required_outer, loop_count, 0)?;
    Ok(run.root.path(bpath).base().total_cost)
}

// bitmap_and_cost_est (indxpath.c).
fn bitmap_and_cost_est<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    paths: &[PathId],
) -> PgResult<f64> {
    let mut quals: PgVec<'mcx, PathId> = PgVec::new_in(run.mcx);
    quals.extend(paths.iter().copied());
    let apath = pathnode::create_bitmap_and_path(run, rel, quals)?;
    bitmap_scan_cost_est(run, rel, apath)
}

// relation_has_unique_index_ext (indxpath.c): join clauses (outer_is_left
// pre-set by the caller) plus the rel's mergejoinable var-op-pseudoconstant
// baserestrictinfo clauses plus expr/operator pairs. extra_clauses receives
// the baserestrictinfo clauses the proof used (SJE's uclauses).
pub fn relation_has_unique_index_ext<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    restrictlist: &[RinfoId],
    exprlist: &[Node<'mcx>],
    oprlist: &[u32],
    mut extra_clauses: Option<&mut PgVec<'mcx, RinfoId>>,
) -> PgResult<bool> {
    debug_assert!(exprlist.len() == oprlist.len());
    if run.root.rel(rel).indexlist.is_empty() {
        return Ok(false);
    }
    let mut rids: PgVec<'_, RinfoId> = PgVec::new_in(run.mcx);
    rids.extend(restrictlist.iter().copied());
    for i in 0..run.root.rel(rel).baserestrictinfo.len() {
        let rid = run.root.rel(rel).baserestrictinfo[i];
        {
            let ri = run.root.rinfo(rid);
            if ri.mergeopfamilies.is_empty() {
                continue;
            }
        }
        let (left_empty, right_empty) = {
            let ri = run.root.rinfo(rid);
            (
                types_pathnodes::relids::relids_is_empty(&ri.left_relids),
                types_pathnodes::relids::relids_is_empty(&ri.right_relids),
            )
        };
        if left_empty {
            run.root.rinfo_mut(rid).outer_is_left = true;
        } else if right_empty {
            run.root.rinfo_mut(rid).outer_is_left = false;
        } else {
            continue;
        }
        rids.push(rid);
    }
    if rids.is_empty() && exprlist.is_empty() {
        return Ok(false);
    }

    let n_indexes = run.root.rel(rel).indexlist.len();
    for i in 0..n_indexes {
        let ind = run.root.rel(rel).indexlist[i];
        if !ind.unique || !ind.immediate || !ind.indpred.is_empty() {
            continue;
        }
        let mut exprs: PgVec<'mcx, RinfoId> = PgVec::new_in(run.mcx);
        let mut all_matched = true;
        for c in 0..ind.nkeycolumns as usize {
            let mut matched = false;
            for &rid in rids.iter() {
                {
                    let ri = run.root.rinfo(rid);
                    if !ri.mergeopfamilies.iter().any(|&f| f == ind.opfamily[c]) {
                        continue;
                    }
                }
                let (clause_id, outer_is_left) = {
                    let ri = run.root.rinfo(rid);
                    (ri.clause, ri.outer_is_left)
                };
                let clause = *run.root.expr_node(clause_id);
                if !lsyscache::misc::collations_agree_on_equality(
                    ind.indexcollations[c],
                    nodes_core::node_funcs::expr_input_collation(clause),
                )? {
                    continue;
                }
                let o = clause.as_op_expr().expect("mergejoinable clause is an OpExpr");
                let rexpr = if outer_is_left { o.args.nth(1) } else { o.args.nth(0) };
                if match_index_to_operand(run, rexpr, c, ind) {
                    matched = true;
                    if extra_clauses.is_some()
                        && types_pathnodes::relids::relids_num_members(
                            &run.root.rinfo(rid).clause_relids,
                        ) == 1
                    {
                        debug_assert!(
                            types_pathnodes::relids::relids_is_empty(
                                &run.root.rinfo(rid).left_relids
                            ) || types_pathnodes::relids::relids_is_empty(
                                &run.root.rinfo(rid).right_relids
                            )
                        );
                        exprs.push(rid);
                    }
                    break;
                }
            }
            if !matched {
                for (j, &expr) in exprlist.iter().enumerate() {
                    if !match_index_to_operand(run, expr, c, ind) {
                        continue;
                    }
                    if !lsyscache::amop::op_in_opfamily(oprlist[j], ind.opfamily[c])? {
                        continue;
                    }
                    if !lsyscache::misc::collations_agree_on_equality(
                        ind.indexcollations[c],
                        nodes_core::node_funcs::expr_collation(expr),
                    )? {
                        continue;
                    }
                    matched = true;
                    break;
                }
            }
            if !matched {
                all_matched = false;
                break;
            }
        }
        if all_matched {
            if let Some(out) = extra_clauses.as_deref_mut() {
                *out = exprs;
            }
            return Ok(true);
        }
    }
    Ok(false)
}
