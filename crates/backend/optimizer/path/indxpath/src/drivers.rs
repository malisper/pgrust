//! The per-relation index-path driver functions (indxpath.c).

use alloc::vec::Vec;

use ::mcx::Mcx;
use ::types_error::PgError;
use pathnodes::{
    EcId, IndexClause, IndexOptInfo, NodeId, PathId, PlannerInfo, RelId, Relids,
    BackwardScanDirection, ForwardScanDirection,
};

// pathkeys.c is ported (backend-optimizer-path-pathkeys); call it directly.
use pathkeys::{
    build_index_pathkeys, has_useful_pathkeys, truncate_useless_pathkeys,
};
use pathnode_seams as pathnode;

use crate::bitmap::{choose_bitmap_and, generate_bitmap_or_paths, ScanTypeControl};
use crate::matchers::IndexClauseSet;
use crate::cost::get_loop_count;
use crate::matchers::{
    match_eclass_clauses_to_index, match_join_clauses_to_index, match_restriction_clauses_to_index,
};
use crate::predicates::check_index_only;
use crate::pathkeys::match_pathkeys_to_index;
use crate::util::{relids_add_members, relids_copy, relids_is_subset, relids_union};

/* ==========================================================================
 * create_index_paths — the top-level entry point.
 * ======================================================================== */

/// `create_index_paths(root, rel)` (indxpath.c:241) — generate all index paths
/// (plain + bitmap) for the relation and submit them to the rel's pathlist.
pub fn create_index_paths<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &::pathnodes::planner_run::PlannerRun<'mcx>,
    rel: RelId,
) -> Result<(), PgError> {
    // Skip the whole mess if no indexes.
    if root.rel(rel).indexlist.is_empty() {
        return Ok(());
    }

    // Bitmap paths are collected and then dealt with at the end.
    let mut bitindexpaths: Vec<PathId> = Vec::new();
    let mut bitjoinpaths: Vec<PathId> = Vec::new();
    let mut joinorclauses: Vec<::pathnodes::RinfoId> = Vec::new();

    // Examine each index in turn.
    let nindexes = root.rel(rel).indexlist.len();
    for idx in 0..nindexes {
        let index = root.rel(rel).indexlist[idx].clone();

        // Ignore partial indexes that do not match the query.
        if !index.indpred.is_empty() && !index.predOK {
            continue;
        }

        // Identify the restriction clauses that can match the index.
        let mut rclauseset = IndexClauseSet::new(index.nkeycolumns as usize);
        match_restriction_clauses_to_index(mcx, root, &index, &mut rclauseset)?;

        // Build index paths from the restriction clauses (non-parameterized).
        get_index_paths(mcx, root, run, rel, &index, &rclauseset, &mut bitindexpaths)?;

        // Identify the join clauses that can match the index; collect join OR
        // clauses for later.
        let mut jclauseset = IndexClauseSet::new(index.nkeycolumns as usize);
        match_join_clauses_to_index(
            mcx,
            root,
            rel,
            &index,
            &mut jclauseset,
            &mut joinorclauses,
        )?;

        // Look for EquivalenceClasses that can generate joinclauses matching it.
        let mut eclauseset = IndexClauseSet::new(index.nkeycolumns as usize);
        match_eclass_clauses_to_index(mcx, root, run, &index, &mut eclauseset)?;

        // If we found any plain or eclass join clauses, build parameterized
        // index paths using them.
        if jclauseset.nonempty || eclauseset.nonempty {
            consider_index_join_clauses(
                mcx,
                root,
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

    // Generate BitmapOrPaths for suitable OR-clauses in the restriction list.
    {
        let baserestrictinfo = root.rel(rel).baserestrictinfo.clone();
        let indexpaths = generate_bitmap_or_paths(mcx, root, run, rel, &baserestrictinfo, &[])?;
        bitindexpaths.extend(indexpaths);
    }

    // Likewise for OR-clauses in the joinclause list.
    {
        let baserestrictinfo = root.rel(rel).baserestrictinfo.clone();
        let indexpaths =
            generate_bitmap_or_paths(mcx, root, run, rel, &joinorclauses, &baserestrictinfo)?;
        bitjoinpaths.extend(indexpaths);
    }

    // If we found anything usable, generate a BitmapHeapPath for the most
    // promising combination of restriction bitmap index paths.
    if !bitindexpaths.is_empty() {
        let bitmapqual = choose_bitmap_and(mcx, root, run, rel, bitindexpaths.clone())?;
        let lateral_relids = relids_copy(&root.rel(rel).lateral_relids);
        let bpath =
            pathnode::create_bitmap_heap_path::call(root, run, rel, bitmapqual, &lateral_relids, 1.0, 0)?;
        pathnode::add_path::call(root, rel, bpath)?;

        // Create a partial bitmap heap path.
        if root.rel(rel).consider_parallel && root.rel(rel).lateral_relids.is_none() {
            costsize_seams::create_partial_bitmap_paths::call(
                root, run, rel, bitmapqual,
            )?;
        }
    }

    // Likewise generate BitmapHeapPaths for each distinct parameterization among
    // the join bitmap index paths.
    if !bitjoinpaths.is_empty() {
        // Identify each distinct parameterization seen in bitjoinpaths.
        let mut all_path_outers: Vec<Relids> = Vec::new();
        for &path in &bitjoinpaths {
            let required_outer = path_req_outer(root, path);
            // list_append_unique (by bms_equal).
            if !all_path_outers
                .iter()
                .any(|o| crate::util::relids_equal(o, &required_outer))
            {
                all_path_outers.push(required_outer);
            }
        }

        // For each distinct parameterization set ...
        for max_outers in all_path_outers {
            // Identify all bitmap join paths needing no more than that.
            let mut this_path_set: Vec<PathId> = Vec::new();
            for &path in &bitjoinpaths {
                if relids_is_subset(&path_req_outer(root, path), &max_outers) {
                    this_path_set.push(path);
                }
            }
            // Add in restriction bitmap paths, usable with any join paths.
            this_path_set.extend(bitindexpaths.iter().copied());

            // Select best AND combination for this parameterization.
            let bitmapqual = choose_bitmap_and(mcx, root, run, rel, this_path_set)?;
            let required_outer = path_req_outer(root, bitmapqual);
            let relid = root.rel(rel).relid;
            let loop_count = get_loop_count(run, root, relid, &required_outer)?;
            let bpath = pathnode::create_bitmap_heap_path::call(
                root,
                run,
                rel,
                bitmapqual,
                &required_outer,
                loop_count,
                0,
            )?;
            pathnode::add_path::call(root, rel, bpath)?;
        }
    }

    Ok(())
}

/// `PATH_REQ_OUTER(path)` — the path's required-outer relids (its param_info's
/// `ppi_req_outer`, or empty).
fn path_req_outer(root: &PlannerInfo, path: PathId) -> Relids {
    root.path(path)
        .base()
        .param_info
        .as_ref()
        .map(|ppi| relids_copy(&ppi.ppi_req_outer))
        .unwrap_or(None)
}

/* ==========================================================================
 * consider_index_join_clauses / _outer_rels / get_join_index_paths.
 * ======================================================================== */

/// `consider_index_join_clauses(...)` (indxpath.c:437) — decide which
/// parameterized index paths to build from the index's join clauses.
pub fn consider_index_join_clauses<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &::pathnodes::planner_run::PlannerRun<'mcx>,
    rel: RelId,
    index: &IndexOptInfo,
    rclauseset: &IndexClauseSet,
    jclauseset: &IndexClauseSet,
    eclauseset: &IndexClauseSet,
    bitindexpaths: &mut Vec<PathId>,
) -> Result<(), PgError> {
    let mut considered_clauses = 0i32;
    let mut considered_relids: Vec<Relids> = Vec::new();

    let nkeycolumns = index.nkeycolumns as usize;
    for indexcol in 0..nkeycolumns {
        // Consider each applicable simple join clause.
        considered_clauses += jclauseset.indexclauses[indexcol].len() as i32;
        let jcol = jclauseset.indexclauses[indexcol].clone();
        consider_index_join_outer_rels(
            mcx,
            root,
            run,
            rel,
            index,
            rclauseset,
            jclauseset,
            eclauseset,
            bitindexpaths,
            &jcol,
            considered_clauses,
            &mut considered_relids,
        )?;
        // Consider each applicable eclass join clause.
        considered_clauses += eclauseset.indexclauses[indexcol].len() as i32;
        let ecol = eclauseset.indexclauses[indexcol].clone();
        consider_index_join_outer_rels(
            mcx,
            root,
            run,
            rel,
            index,
            rclauseset,
            jclauseset,
            eclauseset,
            bitindexpaths,
            &ecol,
            considered_clauses,
            &mut considered_relids,
        )?;
    }
    Ok(())
}

/// `consider_index_join_outer_rels(...)` (indxpath.c:503) — generate
/// parameterized paths based on clause relids in the clause list.
pub fn consider_index_join_outer_rels<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &::pathnodes::planner_run::PlannerRun<'mcx>,
    rel: RelId,
    index: &IndexOptInfo,
    rclauseset: &IndexClauseSet,
    jclauseset: &IndexClauseSet,
    eclauseset: &IndexClauseSet,
    bitindexpaths: &mut Vec<PathId>,
    indexjoinclauses: &[IndexClause],
    considered_clauses: i32,
    considered_relids: &mut Vec<Relids>,
) -> Result<(), PgError> {
    // Examine relids of each joinclause in the given list.
    for iclause in indexjoinclauses {
        let rinfo_id = iclause.rinfo.expect("IndexClause without rinfo");
        let clause_relids = relids_copy(&root.rinfo(rinfo_id).clause_relids);
        let parent_ec: Option<EcId> = root.rinfo(rinfo_id).parent_ec;

        // If we already tried its relids set, no need to do so again.
        if considered_relids
            .iter()
            .any(|r| crate::util::relids_equal(r, &clause_relids))
        {
            continue;
        }

        // Generate the union of this clause's relids with each previously-tried
        // set (capped at 10 * considered_clauses).
        let num_considered_relids = considered_relids.len();
        for pos in 0..num_considered_relids {
            let oldrelids = relids_copy(&considered_relids[pos]);

            // If either is a subset of the other, no new set is possible.
            if !relids_subset_is_different(&clause_relids, &oldrelids) {
                continue;
            }

            // Skip if any clause derived from the same eclass would already be
            // included when using oldrelids.
            if parent_ec.is_some()
                && eclass_already_used(root, parent_ec, &oldrelids, indexjoinclauses)
            {
                continue;
            }

            // Heuristic limit on the number of relid sets considered.
            if considered_relids.len() as i32 >= 10 * considered_clauses {
                break;
            }

            // OK, try the union set.
            let union = relids_union(&clause_relids, &oldrelids);
            get_join_index_paths(
                mcx,
                root,
                run,
                rel,
                index,
                rclauseset,
                jclauseset,
                eclauseset,
                bitindexpaths,
                union,
                considered_relids,
            )?;
        }

        // Also try this set of relids by itself.
        get_join_index_paths(
            mcx,
            root,
            run,
            rel,
            index,
            rclauseset,
            jclauseset,
            eclauseset,
            bitindexpaths,
            clause_relids,
            considered_relids,
        )?;
    }
    Ok(())
}

/// `get_join_index_paths(...)` (indxpath.c:606) — generate index paths using
/// clauses from the specified outer relations; record `relids` in
/// `considered_relids`.
pub fn get_join_index_paths<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &::pathnodes::planner_run::PlannerRun<'mcx>,
    rel: RelId,
    index: &IndexOptInfo,
    rclauseset: &IndexClauseSet,
    jclauseset: &IndexClauseSet,
    eclauseset: &IndexClauseSet,
    bitindexpaths: &mut Vec<PathId>,
    relids: Relids,
    considered_relids: &mut Vec<Relids>,
) -> Result<(), PgError> {
    // If we already considered this relids set, don't repeat the work.
    if considered_relids
        .iter()
        .any(|r| crate::util::relids_equal(r, &relids))
    {
        return Ok(());
    }

    // Identify indexclauses usable with this relids set.
    let nkeycolumns = index.nkeycolumns as usize;
    let mut clauseset = IndexClauseSet::new(nkeycolumns);

    for indexcol in 0..nkeycolumns {
        // First find applicable simple join clauses.
        for iclause in &jclauseset.indexclauses[indexcol] {
            let rinfo_id = iclause.rinfo.expect("IndexClause without rinfo");
            if relids_is_subset(&root.rinfo(rinfo_id).clause_relids, &relids) {
                clauseset.indexclauses[indexcol].push(iclause.clone());
            }
        }

        // Add applicable eclass join clauses (at most one; they're redundant).
        for iclause in &eclauseset.indexclauses[indexcol] {
            let rinfo_id = iclause.rinfo.expect("IndexClause without rinfo");
            if relids_is_subset(&root.rinfo(rinfo_id).clause_relids, &relids) {
                clauseset.indexclauses[indexcol].push(iclause.clone());
                break;
            }
        }

        // Add restriction clauses.
        clauseset.indexclauses[indexcol]
            .extend(rclauseset.indexclauses[indexcol].iter().cloned());

        if !clauseset.indexclauses[indexcol].is_empty() {
            clauseset.nonempty = true;
        }
    }

    // We should have found something.
    debug_assert!(clauseset.nonempty);

    // Build index path(s) using the collected set of clauses.
    get_index_paths(mcx, root, run, rel, index, &clauseset, bitindexpaths)?;

    // Remember we considered paths for this set of relids.
    considered_relids.push(relids);
    Ok(())
}

/// `eclass_already_used(parent_ec, oldrelids, indexjoinclauses)`
/// (indxpath.c:684) — true if any join clause usable with `oldrelids` was
/// generated from the specified equivalence class.
pub fn eclass_already_used(
    root: &PlannerInfo,
    parent_ec: Option<EcId>,
    oldrelids: &Relids,
    indexjoinclauses: &[IndexClause],
) -> bool {
    for iclause in indexjoinclauses {
        let rinfo_id = iclause.rinfo.expect("IndexClause without rinfo");
        let rinfo = root.rinfo(rinfo_id);
        if rinfo.parent_ec == parent_ec
            && relids_is_subset(&rinfo.clause_relids, oldrelids)
        {
            return true;
        }
    }
    false
}

/* ==========================================================================
 * get_index_paths + build_index_paths.
 * ======================================================================== */

/// `get_index_paths(root, rel, index, clauses, bitindexpaths)`
/// (indxpath.c:717) — construct IndexPaths from the clauses, sending plain ones
/// to `add_path` and bitmap-capable ones to `bitindexpaths`. Handles the
/// SAOP-native vs. non-native split.
pub fn get_index_paths<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &::pathnodes::planner_run::PlannerRun<'mcx>,
    rel: RelId,
    index: &IndexOptInfo,
    clauses: &IndexClauseSet,
    bitindexpaths: &mut Vec<PathId>,
) -> Result<(), PgError> {
    let mut skip_nonnative_saop = false;

    // Build simple index paths (allow SAOP only if native).
    let indexpaths = build_index_paths(
        mcx,
        root,
        run,
        rel,
        index,
        clauses,
        index.predOK,
        ScanTypeControl::AnyScan,
        Some(&mut skip_nonnative_saop),
    )?;

    // Submit the ones forming plain IndexScans to add_path; collect the
    // bitmap-usable ones.
    for ipath in indexpaths {
        if index.amhasgettuple {
            pathnode::add_path::call(root, rel, ipath)?;
        }

        if index.amhasgetbitmap {
            let (no_pathkeys, selec) = {
                let p = root.path(ipath);
                let base = p.base();
                let sel = match p {
                    ::pathnodes::PathNode::IndexPath(ip) => ip.indexselectivity,
                    _ => 1.0,
                };
                (base.pathkeys.is_empty(), sel)
            };
            if no_pathkeys || selec < 1.0 {
                bitindexpaths.push(ipath);
            }
        }
    }

    // If there were non-native SAOP clauses, generate bitmap scan paths.
    if skip_nonnative_saop {
        let indexpaths = build_index_paths(
            mcx,
            root,
            run,
            rel,
            index,
            clauses,
            false,
            ScanTypeControl::BitmapScan,
            None,
        )?;
        bitindexpaths.extend(indexpaths);
    }

    Ok(())
}

/// `build_index_paths(root, rel, index, clauses, useful_predicate, scantype,
/// skip_nonnative_saop)` (indxpath.c:811) — construct zero or more IndexPaths
/// (and partial IndexPaths) for the index and clause set. Returns the candidate
/// `IndexPath` handles (NOT yet added to the rel).
pub fn build_index_paths<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &::pathnodes::planner_run::PlannerRun<'mcx>,
    rel: RelId,
    index: &IndexOptInfo,
    clauses: &IndexClauseSet,
    useful_predicate: bool,
    scantype: ScanTypeControl,
    mut skip_nonnative_saop: Option<&mut bool>,
) -> Result<Vec<PathId>, PgError> {
    debug_assert!(skip_nonnative_saop.is_some() || scantype == ScanTypeControl::BitmapScan);

    let mut result: Vec<PathId> = Vec::new();

    // Check that the index supports the desired scan type(s).
    match scantype {
        ScanTypeControl::IndexScan => {
            if !index.amhasgettuple {
                return Ok(result);
            }
        }
        ScanTypeControl::BitmapScan => {
            if !index.amhasgetbitmap {
                return Ok(result);
            }
        }
        ScanTypeControl::AnyScan => {}
    }

    // 1. Combine the per-column IndexClause lists into an overall list, ordered
    // by index key. Build the outer_relids set.
    let mut index_clauses: Vec<IndexClause> = Vec::new();
    let mut outer_relids: Relids = relids_copy(&root.rel(rel).lateral_relids);
    let nkeycolumns = index.nkeycolumns as usize;
    for indexcol in 0..nkeycolumns {
        for iclause in &clauses.indexclauses[indexcol] {
            let rinfo_id = iclause.rinfo.expect("IndexClause without rinfo");
            let clause_is_saop = {
                let clause_id = root.rinfo(rinfo_id).clause;
                root.node(clause_id).is_scalararrayopexpr()
            };
            if skip_nonnative_saop.is_some() && !index.amsearcharray && clause_is_saop {
                // Omit this clause and tell the caller.
                if let Some(flag) = skip_nonnative_saop.as_deref_mut() {
                    *flag = true;
                }
                continue;
            }

            // OK to include this clause.
            index_clauses.push(iclause.clone());
            let cr = relids_copy(&root.rinfo(rinfo_id).clause_relids);
            outer_relids = relids_add_members(outer_relids, &cr);
        }

        // If no clauses match the first index column, check amoptionalkey.
        if index_clauses.is_empty() && !index.amoptionalkey {
            return Ok(result);
        }
    }

    // We do not want the index's rel itself listed in outer_relids.
    outer_relids = relids_del_member(outer_relids, root.rel(rel).relid as i32);

    // Compute loop_count for cost estimation.
    let relid = root.rel(rel).relid;
    let loop_count = get_loop_count(run, root, relid, &outer_relids)?;

    // 2. Compute index ordering pathkeys, if relevant.
    let pathkeys_possibly_useful = scantype != ScanTypeControl::BitmapScan
        && has_useful_pathkeys(root, root.rel(rel));
    let index_is_ordered = !index.sortopfamily.is_empty();

    let mut orderbyclauses: Vec<NodeId> = Vec::new();
    let mut orderbyclausecols: Vec<i32> = Vec::new();
    let useful_pathkeys: Vec<::pathnodes::PathKey>;

    if index_is_ordered && pathkeys_possibly_useful {
        let index_pathkeys = build_index_pathkeys(root, mcx, index, ForwardScanDirection);
        useful_pathkeys = truncate_useless_pathkeys(root, rel, &index_pathkeys);
        // orderbyclauses / orderbyclausecols stay NIL.
    } else if index.amcanorderbyop && pathkeys_possibly_useful {
        // Generate ordering operators for query_pathkeys (or a prefix).
        let query_pathkeys = root.query_pathkeys.clone();
        let (oc, occ) = match_pathkeys_to_index(mcx, root, index, &query_pathkeys)?;
        orderbyclauses = oc;
        orderbyclausecols = occ;
        if root.query_pathkeys.len() == orderbyclauses.len() {
            useful_pathkeys = root.query_pathkeys.clone();
        } else {
            useful_pathkeys = root.query_pathkeys[..orderbyclauses.len()].to_vec();
        }
    } else {
        useful_pathkeys = Vec::new();
    }

    // 3. Check if an index-only scan is possible.
    let index_only_scan = scantype != ScanTypeControl::BitmapScan
        && check_index_only(mcx, root, rel, index)?;

    // 4. Generate a (forward) indexscan path if worthwhile.
    if !index_clauses.is_empty()
        || !useful_pathkeys.is_empty()
        || useful_predicate
        || index_only_scan
    {
        let ipath = pathnode::create_index_path::call(
            root,
            run,
            alloc::boxed::Box::new(index.clone()),
            index_clauses.clone(),
            orderbyclauses.clone(),
            orderbyclausecols.clone(),
            useful_pathkeys.clone(),
            ForwardScanDirection,
            index_only_scan,
            &outer_relids,
            loop_count,
            false,
        )?;
        result.push(ipath);

        // Consider a parallel index scan.
        if index.amcanparallel
            && root.rel(rel).consider_parallel
            && outer_relids.is_none()
            && scantype != ScanTypeControl::BitmapScan
        {
            let ipath = pathnode::create_index_path::call(
                root,
                run,
                alloc::boxed::Box::new(index.clone()),
                index_clauses.clone(),
                orderbyclauses.clone(),
                orderbyclausecols.clone(),
                useful_pathkeys.clone(),
                ForwardScanDirection,
                index_only_scan,
                &outer_relids,
                loop_count,
                true,
            )?;
            // If it's not worth parallel workers, just drop it.
            if root.path(ipath).base().parallel_workers > 0 {
                pathnode::add_partial_path::call(root, rel, ipath)?;
            }
        }
    }

    // 5. If the index is ordered, a backwards scan might be interesting.
    if index_is_ordered && pathkeys_possibly_useful {
        let index_pathkeys = build_index_pathkeys(root, mcx, index, BackwardScanDirection);
        let useful_pathkeys = truncate_useless_pathkeys(root, rel, &index_pathkeys);
        if !useful_pathkeys.is_empty() {
            let ipath = pathnode::create_index_path::call(
                root,
                run,
                alloc::boxed::Box::new(index.clone()),
                index_clauses.clone(),
                Vec::new(),
                Vec::new(),
                useful_pathkeys.clone(),
                BackwardScanDirection,
                index_only_scan,
                &outer_relids,
                loop_count,
                false,
            )?;
            result.push(ipath);

            // Consider a parallel index scan.
            if index.amcanparallel
                && root.rel(rel).consider_parallel
                && outer_relids.is_none()
                && scantype != ScanTypeControl::BitmapScan
            {
                let ipath = pathnode::create_index_path::call(
                    root,
                    run,
                    alloc::boxed::Box::new(index.clone()),
                    index_clauses.clone(),
                    Vec::new(),
                    Vec::new(),
                    useful_pathkeys.clone(),
                    BackwardScanDirection,
                    index_only_scan,
                    &outer_relids,
                    loop_count,
                    true,
                )?;
                if root.path(ipath).base().parallel_workers > 0 {
                    pathnode::add_partial_path::call(root, rel, ipath)?;
                }
            }
        }
    }

    Ok(result)
}

/* ---- Relids helpers used only by the drivers ---------------------------- */

/// `bms_subset_compare(a, b) != BMS_DIFFERENT` — true when one set is a subset
/// of (or equal to) the other.
fn relids_subset_is_different(a: &Relids, b: &Relids) -> bool {
    // BMS_DIFFERENT iff neither is a subset of the other.
    !(relids_is_subset(a, b) || relids_is_subset(b, a))
}

/// `bms_del_member(a, x)` over the planner `Relids` (clear bit `x`).
fn relids_del_member(a: Relids, x: i32) -> Relids {
    if x < 0 {
        return a;
    }
    let mut words: Vec<u64> = match a {
        None => return None,
        Some(b) => b.words,
    };
    let wnum = (x / 64) as usize;
    if wnum < words.len() {
        words[wnum] &= !(1u64 << (x % 64));
    }
    while words.last() == Some(&0) {
        words.pop();
    }
    if words.is_empty() {
        None
    } else {
        Some(alloc::boxed::Box::new(::pathnodes::Bitmapset { words }))
    }
}
