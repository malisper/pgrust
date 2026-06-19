#![no_std]
#![forbid(unsafe_code)]
#![allow(non_snake_case)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::collapsible_else_if)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::needless_late_init)]

//! Safe-Rust port of `src/backend/optimizer/path/joinpath.c` (postgres-18.3):
//! the join-path enumerator.
//!
//! Given a join relation and its two component rels, joinpath.c considers every
//! join *method* (nestloop, mergejoin, hashjoin, and their parallel/partial
//! variants) for the pair and submits the survivors to the joinrel's pathlist
//! ([`add_paths_to_joinrel`]).
//!
//! # Arena model
//!
//! Path inputs are [`PathId`] handles into the
//! [`PlannerInfo`](types_pathnodes::PlannerInfo) arena (the C `Path *`);
//! `root.path(id)` / `root.rel(id)` / `root.rinfo(id)` recover the node. The
//! path constructors allocate into the arena and return the new `PathId`;
//! `add_path` consumes a `PathId` into the joinrel. The entire enumeration
//! control flow ports 1:1; everything crossing a subsystem boundary crosses
//! through a seam (pathnode.c / costsize.c / pathkeys.c / joininfo.c /
//! restrictinfo.c / lsyscache.c / execAmi.c / FDW + extension hooks / the
//! bundled memoize cache-key analysis), plus the `relids_*` set algebra.
//!
//! Allocating functions take an [`Mcx`](mcx::Mcx) and return
//! [`PgResult`](types_error::PgResult): in C every `palloc` can
//! `ereport(ERROR, ERRCODE_OUT_OF_MEMORY)`, and that OOM channel is part of the
//! C failure surface. Transient lists (clause/pathkey working sets) are charged
//! to the passed context, fallibly.

extern crate alloc;

use alloc::vec::Vec;

use mcx::{Mcx, PgVec};
use types_error::PgResult;

use backend_optimizer_path_joinpath_seams as jp;
use backend_optimizer_util_relnode_seams as bms;
use backend_utils_cache_lsyscache_seams as lsc;

use types_pathnodes::optimizer_plan::{CostSelector, JoinPathExtraData, SemiAntiJoinFactors};
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{
    JoinType, NodeId, PathId, PathKey, PlannerInfo, RelId, Relids, RestrictInfo, RinfoId,
    SpecialJoinInfo, JOIN_ANTI, JOIN_FULL, JOIN_INNER, JOIN_LEFT, JOIN_RIGHT, JOIN_RIGHT_ANTI,
    JOIN_RIGHT_SEMI, JOIN_SEMI, JOIN_UNIQUE_INNER, JOIN_UNIQUE_OUTER, RELOPT_OTHER_JOINREL,
};

use types_core::primitive::{InvalidOid, Oid};

/// The join-method enable GUCs (`optimizer/cost.h`: `enable_mergejoin`,
/// `enable_hashjoin`, `enable_material`, `enable_parallel_hash`,
/// `enable_memoize`), passed in by value. Per the no-ambient-global-seams rule
/// these per-backend planner knobs are explicit parameters, not zero-arg getter
/// seams; the join-search driver reads them off its own facet and hands them
/// down. `enable_memoize` is read by `get_memoize_path` (now in-crate), so it
/// rides along here too.
#[derive(Clone, Copy, Debug)]
pub struct JoinEnableFlags {
    pub enable_mergejoin: bool,
    pub enable_hashjoin: bool,
    pub enable_material: bool,
    pub enable_parallel_hash: bool,
    pub enable_memoize: bool,
}

/// `IS_OUTER_JOIN(jointype)` (nodes.h) — LEFT/FULL/RIGHT/ANTI/RIGHT_ANTI.
#[inline]
fn is_outer_join(jointype: JoinType) -> bool {
    matches!(
        jointype,
        JOIN_LEFT | JOIN_FULL | JOIN_RIGHT | JOIN_ANTI | JOIN_RIGHT_ANTI
    )
}

/* ==========================================================================
 * Working `extra` (the C `JoinPathExtraData`), arena-friendly.
 *
 * Identical to the C struct except the clause lists are `RinfoId` handles into
 * the rinfo arena (the C `RestrictInfo *` lists). A `materialize` view rebuilds
 * the central `JoinPathExtraData` (owned clauses) for the cost / construction
 * seams that expect it.
 * ======================================================================== */

struct JoinPathExtra<'mcx> {
    restrictlist: PgVec<'mcx, RinfoId>,
    mergeclause_list: PgVec<'mcx, RinfoId>,
    inner_unique: bool,
    sjinfo: SpecialJoinInfo,
    semifactors: SemiAntiJoinFactors,
    param_source_rels: Relids,
}

impl<'mcx> JoinPathExtra<'mcx> {
    /// Build the central [`JoinPathExtraData`] the cost/construction seams
    /// consume (cloning the clause `RestrictInfo`s out of the arena, the
    /// faithful boundary marshalling of the C `RestrictInfo *` lists).
    fn materialize(&self, root: &PlannerInfo) -> JoinPathExtraData {
        let restrictlist: Vec<RestrictInfo> = self
            .restrictlist
            .iter()
            .map(|&id| root.rinfo(id).clone())
            .collect();
        let mergeclause_list: Vec<RestrictInfo> = self
            .mergeclause_list
            .iter()
            .map(|&id| root.rinfo(id).clone())
            .collect();
        JoinPathExtraData {
            restrictlist,
            mergeclause_list,
            inner_unique: self.inner_unique,
            sjinfo: Some(alloc::boxed::Box::new(self.sjinfo.clone())),
            semifactors: self.semifactors,
            param_source_rels: clone_relids(&self.param_source_rels),
        }
    }
}

/* ==========================================================================
 * Small arena/Relids helpers (the C macros + bms_* convenience).
 * ======================================================================== */

#[inline]
fn clone_relids(a: &Relids) -> Relids {
    bms::relids_copy::call(a)
}

/// Charge a copy of a pathkey slice to `mcx` (the owned-tree analogue of the C
/// code re-using a `List *` of pathkeys), OOM-fallibly.
fn charged_pathkeys<'mcx>(mcx: Mcx<'mcx>, src: &[PathKey]) -> PgResult<PgVec<'mcx, PathKey>> {
    let mut v = mcx::vec_with_capacity_in(mcx, src.len())?;
    for pk in src {
        v.push(pk.clone());
    }
    Ok(v)
}

/// Push onto an mcx-charged `PgVec`, reserving fallibly first (the allocating
/// step; the C `lappend` palloc).
fn charged_push<T>(mcx: Mcx<'_>, v: &mut PgVec<'_, T>, x: T) -> PgResult<()> {
    v.try_reserve(1).map_err(|_| mcx.oom(core::mem::size_of::<T>()))?;
    v.push(x);
    Ok(())
}

/// `PATH_REQ_OUTER(path)` (pathnodes.h) — `path->param_info ?
/// param_info->ppi_req_outer : NULL`.
fn path_req_outer(root: &PlannerInfo, path: PathId) -> Relids {
    match &root.path(path).base().param_info {
        Some(ppi) => clone_relids(&ppi.ppi_req_outer),
        None => None,
    }
}

/// `PATH_PARAM_BY_PARENT(path, rel)`.
fn path_param_by_parent(root: &PlannerInfo, path: PathId, rel: RelId) -> bool {
    if root.path(path).base().param_info.is_none() {
        return false;
    }
    let req = path_req_outer(root, path);
    bms::relids_overlap::call(&req, &root.rel(rel).top_parent_relids)
}

/// `PATH_PARAM_BY_REL_SELF(path, rel)`.
fn path_param_by_rel_self(root: &PlannerInfo, path: PathId, rel: RelId) -> bool {
    if root.path(path).base().param_info.is_none() {
        return false;
    }
    let req = path_req_outer(root, path);
    bms::relids_overlap::call(&req, &root.rel(rel).relids)
}

/// `PATH_PARAM_BY_REL(path, rel)` — self or by-parent.
fn path_param_by_rel(root: &PlannerInfo, path: PathId, rel: RelId) -> bool {
    path_param_by_rel_self(root, path, rel) || path_param_by_parent(root, path, rel)
}

/* ==========================================================================
 * add_paths_to_joinrel (joinpath.c:123)
 * ======================================================================== */

/// `add_paths_to_joinrel` — consider every join method for `outerrel`/`innerrel`
/// and add the surviving paths to `joinrel`.
pub fn add_paths_to_joinrel<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    joinrel: RelId,
    outerrel: RelId,
    innerrel: RelId,
    jointype: JoinType,
    sjinfo: &SpecialJoinInfo,
    restrictlist: &[RinfoId],
    enable: JoinEnableFlags,
) -> PgResult<()> {
    let mut mergejoin_allowed = true;

    // joinrelids: top_parent for OTHER_JOINREL, else relids.
    let joinrelids = if root.rel(joinrel).reloptkind == RELOPT_OTHER_JOINREL {
        clone_relids(&root.rel(joinrel).top_parent_relids)
    } else {
        clone_relids(&root.rel(joinrel).relids)
    };

    let mut extra = JoinPathExtra {
        restrictlist: mcx::slice_in(mcx, restrictlist)?,
        mergeclause_list: mcx::vec_with_capacity_in(mcx, 0)?,
        inner_unique: false,
        sjinfo: sjinfo.clone(),
        semifactors: SemiAntiJoinFactors {
            outer_match_frac: 0.0,
            match_count: 0.0,
        },
        param_source_rels: None,
    };

    // See if the inner relation is provably unique for this outer rel.
    extra.inner_unique = match jointype {
        JOIN_SEMI | JOIN_ANTI => false, // unproven
        JOIN_UNIQUE_INNER => {
            bms::relids_is_subset::call(&sjinfo.min_lefthand, &root.rel(outerrel).relids)
        }
        JOIN_UNIQUE_OUTER => jp::innerrel_is_unique::call(
            root,
            run,
            &clone_relids(&root.rel(joinrel).relids),
            &clone_relids(&root.rel(outerrel).relids),
            innerrel,
            JOIN_INNER,
            restrictlist,
            false,
        ),
        _ => jp::innerrel_is_unique::call(
            root,
            run,
            &clone_relids(&root.rel(joinrel).relids),
            &clone_relids(&root.rel(outerrel).relids),
            innerrel,
            jointype,
            restrictlist,
            false,
        ),
    };

    // Find potential mergejoin clauses (unless not interested; FULL overrides).
    if enable.enable_mergejoin || jointype == JOIN_FULL {
        extra.mergeclause_list = select_mergejoin_clauses(
            mcx,
            root,
            joinrel,
            outerrel,
            innerrel,
            restrictlist,
            jointype,
            &mut mergejoin_allowed,
        )?;
    }

    // SEMI/ANTI/inner_unique correction factors.
    if jointype == JOIN_SEMI || jointype == JOIN_ANTI || extra.inner_unique {
        extra.semifactors = jp::compute_semi_anti_join_factors::call(
            run, root, joinrel, outerrel, innerrel, jointype, sjinfo, restrictlist,
        );
    }

    // Decide param_source_rels (iterate join_info_list; clone the small Relids
    // we need so we needn't hold an immutable borrow of root across the
    // mutation of extra).
    let n_sj = root.join_info_list.len();
    for i in 0..n_sj {
        let (sj_min_rh, sj_min_lh, sj_is_full) = {
            let sj = &root.join_info_list[i];
            (
                clone_relids(&sj.min_righthand),
                clone_relids(&sj.min_lefthand),
                sj.jointype == JOIN_FULL,
            )
        };

        if bms::relids_overlap::call(&joinrelids, &sj_min_rh)
            && !bms::relids_overlap::call(&joinrelids, &sj_min_lh)
        {
            let diff = jp::bms_difference::call(&root.all_baserels, &sj_min_rh);
            let acc = core::mem::take(&mut extra.param_source_rels);
            extra.param_source_rels = bms::relids_join::call(acc, diff);
        }

        // full joins constrain both sides symmetrically.
        if sj_is_full
            && bms::relids_overlap::call(&joinrelids, &sj_min_lh)
            && !bms::relids_overlap::call(&joinrelids, &sj_min_rh)
        {
            let diff = jp::bms_difference::call(&root.all_baserels, &sj_min_lh);
            let acc = core::mem::take(&mut extra.param_source_rels);
            extra.param_source_rels = bms::relids_join::call(acc, diff);
        }
    }

    // Allow residual lateral dependencies.
    let lat = clone_relids(&root.rel(joinrel).lateral_relids);
    let acc = core::mem::take(&mut extra.param_source_rels);
    extra.param_source_rels = bms::relids_add_members::call(acc, &lat);

    // 1. Mergejoin paths sorting both rels.
    if mergejoin_allowed {
        sort_inner_and_outer(mcx, root, run, joinrel, outerrel, innerrel, jointype, &extra)?;
    }

    // 2. Paths where the outer need not be explicitly sorted (nestloop +
    //    already-ordered mergejoin).
    if mergejoin_allowed {
        match_unsorted_outer(mcx, root, run, joinrel, outerrel, innerrel, jointype, &extra, enable)?;
    }

    // (3. match_unsorted_inner is diked out in C — #ifdef NOT_USED.)

    // 4. Hashjoin paths (FULL overrides enable_hashjoin).
    if enable.enable_hashjoin || jointype == JOIN_FULL {
        hash_inner_and_outer(mcx, root, run, joinrel, outerrel, innerrel, jointype, &extra, enable)?;
    }

    // 5. FDW join pushdown (presence-checked inside the seam).
    {
        let owned = extra.materialize(root);
        jp::fdw_get_foreign_join_paths::call(root, joinrel, outerrel, innerrel, jointype, &owned)?;
    }

    // 6. Extension hook.
    {
        let owned = extra.materialize(root);
        jp::set_join_pathlist_hook::call(root, joinrel, outerrel, innerrel, jointype, &owned)?;
    }

    Ok(())
}

/* ==========================================================================
 * allow_star_schema_join / have_unsafe_outer_join_ref (joinpath.c:362/389)
 * ======================================================================== */

/// `allow_star_schema_join(root, outerrelids, inner_paramrels)` — true iff the
/// outer rel provides *some but not all* of the inner rel's parameterization.
fn allow_star_schema_join(outerrelids: &Relids, inner_paramrels: &Relids) -> bool {
    bms::relids_overlap::call(inner_paramrels, outerrelids)
        && bms::relids_nonempty_difference::call(inner_paramrels, outerrelids)
}

/// `have_unsafe_outer_join_ref` (joinpath.c:389) — the assert-only backstop
/// (`USE_ASSERT_CHECKING`). Ported faithfully; called from `debug_assert!`.
#[cfg(debug_assertions)]
fn have_unsafe_outer_join_ref(
    root: &PlannerInfo,
    outerrelids: &Relids,
    inner_paramrels: &Relids,
) -> bool {
    let mut result = false;
    let unsatisfied = jp::bms_difference::call(inner_paramrels, outerrelids);
    let satisfied = bms::relids_intersect::call(inner_paramrels, outerrelids);

    if bms::relids_overlap::call(&unsatisfied, &root.outer_join_rels) {
        for sjinfo in root.join_info_list.iter() {
            if !bms::relids_is_member::call(sjinfo.ojrelid as i32, &unsatisfied) {
                continue; // not relevant
            }
            if bms::relids_overlap::call(&satisfied, &sjinfo.min_righthand)
                || (sjinfo.jointype == JOIN_FULL
                    && bms::relids_overlap::call(&satisfied, &sjinfo.min_lefthand))
            {
                result = true; // doesn't work
                break;
            }
        }
    }
    result
}

/* ==========================================================================
 * paraminfo_get_equal_hashops (joinpath.c:438)
 *
 * Determine whether all of `param_info`'s join clauses plus the inner rel's
 * lateral Vars (and any PHV-derived lateral vars in `ph_lateral_vars`) can be
 * used as memoize cache keys; if so, build the parallel `param_exprs` /
 * `operators` lists and the `binary_mode` flag. Returns `false` (with the
 * outputs cleared) the moment any key is unusable.
 *
 * The node-payload reads (`IsA(clause, OpExpr)` with two args, the
 * left/right hasheqoperator, expr identity for `list_member`, the
 * type-cache hash/eq lookup, volatility) cross the clauses.c/typcache.c
 * boundary as thin seams; the orchestration is faithful to the C.
 * ======================================================================== */

#[allow(clippy::type_complexity)]
fn paraminfo_get_equal_hashops<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    param_info: Option<&ParamPathInfoKeys>,
    outerrel: RelId,
    innerrel: RelId,
    ph_lateral_vars: &[NodeId],
) -> PgResult<Option<(PgVec<'mcx, NodeId>, PgVec<'mcx, Oid>, bool)>> {
    let mut param_exprs: PgVec<'mcx, NodeId> = mcx::vec_with_capacity_in(mcx, 0)?;
    let mut operators: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, 0)?;
    let mut binary_mode = false;

    let outer_relids = clone_relids(&root.rel(outerrel).relids);
    let inner_relids = clone_relids(&root.rel(innerrel).relids);

    // Add join clauses from param_info to the hash key.
    if let Some(pi) = param_info {
        for &rinfo in pi.ppi_clauses.iter() {
            // Need a join OpExpr with 2 args, and the clause must match the join.
            //
            // `clause_sides_match_join` also sets `rinfo->outer_is_left`, hence the
            // &mut root.
            if !jp::clause_is_opexpr_with_two_args::call(root, rinfo)
                || !jp::clause_sides_match_join::call(root, rinfo, &outer_relids, &inner_relids)
            {
                return Ok(None);
            }

            let (expr, hasheqoperator) = {
                let ri = root.rinfo(rinfo);
                if ri.outer_is_left {
                    let hasheq = ri.left_hasheqoperator;
                    (jp::opexpr_arg::call(root, rinfo, 0), hasheq)
                } else {
                    let hasheq = ri.right_hasheqoperator;
                    (jp::opexpr_arg::call(root, rinfo, 1), hasheq)
                }
            };

            // Can't do memoize if we can't hash the outer type.
            if hasheqoperator == InvalidOid {
                return Ok(None);
            }

            // 'expr' may already be a parameter; if not, add it.
            if !param_exprs.contains(&expr) {
                charged_push(mcx, &mut operators, hasheqoperator)?;
                charged_push(mcx, &mut param_exprs, expr)?;
            }

            // Non-hashable join operator forces binary comparison mode.
            if root.rinfo(rinfo).hashjoinoperator == InvalidOid {
                binary_mode = true;
            }
        }
    }

    // Now add any lateral vars to the cache key too. C: list_concat of
    // ph_lateral_vars and innerrel->lateral_vars (in that order).
    let inner_lateral = mcx::slice_in(mcx, &root.rel(innerrel).lateral_vars)?;
    let lateral_iter = ph_lateral_vars.iter().chain(inner_lateral.iter());
    for &expr in lateral_iter {
        // Reject if there are any volatile functions in lateral vars.
        if jp::contain_volatile_functions_node::call(root, expr) {
            return Ok(None);
        }

        // Need a valid hash proc + equality operator for the expr's type.
        let eq_opr = match jp::expr_hash_eq_operator::call(root, expr) {
            Some(op) => op,
            None => return Ok(None),
        };

        if !param_exprs.contains(&expr) {
            charged_push(mcx, &mut operators, eq_opr)?;
            charged_push(mcx, &mut param_exprs, expr)?;
        }

        // Lateral vars always force binary comparison mode.
        binary_mode = true;
    }

    Ok(Some((param_exprs, operators, binary_mode)))
}

/// The fields of a `ParamPathInfo` the memoize cache-key analysis reads. Cloned
/// out of the arena so the analysis needn't hold an immutable borrow of `root`
/// across the `&mut root` clause-side seam.
struct ParamPathInfoKeys {
    ppi_clauses: Vec<RinfoId>,
    ppi_serials: Relids,
}

/* ==========================================================================
 * extract_lateral_vars_from_PHVs (joinpath.c:583)
 *
 * Extract lateral references within PlaceHolderVars that are due to be
 * evaluated at `innerrelids`, returning them as cache-key expr handles.
 * ======================================================================== */

fn extract_lateral_vars_from_PHVs<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    innerrelids: &Relids,
) -> PgResult<PgVec<'mcx, NodeId>> {
    let mut ph_lateral_vars: PgVec<'mcx, NodeId> = mcx::vec_with_capacity_in(mcx, 0)?;

    // Nothing to find if the query has no LATERAL RTEs.
    if !root.hasLateralRTEs {
        return Ok(ph_lateral_vars);
    }

    // No PHVs evaluated at joinrels — we never memoize atop joinrel paths.
    if jp::bms_membership_is_multiple::call(innerrelids) {
        return Ok(ph_lateral_vars);
    }

    let placeholders = mcx::slice_in(mcx, &root.placeholder_list)?;
    for &phid in placeholders.iter() {
        let (ph_lateral, ph_eval_at, phexpr) = {
            let phinfo = root.phinfo(phid);
            (
                clone_relids(&phinfo.ph_lateral),
                clone_relids(&phinfo.ph_eval_at),
                phinfo.ph_var_phexpr,
            )
        };

        // PHV uninteresting if no lateral refs.
        if bms::relids_is_empty::call(&ph_lateral) {
            continue;
        }
        // PHV uninteresting if not evaluated at innerrelids.
        if !jp::bms_equal::call(&ph_eval_at, innerrelids) {
            continue;
        }

        // If the PHV references no rels in innerrelids, use its contained
        // expression as a cache key directly.
        if !bms::relids_overlap::call(&jp::pull_varnos::call(root, phexpr), innerrelids) {
            charged_push(mcx, &mut ph_lateral_vars, phexpr)?;
            continue;
        }

        // Otherwise fetch the level-0 Vars/PHVs of the contained expression.
        let vars = jp::pull_vars_of_level::call(root, phexpr, 0)?;
        for &node in vars.iter() {
            if jp::node_is_var::call(root, node) {
                // Assert(var->varlevelsup == 0) in C.
                let varno = jp::var_varno::call(root, node);
                if bms::relids_is_member::call(varno, &ph_lateral) {
                    charged_push(mcx, &mut ph_lateral_vars, node)?;
                }
            } else if jp::node_is_placeholdervar::call(root, node) {
                // Assert(phv->phlevelsup == 0) in C.
                let inner_phid = jp::find_placeholder_info::call(root, node);
                let inner_eval_at = clone_relids(&root.phinfo(inner_phid).ph_eval_at);
                if bms::relids_is_subset::call(&inner_eval_at, &ph_lateral) {
                    charged_push(mcx, &mut ph_lateral_vars, node)?;
                }
            }
            // else: Assert(false) in C — neither a Var nor PHV; skip.
        }
    }

    Ok(ph_lateral_vars)
}

/* ==========================================================================
 * get_memoize_path (joinpath.c:674)
 *
 * If possible, make and return a Memoize path atop `inner_path`; else `None`.
 * ======================================================================== */

fn get_memoize_path<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    innerrel: RelId,
    outerrel: RelId,
    inner_path: PathId,
    outer_path: PathId,
    jointype: JoinType,
    extra: &JoinPathExtra,
    enable_memoize: bool,
) -> PgResult<Option<PathId>> {
    // Obviously not if it's disabled.
    if !enable_memoize {
        return Ok(None);
    }

    // Not worth it unless we expect more than one inner scan (first is a miss).
    {
        let outer_parent = root.path(outer_path).base().parent;
        if root.rel(outer_parent).rows < 2.0 {
            return Ok(None);
        }
    }

    // Extract lateral Vars/PHVs evaluated at innerrel, usable as cache keys.
    let innerrelids = clone_relids(&root.rel(innerrel).relids);
    let ph_lateral_vars = extract_lateral_vars_from_PHVs(mcx, root, &innerrelids)?;

    // Need some cache key: parameterized clauses or lateral Vars.
    let (inner_has_param_clauses, inner_param_keys) = match &root.path(inner_path).base().param_info
    {
        Some(pi) => (
            !pi.ppi_clauses.is_empty(),
            Some(ParamPathInfoKeys {
                ppi_clauses: pi.ppi_clauses.clone(),
                ppi_serials: clone_relids(&pi.ppi_serials),
            }),
        ),
        None => (false, None),
    };
    if !inner_has_param_clauses
        && root.rel(innerrel).lateral_vars.is_empty()
        && ph_lateral_vars.is_empty()
    {
        return Ok(None);
    }

    // No memoize for non-unique SEMI/ANTI (inner not scanned to completion).
    if !extra.inner_unique && (jointype == JOIN_SEMI || jointype == JOIN_ANTI) {
        return Ok(None);
    }

    // For unique joins the whole join condition must be parameterized, i.e.
    // every restrictlist rinfo's serial must be in the inner param's ppi_serials.
    if extra.inner_unique {
        let ppi_serials = match &inner_param_keys {
            // inner_path->param_info == NULL → bail.
            None => return Ok(None),
            Some(k) => clone_relids(&k.ppi_serials),
        };
        for &rinfo in extra.restrictlist.iter() {
            let serial = root.rinfo(rinfo).rinfo_serial;
            if !bms::relids_is_member::call(serial, &ppi_serials) {
                return Ok(None);
            }
        }
    }

    // No memoize if there are volatile functions in the inner rel's target list.
    if jp::contain_volatile_functions_reltarget::call(root, innerrel) {
        return Ok(None);
    }
    // …nor in any of its base restrict clauses.
    {
        let baserestrict = mcx::slice_in(mcx, &root.rel(innerrel).baserestrictinfo)?;
        for &rinfo in baserestrict.iter() {
            if jp::contain_volatile_functions_rinfo::call(root, rinfo) {
                return Ok(None);
            }
        }
    }
    // …nor in the parameterized path's restrict clauses.
    if let Some(k) = &inner_param_keys {
        let clauses = mcx::slice_in(mcx, &k.ppi_clauses)?;
        for &rinfo in clauses.iter() {
            if jp::contain_volatile_functions_rinfo::call(root, rinfo) {
                return Ok(None);
            }
        }
    }

    // Check we have hash ops for each cache-key parameter. Use the outer rel's
    // top_parent if it has one.
    let hashops_outerrel = root.rel(outerrel).top_parent.unwrap_or(outerrel);
    let hashops = paraminfo_get_equal_hashops(
        mcx,
        root,
        inner_param_keys.as_ref(),
        hashops_outerrel,
        innerrel,
        &ph_lateral_vars,
    )?;

    if let Some((param_exprs, hash_operators, binary_mode)) = hashops {
        let calls = root.path(outer_path).base().rows;
        let singlerow = extra.inner_unique;
        let p = jp::create_memoize_path::call(
            root,
            innerrel,
            inner_path,
            &param_exprs,
            &hash_operators,
            singlerow,
            binary_mode,
            calls,
        )?;
        return Ok(Some(p));
    }

    Ok(None)
}

/* ==========================================================================
 * try_nestloop_path (joinpath.c:830)
 * ======================================================================== */

fn try_nestloop_path<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    joinrel: RelId,
    outer_path: PathId,
    inner_path: PathId,
    pathkeys: &[PathKey],
    jointype: JoinType,
    extra: &JoinPathExtra,
) -> PgResult<()> {
    let innerrel = root.path(inner_path).base().parent;
    let outerrel = root.path(outer_path).base().parent;
    let inner_paramrels = path_req_outer(root, inner_path);
    let outer_paramrels = path_req_outer(root, outer_path);

    // Nonsensical to use an input parameterized by the OJ we're forming.
    if extra.sjinfo.ojrelid != 0
        && (bms::relids_is_member::call(extra.sjinfo.ojrelid as i32, &inner_paramrels)
            || bms::relids_is_member::call(extra.sjinfo.ojrelid as i32, &outer_paramrels))
    {
        return Ok(());
    }

    // Use topmost parents for parameterization (reparameterize not yet called).
    let innerrelids = {
        let r = root.rel(innerrel);
        if !bms::relids_is_empty::call(&r.top_parent_relids) {
            clone_relids(&r.top_parent_relids)
        } else {
            clone_relids(&r.relids)
        }
    };
    let outerrelids = {
        let r = root.rel(outerrel);
        if !bms::relids_is_empty::call(&r.top_parent_relids) {
            clone_relids(&r.top_parent_relids)
        } else {
            clone_relids(&r.relids)
        }
    };

    let required_outer = jp::calc_nestloop_required_outer::call(
        &outerrelids,
        &outer_paramrels,
        &innerrelids,
        &inner_paramrels,
    );
    if !bms::relids_is_empty::call(&required_outer)
        && !bms::relids_overlap::call(&required_outer, &extra.param_source_rels)
        && !allow_star_schema_join(&outerrelids, &inner_paramrels)
    {
        return Ok(());
    }

    // If we got past that, no unsafe outer-join refs (assert backstop).
    #[cfg(debug_assertions)]
    debug_assert!(!have_unsafe_outer_join_ref(root, &outerrelids, &inner_paramrels));

    // Reparameterizability check for inner parameterized by parent.
    if path_param_by_parent(root, inner_path, outerrel)
        && !jp::path_is_reparameterizable_by_child::call(root, inner_path, outerrel)
    {
        return Ok(());
    }

    let materialized_extra = extra.materialize(root);
    let workspace = jp::initial_cost_nestloop::call(
        run,
        root,
        jointype,
        outer_path,
        inner_path,
        &materialized_extra,
    )?;

    if jp::add_path_precheck::call(
        root,
        joinrel,
        workspace.disabled_nodes,
        workspace.startup_cost,
        workspace.total_cost,
        pathkeys,
        &required_outer,
    ) {
        let owned = extra.materialize(root);
        let p = jp::create_nestloop_path::call(
            root,
            run,
            joinrel,
            jointype,
            &workspace,
            &owned,
            outer_path,
            inner_path,
            &extra.restrictlist,
            pathkeys,
            &required_outer,
        )?;
        jp::add_path::call(root, joinrel, p)?;
    }
    Ok(())
}

/* ==========================================================================
 * try_partial_nestloop_path (joinpath.c:949)
 * ======================================================================== */

fn try_partial_nestloop_path<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    joinrel: RelId,
    outer_path: PathId,
    inner_path: PathId,
    pathkeys: &[PathKey],
    jointype: JoinType,
    extra: &JoinPathExtra,
) -> PgResult<()> {
    debug_assert!(bms::relids_is_empty::call(&root.rel(joinrel).lateral_relids));
    debug_assert!(bms::relids_is_empty::call(&path_req_outer(root, outer_path)));

    if root.path(inner_path).base().param_info.is_some() {
        let inner_paramrels = {
            let ppi = root.path(inner_path).base().param_info.as_ref().unwrap();
            clone_relids(&ppi.ppi_req_outer)
        };
        let outerrel = root.path(outer_path).base().parent;
        let outerrelids = {
            let r = root.rel(outerrel);
            if !bms::relids_is_empty::call(&r.top_parent_relids) {
                clone_relids(&r.top_parent_relids)
            } else {
                clone_relids(&r.relids)
            }
        };
        if !bms::relids_is_subset::call(&inner_paramrels, &outerrelids) {
            return Ok(());
        }
    }

    let outer_parent = root.path(outer_path).base().parent;
    if path_param_by_parent(root, inner_path, outer_parent)
        && !jp::path_is_reparameterizable_by_child::call(root, inner_path, outer_parent)
    {
        return Ok(());
    }

    let materialized_extra = extra.materialize(root);
    let workspace = jp::initial_cost_nestloop::call(
        run,
        root,
        jointype,
        outer_path,
        inner_path,
        &materialized_extra,
    )?;
    if !jp::add_partial_path_precheck::call(
        root,
        joinrel,
        workspace.disabled_nodes,
        workspace.total_cost,
        pathkeys,
    ) {
        return Ok(());
    }

    let owned = extra.materialize(root);
    let p = jp::create_nestloop_path::call(
        root,
        run,
        joinrel,
        jointype,
        &workspace,
        &owned,
        outer_path,
        inner_path,
        &extra.restrictlist,
        pathkeys,
        &None,
    )?;
    jp::add_partial_path::call(root, joinrel, p)?;
    Ok(())
}

/* ==========================================================================
 * try_mergejoin_path (joinpath.c:1028)
 * ======================================================================== */

fn try_mergejoin_path<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    joinrel: RelId,
    outer_path: PathId,
    inner_path: PathId,
    pathkeys: &[PathKey],
    mergeclauses: &[RinfoId],
    mut outersortkeys: PgVec<'mcx, PathKey>,
    mut innersortkeys: PgVec<'mcx, PathKey>,
    jointype: JoinType,
    extra: &JoinPathExtra,
    is_partial: bool,
) -> PgResult<()> {
    if is_partial {
        return try_partial_mergejoin_path(
            mcx,
            root,
            run,
            joinrel,
            outer_path,
            inner_path,
            pathkeys,
            mergeclauses,
            outersortkeys,
            innersortkeys,
            jointype,
            extra,
        );
    }

    if extra.sjinfo.ojrelid != 0
        && (bms::relids_is_member::call(
            extra.sjinfo.ojrelid as i32,
            &path_req_outer(root, inner_path),
        ) || bms::relids_is_member::call(
            extra.sjinfo.ojrelid as i32,
            &path_req_outer(root, outer_path),
        ))
    {
        return Ok(());
    }

    let required_outer = jp::calc_non_nestloop_required_outer::call(root, outer_path, inner_path);
    if !bms::relids_is_empty::call(&required_outer)
        && !bms::relids_overlap::call(&required_outer, &extra.param_source_rels)
    {
        return Ok(());
    }

    // Skip explicit sorts if already well enough ordered.
    let mut outer_presorted_keys = 0;
    if !outersortkeys.is_empty() {
        let (contained, n) = jp::pathkeys_count_contained_in::call(
            &outersortkeys,
            &root.path(outer_path).base().pathkeys,
        );
        if contained {
            // C: `outersortkeys = NIL` (no pfree; charge stays until reset).
            outersortkeys = mcx::vec_with_capacity_in(mcx, 0)?;
        } else {
            outer_presorted_keys = n;
        }
    }
    if !innersortkeys.is_empty()
        && jp::pathkeys_contained_in::call(&innersortkeys, &root.path(inner_path).base().pathkeys)
    {
        innersortkeys = mcx::vec_with_capacity_in(mcx, 0)?;
    }

    let materialized_extra = extra.materialize(root);
    let workspace = jp::initial_cost_mergejoin::call(
        run,
        root,
        jointype,
        mergeclauses,
        outer_path,
        inner_path,
        &outersortkeys,
        &innersortkeys,
        outer_presorted_keys,
        &materialized_extra,
    )?;

    if jp::add_path_precheck::call(
        root,
        joinrel,
        workspace.disabled_nodes,
        workspace.startup_cost,
        workspace.total_cost,
        pathkeys,
        &required_outer,
    ) {
        let owned = extra.materialize(root);
        let p = jp::create_mergejoin_path::call(
            root,
            run,
            joinrel,
            jointype,
            &workspace,
            &owned,
            outer_path,
            inner_path,
            &extra.restrictlist,
            pathkeys,
            &required_outer,
            mergeclauses,
            &outersortkeys,
            &innersortkeys,
            outer_presorted_keys,
        )?;
        jp::add_path::call(root, joinrel, p)?;
    }
    Ok(())
}

/* ==========================================================================
 * try_partial_mergejoin_path (joinpath.c:1144)
 * ======================================================================== */

fn try_partial_mergejoin_path<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    joinrel: RelId,
    outer_path: PathId,
    inner_path: PathId,
    pathkeys: &[PathKey],
    mergeclauses: &[RinfoId],
    mut outersortkeys: PgVec<'mcx, PathKey>,
    mut innersortkeys: PgVec<'mcx, PathKey>,
    jointype: JoinType,
    extra: &JoinPathExtra,
) -> PgResult<()> {
    debug_assert!(bms::relids_is_empty::call(&root.rel(joinrel).lateral_relids));
    debug_assert!(bms::relids_is_empty::call(&path_req_outer(root, outer_path)));
    if !bms::relids_is_empty::call(&path_req_outer(root, inner_path)) {
        return Ok(());
    }

    let mut outer_presorted_keys = 0;
    if !outersortkeys.is_empty() {
        let (contained, n) = jp::pathkeys_count_contained_in::call(
            &outersortkeys,
            &root.path(outer_path).base().pathkeys,
        );
        if contained {
            outersortkeys = mcx::vec_with_capacity_in(mcx, 0)?;
        } else {
            outer_presorted_keys = n;
        }
    }
    if !innersortkeys.is_empty()
        && jp::pathkeys_contained_in::call(&innersortkeys, &root.path(inner_path).base().pathkeys)
    {
        innersortkeys = mcx::vec_with_capacity_in(mcx, 0)?;
    }

    let materialized_extra = extra.materialize(root);
    let workspace = jp::initial_cost_mergejoin::call(
        run,
        root,
        jointype,
        mergeclauses,
        outer_path,
        inner_path,
        &outersortkeys,
        &innersortkeys,
        outer_presorted_keys,
        &materialized_extra,
    )?;

    if !jp::add_partial_path_precheck::call(
        root,
        joinrel,
        workspace.disabled_nodes,
        workspace.total_cost,
        pathkeys,
    ) {
        return Ok(());
    }

    let owned = extra.materialize(root);
    let p = jp::create_mergejoin_path::call(
        root,
        run,
        joinrel,
        jointype,
        &workspace,
        &owned,
        outer_path,
        inner_path,
        &extra.restrictlist,
        pathkeys,
        &None,
        mergeclauses,
        &outersortkeys,
        &innersortkeys,
        outer_presorted_keys,
    )?;
    jp::add_partial_path::call(root, joinrel, p)?;
    Ok(())
}

/* ==========================================================================
 * try_hashjoin_path (joinpath.c:1221)
 * ======================================================================== */

fn try_hashjoin_path<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    joinrel: RelId,
    outer_path: PathId,
    inner_path: PathId,
    hashclauses: &[RinfoId],
    jointype: JoinType,
    extra: &JoinPathExtra,
) -> PgResult<()> {
    if extra.sjinfo.ojrelid != 0
        && (bms::relids_is_member::call(
            extra.sjinfo.ojrelid as i32,
            &path_req_outer(root, inner_path),
        ) || bms::relids_is_member::call(
            extra.sjinfo.ojrelid as i32,
            &path_req_outer(root, outer_path),
        ))
    {
        return Ok(());
    }

    let required_outer = jp::calc_non_nestloop_required_outer::call(root, outer_path, inner_path);
    if !bms::relids_is_empty::call(&required_outer)
        && !bms::relids_overlap::call(&required_outer, &extra.param_source_rels)
    {
        return Ok(());
    }

    // Hashjoin paths never have output pathkeys.
    let workspace = jp::initial_cost_hashjoin::call(
        root,
        jointype,
        hashclauses,
        outer_path,
        inner_path,
        &extra.materialize(root),
        false,
    );

    let no_keys: &[PathKey] = &[];
    if jp::add_path_precheck::call(
        root,
        joinrel,
        workspace.disabled_nodes,
        workspace.startup_cost,
        workspace.total_cost,
        no_keys,
        &required_outer,
    ) {
        let owned = extra.materialize(root);
        let p = jp::create_hashjoin_path::call(
            root,
            run,
            joinrel,
            jointype,
            &workspace,
            &owned,
            outer_path,
            inner_path,
            false, // parallel_hash
            &extra.restrictlist,
            &required_outer,
            hashclauses,
        )?;
        jp::add_path::call(root, joinrel, p)?;
    }
    Ok(())
}

/* ==========================================================================
 * try_partial_hashjoin_path (joinpath.c:1298)
 * ======================================================================== */

fn try_partial_hashjoin_path<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    joinrel: RelId,
    outer_path: PathId,
    inner_path: PathId,
    hashclauses: &[RinfoId],
    jointype: JoinType,
    extra: &JoinPathExtra,
    parallel_hash: bool,
) -> PgResult<()> {
    debug_assert!(bms::relids_is_empty::call(&root.rel(joinrel).lateral_relids));
    debug_assert!(bms::relids_is_empty::call(&path_req_outer(root, outer_path)));
    if !bms::relids_is_empty::call(&path_req_outer(root, inner_path)) {
        return Ok(());
    }

    let workspace = jp::initial_cost_hashjoin::call(
        root,
        jointype,
        hashclauses,
        outer_path,
        inner_path,
        &extra.materialize(root),
        parallel_hash,
    );
    let no_keys: &[PathKey] = &[];
    if !jp::add_partial_path_precheck::call(
        root,
        joinrel,
        workspace.disabled_nodes,
        workspace.total_cost,
        no_keys,
    ) {
        return Ok(());
    }

    let owned = extra.materialize(root);
    let p = jp::create_hashjoin_path::call(
        root,
        run,
        joinrel,
        jointype,
        &workspace,
        &owned,
        outer_path,
        inner_path,
        parallel_hash,
        &extra.restrictlist,
        &None,
        hashclauses,
    )?;
    jp::add_partial_path::call(root, joinrel, p)?;
    Ok(())
}

/* ==========================================================================
 * sort_inner_and_outer (joinpath.c:1356)
 * ======================================================================== */

fn sort_inner_and_outer<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    joinrel: RelId,
    outerrel: RelId,
    innerrel: RelId,
    mut jointype: JoinType,
    extra: &JoinPathExtra,
) -> PgResult<()> {
    let save_jointype = jointype;

    // Nothing to do if no mergejoin clauses.
    if extra.mergeclause_list.is_empty() {
        return Ok(());
    }

    // Only cheapest-total-cost input paths.
    let mut outer_path = match root.rel(outerrel).cheapest_total_path {
        Some(p) => p,
        None => return Ok(()),
    };
    let mut inner_path = match root.rel(innerrel).cheapest_total_path {
        Some(p) => p,
        None => return Ok(()),
    };

    // If either cheapest-total is parameterized by the other rel, no mergejoin.
    if path_param_by_rel(root, outer_path, innerrel) || path_param_by_rel(root, inner_path, outerrel)
    {
        return Ok(());
    }

    // Unique-ification.
    if jointype == JOIN_UNIQUE_OUTER {
        outer_path = match jp::create_unique_path::call(run, root, outerrel, outer_path, &extra.sjinfo)? {
            Some(p) => p,
            None => return Ok(()), // Assert(outer_path) in C
        };
        jointype = JOIN_INNER;
    } else if jointype == JOIN_UNIQUE_INNER {
        inner_path = match jp::create_unique_path::call(run, root, innerrel, inner_path, &extra.sjinfo)? {
            Some(p) => p,
            None => return Ok(()),
        };
        jointype = JOIN_INNER;
    }

    // Maybe a partial merge join.
    let mut cheapest_partial_outer: Option<PathId> = None;
    let mut cheapest_safe_inner: Option<PathId> = None;
    if root.rel(joinrel).consider_parallel
        && save_jointype != JOIN_UNIQUE_OUTER
        && save_jointype != JOIN_FULL
        && save_jointype != JOIN_RIGHT
        && save_jointype != JOIN_RIGHT_ANTI
        && !root.rel(outerrel).partial_pathlist.is_empty()
        && bms::relids_is_empty::call(&root.rel(joinrel).lateral_relids)
    {
        cheapest_partial_outer = Some(root.rel(outerrel).partial_pathlist[0]);

        if root.path(inner_path).base().parallel_safe {
            cheapest_safe_inner = Some(inner_path);
        } else if save_jointype != JOIN_UNIQUE_INNER {
            let pathlist = mcx::slice_in(mcx, &root.rel(innerrel).pathlist)?;
            cheapest_safe_inner = jp::get_cheapest_parallel_safe_total_inner::call(root, &pathlist);
        }
    }

    // Convert mergeclauses to canonical pathkeys, consider orderings.
    let all_pathkeys = jp::select_outer_pathkeys_for_merge::call(root, &extra.mergeclause_list, joinrel)?;

    for l in 0..all_pathkeys.len() {
        // Make a pathkey list with this guy first.
        let outerkeys: PgVec<'mcx, PathKey> = if l != 0 {
            let mut v = mcx::vec_with_capacity_in(mcx, all_pathkeys.len())?;
            v.push(all_pathkeys[l].clone());
            for (i, pk) in all_pathkeys.iter().enumerate() {
                if i != l {
                    v.push(pk.clone());
                }
            }
            v
        } else {
            charged_pathkeys(mcx, &all_pathkeys)? // no work at first one
        };

        // Sort the mergeclauses into the corresponding ordering.
        let cur_mergeclauses =
            jp::find_mergeclauses_for_outer_pathkeys::call(root, &outerkeys, &extra.mergeclause_list)?;

        // Should have used them all.
        debug_assert_eq!(cur_mergeclauses.len(), extra.mergeclause_list.len());

        // Build sort pathkeys for the inner side.
        let innerkeys = jp::make_inner_pathkeys_for_merge::call(root, &cur_mergeclauses, &outerkeys)?;

        // Build pathkeys representing output sort order.
        let merge_pathkeys = jp::build_join_pathkeys::call(root, joinrel, jointype, &outerkeys)?;

        // Charge mcx copies of the inner/outer key lists for the try_* calls
        // (the C reuses the same lists; ownership-charged copies here).
        let outerkeys_copy = charged_pathkeys(mcx, &outerkeys)?;
        let innerkeys_copy = charged_pathkeys(mcx, &innerkeys)?;

        try_mergejoin_path(
            mcx,
            root,
            run,
            joinrel,
            outer_path,
            inner_path,
            &merge_pathkeys,
            &cur_mergeclauses,
            outerkeys_copy,
            innerkeys_copy,
            jointype,
            extra,
            false,
        )?;

        // Partial mergejoin if we have partial outer + parallel safe inner.
        if let (Some(cpo), Some(csi)) = (cheapest_partial_outer, cheapest_safe_inner) {
            try_partial_mergejoin_path(
                mcx,
                root,
                run,
                joinrel,
                cpo,
                csi,
                &merge_pathkeys,
                &cur_mergeclauses,
                charged_pathkeys(mcx, &outerkeys)?,
                charged_pathkeys(mcx, &innerkeys)?,
                jointype,
                extra,
            )?;
        }
    }

    Ok(())
}

/* ==========================================================================
 * generate_mergejoin_paths (joinpath.c:1563)
 * ======================================================================== */

fn generate_mergejoin_paths<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    joinrel: RelId,
    innerrel: RelId,
    outerpath: PathId,
    mut jointype: JoinType,
    extra: &JoinPathExtra,
    useallclauses: bool,
    inner_cheapest_total: PathId,
    merge_pathkeys: &[PathKey],
    is_partial: bool,
) -> PgResult<()> {
    let save_jointype = jointype;

    if jointype == JOIN_UNIQUE_OUTER || jointype == JOIN_UNIQUE_INNER {
        jointype = JOIN_INNER;
    }

    // Look for useful mergeclauses.
    let mergeclauses = {
        let outer_pk = charged_pathkeys(mcx, &root.path(outerpath).base().pathkeys)?;
        jp::find_mergeclauses_for_outer_pathkeys::call(root, &outer_pk, &extra.mergeclause_list)?
    };

    // Done if no chance for a mergejoin (FULL-with-no-clauses corner case).
    if mergeclauses.is_empty() {
        if jointype == JOIN_FULL {
            // okay to try for mergejoin
        } else {
            return Ok(());
        }
    }
    if useallclauses && mergeclauses.len() != extra.mergeclause_list.len() {
        return Ok(());
    }

    // Required ordering of the inner path.
    let innersortkeys = {
        let outer_pk = charged_pathkeys(mcx, &root.path(outerpath).base().pathkeys)?;
        jp::make_inner_pathkeys_for_merge::call(root, &mergeclauses, &outer_pk)?
    };

    // Mergejoin on the basis of sorting the cheapest inner.
    try_mergejoin_path(
        mcx,
        root,
        run,
        joinrel,
        outerpath,
        inner_cheapest_total,
        merge_pathkeys,
        &mergeclauses,
        mcx::vec_with_capacity_in(mcx, 0)?,
        charged_pathkeys(mcx, &innersortkeys)?,
        jointype,
        extra,
        is_partial,
    )?;

    // Can't do anything else if inner needs to be unique'd.
    if save_jointype == JOIN_UNIQUE_INNER {
        return Ok(());
    }

    // Look for presorted inner paths satisfying innersortkeys (or truncations).
    let mut cheapest_startup_inner: Option<PathId>;
    let mut cheapest_total_inner: Option<PathId>;
    if jp::pathkeys_contained_in::call(
        &innersortkeys,
        &root.path(inner_cheapest_total).base().pathkeys,
    ) {
        cheapest_startup_inner = Some(inner_cheapest_total);
        cheapest_total_inner = Some(inner_cheapest_total);
    } else {
        cheapest_startup_inner = None;
        cheapest_total_inner = None;
    }

    let num_sortkeys = innersortkeys.len();
    // trialsortkeys: modifiable copy only when truncation is possible.
    let mut trialsortkeys = innersortkeys.clone();

    let mut sortkeycnt = num_sortkeys;
    while sortkeycnt > 0 {
        let mut newclauses: Vec<RinfoId> = Vec::new();
        let mut have_newclauses = false;

        // Truncate trialsortkeys to sortkeycnt.
        trialsortkeys.truncate(sortkeycnt);

        let pathlist = mcx::slice_in(mcx, &root.rel(innerrel).pathlist)?;
        let innerpath = jp::get_cheapest_path_for_pathkeys::call(
            root,
            &pathlist,
            &trialsortkeys,
            &None,
            CostSelector::TOTAL_COST,
            is_partial,
        );
        if let Some(innerpath) = innerpath {
            if cheapest_total_inner.is_none()
                || jp::compare_path_costs::call(
                    root,
                    innerpath,
                    cheapest_total_inner.unwrap(),
                    CostSelector::TOTAL_COST,
                ) < 0
            {
                // Select the right mergeclauses, if we didn't already.
                if sortkeycnt < num_sortkeys {
                    newclauses = jp::trim_mergeclauses_for_inner_pathkeys::call(
                        root,
                        &mergeclauses,
                        &trialsortkeys,
                    )?;
                    debug_assert!(!newclauses.is_empty());
                } else {
                    newclauses = mergeclauses.clone();
                }
                have_newclauses = true;
                try_mergejoin_path(
                    mcx,
                    root,
                    run,
                    joinrel,
                    outerpath,
                    innerpath,
                    merge_pathkeys,
                    &newclauses,
                    mcx::vec_with_capacity_in(mcx, 0)?,
                    mcx::vec_with_capacity_in(mcx, 0)?,
                    jointype,
                    extra,
                    is_partial,
                )?;
                cheapest_total_inner = Some(innerpath);
            }
        }

        // Same on the basis of cheapest startup cost.
        let innerpath = jp::get_cheapest_path_for_pathkeys::call(
            root,
            &pathlist,
            &trialsortkeys,
            &None,
            CostSelector::STARTUP_COST,
            is_partial,
        );
        if let Some(innerpath) = innerpath {
            if cheapest_startup_inner.is_none()
                || jp::compare_path_costs::call(
                    root,
                    innerpath,
                    cheapest_startup_inner.unwrap(),
                    CostSelector::STARTUP_COST,
                ) < 0
            {
                if Some(innerpath) != cheapest_total_inner {
                    // Avoid rebuilding clause list if we already made one.
                    if !have_newclauses {
                        if sortkeycnt < num_sortkeys {
                            newclauses = jp::trim_mergeclauses_for_inner_pathkeys::call(
                                root,
                                &mergeclauses,
                                &trialsortkeys,
                            )?;
                            debug_assert!(!newclauses.is_empty());
                        } else {
                            newclauses = mergeclauses.clone();
                        }
                        have_newclauses = true;
                    }
                    let _ = have_newclauses;
                    try_mergejoin_path(
                        mcx,
                        root,
                        run,
                        joinrel,
                        outerpath,
                        innerpath,
                        merge_pathkeys,
                        &newclauses,
                        mcx::vec_with_capacity_in(mcx, 0)?,
                        mcx::vec_with_capacity_in(mcx, 0)?,
                        jointype,
                        extra,
                        is_partial,
                    )?;
                }
                cheapest_startup_inner = Some(innerpath);
            }
        }

        // Don't consider truncated sortkeys if we need all clauses.
        if useallclauses {
            break;
        }

        sortkeycnt -= 1;
    }

    Ok(())
}

/* ==========================================================================
 * match_unsorted_outer (joinpath.c:1811)
 * ======================================================================== */

fn match_unsorted_outer<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    joinrel: RelId,
    outerrel: RelId,
    innerrel: RelId,
    mut jointype: JoinType,
    extra: &JoinPathExtra,
    enable: JoinEnableFlags,
) -> PgResult<()> {
    let save_jointype = jointype;
    let nestjoinOK;
    let useallclauses;
    let mut inner_cheapest_total = root.rel(innerrel).cheapest_total_path;
    let mut matpath: Option<PathId> = None;

    // No RIGHT_SEMI in mergejoin/nestloop.
    if jointype == JOIN_RIGHT_SEMI {
        return Ok(());
    }

    match jointype {
        JOIN_INNER | JOIN_LEFT | JOIN_SEMI | JOIN_ANTI => {
            nestjoinOK = true;
            useallclauses = false;
        }
        JOIN_RIGHT | JOIN_RIGHT_ANTI | JOIN_FULL => {
            nestjoinOK = false;
            useallclauses = true;
        }
        JOIN_UNIQUE_OUTER | JOIN_UNIQUE_INNER => {
            jointype = JOIN_INNER;
            nestjoinOK = true;
            useallclauses = false;
        }
        other => {
            // elog(ERROR, "unrecognized join type: %d", jointype)
            return Err(types_error::PgError::error(alloc::format!(
                "unrecognized join type: {other}"
            )));
        }
    }

    // Ignore inner_cheapest_total if parameterized by the outer rel.
    if let Some(ict) = inner_cheapest_total {
        if path_param_by_rel(root, ict, outerrel) {
            inner_cheapest_total = None;
        }
    }

    if save_jointype == JOIN_UNIQUE_INNER {
        // Only the cheapest-total inner, unique-ified.
        let ict = match inner_cheapest_total {
            Some(p) => p,
            None => return Ok(()),
        };
        inner_cheapest_total = jp::create_unique_path::call(run, root, innerrel, ict, &extra.sjinfo)?;
        if inner_cheapest_total.is_none() {
            return Ok(()); // Assert(inner_cheapest_total)
        }
    } else if nestjoinOK {
        // Consider materializing the cheapest inner path.
        if enable.enable_material {
            if let Some(ict) = inner_cheapest_total {
                if !jp::exec_materializes_output::call(root.path(ict).base().pathtype) {
                    matpath = Some(jp::create_material_path::call(root, innerrel, ict)?);
                }
            }
        }
    }

    let outer_pathlist = mcx::slice_in(mcx, &root.rel(outerrel).pathlist)?;
    for &outerpath_orig in outer_pathlist.iter() {
        let mut outerpath = outerpath_orig;

        // Can't use an outer path parameterized by the inner rel.
        if path_param_by_rel(root, outerpath, innerrel) {
            continue;
        }

        // Unique-ify the outer path: only the cheapest outer.
        if save_jointype == JOIN_UNIQUE_OUTER {
            if Some(outerpath) != root.rel(outerrel).cheapest_total_path {
                continue;
            }
            outerpath = match jp::create_unique_path::call(run, root, outerrel, outerpath, &extra.sjinfo)?
            {
                Some(p) => p,
                None => continue, // Assert(outerpath)
            };
        }

        let merge_pathkeys = {
            let outer_pk = charged_pathkeys(mcx, &root.path(outerpath).base().pathkeys)?;
            jp::build_join_pathkeys::call(root, joinrel, jointype, &outer_pk)?
        };

        if save_jointype == JOIN_UNIQUE_INNER {
            // Nestloop with the unique-ified cheapest inner path.
            try_nestloop_path(
                root,
                run,
                joinrel,
                outerpath,
                inner_cheapest_total.unwrap(),
                &merge_pathkeys,
                jointype,
                extra,
            )?;
        } else if nestjoinOK {
            // Nestloop on each parameterized inner path.
            let inner_params = mcx::slice_in(mcx, &root.rel(innerrel).cheapest_parameterized_paths)?;
            for &innerpath in inner_params.iter() {
                try_nestloop_path(
                    root,
                    run,
                    joinrel,
                    outerpath,
                    innerpath,
                    &merge_pathkeys,
                    jointype,
                    extra,
                )?;

                // Try a memoize path atop the nested loop.
                let mpath = get_memoize_path(
                    mcx, root, innerrel, outerrel, innerpath, outerpath, jointype, extra,
                    enable.enable_memoize,
                )?;
                if let Some(mpath) = mpath {
                    try_nestloop_path(
                        root,
                        run,
                        joinrel,
                        outerpath,
                        mpath,
                        &merge_pathkeys,
                        jointype,
                        extra,
                    )?;
                }
            }

            // Materialized form of the cheapest inner path.
            if let Some(matpath) = matpath {
                try_nestloop_path(
                    root,
                    run,
                    joinrel,
                    outerpath,
                    matpath,
                    &merge_pathkeys,
                    jointype,
                    extra,
                )?;
            }
        }

        // Can't do anything else if outer needs to be unique'd.
        if save_jointype == JOIN_UNIQUE_OUTER {
            continue;
        }

        // Can't do anything else if inner is parameterized by outer.
        if inner_cheapest_total.is_none() {
            continue;
        }

        // Generate merge join paths.
        generate_mergejoin_paths(
            mcx,
            root,
            run,
            joinrel,
            innerrel,
            outerpath,
            save_jointype,
            extra,
            useallclauses,
            inner_cheapest_total.unwrap(),
            &merge_pathkeys,
            false,
        )?;
    }

    // Consider partial nestloop and mergejoin plans.
    if root.rel(joinrel).consider_parallel
        && save_jointype != JOIN_UNIQUE_OUTER
        && save_jointype != JOIN_FULL
        && save_jointype != JOIN_RIGHT
        && save_jointype != JOIN_RIGHT_ANTI
        && !root.rel(outerrel).partial_pathlist.is_empty()
        && bms::relids_is_empty::call(&root.rel(joinrel).lateral_relids)
    {
        if nestjoinOK {
            consider_parallel_nestloop(
                mcx, root, run, joinrel, outerrel, innerrel, save_jointype, extra, enable,
            )?;
        }

        // If inner_cheapest_total is NULL or non parallel-safe, find cheapest.
        let need_safe = match inner_cheapest_total {
            None => true,
            Some(ict) => !root.path(ict).base().parallel_safe,
        };
        if need_safe {
            if save_jointype == JOIN_UNIQUE_INNER {
                return Ok(());
            }
            let pathlist = mcx::slice_in(mcx, &root.rel(innerrel).pathlist)?;
            inner_cheapest_total = jp::get_cheapest_parallel_safe_total_inner::call(root, &pathlist);
        }

        if let Some(ict) = inner_cheapest_total {
            consider_parallel_mergejoin(
                mcx, root, run, joinrel, outerrel, innerrel, save_jointype, extra, ict,
            )?;
        }
    }

    Ok(())
}

/* ==========================================================================
 * consider_parallel_mergejoin (joinpath.c:2070)
 * ======================================================================== */

fn consider_parallel_mergejoin<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    joinrel: RelId,
    outerrel: RelId,
    innerrel: RelId,
    jointype: JoinType,
    extra: &JoinPathExtra,
    inner_cheapest_total: PathId,
) -> PgResult<()> {
    let partial = mcx::slice_in(mcx, &root.rel(outerrel).partial_pathlist)?;
    for &outerpath in partial.iter() {
        let merge_pathkeys = {
            let outer_pk = charged_pathkeys(mcx, &root.path(outerpath).base().pathkeys)?;
            jp::build_join_pathkeys::call(root, joinrel, jointype, &outer_pk)?
        };
        generate_mergejoin_paths(
            mcx,
            root,
            run,
            joinrel,
            innerrel,
            outerpath,
            jointype,
            extra,
            false,
            inner_cheapest_total,
            &merge_pathkeys,
            true,
        )?;
    }

    Ok(())
}

/* ==========================================================================
 * consider_parallel_nestloop (joinpath.c:2110)
 * ======================================================================== */

fn consider_parallel_nestloop<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    joinrel: RelId,
    outerrel: RelId,
    innerrel: RelId,
    mut jointype: JoinType,
    extra: &JoinPathExtra,
    enable: JoinEnableFlags,
) -> PgResult<()> {
    let save_jointype = jointype;
    let inner_cheapest_total = root.rel(innerrel).cheapest_total_path;
    let mut matpath: Option<PathId> = None;

    if jointype == JOIN_UNIQUE_INNER {
        jointype = JOIN_INNER;
    }

    // Consider materializing the cheapest inner path (5 conditions).
    if let Some(ict) = inner_cheapest_total {
        if save_jointype != JOIN_UNIQUE_INNER
            && enable.enable_material
            && root.path(ict).base().parallel_safe
            && !path_param_by_rel(root, ict, outerrel)
            && !jp::exec_materializes_output::call(root.path(ict).base().pathtype)
        {
            matpath = Some(jp::create_material_path::call(root, innerrel, ict)?);
            debug_assert!(root.path(matpath.unwrap()).base().parallel_safe);
        }
    }

    let partial = mcx::slice_in(mcx, &root.rel(outerrel).partial_pathlist)?;
    for &outerpath in partial.iter() {
        let pathkeys = {
            let outer_pk = charged_pathkeys(mcx, &root.path(outerpath).base().pathkeys)?;
            jp::build_join_pathkeys::call(root, joinrel, jointype, &outer_pk)?
        };

        let inner_params = mcx::slice_in(mcx, &root.rel(innerrel).cheapest_parameterized_paths)?;
        for &innerpath_orig in inner_params.iter() {
            let mut innerpath = innerpath_orig;

            // Can't join to a non-parallel-safe inner path.
            if !root.path(innerpath).base().parallel_safe {
                continue;
            }

            if save_jointype == JOIN_UNIQUE_INNER {
                if Some(innerpath) != root.rel(innerrel).cheapest_total_path {
                    continue;
                }
                innerpath =
                    match jp::create_unique_path::call(run, root, innerrel, innerpath, &extra.sjinfo)? {
                        Some(p) => p,
                        None => continue,
                    };
            }

            try_partial_nestloop_path(root, run, joinrel, outerpath, innerpath, &pathkeys, jointype, extra)?;

            // Try a memoize path.
            let mpath = get_memoize_path(
                mcx, root, innerrel, outerrel, innerpath, outerpath, jointype, extra,
                enable.enable_memoize,
            )?;
            if let Some(mpath) = mpath {
                try_partial_nestloop_path(root, run, joinrel, outerpath, mpath, &pathkeys, jointype, extra)?;
            }
        }

        // Materialized form of the cheapest inner path.
        if let Some(matpath) = matpath {
            try_partial_nestloop_path(root, run, joinrel, outerpath, matpath, &pathkeys, jointype, extra)?;
        }
    }

    Ok(())
}

/* ==========================================================================
 * hash_inner_and_outer (joinpath.c:2219)
 * ======================================================================== */

fn hash_inner_and_outer<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    joinrel: RelId,
    outerrel: RelId,
    innerrel: RelId,
    mut jointype: JoinType,
    extra: &JoinPathExtra,
    enable: JoinEnableFlags,
) -> PgResult<()> {
    let save_jointype = jointype;
    let isouterjoin = is_outer_join(jointype);

    // Build the single hashclauses list usable for this pair.
    let mut hashclauses: PgVec<'mcx, RinfoId> = mcx::vec_with_capacity_in(mcx, 0)?;
    let joinrel_relids = clone_relids(&root.rel(joinrel).relids);
    let outer_relids = clone_relids(&root.rel(outerrel).relids);
    let inner_relids = clone_relids(&root.rel(innerrel).relids);
    let restrictlist = mcx::slice_in(mcx, &extra.restrictlist)?;
    for &restrictinfo in restrictlist.iter() {
        // For outer joins only use own join clauses.
        if isouterjoin && rinfo_is_pushed_down(root, restrictinfo, &joinrel_relids) {
            continue;
        }

        let (can_join, hashop) = {
            let ri = root.rinfo(restrictinfo);
            (ri.can_join, ri.hashjoinoperator)
        };
        if !can_join || hashop == 0 {
            continue; // not hashjoinable
        }

        // Check clause form "outer op inner" / "inner op outer".
        if !jp::clause_sides_match_join::call(root, restrictinfo, &outer_relids, &inner_relids) {
            continue;
        }

        // If "inner op outer", require valid commutator.
        if !root.rinfo(restrictinfo).outer_is_left
            && lsc::get_commutator::call(jp::clause_opexpr_opno::call(root, restrictinfo))? == 0
        {
            continue;
        }

        charged_push(mcx, &mut hashclauses, restrictinfo)?;
    }

    // If we found any usable hashclauses, make paths.
    if hashclauses.is_empty() {
        return Ok(());
    }

    let cheapest_startup_outer = root.rel(outerrel).cheapest_startup_path;
    let mut cheapest_total_outer = match root.rel(outerrel).cheapest_total_path {
        Some(p) => p,
        None => return Ok(()),
    };
    let mut cheapest_total_inner = match root.rel(innerrel).cheapest_total_path {
        Some(p) => p,
        None => return Ok(()),
    };

    // If either cheapest-total is parameterized by the other rel, no hashjoin.
    if path_param_by_rel(root, cheapest_total_outer, innerrel)
        || path_param_by_rel(root, cheapest_total_inner, outerrel)
    {
        return Ok(());
    }

    if jointype == JOIN_UNIQUE_OUTER {
        cheapest_total_outer =
            match jp::create_unique_path::call(run, root, outerrel, cheapest_total_outer, &extra.sjinfo)? {
                Some(p) => p,
                None => return Ok(()),
            };
        jointype = JOIN_INNER;
        try_hashjoin_path(
            root,
            run,
            joinrel,
            cheapest_total_outer,
            cheapest_total_inner,
            &hashclauses,
            jointype,
            extra,
        )?;
    // no possibility of cheap startup here
    } else if jointype == JOIN_UNIQUE_INNER {
        cheapest_total_inner =
            match jp::create_unique_path::call(run, root, innerrel, cheapest_total_inner, &extra.sjinfo)? {
                Some(p) => p,
                None => return Ok(()),
            };
        jointype = JOIN_INNER;
        try_hashjoin_path(
            root,
            run,
            joinrel,
            cheapest_total_outer,
            cheapest_total_inner,
            &hashclauses,
            jointype,
            extra,
        )?;
        if let Some(cso) = cheapest_startup_outer {
            if cso != cheapest_total_outer {
                try_hashjoin_path(
                    root,
                    run,
                    joinrel,
                    cso,
                    cheapest_total_inner,
                    &hashclauses,
                    jointype,
                    extra,
                )?;
            }
        }
    } else {
        // Other jointypes.
        if let Some(cso) = cheapest_startup_outer {
            try_hashjoin_path(
                root,
                run,
                joinrel,
                cso,
                cheapest_total_inner,
                &hashclauses,
                jointype,
                extra,
            )?;
        }

        let outer_params = mcx::slice_in(mcx, &root.rel(outerrel).cheapest_parameterized_paths)?;
        for &outerpath in outer_params.iter() {
            // Can't use an outer path parameterized by the inner rel.
            if path_param_by_rel(root, outerpath, innerrel) {
                continue;
            }

            let inner_params = mcx::slice_in(mcx, &root.rel(innerrel).cheapest_parameterized_paths)?;
            for &innerpath in inner_params.iter() {
                // Can't use an inner path parameterized by the outer rel.
                if path_param_by_rel(root, innerpath, outerrel) {
                    continue;
                }

                if Some(outerpath) == cheapest_startup_outer && innerpath == cheapest_total_inner {
                    continue; // already tried it
                }

                try_hashjoin_path(
                    root,
                    run,
                    joinrel,
                    outerpath,
                    innerpath,
                    &hashclauses,
                    jointype,
                    extra,
                )?;
            }
        }
    }

    // Partial hash join.
    if root.rel(joinrel).consider_parallel
        && save_jointype != JOIN_UNIQUE_OUTER
        && save_jointype != JOIN_RIGHT_SEMI
        && !root.rel(outerrel).partial_pathlist.is_empty()
        && bms::relids_is_empty::call(&root.rel(joinrel).lateral_relids)
    {
        let cheapest_partial_outer = root.rel(outerrel).partial_pathlist[0];
        let mut cheapest_safe_inner: Option<PathId> = None;

        // Partial inner plan too (shared hash table)?
        if !root.rel(innerrel).partial_pathlist.is_empty()
            && save_jointype != JOIN_UNIQUE_INNER
            && enable.enable_parallel_hash
        {
            let cheapest_partial_inner = root.rel(innerrel).partial_pathlist[0];
            try_partial_hashjoin_path(
                root,
                run,
                joinrel,
                cheapest_partial_outer,
                cheapest_partial_inner,
                &hashclauses,
                jointype,
                extra,
                true, // parallel_hash
            )?;
        }

        // Cheapest safe unparameterized inner.
        if save_jointype == JOIN_FULL
            || save_jointype == JOIN_RIGHT
            || save_jointype == JOIN_RIGHT_ANTI
        {
            cheapest_safe_inner = None;
        } else if root.path(cheapest_total_inner).base().parallel_safe {
            cheapest_safe_inner = Some(cheapest_total_inner);
        } else if save_jointype != JOIN_UNIQUE_INNER {
            let pathlist = mcx::slice_in(mcx, &root.rel(innerrel).pathlist)?;
            cheapest_safe_inner = jp::get_cheapest_parallel_safe_total_inner::call(root, &pathlist);
        }

        if let Some(csi) = cheapest_safe_inner {
            try_partial_hashjoin_path(
                root,
                run,
                joinrel,
                cheapest_partial_outer,
                csi,
                &hashclauses,
                jointype,
                extra,
                false, // parallel_hash
            )?;
        }
    }

    Ok(())
}

/* ==========================================================================
 * select_mergejoin_clauses (joinpath.c:2500)
 * ======================================================================== */

fn select_mergejoin_clauses<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    joinrel: RelId,
    outerrel: RelId,
    innerrel: RelId,
    restrictlist: &[RinfoId],
    jointype: JoinType,
    mergejoin_allowed: &mut bool,
) -> PgResult<PgVec<'mcx, RinfoId>> {
    let mut result_list: PgVec<'mcx, RinfoId> = mcx::vec_with_capacity_in(mcx, 0)?;
    let isouterjoin = is_outer_join(jointype);
    let mut have_nonmergeable_joinclause = false;

    // No RIGHT_SEMI in mergejoin.
    if jointype == JOIN_RIGHT_SEMI {
        *mergejoin_allowed = false;
        return Ok(result_list);
    }

    let joinrel_relids = clone_relids(&root.rel(joinrel).relids);
    let outer_relids = clone_relids(&root.rel(outerrel).relids);
    let inner_relids = clone_relids(&root.rel(innerrel).relids);

    for &restrictinfo in restrictlist.iter() {
        // For outer joins only use own join clauses.
        if isouterjoin && rinfo_is_pushed_down(root, restrictinfo, &joinrel_relids) {
            continue;
        }

        // Mergeable operator clause?
        let (can_join, mergeopfamilies_empty) = {
            let ri = root.rinfo(restrictinfo);
            (ri.can_join, ri.mergeopfamilies.is_empty())
        };
        if !can_join || mergeopfamilies_empty {
            // Executor handles extra constant joinquals, nothing else, for
            // right/right-anti/full merge join.
            if !jp::clause_is_const::call(root, restrictinfo) {
                have_nonmergeable_joinclause = true;
            }
            continue; // not mergejoinable
        }

        // Clause form "outer op inner" / "inner op outer".
        if !jp::clause_sides_match_join::call(root, restrictinfo, &outer_relids, &inner_relids) {
            have_nonmergeable_joinclause = true;
            continue; // no good for these input relations
        }

        // If "inner op outer", require valid commutator.
        if !root.rinfo(restrictinfo).outer_is_left
            && lsc::get_commutator::call(jp::clause_opexpr_opno::call(root, restrictinfo))? == 0
        {
            have_nonmergeable_joinclause = true;
            continue;
        }

        // Insist each side have a non-redundant eclass.
        jp::update_mergeclause_eclasses::call(root, restrictinfo)?;

        if jp::ec_must_be_redundant_left::call(root, restrictinfo)
            || jp::ec_must_be_redundant_right::call(root, restrictinfo)
        {
            have_nonmergeable_joinclause = true;
            continue; // can't handle redundant eclasses
        }

        charged_push(mcx, &mut result_list, restrictinfo)?;
    }

    // Report whether mergejoin is allowed.
    match jointype {
        JOIN_RIGHT | JOIN_RIGHT_ANTI | JOIN_FULL => {
            *mergejoin_allowed = !have_nonmergeable_joinclause;
        }
        _ => {
            *mergejoin_allowed = true;
        }
    }

    Ok(result_list)
}

/* ==========================================================================
 * RINFO_IS_PUSHED_DOWN (pathnodes.h) — `rinfo->is_pushed_down ||
 * !bms_is_subset(rinfo->required_relids, joinrelids)`.
 * ======================================================================== */

fn rinfo_is_pushed_down(root: &PlannerInfo, rinfo: RinfoId, joinrelids: &Relids) -> bool {
    let ri = root.rinfo(rinfo);
    ri.is_pushed_down || !bms::relids_is_subset::call(&ri.required_relids, joinrelids)
}

/// `joinrel->fdwroutine->GetForeignJoinPaths(...)` if set (joinpath.c step 5).
///
/// A baserel/joinrel only carries an `fdwroutine` once the FDW machinery has
/// resolved one for a foreign table or foreign join; in that case the planner
/// gives the FDW a chance to push the join down. In the current bootstrap no
/// `RelOptInfo` ever has `fdwroutine` set, so this faithfully mirrors the C
/// `if (joinrel->fdwroutine && joinrel->fdwroutine->GetForeignJoinPaths)`
/// guard: when there is no routine it is a no-op (the C `false` branch). The
/// modelled [`FdwRoutine`](types_nodes::FdwRoutine) carries only the
/// executor-time callbacks, so a present routine's planner-time
/// `GetForeignJoinPaths` callback is not yet expressible — reaching it is the
/// unported FDW join-pushdown path and panics loudly.
fn fdw_get_foreign_join_paths(
    root: &mut PlannerInfo,
    joinrel: RelId,
    _outerrel: RelId,
    _innerrel: RelId,
    _jointype: JoinType,
    _extra: &JoinPathExtraData,
) -> PgResult<()> {
    if root.rel(joinrel).fdwroutine.is_some() {
        panic!(
            "GetForeignJoinPaths: FDW join pushdown is not ported \
             (FdwRoutine planner callbacks are unmodeled)"
        );
    }
    Ok(())
}

/// `set_join_pathlist_hook(...)` if installed (joinpath.c step 6).
///
/// `set_join_pathlist_hook` is a plain global function pointer that defaults to
/// `NULL`; an extension assigns it from its `_PG_init`. With no extension
/// loaded the pointer is `NULL`, so the C `if (set_join_pathlist_hook)` guard
/// is false and this is a no-op. There is no extension-hook registry yet, so
/// the hook is always absent here and this faithfully reproduces the default.
fn set_join_pathlist_hook(
    _root: &mut PlannerInfo,
    _joinrel: RelId,
    _outerrel: RelId,
    _innerrel: RelId,
    _jointype: JoinType,
    _extra: &JoinPathExtraData,
) -> PgResult<()> {
    Ok(())
}

/// Install the seams joinpath owns. The join-method enumeration itself is
/// reached through the direct `add_paths_to_joinrel` call from the join-search
/// driver (`joinrels.c`), not a seam; the cross-subsystem seams joinpath *calls*
/// (pathnode.c / costsize.c / pathkeys.c / …) are installed by their own owners.
/// What joinpath.c owns are its two trailing hook dispatch sites — the FDW
/// join-pushdown callback and the `set_join_pathlist_hook` extension hook — both
/// of which default to no-op (no FDW routine / NULL hook) in C.
/* --------------------------------------------------------------------------
 * RestrictInfo clause-payload accessors (joinpath-seams). The clause node lives
 * in this crate's planner arena and is reached only by `RinfoId` handle, so
 * joinpath owns these thin reads and installs them itself.
 * ------------------------------------------------------------------------ */

/// `restrictinfo->clause && IsA(restrictinfo->clause, Const)`.
fn clause_is_const(root: &PlannerInfo, rinfo: RinfoId) -> bool {
    let clause = root.rinfo(rinfo).clause;
    matches!(root.node(clause), types_nodes::primnodes::Expr::Const(_))
}

/// `castNode(OpExpr, restrictinfo->clause)->opno` — the operator OID of a
/// hash/merge-joinable clause (known to be an `OpExpr`).
fn clause_opexpr_opno(root: &PlannerInfo, rinfo: RinfoId) -> Oid {
    let clause = root.rinfo(rinfo).clause;
    match root.node(clause) {
        types_nodes::primnodes::Expr::OpExpr(o) => o.opno,
        _ => panic!(
            "backend-optimizer-path-joinpath::clause_opexpr_opno: clause is not an OpExpr (the \
             caller restricts this to mergejoinable clauses)"
        ),
    }
}

/// `IsA(rinfo->clause, OpExpr) && list_length(opexpr->args) == 2`.
fn clause_is_opexpr_with_two_args(root: &PlannerInfo, rinfo: RinfoId) -> bool {
    let clause = root.rinfo(rinfo).clause;
    match root.node(clause) {
        types_nodes::primnodes::Expr::OpExpr(o) => o.args.len() == 2,
        _ => false,
    }
}

// `list_nth(castNode(OpExpr, rinfo->clause)->args, n)` — the n-th arg of a 2-arg
// OpExpr join clause. The owned-arena `OpExpr` stores its args as inline `Expr`
// values, so producing a node handle interns a clone into the node arena (hence
// the `&mut PlannerInfo`). Reached on the Memoize path
// (`paraminfo_get_equal_hashops`).
fn opexpr_arg(root: &mut PlannerInfo, rinfo: RinfoId, n: i32) -> NodeId {
    let clause = root.rinfo(rinfo).clause;
    let arg = match root.node(clause) {
        types_nodes::primnodes::Expr::OpExpr(o) => o.args[n as usize].clone(),
        _ => unreachable!("opexpr_arg: rinfo->clause is not an OpExpr"),
    };
    root.alloc_node(arg)
}

pub fn init_seams() {
    jp::fdw_get_foreign_join_paths::set(fdw_get_foreign_join_paths);
    jp::set_join_pathlist_hook::set(set_join_pathlist_hook);

    // RestrictInfo clause-payload reads over this crate's arena.
    jp::clause_is_const::set(clause_is_const);
    jp::clause_opexpr_opno::set(clause_opexpr_opno);
    jp::clause_is_opexpr_with_two_args::set(clause_is_opexpr_with_two_args);
    jp::opexpr_arg::set(opexpr_arg);
}

#[cfg(test)]
mod tests;
