#![forbid(unsafe_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::collapsible_else_if)]
#![allow(clippy::needless_late_init)]
#![allow(clippy::too_many_arguments)]

//! Safe-Rust port of `src/backend/optimizer/path/joinrels.c` (postgres-18.3):
//! the join-relation enumeration driver of the dynamic-programming join search.
//!
//! At each level `N`, [`join_search_one_level`] pairs up rels of `N-1` lower-level
//! members, decides whether each candidate join is legal and its join type
//! (`join_is_legal`), finds-or-builds the resulting join `RelOptInfo`
//! ([`make_join_rel`] → `populate_joinrel_with_paths`), and — for partitioned
//! inputs — recurses into matching child partitions (`try_partitionwise_join`).
//!
//! # Arena model
//!
//! The C pointer graph is modelled over the
//! [`PlannerInfo`](pathnodes::PlannerInfo) arena: a
//! [`RelId`](pathnodes::RelId)/[`PathId`](pathnodes::PathId)/
//! [`RinfoId`](pathnodes::RinfoId) handle indexes the matching arena, and
//! `root.rel(id)` / `root.path(id)` / `root.rinfo(id)` recover the node.
//! `join_rel_level` is a `Vec<Vec<RelId>>`; the `bms_*` set algebra over
//! `Relids` crosses through the canonical `relids_*` seams in
//! `backend-optimizer-util-relnode-seams`. Sibling-optimizer externals (pathnode,
//! joinpath, joininfo/geqo, relnode, appendinfo, partbounds, the `Const`-clause
//! reads) all cross through their owners' `*-seams` crates.
//!
//! Functions that `palloc` in C carry the OOM channel as
//! [`PgResult`](types_error::PgResult); the in-crate working `Vec`s
//! (`pushed_down_joins`, the partition-pairing lists) reserve fallibly first.
//!
//! There is no `extern "C"`, no raw pointers, and no `c_void`.

extern crate alloc;

use alloc::vec::Vec;

use types_error::{PgError, PgResult};
use nodes::nodes::NodeTag;

use pathnodes::{
    PathNode, PlannerInfo, RelId, Relids, RinfoId, SpecialJoinInfo, JOIN_ANTI, JOIN_FULL,
    JOIN_INNER, JOIN_LEFT, JOIN_RIGHT, JOIN_RIGHT_ANTI, JOIN_RIGHT_SEMI, JOIN_SEMI,
    JOIN_UNIQUE_INNER, JOIN_UNIQUE_OUTER, RELOPT_BASEREL, RELOPT_OTHER_MEMBER_REL,
};

use mcx::Mcx;
use pathnodes::planner_run::PlannerRun;

use geqo_all_seams as geqo;
use joinpath::{add_paths_to_joinrel, JoinEnableFlags};
use appendinfo_seams as appendinfo;
use pathnode_seams as pathnode;
use relnode_seams as bms;
use partbounds_seams as partbounds;
use postgres_seams as tcop;

/* --------------------------------------------------------------------------
 * NodeTag constant mirrored from the canonical generated table
 * (`T_SpecialJoinInfo = 320` in nodes/nodetags.h).
 * ------------------------------------------------------------------------ */

/// `T_SpecialJoinInfo` (nodes/nodetags.h) — written by `makeNode(SpecialJoinInfo)`.
///
/// The fabled `SpecialJoinInfo` keystone struct carries no `type_`/NodeTag field
/// (the tag lives in the unified `Node` enum discriminant), so this constant is
/// retained only for parity with the C source and the canonical tag table.
pub const T_SpecialJoinInfo: NodeTag = NodeTag(320);

/// Map a `TryReserveError` onto the project's owned out-of-memory `PgError`, per
/// the allocation-safety rule: every data-derived `Vec` growth reserves fallibly.
#[inline]
fn oom(_: alloc::collections::TryReserveError) -> PgError {
    PgError::error("out of memory")
}

/// Snapshot the join-method enable GUCs into a [`JoinEnableFlags`] value for
/// `add_paths_to_joinrel`.
///
/// In C these are file-scope GUC globals (`enable_mergejoin`, `enable_hashjoin`,
/// `enable_material`, `enable_parallel_hash`, `enable_memoize`) read directly
/// inside `add_paths_to_joinrel`. The fabled joinpath port lifts them to an
/// explicit value parameter (no ambient globals), so we read the current GUC
/// values here and pass them along — behaviour-identical to C's direct reads.
#[inline]
fn join_enable_flags() -> JoinEnableFlags {
    use guc_tables::vars;
    JoinEnableFlags {
        enable_mergejoin: vars::enable_mergejoin.read(),
        enable_hashjoin: vars::enable_hashjoin.read(),
        enable_material: vars::enable_material.read(),
        enable_parallel_hash: vars::enable_parallel_hash.read(),
        enable_memoize: vars::enable_memoize.read(),
    }
}

/* --------------------------------------------------------------------------
 * Inline accessors mirroring the pathnodes.h macros (pure struct reads).
 * ------------------------------------------------------------------------ */

/// `IS_SIMPLE_REL(rel)` (pathnodes.h) — base rel or "other member" rel.
#[inline]
pub fn is_simple_rel(rel: &pathnodes::RelOptInfo) -> bool {
    rel.reloptkind == RELOPT_BASEREL || rel.reloptkind == RELOPT_OTHER_MEMBER_REL
}

/// `IS_PARTITIONED_REL(rel)` field-only conjuncts (pathnodes.h): has a scheme,
/// bound info, at least one partition, and the per-partition rel array.
///
/// The macro's `&& !IS_DUMMY_REL(rel)` conjunct depends on the `is_dummy_rel`
/// Path descent and is applied at the callsite (as in C).
#[inline]
pub fn is_partitioned_rel(rel: &pathnodes::RelOptInfo) -> bool {
    rel.part_scheme.is_some() && rel.boundinfo.is_some() && rel.nparts > 0 && !rel.part_rels.is_empty()
}

/// `REL_HAS_ALL_PART_PROPS(rel)` (pathnodes.h) — every required member set.
#[inline]
pub fn rel_has_all_part_props(rel: &pathnodes::RelOptInfo) -> bool {
    rel.part_scheme.is_some()
        && rel.boundinfo.is_some()
        && rel.nparts > 0
        && !rel.part_rels.is_empty()
        && !rel.partexprs.is_empty()
        && !rel.nullable_partexprs.is_empty()
}

/* --------------------------------------------------------------------------
 * init_dummy_sjinfo  (joinrels.c:660)
 * ------------------------------------------------------------------------ */

/// `init_dummy_sjinfo` (joinrels.c:660) — populate `sjinfo` for a plain inner
/// join between the left and right relations.
///
/// Ported 1:1. The C code aliases the same `Relids` pointer into all four
/// min/syn slots; with owned values we store equal clones (the values are equal,
/// exactly as the four aliased pointers are in C). The fabled `SpecialJoinInfo`
/// has no `type_`/`semi_rhs_exprs` field, so those C assignments are dropped
/// (the NodeTag lives in the unified `Node` discriminant; `semi_rhs_exprs` is
/// not carried in this consumer-facing keystone).
pub fn init_dummy_sjinfo(sjinfo: &mut SpecialJoinInfo, left_relids: Relids, right_relids: Relids) {
    sjinfo.syn_lefthand = left_relids.clone();
    sjinfo.min_lefthand = left_relids;
    sjinfo.syn_righthand = right_relids.clone();
    sjinfo.min_righthand = right_relids;
    sjinfo.jointype = JOIN_INNER;
    sjinfo.ojrelid = 0;
    sjinfo.commute_above_l = None;
    sjinfo.commute_above_r = None;
    sjinfo.commute_below_l = None;
    sjinfo.commute_below_r = None;
    /* we don't bother trying to make the remaining fields valid */
    sjinfo.lhs_strict = false;
    sjinfo.semi_can_btree = false;
    sjinfo.semi_can_hash = false;
    sjinfo.semi_operators = Vec::new();
}

/// A fully-zeroed `SpecialJoinInfo` — the owned-tree analogue of the C stack
/// `SpecialJoinInfo sjinfo_data` that `init_dummy_sjinfo` then fills.
fn make_dummy_sjinfo() -> SpecialJoinInfo {
    SpecialJoinInfo {
        min_lefthand: None,
        min_righthand: None,
        syn_lefthand: None,
        syn_righthand: None,
        jointype: JOIN_INNER,
        ojrelid: 0,
        commute_above_l: None,
        commute_above_r: None,
        commute_below_l: None,
        commute_below_r: None,
        lhs_strict: false,
        semi_can_btree: false,
        semi_can_hash: false,
        semi_operators: Vec::new(),
        semi_rhs_exprs: Vec::new(),
    }
}

/* ==========================================================================
 * is_dummy_rel (joinrels.c:1275)
 * ======================================================================== */

/// `IS_DUMMY_APPEND(p)` (pathnodes.h) — `IsA(p, AppendPath) && p->subpaths == NIL`.
fn is_dummy_append(node: &PathNode) -> bool {
    matches!(node, PathNode::AppendPath(ap) if ap.subpaths.is_empty())
}

/// `is_dummy_rel` (joinrels.c:1275) — has the relation been proven empty?
///
/// A known-dummy rel has just one path that is a childless `Append` (possibly
/// under a `ProjectionPath`/`ProjectSetPath` added in later planning stages, so
/// we descend through whatever we find). Ported 1:1 over the arena `PathNode`.
pub fn is_dummy_rel(root: &PlannerInfo, rel: RelId) -> bool {
    let pathlist = &root.rel(rel).pathlist;
    if pathlist.is_empty() {
        return false;
    }
    // path = (Path *) linitial(rel->pathlist);
    let mut path = pathlist[0];

    // Descend through any ProjectionPath/ProjectSetPath on top.
    loop {
        match root.path(path) {
            PathNode::ProjectionPath(pp) => match pp.subpath {
                Some(sp) => path = sp,
                None => break,
            },
            PathNode::ProjectSetPath(psp) => match psp.subpath {
                Some(sp) => path = sp,
                None => break,
            },
            _ => break,
        }
    }

    is_dummy_append(root.path(path))
}

/* ==========================================================================
 * mark_dummy_rel (joinrels.c:1324)
 * ======================================================================== */

/// `mark_dummy_rel` (joinrels.c:1324) — mark a relation as proven empty.
///
/// The "evict paths, make a childless dummy `Append`, recost" body
/// (`create_append_path`/`add_path`/`set_cheapest`, plus the
/// `MemoryContextSwitchTo(GetMemoryChunkContext(rel))` dance) is a pathnode-crate
/// operation over the arena rel, bundled behind the `install_dummy_append_path`
/// seam. The already-marked early-out is ported in-crate.
pub fn mark_dummy_rel<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    rel: RelId,
) -> PgResult<()> {
    // Already marked?
    if is_dummy_rel(root, rel) {
        return Ok(());
    }
    // No, so install the dummy childless-Append path (in the rel's own context).
    pathnode::install_dummy_append_path::call(root, run, rel)
}

/* ==========================================================================
 * restriction_is_constant_false (joinrels.c:1367)
 * ======================================================================== */

/// `RINFO_IS_PUSHED_DOWN(rinfo, relids)` (pathnodes.h).
fn rinfo_is_pushed_down(root: &PlannerInfo, rinfo: RinfoId, relids: &Relids) -> bool {
    let ri = root.rinfo(rinfo);
    ri.is_pushed_down || !bms::relids_is_subset::call(&ri.required_relids, relids)
}

/// `restriction_is_constant_false` (joinrels.c:1367) — is a restrictlist just
/// FALSE? Ported 1:1. The per-`RestrictInfo` `clause` reads
/// (`IsA(clause, Const)` / `con->constisnull` / `DatumGetBool(con->constvalue)`)
/// are pure struct reads over the canonical `Expr` node arena: `ri.clause` is a
/// `NodeId`, `root.node(clause)` recovers the `&Expr`, and a [`Expr::Const`]
/// match yields the `Const` whose `constisnull`/`constvalue` fields are read
/// directly (`Datum::as_bool` is `DatumGetBool`). No fmgr/catalog is involved,
/// so no seam is needed.
fn restriction_is_constant_false(
    root: &PlannerInfo,
    restrictlist: &[RinfoId],
    joinrel: RelId,
    only_pushed_down: bool,
) -> bool {
    let joinrel_relids = root.rel(joinrel).relids.clone();
    for &rinfo in restrictlist {
        if only_pushed_down && !rinfo_is_pushed_down(root, rinfo, &joinrel_relids) {
            continue;
        }

        // Reject if it's not a "constant" RestrictInfo. (Note: it is presently
        // impossible for this to be true, because we restrict the matched
        // clause to be a pseudoconstant; see the C source. We still test the
        // Const-ness exactly as C does via IsA(rinfo->clause, Const).)
        let clause_id = root.rinfo(rinfo).clause;
        if let nodes::primnodes::Expr::Const(con) = root.node(clause_id) {
            // constant NULL is as good as constant FALSE for our purposes.
            if con.constisnull {
                return true;
            }
            if !con.constvalue.as_bool() {
                return true;
            }
        }
    }
    false
}

/* ==========================================================================
 * join_search_one_level (joinrels.c:72)
 * ======================================================================== */

/// `join_search_one_level` (joinrels.c:72) — consider all ways to produce join
/// relations containing exactly `level` jointree items.
pub fn join_search_one_level<'mcx>(mcx: Mcx<'mcx>, root: &mut PlannerInfo, run: &PlannerRun<'mcx>, level: i32) -> PgResult<()> {
    debug_assert!(root.join_rel_level[level as usize].is_empty());

    // Set join_cur_level so that new joinrels are added to the proper list.
    root.join_cur_level = level;

    // First, consider left-sided and right-sided plans: rels of exactly level-1
    // member relations joined against initial relations.
    let prev_level = root.join_rel_level[(level - 1) as usize].clone();
    for (idx, &old_rel) in prev_level.iter().enumerate() {
        let or = root.rel(old_rel);
        if !or.joininfo.is_empty() || or.has_eclass_joins || has_join_restriction(root, old_rel) {
            // There are join clauses or join order restrictions relevant to this
            // rel: consider joins between it and (only) those initial rels it is
            // linked to by a clause or restriction.
            let first_rel: i32 = if level == 2 {
                // consider remaining initial rels (symmetric at level 2)
                (idx as i32) + 1
            } else {
                0
            };
            let level1 = root.join_rel_level[1].clone();
            make_rels_by_clause_joins(mcx, root, run, old_rel, &level1, first_rel)?;
        } else {
            // A relation not joined to any other relation directly or by
            // join-order restrictions. Cartesian product time.
            let level1 = root.join_rel_level[1].clone();
            make_rels_by_clauseless_joins(mcx, root, run, old_rel, &level1)?;
        }
    }

    // Now consider "bushy plans": relations of k initial rels joined to
    // relations of level-k initial rels, for 2 <= k <= level-2.
    let mut k = 2;
    loop {
        let other_level = level - k;

        // make_join_rel(x, y) handles both x,y and y,x, so only go to halfway.
        if k > other_level {
            break;
        }

        let level_k = root.join_rel_level[k as usize].clone();
        for (idx, &old_rel) in level_k.iter().enumerate() {
            // Ignore relations without join clauses unless they participate in
            // join-order restrictions (then we might have to force a bushy plan).
            let or = root.rel(old_rel);
            if or.joininfo.is_empty() && !or.has_eclass_joins && !has_join_restriction(root, old_rel)
            {
                continue;
            }

            let first_rel: i32 = if k == other_level {
                // only consider remaining rels
                (idx as i32) + 1
            } else {
                0
            };

            let other = root.join_rel_level[other_level as usize].clone();
            for &new_rel in other.iter().skip(first_rel as usize) {
                if !bms::relids_overlap::call(&root.rel(old_rel).relids, &root.rel(new_rel).relids) {
                    // We can build a rel of the right level from this pair. Do so
                    // if there is at least one relevant join clause or restriction.
                    if geqo::have_relevant_joinclause::call(root, old_rel, new_rel)
                        || have_join_order_restriction(run, root, old_rel, new_rel)
                    {
                        make_join_rel(mcx, root, run, old_rel, new_rel)?;
                    }
                }
            }
        }

        k += 1;
    }

    // Last-ditch effort: if we failed to find any usable joins so far, force a
    // set of cartesian-product joins to be generated.
    if root.join_rel_level[level as usize].is_empty() {
        // Just like the first loop, except always clauseless.
        let prev_level = root.join_rel_level[(level - 1) as usize].clone();
        for &old_rel in &prev_level {
            let level1 = root.join_rel_level[1].clone();
            make_rels_by_clauseless_joins(mcx, root, run, old_rel, &level1)?;
        }

        // When special joins are involved there may be no legal N-way join for
        // some N. But with no special joins and no lateral references,
        // join_is_legal() should never fail, so the sanity check is useful.
        if root.join_rel_level[level as usize].is_empty()
            && root.join_info_list.is_empty()
            && !root.hasLateralRTEs
        {
            return Err(PgError::error(alloc::format!(
                "failed to build any {level}-way joins"
            )));
        }
    }

    Ok(())
}

/* ==========================================================================
 * make_rels_by_clause_joins (joinrels.c:279)
 * ======================================================================== */

/// `make_rels_by_clause_joins` (joinrels.c:279) — build joins between `old_rel`
/// and other relations that participate in join clauses (or join-order
/// restrictions) `old_rel` also participates in.
fn make_rels_by_clause_joins<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    old_rel: RelId,
    other_rels: &[RelId],
    first_rel_idx: i32,
) -> PgResult<()> {
    for &other_rel in other_rels.iter().skip(first_rel_idx as usize) {
        if !bms::relids_overlap::call(&root.rel(old_rel).relids, &root.rel(other_rel).relids)
            && (geqo::have_relevant_joinclause::call(root, old_rel, other_rel)
                || have_join_order_restriction(run, root, old_rel, other_rel))
        {
            make_join_rel(mcx, root, run, old_rel, other_rel)?;
        }
    }
    Ok(())
}

/* ==========================================================================
 * make_rels_by_clauseless_joins (joinrels.c:313)
 * ======================================================================== */

/// `make_rels_by_clauseless_joins` (joinrels.c:313) — create a join relation
/// between `old_rel` and each member of `other_rels` not already in it.
fn make_rels_by_clauseless_joins<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    old_rel: RelId,
    other_rels: &[RelId],
) -> PgResult<()> {
    for &other_rel in other_rels {
        if !bms::relids_overlap::call(&root.rel(other_rel).relids, &root.rel(old_rel).relids) {
            make_join_rel(mcx, root, run, old_rel, other_rel)?;
        }
    }
    Ok(())
}

/* ==========================================================================
 * join_is_legal (joinrels.c:349)
 * ======================================================================== */

/// Result of [`join_is_legal`]: `Some((sjinfo_index, reversed))` for a legal
/// join, `None` for an illegal one. `sjinfo_index` is the index into
/// `root.join_info_list` of the matched `SpecialJoinInfo`, or `None` for a plain
/// inner join (the C `*sjinfo_p == NULL` case).
type JoinLegality = Option<(Option<usize>, bool)>;

/// `join_is_legal` (joinrels.c:349) — determine whether a proposed join is legal
/// given the query's join-order constraints, and if so its join type.
///
/// Caller supplies the two rels plus the union of their relids (`joinrelids`).
/// Returns `None` on failure; `Some((match_index, reversed))` on success.
fn join_is_legal<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rel1: RelId,
    rel2: RelId,
    joinrelids: &Relids,
) -> JoinLegality {
    let mut match_sjinfo: Option<usize> = None;
    let mut reversed = false;
    let mut unique_ified = false;
    let mut must_be_leftjoin = false;

    // Scan the join info list for matches and conflicts.
    let n_sj = root.join_info_list.len();
    for i in 0..n_sj {
        // Snapshot the per-SJ relids/jointype this iteration reads. We must
        // release the `&root.join_info_list[i]` borrow before any
        // `can_create_unique_path` call (which needs `&mut root` to cache the
        // UniquePath on the rel, exactly as C `create_unique_path` does); the SJ
        // is identified by index `i` for the matched-SJ accessors afterward.
        let sj = &root.join_info_list[i];
        let sj_min_righthand = sj.min_righthand.clone();
        let sj_min_lefthand = sj.min_lefthand.clone();
        let sj_syn_righthand = sj.syn_righthand.clone();
        let sj_jointype = sj.jointype;

        // Not relevant unless its RHS overlaps the proposed join.
        if !bms::relids_overlap::call(&sj_min_righthand, joinrelids) {
            continue;
        }

        // Not relevant if proposed join is fully contained within RHS.
        if bms::relids_is_subset::call(joinrelids, &sj_min_righthand) {
            continue;
        }

        let rel1_relids = root.rel(rel1).relids.clone();
        let rel2_relids = root.rel(rel2).relids.clone();

        // Not relevant if SJ is already done within either input.
        if bms::relids_is_subset::call(&sj_min_lefthand, &rel1_relids)
            && bms::relids_is_subset::call(&sj_min_righthand, &rel1_relids)
        {
            continue;
        }
        if bms::relids_is_subset::call(&sj_min_lefthand, &rel2_relids)
            && bms::relids_is_subset::call(&sj_min_righthand, &rel2_relids)
        {
            continue;
        }

        // If it's a semijoin and we already joined the RHS to any other rels
        // within either input, then the semijoin is no longer relevant.
        if sj_jointype == JOIN_SEMI {
            if bms::relids_is_subset::call(&sj_syn_righthand, &rel1_relids)
                && !pathnode::relids_equal::call(&sj_syn_righthand, &rel1_relids)
            {
                continue;
            }
            if bms::relids_is_subset::call(&sj_syn_righthand, &rel2_relids)
                && !pathnode::relids_equal::call(&sj_syn_righthand, &rel2_relids)
            {
                continue;
            }
        }

        // If one input contains min_lefthand and the other min_righthand, then we
        // can perform the SJ at this join. Reject matches to more than one SJ.
        if bms::relids_is_subset::call(&sj_min_lefthand, &rel1_relids)
            && bms::relids_is_subset::call(&sj_min_righthand, &rel2_relids)
        {
            if match_sjinfo.is_some() {
                return None; // invalid join path
            }
            match_sjinfo = Some(i);
            reversed = false;
        } else if bms::relids_is_subset::call(&sj_min_lefthand, &rel2_relids)
            && bms::relids_is_subset::call(&sj_min_righthand, &rel1_relids)
        {
            if match_sjinfo.is_some() {
                return None; // invalid join path
            }
            match_sjinfo = Some(i);
            reversed = true;
        } else if sj_jointype == JOIN_SEMI
            && pathnode::relids_equal::call(&sj_syn_righthand, &rel2_relids)
            && {
                let sj_owned = root.join_info_list[i].clone();
                pathnode::can_create_unique_path::call(run, root, rel2, &sj_owned)
            }
        {
            // For a semijoin, we can join the RHS to anything else by
            // unique-ifying the RHS (if the RHS can be unique-ified).
            if match_sjinfo.is_some() {
                return None; // invalid join path
            }
            match_sjinfo = Some(i);
            reversed = false;
            unique_ified = true;
        } else if sj_jointype == JOIN_SEMI
            && pathnode::relids_equal::call(&sj_syn_righthand, &rel1_relids)
            && {
                let sj_owned = root.join_info_list[i].clone();
                pathnode::can_create_unique_path::call(run, root, rel1, &sj_owned)
            }
        {
            // Reversed semijoin case.
            if match_sjinfo.is_some() {
                return None; // invalid join path
            }
            match_sjinfo = Some(i);
            reversed = true;
            unique_ified = true;
        } else {
            // Otherwise, the proposed join overlaps the RHS but isn't a valid
            // implementation of this SJ. If both inputs overlap the RHS, allow it.
            if bms::relids_overlap::call(&rel1_relids, &sj_min_righthand)
                && bms::relids_overlap::call(&rel2_relids, &sj_min_righthand)
            {
                continue; // assume valid previous violation of RHS
            }

            // The proposed join could still be legal, but only if we can
            // associate it into the RHS of this SJ: it must be a LEFT join and
            // not overlap the LHS.
            if sj_jointype != JOIN_LEFT || bms::relids_overlap::call(joinrelids, &sj_min_lefthand) {
                return None; // invalid join path
            }

            // Remember the requirement for later.
            must_be_leftjoin = true;
        }
    }

    // Fail if we violated any SJ's RHS and didn't match to a LEFT SJ; also fail
    // if the matched join's predicate isn't strict.
    if must_be_leftjoin {
        let bad = match match_sjinfo {
            None => true,
            Some(i) => {
                let m = &root.join_info_list[i];
                m.jointype != JOIN_LEFT || !m.lhs_strict
            }
        };
        if bad {
            return None; // invalid join path
        }
    }

    // We also have to check for constraints imposed by LATERAL references.
    if root.hasLateralRTEs {
        let rel1_relids = root.rel(rel1).relids.clone();
        let rel2_relids = root.rel(rel2).relids.clone();
        let rel1_lateral = root.rel(rel1).lateral_relids.clone();
        let rel2_lateral = root.rel(rel2).lateral_relids.clone();
        let rel1_direct = root.rel(rel1).direct_lateral_relids.clone();
        let rel2_direct = root.rel(rel2).direct_lateral_relids.clone();

        let lateral_fwd = bms::relids_overlap::call(&rel1_relids, &rel2_lateral);
        let lateral_rev = bms::relids_overlap::call(&rel2_relids, &rel1_lateral);
        if lateral_fwd && lateral_rev {
            return None; // have lateral refs in both directions
        }
        if lateral_fwd {
            // has to be implemented as nestloop with rel1 on left
            if let Some(i) = match_sjinfo {
                let m = &root.join_info_list[i];
                if reversed || unique_ified || m.jointype == JOIN_FULL {
                    return None; // not implementable as nestloop
                }
            }
            // check there is a direct reference from rel2 to rel1
            if !bms::relids_overlap::call(&rel1_relids, &rel2_direct) {
                return None; // only indirect refs, so reject
            }
        } else if lateral_rev {
            // has to be implemented as nestloop with rel2 on left
            if let Some(i) = match_sjinfo {
                let m = &root.join_info_list[i];
                if !reversed || unique_ified || m.jointype == JOIN_FULL {
                    return None; // not implementable as nestloop
                }
            }
            // check there is a direct reference from rel1 to rel2
            if !bms::relids_overlap::call(&rel2_relids, &rel1_direct) {
                return None; // only indirect refs, so reject
            }
        }

        // LATERAL references could also cause problems later: compute the minimum
        // parameterization and check it can be honored.
        let join_lateral_rels =
            bms::min_join_parameterization::call(root, joinrelids, rel1, rel2);
        if !bms::relids_is_empty::call(&join_lateral_rels) {
            let mut join_plus_rhs = bms::relids_copy::call(joinrelids);
            let mut more = true;
            while more {
                more = false;
                for i in 0..root.join_info_list.len() {
                    let sj = &root.join_info_list[i];
                    // ignore full joins --- their ordering is predetermined
                    if sj.jointype == JOIN_FULL {
                        continue;
                    }
                    if bms::relids_overlap::call(&sj.min_lefthand, &join_plus_rhs)
                        && !bms::relids_is_subset::call(&sj.min_righthand, &join_plus_rhs)
                    {
                        let min_righthand = sj.min_righthand.clone();
                        join_plus_rhs = bms::relids_add_members::call(join_plus_rhs, &min_righthand);
                        more = true;
                    }
                }
            }
            if bms::relids_overlap::call(&join_plus_rhs, &join_lateral_rels) {
                return None; // will not be able to join to some RHS rel
            }
        }
    }

    // Otherwise, it's a valid join.
    Some((match_sjinfo, reversed))
}

/* ==========================================================================
 * make_join_rel (joinrels.c:695)
 * ======================================================================== */

/// `make_join_rel` (joinrels.c:695) — find or create a join `RelOptInfo` for the
/// join of two rels and add path information. Returns the joinrel handle, or
/// `None` if the attempted join is not valid.
pub fn make_join_rel<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    mut rel1: RelId,
    mut rel2: RelId,
) -> PgResult<Option<RelId>> {
    // We should never try to join two overlapping sets of rels.
    debug_assert!(!bms::relids_overlap::call(&root.rel(rel1).relids, &root.rel(rel2).relids));

    // Construct Relids set that identifies the joinrel (without OJ as yet).
    let joinrelids = pathnode::relids_union::call(&root.rel(rel1).relids, &root.rel(rel2).relids);

    // Check validity and determine join type.
    let (match_index, reversed) = match join_is_legal(run, root, rel1, rel2, &joinrelids) {
        Some(v) => v,
        None => return Ok(None), // invalid join path
    };

    // Add outer join relid(s) to form the canonical relids. Any added outer joins
    // besides sjinfo itself are appended to pushed_down_joins.
    let mut pushed_down_joins: Vec<SpecialJoinInfo> = Vec::new();
    let match_sjinfo = match_index.map(|i| &root.join_info_list[i]);
    let joinrelids =
        add_outer_joins_to_relids(root, joinrelids, match_sjinfo, &mut pushed_down_joins)?;

    // Swap rels if needed to match the join info.
    if reversed {
        core::mem::swap(&mut rel1, &mut rel2);
    }

    // If it's a plain inner join, make up a SpecialJoinInfo so selectivity
    // estimation knows what's being joined. We resolve the matched
    // SpecialJoinInfo into a concrete owned value here.
    let sjinfo: SpecialJoinInfo = match match_index {
        Some(i) => root.join_info_list[i].clone(),
        None => {
            let mut sjinfo_data = make_dummy_sjinfo();
            let l = bms::relids_copy::call(&root.rel(rel1).relids);
            let r = bms::relids_copy::call(&root.rel(rel2).relids);
            init_dummy_sjinfo(&mut sjinfo_data, l, r);
            sjinfo_data
        }
    };

    // Find or build the join RelOptInfo, and compute the restrictlist.
    let (joinrel, restrictlist) =
        bms::build_join_rel::call(root, run, &joinrelids, rel1, rel2, &sjinfo, &pushed_down_joins)?;

    // If we've already proven this join is empty, we needn't consider more paths.
    if is_dummy_rel(root, joinrel) {
        return Ok(Some(joinrel));
    }

    // Add paths to the join relation.
    populate_joinrel_with_paths(mcx, root, run, rel1, rel2, joinrel, &sjinfo, &restrictlist)?;

    Ok(Some(joinrel))
}

/* ==========================================================================
 * add_outer_joins_to_relids (joinrels.c:792)
 * ======================================================================== */

/// `add_outer_joins_to_relids` (joinrels.c:792) — add relids representing any
/// outer joins that will be calculated at this join.
///
/// `input_relids` is modified in-place and returned (the C convention).
/// `sjinfo` is the SpecialJoinInfo for the join being performed, or `None` for
/// a plain inner join (the C `sjinfo == NULL` case). SpecialJoinInfos for added
/// pushed-down outer joins are appended to `pushed_down_joins`.
fn add_outer_joins_to_relids(
    root: &PlannerInfo,
    input_relids: Relids,
    sjinfo: Option<&SpecialJoinInfo>,
    pushed_down_joins: &mut Vec<SpecialJoinInfo>,
) -> PgResult<Relids> {
    // Nothing to do if this isn't an outer join with an assigned relid.
    let sjinfo = match sjinfo {
        Some(sj) => sj,
        None => return Ok(input_relids),
    };
    if sjinfo.ojrelid == 0 {
        return Ok(input_relids);
    }

    // If it's not a left join, just form the syntactic relid set.
    if sjinfo.jointype != JOIN_LEFT {
        return Ok(bms::relids_add_member::call(input_relids, sjinfo.ojrelid as i32));
    }

    // We cannot add the OJ relid if this join has been pushed into the RHS of a
    // syntactically-lower left join per OJ identity 3.
    if !bms::relids_is_subset::call(&sjinfo.commute_below_l, &input_relids) {
        return Ok(input_relids);
    }

    // OK to add OJ's own relid.
    let mut input_relids = bms::relids_add_member::call(input_relids, sjinfo.ojrelid as i32);

    // If we are now forming the final result of a commuted pair of OJs, it's time
    // to add the relid(s) of the pushed-down join(s).
    if !bms::relids_is_empty::call(&sjinfo.commute_above_l) {
        let mut commute_above_rels = bms::relids_copy::call(&sjinfo.commute_above_l);

        // join_info_list was built bottom-up, so a single forward traversal
        // suffices. Read the matched ojrelid up front so we don't hold a borrow
        // of `sjinfo` across the mutation of `pushed_down_joins`.
        let self_ojrelid = sjinfo.ojrelid;

        for lc in 0..root.join_info_list.len() {
            let othersj = &root.join_info_list[lc];

            if othersj.ojrelid == self_ojrelid || othersj.ojrelid == 0 || othersj.jointype != JOIN_LEFT
            {
                continue; // definitely not interesting
            }

            if !bms::relids_is_member::call(othersj.ojrelid as i32, &commute_above_rels) {
                continue;
            }

            // Add it if not already present but conditions now satisfied.
            if !bms::relids_is_member::call(othersj.ojrelid as i32, &input_relids)
                && bms::relids_is_subset::call(&othersj.min_lefthand, &input_relids)
                && bms::relids_is_subset::call(&othersj.min_righthand, &input_relids)
                && bms::relids_is_subset::call(&othersj.commute_below_l, &input_relids)
            {
                input_relids = bms::relids_add_member::call(input_relids, othersj.ojrelid as i32);
                // report such pushed down outer joins
                pushed_down_joins.try_reserve(1).map_err(oom)?;
                pushed_down_joins.push(othersj.clone());

                // We must also check any joins that othersj potentially commutes
                // with.
                let commute_above_l = othersj.commute_above_l.clone();
                commute_above_rels = bms::relids_add_members::call(commute_above_rels, &commute_above_l);
            }
        }
    }

    Ok(input_relids)
}

/* ==========================================================================
 * populate_joinrel_with_paths (joinrels.c:884)
 * ======================================================================== */

/// `populate_joinrel_with_paths` (joinrels.c:884) — add paths to the given
/// joinrel for the given pair of joining relations.
fn populate_joinrel_with_paths<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    rel1: RelId,
    rel2: RelId,
    joinrel: RelId,
    sjinfo: &SpecialJoinInfo,
    restrictlist: &[RinfoId],
) -> PgResult<()> {
    // Snapshot the join-method enable GUCs once for the add_paths_to_joinrel
    // calls below (C reads these globals directly inside add_paths_to_joinrel).
    let enable = join_enable_flags();

    // We need only consider the jointypes that appear in join_info_list, plus
    // JOIN_INNER.
    match sjinfo.jointype {
        JOIN_INNER => {
            if is_dummy_rel(root, rel1)
                || is_dummy_rel(root, rel2)
                || restriction_is_constant_false(root, restrictlist, joinrel, false)
            {
                mark_dummy_rel(root, run, joinrel)?;
            } else {
                add_paths_to_joinrel(mcx, root, run, joinrel, rel1, rel2, JOIN_INNER, sjinfo, restrictlist, enable)?;
                add_paths_to_joinrel(mcx, root, run, joinrel, rel2, rel1, JOIN_INNER, sjinfo, restrictlist, enable)?;
            }
        }
        JOIN_LEFT => {
            if is_dummy_rel(root, rel1)
                || restriction_is_constant_false(root, restrictlist, joinrel, true)
            {
                mark_dummy_rel(root, run, joinrel)?;
            } else {
                if restriction_is_constant_false(root, restrictlist, joinrel, false)
                    && bms::relids_is_subset::call(&root.rel(rel2).relids, &sjinfo.syn_righthand)
                {
                    mark_dummy_rel(root, run, rel2)?;
                }
                add_paths_to_joinrel(mcx, root, run, joinrel, rel1, rel2, JOIN_LEFT, sjinfo, restrictlist, enable)?;
                add_paths_to_joinrel(mcx, root, run, joinrel, rel2, rel1, JOIN_RIGHT, sjinfo, restrictlist, enable)?;
            }
        }
        JOIN_FULL => {
            if (is_dummy_rel(root, rel1) && is_dummy_rel(root, rel2))
                || restriction_is_constant_false(root, restrictlist, joinrel, true)
            {
                mark_dummy_rel(root, run, joinrel)?;
            } else {
                add_paths_to_joinrel(mcx, root, run, joinrel, rel1, rel2, JOIN_FULL, sjinfo, restrictlist, enable)?;
                add_paths_to_joinrel(mcx, root, run, joinrel, rel2, rel1, JOIN_FULL, sjinfo, restrictlist, enable)?;

                // If there are join quals that aren't mergeable or hashable, we
                // may not be able to build any valid plan.
                if root.rel(joinrel).pathlist.is_empty() {
                    return Err(PgError::error(
                        "FULL JOIN is only supported with merge-joinable or hash-joinable join conditions",
                    )
                    .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED));
                }
            }
        }
        JOIN_SEMI => {
            // We might have a normal semijoin, or a case where we don't have
            // enough rels to do the semijoin but can unique-ify the RHS and then
            // do an innerjoin. In the latter case we can't apply JOIN_SEMI.
            if bms::relids_is_subset::call(&sjinfo.min_lefthand, &root.rel(rel1).relids)
                && bms::relids_is_subset::call(&sjinfo.min_righthand, &root.rel(rel2).relids)
            {
                if is_dummy_rel(root, rel1)
                    || is_dummy_rel(root, rel2)
                    || restriction_is_constant_false(root, restrictlist, joinrel, false)
                {
                    mark_dummy_rel(root, run, joinrel)?;
                    // C `break`s out of the switch case after mark_dummy_rel.
                    try_partitionwise_join(mcx, root, run, rel1, rel2, joinrel, sjinfo, restrictlist)?;
                    return Ok(());
                }
                add_paths_to_joinrel(mcx, root, run, joinrel, rel1, rel2, JOIN_SEMI, sjinfo, restrictlist, enable)?;
                add_paths_to_joinrel(mcx, root, run, joinrel, rel2, rel1, JOIN_RIGHT_SEMI, sjinfo, restrictlist, enable)?;
            }

            // If we know how to unique-ify the RHS and one input rel is exactly
            // the RHS we can consider unique-ifying it and doing a regular join.
            if pathnode::relids_equal::call(&sjinfo.syn_righthand, &root.rel(rel2).relids.clone())
                && pathnode::can_create_unique_path::call(run, root, rel2, sjinfo)
            {
                if is_dummy_rel(root, rel1)
                    || is_dummy_rel(root, rel2)
                    || restriction_is_constant_false(root, restrictlist, joinrel, false)
                {
                    mark_dummy_rel(root, run, joinrel)?;
                    try_partitionwise_join(mcx, root, run, rel1, rel2, joinrel, sjinfo, restrictlist)?;
                    return Ok(());
                }
                add_paths_to_joinrel(mcx, root, run, joinrel, rel1, rel2, JOIN_UNIQUE_INNER, sjinfo, restrictlist, enable)?;
                add_paths_to_joinrel(mcx, root, run, joinrel, rel2, rel1, JOIN_UNIQUE_OUTER, sjinfo, restrictlist, enable)?;
            }
        }
        JOIN_ANTI => {
            if is_dummy_rel(root, rel1)
                || restriction_is_constant_false(root, restrictlist, joinrel, true)
            {
                mark_dummy_rel(root, run, joinrel)?;
            } else {
                if restriction_is_constant_false(root, restrictlist, joinrel, false)
                    && bms::relids_is_subset::call(&root.rel(rel2).relids, &sjinfo.syn_righthand)
                {
                    mark_dummy_rel(root, run, rel2)?;
                }
                add_paths_to_joinrel(mcx, root, run, joinrel, rel1, rel2, JOIN_ANTI, sjinfo, restrictlist, enable)?;
                add_paths_to_joinrel(mcx, root, run, joinrel, rel2, rel1, JOIN_RIGHT_ANTI, sjinfo, restrictlist, enable)?;
            }
        }
        other => {
            // other values not expected here
            return Err(PgError::error(alloc::format!("unrecognized join type: {other}")));
        }
    }

    // Apply partitionwise join technique, if possible.
    try_partitionwise_join(mcx, root, run, rel1, rel2, joinrel, sjinfo, restrictlist)?;
    Ok(())
}

/* ==========================================================================
 * have_join_order_restriction (joinrels.c:1065)
 * ======================================================================== */

/// `have_join_order_restriction` (joinrels.c:1065) — detect whether the two
/// relations should be joined to satisfy a join-order restriction arising from
/// special or lateral joins.
pub fn have_join_order_restriction<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rel1: RelId,
    rel2: RelId,
) -> bool {
    let mut result = false;

    let rel1_relids = root.rel(rel1).relids.clone();
    let rel2_relids = root.rel(rel2).relids.clone();
    let rel1_direct = root.rel(rel1).direct_lateral_relids.clone();
    let rel2_direct = root.rel(rel2).direct_lateral_relids.clone();

    // If either side has a direct lateral reference to the other, attempt the
    // join regardless of outer-join considerations.
    if bms::relids_overlap::call(&rel1_relids, &rel2_direct)
        || bms::relids_overlap::call(&rel2_relids, &rel1_direct)
    {
        return true;
    }

    // Likewise, if both rels are needed to compute some PlaceHolderVar, attempt
    // the join regardless of outer-join considerations.
    for &phid in &root.placeholder_list {
        let ph_eval_at = &root.phinfo(phid).ph_eval_at;
        if bms::relids_is_subset::call(&rel1_relids, ph_eval_at)
            && bms::relids_is_subset::call(&rel2_relids, ph_eval_at)
        {
            return true;
        }
    }

    // The rels could correspond to the left and right sides of a degenerate
    // outer join, in which case we should force the join to occur.
    for sjinfo in &root.join_info_list {
        // ignore full joins --- other mechanisms handle them
        if sjinfo.jointype == JOIN_FULL {
            continue;
        }

        // Can we perform the SJ with these rels?
        if bms::relids_is_subset::call(&sjinfo.min_lefthand, &rel1_relids)
            && bms::relids_is_subset::call(&sjinfo.min_righthand, &rel2_relids)
        {
            result = true;
            break;
        }
        if bms::relids_is_subset::call(&sjinfo.min_lefthand, &rel2_relids)
            && bms::relids_is_subset::call(&sjinfo.min_righthand, &rel1_relids)
        {
            result = true;
            break;
        }

        // Might we need to join these rels to complete the RHS?
        if bms::relids_overlap::call(&sjinfo.min_righthand, &rel1_relids)
            && bms::relids_overlap::call(&sjinfo.min_righthand, &rel2_relids)
        {
            result = true;
            break;
        }

        // Likewise for the LHS.
        if bms::relids_overlap::call(&sjinfo.min_lefthand, &rel1_relids)
            && bms::relids_overlap::call(&sjinfo.min_lefthand, &rel2_relids)
        {
            result = true;
            break;
        }
    }

    // We do not force the join if either input rel can legally be joined to
    // anything else using joinclauses.
    if result && (has_legal_joinclause(run, root, rel1) || has_legal_joinclause(run, root, rel2)) {
        result = false;
    }

    result
}

/* ==========================================================================
 * has_join_restriction (joinrels.c:1178)
 * ======================================================================== */

/// `has_join_restriction` (joinrels.c:1178) — detect whether the specified
/// relation has join-order restrictions.
fn has_join_restriction(root: &PlannerInfo, rel: RelId) -> bool {
    let r = root.rel(rel);
    if r.lateral_relids.is_some() || r.lateral_referencers.is_some() {
        return true;
    }
    let rel_relids = &r.relids;

    for &phid in &root.placeholder_list {
        let ph_eval_at = &root.phinfo(phid).ph_eval_at;
        if bms::relids_is_subset::call(rel_relids, ph_eval_at)
            && !pathnode::relids_equal::call(rel_relids, ph_eval_at)
        {
            return true;
        }
    }

    for sjinfo in &root.join_info_list {
        // ignore full joins --- other mechanisms preserve their ordering
        if sjinfo.jointype == JOIN_FULL {
            continue;
        }

        // ignore if SJ is already contained in rel
        if bms::relids_is_subset::call(&sjinfo.min_lefthand, rel_relids)
            && bms::relids_is_subset::call(&sjinfo.min_righthand, rel_relids)
        {
            continue;
        }

        // restricted if it overlaps LHS or RHS, but doesn't contain SJ
        if bms::relids_overlap::call(&sjinfo.min_lefthand, rel_relids)
            || bms::relids_overlap::call(&sjinfo.min_righthand, rel_relids)
        {
            return true;
        }
    }

    false
}

/* ==========================================================================
 * has_legal_joinclause (joinrels.c:1234)
 * ======================================================================== */

/// `has_legal_joinclause` (joinrels.c:1234) — detect whether the specified
/// relation can legally be joined to any other rels using join clauses.
fn has_legal_joinclause<'mcx>(run: &PlannerRun<'mcx>, root: &mut PlannerInfo, rel: RelId) -> bool {
    // Clone the initial-rels snapshot so the loop body can take `&mut root` for
    // `join_is_legal` (which now caches a UniquePath via create_unique_path).
    let initial_rels = root.initial_rels.clone();
    for rel2 in initial_rels {
        // ignore rels that are already in "rel"
        if bms::relids_overlap::call(&root.rel(rel).relids, &root.rel(rel2).relids) {
            continue;
        }

        if geqo::have_relevant_joinclause::call(root, rel, rel2) {
            // join_is_legal needs relids of the union
            let joinrelids = pathnode::relids_union::call(&root.rel(rel).relids, &root.rel(rel2).relids);

            if join_is_legal(run, root, rel, rel2, &joinrelids).is_some() {
                // Yes, this will work
                return true;
            }
        }
    }

    false
}

/* ==========================================================================
 * try_partitionwise_join (joinrels.c:1421)
 * ======================================================================== */

/// `rel->part_scheme->partnatts` (or `-1` when there is no scheme) — the
/// structural witness used by the debug-only partition-scheme-identity check
/// (the C pointer-equality `Assert` over canonicalized schemes).
#[inline]
fn scheme_partnatts(rel: &pathnodes::RelOptInfo) -> i16 {
    match rel.part_scheme.as_ref() {
        Some(s) => s.partnatts,
        None => -1,
    }
}

/// `try_partitionwise_join` (joinrels.c:1421) — assess whether a join between two
/// partitioned relations can be broken into joins between matching partitions,
/// and if so create the child-join `RelOptInfo`s and add paths to them.
///
/// Ported 1:1 over the arena. A no-op for a non-partitioned joinrel. Called
/// directly from `populate_joinrel_with_paths`, exactly as in C.
fn try_partitionwise_join<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    rel1: RelId,
    rel2: RelId,
    joinrel: RelId,
    parent_sjinfo: &SpecialJoinInfo,
    parent_restrictlist: &[RinfoId],
) -> PgResult<()> {
    let rel1_is_simple = is_simple_rel(root.rel(rel1));
    let rel2_is_simple = is_simple_rel(root.rel(rel2));

    // Guard against stack overflow due to overly deep partition hierarchy.
    tcop::check_stack_depth::call()?;

    // Nothing to do, if the join relation is not partitioned.
    if root.rel(joinrel).part_scheme.is_none() || root.rel(joinrel).nparts == 0 {
        return Ok(());
    }

    // The join relation should have consider_partitionwise_join set.
    debug_assert!(root.rel(joinrel).consider_partitionwise_join);

    // We can not perform partitionwise join if either of the joining relations is
    // not partitioned. `IS_PARTITIONED_REL(rel)` is the field-only predicate AND
    // `!IS_DUMMY_REL(rel)`; the dummy-rel conjunct is applied here at the callsite.
    if !(is_partitioned_rel(root.rel(rel1)) && !is_dummy_rel(root, rel1))
        || !(is_partitioned_rel(root.rel(rel2)) && !is_dummy_rel(root, rel2))
    {
        return Ok(());
    }

    debug_assert!(rel_has_all_part_props(root.rel(rel1)) && rel_has_all_part_props(root.rel(rel2)));

    // The joining relations should have consider_partitionwise_join set.
    debug_assert!(
        root.rel(rel1).consider_partitionwise_join && root.rel(rel2).consider_partitionwise_join
    );

    // The partition scheme of the join relation should match that of the joining
    // relations. C compares `PartitionScheme` pointers (schemes canonicalized
    // into `root.part_schemes`); the keystone `PartitionSchemeData` has no
    // `PartialEq`, so this debug-only check uses `partnatts` as a witness.
    debug_assert!(
        scheme_partnatts(root.rel(joinrel)) == scheme_partnatts(root.rel(rel1))
            && scheme_partnatts(root.rel(joinrel)) == scheme_partnatts(root.rel(rel2))
    );

    debug_assert!(!(root.rel(joinrel).partbounds_merged && root.rel(joinrel).nparts <= 0));

    let (parts1, parts2) = compute_partition_bounds(root, rel1, rel2, joinrel, parent_sjinfo)?;

    // After compute_partition_bounds, nparts may have been set to 0 (merge failed).
    if root.rel(joinrel).nparts == 0 {
        return Ok(());
    }

    // When the bounds were merged, parts1/parts2 hold the per-segment pairings;
    // we walk them by loop index. When not merged, the pairing is by cardinal
    // position into the inputs' part_rels arrays.
    let partbounds_merged = root.rel(joinrel).partbounds_merged;

    // Create child-join relations for this partitioned join, if those don't
    // exist; add paths to child-joins for a pair of child relations.
    let nparts = root.rel(joinrel).nparts;
    for cnt_parts in 0..nparts as usize {
        let child_rel1: Option<RelId>;
        let child_rel2: Option<RelId>;

        if partbounds_merged {
            child_rel1 = parts1[cnt_parts];
            child_rel2 = parts2[cnt_parts];
        } else {
            child_rel1 = root.rel(rel1).part_rels[cnt_parts];
            child_rel2 = root.rel(rel2).part_rels[cnt_parts];
        }

        let rel1_empty = child_rel1.is_none() || is_dummy_rel(root, child_rel1.unwrap());
        let rel2_empty = child_rel2.is_none() || is_dummy_rel(root, child_rel2.unwrap());

        // Check for cases where we can prove that this segment returns no rows,
        // due to one or both inputs being empty. If so, ignore it. These rules
        // are equivalent to populate_joinrel_with_paths's dummy-input rules.
        match parent_sjinfo.jointype {
            JOIN_INNER | JOIN_SEMI => {
                if rel1_empty || rel2_empty {
                    continue; // ignore this join segment
                }
            }
            JOIN_LEFT | JOIN_ANTI => {
                if rel1_empty {
                    continue; // ignore this join segment
                }
            }
            JOIN_FULL => {
                if rel1_empty && rel2_empty {
                    continue; // ignore this join segment
                }
            }
            other => {
                // other values not expected here
                return Err(PgError::error(alloc::format!("unrecognized join type: {other}")));
            }
        }

        // If a child has been pruned entirely then we can't generate paths for
        // it, so we have to reject partitionwise joining unless we were able to
        // eliminate this partition above.
        if child_rel1.is_none() || child_rel2.is_none() {
            // Mark the joinrel as unpartitioned so later functions treat it right.
            root.rel_mut(joinrel).nparts = 0;
            return Ok(());
        }
        let child_rel1 = child_rel1.unwrap();
        let child_rel2 = child_rel2.unwrap();

        // If a leaf relation has consider_partitionwise_join=false, it's a dummy
        // relation for which we skipped setting up tlist expressions and adding EC
        // members in set_append_rel_size(), so again we have to fail here.
        if rel1_is_simple && !root.rel(child_rel1).consider_partitionwise_join {
            debug_assert!(root.rel(child_rel1).reloptkind == RELOPT_OTHER_MEMBER_REL);
            debug_assert!(is_dummy_rel(root, child_rel1));
            root.rel_mut(joinrel).nparts = 0;
            return Ok(());
        }
        if rel2_is_simple && !root.rel(child_rel2).consider_partitionwise_join {
            debug_assert!(root.rel(child_rel2).reloptkind == RELOPT_OTHER_MEMBER_REL);
            debug_assert!(is_dummy_rel(root, child_rel2));
            root.rel_mut(joinrel).nparts = 0;
            return Ok(());
        }

        // We should never try to join two overlapping sets of rels.
        debug_assert!(!bms::relids_overlap::call(
            &root.rel(child_rel1).relids,
            &root.rel(child_rel2).relids
        ));

        // Construct SpecialJoinInfo from parent join relations's SpecialJoinInfo.
        let child_rel1_relids = root.rel(child_rel1).relids.clone();
        let child_rel2_relids = root.rel(child_rel2).relids.clone();
        let child_sjinfo =
            build_child_join_sjinfo(root, parent_sjinfo, &child_rel1_relids, &child_rel2_relids)?;

        // Find the AppendRelInfo structures.
        let child_relids = pathnode::relids_union::call(&child_rel1_relids, &child_rel2_relids);
        let appinfos = appendinfo::find_appinfos_by_relids::call(root, &child_relids)?;

        // Construct restrictions applicable to the child join from those for the
        // parent join.
        let child_restrictlist =
            appendinfo::adjust_appendrel_attrs_restrictlist::call(mcx, root, parent_restrictlist, &appinfos)?;

        // Find or construct the child join's RelOptInfo.
        let mut child_joinrel = root.rel(joinrel).part_rels[cnt_parts];
        if child_joinrel.is_none() {
            let built = bms::build_child_join_rel::call(
                run,
                root,
                child_rel1,
                child_rel2,
                joinrel,
                &child_restrictlist,
                &child_sjinfo,
                &appinfos,
            )?;
            root.rel_mut(joinrel).part_rels[cnt_parts] = Some(built);
            let lp = root.rel_mut(joinrel).live_parts.take();
            root.rel_mut(joinrel).live_parts = bms::relids_add_member::call(lp, cnt_parts as i32);
            let built_relids = root.rel(built).relids.clone();
            let ap = root.rel_mut(joinrel).all_partrels.take();
            root.rel_mut(joinrel).all_partrels = bms::relids_add_members::call(ap, &built_relids);
            child_joinrel = Some(built);
        }
        let child_joinrel = child_joinrel.unwrap();

        // Assert we got the right one.
        debug_assert!(pathnode::relids_equal::call(
            &root.rel(child_joinrel).relids,
            &appendinfo::adjust_child_relids::call(&root.rel(joinrel).relids.clone(), &appinfos)
        ));

        // And make paths for the child join.
        populate_joinrel_with_paths(
            mcx,
            root,
            run,
            child_rel1,
            child_rel2,
            child_joinrel,
            &child_sjinfo,
            &child_restrictlist,
        )?;

        // Free these local objects eagerly at the end of each iteration (the
        // owned-tree analogue drops `appinfos`/`child_relids` at end of scope;
        // free_child_join_sjinfo runs the parent-vs-child identity asserts for
        // the non-inner case before the child_sjinfo value is dropped).
        free_child_join_sjinfo(&child_sjinfo, parent_sjinfo);
        drop(appinfos);
        drop(child_relids);
    }

    Ok(())
}

/* ==========================================================================
 * build_child_join_sjinfo (joinrels.c:1643)
 * ======================================================================== */

/// `build_child_join_sjinfo(root, parent_sjinfo, left_relids, right_relids)`
/// (joinrels.c:1643) — construct the `SpecialJoinInfo` for a child-join by
/// translating the parent join's.
///
/// The fabled `SpecialJoinInfo` carries no `semi_rhs_exprs`, so the C
/// `adjust_appendrel_attrs` translation of that field is not applicable (the
/// other translated fields are the min/syn lefthand/righthand relid sets).
fn build_child_join_sjinfo(
    root: &mut PlannerInfo,
    parent_sjinfo: &SpecialJoinInfo,
    left_relids: &Relids,
    right_relids: &Relids,
) -> PgResult<SpecialJoinInfo> {
    // Dummy SpecialJoinInfos can be created without any translation.
    if parent_sjinfo.jointype == JOIN_INNER {
        debug_assert!(parent_sjinfo.ojrelid == 0);
        let mut sjinfo = make_dummy_sjinfo();
        init_dummy_sjinfo(
            &mut sjinfo,
            bms::relids_copy::call(left_relids),
            bms::relids_copy::call(right_relids),
        );
        return Ok(sjinfo);
    }

    // memcpy(sjinfo, parent_sjinfo, sizeof(SpecialJoinInfo)).
    let mut sjinfo = parent_sjinfo.clone();

    let left_appinfos = appendinfo::find_appinfos_by_relids::call(root, left_relids)?;
    let right_appinfos = appendinfo::find_appinfos_by_relids::call(root, right_relids)?;

    sjinfo.min_lefthand = appendinfo::adjust_child_relids::call(&sjinfo.min_lefthand, &left_appinfos);
    sjinfo.min_righthand = appendinfo::adjust_child_relids::call(&sjinfo.min_righthand, &right_appinfos);
    sjinfo.syn_lefthand = appendinfo::adjust_child_relids::call(&sjinfo.syn_lefthand, &left_appinfos);
    sjinfo.syn_righthand = appendinfo::adjust_child_relids::call(&sjinfo.syn_righthand, &right_appinfos);
    // outer-join relids need no adjustment

    // left_appinfos/right_appinfos dropped at scope (the C pfree).
    Ok(sjinfo)
}

/* ==========================================================================
 * free_child_join_sjinfo (joinrels.c:1697)
 * ======================================================================== */

/// `free_child_join_sjinfo(child_sjinfo, parent_sjinfo)` (joinrels.c:1697) — the
/// owned-tree analogue: the actual frees are handled by `Drop`; this function
/// survives to run the parent-vs-child identity `Assert`s C performs here.
fn free_child_join_sjinfo(child_sjinfo: &SpecialJoinInfo, parent_sjinfo: &SpecialJoinInfo) {
    // Dummy SpecialJoinInfos of inner joins have no translated fields.
    if child_sjinfo.jointype != JOIN_INNER {
        // C frees the translated relid sets iff they differ from the parent's;
        // owned-tree Drop handles those frees.

        // The non-translated fields must be identical to the parent's.
        debug_assert!(child_sjinfo.commute_above_l == parent_sjinfo.commute_above_l);
        debug_assert!(child_sjinfo.commute_above_r == parent_sjinfo.commute_above_r);
        debug_assert!(child_sjinfo.commute_below_l == parent_sjinfo.commute_below_l);
        debug_assert!(child_sjinfo.commute_below_r == parent_sjinfo.commute_below_r);

        debug_assert!(child_sjinfo.semi_operators == parent_sjinfo.semi_operators);

        // semi_rhs_exprs is not carried in this keystone (C leaves it alone).
    }
    // child_sjinfo is dropped by the caller (the C pfree).
}

/* ==========================================================================
 * compute_partition_bounds (joinrels.c:1739)
 * ======================================================================== */

/// `compute_partition_bounds` (joinrels.c:1739) — compute the partition bounds
/// for a join rel from those of its inputs, returning the two per-segment
/// partition-pairing lists (the C `parts1`/`parts2` out-params; empty unless the
/// bounds were merged or already merged).
fn compute_partition_bounds(
    root: &mut PlannerInfo,
    rel1: RelId,
    rel2: RelId,
    joinrel: RelId,
    parent_sjinfo: &SpecialJoinInfo,
) -> PgResult<(Vec<Option<RelId>>, Vec<Option<RelId>>)> {
    let mut parts1: Vec<Option<RelId>> = Vec::new();
    let mut parts2: Vec<Option<RelId>> = Vec::new();

    // If we don't have the partition bounds for the join rel yet, try to compute
    // those along with pairs of partitions to be joined.
    if root.rel(joinrel).nparts == -1 {
        debug_assert!(root.rel(joinrel).boundinfo.is_none());
        debug_assert!(root.rel(joinrel).part_rels.is_empty());

        // part_scheme cached fields (read off the canonicalized scheme).
        let scheme = root.rel(joinrel).part_scheme.as_ref().ok_or_else(|| {
            PgError::error("compute_partition_bounds: partitioned joinrel must have a part_scheme")
        })?;
        let partnatts = scheme.partnatts as i32;
        let parttyplen = scheme.parttyplen.clone();
        let parttypbyval = scheme.parttypbyval.clone();

        let boundinfo;
        let nparts;

        // See if the partition bounds for inputs are exactly the same, in which
        // case the join rel has the same bounds and same-position partitions pair.
        let inputs_match = !root.rel(rel1).partbounds_merged
            && !root.rel(rel2).partbounds_merged
            && root.rel(rel1).nparts == root.rel(rel2).nparts
            && match (
                root.rel(rel1).boundinfo.as_deref(),
                root.rel(rel2).boundinfo.as_deref(),
            ) {
                (Some(b1), Some(b2)) => {
                    partbounds::partition_bounds_equal::call(partnatts, &parttyplen, &parttypbyval, b1, b2)
                }
                _ => false,
            };

        if inputs_match {
            boundinfo = root.rel(rel1).boundinfo.clone();
            nparts = root.rel(rel1).nparts;
        } else {
            // Try merging the partition bounds for inputs.
            match partbounds::partition_bounds_merge::call(root, rel1, rel2, parent_sjinfo.jointype)? {
                None => {
                    root.rel_mut(joinrel).nparts = 0;
                    return Ok((parts1, parts2));
                }
                Some((merged, p1, p2)) => {
                    parts1 = p1;
                    parts2 = p2;
                    nparts = parts1.len() as i32;
                    boundinfo = Some(alloc::boxed::Box::new(merged));
                    root.rel_mut(joinrel).partbounds_merged = true;
                }
            }
        }

        debug_assert!(nparts > 0);
        root.rel_mut(joinrel).boundinfo = boundinfo;
        root.rel_mut(joinrel).nparts = nparts;
        // palloc0(sizeof(RelOptInfo *) * nparts) — a NULL-filled part_rels array.
        let mut pr: Vec<Option<RelId>> = Vec::new();
        pr.try_reserve(nparts as usize).map_err(oom)?;
        pr.resize(nparts as usize, None);
        root.rel_mut(joinrel).part_rels = pr;
    } else {
        debug_assert!(root.rel(joinrel).nparts > 0);
        debug_assert!(root.rel(joinrel).boundinfo.is_some());
        debug_assert!(!root.rel(joinrel).part_rels.is_empty());

        // If the join rel's partbounds_merged flag is true, inputs aren't
        // guaranteed to have the same bounds, so let get_matching_part_pairs()
        // generate the pairs. Otherwise, nothing to do.
        if root.rel(joinrel).partbounds_merged {
            let (p1, p2) = get_matching_part_pairs(root, joinrel, rel1, rel2)?;
            debug_assert!(p1.len() as i32 == root.rel(joinrel).nparts);
            debug_assert!(p2.len() as i32 == root.rel(joinrel).nparts);
            parts1 = p1;
            parts2 = p2;
        }
    }

    Ok((parts1, parts2))
}

/* ==========================================================================
 * get_matching_part_pairs (joinrels.c:1830)
 * ======================================================================== */

/// `get_matching_part_pairs` (joinrels.c:1830) — generate pairs of partitions to
/// be joined from the inputs, returned as two ordered lists (`None` for an
/// empty/ignored segment).
fn get_matching_part_pairs(
    root: &mut PlannerInfo,
    joinrel: RelId,
    rel1: RelId,
    rel2: RelId,
) -> PgResult<(Vec<Option<RelId>>, Vec<Option<RelId>>)> {
    let rel1_is_simple = is_simple_rel(root.rel(rel1));
    let rel2_is_simple = is_simple_rel(root.rel(rel2));

    let mut parts1: Vec<Option<RelId>> = Vec::new();
    let mut parts2: Vec<Option<RelId>> = Vec::new();

    let nparts = root.rel(joinrel).nparts;
    for cnt_parts in 0..nparts as usize {
        let child_joinrel = root.rel(joinrel).part_rels[cnt_parts];

        // If this segment of the join is empty, it was ignored when previously
        // creating child-join paths in try_partitionwise_join(); add NULL to each
        // list so this segment is ignored again there.
        let child_joinrel = match child_joinrel {
            None => {
                parts1.try_reserve(1).map_err(oom)?;
                parts1.push(None);
                parts2.try_reserve(1).map_err(oom)?;
                parts2.push(None);
                continue;
            }
            Some(id) => id,
        };

        // Get a relids set of partition(s) in this join segment from the rel1 side.
        let child_relids1 = bms::relids_intersect::call(
            &root.rel(child_joinrel).relids,
            &root.rel(rel1).all_partrels,
        );
        debug_assert!(
            bms::relids_num_members::call(&child_relids1)
                == bms::relids_num_members::call(&root.rel(rel1).relids)
        );

        // Get a child rel for rel1 with the relids. We should have the child rel
        // even if rel1 is a join rel (see C comment).
        let child_rel1 = if rel1_is_simple {
            let varno = bms::relids_singleton_member::call(&child_relids1);
            bms::find_base_rel::call(root, varno)
        } else {
            bms::find_join_rel::call(root, &child_relids1)
                .ok_or_else(|| PgError::error("get_matching_part_pairs: child_rel1 must exist"))?
        };

        // Get a relids set of partition(s) in this join segment from the rel2 side.
        let child_relids2 = bms::relids_intersect::call(
            &root.rel(child_joinrel).relids,
            &root.rel(rel2).all_partrels,
        );
        debug_assert!(
            bms::relids_num_members::call(&child_relids2)
                == bms::relids_num_members::call(&root.rel(rel2).relids)
        );

        // Get a child rel for rel2 with the relids. See above comments.
        let child_rel2 = if rel2_is_simple {
            let varno = bms::relids_singleton_member::call(&child_relids2);
            bms::find_base_rel::call(root, varno)
        } else {
            bms::find_join_rel::call(root, &child_relids2)
                .ok_or_else(|| PgError::error("get_matching_part_pairs: child_rel2 must exist"))?
        };

        // The join of rel1 and rel2 is legal, so is the join of the child rels;
        // add them as a join pair producing this join segment.
        parts1.try_reserve(1).map_err(oom)?;
        parts1.push(Some(child_rel1));
        parts2.try_reserve(1).map_err(oom)?;
        parts2.push(Some(child_rel2));
    }

    Ok((parts1, parts2))
}

/* ==========================================================================
 * init_seams
 * ======================================================================== */

/// Install the joinrels-owned inward seams at single-threaded startup.
///
/// joinrels.c owns `have_join_order_restriction`, declared (for the GEQO
/// join-search cycle) in `backend-geqo-all-seams`. We install our real
/// implementation here so the GEQO driver's `have_join_order_restriction::call`
/// reaches it.
pub fn init_seams() {
    geqo::have_join_order_restriction::set(have_join_order_restriction);

    // relnode.c `build_simple_rel` proves a child empty and calls
    // `mark_dummy_rel(rel)` (joinrels.c, owned here) through its no-owner ext
    // seam crate; install the real body.
    relnode_ext_seams::mark_dummy_rel::set(mark_dummy_rel);

    // `init_dummy_sjinfo(left_relids, right_relids)` (joinrels.c, owned here) is
    // read by costsize.c (`calc_joinrel_size_estimate`, the no-clause path, and
    // `get_parameterized_joinrel_size`) through the costsize self-seam crate. The
    // seam keys the two rels by `RelId`; resolve each to `rel->relids` and fill a
    // fresh dummy `SpecialJoinInfo`.
    // `add_outer_joins_to_relids(root, input_relids, sjinfo)` (joinrels.c, owned
    // here) is read by equivclass.c `generate_join_implied_equalities` through the
    // equivclass-ext-seams crate. The equivclass call passes `pushed_down_joins ==
    // NULL`, so we feed a throwaway Vec and discard it. The seam returns a bare
    // `Relids`; the only fallible step is `pushed_down_joins.push` (OOM), which is
    // surfaced via `.expect` to mirror the seam's infallible signature.
    equivclass_ext_seams::add_outer_joins_to_relids::set(
        |root: &PlannerInfo, input_relids, sjinfo: Option<SpecialJoinInfo>| {
            let mut pushed_down_joins: Vec<SpecialJoinInfo> = Vec::new();
            add_outer_joins_to_relids(root, input_relids, sjinfo.as_ref(), &mut pushed_down_joins)
                .expect("add_outer_joins_to_relids")
        },
    );

    costsize_seams::init_dummy_sjinfo::set(
        |root: &PlannerInfo, outer_rel, inner_rel| {
            let left_relids = root.rel(outer_rel).relids.clone();
            let right_relids = root.rel(inner_rel).relids.clone();
            let mut sjinfo = make_dummy_sjinfo();
            init_dummy_sjinfo(&mut sjinfo, left_relids, right_relids);
            sjinfo
        },
    );

    // `is_dummy_rel(root, rel)` (joinrels.c, owned here) is read by the
    // partbounds.c merge routines (`is_dummy_partition`) through the
    // partbounds-seams crate; install the real body.
    partbounds::is_dummy_rel::set(is_dummy_rel);
}

#[cfg(test)]
mod tests;
