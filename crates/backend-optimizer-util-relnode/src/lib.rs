#![forbid(unsafe_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::collapsible_else_if)]
#![allow(clippy::needless_late_init)]
#![allow(clippy::too_many_arguments)]

//! Safe-Rust port of `src/backend/optimizer/util/relnode.c` (postgres-18.3):
//! relation-node lookup/construction routines — building base/join/upper
//! `RelOptInfo`s, parameterized-path `ParamPathInfo`s, and partitionwise-join
//! partition metadata.
//!
//! # Arena model
//!
//! The C pointer graph is modelled over the
//! [`PlannerInfo`](types_pathnodes::PlannerInfo) arena: a
//! [`RelId`]/[`PathId`]/[`RinfoId`]/[`NodeId`] handle indexes the matching arena,
//! and `root.rel(id)` / `root.path(id)` / `root.rinfo(id)` / `root.node(id)`
//! recover the node. The `bms_*` set algebra over `Relids` crosses through the
//! canonical `relids_*` seams (`backend-optimizer-util-relnode-seams`, here
//! aliased `bms`, plus the few in `pathnode-seams`). Externals owned by other
//! (sometimes unported) units cross through their `*-seams` crates; the genuinely
//! absent ones are declared in `backend-optimizer-util-relnode-ext-seams`.
//!
//! There is no `extern "C"`, no raw pointers, and no `c_void`.

extern crate alloc;

use alloc::vec::Vec;

use types_error::PgResult;
use types_nodes::primnodes::{Expr, ExprRelids};

use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{
    AppendRelInfo, ParamPathInfo, PathNode, PlannerInfo, RelId, Relids, RinfoId, SpecialJoinInfo,
    UpperRelationKind, JOIN_ANTI, JOIN_FULL, JOIN_INNER, JOIN_LEFT, JOIN_SEMI, NodeId,
    RELOPT_BASEREL, RELOPT_JOINREL, RELOPT_OTHER_JOINREL, RELOPT_OTHER_MEMBER_REL,
    RELOPT_OTHER_UPPER_REL, RELOPT_UPPER_REL, RTE_RELATION,
};

use backend_optimizer_util_relnode_seams as bms;

use backend_nodes_nodeFuncs_seams as nodefuncs;
use backend_optimizer_path_equivclass_ext_seams as eq_ext;
use backend_optimizer_path_equivclass_seams as equivclass;
use backend_optimizer_path_joinpath_seams as joinpath;
use backend_optimizer_rte_seams as rte;
use backend_optimizer_util_appendinfo_seams as appendinfo;
use backend_optimizer_util_pathnode_seams as pathnode;
use backend_optimizer_util_relnode_ext_seams as ext;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_init_miscinit_seams as miscinit;

/* --------------------------------------------------------------------------
 * Constants mirrored from C headers (not present in the trimmed types crates).
 * ------------------------------------------------------------------------ */

/// `RTE_SUBQUERY` (parsenodes.h `RTEKind`).
pub const RTE_SUBQUERY: u32 = 1;
/// `RTE_JOIN` (parsenodes.h `RTEKind`).
pub const RTE_JOIN: u32 = 2;
/// `RTE_FUNCTION`.
pub const RTE_FUNCTION: u32 = 3;
/// `RTE_TABLEFUNC`.
pub const RTE_TABLEFUNC: u32 = 4;
/// `RTE_VALUES`.
pub const RTE_VALUES: u32 = 5;
/// `RTE_CTE`.
pub const RTE_CTE: u32 = 6;
/// `RTE_NAMEDTUPLESTORE`.
pub const RTE_NAMEDTUPLESTORE: u32 = 7;
/// `RTE_RESULT`.
pub const RTE_RESULT: u32 = 8;

/// `PARTITION_MAX_KEYS` (pg_config_manual.h).
pub const PARTITION_MAX_KEYS: usize = 32;
/// `HTEqualStrategyNumber` (hash AM strategy number for equality).
pub const HTEqualStrategyNumber: i16 = 1;
/// `PARTITION_STRATEGY_HASH` (partdefs.h) — the partition strategy code 'h'.
pub const PARTITION_STRATEGY_HASH: i8 = b'h' as i8;

/// `UINT_MAX` — used for the initial `baserestrict_min_security`.
const UINT_MAX: u32 = u32::MAX;

/// `InvalidOid`.
const INVALID_OID: types_core::primitive::Oid = 0;

/* --------------------------------------------------------------------------
 * Inline macros mirrored from pathnodes.h.
 * ------------------------------------------------------------------------ */

/// `IS_OTHER_REL(rel)` (pathnodes.h).
#[inline]
fn is_other_rel(rel: &types_pathnodes::RelOptInfo) -> bool {
    rel.reloptkind == RELOPT_OTHER_MEMBER_REL
        || rel.reloptkind == RELOPT_OTHER_JOINREL
        || rel.reloptkind == RELOPT_OTHER_UPPER_REL
}

/// `IS_OUTER_JOIN(jointype)` (nodes.h).
#[inline]
fn is_outer_join(jointype: u32) -> bool {
    (1u32 << jointype)
        & ((1 << JOIN_LEFT)
            | (1 << JOIN_FULL)
            | (1 << types_pathnodes::JOIN_RIGHT)
            | (1 << JOIN_ANTI)
            | (1 << types_pathnodes::JOIN_RIGHT_ANTI))
        != 0
}

/// `RINFO_IS_PUSHED_DOWN(rinfo, relids)` (pathnodes.h):
/// `rinfo->is_pushed_down || !bms_is_subset(rinfo->required_relids, relids)`.
#[inline]
fn rinfo_is_pushed_down(root: &PlannerInfo, rinfo: RinfoId, relids: &Relids) -> bool {
    let ri = root.rinfo(rinfo);
    ri.is_pushed_down || !bms::relids_is_subset::call(&ri.required_relids, relids)
}

/* --------------------------------------------------------------------------
 * ExprRelids <-> Relids bridges. Both are word-vector bitmapsets; the empty set
 * is `None` (Relids) / empty words (ExprRelids). This is a pure bit re-pack so
 * the relnode logic that the C runs *inline* on a Var/PHV's nullingrels can use
 * the canonical `relids_*` seams.
 * ------------------------------------------------------------------------ */

#[inline]
fn exprrelids_to_relids(er: &ExprRelids) -> Relids {
    if er.words.iter().all(|&w| w == 0) {
        None
    } else {
        Some(alloc::boxed::Box::new(types_pathnodes::Bitmapset {
            words: er.words.clone(),
        }))
    }
}

#[inline]
fn relids_to_exprrelids(r: &Relids) -> ExprRelids {
    match r {
        None => ExprRelids { words: Vec::new() },
        Some(b) => ExprRelids {
            words: b.words.clone(),
        },
    }
}

/* ==========================================================================
 * setup_simple_rel_arrays
 * ======================================================================== */

/// `setup_simple_rel_arrays(run, root, mcx)` (relnode.c) — prepare the per-RTE
/// arrays.
///
/// C walks the top-level `parse->rtable` (a `List *` of `RangeTblEntry *`) and
/// fills `root->simple_rte_array[rti] = rt_fetch(rti, parse->rtable)`. In this
/// repo the top `Query`'s `rtable` lives in the [`PlannerRun`] store, keyed by
/// `root.parse` ([`QueryId`]); `simple_rte_array` carries opaque
/// [`RangeTblEntryId`] handles into the same store (#300). To fill it we copy
/// each top-level `RangeTblEntry` into the run's RTE store and record the handle.
///
/// Two phases are required because `run.rtable(...)` borrows `run` immutably
/// while `run.intern_rte(...)` borrows it mutably: phase 1 clones every entry
/// out from under the shared borrow into owned `RangeTblEntry<'mcx>` locals,
/// phase 2 interns each (the now-released shared borrow lets `intern_rte` take
/// `&mut self`).
pub fn setup_simple_rel_arrays<'mcx>(
    run: &mut types_pathnodes::planner_run::PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    mcx: mcx::Mcx<'mcx>,
) -> PgResult<()> {
    /* Arrays are accessed using RT indexes (1..N) */
    let size = run.rtable(root.parse).len() as i32 + 1;
    root.simple_rel_array_size = size;

    /*
     * simple_rel_array is initialized to all NULLs, since no RelOptInfos exist
     * yet. It'll be filled by later calls to build_simple_rel().
     */
    root.simple_rel_array = alloc::vec![None; size as usize];

    /*
     * simple_rte_array is an array equivalent of the rtable list (1..N).
     *
     * Phase 1: clone every top-level RTE into `mcx`-owned values. We cannot
     * call `intern_rte` (which needs `&mut run`) while holding the `&run`
     * borrow that `rtable()` returns, so collect first.
     */
    let mut cloned: Vec<types_nodes::parsenodes::RangeTblEntry<'mcx>> =
        Vec::with_capacity((size as usize).saturating_sub(1));
    for rte in run.rtable(root.parse).iter() {
        cloned.push(rte.clone_in(mcx)?);
    }

    /*
     * Phase 2: intern each cloned RTE into the run store, recording the
     * returned RangeTblEntryId at its RT index (slot 0 is unused).
     */
    let mut simple_rte_array: Vec<types_pathnodes::RangeTblEntryId> =
        alloc::vec![types_pathnodes::RangeTblEntryId(0); size as usize];
    for (i, rte) in cloned.into_iter().enumerate() {
        simple_rte_array[i + 1] = run.intern_rte(rte);
    }
    root.simple_rte_array = simple_rte_array;

    /* append_rel_array is not needed if there are no AppendRelInfos */
    if root.append_rel_list.is_empty() {
        root.append_rel_array = Vec::new();
        return Ok(());
    }

    let mut append_rel_array: Vec<Option<AppendRelInfo>> = alloc::vec![None; size as usize];

    /*
     * append_rel_array is filled with any already-existing AppendRelInfos.
     */
    for appinfo in root.append_rel_list.iter() {
        let child_relid = appinfo.child_relid;

        /* Sanity check */
        debug_assert!((child_relid as i32) < size);

        if append_rel_array[child_relid as usize].is_some() {
            panic!("child relation already exists");
        }

        append_rel_array[child_relid as usize] = Some(appinfo.clone());
    }
    root.append_rel_array = append_rel_array;
    Ok(())
}

/* ==========================================================================
 * expand_planner_arrays
 * ======================================================================== */

/// `expand_planner_arrays(root, add_size)` (relnode.c).
pub fn expand_planner_arrays(root: &mut PlannerInfo, add_size: i32) {
    debug_assert!(add_size > 0);

    let new_size = root.simple_rel_array_size + add_size;

    root.simple_rel_array.resize(new_size as usize, None);
    root.simple_rte_array
        .resize(new_size as usize, types_pathnodes::RangeTblEntryId(0));

    if !root.append_rel_array.is_empty() {
        root.append_rel_array.resize(new_size as usize, None);
    } else {
        root.append_rel_array = alloc::vec![None; new_size as usize];
    }

    root.simple_rel_array_size = new_size;
}

/* ==========================================================================
 * build_simple_rel
 * ======================================================================== */

/// `build_simple_rel(root, relid, parent)` (relnode.c) — construct a new
/// `RelOptInfo` for a base relation or 'other' relation.
pub fn build_simple_rel<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    relid: i32,
    parent: Option<RelId>,
) -> PgResult<RelId> {
    /* Rel should not exist already */
    debug_assert!(relid > 0 && relid < root.simple_rel_array_size);
    if root.simple_rel_array[relid as usize].is_some() {
        panic!("rel {} already exists", relid);
    }

    /* Fetch RTE for relation */
    let rtekind = rte::rte_rtekind::call(run, root, relid as u32);

    let mut rel = types_pathnodes::RelOptInfo {
        reloptkind: if parent.is_some() {
            RELOPT_OTHER_MEMBER_REL
        } else {
            RELOPT_BASEREL
        },
        relids: bms::relids_make_singleton::call(relid),
        rows: 0.0,
        /* cheap startup cost is interesting iff not all tuples retrieved */
        consider_startup: root.tuple_fraction > 0.0,
        consider_param_startup: false,
        consider_parallel: false,
        reltarget: Some(alloc::boxed::Box::new(ext::create_empty_pathtarget::call())),
        relid: relid as u32,
        rtekind,
        rel_parallel_workers: -1,
        serverid: INVALID_OID,
        baserestrict_min_security: UINT_MAX,
        nparts: -1,
        ..Default::default()
    };
    /* most fields default to NIL/NULL/0, matching the C make/zero-out. */

    if rtekind == RTE_RELATION {
        /* parent->rtekind asserts are debug-only and require a parent fetch. */
        #[cfg(debug_assertions)]
        if let Some(p) = parent {
            let prtekind = root.rel(p).rtekind;
            debug_assert!(prtekind == RTE_RELATION || prtekind == RTE_SUBQUERY);
        }

        /*
         * For any RELATION rte, we need a userid for permission access.
         */
        let parent_is_subquery = parent
            .map(|p| root.rel(p).rtekind == RTE_SUBQUERY)
            .unwrap_or(false);
        if rel.reloptkind == RELOPT_BASEREL
            || (rel.reloptkind == RELOPT_OTHER_MEMBER_REL && parent_is_subquery)
        {
            rel.userid = ext::get_rte_perminfo_checkasuser::call(root, relid as u32);
        } else {
            rel.userid = root.rel(parent.unwrap()).userid;
        }
    } else {
        rel.userid = INVALID_OID;
    }

    /*
     * Pass assorted information down the inheritance hierarchy.
     */
    if let Some(p) = parent {
        let prel = root.rel(p);
        /* We keep back-links to immediate parent and topmost parent. */
        rel.parent = Some(p);
        rel.top_parent = if prel.top_parent.is_some() {
            prel.top_parent
        } else {
            Some(p)
        };
        rel.top_parent_relids = bms::relids_copy::call(&root.rel(rel.top_parent.unwrap()).relids);

        /* A child rel is below the same outer joins as its parent. */
        rel.nulling_relids = bms::relids_copy::call(&prel.nulling_relids);

        /* Propagate lateral-reference information. */
        rel.direct_lateral_relids = bms::relids_copy::call(&prel.direct_lateral_relids);
        rel.lateral_relids = bms::relids_copy::call(&prel.lateral_relids);
        rel.lateral_referencers = bms::relids_copy::call(&prel.lateral_referencers);
    } else {
        rel.parent = None;
        rel.top_parent = None;
        rel.top_parent_relids = None;
        rel.nulling_relids = None;
        rel.direct_lateral_relids = None;
        rel.lateral_relids = None;
        rel.lateral_referencers = None;
    }

    /* Check type of rtable entry */
    match rtekind {
        RTE_RELATION => {
            /* Table --- retrieve statistics from the system catalogs */
            /* deferred until rel is in the array (see below). */
        }
        RTE_SUBQUERY | RTE_FUNCTION | RTE_TABLEFUNC | RTE_VALUES | RTE_CTE
        | RTE_NAMEDTUPLESTORE => {
            /*
             * Subquery/function/tablefunc/values/CTE/ENR --- set up attr range.
             * Note: 0 is included in range to support whole-row Vars.
             */
            rel.min_attr = 0;
            rel.max_attr = ext::rte_eref_colnames_len::call(root, relid as u32) as i16;
            let n = (rel.max_attr - rel.min_attr + 1) as usize;
            rel.attr_needed = alloc::vec![None; n];
            rel.attr_widths = alloc::vec![0i32; n];
        }
        RTE_RESULT => {
            /* RTE_RESULT has no columns, nor a whole-row Var */
            rel.min_attr = 0;
            rel.max_attr = -1;
            rel.attr_needed = Vec::new();
            rel.attr_widths = Vec::new();
        }
        other => {
            panic!("unrecognized RTE kind: {}", other);
        }
    }

    /*
     * We must apply the partially filled in RelOptInfo before calling
     * apply_child_basequals due to transformations within that function which
     * require the RelOptInfo to be available in the simple_rel_array.
     */
    let id = root.alloc_rel(rel);
    root.simple_rel_array[relid as usize] = Some(id);

    /* For RTE_RELATION, retrieve statistics now that the rel is in the array. */
    if rtekind == RTE_RELATION {
        let relation_object_id = rte::rte_relid::call(run, root, relid as u32);
        let inh = rte::rte_inh::call(run, root, relid as u32);
        ext::get_relation_info::call(run, root, relation_object_id, inh, id)?;
    }

    /*
     * Apply the parent's quals to the child, with appropriate substitution of
     * variables. If the resulting clause is constant-FALSE or NULL, mark the
     * child as dummy right away.
     */
    if let Some(p) = parent {
        let appinfo = root.append_rel_array[relid as usize]
            .clone()
            .expect("appinfo != NULL");
        if !ext::apply_child_basequals::call(root, p, id, relid as u32, &appinfo)? {
            /* Restriction clause reduced to constant FALSE or NULL. */
            ext::mark_dummy_rel::call(root, id)?;
        }
    }

    Ok(id)
}

/* ==========================================================================
 * find_base_rel / find_base_rel_noerr / find_base_rel_ignore_join
 * ======================================================================== */

/// `find_base_rel(root, relid)` (relnode.c) — a base/otherrel entry that must
/// already exist.
pub fn find_base_rel(root: &PlannerInfo, relid: i32) -> RelId {
    /* use an unsigned comparison to prevent negative array element access */
    if (relid as u32) < (root.simple_rel_array_size as u32) {
        if let Some(rel) = root.simple_rel_array[relid as usize] {
            return rel;
        }
    }

    panic!("no relation entry for relid {}", relid);
}

/// `find_base_rel_noerr(root, relid)` (relnode.c) — returns `None` if there's
/// no such entry.
pub fn find_base_rel_noerr(root: &PlannerInfo, relid: i32) -> Option<RelId> {
    if (relid as u32) < (root.simple_rel_array_size as u32) {
        return root.simple_rel_array[relid as usize];
    }
    None
}

/// `find_base_rel_ignore_join(root, relid)` (relnode.c) — like `find_base_rel`,
/// but returns `None` rather than erroring when `relid` references an outer join.
pub fn find_base_rel_ignore_join<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &PlannerInfo,
    relid: i32,
) -> Option<RelId> {
    if (relid as u32) < (root.simple_rel_array_size as u32) {
        let rel = root.simple_rel_array[relid as usize];
        if let Some(rel) = rel {
            return Some(rel);
        }

        /*
         * For debugging, verify that the relid is an outer join and not weird.
         */
        let rtekind = rte::rte_rtekind::call(run, root, relid as u32);
        if rtekind == RTE_JOIN && rte::rte_jointype::call(run, root, relid as u32) != JOIN_INNER {
            return None;
        }
    }

    panic!("no relation entry for relid {}", relid);
}

/* ==========================================================================
 * find_join_rel (build_join_rel_hash folded — the join_rel_hash is modeled as a
 * presence flag; lookups remain identity over join_rel_list).
 * ======================================================================== */

/// `find_join_rel(root, relids)` (relnode.c) — the join entry for `relids`, or
/// `None`.
pub fn find_join_rel(root: &PlannerInfo, relids: &Relids) -> Option<RelId> {
    /*
     * The hash table is an opaque accelerator over the same join_rel_list; in
     * this arena model we always do the identity scan (the C linear-search
     * branch), which yields the same answer. (build_join_rel_hash is a pure
     * performance helper with no observable effect.)
     */
    for &rel in root.join_rel_list.iter() {
        if bms::relids_equal::call(&root.rel(rel).relids, relids) {
            return Some(rel);
        }
    }

    None
}

/* ==========================================================================
 * set_foreign_rel_properties
 * ======================================================================== */

/// `set_foreign_rel_properties(joinrel, outer_rel, inner_rel)` (relnode.c).
fn set_foreign_rel_properties(root: &mut PlannerInfo, joinrel: RelId, outer_rel: RelId, inner_rel: RelId) {
    let o = root.rel(outer_rel);
    let i = root.rel(inner_rel);
    let oserverid = o.serverid;
    let ouserid = o.userid;
    let ouseridiscurrent = o.useridiscurrent;
    let ohas_fdwroutine = o.has_fdwroutine;
    let iserverid = i.serverid;
    let iuserid = i.userid;
    let iuseridiscurrent = i.useridiscurrent;

    if oserverid != INVALID_OID && iserverid == oserverid {
        if iuserid == ouserid {
            let jr = root.rel_mut(joinrel);
            jr.serverid = oserverid;
            jr.userid = ouserid;
            jr.useridiscurrent = ouseridiscurrent || iuseridiscurrent;
            jr.has_fdwroutine = ohas_fdwroutine;
        } else if iuserid == INVALID_OID && ouserid == miscinit::get_user_id::call() {
            let jr = root.rel_mut(joinrel);
            jr.serverid = oserverid;
            jr.userid = ouserid;
            jr.useridiscurrent = true;
            jr.has_fdwroutine = ohas_fdwroutine;
        } else if ouserid == INVALID_OID && iuserid == miscinit::get_user_id::call() {
            let jr = root.rel_mut(joinrel);
            jr.serverid = oserverid;
            jr.userid = iuserid;
            jr.useridiscurrent = true;
            jr.has_fdwroutine = ohas_fdwroutine;
        }
    }
}

/* ==========================================================================
 * add_join_rel
 * ======================================================================== */

/// `add_join_rel(root, joinrel)` (relnode.c).
fn add_join_rel(root: &mut PlannerInfo, joinrel: RelId) {
    /* GEQO requires us to append the new joinrel to the end of the list! */
    root.join_rel_list.push(joinrel);

    /*
     * The auxiliary hashtable (join_rel_hash) is an opaque accelerator with no
     * observable effect; find_join_rel always scans join_rel_list, so nothing
     * more needs to be done here.
     */
}

/* ==========================================================================
 * build_join_rel
 * ======================================================================== */

/// `build_join_rel(root, joinrelids, outer_rel, inner_rel, sjinfo,
/// pushed_down_joins, &restrictlist)` (relnode.c) — find or build the join
/// `RelOptInfo`, returning it plus the restrictlist.
pub fn build_join_rel<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    joinrelids: &Relids,
    outer_rel: RelId,
    inner_rel: RelId,
    sjinfo: &SpecialJoinInfo,
    pushed_down_joins: &[SpecialJoinInfo],
) -> PgResult<(RelId, Vec<RinfoId>)> {
    /* This function should be used only for join between parents. */
    debug_assert!(!is_other_rel(root.rel(outer_rel)) && !is_other_rel(root.rel(inner_rel)));

    /* See if we already have a joinrel for this set of base rels. */
    if let Some(joinrel) = find_join_rel(root, joinrelids) {
        /*
         * Yes, so we only need to figure the restrictlist for this particular
         * pair of component relations.
         */
        let restrictlist =
            build_joinrel_restrictlist(root, run, joinrel, outer_rel, inner_rel, sjinfo)?;
        return Ok((joinrel, restrictlist));
    }

    /* Nope, so make one. */
    let direct_lateral_relids = bms::relids_union::call(
        &root.rel(outer_rel).direct_lateral_relids,
        &root.rel(inner_rel).direct_lateral_relids,
    );
    let relids = bms::relids_copy::call(joinrelids);
    let lateral_relids = min_join_parameterization(root, &relids, outer_rel, inner_rel);

    let joinrel = types_pathnodes::RelOptInfo {
        reloptkind: RELOPT_JOINREL,
        relids,
        rows: 0.0,
        consider_startup: root.tuple_fraction > 0.0,
        consider_param_startup: false,
        consider_parallel: false,
        reltarget: Some(alloc::boxed::Box::new(ext::create_empty_pathtarget::call())),
        direct_lateral_relids,
        lateral_relids,
        relid: 0, /* indicates not a baserel */
        rtekind: RTE_JOIN,
        min_attr: 0,
        max_attr: 0,
        rel_parallel_workers: -1,
        serverid: INVALID_OID,
        userid: INVALID_OID,
        useridiscurrent: false,
        baserestrict_min_security: UINT_MAX,
        has_eclass_joins: false,
        consider_partitionwise_join: false,
        parent: None,
        top_parent: None,
        top_parent_relids: None,
        nparts: -1,
        partbounds_merged: false,
        ..Default::default()
    };

    let joinrel = root.alloc_rel(joinrel);

    /* Compute information relevant to the foreign relations. */
    set_foreign_rel_properties(root, joinrel, outer_rel, inner_rel);

    /*
     * Fill the joinrel's tlist with just the Vars and PHVs needed above.
     */
    build_joinrel_tlist(
        root,
        joinrel,
        outer_rel,
        sjinfo,
        pushed_down_joins,
        sjinfo.jointype == JOIN_FULL,
    )?;
    build_joinrel_tlist(
        root,
        joinrel,
        inner_rel,
        sjinfo,
        pushed_down_joins,
        sjinfo.jointype != JOIN_INNER,
    )?;
    ext::add_placeholders_to_joinrel::call(root, joinrel, outer_rel, inner_rel, sjinfo)?;

    /*
     * Finish computing direct_lateral_relids now that PHVs are added.
     */
    let jrelids = bms::relids_copy::call(&root.rel(joinrel).relids);
    let dlr = root.rel_mut(joinrel).direct_lateral_relids.take();
    root.rel_mut(joinrel).direct_lateral_relids =
        pathnode::relids_del_members::call(dlr, &jrelids);

    /*
     * Construct restrict and join clause lists for the new joinrel.
     */
    let restrictlist = build_joinrel_restrictlist(root, run, joinrel, outer_rel, inner_rel, sjinfo)?;
    build_joinrel_joinlist(root, joinrel, outer_rel, inner_rel);

    /* Check whether the joinrel has any pending EquivalenceClass joins. */
    let has_eclass_joins = equivclass::has_relevant_eclass_joinclause::call(root, joinrel);
    root.rel_mut(joinrel).has_eclass_joins = has_eclass_joins;

    /* Store the partition information. */
    build_joinrel_partition_info(root, joinrel, outer_rel, inner_rel, sjinfo, &restrictlist)?;

    /* Set estimates of the joinrel's size. */
    ext::set_joinrel_size_estimates::call(run, root, joinrel, outer_rel, inner_rel, sjinfo, &restrictlist)?;

    /*
     * Set the consider_parallel flag if this joinrel could be scanned within a
     * parallel worker.
     */
    let exprs: Vec<NodeId> = root.rel(joinrel).reltarget.as_ref().unwrap().exprs.clone();
    if root.rel(inner_rel).consider_parallel
        && root.rel(outer_rel).consider_parallel
        && is_parallel_safe_rinfos(root, &restrictlist)
        && pathnode::is_parallel_safe::call(root, &exprs)
    {
        root.rel_mut(joinrel).consider_parallel = true;
    }

    /* Add the joinrel to the PlannerInfo. */
    add_join_rel(root, joinrel);

    /*
     * If dynamic-programming join search is active, add to the appropriate
     * sublist.
     */
    if !root.join_rel_level.is_empty() {
        debug_assert!(root.join_cur_level > 0);
        debug_assert!(root.join_cur_level <= bms::relids_num_members::call(&root.rel(joinrel).relids));
        let lvl = root.join_cur_level as usize;
        root.join_rel_level[lvl].push(joinrel);
    }

    Ok((joinrel, restrictlist))
}

/// `is_parallel_safe(root, (Node *) restrictlist)` — restrictlist is a list of
/// RestrictInfos; the C `is_parallel_safe` walks each clause. We forward the
/// clause node handles to the qual-list parallel-safety seam.
fn is_parallel_safe_rinfos(root: &PlannerInfo, rinfos: &[RinfoId]) -> bool {
    let quals: Vec<NodeId> = rinfos.iter().map(|&ri| root.rinfo(ri).clause).collect();
    pathnode::is_parallel_safe_quals::call(root, &quals)
}

/* ==========================================================================
 * build_child_join_rel
 * ======================================================================== */

/// `build_child_join_rel(root, outer_rel, inner_rel, parent_joinrel,
/// restrictlist, sjinfo, nappinfos, appinfos)` (relnode.c).
pub fn build_child_join_rel<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    outer_rel: RelId,
    inner_rel: RelId,
    parent_joinrel: RelId,
    restrictlist: &[RinfoId],
    sjinfo: &SpecialJoinInfo,
    appinfos: &[AppendRelInfo],
) -> PgResult<RelId> {
    /* Only joins between "other" relations land here. */
    debug_assert!(is_other_rel(root.rel(outer_rel)) && is_other_rel(root.rel(inner_rel)));
    /* The parent joinrel should have consider_partitionwise_join set. */
    debug_assert!(root.rel(parent_joinrel).consider_partitionwise_join);

    let relids = appendinfo::adjust_child_relids::call(&root.rel(parent_joinrel).relids, appinfos);
    let top_parent = if root.rel(parent_joinrel).top_parent.is_some() {
        root.rel(parent_joinrel).top_parent
    } else {
        Some(parent_joinrel)
    };
    let top_parent_relids = bms::relids_copy::call(&root.rel(top_parent.unwrap()).relids);

    let joinrel = types_pathnodes::RelOptInfo {
        reloptkind: RELOPT_OTHER_JOINREL,
        relids,
        rows: 0.0,
        consider_startup: root.tuple_fraction > 0.0,
        consider_param_startup: false,
        consider_parallel: false,
        reltarget: Some(alloc::boxed::Box::new(ext::create_empty_pathtarget::call())),
        direct_lateral_relids: None,
        lateral_relids: None,
        relid: 0,
        rtekind: RTE_JOIN,
        min_attr: 0,
        max_attr: 0,
        serverid: INVALID_OID,
        userid: INVALID_OID,
        useridiscurrent: false,
        has_eclass_joins: false,
        consider_partitionwise_join: false,
        parent: Some(parent_joinrel),
        top_parent,
        top_parent_relids,
        nparts: -1,
        partbounds_merged: false,
        ..Default::default()
    };
    let joinrel = root.alloc_rel(joinrel);

    /* Compute information relevant to foreign relations. */
    set_foreign_rel_properties(root, joinrel, outer_rel, inner_rel);

    /* Set up reltarget struct */
    build_child_join_reltarget(root, parent_joinrel, joinrel, appinfos)?;

    /* Construct joininfo list. */
    let parent_joininfo = root.rel(parent_joinrel).joininfo.clone();
    let new_joininfo =
        appendinfo::adjust_appendrel_attrs_restrictlist::call(root, &parent_joininfo, appinfos)?;
    root.rel_mut(joinrel).joininfo = new_joininfo;

    /* Lateral relids referred in child join == those of the parent. */
    let dlr = bms::relids_copy::call(&root.rel(parent_joinrel).direct_lateral_relids);
    let lr = bms::relids_copy::call(&root.rel(parent_joinrel).lateral_relids);
    root.rel_mut(joinrel).direct_lateral_relids = dlr;
    root.rel_mut(joinrel).lateral_relids = lr;

    /* If the parent joinrel has pending eclasses, so does the child. */
    let pej = root.rel(parent_joinrel).has_eclass_joins;
    root.rel_mut(joinrel).has_eclass_joins = pej;

    /* Is the join between partitions itself partitioned? */
    build_joinrel_partition_info(root, joinrel, outer_rel, inner_rel, sjinfo, restrictlist)?;

    /* Child joinrel is parallel safe if parent is parallel safe. */
    let pcp = root.rel(parent_joinrel).consider_parallel;
    root.rel_mut(joinrel).consider_parallel = pcp;

    /* Set estimates of the child-joinrel's size. */
    ext::set_joinrel_size_estimates::call(run, root, joinrel, outer_rel, inner_rel, sjinfo, restrictlist)?;

    /* We build the join only once. */
    debug_assert!(find_join_rel(root, &root.rel(joinrel).relids).is_none());

    /* Add the relation to the PlannerInfo. */
    add_join_rel(root, joinrel);

    /*
     * We might need EquivalenceClass members corresponding to the child join.
     */
    if root.rel(joinrel).has_eclass_joins || ext::has_useful_pathkeys::call(root, parent_joinrel) {
        ext::add_child_join_rel_equivalences::call(root, appinfos, parent_joinrel, joinrel)?;
    }

    Ok(joinrel)
}

/* ==========================================================================
 * min_join_parameterization
 * ======================================================================== */

/// `min_join_parameterization(root, joinrelids, outer_rel, inner_rel)`
/// (relnode.c).
pub fn min_join_parameterization(
    root: &PlannerInfo,
    joinrelids: &Relids,
    outer_rel: RelId,
    inner_rel: RelId,
) -> Relids {
    /*
     * The union of the inputs' lateral_relids, less whatever is in the join.
     */
    let result = bms::relids_union::call(
        &root.rel(outer_rel).lateral_relids,
        &root.rel(inner_rel).lateral_relids,
    );
    pathnode::relids_del_members::call(result, joinrelids)
}

/* ==========================================================================
 * build_joinrel_tlist
 * ======================================================================== */

/// `build_joinrel_tlist(root, joinrel, input_rel, sjinfo, pushed_down_joins,
/// can_null)` (relnode.c).
fn build_joinrel_tlist(
    root: &mut PlannerInfo,
    joinrel: RelId,
    input_rel: RelId,
    sjinfo: &SpecialJoinInfo,
    pushed_down_joins: &[SpecialJoinInfo],
    can_null: bool,
) -> PgResult<()> {
    let relids = bms::relids_copy::call(&root.rel(joinrel).relids);
    let mut tuple_width: i64 = root.rel(joinrel).reltarget.as_ref().unwrap().width as i64;

    let input_exprs: Vec<NodeId> = root.rel(input_rel).reltarget.as_ref().unwrap().exprs.clone();

    for var_id in input_exprs {
        let node = root.node(var_id).clone();

        /* For a PlaceHolderVar, look up the PlaceHolderInfo. */
        if let Expr::PlaceHolderVar(_) = node {
            let phinfo = joinpath::find_placeholder_info::call(root, var_id);

            /* Is it still needed above this joinrel? */
            if bms::relids_nonempty_difference::call(&root.phinfo(phinfo).ph_needed, &relids) {
                /*
                 * Yes, add it. If this join nulls the input, update the PHV's
                 * phnullingrels (making a copy).
                 */
                let out_id;
                if can_null {
                    let mut phv = node.clone();
                    let phv_inner = match &mut phv {
                        Expr::PlaceHolderVar(p) => p,
                        _ => unreachable!(),
                    };
                    let phrels = exprrelids_to_relids(&phv_inner.phrels);
                    let mut phnullingrels = exprrelids_to_relids(&phv_inner.phnullingrels);

                    /* See comments in C to understand this logic. */
                    if sjinfo.ojrelid != 0
                        && bms::relids_is_member::call(sjinfo.ojrelid as i32, &relids)
                        && (bms::relids_is_subset::call(&phrels, &sjinfo.syn_righthand)
                            || (sjinfo.jointype == JOIN_FULL
                                && bms::relids_is_subset::call(&phrels, &sjinfo.syn_lefthand)))
                    {
                        phnullingrels =
                            bms::relids_add_member::call(phnullingrels, sjinfo.ojrelid as i32);
                    }
                    for othersj in pushed_down_joins.iter() {
                        debug_assert!(bms::relids_is_member::call(othersj.ojrelid as i32, &relids));
                        if bms::relids_is_subset::call(&phrels, &othersj.syn_righthand) {
                            phnullingrels =
                                bms::relids_add_member::call(phnullingrels, othersj.ojrelid as i32);
                        }
                    }
                    let inter = bms::relids_intersect::call(&sjinfo.commute_above_r, &relids);
                    phnullingrels = bms::relids_join::call(phnullingrels, inter);

                    phv_inner.phnullingrels = relids_to_exprrelids(&phnullingrels);
                    out_id = root.alloc_node(phv);
                } else {
                    out_id = var_id;
                }

                root.rel_mut(joinrel)
                    .reltarget
                    .as_mut()
                    .unwrap()
                    .exprs
                    .push(out_id);
                /* Bubbling up the precomputed result has cost zero */
                tuple_width += root.phinfo(phinfo).ph_width as i64;
            }
            continue;
        }

        /*
         * Otherwise, anything here ought to be a Var.
         */
        let var = match &node {
            Expr::Var(v) => v.clone(),
            other => panic!("unexpected node type in rel targetlist: {:?}", other),
        };

        if var.varno == ROWID_VAR {
            /* UPDATE/DELETE/MERGE row identity vars are always needed */
            /* Update reltarget width estimate from RowIdentityVarInfo */
            tuple_width +=
                ext::row_identity_var_rowidwidth::call(root, (var.varattno - 1) as i32) as i64;
        } else {
            /* Get the Var's original base rel */
            let baserel = find_base_rel(root, var.varno);
            let br = root.rel(baserel);

            /* Is it still needed above this joinrel? */
            let ndx = (var.varattno - br.min_attr) as usize;
            if !bms::relids_nonempty_difference::call(&br.attr_needed[ndx], &relids) {
                continue; /* nope, skip it */
            }

            /* Update reltarget width estimate from baserel's attr_widths */
            tuple_width += br.attr_widths[ndx] as i64;
        }

        /*
         * Add the Var to the output. If this join nulls this input, update the
         * Var's varnullingrels (making a copy). Never add nullingrel bits to
         * row identity Vars.
         */
        let out_id;
        if can_null && var.varno != ROWID_VAR {
            let mut newvar = node.clone();
            let v = match &mut newvar {
                Expr::Var(v) => v,
                _ => unreachable!(),
            };
            let mut varnullingrels = exprrelids_to_relids(&v.varnullingrels);

            /* See comments in C to understand this logic. */
            if sjinfo.ojrelid != 0
                && bms::relids_is_member::call(sjinfo.ojrelid as i32, &relids)
                && (bms::relids_is_member::call(v.varno, &sjinfo.syn_righthand)
                    || (sjinfo.jointype == JOIN_FULL
                        && bms::relids_is_member::call(v.varno, &sjinfo.syn_lefthand)))
            {
                varnullingrels =
                    bms::relids_add_member::call(varnullingrels, sjinfo.ojrelid as i32);
            }
            for othersj in pushed_down_joins.iter() {
                debug_assert!(bms::relids_is_member::call(othersj.ojrelid as i32, &relids));
                if bms::relids_is_member::call(v.varno, &othersj.syn_righthand) {
                    varnullingrels =
                        bms::relids_add_member::call(varnullingrels, othersj.ojrelid as i32);
                }
            }
            let inter = bms::relids_intersect::call(&sjinfo.commute_above_r, &relids);
            varnullingrels = bms::relids_join::call(varnullingrels, inter);

            v.varnullingrels = relids_to_exprrelids(&varnullingrels);
            out_id = root.alloc_node(newvar);
        } else {
            out_id = var_id;
        }

        root.rel_mut(joinrel)
            .reltarget
            .as_mut()
            .unwrap()
            .exprs
            .push(out_id);

        /* Vars have cost zero, so no need to adjust reltarget->cost */
    }

    let w = ext::clamp_width_est::call(tuple_width);
    root.rel_mut(joinrel).reltarget.as_mut().unwrap().width = w;

    Ok(())
}

/// `ROWID_VAR` (primnodes.h) — the special varno for row identity Vars.
const ROWID_VAR: i32 = -2;

/* ==========================================================================
 * build_joinrel_restrictlist / build_joinrel_joinlist + subbuild helpers
 * ======================================================================== */

/// `build_joinrel_restrictlist(root, joinrel, outer_rel, inner_rel, sjinfo)`
/// (relnode.c).
fn build_joinrel_restrictlist<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    joinrel: RelId,
    outer_rel: RelId,
    inner_rel: RelId,
    sjinfo: &SpecialJoinInfo,
) -> PgResult<Vec<RinfoId>> {
    let both_input_relids =
        bms::relids_union::call(&root.rel(outer_rel).relids, &root.rel(inner_rel).relids);

    /*
     * Collect all clauses that syntactically belong at this level, eliminating
     * duplicates.
     */
    let mut result = subbuild_joinrel_restrictlist(root, joinrel, outer_rel, &both_input_relids, Vec::new());
    result = subbuild_joinrel_restrictlist(root, joinrel, inner_rel, &both_input_relids, result);

    /*
     * Add on any clauses derived from EquivalenceClasses.
     */
    let outer_relids = bms::relids_copy::call(&root.rel(outer_rel).relids);
    let join_relids = bms::relids_copy::call(&root.rel(joinrel).relids);
    let eq = equivclass::generate_join_implied_equalities::call(
        root,
        run,
        join_relids,
        outer_relids,
        inner_rel,
        Some(sjinfo.clone()),
    )?;
    result.extend(eq);

    Ok(result)
}

/// `build_joinrel_joinlist(joinrel, outer_rel, inner_rel)` (relnode.c).
fn build_joinrel_joinlist(root: &mut PlannerInfo, joinrel: RelId, outer_rel: RelId, inner_rel: RelId) {
    let outer_joininfo = root.rel(outer_rel).joininfo.clone();
    let inner_joininfo = root.rel(inner_rel).joininfo.clone();

    let mut result = subbuild_joinrel_joinlist(root, joinrel, &outer_joininfo, Vec::new());
    result = subbuild_joinrel_joinlist(root, joinrel, &inner_joininfo, result);

    root.rel_mut(joinrel).joininfo = result;
}

/// `subbuild_joinrel_restrictlist(root, joinrel, input_rel, both_input_relids,
/// new_restrictlist)` (relnode.c).
fn subbuild_joinrel_restrictlist(
    root: &PlannerInfo,
    joinrel: RelId,
    input_rel: RelId,
    both_input_relids: &Relids,
    mut new_restrictlist: Vec<RinfoId>,
) -> Vec<RinfoId> {
    let joinrel_relids = &root.rel(joinrel).relids;
    let input_joininfo = root.rel(input_rel).joininfo.clone();

    for rinfo in input_joininfo {
        let ri = root.rinfo(rinfo);
        if bms::relids_is_subset::call(&ri.required_relids, joinrel_relids) {
            /*
             * This clause should become a restriction clause for the joinrel.
             * If it's a clone clause it might be too late to evaluate it.
             */
            if ri.has_clone || ri.is_clone {
                debug_assert!(!rinfo_is_pushed_down(root, rinfo, joinrel_relids));
                if !bms::relids_is_subset::call(&ri.required_relids, both_input_relids) {
                    continue;
                }
                if bms::relids_overlap::call(&ri.incompatible_relids, both_input_relids) {
                    continue;
                }
            } else {
                /*
                 * For non-clone clauses, just Assert it's OK.
                 */
                debug_assert!(
                    rinfo_is_pushed_down(root, rinfo, joinrel_relids)
                        || bms::relids_is_subset::call(&ri.required_relids, both_input_relids)
                );
            }

            /* Add it, eliminating duplicates (pointer/handle equality). */
            if !new_restrictlist.contains(&rinfo) {
                new_restrictlist.push(rinfo);
            }
        } else {
            /* still a join clause at this level; ignore here. */
        }
    }

    new_restrictlist
}

/// `subbuild_joinrel_joinlist(joinrel, joininfo_list, new_joininfo)`
/// (relnode.c).
fn subbuild_joinrel_joinlist(
    root: &PlannerInfo,
    joinrel: RelId,
    joininfo_list: &[RinfoId],
    mut new_joininfo: Vec<RinfoId>,
) -> Vec<RinfoId> {
    /* Expected to be called only for join between parent relations. */
    debug_assert!(root.rel(joinrel).reloptkind == RELOPT_JOINREL);

    let joinrel_relids = &root.rel(joinrel).relids;
    for &rinfo in joininfo_list {
        let ri = root.rinfo(rinfo);
        if bms::relids_is_subset::call(&ri.required_relids, joinrel_relids) {
            /* Becomes a restriction clause for the joinrel; ignore here. */
        } else {
            /* Still a join clause; add it, eliminating duplicates. */
            if !new_joininfo.contains(&rinfo) {
                new_joininfo.push(rinfo);
            }
        }
    }

    new_joininfo
}

/* ==========================================================================
 * fetch_upper_rel
 * ======================================================================== */

/// `fetch_upper_rel(root, kind, relids)` (relnode.c).
pub fn fetch_upper_rel(root: &mut PlannerInfo, kind: UpperRelationKind, relids: &Relids) -> RelId {
    /* If we already made this upperrel for the query, return it */
    for &upperrel in root.upper_rels[kind as usize].iter() {
        if bms::relids_equal::call(&root.rel(upperrel).relids, relids) {
            return upperrel;
        }
    }

    let upperrel = types_pathnodes::RelOptInfo {
        reloptkind: RELOPT_UPPER_REL,
        relids: bms::relids_copy::call(relids),
        consider_startup: root.tuple_fraction > 0.0,
        consider_param_startup: false,
        consider_parallel: false,
        reltarget: Some(alloc::boxed::Box::new(ext::create_empty_pathtarget::call())),
        ..Default::default()
    };
    let upperrel = root.alloc_rel(upperrel);

    root.upper_rels[kind as usize].push(upperrel);

    upperrel
}

/* ==========================================================================
 * find_childrel_parents
 * ======================================================================== */

/// `find_childrel_parents(root, rel)` (relnode.c).
pub fn find_childrel_parents(root: &PlannerInfo, rel: RelId) -> Relids {
    let mut result: Relids = None;

    debug_assert!(root.rel(rel).reloptkind == RELOPT_OTHER_MEMBER_REL);
    debug_assert!(
        root.rel(rel).relid > 0 && (root.rel(rel).relid as i32) < root.simple_rel_array_size
    );

    let mut cur = rel;
    loop {
        let relid = root.rel(cur).relid;
        let appinfo = root.append_rel_array[relid as usize]
            .as_ref()
            .expect("appinfo");
        let prelid = appinfo.parent_relid;

        result = bms::relids_add_member::call(result, prelid as i32);

        /* traverse up to the parent rel, loop if it's also a child rel */
        cur = find_base_rel(root, prelid as i32);

        if root.rel(cur).reloptkind != RELOPT_OTHER_MEMBER_REL {
            break;
        }
    }

    debug_assert!(root.rel(cur).reloptkind == RELOPT_BASEREL);

    result
}

/* ==========================================================================
 * get_baserel_parampathinfo
 * ======================================================================== */

/// `get_baserel_parampathinfo(root, baserel, required_outer)` (relnode.c).
pub fn get_baserel_parampathinfo<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    baserel: RelId,
    required_outer: &Relids,
) -> PgResult<Option<alloc::boxed::Box<ParamPathInfo>>> {
    /* If rel has LATERAL refs, every path should account for them */
    debug_assert!(bms::relids_is_subset::call(&root.rel(baserel).lateral_relids, required_outer));

    /* Unparameterized paths have no ParamPathInfo */
    if bms::relids_is_empty::call(required_outer) {
        return Ok(None);
    }

    debug_assert!(!bms::relids_overlap::call(&root.rel(baserel).relids, required_outer));

    /* If we already have a PPI for this parameterization, return it */
    if let Some(ppi) = find_param_path_info(root, baserel, required_outer) {
        return Ok(Some(alloc::boxed::Box::new(ppi)));
    }

    /*
     * Identify all joinclauses movable to this base rel given this param.
     */
    let joinrelids = bms::relids_union::call(&root.rel(baserel).relids, required_outer);
    let mut pclauses: Vec<RinfoId> = Vec::new();
    let baserel_relids = bms::relids_copy::call(&root.rel(baserel).relids);
    let baserel_joininfo = root.rel(baserel).joininfo.clone();
    for &rinfo in baserel_joininfo.iter() {
        if join_clause_is_movable_into_relids(root, rinfo, &baserel_relids, &joinrelids) {
            pclauses.push(rinfo);
        }
    }

    /*
     * Add in joinclauses generated by EquivalenceClasses.
     */
    let required_outer_copy = bms::relids_copy::call(required_outer);
    let joinrelids_copy = bms::relids_copy::call(&joinrelids);
    let eqclauses =
        equivclass::generate_join_implied_equalities::call(root, run, joinrelids_copy, required_outer_copy, baserel, None)?;

    #[cfg(debug_assertions)]
    {
        for &rinfo in eqclauses.iter() {
            debug_assert!(join_clause_is_movable_into_relids(root, rinfo, &baserel_relids, &joinrelids));
        }
    }
    pclauses.extend(eqclauses);

    /* Compute set of serial numbers of the enforced clauses */
    let mut pserials: Relids = None;
    for &rinfo in pclauses.iter() {
        pserials = bms::relids_add_member::call(pserials, root.rinfo(rinfo).rinfo_serial);
    }

    /* Estimate the number of rows returned by the parameterized scan */
    let rows = ext::get_parameterized_baserel_size::call(run, root, baserel, &pclauses);

    /* And now we can build the ParamPathInfo */
    let ppi = ParamPathInfo {
        ppi_req_outer: bms::relids_copy::call(required_outer),
        ppi_rows: rows,
        ppi_clauses: pclauses,
        ppi_serials: pserials,
    };
    root.rel_mut(baserel).ppilist.push(ppi.clone());

    Ok(Some(alloc::boxed::Box::new(ppi)))
}

/// `join_clause_is_movable_into(rinfo, currentrelids, current_and_required)`
/// (restrictinfo.c) — the C takes `Relids`, so we cross the relids-typed ext
/// seam (the existing `costsize` seam keys both sides by built-rel `RelId`,
/// which can't express a transient relid set like
/// `bms_union(baserel->relids, required_outer)`).
fn join_clause_is_movable_into_relids(
    root: &PlannerInfo,
    rinfo: RinfoId,
    current_relids: &Relids,
    join_relids: &Relids,
) -> bool {
    ext::join_clause_is_movable_into_relids::call(root, rinfo, current_relids, join_relids)
}

/* ==========================================================================
 * get_joinrel_parampathinfo
 * ======================================================================== */

/// `get_joinrel_parampathinfo(root, joinrel, outer_path, inner_path, sjinfo,
/// required_outer, *restrict_clauses)` (relnode.c).
pub fn get_joinrel_parampathinfo<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    joinrel: RelId,
    outer_path: types_pathnodes::PathId,
    inner_path: types_pathnodes::PathId,
    sjinfo: &SpecialJoinInfo,
    required_outer: &Relids,
    restrict_clauses: Vec<RinfoId>,
) -> PgResult<(Option<alloc::boxed::Box<ParamPathInfo>>, Vec<RinfoId>)> {
    /* If rel has LATERAL refs, every path should account for them */
    debug_assert!(bms::relids_is_subset::call(&root.rel(joinrel).lateral_relids, required_outer));

    /* Unparameterized paths have no ParamPathInfo or extra join clauses */
    if bms::relids_is_empty::call(required_outer) {
        return Ok((None, restrict_clauses));
    }

    debug_assert!(!bms::relids_overlap::call(&root.rel(joinrel).relids, required_outer));

    /*
     * Identify all joinclauses movable to this join rel given this param.
     */
    let join_and_req = bms::relids_union::call(&root.rel(joinrel).relids, required_outer);

    let outer_parent = root.path(outer_path).base().parent;
    let outer_param = root.path(outer_path).base().param_info.is_some();
    let outer_and_req: Relids = if outer_param {
        let req = path_req_outer(root, outer_path);
        bms::relids_union::call(&root.rel(outer_parent).relids, &req)
    } else {
        None
    };
    let inner_parent = root.path(inner_path).base().parent;
    let inner_param = root.path(inner_path).base().param_info.is_some();
    let inner_and_req: Relids = if inner_param {
        let req = path_req_outer(root, inner_path);
        bms::relids_union::call(&root.rel(inner_parent).relids, &req)
    } else {
        None
    };

    let joinrel_relids = bms::relids_copy::call(&root.rel(joinrel).relids);
    let outer_parent_relids = bms::relids_copy::call(&root.rel(outer_parent).relids);
    let inner_parent_relids = bms::relids_copy::call(&root.rel(inner_parent).relids);

    let mut pclauses: Vec<RinfoId> = Vec::new();
    let joinrel_joininfo = root.rel(joinrel).joininfo.clone();
    for &rinfo in joinrel_joininfo.iter() {
        if join_clause_is_movable_into_relids(root, rinfo, &joinrel_relids, &join_and_req)
            && !join_clause_is_movable_into_relids(root, rinfo, &outer_parent_relids, &outer_and_req)
            && !join_clause_is_movable_into_relids(root, rinfo, &inner_parent_relids, &inner_and_req)
        {
            pclauses.push(rinfo);
        }
    }

    /* Consider joinclauses generated by EquivalenceClasses, too */
    let req_copy = bms::relids_copy::call(required_outer);
    let join_and_req_copy = bms::relids_copy::call(&join_and_req);
    let eclauses = equivclass::generate_join_implied_equalities::call(
        root,
        run,
        join_and_req_copy,
        req_copy,
        joinrel,
        None,
    )?;
    /* We only want ones that aren't movable to lower levels */
    let mut dropped_ecs: Vec<types_pathnodes::EcId> = Vec::new();
    for &rinfo in eclauses.iter() {
        debug_assert!(join_clause_is_movable_into_relids(root, rinfo, &joinrel_relids, &join_and_req));
        if join_clause_is_movable_into_relids(root, rinfo, &outer_parent_relids, &outer_and_req) {
            continue; /* drop if movable into LHS */
        }
        if join_clause_is_movable_into_relids(root, rinfo, &inner_parent_relids, &inner_and_req) {
            /* drop if movable into RHS, but remember EC */
            debug_assert!(root.rinfo(rinfo).left_ec == root.rinfo(rinfo).right_ec);
            if let Some(ec) = root.rinfo(rinfo).left_ec {
                dropped_ecs.push(ec);
            }
            continue;
        }
        pclauses.push(rinfo);
    }

    /*
     * EquivalenceClass fixup for dropped ECs (see C comment).
     */
    if !dropped_ecs.is_empty() {
        let real_outer_and_req =
            bms::relids_union::call(&root.rel(outer_parent).relids, required_outer);
        let req_copy = bms::relids_copy::call(required_outer);
        let roar_copy = bms::relids_copy::call(&real_outer_and_req);
        let eclauses2 = equivclass::generate_join_implied_equalities_for_ecs::call(
            root,
            run,
            dropped_ecs,
            roar_copy,
            req_copy,
            outer_parent,
        )?;
        for &rinfo in eclauses2.iter() {
            debug_assert!(join_clause_is_movable_into_relids(
                root,
                rinfo,
                &outer_parent_relids,
                &real_outer_and_req
            ));
            if !join_clause_is_movable_into_relids(root, rinfo, &outer_parent_relids, &outer_and_req) {
                pclauses.push(rinfo);
            }
        }
    }

    /*
     * Attach the moved-down clauses to the caller's restrict_clauses list (in
     * front, leaving the original structure undamaged).
     */
    let mut restrict_clauses_out = pclauses;
    restrict_clauses_out.extend(restrict_clauses);

    /* If we already have a PPI for this parameterization, return it */
    if let Some(ppi) = find_param_path_info(root, joinrel, required_outer) {
        return Ok((Some(alloc::boxed::Box::new(ppi)), restrict_clauses_out));
    }

    /* Estimate the number of rows returned by the parameterized join */
    let rows = ext::get_parameterized_joinrel_size::call(
        run,
        root,
        joinrel,
        outer_path,
        inner_path,
        sjinfo,
        &restrict_clauses_out,
    );

    /* And now we can build the ParamPathInfo. */
    let ppi = ParamPathInfo {
        ppi_req_outer: bms::relids_copy::call(required_outer),
        ppi_rows: rows,
        ppi_clauses: Vec::new(),
        ppi_serials: None,
    };
    root.rel_mut(joinrel).ppilist.push(ppi.clone());

    Ok((Some(alloc::boxed::Box::new(ppi)), restrict_clauses_out))
}

/// `PATH_REQ_OUTER(path)` — `path->param_info ? path->param_info->ppi_req_outer
/// : NULL`.
fn path_req_outer(root: &PlannerInfo, path: types_pathnodes::PathId) -> Relids {
    match &root.path(path).base().param_info {
        Some(pi) => bms::relids_copy::call(&pi.ppi_req_outer),
        None => None,
    }
}

/* ==========================================================================
 * get_appendrel_parampathinfo
 * ======================================================================== */

/// `get_appendrel_parampathinfo(appendrel, required_outer)` (relnode.c).
pub fn get_appendrel_parampathinfo(
    root: &mut PlannerInfo,
    appendrel: RelId,
    required_outer: &Relids,
) -> PgResult<Option<alloc::boxed::Box<ParamPathInfo>>> {
    /* If rel has LATERAL refs, every path should account for them */
    debug_assert!(bms::relids_is_subset::call(&root.rel(appendrel).lateral_relids, required_outer));

    /* Unparameterized paths have no ParamPathInfo */
    if bms::relids_is_empty::call(required_outer) {
        return Ok(None);
    }

    debug_assert!(!bms::relids_overlap::call(&root.rel(appendrel).relids, required_outer));

    /* If we already have a PPI for this parameterization, return it */
    if let Some(ppi) = find_param_path_info(root, appendrel, required_outer) {
        return Ok(Some(alloc::boxed::Box::new(ppi)));
    }

    /* Else build the ParamPathInfo */
    let ppi = ParamPathInfo {
        ppi_req_outer: bms::relids_copy::call(required_outer),
        ppi_rows: 0.0,
        ppi_clauses: Vec::new(),
        ppi_serials: None,
    };
    root.rel_mut(appendrel).ppilist.push(ppi.clone());

    Ok(Some(alloc::boxed::Box::new(ppi)))
}

/* ==========================================================================
 * find_param_path_info
 * ======================================================================== */

/// `find_param_path_info(rel, required_outer)` (relnode.c).
pub fn find_param_path_info(
    root: &PlannerInfo,
    rel: RelId,
    required_outer: &Relids,
) -> Option<ParamPathInfo> {
    for ppi in root.rel(rel).ppilist.iter() {
        if bms::relids_equal::call(&ppi.ppi_req_outer, required_outer) {
            return Some(ppi.clone());
        }
    }
    None
}

/* ==========================================================================
 * get_param_path_clause_serials
 * ======================================================================== */

/// `get_param_path_clause_serials(path)` (relnode.c).
pub fn get_param_path_clause_serials(root: &PlannerInfo, path: types_pathnodes::PathId) -> Relids {
    if root.path(path).base().param_info.is_none() {
        return None; /* not parameterized */
    }

    let node = root.path(path);

    /* We don't support parameterized MergeAppend paths. */
    debug_assert!(!matches!(node, PathNode::MergeAppendPath(_)));

    match node {
        PathNode::NestPath(_) | PathNode::MergePath(_) | PathNode::HashPath(_) => {
            /*
             * For a join path, combine clauses enforced within either input path
             * with those enforced as joinrestrictinfo in this path.
             */
            let (outerjoinpath, innerjoinpath, joinrestrictinfo) = match node {
                PathNode::NestPath(p) => (
                    p.jpath.outerjoinpath,
                    p.jpath.innerjoinpath,
                    p.jpath.joinrestrictinfo.clone(),
                ),
                PathNode::MergePath(p) => (
                    p.jpath.outerjoinpath,
                    p.jpath.innerjoinpath,
                    p.jpath.joinrestrictinfo.clone(),
                ),
                PathNode::HashPath(p) => (
                    p.jpath.outerjoinpath,
                    p.jpath.innerjoinpath,
                    p.jpath.joinrestrictinfo.clone(),
                ),
                _ => unreachable!(),
            };

            let mut pserials: Relids = None;
            if let Some(op) = outerjoinpath {
                let s = get_param_path_clause_serials(root, op);
                pserials = bms::relids_add_members::call(pserials, &s);
            }
            if let Some(ip) = innerjoinpath {
                let s = get_param_path_clause_serials(root, ip);
                pserials = bms::relids_add_members::call(pserials, &s);
            }
            for rinfo in joinrestrictinfo {
                pserials = bms::relids_add_member::call(pserials, root.rinfo(rinfo).rinfo_serial);
            }
            pserials
        }
        PathNode::AppendPath(p) => {
            /*
             * For an appendrel, take the intersection of the sets of clauses
             * enforced in each input path.
             */
            let subpaths = p.subpaths.clone();
            let mut pserials: Relids = None;
            for (idx, &subpath) in subpaths.iter().enumerate() {
                let subserials = get_param_path_clause_serials(root, subpath);
                if idx == 0 {
                    pserials = bms::relids_copy::call(&subserials);
                } else {
                    pserials = bms::relids_int_members::call(pserials, &subserials);
                }
            }
            pserials
        }
        _ => {
            /*
             * Otherwise, it's a baserel path; use the precomputed serials.
             */
            bms::relids_copy::call(&root.path(path).base().param_info.as_ref().unwrap().ppi_serials)
        }
    }
}

/* ==========================================================================
 * build_joinrel_partition_info
 * ======================================================================== */

/// `build_joinrel_partition_info(root, joinrel, outer_rel, inner_rel, sjinfo,
/// restrictlist)` (relnode.c).
fn build_joinrel_partition_info(
    root: &mut PlannerInfo,
    joinrel: RelId,
    outer_rel: RelId,
    inner_rel: RelId,
    sjinfo: &SpecialJoinInfo,
    restrictlist: &[RinfoId],
) -> PgResult<()> {
    /* Nothing to do if partitionwise join technique is disabled. */
    if !ext::enable_partitionwise_join::call() {
        debug_assert!(!is_partitioned_rel(root.rel(joinrel)));
        return Ok(());
    }

    /*
     * We can only consider this join as input to further partitionwise joins
     * if (a) the inputs are partitioned with consider_partitionwise_join, (b)
     * the partition schemes match, and (c) there's a partition-key equi-join.
     */
    let outer_scheme = root.rel(outer_rel).part_scheme.clone();
    let inner_scheme = root.rel(inner_rel).part_scheme.clone();
    if outer_scheme.is_none()
        || inner_scheme.is_none()
        || !root.rel(outer_rel).consider_partitionwise_join
        || !root.rel(inner_rel).consider_partitionwise_join
        || outer_scheme != inner_scheme
        || !have_partkey_equi_join(root, joinrel, outer_rel, inner_rel, sjinfo.jointype, restrictlist)?
    {
        debug_assert!(!is_partitioned_rel(root.rel(joinrel)));
        return Ok(());
    }

    let part_scheme = outer_scheme;

    /*
     * Will be called only once for each joinrel; partitioning fields not yet
     * filled.
     */
    debug_assert!(
        root.rel(joinrel).part_scheme.is_none()
            && root.rel(joinrel).partexprs.is_empty()
            && root.rel(joinrel).nullable_partexprs.is_empty()
            && root.rel(joinrel).part_rels.is_empty()
            && root.rel(joinrel).boundinfo.is_none()
    );

    /*
     * If the join relation is partitioned, it uses the same scheme.
     */
    root.rel_mut(joinrel).part_scheme = part_scheme;
    set_joinrel_partition_key_exprs(root, joinrel, outer_rel, inner_rel, sjinfo.jointype)?;

    /* Set the consider_partitionwise_join flag. */
    debug_assert!(root.rel(outer_rel).consider_partitionwise_join);
    debug_assert!(root.rel(inner_rel).consider_partitionwise_join);
    root.rel_mut(joinrel).consider_partitionwise_join = true;

    Ok(())
}

/// `IS_PARTITIONED_REL(rel)` field-only conjuncts (pathnodes.h).
#[inline]
fn is_partitioned_rel(rel: &types_pathnodes::RelOptInfo) -> bool {
    rel.part_scheme.is_some()
        && rel.boundinfo.is_some()
        && rel.nparts > 0
        && !rel.part_rels.is_empty()
}

/* ==========================================================================
 * have_partkey_equi_join
 * ======================================================================== */

/// `have_partkey_equi_join(root, joinrel, rel1, rel2, jointype, restrictlist)`
/// (relnode.c).
fn have_partkey_equi_join(
    root: &PlannerInfo,
    joinrel: RelId,
    rel1: RelId,
    rel2: RelId,
    jointype: u32,
    restrictlist: &[RinfoId],
) -> PgResult<bool> {
    let part_scheme = root.rel(rel1).part_scheme.clone().expect("part_scheme");

    debug_assert!(root.rel(rel1).part_scheme == root.rel(rel2).part_scheme);

    /* track which partkey columns are known equal */
    let mut pk_known_equal = [false; PARTITION_MAX_KEYS];
    let mut num_equal_pks: i32 = 0;

    let joinrel_relids = bms::relids_copy::call(&root.rel(joinrel).relids);

    /* First, look through the join's restriction clauses */
    for &rinfo in restrictlist.iter() {
        let ri = root.rinfo(rinfo);

        /* If processing an outer join, only use its own join clauses. */
        if is_outer_join(jointype) && rinfo_is_pushed_down(root, rinfo, &joinrel_relids) {
            continue;
        }

        /* Skip clauses which can not be used for a join. */
        if !ri.can_join {
            continue;
        }

        /* Skip clauses which are not equality conditions. */
        if ri.mergeopfamilies.is_empty() && ri.hashjoinoperator == INVALID_OID {
            continue;
        }

        /* Should be OK to assume it's an OpExpr. */
        let opexpr = root
            .node(ri.clause)
            .expect_opexpr()
            .expect("castNode(OpExpr, rinfo->clause)")
            .clone();

        /* Match the operands to the relation. */
        let (mut expr1, mut expr2): (Expr, Expr);
        if bms::relids_is_subset::call(&ri.left_relids, &root.rel(rel1).relids)
            && bms::relids_is_subset::call(&ri.right_relids, &root.rel(rel2).relids)
        {
            expr1 = opexpr.args[0].clone();
            expr2 = opexpr.args[1].clone();
        } else if bms::relids_is_subset::call(&ri.left_relids, &root.rel(rel2).relids)
            && bms::relids_is_subset::call(&ri.right_relids, &root.rel(rel1).relids)
        {
            expr1 = opexpr.args[1].clone();
            expr2 = opexpr.args[0].clone();
        } else {
            continue;
        }

        /* Is the join operator strict? */
        let strict_op = lsyscache::op_strict::call(opexpr.opno)?;

        /*
         * Vars in the partition keys have no varnullingrels, but expr1/expr2 do
         * if we're above outer joins. OK to match if the operator is strict.
         */
        if strict_op {
            if bms::relids_overlap::call(&root.rel(rel1).relids, &root.outer_join_rels) {
                expr1 = nodefuncs::remove_nulling_relids::call(&expr1, &root.outer_join_rels, &None);
            }
            if bms::relids_overlap::call(&root.rel(rel2).relids, &root.outer_join_rels) {
                expr2 = nodefuncs::remove_nulling_relids::call(&expr2, &root.outer_join_rels, &None);
            }
        }

        /* Only clauses referencing the partition keys are useful. */
        let ipk1 = match_expr_to_partition_keys(root, &expr1, rel1, strict_op);
        if ipk1 < 0 {
            continue;
        }
        let ipk2 = match_expr_to_partition_keys(root, &expr2, rel2, strict_op);
        if ipk2 < 0 {
            continue;
        }

        /* If the clause refers to keys at different positions, skip. */
        if ipk1 != ipk2 {
            continue;
        }

        let ipk1u = ipk1 as usize;

        /* Ignore if we already proved these keys equal. */
        if pk_known_equal[ipk1u] {
            continue;
        }

        /* Reject if the partition key collation differs from the clause's. */
        if root.rel(rel1).part_scheme.as_ref().unwrap().partcollation[ipk1u] != opexpr.inputcollid {
            return Ok(false);
        }

        /*
         * The clause allows partitionwise join only if it uses the same
         * operator family as the partition key.
         */
        if part_scheme.strategy == PARTITION_STRATEGY_HASH {
            if ri.hashjoinoperator == INVALID_OID
                || !lsyscache::op_in_opfamily::call(ri.hashjoinoperator, part_scheme.partopfamily[ipk1u])?
            {
                continue;
            }
        } else if !ri.mergeopfamilies.contains(&part_scheme.partopfamily[ipk1u]) {
            continue;
        }

        /* Mark the partition key as having an equi-join clause. */
        pk_known_equal[ipk1u] = true;

        /* Stop once we prove all keys equal. */
        num_equal_pks += 1;
        if num_equal_pks == part_scheme.partnatts as i32 {
            return Ok(true);
        }
    }

    /*
     * Also check keys known equal by equivclass.c.
     */
    for ipk in 0..(part_scheme.partnatts as usize) {
        /* Ignore if we already proved these keys equal. */
        if pk_known_equal[ipk] {
            continue;
        }

        /*
         * We need a btree opfamily to ask equivclass.c about.
         */
        let btree_opfamily: types_core::primitive::Oid;
        if part_scheme.strategy == PARTITION_STRATEGY_HASH {
            let eq_op = lsyscache::get_opfamily_member::call(
                part_scheme.partopfamily[ipk],
                part_scheme.partopcintype[ipk],
                part_scheme.partopcintype[ipk],
                HTEqualStrategyNumber,
            )?;
            if eq_op == INVALID_OID {
                break; /* we're not going to succeed */
            }
            match ext::get_mergejoin_opfamilies_first::call(eq_op) {
                None => break, /* NIL: not going to succeed */
                Some(first) => btree_opfamily = first,
            }
        } else {
            btree_opfamily = part_scheme.partopfamily[ipk];
        }

        /*
         * We consider only non-nullable partition keys here.
         */
        let rel1_partexprs = root.rel(rel1).partexprs[ipk].clone();
        let rel2_partexprs = root.rel(rel2).partexprs[ipk].clone();
        let partcoll1 = root.rel(rel1).part_scheme.as_ref().unwrap().partcollation[ipk];

        let mut found = false;
        'outer: for &e1 in rel1_partexprs.iter() {
            let expr1 = root.node(e1).clone();
            let exprcoll1 = eq_ext::expr_collation::call(&expr1);

            for &e2 in rel2_partexprs.iter() {
                let expr2 = root.node(e2).clone();

                if equivclass::exprs_known_equal::call(root, expr1.clone(), expr2.clone(), btree_opfamily) {
                    /*
                     * Ensure the expression collation matches the partition key.
                     */
                    if partcoll1 == exprcoll1 {
                        #[cfg(debug_assertions)]
                        {
                            let partcoll2 =
                                root.rel(rel2).part_scheme.as_ref().unwrap().partcollation[ipk];
                            let exprcoll2 = eq_ext::expr_collation::call(&expr2);
                            debug_assert!(partcoll2 == exprcoll2);
                        }
                        pk_known_equal[ipk] = true;
                        found = true;
                        break 'outer;
                    }
                }
            }
            if pk_known_equal[ipk] {
                break;
            }
        }
        let _ = found;

        if pk_known_equal[ipk] {
            /* Stop once we prove all keys equal. */
            num_equal_pks += 1;
            if num_equal_pks == part_scheme.partnatts as i32 {
                return Ok(true);
            }
        } else {
            break; /* no chance to succeed, give up */
        }
    }

    Ok(false)
}

/* ==========================================================================
 * match_expr_to_partition_keys
 * ======================================================================== */

/// `match_expr_to_partition_keys(expr, rel, strict_op)` (relnode.c).
fn match_expr_to_partition_keys(
    root: &PlannerInfo,
    expr: &Expr,
    rel: RelId,
    strict_op: bool,
) -> i32 {
    /* Should be called only for partitioned relations. */
    debug_assert!(root.rel(rel).part_scheme.is_some());

    /* Remove any relabel decorations. */
    let mut cur = expr.clone();
    loop {
        match &cur {
            Expr::RelabelType(rt) => {
                cur = (*rt.arg.as_ref().expect("RelabelType.arg")).as_ref().clone();
            }
            _ => break,
        }
    }

    let partnatts = root.rel(rel).part_scheme.as_ref().unwrap().partnatts as usize;
    for cnt in 0..partnatts {
        /* We can always match to the non-nullable partition keys. */
        for &e in root.rel(rel).partexprs[cnt].iter() {
            if eq_ext::equal::call(root.node(e), &cur) {
                return cnt as i32;
            }
        }

        if !strict_op {
            continue;
        }

        /*
         * For a strict join operator, NULL partition keys won't join across
         * partitions, so it's OK to search the nullable partition keys too.
         */
        for &e in root.rel(rel).nullable_partexprs[cnt].iter() {
            if eq_ext::equal::call(root.node(e), &cur) {
                return cnt as i32;
            }
        }
    }

    -1
}

/* ==========================================================================
 * set_joinrel_partition_key_exprs
 * ======================================================================== */

/// `set_joinrel_partition_key_exprs(joinrel, outer_rel, inner_rel, jointype)`
/// (relnode.c).
fn set_joinrel_partition_key_exprs(
    root: &mut PlannerInfo,
    joinrel: RelId,
    outer_rel: RelId,
    inner_rel: RelId,
    jointype: u32,
) -> PgResult<()> {
    let partnatts = root.rel(joinrel).part_scheme.as_ref().unwrap().partnatts as usize;

    let mut joinrel_partexprs: Vec<Vec<NodeId>> = alloc::vec![Vec::new(); partnatts];
    let mut joinrel_nullable_partexprs: Vec<Vec<NodeId>> = alloc::vec![Vec::new(); partnatts];

    for cnt in 0..partnatts {
        let outer_expr = root.rel(outer_rel).partexprs[cnt].clone();
        let outer_null_expr = root.rel(outer_rel).nullable_partexprs[cnt].clone();
        let inner_expr = root.rel(inner_rel).partexprs[cnt].clone();
        let inner_null_expr = root.rel(inner_rel).nullable_partexprs[cnt].clone();
        #[allow(unused_assignments)]
        let mut partexpr: Vec<NodeId> = Vec::new();
        #[allow(unused_assignments)]
        let mut nullable_partexpr: Vec<NodeId> = Vec::new();

        match jointype {
            JOIN_INNER => {
                partexpr = list_concat_copy(&outer_expr, &inner_expr);
                nullable_partexpr = list_concat_copy(&outer_null_expr, &inner_null_expr);
            }
            JOIN_SEMI | JOIN_ANTI => {
                partexpr = outer_expr.clone();
                nullable_partexpr = outer_null_expr.clone();
            }
            JOIN_LEFT => {
                partexpr = outer_expr.clone();
                nullable_partexpr = list_concat_copy(&inner_expr, &outer_null_expr);
                nullable_partexpr.extend(inner_null_expr.iter().cloned());
            }
            JOIN_FULL => {
                nullable_partexpr = list_concat_copy(&outer_expr, &inner_expr);
                nullable_partexpr.extend(outer_null_expr.iter().cloned());
                nullable_partexpr.extend(inner_null_expr.iter().cloned());

                /*
                 * Add CoalesceExprs for each possible full-join output variable.
                 */
                let largs = list_concat_copy(&outer_expr, &outer_null_expr);
                let rargs = list_concat_copy(&inner_expr, &inner_null_expr);
                for &larg in largs.iter() {
                    for &rarg in rargs.iter() {
                        let larg_expr = root.node(larg).clone();
                        let rarg_expr = root.node(rarg).clone();
                        let c = ext::make_coalesce_expr::call(&larg_expr, &rarg_expr);
                        let cid = root.alloc_node(c);
                        nullable_partexpr.push(cid);
                    }
                }
            }
            other => {
                panic!("unrecognized join type: {}", other);
            }
        }

        joinrel_partexprs[cnt] = partexpr;
        joinrel_nullable_partexprs[cnt] = nullable_partexpr;
    }

    root.rel_mut(joinrel).partexprs = joinrel_partexprs;
    root.rel_mut(joinrel).nullable_partexprs = joinrel_nullable_partexprs;

    Ok(())
}

/// `list_concat_copy(a, b)` — a new list = a ++ b (copies, leaving inputs).
fn list_concat_copy(a: &[NodeId], b: &[NodeId]) -> Vec<NodeId> {
    let mut out = Vec::with_capacity(a.len() + b.len());
    out.extend_from_slice(a);
    out.extend_from_slice(b);
    out
}

/* ==========================================================================
 * build_child_join_reltarget
 * ======================================================================== */

/// `build_child_join_reltarget(root, parentrel, childrel, nappinfos, appinfos)`
/// (relnode.c).
fn build_child_join_reltarget(
    root: &mut PlannerInfo,
    parentrel: RelId,
    childrel: RelId,
    appinfos: &[AppendRelInfo],
) -> PgResult<()> {
    /* Build the targetlist */
    let parent_exprs = root.rel(parentrel).reltarget.as_ref().unwrap().exprs.clone();
    let mut child_exprs: Vec<NodeId> = Vec::with_capacity(parent_exprs.len());
    for e in parent_exprs {
        let node = root.node(e).clone();
        let adjusted = ext::adjust_appendrel_attrs_node::call(root, node, appinfos)?;
        child_exprs.push(root.alloc_node(adjusted));
    }

    /* Set the cost and width fields */
    let cost = root.rel(parentrel).reltarget.as_ref().unwrap().cost.clone();
    let width = root.rel(parentrel).reltarget.as_ref().unwrap().width;

    let target = root.rel_mut(childrel).reltarget.as_mut().unwrap();
    target.exprs = child_exprs;
    target.cost = cost;
    target.width = width;

    Ok(())
}

/* ==========================================================================
 * Bitmapset ops (nodes/bitmapset.c) owned by the relnode-seams crate.
 *
 * relnode-seams bundles the planner's `bms_*` set algebra over `Relids`
 * (`nodes/bitmapset.c` is not a separate ported unit in this repo). The
 * canonical lifetime-free `Relids = Option<Box<Bitmapset>>` with the empty set
 * represented as `None`. These six ops are the relnode-seams inward seams that
 * relnode does not itself consume; they are implemented here field-for-field
 * vs bitmapset.c and installed by `init_seams`.
 * ======================================================================== */

use types_pathnodes::Bitmapset;

/// `BITS_PER_BITMAPWORD` — width of a `bitmapword` (`uint64`).
const BITS_PER_BITMAPWORD: i32 = 64;

#[inline]
fn wordnum(x: i32) -> usize {
    (x / BITS_PER_BITMAPWORD) as usize
}
#[inline]
fn bitnum(x: i32) -> i32 {
    x % BITS_PER_BITMAPWORD
}
#[inline]
fn bmw_rightmost_one_pos(w: u64) -> i32 {
    w.trailing_zeros() as i32
}
#[inline]
fn has_multiple_ones(w: u64) -> bool {
    (w & (w.wrapping_sub(1))) != 0
}

/// `bms_next_member(a, prevbit)` (bitmapset.c) — next member > prevbit, or -2.
fn bms_next_member(a: &Relids, prevbit: i32) -> i32 {
    let a = match a {
        None => return -2,
        Some(a) => a,
    };
    let nwords = a.words.len();
    let prevbit = prevbit + 1;
    let mut mask: u64 = (!0u64) << bitnum(prevbit);
    let mut wnum = wordnum(prevbit);
    while wnum < nwords {
        let w = a.words[wnum] & mask;
        if w != 0 {
            return wnum as i32 * BITS_PER_BITMAPWORD + bmw_rightmost_one_pos(w);
        }
        mask = !0u64;
        wnum += 1;
    }
    -2
}

/// `bms_get_singleton_member(a, &member)` (bitmapset.c) — `Some(member)` iff
/// `a` is a singleton, else `None`.
fn bms_get_singleton_member(a: &Relids) -> Option<i32> {
    let a = a.as_ref()?;
    let mut result: i32 = -1;
    for (wnum, &w) in a.words.iter().enumerate() {
        if w != 0 {
            if result >= 0 || has_multiple_ones(w) {
                return None;
            }
            result = wnum as i32 * BITS_PER_BITMAPWORD + bmw_rightmost_one_pos(w);
        }
    }
    if result < 0 {
        None
    } else {
        Some(result)
    }
}

/// `bms_singleton_member(a)` (bitmapset.c) — the single member of a one-element
/// set (caller has established `a` is a singleton).
fn bms_singleton_member(a: &Relids) -> i32 {
    let a = match a {
        None => panic!("bitmapset is empty"),
        Some(a) => a,
    };
    let mut result: i32 = -1;
    for (wnum, &w) in a.words.iter().enumerate() {
        if w != 0 {
            if result >= 0 || has_multiple_ones(w) {
                panic!("bitmapset has multiple members");
            }
            result = wnum as i32 * BITS_PER_BITMAPWORD + bmw_rightmost_one_pos(w);
        }
    }
    result
}

/// `bms_membership(a)` (bitmapset.c) — `BMS_EMPTY_SET`(0)/`BMS_SINGLETON`(1)/
/// `BMS_MULTIPLE`(2).
fn bms_membership(a: &Relids) -> i32 {
    let a = match a {
        None => return 0,
        Some(a) => a,
    };
    let mut result = 0; /* BMS_EMPTY_SET */
    for &w in a.words.iter() {
        if w != 0 {
            if result != 0 || has_multiple_ones(w) {
                return 2; /* BMS_MULTIPLE */
            }
            result = 1; /* BMS_SINGLETON */
        }
    }
    result
}

/// Normalize: drop trailing all-zero words; empty → `None` (canonical form).
#[inline]
fn bms_normalize(mut words: Vec<u64>) -> Relids {
    while let Some(&last) = words.last() {
        if last == 0 {
            words.pop();
        } else {
            break;
        }
    }
    if words.is_empty() {
        None
    } else {
        Some(alloc::boxed::Box::new(Bitmapset { words }))
    }
}

/// `bms_difference(a, b)` (bitmapset.c) — a fresh set `a \ b`.
fn bms_difference(a: &Relids, b: &Relids) -> Relids {
    let a = match a {
        None => return None,
        Some(a) => a,
    };
    let b = match b {
        None => return Some(alloc::boxed::Box::new(Bitmapset { words: a.words.clone() })),
        Some(b) => b,
    };
    /* Apply relevant mask to each word; words past b stay as-is. */
    let mut result = a.words.clone();
    let n = core::cmp::min(result.len(), b.words.len());
    for i in 0..n {
        result[i] &= !b.words[i];
    }
    bms_normalize(result)
}

/// `bms_add_range(a, lower, upper)` (bitmapset.c) — add `lower..=upper` to `a`
/// (recycled), returning the result.
fn bms_add_range(a: Relids, lower: i32, upper: i32) -> Relids {
    if upper < lower {
        return a;
    }
    if lower < 0 {
        panic!("negative bitmapset member not allowed");
    }
    let uwordnum = wordnum(upper);
    let mut words = match a {
        None => alloc::vec![0u64; uwordnum + 1],
        Some(b) => {
            let mut w = b.words;
            if uwordnum >= w.len() {
                w.resize(uwordnum + 1, 0);
            }
            w
        }
    };

    let mut lwordnum = wordnum(lower);
    let lbitnum = bitnum(lower);
    let ubitnum = bitnum(upper);

    if lwordnum == uwordnum {
        /* All bits in a single word. */
        let mask = ((!0u64) << lbitnum) & ((!0u64) >> (BITS_PER_BITMAPWORD - 1 - ubitnum));
        words[lwordnum] |= mask;
    } else {
        /* First word: bits from lbitnum up. */
        words[lwordnum] |= (!0u64) << lbitnum;
        lwordnum += 1;
        /* Full words in between. */
        while lwordnum < uwordnum {
            words[lwordnum] = !0u64;
            lwordnum += 1;
        }
        /* Last word: bits up to ubitnum. */
        words[uwordnum] |= (!0u64) >> (BITS_PER_BITMAPWORD - 1 - ubitnum);
    }

    Some(alloc::boxed::Box::new(Bitmapset { words }))
}

/// `bms_copy(a)` (bitmapset.c) — a fresh copy of set `a` (empty → empty).
fn bms_copy(a: &Relids) -> Relids {
    a.as_ref()
        .map(|a| alloc::boxed::Box::new(Bitmapset { words: a.words.clone() }))
}

/// `bms_equal(a, b)` (bitmapset.c) — `a` and `b` contain the same members. In
/// this normalized model trailing-zero words are always trimmed, so equal sets
/// have equal word vectors.
fn bms_equal(a: &Relids, b: &Relids) -> bool {
    match (a, b) {
        (None, None) => true,
        (None, Some(_)) | (Some(_), None) => false,
        (Some(a), Some(b)) => a.words == b.words,
    }
}

/// `bms_make_singleton(x)` (bitmapset.c) — a fresh set `{x}`.
fn bms_make_singleton(x: i32) -> Relids {
    if x < 0 {
        panic!("negative bitmapset member not allowed");
    }
    let wnum = wordnum(x);
    let bnum = bitnum(x);
    let mut words = alloc::vec![0u64; wnum + 1];
    words[wnum] = 1u64 << bnum;
    Some(alloc::boxed::Box::new(Bitmapset { words }))
}

/// `bms_is_member(x, a)` (bitmapset.c) — `x` is present in `a`.
fn bms_is_member(x: i32, a: &Relids) -> bool {
    if x < 0 {
        panic!("negative bitmapset member not allowed");
    }
    let a = match a {
        None => return false,
        Some(a) => a,
    };
    let wnum = wordnum(x);
    if wnum >= a.words.len() {
        return false;
    }
    (a.words[wnum] & (1u64 << bitnum(x))) != 0
}

/// `bms_is_empty(a)` (bitmapset.c) — `a` has no members (canonically `None`).
fn bms_is_empty(a: &Relids) -> bool {
    a.is_none()
}

/// `bms_num_members(a)` (bitmapset.c) — number of members in `a`.
fn bms_num_members(a: &Relids) -> i32 {
    match a {
        None => 0,
        Some(a) => a.words.iter().map(|&w| w.count_ones() as i32).sum(),
    }
}

/// `bms_union(a, b)` (bitmapset.c) — a fresh set `a ∪ b`.
fn bms_union(a: &Relids, b: &Relids) -> Relids {
    let (a, b) = match (a, b) {
        (None, _) => return bms_copy(b),
        (_, None) => return bms_copy(a),
        (Some(a), Some(b)) => (a, b),
    };
    // Copy the longer one; union the shorter into it.
    let (mut result, other) = if a.words.len() <= b.words.len() {
        (b.words.clone(), a)
    } else {
        (a.words.clone(), b)
    };
    for (i, &w) in other.words.iter().enumerate() {
        result[i] |= w;
    }
    // A union of non-empty sets is non-empty and already trailing-trimmed.
    Some(alloc::boxed::Box::new(Bitmapset { words: result }))
}

/// `bms_intersect(a, b)` (bitmapset.c) — a fresh set `a ∩ b`.
fn bms_intersect(a: &Relids, b: &Relids) -> Relids {
    let (a, b) = match (a, b) {
        (None, _) | (_, None) => return None,
        (Some(a), Some(b)) => (a, b),
    };
    // Copy the shorter one; intersect the longer into it.
    let (shorter, longer) = if a.words.len() <= b.words.len() {
        (a, b)
    } else {
        (b, a)
    };
    let mut result = shorter.words.clone();
    for i in 0..result.len() {
        result[i] &= longer.words[i];
    }
    bms_normalize(result)
}

/// `bms_add_member(a, x)` (bitmapset.c) — add `x` to `a` (recycled).
fn bms_add_member(a: Relids, x: i32) -> Relids {
    if x < 0 {
        panic!("negative bitmapset member not allowed");
    }
    let wnum = wordnum(x);
    let bnum = bitnum(x);
    let mut words = match a {
        None => return bms_make_singleton(x),
        Some(b) => b.words,
    };
    if wnum >= words.len() {
        words.resize(wnum + 1, 0);
    }
    words[wnum] |= 1u64 << bnum;
    Some(alloc::boxed::Box::new(Bitmapset { words }))
}

/// `bms_add_members(a, b)` (bitmapset.c) — add all of `b`'s members to `a`
/// (recycled), returning `a ∪ b`.
fn bms_add_members(a: Relids, b: &Relids) -> Relids {
    let mut words = match a {
        None => return bms_copy(b),
        Some(b) => b.words,
    };
    let b = match b {
        None => return Some(alloc::boxed::Box::new(Bitmapset { words })),
        Some(b) => b,
    };
    if b.words.len() > words.len() {
        words.resize(b.words.len(), 0);
    }
    for (i, &w) in b.words.iter().enumerate() {
        words[i] |= w;
    }
    Some(alloc::boxed::Box::new(Bitmapset { words }))
}

/// `bms_int_members(a, b)` (bitmapset.c) — set `a` to `a ∩ b` (recycled).
fn bms_int_members(a: Relids, b: &Relids) -> Relids {
    let words = match a {
        None => return None,
        Some(a) => a.words,
    };
    let b = match b {
        None => return None,
        Some(b) => b,
    };
    let shortlen = core::cmp::min(words.len(), b.words.len());
    let mut result = words;
    for i in 0..shortlen {
        result[i] &= b.words[i];
    }
    // Anything past the shorter length intersects to empty.
    result.truncate(shortlen);
    bms_normalize(result)
}

/// `bms_join(a, b)` (bitmapset.c) — union of `a` and `b`, recycling both. With
/// owned (non-aliased) `Relids` this is just `bms_add_members(a, &b)`.
fn bms_join(a: Relids, b: Relids) -> Relids {
    bms_add_members(a, &b)
}

/// `bms_is_subset(a, b)` (bitmapset.c) — every member of `a` is in `b`.
fn bms_is_subset(a: &Relids, b: &Relids) -> bool {
    let a = match a {
        None => return true, // empty set is a subset of anything
        Some(a) => a,
    };
    let b = match b {
        None => return false,
        Some(b) => b,
    };
    // 'a' can't be a subset of 'b' if it contains more words (sets are
    // normalized, so extra words in 'a' carry real members).
    if a.words.len() > b.words.len() {
        return false;
    }
    for i in 0..a.words.len() {
        if (a.words[i] & !b.words[i]) != 0 {
            return false;
        }
    }
    true
}

/// `bms_overlap(a, b)` (bitmapset.c) — `a` and `b` share a member.
fn bms_overlap(a: &Relids, b: &Relids) -> bool {
    let (a, b) = match (a, b) {
        (None, _) | (_, None) => return false,
        (Some(a), Some(b)) => (a, b),
    };
    let shortlen = core::cmp::min(a.words.len(), b.words.len());
    for i in 0..shortlen {
        if (a.words[i] & b.words[i]) != 0 {
            return true;
        }
    }
    false
}

/// `bms_nonempty_difference(a, b)` (bitmapset.c) — `a \ b` is non-empty.
fn bms_nonempty_difference(a: &Relids, b: &Relids) -> bool {
    let a = match a {
        None => return false,
        Some(a) => a,
    };
    let b = match b {
        None => return true,
        Some(b) => b,
    };
    if a.words.len() > b.words.len() {
        return true;
    }
    for i in 0..a.words.len() {
        if (a.words[i] & !b.words[i]) != 0 {
            return true;
        }
    }
    false
}

/* ==========================================================================
 * Seam installation
 * ======================================================================== */

/// Install the relnode-owned seams. relnode is the OWNER of these.
pub fn init_seams() {
    bms::find_base_rel::set(seam_find_base_rel);
    bms::find_join_rel::set(seam_find_join_rel);
    bms::build_join_rel::set(seam_build_join_rel);
    bms::build_child_join_rel::set(seam_build_child_join_rel);
    bms::min_join_parameterization::set(seam_min_join_parameterization);

    pathnode::get_baserel_parampathinfo::set(seam_get_baserel_parampathinfo);
    pathnode::get_appendrel_parampathinfo::set(seam_get_appendrel_parampathinfo);
    pathnode::get_joinrel_parampathinfo::set(seam_get_joinrel_parampathinfo);
    pathnode::get_param_path_clause_serials::set(seam_get_param_path_clause_serials);

    eq_ext::find_childrel_parents::set(seam_find_childrel_parents);

    /* relnode-seams bitmapset ops relnode owns but does not itself consume. */
    bms::relids_next_member::set(bms_next_member);
    bms::relids_get_singleton_member::set(bms_get_singleton_member);
    bms::relids_singleton_member::set(bms_singleton_member);
    bms::relids_membership::set(bms_membership);
    bms::relids_difference::set(bms_difference);
    bms::relids_add_range::set(bms_add_range);
    bms::relids_copy::set(bms_copy);
    bms::relids_equal::set(bms_equal);
    bms::relids_make_singleton::set(bms_make_singleton);
    bms::relids_is_member::set(bms_is_member);
    bms::relids_is_empty::set(bms_is_empty);
    bms::relids_num_members::set(bms_num_members);
    bms::relids_union::set(bms_union);
    bms::relids_intersect::set(bms_intersect);
    bms::relids_add_member::set(bms_add_member);
    bms::relids_add_members::set(bms_add_members);
    bms::relids_int_members::set(bms_int_members);
    bms::relids_join::set(bms_join);
    bms::relids_is_subset::set(bms_is_subset);
    bms::relids_overlap::set(bms_overlap);
    bms::relids_nonempty_difference::set(bms_nonempty_difference);
}

fn seam_find_base_rel(root: &PlannerInfo, relid: i32) -> RelId {
    find_base_rel(root, relid)
}
fn seam_find_join_rel(root: &PlannerInfo, relids: &Relids) -> Option<RelId> {
    find_join_rel(root, relids)
}
fn seam_build_join_rel<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    joinrelids: &Relids,
    outer_rel: RelId,
    inner_rel: RelId,
    sjinfo: &SpecialJoinInfo,
    pushed_down_joins: &[SpecialJoinInfo],
) -> PgResult<(RelId, Vec<RinfoId>)> {
    build_join_rel(root, run, joinrelids, outer_rel, inner_rel, sjinfo, pushed_down_joins)
}
fn seam_build_child_join_rel<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    outer_rel: RelId,
    inner_rel: RelId,
    parent_joinrel: RelId,
    restrictlist: &[RinfoId],
    sjinfo: &SpecialJoinInfo,
    appinfos: &[AppendRelInfo],
) -> PgResult<RelId> {
    build_child_join_rel(run, root, outer_rel, inner_rel, parent_joinrel, restrictlist, sjinfo, appinfos)
}
fn seam_min_join_parameterization(
    root: &PlannerInfo,
    joinrelids: &Relids,
    outer_rel: RelId,
    inner_rel: RelId,
) -> Relids {
    min_join_parameterization(root, joinrelids, outer_rel, inner_rel)
}
fn seam_get_baserel_parampathinfo<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    baserel: RelId,
    required_outer: &Relids,
) -> Option<alloc::boxed::Box<ParamPathInfo>> {
    get_baserel_parampathinfo(root, run, baserel, required_outer).expect("get_baserel_parampathinfo")
}
fn seam_get_appendrel_parampathinfo(
    root: &mut PlannerInfo,
    appendrel: RelId,
    required_outer: &Relids,
) -> Option<alloc::boxed::Box<ParamPathInfo>> {
    get_appendrel_parampathinfo(root, appendrel, required_outer)
        .expect("get_appendrel_parampathinfo")
}
fn seam_get_joinrel_parampathinfo<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    joinrel: RelId,
    outer_path: types_pathnodes::PathId,
    inner_path: types_pathnodes::PathId,
    sjinfo: &SpecialJoinInfo,
    required_outer: &Relids,
    restrict_clauses: Vec<RinfoId>,
) -> (Option<alloc::boxed::Box<ParamPathInfo>>, Vec<RinfoId>) {
    get_joinrel_parampathinfo(
        root,
        run,
        joinrel,
        outer_path,
        inner_path,
        sjinfo,
        required_outer,
        restrict_clauses,
    )
    .expect("get_joinrel_parampathinfo")
}
fn seam_get_param_path_clause_serials(root: &PlannerInfo, path: types_pathnodes::PathId) -> Relids {
    get_param_path_clause_serials(root, path)
}
fn seam_find_childrel_parents(root: &PlannerInfo, rel: RelId) -> Relids {
    find_childrel_parents(root, rel)
}
