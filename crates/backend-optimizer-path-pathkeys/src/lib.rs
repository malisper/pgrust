#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::collapsible_else_if)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::needless_late_init)]

//! Safe-Rust port of `src/backend/optimizer/path/pathkeys.c` (postgres-18.3):
//! the planner's pathkey engine — pathkey construction, matching, redundancy and
//! usefulness checks, over the planner arena ([`types_pathnodes::PlannerInfo`]).
//!
//! pathkeys.c is built on the **pointer identity of canonical `PathKey` /
//! `EquivalenceClass` objects** (PostgreSQL interns one canonical `PathKey` per
//! `(eclass, opfamily, cmptype, nulls_first)` tuple and one `EquivalenceClass`
//! per equivalence set, then compares with `==` on those shared pointers). The
//! arena models that faithfully: `PathKey.pk_eclass` /
//! `RestrictInfo.left_ec`/`right_ec` / `EquivalenceClass.ec_merged` are
//! [`Option<EcId>`](types_pathnodes::EcId) handles into
//! [`PlannerInfo::eq_classes`]; [`PlannerInfo::ec_canonical`] chases the
//! `ec_merged` union-find, so "same canonical EC" is a handle `==`. Canonical
//! `PathKey`s are still interned in [`PlannerInfo::canon_pathkeys`]
//! ([`make_canonical_pathkey`] reuses an existing entry); a canonical pathkey is
//! uniquely determined by its four scalar fields, so a field-wise `PathKey`
//! equality is the faithful analogue of C's `pathkey1 == pathkey2`.
//!
//! # Arena / handle model
//!
//! Expression nodes (`Expr *`) are [`NodeId`](types_pathnodes::NodeId) handles
//! into [`PlannerInfo::node_arena`] (`Vec<Expr>`); `root.node(id) -> &Expr`.
//! `RelOptInfo`/`Path`/`RestrictInfo`/`EquivalenceMember` are reached by
//! `RelId`/`PathId`/`RinfoId`/`EmId` handles. Allocating functions return a
//! `Vec<PathKey>` (the C `palloc`'d list); the join-path / pathnode seams that
//! consume them wrap the list in [`PgResult`](types_error::PgResult) (the C OOM
//! channel), filled at the seam install boundary in [`init_seams`].
//!
//! # Cross-subsystem seams
//!
//! Everything crossing a subsystem boundary crosses a seam: the `relids_*` set
//! algebra (relnode-seams), cost comparison (joinpath/pathnode-seams),
//! lsyscache catalog lookups, nodeFuncs/equalfuncs/copyfuncs/tlist node
//! inspections, and — for the find/create of EquivalenceClasses over a sort
//! expression — `get_eclass_for_sort_expr` / `canonicalize_ec_expression` /
//! `eclass_useful_for_merging` in the **not-yet-ported** `equivclass.c`
//! ([`backend_optimizer_path_equivclass_seams`], panic-until-owner).
//!
//! ## A note on the `TargetEntry` / `SortGroupClause` node-payload reads
//!
//! The current `node_arena` element type is `Expr`, which has no `TargetEntry`
//! or `SortGroupClause` variant. The functions that walk a target list or a
//! sort/group-clause list (`build_index_pathkeys`,
//! `make_pathkeys_for_sortclauses[_extended]`, `convert_subquery_pathkeys`,
//! `find_var_for_subquery_tle`, `group_keys_reorder_by_pathkeys`,
//! `get_useful_group_keys_orderings`) therefore read those node payloads through
//! nodeFuncs/tlist seams keyed by `NodeId` + `&PlannerInfo` (the owner resolves
//! the handle in its richer node model). The pathkey control flow itself is
//! ported here 1:1; only the unmodellable node-payload reads are delegated.

extern crate alloc;

use alloc::vec::Vec;

use types_core::primitive::{Index, Oid};
use types_pathnodes::{
    optimizer_plan::CostSelector, CompareType, EcId, GroupByOrdering, IndexOptInfo, NodeId, PathId,
    PathKey, PlannerInfo, RelId, RelOptInfo, Relids, RestrictInfo, RinfoId, ScanDirection,
    BackwardScanDirection, RELOPT_BASEREL,
};

use backend_optimizer_util_pathnode_seams::PathKeysComparison;

use backend_optimizer_path_equivclass_seams as ec;
use backend_optimizer_path_equivclass as equivclass;
use backend_optimizer_util_relnode_seams as bms;
use backend_nodes_nodeFuncs_seams as nf;
use backend_utils_cache_lsyscache_seams as lsc;

mod install;
#[cfg(test)]
mod tests;

pub use install::init_seams;

// `CompareType` constants (access/cmptype.h). pathkeys.c uses COMPARE_LT /
// COMPARE_GT / COMPARE_EQ; `CompareType` is the `i32` alias in `types_pathnodes`.
const COMPARE_LT: CompareType = 1;
const COMPARE_EQ: CompareType = 3;
const COMPARE_GT: CompareType = 5;

// JoinType constants used by build_join_pathkeys (pathnodes/nodes.h values).
use types_pathnodes::{JoinType, JOIN_FULL, JOIN_RIGHT, JOIN_RIGHT_ANTI, JOIN_RIGHT_SEMI};

// ===========================================================================
// Canonical-pathkey identity helpers (the arena analogue of C pointer ==).
// ===========================================================================

/// `while (eclass->ec_merged) eclass = eclass->ec_merged;` — chase the
/// `ec_merged` union-find to the canonical EC, path-compressing. `PlannerInfo`
/// exposes `ec(id)` but no canonicalize accessor, so the chase lives here over
/// the public `eq_classes` field.
fn ec_canonical(root: &mut PlannerInfo, mut ec: EcId) -> EcId {
    // Find the canonical root.
    let mut canon = ec;
    while let Some(next) = root.eq_classes[canon.index()].ec_merged {
        canon = next;
    }
    // Path-compress the chain to point straight at the canonical EC.
    while ec != canon {
        let next = match root.eq_classes[ec.index()].ec_merged {
            Some(n) => n,
            None => break,
        };
        root.eq_classes[ec.index()].ec_merged = Some(canon);
        ec = next;
    }
    canon
}

/// Field-wise equality of two canonical `PathKey`s — the faithful arena analogue
/// of C's `pathkey1 == pathkey2` (a pointer compare on interned canonical
/// pathkeys). `pk_eclass` is compared as the canonical `EcId` handle.
#[inline]
fn pathkeys_equal_pk(a: &PathKey, b: &PathKey) -> bool {
    a.pk_eclass == b.pk_eclass
        && a.pk_opfamily == b.pk_opfamily
        && a.pk_cmptype == b.pk_cmptype
        && a.pk_nulls_first == b.pk_nulls_first
}

/// `list_member_ptr(list, pathkey)` over a canonical pathkey list.
#[inline]
fn list_member_pk(list: &[PathKey], pathkey: &PathKey) -> bool {
    list.iter().any(|pk| pathkeys_equal_pk(pk, pathkey))
}

/// `PATH_REQ_OUTER(path)` (pathnodes.h) — `path->param_info ?
/// param_info->ppi_req_outer : NULL`.
#[inline]
fn path_req_outer(root: &PlannerInfo, path: PathId) -> Relids {
    match &root.path(path).base().param_info {
        Some(ppi) => bms::relids_copy::call(&ppi.ppi_req_outer),
        None => None,
    }
}

// ===========================================================================
// PATHKEY CONSTRUCTION AND REDUNDANCY TESTING
// ===========================================================================

/// `make_canonical_pathkey`
///   Given the parameters for a PathKey, find any pre-existing matching pathkey
///   in the query's list of "canonical" pathkeys. Make a new entry if there's
///   not one already.
///
/// Must not be used until after EquivalenceClass merging is complete.
pub fn make_canonical_pathkey(
    root: &mut PlannerInfo,
    eclass: EcId,
    opfamily: Oid,
    cmptype: CompareType,
    nulls_first: bool,
) -> PathKey {
    // Can't make canonical pathkeys if the set of ECs might still change.
    if !root.ec_merging_done {
        panic!("too soon to build canonical pathkeys");
    }

    // The passed eclass might be non-canonical, so chase up to the top.
    let eclass = ec_canonical(root, eclass);

    for pk in &root.canon_pathkeys {
        if Some(eclass) == pk.pk_eclass
            && opfamily == pk.pk_opfamily
            && cmptype == pk.pk_cmptype
            && nulls_first == pk.pk_nulls_first
        {
            return pk.clone();
        }
    }

    // (MemoryContextSwitchTo(root->planner_cxt) is a no-op in the owned model.)
    let pk = PathKey {
        pk_eclass: Some(eclass),
        pk_opfamily: opfamily,
        pk_cmptype: cmptype,
        pk_nulls_first: nulls_first,
    };

    root.canon_pathkeys.push(pk.clone());

    pk
}

/// `append_pathkeys`
///   Append all non-redundant PathKeys in `source` onto `target` and return the
///   updated `target` list.
pub fn append_pathkeys(
    root: &PlannerInfo,
    mut target: Vec<PathKey>,
    source: &[PathKey],
) -> Vec<PathKey> {
    // Assert(target != NIL) is a C debug-only assertion; not enforced.
    for pk in source {
        if !pathkey_is_redundant(root, pk, &target) {
            target.push(pk.clone());
        }
    }
    target
}

/// `pathkey_is_redundant`
///   Is a pathkey redundant with one already in the given list?
pub fn pathkey_is_redundant(
    root: &PlannerInfo,
    new_pathkey: &PathKey,
    pathkeys: &[PathKey],
) -> bool {
    let new_ec = new_pathkey
        .pk_eclass
        .expect("canonical PathKey must have an EquivalenceClass");

    // Check for EC containing a constant --- unconditionally redundant.
    // EC_MUST_BE_REDUNDANT(eclass) == eclass->ec_has_const.
    if root.ec(new_ec).ec_has_const {
        return true;
    }

    // If same EC already used in list, then redundant.
    for old_pathkey in pathkeys {
        if Some(new_ec) == old_pathkey.pk_eclass {
            return true;
        }
    }

    false
}

// ===========================================================================
// PATHKEY COMPARISONS
// ===========================================================================

/// `compare_pathkeys`
///   Compare two pathkeys lists for equivalence / containment.
pub fn compare_pathkeys(keys1: &[PathKey], keys2: &[PathKey]) -> PathKeysComparison {
    let mut i1 = keys1.iter();
    let mut i2 = keys2.iter();
    loop {
        match (i1.next(), i2.next()) {
            (Some(pathkey1), Some(pathkey2)) => {
                if !pathkeys_equal_pk(pathkey1, pathkey2) {
                    return PathKeysComparison::Different;
                }
            }
            (Some(_), None) => return PathKeysComparison::Better1, // keys1 longer
            (None, Some(_)) => return PathKeysComparison::Better2, // keys2 longer
            (None, None) => return PathKeysComparison::Equal,
        }
    }
}

/// `pathkeys_contained_in`
///   Common special case of `compare_pathkeys`: are `keys2` at least as well
///   sorted as `keys1`?
pub fn pathkeys_contained_in(keys1: &[PathKey], keys2: &[PathKey]) -> bool {
    matches!(
        compare_pathkeys(keys1, keys2),
        PathKeysComparison::Equal | PathKeysComparison::Better2
    )
}

/// `pathkeys_count_contained_in`
///   Same as `pathkeys_contained_in`, but also returns the length of the longest
///   common prefix of `keys1` and `keys2`. Returns `(contained, n_common)`.
pub fn pathkeys_count_contained_in(keys1: &[PathKey], keys2: &[PathKey]) -> (bool, i32) {
    if keys1.is_empty() {
        return (true, 0);
    } else if keys2.is_empty() {
        return (false, 0);
    }

    let mut n = 0;
    let mut i1 = keys1.iter();
    let mut i2 = keys2.iter();
    loop {
        match (i1.next(), i2.next()) {
            (Some(pathkey1), Some(pathkey2)) => {
                if !pathkeys_equal_pk(pathkey1, pathkey2) {
                    return (false, n);
                }
                n += 1;
            }
            (None, _) => return (true, n), // processed all of keys1
            (Some(_), None) => return (false, n), // keys1 longer than keys2
        }
    }
}

// ===========================================================================
// GROUP-BY ORDERING (incremental-sort group-key reorder)
// ===========================================================================

/// `group_keys_reorder_by_pathkeys`
///   Reorder the GROUP BY keys (`group_pathkeys` + `group_clauses`) to match a
///   prefix of `pathkeys`, returning the number of leading keys that matched. The
///   matched keys are moved to the front; the rest are appended (treated as
///   unsorted). Both `group_pathkeys` and `group_clauses` are mutated in place,
///   matching the C `**group_pathkeys`/`**group_clauses`.
///
/// `group_clauses` are `SortGroupClause` `NodeId`s; the per-clause sortref lookup
/// (`get_sortgroupref_clause_noerr`) is delegated to the tlist seam.
pub fn group_keys_reorder_by_pathkeys(
    root: &PlannerInfo,
    pathkeys: &[PathKey],
    group_pathkeys: &mut Vec<PathKey>,
    group_clauses: &mut Vec<NodeId>,
    num_groupby_pathkeys: i32,
) -> i32 {
    let mut new_group_pathkeys: Vec<PathKey> = Vec::new();
    let mut new_group_clauses: Vec<NodeId> = Vec::new();

    if pathkeys.is_empty() || group_pathkeys.is_empty() {
        return 0;
    }

    // We only search within the first num_groupby_pathkeys of *group_pathkeys
    // (the rest are aggregate pathkeys whose ec_sortref doesn't reference the
    // query targetlist). list_copy_head(*group_pathkeys, num_groupby_pathkeys).
    let head = core::cmp::min(num_groupby_pathkeys.max(0) as usize, group_pathkeys.len());
    let grouping_pathkeys: Vec<PathKey> = group_pathkeys[..head].to_vec();

    for (idx, pathkey) in pathkeys.iter().enumerate() {
        // Give up at/after num_groupby_pathkeys, if no matching pointer, or if
        // there's no sortclause reference for this pathkey's EC.
        let sortref = match pathkey.pk_eclass {
            Some(ec_id) => root.ec(ec_id).ec_sortref,
            None => 0,
        };
        if idx as i32 >= num_groupby_pathkeys
            || !list_member_pk(&grouping_pathkeys, pathkey)
            || sortref == 0
        {
            break;
        }

        // Find the matching SortGroupClause; since 1349d27 a pathkey can be in
        // group_pathkeys but not in processed_groupClause, so be careful.
        let sgc = match nf::get_sortgroupref_clause_noerr::call(root, sortref, group_clauses) {
            Some(s) => s,
            None => break, // grouping clause does not cover this pathkey
        };

        // Sort group clause should have an ordering operator (Assert(OidIsValid)).
        debug_assert!(nf::sortgroupclause_info::call(root, sgc).sortop != 0);

        new_group_pathkeys.push(pathkey.clone());
        new_group_clauses.push(sgc);
    }

    // The number of pathkeys with a matching GROUP BY key.
    let n = new_group_pathkeys.len() as i32;

    // Append the remaining group pathkeys/clauses (treated as not sorted), as
    // list_concat_unique_ptr (dropping ptr-duplicates already in the new lists).
    list_concat_unique_pk(&mut new_group_pathkeys, group_pathkeys);
    list_concat_unique_node(&mut new_group_clauses, group_clauses);

    *group_pathkeys = new_group_pathkeys;
    *group_clauses = new_group_clauses;

    n
}

/// `list_concat_unique_ptr(dst, src)` over a `PathKey` list — append elements of
/// `src` not already present (by canonical equality) in `dst`.
fn list_concat_unique_pk(dst: &mut Vec<PathKey>, src: &[PathKey]) {
    for pk in src {
        if !list_member_pk(dst, pk) {
            dst.push(pk.clone());
        }
    }
}

/// `list_concat_unique_ptr(dst, src)` over a `NodeId` list — append `NodeId`s of
/// `src` not already present in `dst`.
fn list_concat_unique_node(dst: &mut Vec<NodeId>, src: &[NodeId]) {
    for &id in src {
        if !dst.contains(&id) {
            dst.push(id);
        }
    }
}

/// `get_useful_group_keys_orderings`
///   Determine which orderings of GROUP BY keys are potentially interesting.
///   Returns a list of [`GroupByOrdering`] items.
///
/// `path` is the input path (its `pathkeys` may suggest a reorder for cheap
/// incremental sort). The `enable_group_by_reordering` / `enable_incremental_sort`
/// GUCs are passed by value (the no-ambient-global-seams rule). `has_grouping_sets`
/// is the C `parse->groupingSets != NIL` predicate, also passed by value: the
/// planner `Query` is reached through the opaque `QueryId` handle which the arena
/// exposes no resolver for, so the caller (the planner driver, which holds the
/// `Query`) supplies the boolean rather than this leaf inventing a Query store.
pub fn get_useful_group_keys_orderings(
    root: &PlannerInfo,
    path: PathId,
    enable_group_by_reordering: bool,
    enable_incremental_sort: bool,
    has_grouping_sets: bool,
) -> Vec<GroupByOrdering> {
    let mut infos: Vec<GroupByOrdering> = Vec::new();

    let mut pathkeys: Vec<PathKey> = root.group_pathkeys.clone();
    let mut clauses: Vec<NodeId> = root.processed_groupClause.clone();

    // Always return at least the original pathkeys/clauses.
    infos.push(GroupByOrdering {
        pathkeys: pathkeys.clone(),
        clauses: clauses.clone(),
    });

    // Should we try generating alternative orderings of the group keys?
    if !enable_group_by_reordering {
        return infos;
    }

    // Grouping sets have their own, more complex ordering logic.
    if has_grouping_sets {
        return infos;
    }

    // If the path is sorted in some way, try reordering the group keys to match
    // as much of the ordering as possible (cheap via incremental sort).
    let path_pathkeys = root.path(path).base().pathkeys.clone();
    if !path_pathkeys.is_empty()
        && !pathkeys_contained_in(&path_pathkeys, &root.group_pathkeys)
    {
        let n = group_keys_reorder_by_pathkeys(
            root,
            &path_pathkeys,
            &mut pathkeys,
            &mut clauses,
            root.num_groupby_pathkeys,
        );

        if n > 0
            && (enable_incremental_sort || n == root.num_groupby_pathkeys)
            && compare_pathkeys(&pathkeys, &root.group_pathkeys) != PathKeysComparison::Equal
        {
            infos.push(GroupByOrdering { pathkeys, clauses });
        }
    }

    infos
}

// ===========================================================================
// CHEAPEST-PATH SELECTORS
// ===========================================================================

/// `get_cheapest_path_for_pathkeys`
///   Find the cheapest path (by `cost_criterion`) that satisfies the given
///   pathkeys and parameterization, and is parallel-safe if required. `None` if
///   no such path. `paths` are `PathId`s for the same relation.
pub fn get_cheapest_path_for_pathkeys(
    root: &PlannerInfo,
    paths: &[PathId],
    pathkeys: &[PathKey],
    required_outer: &Relids,
    cost_criterion: CostSelector,
    require_parallel_safe: bool,
) -> Option<PathId> {
    let mut matched_path: Option<PathId> = None;

    for &path_id in paths {
        // Reject paths that are not parallel-safe, if required.
        if require_parallel_safe && !root.path(path_id).base().parallel_safe {
            continue;
        }

        // Cost comparison is cheaper than pathkey comparison; do it first.
        if let Some(mp) = matched_path {
            if compare_path_costs(root, mp, path_id, cost_criterion) <= 0 {
                continue;
            }
        }

        if pathkeys_contained_in(pathkeys, &root.path(path_id).base().pathkeys)
            && bms::relids_is_subset::call(&path_req_outer(root, path_id), required_outer)
        {
            matched_path = Some(path_id);
        }
    }
    matched_path
}

/// `get_cheapest_fractional_path_for_pathkeys`
///   Find the cheapest path (for retrieving `fraction` of the rows) that
///   satisfies the given pathkeys and parameterization. `None` if none.
pub fn get_cheapest_fractional_path_for_pathkeys(
    root: &PlannerInfo,
    paths: &[PathId],
    pathkeys: &[PathKey],
    required_outer: &Relids,
    fraction: f64,
) -> Option<PathId> {
    let mut matched_path: Option<PathId> = None;

    for &path_id in paths {
        // Cost comparison first (cheaper than pathkey comparison).
        if let Some(mp) = matched_path {
            if compare_fractional_path_costs(root, mp, path_id, fraction) <= 0 {
                continue;
            }
        }

        if pathkeys_contained_in(pathkeys, &root.path(path_id).base().pathkeys)
            && bms::relids_is_subset::call(&path_req_outer(root, path_id), required_outer)
        {
            matched_path = Some(path_id);
        }
    }
    matched_path
}

/// `get_cheapest_parallel_safe_total_inner`
///   Find the unparameterized parallel-safe path with the least total cost.
///   `paths` are in ascending total-cost order; the first acceptable wins.
pub fn get_cheapest_parallel_safe_total_inner(
    root: &PlannerInfo,
    paths: &[PathId],
) -> Option<PathId> {
    for &path_id in paths {
        if root.path(path_id).base().parallel_safe
            && bms::relids_is_empty::call(&path_req_outer(root, path_id))
        {
            return Some(path_id);
        }
    }
    None
}

/// `compare_path_costs(path1, path2, criterion)` — the costsize.c seam, declared
/// in both pathnode-seams and joinpath-seams (same contract). We route through
/// pathnode-seams (the owner).
#[inline]
fn compare_path_costs(
    root: &PlannerInfo,
    path1: PathId,
    path2: PathId,
    criterion: CostSelector,
) -> i32 {
    backend_optimizer_util_pathnode_seams::compare_path_costs::call(root, path1, path2, criterion)
}

/// `compare_fractional_path_costs(path1, path2, fraction)` — pathnode-seams.
#[inline]
fn compare_fractional_path_costs(
    root: &PlannerInfo,
    path1: PathId,
    path2: PathId,
    fraction: f64,
) -> i32 {
    backend_optimizer_util_pathnode_seams::compare_fractional_path_costs::call(
        root, path1, path2, fraction,
    )
}

// ===========================================================================
// NEW PATHKEY FORMATION
// ===========================================================================

/// `make_pathkey_from_sortinfo`
///   Given an expression and sort-order information, create a PathKey. If
///   `create_it` is true, create a canonical EquivalenceClass for the expression
///   if there's not one already, hence a canonical pathkey is returned; if false
///   and no EC matches, returns `None`.
///
/// `expr` is the sort key (a resolved arena `Expr`); `rel` is the set of relids
/// it must be evaluable at (for an index/partition key); `sortref` is the
/// sortgroupref (0 for none, e.g. index/partition keys).
pub fn make_pathkey_from_sortinfo(
    root: &mut PlannerInfo,
    expr: &types_nodes::primnodes::Expr,
    opfamily: Oid,
    opcintype: Oid,
    collation: Oid,
    reverse_sort: bool,
    nulls_first: bool,
    sortref: Index,
    rel: &Relids,
    create_it: bool,
) -> Option<PathKey> {
    let cmptype: CompareType = if reverse_sort { COMPARE_GT } else { COMPARE_LT };

    // EquivalenceClasses need to contain opfamily lists based on the family
    // membership of mergejoinable equality operators, which could belong to more
    // than one opfamily.  So we have to look up the opfamily's equality operator
    // and get its membership.
    let equality_op = lsc::get_opfamily_member_for_cmptype::call(
        opfamily, opcintype, opcintype, COMPARE_EQ,
    )
    .unwrap_or_else(|e| panic!("make_pathkey_from_sortinfo: get_opfamily_member_for_cmptype: {e:?}"));
    if equality_op == 0 {
        panic!(
            "missing operator {COMPARE_EQ}({opcintype},{opcintype}) in opfamily {opfamily}"
        );
    }
    let opfamilies = mcx_collect(
        lsc::get_mergejoin_opfamilies::call,
        equality_op,
    );
    if opfamilies.is_empty() {
        panic!("could not find opfamilies for equality operator {equality_op}");
    }

    // When dealing with binary-compatible opclasses, we have to ensure that the
    // exposed type of the expression tree matches the declared input type of the
    // opclass, except in the case of a "default" opclass.  (This is handled by
    // get_eclass_for_sort_expr's call to canonicalize_ec_expression.)
    let eclass = ec::get_eclass_for_sort_expr::call(
        root,
        expr.clone(),
        opfamilies,
        opcintype,
        collation,
        sortref,
        rel.clone(),
        create_it,
    )
    .unwrap_or_else(|e| {
        panic!("make_pathkey_from_sortinfo: get_eclass_for_sort_expr: {e:?}")
    })?;

    // And finally we can find or create a PathKey node.
    Some(make_canonical_pathkey(
        root, eclass, opfamily, cmptype, nulls_first,
    ))
}

/// `make_pathkey_from_sortop`
///   Like `make_pathkey_from_sortinfo`, but work from a sort operator.
pub fn make_pathkey_from_sortop(
    root: &mut PlannerInfo,
    expr: &types_nodes::primnodes::Expr,
    ordering_op: Oid,
    reverse_sort: bool,
    nulls_first: bool,
    sortref: Index,
    create_it: bool,
) -> Option<PathKey> {
    // get_ordering_op_properties returns Some((opfamily, opcintype, cmptype)).
    let (opfamily, opcintype, _cmptype) = lsc::get_ordering_op_properties::call(ordering_op)
        .unwrap_or_else(|e| panic!("make_pathkey_from_sortop: get_ordering_op_properties: {e:?}"))
        .unwrap_or_else(|| {
            panic!("operator {ordering_op} is not a valid ordering operator");
        });

    // SortGroupClause doesn't carry collation, so consult the expr instead.
    let collation = nf::exprCollation::call(expr);

    make_pathkey_from_sortinfo(
        root,
        expr,
        opfamily,
        opcintype,
        collation,
        reverse_sort,
        nulls_first,
        sortref,
        &None, // rel = NULL
        create_it,
    )
}

/// `build_index_pathkeys`
///   Build a pathkeys list that describes the ordering induced by an index scan
///   using the given index. (Note that an unordered index doesn't induce any
///   ordering, so we return NIL.) `index` is the [`IndexOptInfo`]; `scandir` the
///   scan direction.
pub fn build_index_pathkeys(
    root: &mut PlannerInfo,
    index: &IndexOptInfo,
    scandir: ScanDirection,
) -> Vec<PathKey> {
    let mut retval: Vec<PathKey> = Vec::new();

    if index.sortopfamily.is_empty() {
        return Vec::new(); // non-orderable index
    }

    let index_relids: Relids = index
        .rel
        .map(|r| bms::relids_copy::call(&root.rel(r).relids))
        .unwrap_or(None);

    for (i, &indextle_id) in index.indextlist.iter().enumerate() {
        // INCLUDE columns are stored unordered, so they don't support ordered
        // index scan.
        if i >= index.nkeycolumns as usize {
            break;
        }

        // We assume we don't need to make a copy of the tlist item.
        // indexkey = indextle->expr.
        let indexkey_id = nf::targetentry_info::call(root, indextle_id).expr;
        let indexkey = root.node(indexkey_id).clone();

        let (reverse_sort, nulls_first) = if scan_direction_is_backward(scandir) {
            (!index.reverse_sort[i], !index.nulls_first[i])
        } else {
            (index.reverse_sort[i], index.nulls_first[i])
        };

        // OK, try to make a canonical pathkey for this sort key.
        let cpathkey = make_pathkey_from_sortinfo(
            root,
            &indexkey,
            index.sortopfamily[i],
            index.opcintype[i],
            index.indexcollations[i],
            reverse_sort,
            nulls_first,
            0,
            &index_relids,
            false,
        );

        match cpathkey {
            Some(cpathkey) => {
                // Found the sort key in an EC, so it's relevant for this query.
                if !pathkey_is_redundant(root, &cpathkey, &retval) {
                    retval.push(cpathkey);
                }
            }
            None => {
                // Boolean index keys might be redundant even if not in an EC
                // (see indexcol_is_bool_constant_for_query). If that applies, we
                // can keep examining lower-order index columns; else stop.
                if !indexcol_is_bool_constant_for_query(root, index, i as i32) {
                    break;
                }
            }
        }
    }

    retval
}

/// `partkey_is_bool_constant_for_query`
///   Detect whether a partition key column is constrained to a constant boolean
///   value by the query's WHERE conditions (boolean partition keys are simplified
///   to `WHERE partkeycol` / `WHERE NOT partkeycol` and so never form an EC).
/// `IsBuiltinBooleanOpfamily(opfamily)` (`catalog/pg_opfamily.h` macro) —
/// `(opfamily == BOOL_BTREE_FAM_OID || opfamily == BOOL_HASH_FAM_OID)`.
/// (Does not account for non-core opfamilies that might accept boolean.)
#[inline]
fn is_builtin_boolean_opfamily(opfamily: Oid) -> bool {
    opfamily == types_core::catalog::BOOL_BTREE_FAM_OID
        || opfamily == types_core::catalog::BOOL_HASH_FAM_OID
}

pub fn partkey_is_bool_constant_for_query(
    root: &PlannerInfo,
    partrel: RelId,
    partkeycol: i32,
) -> bool {
    let partopfamily = {
        let part_scheme = root
            .rel(partrel)
            .part_scheme
            .as_ref()
            .expect("partkey_is_bool_constant_for_query: rel has no part_scheme");
        part_scheme.partopfamily[partkeycol as usize]
    };

    // If the partkey isn't boolean, we can't possibly get a match. Partitioning
    // can only use built-in AMs, so a built-in boolean opfamily check is enough.
    if !is_builtin_boolean_opfamily(partopfamily) {
        return false;
    }

    let baserestrictinfo: Vec<RinfoId> = root.rel(partrel).baserestrictinfo.clone();
    for rinfo in baserestrictinfo {
        // Ignore pseudoconstant quals, they won't match.
        if root.rinfo(rinfo).pseudoconstant {
            continue;
        }
        // See if we can match the clause's expression to the partkey column.
        if matches_boolean_partition_clause(root, rinfo, partrel, partkeycol) {
            return true;
        }
    }

    false
}

/// `matches_boolean_partition_clause`
///   Determine if the boolean clause described by `rinfo` matches `partrel`'s
///   `partkeycol`-th partition key column (exact, or NOT above an exact match).
pub fn matches_boolean_partition_clause(
    root: &PlannerInfo,
    rinfo: RinfoId,
    partrel: RelId,
    partkeycol: i32,
) -> bool {
    let clause = root.node(root.rinfo(rinfo).clause).clone();
    // partexpr = (Node *) linitial(partrel->partexprs[partkeycol]).
    let partexpr_id = root.rel(partrel).partexprs[partkeycol as usize][0];
    let partexpr = root.node(partexpr_id).clone();

    // Direct match?
    if nf::equal::call(&partexpr, &clause) {
        return true;
    }
    // NOT clause?
    if nf::is_notclause::call(&clause) {
        let arg = nf::get_notclausearg::call(&clause);
        if nf::equal::call(&partexpr, &arg) {
            return true;
        }
    }

    false
}

/// `build_partition_pathkeys`
///   Build a pathkeys list describing the ordering induced by the partitions of
///   `partrel` under `scandir`. Returns `(pathkeys, partialkeys)`; `partialkeys`
///   is true if pathkeys were built only for a prefix of the partition key.
pub fn build_partition_pathkeys(
    root: &mut PlannerInfo,
    partrel: RelId,
    scandir: ScanDirection,
) -> (Vec<PathKey>, bool) {
    let mut retval: Vec<PathKey> = Vec::new();

    let (partnatts, partrel_relids) = {
        let rel = root.rel(partrel);
        let part_scheme = rel
            .part_scheme
            .as_ref()
            .expect("build_partition_pathkeys: part_scheme is NULL");
        debug_assert!(rel.reloptkind == RELOPT_BASEREL); // IS_SIMPLE_REL
        (part_scheme.partnatts, bms::relids_copy::call(&rel.relids))
    };

    let backward = scan_direction_is_backward(scandir);

    for i in 0..partnatts as usize {
        // keyCol = (Expr *) linitial(partrel->partexprs[i]).
        let key_col_id = root.rel(partrel).partexprs[i][0];
        let key_col = root.node(key_col_id).clone();

        let (opfamily, opcintype, partcollation) = {
            let part_scheme = root.rel(partrel).part_scheme.as_ref().unwrap();
            (
                part_scheme.partopfamily[i],
                part_scheme.partopcintype[i],
                part_scheme.partcollation[i],
            )
        };

        // We assume the PartitionDesc lists any NULL partition last, so treat the
        // scan like a NULLS LAST index: nulls_first for backward scan only.
        let cpathkey = make_pathkey_from_sortinfo(
            root,
            &key_col,
            opfamily,
            opcintype,
            partcollation,
            backward,
            backward,
            0,
            &partrel_relids,
            false,
        );

        match cpathkey {
            Some(cpathkey) => {
                if !pathkey_is_redundant(root, &cpathkey, &retval) {
                    retval.push(cpathkey);
                }
            }
            None => {
                // Boolean partition keys might be redundant even outside an EC.
                if !partkey_is_bool_constant_for_query(root, partrel, i as i32) {
                    return (retval, true); // *partialkeys = true
                }
            }
        }
    }

    (retval, false) // *partialkeys = false
}

/// `build_expression_pathkey`
///   Build a pathkeys list that describes an ordering by a single expression
///   using the given sort operator. Result is NIL if `create_it` is false and
///   the expression isn't already in some EC.
pub fn build_expression_pathkey(
    root: &mut PlannerInfo,
    expr: &types_nodes::primnodes::Expr,
    opno: Oid,
    rel: &Relids,
    create_it: bool,
) -> Vec<PathKey> {
    // Find the operator in pg_amop --- failure shouldn't happen.
    let (opfamily, opcintype, cmptype) = lsc::get_ordering_op_properties::call(opno)
        .unwrap_or_else(|e| panic!("build_expression_pathkey: get_ordering_op_properties: {e:?}"))
        .unwrap_or_else(|| panic!("operator {opno} is not a valid ordering operator"));

    let collation = nf::exprCollation::call(expr);
    let cpathkey = make_pathkey_from_sortinfo(
        root,
        expr,
        opfamily,
        opcintype,
        collation,
        cmptype == COMPARE_GT,
        cmptype == COMPARE_GT,
        0,
        rel,
        create_it,
    );

    match cpathkey {
        Some(pk) => alloc::vec![pk], // list_make1(cpathkey)
        None => Vec::new(),
    }
}

/// `convert_subquery_pathkeys`
///   Build a pathkeys list that describes the ordering of a subquery's result, in
///   the terms of the outer query — a conversion task.
///
/// `subquery_tlist` entries are `TargetEntry` `NodeId`s. We intentionally do not
/// `truncate_useless_pathkeys` here.
pub fn convert_subquery_pathkeys(
    root: &mut PlannerInfo,
    rel: RelId,
    subquery_pathkeys: &[PathKey],
    subquery_tlist: &[NodeId],
) -> Vec<PathKey> {
    let mut retval: Vec<PathKey> = Vec::new();
    let mut retvallen = 0usize;
    let outer_query_keys = root.query_pathkeys.len();
    let rel_relids = bms::relids_copy::call(&root.rel(rel).relids);

    for sub_pathkey in subquery_pathkeys {
        let sub_eclass = sub_pathkey
            .pk_eclass
            .expect("convert_subquery_pathkeys: sub_pathkey has no EC");
        let mut best_pathkey: Option<PathKey> = None;

        if root.ec(sub_eclass).ec_has_volatile {
            // A volatile sub_eclass must have come from an ORDER BY clause; match
            // it to that same targetlist entry.
            let sortref = root.ec(sub_eclass).ec_sortref;
            if sortref == 0 {
                panic!("volatile EquivalenceClass has no sortref");
            }
            let tle = nf::get_sortgroupref_tle::call(root, sortref, subquery_tlist);
            // Is the TLE actually available to the outer query?
            if let Some(outer_var) = find_var_for_subquery_tle(root, rel, tle) {
                // We can represent this sub_pathkey.
                debug_assert!(root.ec(sub_eclass).ec_members.len() == 1);
                let sub_member = root.ec(sub_eclass).ec_members[0];
                let (opfamilies, em_datatype, ec_collation) = {
                    let ec = root.ec(sub_eclass);
                    (
                        ec.ec_opfamilies.clone(),
                        root.em(sub_member).em_datatype,
                        ec.ec_collation,
                    )
                };
                // Note: sortref = 0 even for a volatile sub_eclass; the expression
                // is not volatile in the outer query (just a Var ref).
                let outer_ec = ec::get_eclass_for_sort_expr::call(
                    root,
                    outer_var,
                    opfamilies,
                    em_datatype,
                    ec_collation,
                    0,
                    rel_relids.clone(),
                    false,
                )
                .unwrap_or_else(|e| {
                    panic!("convert_subquery_pathkeys: get_eclass_for_sort_expr: {e:?}")
                });
                if let Some(outer_ec) = outer_ec {
                    best_pathkey = Some(make_canonical_pathkey(
                        root,
                        outer_ec,
                        sub_pathkey.pk_opfamily,
                        sub_pathkey.pk_cmptype,
                        sub_pathkey.pk_nulls_first,
                    ));
                }
            }
        } else {
            // Non-volatile: the sub_eclass can contain multiple members. Each
            // might match none/one/more outer-query-visible output columns. Prefer
            // the representation with the highest score.
            let mut best_score: i32 = -1;
            let ec_members = root.ec(sub_eclass).ec_members.clone();
            let (opfamilies, ec_collation) = {
                let ec = root.ec(sub_eclass);
                (ec.ec_opfamilies.clone(), ec.ec_collation)
            };

            for sub_member in ec_members {
                // Child members should not exist in ec_members.
                debug_assert!(!root.em(sub_member).em_is_child);
                let (sub_expr, sub_expr_type) = {
                    let em = root.em(sub_member);
                    (root.node(em.em_expr).clone(), em.em_datatype)
                };
                let sub_expr_coll = ec_collation;

                for &tle in subquery_tlist {
                    // Is the TLE actually available to the outer query?
                    let outer_var = match find_var_for_subquery_tle(root, rel, tle) {
                        Some(v) => v,
                        None => continue,
                    };

                    // The TLE matches if it matches after sort-key
                    // canonicalization (sub_expr went through the same process).
                    let tle_expr_id = nf::targetentry_info::call(root, tle).expr;
                    let tle_expr_raw = root.node(tle_expr_id).clone();
                    let tle_expr = equivclass::canonicalize_ec_expression(
                        tle_expr_raw,
                        sub_expr_type,
                        sub_expr_coll,
                    );
                    if !nf::equal::call(&tle_expr, &sub_expr) {
                        continue;
                    }

                    // See if we have a matching EC for the TLE.
                    let outer_ec = ec::get_eclass_for_sort_expr::call(
                        root,
                        outer_var,
                        opfamilies.clone(),
                        sub_expr_type,
                        sub_expr_coll,
                        0,
                        rel_relids.clone(),
                        false,
                    )
                    .unwrap_or_else(|e| {
                        panic!("convert_subquery_pathkeys: get_eclass_for_sort_expr: {e:?}")
                    });
                    let outer_ec = match outer_ec {
                        Some(e) => e,
                        None => continue,
                    };

                    let outer_pk = make_canonical_pathkey(
                        root,
                        outer_ec,
                        sub_pathkey.pk_opfamily,
                        sub_pathkey.pk_cmptype,
                        sub_pathkey.pk_nulls_first,
                    );
                    // score = # of equivalence peers.
                    let mut score = root.ec(outer_ec).ec_members.len() as i32 - 1;
                    // +1 if it matches the proper query_pathkeys item.
                    if retvallen < outer_query_keys
                        && pathkeys_equal_pk(&root.query_pathkeys[retvallen], &outer_pk)
                    {
                        score += 1;
                    }
                    if score > best_score {
                        best_pathkey = Some(outer_pk);
                        best_score = score;
                    }
                }
            }
        }

        // If we couldn't find a representation, we're done (the ones to its right
        // are unusable too).
        let best_pathkey = match best_pathkey {
            Some(pk) => pk,
            None => break,
        };

        // Eliminate redundant ordering info.
        if !pathkey_is_redundant(root, &best_pathkey, &retval) {
            retval.push(best_pathkey);
            retvallen += 1;
        }
    }

    retval
}

/// `find_var_for_subquery_tle`
///   If `tle` is due to be emitted by the subquery's scan node, return a `Var`
///   for it (a copy), else `None`. `tle` is a `TargetEntry` `NodeId`.
pub fn find_var_for_subquery_tle(
    root: &PlannerInfo,
    rel: RelId,
    tle: NodeId,
) -> Option<types_nodes::primnodes::Expr> {
    let tle_info = nf::targetentry_info::call(root, tle);
    // If the TLE is resjunk, it's certainly not visible to the outer query.
    if tle_info.resjunk {
        return None;
    }

    let relid = root.rel(rel).relid;
    let reltarget_exprs: Vec<NodeId> = root
        .rel(rel)
        .reltarget
        .as_ref()
        .map(|t| t.exprs.clone())
        .unwrap_or_default();

    // Search the rel's targetlist to see what it will return.
    for var_id in reltarget_exprs {
        let var_expr = root.node(var_id);
        // Ignore placeholders (only a Var counts).
        let var = match var_expr.as_var() {
            Some(v) => v,
            None => continue,
        };
        debug_assert!(var.varno as Index == relid);
        // If we find a Var referencing this TLE, we're good.
        if var.varattno == tle_info.resno {
            // copyObject(var) — make a copy for safety.
            return Some(nf::copyObject::call(var_expr));
        }
    }
    None
}

/// `build_join_pathkeys`
///   Build the path keys for a join relation constructed by mergejoin or
///   nestloop join. Normally the same as the outer path's keys; for FULL/RIGHT/
///   RIGHT_ANTI joins the result is unsorted.
pub fn build_join_pathkeys(
    root: &mut PlannerInfo,
    joinrel: RelId,
    jointype: JoinType,
    outer_pathkeys: &[PathKey],
) -> Vec<PathKey> {
    // RIGHT_SEMI should not come here.
    debug_assert!(jointype != JOIN_RIGHT_SEMI);

    if jointype == JOIN_FULL || jointype == JOIN_RIGHT || jointype == JOIN_RIGHT_ANTI {
        return Vec::new();
    }

    // All pathkey sublists start out canonicalized, so we just truncate away
    // pathkeys uninteresting to higher levels.
    truncate_useless_pathkeys(root, joinrel, outer_pathkeys)
}

// ===========================================================================
// PATHKEYS FROM SORT CLAUSES
// ===========================================================================

/// `make_pathkeys_for_sortclauses`
///   Generate a pathkeys list for a list of `SortGroupClause`s. (It is caller
///   error if not all clauses are sortable.) `sortclauses` are `SortGroupClause`
///   `NodeId`s; `tlist` are `TargetEntry` `NodeId`s.
pub fn make_pathkeys_for_sortclauses(
    root: &mut PlannerInfo,
    sortclauses: &[NodeId],
    tlist: &[NodeId],
) -> Vec<PathKey> {
    let mut owned = sortclauses.to_vec();
    let (result, sortable) =
        make_pathkeys_for_sortclauses_extended(root, &mut owned, tlist, false, false, false);
    // It's caller error if not all clauses were sortable.
    debug_assert!(sortable);
    result
}

/// `make_pathkeys_for_sortclauses_extended`
///   Generate a pathkeys list representing the sort order of a list of
///   `SortGroupClause`s. Returns `(pathkeys, sortable)`.
///
/// `remove_redundant`: drop clauses found redundant from `*sortclauses`.
/// `remove_group_rtindex`: strip the grouping RT index from sort expressions.
/// `set_ec_sortref`: copy the sortref into the pathkey's EC if not yet set.
pub fn make_pathkeys_for_sortclauses_extended(
    root: &mut PlannerInfo,
    sortclauses: &mut Vec<NodeId>,
    tlist: &[NodeId],
    remove_redundant: bool,
    remove_group_rtindex: bool,
    set_ec_sortref: bool,
) -> (Vec<PathKey>, bool) {
    let mut pathkeys: Vec<PathKey> = Vec::new();
    let mut sortable = true;
    // Indices into `*sortclauses` to delete (foreach_delete_current).
    let mut to_delete: Vec<usize> = Vec::new();

    for idx in 0..sortclauses.len() {
        let sortcl = sortclauses[idx];
        let info = nf::sortgroupclause_info::call(root, sortcl);

        // sortkey = get_sortgroupclause_expr(sortcl, tlist).
        let sortkey_id = nf::get_sortgroupclause_expr::call(root, sortcl, tlist);
        if info.sortop == 0 {
            sortable = false;
            continue;
        }

        let mut sortkey = root.node(sortkey_id).clone();
        if remove_group_rtindex {
            debug_assert!(root.group_rtindex > 0);
            // remove_nulling_relids(sortkey, bms_make_singleton(group_rtindex), NULL).
            let singleton = bms::relids_add_members::call(
                None,
                &bms_make_singleton(root.group_rtindex),
            );
            sortkey = nf::remove_nulling_relids::call(&sortkey, &singleton, &None);
        }

        let pathkey = make_pathkey_from_sortop(
            root,
            &sortkey,
            info.sortop,
            info.reverse_sort,
            info.nulls_first,
            info.tle_sort_group_ref,
            true, // create_it
        )
        .expect("make_pathkeys_for_sortclauses_extended: make_pathkey_from_sortop returned None with create_it=true");

        // Copy the sortref into the EC if it hasn't been set and requested.
        if set_ec_sortref {
            if let Some(ec_id) = pathkey.pk_eclass {
                if root.ec(ec_id).ec_sortref == 0 {
                    root.eq_classes[ec_id.index()].ec_sortref = info.tle_sort_group_ref;
                }
            }
        }

        // Canonical form eliminates redundant ordering keys.
        if !pathkey_is_redundant(root, &pathkey, &pathkeys) {
            pathkeys.push(pathkey);
        } else if remove_redundant {
            to_delete.push(idx);
        }
    }

    // Apply the foreach_delete_current removals (high index first to keep the
    // remaining indices valid).
    for &idx in to_delete.iter().rev() {
        sortclauses.remove(idx);
    }

    (pathkeys, sortable)
}

/// `bms_make_singleton(x)` — a one-element relids set.
fn bms_make_singleton(x: i32) -> Relids {
    bms::relids_add_members::call(None, &singleton_relids(x))
}

/// Build a one-element `Relids` directly (the planner `Bitmapset { words }`
/// value), used to seed `relids_add_members`. Mirrors `bms_make_singleton`'s
/// single-bit set.
fn singleton_relids(x: i32) -> Relids {
    debug_assert!(x >= 0);
    let bit = x as usize;
    let word = bit / 64;
    let off = bit % 64;
    let mut words = alloc::vec![0u64; word + 1];
    words[word] = 1u64 << off;
    Some(alloc::boxed::Box::new(types_pathnodes::Bitmapset { words }))
}

// ===========================================================================
// PATHKEYS AND MERGECLAUSES
// ===========================================================================

/// `initialize_mergeclause_eclasses`
///   Set the EquivalenceClass links in a mergeclause restrictinfo (when it wasn't
///   used to generate / derived from an EC). Called before EC merging completes,
///   so the links aren't necessarily canonical (use
///   [`update_mergeclause_eclasses`] before using them).
pub fn initialize_mergeclause_eclasses(root: &mut PlannerInfo, restrictinfo: RinfoId) {
    // Should be a mergeclause ... with links not yet set.
    {
        let rinfo = root.rinfo(restrictinfo);
        debug_assert!(!rinfo.mergeopfamilies.is_empty());
        debug_assert!(rinfo.left_ec.is_none());
        debug_assert!(rinfo.right_ec.is_none());
    }

    // clause = restrictinfo->clause, an OpExpr; read opno/inputcollid + operands.
    let clause = root.node(root.rinfo(restrictinfo).clause).clone();
    let opexpr = clause
        .as_opexpr()
        .expect("initialize_mergeclause_eclasses: clause is not an OpExpr");
    let opno = opexpr.opno;
    let inputcollid = opexpr.inputcollid;
    // get_leftop(clause) / get_rightop(clause) — args[0] / args[1].
    let leftop = opexpr.args[0].clone();
    let rightop = opexpr.args[1].clone();
    let mergeopfamilies = root.rinfo(restrictinfo).mergeopfamilies.clone();

    // Need the declared input types of the operator.
    let (lefttype, righttype) = lsc::op_input_types::call(opno)
        .unwrap_or_else(|e| panic!("initialize_mergeclause_eclasses: op_input_types: {e:?}"));

    // Find or create a matching EquivalenceClass for each side. With
    // create_it = true, the C never returns NULL.
    let left_ec = ec::get_eclass_for_sort_expr::call(
        root,
        leftop,
        mergeopfamilies.clone(),
        lefttype,
        inputcollid,
        0,
        None,
        true,
    )
    .unwrap_or_else(|e| {
        panic!("initialize_mergeclause_eclasses: get_eclass_for_sort_expr: {e:?}")
    });
    let right_ec = ec::get_eclass_for_sort_expr::call(
        root,
        rightop,
        mergeopfamilies,
        righttype,
        inputcollid,
        0,
        None,
        true,
    )
    .unwrap_or_else(|e| {
        panic!("initialize_mergeclause_eclasses: get_eclass_for_sort_expr: {e:?}")
    });

    let rinfo = root.rinfo_mut(restrictinfo);
    rinfo.left_ec = left_ec;
    rinfo.right_ec = right_ec;
}

/// `update_mergeclause_eclasses`
///   Make the cached EquivalenceClass links valid in a mergeclause restrictinfo:
///   chase up to the canonical merged parent (the arena `ec_canonical` union-find)
///   and write the canonical handles back into the rinfo.
pub fn update_mergeclause_eclasses(root: &mut PlannerInfo, restrictinfo: RinfoId) {
    let (left_ec, right_ec) = {
        let rinfo = root.rinfo(restrictinfo);
        debug_assert!(!rinfo.mergeopfamilies.is_empty());
        let l = rinfo.left_ec.expect("left_ec must be set");
        let r = rinfo.right_ec.expect("right_ec must be set");
        (l, r)
    };

    let left_canon = ec_canonical(root, left_ec);
    let right_canon = ec_canonical(root, right_ec);

    let rinfo = root.rinfo_mut(restrictinfo);
    rinfo.left_ec = Some(left_canon);
    rinfo.right_ec = Some(right_canon);
}

/// The mergeclause's outer-side EC handle, per `outer_is_left`.
#[inline]
fn rinfo_outer_ec(rinfo: &RestrictInfo) -> Option<EcId> {
    if rinfo.outer_is_left {
        rinfo.left_ec
    } else {
        rinfo.right_ec
    }
}

/// The mergeclause's inner-side EC handle, per `outer_is_left`.
#[inline]
fn rinfo_inner_ec(rinfo: &RestrictInfo) -> Option<EcId> {
    if rinfo.outer_is_left {
        rinfo.right_ec
    } else {
        rinfo.left_ec
    }
}

/// `find_mergeclauses_for_outer_pathkeys`
///   Find a maximal list of mergeclauses usable with a specified ordering for the
///   join's outer relation, ordered to match `pathkeys`. `restrictinfos` must be
///   marked via `outer_is_left`.
pub fn find_mergeclauses_for_outer_pathkeys(
    root: &mut PlannerInfo,
    pathkeys: &[PathKey],
    restrictinfos: &[RinfoId],
) -> Vec<RinfoId> {
    let mut mergeclauses: Vec<RinfoId> = Vec::new();

    // Make sure we have eclasses cached in the clauses (canonicalized).
    for &rinfo in restrictinfos {
        update_mergeclause_eclasses(root, rinfo);
    }

    for pathkey in pathkeys {
        let pathkey_ec = pathkey.pk_eclass;
        let mut matched_restrictinfos: Vec<RinfoId> = Vec::new();

        // A mergejoin clause matches a pathkey if it has the same EC. Take all
        // matches (outer joins can have several).
        for &rinfo_id in restrictinfos {
            let clause_ec = rinfo_outer_ec(root.rinfo(rinfo_id));
            if clause_ec == pathkey_ec {
                matched_restrictinfos.push(rinfo_id);
            }
        }

        // If no mergeclause for this position, we're done.
        if matched_restrictinfos.is_empty() {
            break;
        }

        mergeclauses.append(&mut matched_restrictinfos);
    }

    mergeclauses
}

/// `select_outer_pathkeys_for_merge`
///   Build a pathkey list representing a possible sort ordering usable with the
///   given mergeclauses (marked via `outer_is_left`). `joinrel` is the join
///   relation being constructed.
pub fn select_outer_pathkeys_for_merge(
    root: &mut PlannerInfo,
    mergeclauses: &[RinfoId],
    joinrel: RelId,
) -> Vec<PathKey> {
    let mut pathkeys: Vec<PathKey> = Vec::new();
    let n_clauses = mergeclauses.len();

    if n_clauses == 0 {
        return Vec::new();
    }

    let joinrel_relids = bms::relids_copy::call(&root.rel(joinrel).relids);

    // ECs used by the mergeclauses (dropping duplicates) and their popularity.
    let mut ecs: Vec<EcId> = Vec::new();
    let mut scores: Vec<i32> = Vec::new();

    for &rinfo_id in mergeclauses {
        // Get the outer eclass (canonicalized).
        update_mergeclause_eclasses(root, rinfo_id);
        let oeclass = rinfo_outer_ec(root.rinfo(rinfo_id)).expect("outer EC must be set");

        // Reject duplicates.
        if ecs.iter().any(|&e| e == oeclass) {
            continue;
        }

        // Score = # of EC members that are a potential future join partner.
        let mut score = 0;
        let nmembers = root.ec(oeclass).ec_members.len();
        for mi in 0..nmembers {
            let em_id = root.ec(oeclass).ec_members[mi];
            let em = root.em(em_id);
            debug_assert!(!em.em_is_child); // child members not in ec_members
            if !em.em_is_const && !bms::relids_overlap::call(&em.em_relids, &joinrel_relids) {
                score += 1;
            }
        }

        ecs.push(oeclass);
        scores.push(score);
    }

    let necs = ecs.len();

    // If we have all ECs mentioned in query_pathkeys, generate a sort order also
    // useful for final output; else use a matching prefix for incremental sort.
    if !root.query_pathkeys.is_empty() {
        let mut matches = 0;
        let mut all_present = true;

        for query_pathkey in &root.query_pathkeys {
            let query_ec = query_pathkey.pk_eclass;
            if ecs.iter().any(|&e| Some(e) == query_ec) {
                matches += 1;
            } else {
                all_present = false;
                break;
            }
        }

        if all_present {
            // Copy query_pathkeys as starting point for our output.
            pathkeys = root.query_pathkeys.clone();
            // Mark their ECs as already-emitted.
            let query_pathkeys = root.query_pathkeys.clone();
            for query_pathkey in &query_pathkeys {
                let query_ec = query_pathkey.pk_eclass;
                for j in 0..necs {
                    if Some(ecs[j]) == query_ec {
                        scores[j] = -1;
                        break;
                    }
                }
            }
        } else if matches == n_clauses {
            // Matched all join clauses but not all query_pathkeys; use the prefix.
            let head = core::cmp::min(matches, root.query_pathkeys.len());
            pathkeys = root.query_pathkeys[..head].to_vec();
            return pathkeys; // have all of the join pathkeys
        }
    }

    // Add remaining ECs in popularity order with a default sort ordering.
    loop {
        let mut best_j = 0;
        let mut best_score = scores[0];
        for j in 1..necs {
            if scores[j] > best_score {
                best_j = j;
                best_score = scores[j];
            }
        }
        if best_score < 0 {
            break; // all done
        }
        let ec_id = ecs[best_j];
        scores[best_j] = -1;
        // linitial_oid(ec->ec_opfamilies).
        let opfamily = root.ec(ec_id).ec_opfamilies[0];
        let pathkey = make_canonical_pathkey(root, ec_id, opfamily, COMPARE_LT, false);
        // Can't be redundant because no duplicate ECs.
        debug_assert!(!pathkey_is_redundant(root, &pathkey, &pathkeys));
        pathkeys.push(pathkey);
    }

    pathkeys
}

/// `make_inner_pathkeys_for_merge`
///   Build a pathkey list representing the explicit sort order that must be
///   applied to an inner path to make it usable with the given mergeclauses
///   (marked via `outer_is_left`) and the known canonical `outer_pathkeys`.
pub fn make_inner_pathkeys_for_merge(
    root: &mut PlannerInfo,
    mergeclauses: &[RinfoId],
    outer_pathkeys: &[PathKey],
) -> Vec<PathKey> {
    let mut pathkeys: Vec<PathKey> = Vec::new();
    let mut lastoeclass: Option<EcId> = None;
    let mut opathkey: Option<PathKey> = None;
    let mut lop = outer_pathkeys.iter(); // lop = list_head(outer_pathkeys)

    for &rinfo_id in mergeclauses {
        update_mergeclause_eclasses(root, rinfo_id);
        let (oeclass, ieclass) = {
            let rinfo = root.rinfo(rinfo_id);
            (rinfo_outer_ec(rinfo), rinfo_inner_ec(rinfo))
        };

        // Outer eclass should match current or next pathkey.
        if oeclass != lastoeclass {
            let next = match lop.next() {
                Some(pk) => pk,
                None => panic!("too few pathkeys for mergeclauses"),
            };
            opathkey = Some(next.clone());
            lastoeclass = next.pk_eclass;
            if oeclass != lastoeclass {
                panic!("outer pathkeys do not match mergeclause");
            }
        }

        let opk = opathkey
            .as_ref()
            .expect("opathkey set on first matching mergeclause");

        // Often the same EC on both sides; then the outer pathkey is also
        // canonical for the inner side, skip a useless search.
        let pathkey = if ieclass == oeclass {
            opk.clone()
        } else {
            let ie = ieclass.expect("inner EC must be set");
            make_canonical_pathkey(root, ie, opk.pk_opfamily, opk.pk_cmptype, opk.pk_nulls_first)
        };

        // Don't generate redundant pathkeys.
        if !pathkey_is_redundant(root, &pathkey, &pathkeys) {
            pathkeys.push(pathkey);
        }
    }

    pathkeys
}

/// `trim_mergeclauses_for_inner_pathkeys`
///   Trim a mergeclause list to those that work with a specified inner-rel
///   ordering. Returns a prefix of the given mergeclauses list. Clauses are
///   marked via `outer_is_left`.
//
// `matched_pathkey = false` after advancing is immediately re-evaluated by the
// following `if clause_ec == pathkey_ec` in the same iteration (so the
// assignment looks "never read"), but it is load-bearing across iterations and
// faithful to C.
#[allow(unused_assignments)]
pub fn trim_mergeclauses_for_inner_pathkeys(
    root: &PlannerInfo,
    mergeclauses: &[RinfoId],
    pathkeys: &[PathKey],
) -> Vec<RinfoId> {
    let mut new_mergeclauses: Vec<RinfoId> = Vec::new();

    // No pathkeys => no mergeclauses (not expected).
    if pathkeys.is_empty() {
        return Vec::new();
    }

    // Initialize to consider first pathkey.
    let mut lip = pathkeys.iter();
    let mut pathkey = lip.next().expect("non-empty pathkeys");
    let mut pathkey_ec = pathkey.pk_eclass;
    let mut matched_pathkey = false;

    // Scan mergeclauses to see how many we can use. (No need to call
    // update_mergeclause_eclasses again here.)
    for &rinfo_id in mergeclauses {
        let rinfo = root.rinfo(rinfo_id);
        let clause_ec = rinfo_inner_ec(rinfo);

        // If no match, attempt to advance to the next pathkey.
        if clause_ec != pathkey_ec {
            if !matched_pathkey {
                break; // no clauses matched this inner pathkey; stop
            }
            match lip.next() {
                Some(pk) => {
                    pathkey = pk;
                    pathkey_ec = pathkey.pk_eclass;
                    matched_pathkey = false;
                }
                None => break,
            }
        }

        // If mergeclause matches current inner pathkey, use it.
        if clause_ec == pathkey_ec {
            new_mergeclauses.push(rinfo_id);
            matched_pathkey = true;
        } else {
            break; // no hope of adding any more
        }
    }

    new_mergeclauses
}

// ===========================================================================
// PATHKEY USEFULNESS CHECKS
// ===========================================================================

/// `pathkeys_useful_for_merging`
///   Count the number of leading pathkeys that may be useful for mergejoins above
///   the given relation. `&mut PlannerInfo` because `update_mergeclause_eclasses`
///   canonicalizes the joininfo rinfos in place.
fn pathkeys_useful_for_merging(root: &mut PlannerInfo, rel: RelId, pathkeys: &[PathKey]) -> i32 {
    let mut useful = 0;

    let has_eclass_joins = root.rel(rel).has_eclass_joins;
    let joininfo: Vec<RinfoId> = root.rel(rel).joininfo.clone();

    for pathkey in pathkeys {
        // If "wrong" direction, not useful for merging.
        if !right_merge_direction(root, pathkey) {
            break;
        }

        let mut matched = false;

        // First look into the pathkey's EC: any members not yet joined to the rel
        // mean a mergejoin clause can surely be generated.
        if has_eclass_joins {
            if let Some(ec_id) = pathkey.pk_eclass {
                if ec::eclass_useful_for_merging::call(root, ec_id, rel) {
                    matched = true;
                }
            }
        }

        if !matched {
            // Otherwise search the rel's joininfo for non-EC-derivable join
            // clauses that might nonetheless be mergejoinable.
            for &rinfo_id in &joininfo {
                if root.rinfo(rinfo_id).mergeopfamilies.is_empty() {
                    continue;
                }
                update_mergeclause_eclasses(root, rinfo_id);

                let rinfo = root.rinfo(rinfo_id);
                if pathkey.pk_eclass == rinfo.left_ec || pathkey.pk_eclass == rinfo.right_ec {
                    matched = true;
                    break;
                }
            }
        }

        // If we didn't find a mergeclause, we're done.
        if matched {
            useful += 1;
        } else {
            break;
        }
    }

    useful
}

/// `right_merge_direction`
///   Check whether the pathkey embodies the preferred sort direction for merging
///   its target column.
fn right_merge_direction(root: &PlannerInfo, pathkey: &PathKey) -> bool {
    for query_pathkey in &root.query_pathkeys {
        if pathkey.pk_eclass == query_pathkey.pk_eclass
            && pathkey.pk_opfamily == query_pathkey.pk_opfamily
        {
            // Found a matching query sort column; prefer this direction iff it
            // matches. (We ignore pk_nulls_first.)
            return pathkey.pk_cmptype == query_pathkey.pk_cmptype;
        }
    }
    // If no matching ORDER BY request, prefer the ASC direction.
    pathkey.pk_cmptype == COMPARE_LT
}

/// `pathkeys_useful_for_ordering`
///   Count the leading pathkeys useful for the query's requested output ordering
///   (allowing for incremental sort).
fn pathkeys_useful_for_ordering(root: &PlannerInfo, pathkeys: &[PathKey]) -> i32 {
    let (_contained, n_common) = pathkeys_count_contained_in(&root.query_pathkeys, pathkeys);
    n_common
}

/// `pathkeys_useful_for_grouping`
///   Count the leading pathkeys with a matching group key.
fn pathkeys_useful_for_grouping(root: &PlannerInfo, pathkeys: &[PathKey]) -> i32 {
    if root.group_pathkeys.is_empty() {
        return 0; // no special ordering requested for grouping
    }
    let mut n = 0;
    for pathkey in pathkeys {
        if !list_member_pk(&root.group_pathkeys, pathkey) {
            break; // no matching group key => done
        }
        n += 1;
    }
    n
}

/// `pathkeys_useful_for_distinct`
///   Count the leading pathkeys shared by the distinctClause pathkeys.
fn pathkeys_useful_for_distinct(root: &PlannerInfo, pathkeys: &[PathKey]) -> i32 {
    if root.distinct_pathkeys.is_empty() {
        return 0;
    }
    let mut n_common_pathkeys = 0;
    for pathkey in pathkeys {
        if !list_member_pk(&root.distinct_pathkeys, pathkey) {
            break;
        }
        n_common_pathkeys += 1;
    }
    n_common_pathkeys
}

/// `pathkeys_useful_for_setop`
///   Count the leading common pathkeys of `root.setop_pathkeys` in `pathkeys`.
fn pathkeys_useful_for_setop(root: &PlannerInfo, pathkeys: &[PathKey]) -> i32 {
    let (_contained, n_common) = pathkeys_count_contained_in(&root.setop_pathkeys, pathkeys);
    n_common
}

/// `truncate_useless_pathkeys`
///   Shorten the given pathkey list to just the useful pathkeys.
pub fn truncate_useless_pathkeys(
    root: &mut PlannerInfo,
    rel: RelId,
    pathkeys: &[PathKey],
) -> Vec<PathKey> {
    let mut nuseful = pathkeys_useful_for_merging(root, rel, pathkeys);
    let mut nuseful2 = pathkeys_useful_for_ordering(root, pathkeys);
    if nuseful2 > nuseful {
        nuseful = nuseful2;
    }
    nuseful2 = pathkeys_useful_for_grouping(root, pathkeys);
    if nuseful2 > nuseful {
        nuseful = nuseful2;
    }
    nuseful2 = pathkeys_useful_for_distinct(root, pathkeys);
    if nuseful2 > nuseful {
        nuseful = nuseful2;
    }
    nuseful2 = pathkeys_useful_for_setop(root, pathkeys);
    if nuseful2 > nuseful {
        nuseful = nuseful2;
    }

    // Not safe to modify input list destructively, but avoid copying if not
    // changing it.
    if nuseful == 0 {
        Vec::new()
    } else if nuseful as usize == pathkeys.len() {
        pathkeys.to_vec()
    } else {
        pathkeys[..nuseful as usize].to_vec() // list_copy_head
    }
}

/// `has_useful_pathkeys`
///   Detect whether the specified rel could have any pathkeys that are useful
///   according to `truncate_useless_pathkeys`. A cheap conservative test.
pub fn has_useful_pathkeys(root: &PlannerInfo, rel: &RelOptInfo) -> bool {
    if !rel.joininfo.is_empty() || rel.has_eclass_joins {
        return true; // might be able to use pathkeys for merging
    }
    if !root.group_pathkeys.is_empty() {
        return true; // might be able to use pathkeys for grouping
    }
    if !root.query_pathkeys.is_empty() {
        return true; // might be able to use them for ordering
    }
    false // definitely useless
}

// ===========================================================================
// small helpers
// ===========================================================================

/// `ScanDirectionIsBackward(scandir)` — scandir == BackwardScanDirection.
#[inline]
fn scan_direction_is_backward(scandir: ScanDirection) -> bool {
    scandir == BackwardScanDirection
}

/// `indexcol_is_bool_constant_for_query(root, index, indexcol)` (indxpath.c) —
/// detect whether a boolean index column is constrained to a constant by the
/// query. indxpath.c is **not yet ported**; this is a seam-and-panic into its
/// future owner. (pathkeys.c only *calls* it; the function belongs to indxpath.c.)
fn indexcol_is_bool_constant_for_query(
    _root: &PlannerInfo,
    _index: &IndexOptInfo,
    _indexcol: i32,
) -> bool {
    panic!(
        "indexcol_is_bool_constant_for_query: optimizer/path/indxpath.c is not yet ported; \
         build_index_pathkeys reached a boolean index column that is not in an \
         EquivalenceClass"
    )
}

/// Collect a `get_mergejoin_opfamilies`-style `Mcx`-allocating lsyscache seam
/// into an owned `Vec<Oid>`. The seam takes an `Mcx` and returns a
/// `PgResult<PgVec<Oid>>`; the pathkey caller wants a plain owned list (the C
/// list lands in the planner context). We charge it to a transient context (the
/// `CurrentMemoryContext` analogue) and copy out.
fn mcx_collect(
    f: fn(mcx::Mcx<'_>, Oid) -> types_error::PgResult<mcx::PgVec<'_, Oid>>,
    opno: Oid,
) -> Vec<Oid> {
    let cx = mcx::MemoryContext::new("pathkeys get_mergejoin_opfamilies transient");
    let v = f(cx.mcx(), opno).unwrap_or_else(|e| panic!("get_mergejoin_opfamilies: {e:?}"));
    v.iter().copied().collect::<Vec<Oid>>()
}
