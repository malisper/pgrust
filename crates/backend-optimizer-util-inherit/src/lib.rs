#![no_std]
#![forbid(unsafe_code)]
#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

//! `backend/optimizer/util/inherit.c` — Inheritance / partition / UNION-ALL
//! child-relation expansion.
//!
//! `expand_inherited_rtentry` expands a range-table entry whose `inh` bit is set
//! into its child "otherrels". There are two distinct cases:
//!
//!   * **RTE_SUBQUERY** — a UNION ALL appendrel. `pull_up_simple_union_all`
//!     (prepjointree.c) already built the child RTEs and the `AppendRelInfo`s;
//!     `expand_appendrel_subquery` just materialises a child `RelOptInfo` for
//!     each member via `build_simple_rel`. This branch is ported in full and is
//!     the one reached by a flattened `SELECT ... UNION ALL SELECT ...`.
//!
//!   * **RTE_RELATION** — table inheritance or partitioning. This branch builds
//!     fresh child RTEs (`expand_single_inheritance_child`), expands partition
//!     descriptors (`expand_partitioned_rtentry`), and translates/const-folds
//!     base quals (`apply_child_basequals`). Those bottom out on unported
//!     substrate (`table_open`, `find_all_inheritors`/partition-desc child RTE
//!     construction, and clause walking over `RestrictInfo.clause`), so they
//!     panic loudly here — "mirror PG and panic" until that substrate lands.
//!
//! `translate_col_privs` / `translate_col_privs_multilevel` are pure
//! `Relids`/attribute-set arithmetic over an `AppendRelInfo`'s `translated_vars`
//! and are ported 1:1.

extern crate alloc;

use types_core::primitive::{Index, InvalidAttrNumber};
use types_error::{PgError, PgResult};
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{AppendRelInfo, NodeId, PlannerInfo, RelId, Relids};
use types_tuple::heaptuple::FirstLowInvalidHeapAttributeNumber;

use backend_optimizer_util_relnode::build_simple_rel;
use backend_optimizer_util_relnode_seams as bms;

/// `RTE_SUBQUERY` discriminant (parsenodes.h `RTEKind`). `types_pathnodes`'s
/// `RTEKind` alias (the value `rte_rtekind` returns) only defines `RTE_RELATION`;
/// `RTE_SUBQUERY` is the next enumerator.
const RTE_SUBQUERY: types_pathnodes::RTEKind = 1;

/// The attributable message for the still-unported inheritance/partition
/// expansion branch (the UNION-ALL subquery branch IS ported).
const DEFERRED: &str = "backend-optimizer-util-inherit: RTE_RELATION inheritance/\
partition expansion is not ported — it needs table_open, find_all_inheritors / \
partition-desc child-RTE construction, and clause walking over RestrictInfo.\
clause (the UNION ALL RTE_SUBQUERY branch IS ported)";

/// Install the inherit.c seams owned here: `expand_inherited_rtentry` (declared
/// in `backend-optimizer-plan-init-subselect-ext-seams`) and
/// `apply_child_basequals` (declared in `backend-optimizer-util-relnode-ext-seams`).
pub fn init_seams() {
    backend_optimizer_plan_init_subselect_ext_seams::expand_inherited_rtentry::set(
        |run, root, rti| expand_inherited_rtentry(run, root, rti),
    );
    backend_optimizer_util_relnode_ext_seams::apply_child_basequals::set(
        |run, root, parent, rel, rti, appinfo| {
            apply_child_basequals(run, root, parent, rel, rti, appinfo)
        },
    );
}

/// `expand_inherited_rtentry(root, rel, rte, rti)` (inherit.c:85) — expand a
/// range-table entry with `inh` set into its inheritance/partition/UNION-ALL
/// children.
pub fn expand_inherited_rtentry<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rti: i32,
) -> PgResult<()> {
    // Assert(rte->inh) — the caller (add_other_rels_to_query) already gated on it.
    debug_assert!(backend_optimizer_rte_seams::rte_inh::call(run, root, rti as Index));

    let rtekind = backend_optimizer_rte_seams::rte_rtekind::call(run, root, rti as Index);

    if rtekind == RTE_SUBQUERY {
        expand_appendrel_subquery(run, root, rti)?;
        return Ok(());
    }

    // Assert(rte->rtekind == RTE_RELATION) — the remaining branch is table
    // inheritance / partitioning, which is not ported (unported substrate).
    Err(PgError::error(alloc::format!(
        "{} (expand_inherited_rtentry, rti={})",
        DEFERRED,
        rti
    )))
}

/// `expand_appendrel_subquery(root, rel, rte, rti)` (inherit.c:799) — add
/// children of an appendrel `RTE_SUBQUERY` (a UNION ALL parent). The
/// `AppendRelInfo`s were already built by `pull_up_simple_union_all`; build a
/// child `RelOptInfo` for each, recursing if a child is itself an appendrel.
fn expand_appendrel_subquery<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rti: i32,
) -> PgResult<()> {
    let rel = root.simple_rel_array[rti as usize]
        .expect("expand_appendrel_subquery: parent rel slot is empty");

    // Snapshot the child relids for this parent (append_rel_list contains all
    // append rels; ignore others). Collected up front to release the borrow on
    // root.append_rel_list before build_simple_rel takes &mut root.
    let child_rtindexes: alloc::vec::Vec<Index> = root
        .append_rel_list
        .iter()
        .filter(|appinfo| appinfo.parent_relid == rti as Index)
        .map(|appinfo| appinfo.child_relid)
        .collect();

    for child_rtindex in child_rtindexes {
        // The child RTE should already exist (built by pull_up_simple_union_all).
        debug_assert!((child_rtindex as i32) < root.simple_rel_array_size);

        // Build the child RelOptInfo.
        let childrel = build_simple_rel(run, root, child_rtindex as i32, Some(rel))?;

        // Child may itself be an inherited rel, either table or subquery.
        if backend_optimizer_rte_seams::rte_inh::call(run, root, child_rtindex) {
            // Recurse: expand_inherited_rtentry(root, childrel, childrte, ...).
            let _ = childrel;
            expand_inherited_rtentry(run, root, child_rtindex as i32)?;
        }
    }

    Ok(())
}

/// `translate_col_privs(parent_privs, translated_vars)` (inherit.c:710) —
/// translate a bitmapset of per-column privileges from the parent rel's
/// attribute numbering to the child's, using `appinfo->translated_vars`.
///
/// The set is stored with each attribute number offset by
/// `-FirstLowInvalidHeapAttributeNumber` so that system attributes (which have
/// the same numbers in all tables) can be added directly. A parent whole-row
/// reference (`InvalidAttrNumber`) is **not** translated into a child whole-row
/// reference; instead the per-column bits for all inherited columns are set.
///
/// `translated_vars` is the `Vec<NodeId>` of arena `Var` handles; a dropped
/// child column is `NodeId::default()` (C's `var == NULL`).
pub fn translate_col_privs(
    root: &PlannerInfo,
    parent_privs: &Relids,
    translated_vars: &[NodeId],
) -> Relids {
    let mut child_privs: Relids = None;
    let flhan = FirstLowInvalidHeapAttributeNumber as i32;

    // System attributes have the same numbers in all tables.
    let mut attno = flhan + 1;
    while attno < 0 {
        if bms::relids_is_member::call(attno - flhan, parent_privs) {
            child_privs = bms::relids_add_member::call(child_privs, attno - flhan);
        }
        attno += 1;
    }

    // Check if parent has whole-row reference.
    let whole_row =
        bms::relids_is_member::call((InvalidAttrNumber as i32) - flhan, parent_privs);

    // And now translate the regular user attributes, using the vars list.
    let mut attno = InvalidAttrNumber as i32;
    for id in translated_vars.iter() {
        attno += 1;
        // C: `Var *var = lfirst_node(Var, lc); if (var == NULL) continue;`
        if *id == NodeId::default() {
            continue;
        }
        let var = root
            .node(*id)
            .as_var()
            .expect("translate_col_privs: translated_var is not a Var");
        if whole_row || bms::relids_is_member::call(attno - flhan, parent_privs) {
            child_privs =
                bms::relids_add_member::call(child_privs, (var.varattno as i32) - flhan);
        }
    }

    child_privs
}

/// `translate_col_privs_multilevel(root, rel, parent_rel, parent_cols)`
/// (inherit.c:760) — recursively translate the column numbers in `parent_cols`
/// to the column numbers of the descendant relation `rel`, given the top parent
/// `parent_rel`.
pub fn translate_col_privs_multilevel(
    root: &PlannerInfo,
    rel: RelId,
    parent_rel: RelId,
    parent_cols: Relids,
) -> PgResult<Relids> {
    let mut parent_cols = parent_cols;

    // Fast path for easy case.
    if parent_cols.is_none() {
        return Ok(None);
    }

    let rel_parent = root.rel(rel).parent;
    let rel_relid = root.rel(rel).relid;

    // Recurse if immediate parent is not the top parent.
    if rel_parent != Some(parent_rel) {
        match rel_parent {
            Some(p) => {
                parent_cols = translate_col_privs_multilevel(root, p, parent_rel, parent_cols)?;
            }
            None => {
                return Err(PgError::error(alloc::format!(
                    "rel with relid {} is not a child rel",
                    rel_relid
                )));
            }
        }
    }

    // Now translate for this child.
    debug_assert!(!root.append_rel_array.is_empty());
    let appinfo: &AppendRelInfo = root.append_rel_array[rel_relid as usize]
        .as_ref()
        .ok_or_else(|| {
            PgError::error("translate_col_privs_multilevel: append_rel_array[rel->relid] is NULL")
        })?;
    // Clone the handle list out so the &root borrow on append_rel_array does not
    // collide with the &root read inside translate_col_privs (node arena).
    let vars = appinfo.translated_vars.clone();

    Ok(translate_col_privs(root, &parent_cols, &vars))
}

/// `get_rel_all_updated_cols(root, rel)` (inherit.c:656) — UPDATE-target columns
/// of a simple relation mapped to `rel`'s numbering. Unported: needs
/// `RTEPermissionInfo` updatedCols + plancat.c `get_dependent_generated_columns`.
pub fn get_rel_all_updated_cols(_root: &PlannerInfo, _rel: RelId) -> ! {
    panic!("{} (get_rel_all_updated_cols)", DEFERRED);
}

/// `apply_child_basequals(root, parentrel, childrel, childRTE, appinfo)`
/// (inherit.c:842) — populate `childrel`'s base restriction quals from
/// `parentrel`'s, translating Vars through `appinfo` and re-checking for quals
/// that const-fold to TRUE/FALSE for this child, plus pulling up the child RTE's
/// own securityQuals. Returns `false` if a qual is provably always-false (the
/// child relation can be pruned).
///
/// The clause-translation path (parent has base quals, or the child RTE carries
/// securityQuals) walks/const-folds `RestrictInfo.clause` over the rinfo arena
/// via `adjust_appendrel_attrs` / `make_restrictinfo` / `restriction_is_always_*`;
/// those carriers are not acyclically reachable from this crate, so that path
/// panics loudly here ("mirror PG and panic"). The empty-quals path — a UNION
/// ALL with no WHERE clause and no RLS security quals, i.e. the trivial
/// `SELECT ... UNION ALL SELECT ...` — is handled exactly: `childquals = NIL`,
/// `cq_min_security = UINT_MAX`, returns `true`.
pub fn apply_child_basequals<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    parentrel: RelId,
    childrel: RelId,
    child_rti: Index,
    _appinfo: &AppendRelInfo,
) -> PgResult<bool> {
    // childquals = NIL; cq_min_security = UINT_MAX (C:856-857).
    const UINT_MAX: Index = u32::MAX;

    let has_parent_quals = !root.rel(parentrel).baserestrictinfo.is_empty();
    let has_child_security =
        backend_optimizer_rte_seams::rte_has_security_quals::call(run, root, child_rti);

    if has_parent_quals || has_child_security {
        return Err(PgError::error(alloc::format!(
            "{} (apply_child_basequals: child_rti={} has translated base quals / \
             securityQuals — the rinfo-arena clause translation path is not \
             reachable from this crate)",
            DEFERRED,
            child_rti
        )));
    }

    // childrel->baserestrictinfo = childquals (NIL);
    // childrel->baserestrict_min_security = cq_min_security (UINT_MAX) (C:973-974).
    root.rel_mut(childrel).baserestrictinfo = alloc::vec::Vec::new();
    root.rel_mut(childrel).baserestrict_min_security = UINT_MAX;

    Ok(true)
}

/// `expand_partitioned_rtentry(...)` (inherit.c:318) — recursively expand a
/// partitioned table's partitions. Unported: needs the partition descriptor +
/// child-RTE construction substrate.
pub fn expand_partitioned_rtentry(_root: &mut PlannerInfo, _rti: Index) -> ! {
    panic!("{} (expand_partitioned_rtentry)", DEFERRED);
}

/// `expand_single_inheritance_child(...)` (inherit.c:461) — build a child RTE +
/// AppendRelInfo for one inheritance member. Unported: needs `table_open` +
/// `rtable`/`simple_rte_array` child-RTE construction.
pub fn expand_single_inheritance_child(_root: &mut PlannerInfo, _rti: Index) -> ! {
    panic!("{} (expand_single_inheritance_child)", DEFERRED);
}
