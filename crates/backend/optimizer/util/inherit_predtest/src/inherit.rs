//! `backend/optimizer/util/inherit.c` — inheritance / partition child-relation
//! expansion.
//!
//! # Ported now (pure attribute-set arithmetic)
//!
//! [`translate_col_privs`] and [`translate_col_privs_multilevel`] are ported
//! 1:1 from the C.  They are pure `Relids`/attribute-number arithmetic over an
//! `AppendRelInfo`'s `translated_vars` (each translated Var's `varattno`), with
//! the bitmapset primitives crossing through the established `relids_*` seams in
//! [`relnode_seams`] (the `Relids`-typed bms facade
//! restrictinfo/pathnode/joinrels already use; relnode is its owner).
//!
//! # Keystone-blocked (the inheritance/partition expansion entry points)
//!
//! `expand_inherited_rtentry`, `expand_partitioned_rtentry`,
//! `expand_single_inheritance_child`, `expand_appendrel_subquery`,
//! `get_rel_all_updated_cols`, and `apply_child_basequals` are NOT ported.  They
//! require an owned, writable parser model this repo does not have yet:
//!
//!   * `expand_single_inheritance_child` does `makeNode(RangeTblEntry);
//!     memcpy(childrte, parentrte, sizeof(RangeTblEntry))`, mutates dozens of
//!     child RTE fields, and `lappend`s it to `parse->rtable`; it likewise
//!     `makeNode(PlanRowMark)` and `lappend`s to `root->rowMarks`.  In this
//!     repo `RangeTblEntry`/`PlanRowMark`/`Query` are opaque handles
//!     (`RangeTblEntryId`/`NodeId`/`QueryId`) owned by the (unported) parser —
//!     there is no writable value type to copy/mutate, and `parse->rtable` is
//!     not appendable through the opaque `QueryId`.
//!   * The whole expansion further depends on a long list of unported
//!     neighbours (`table_open`/`find_all_inheritors`/`build_simple_rel`/
//!     `make_append_rel_info`/`expand_planner_arrays`/`get_plan_rowmark`/
//!     `select_rowmark_type`/`add_row_identity_*`/`add_vars_to_targetlist`/
//!     `prune_append_rel_partitions`/`PartitionDirectoryLookup`/
//!     `get_dependent_generated_columns`/`adjust_appendrel_attrs`/
//!     `make_restrictinfo`/`restriction_is_always_*`) and on the
//!     `simple_rte_array`<->`rtable` / `append_rel_array`<->`append_rel_list`
//!     aliasing convention.
//!
//! Porting those is a prerequisite parser-keystone, not work expressible in this
//! value model today.  See the unit's audit report and CATALOG note.

use relnode_seams as relnode;

use ::types_core::primitive::InvalidAttrNumber;
use ::types_error::{PgError, PgResult};
use ::pathnodes::{NodeId, PlannerInfo, RelId, Relids};
use ::types_tuple::heaptuple::FirstLowInvalidHeapAttributeNumber;

/// `FirstLowInvalidHeapAttributeNumber` as `i32` (the offset applied to every
/// attribute number so that system attributes map to non-negative bits).
const FLIHAN: i32 = FirstLowInvalidHeapAttributeNumber as i32;

/// `elog(ERROR, ...)` — internal error (the only `elog` reached by the ported
/// slice is `translate_col_privs_multilevel`'s "rel ... is not a child rel").
fn elog_error(message: alloc::string::String) -> PgError {
    PgError::error(message)
}

/*
 * translate_col_privs
 *	  Translate a bitmapset representing per-column privileges from the
 *	  parent rel's attribute numbering to the child's.
 *
 * The only surprise here is that we don't translate a parent whole-row
 * reference into a child whole-row reference.  That would mean requiring
 * permissions on all child columns, which is overly strict, since the
 * query is really only going to reference the inherited columns.  Instead
 * we set the per-column bits for all inherited columns.
 *
 * `translated_vars` is the `AppendRelInfo.translated_vars` list, carried as
 * opaque expr-node handles into `root`'s arena; a dropped child column is a
 * `NodeId::default()` (0) hole (the C `lappend(vars, NULL)`), resolved here
 * through `root.node`.
 */
pub fn translate_col_privs(
    root: &PlannerInfo,
    parent_privs: &Relids,
    translated_vars: &[NodeId],
) -> Relids {
    let mut child_privs: Relids = None;
    let mut attno: i32;

    /* System attributes have the same numbers in all tables */
    attno = FLIHAN + 1;
    while attno < 0 {
        if relnode::relids_is_member::call(attno - FLIHAN, parent_privs) {
            child_privs = relnode::relids_add_member::call(child_privs, attno - FLIHAN);
        }
        attno += 1;
    }

    /* Check if parent has whole-row reference */
    let whole_row =
        relnode::relids_is_member::call((InvalidAttrNumber as i32) - FLIHAN, parent_privs);

    /* And now translate the regular user attributes, using the vars list */
    attno = InvalidAttrNumber as i32;
    for id in translated_vars.iter() {
        attno += 1;
        /* ignore dropped columns (NULL list entry / NodeId hole) */
        if *id == NodeId::default() {
            continue;
        }
        let var = match root.node(*id).as_var() {
            Some(v) => v,
            None => continue,
        };
        if whole_row || relnode::relids_is_member::call(attno - FLIHAN, parent_privs) {
            child_privs =
                relnode::relids_add_member::call(child_privs, (var.varattno as i32) - FLIHAN);
        }
    }

    child_privs
}

/*
 * translate_col_privs_multilevel
 *		Recursively translates the column numbers contained in 'parent_cols'
 *		to the column numbers of a descendant relation given by 'rel'
 *
 * Note that because this is based on translate_col_privs, it will expand
 * a whole-row reference into all inherited columns.  This is not an issue
 * for current usages, but beware.
 */
pub fn translate_col_privs_multilevel(
    root: &PlannerInfo,
    rel: RelId,
    parent_rel: RelId,
    parent_cols: Relids,
) -> PgResult<Relids> {
    let mut parent_cols = parent_cols;

    /* Fast path for easy case. */
    if parent_cols.is_none() {
        return Ok(None);
    }

    let rel_node = root.rel(rel);

    /* Recurse if immediate parent is not the top parent. */
    if rel_node.parent != Some(parent_rel) {
        if let Some(p) = rel_node.parent {
            parent_cols = translate_col_privs_multilevel(root, p, parent_rel, parent_cols)?;
        } else {
            return Err(elog_error(alloc::format!(
                "rel with relid {} is not a child rel",
                rel_node.relid
            )));
        }
    }

    /* Now translate for this child. */
    debug_assert!(!root.append_rel_array.is_empty());
    let appinfo = match root.append_rel_array.get(rel_node.relid as usize) {
        Some(Some(a)) => a,
        _ => {
            return Err(elog_error(alloc::format!(
                "translate_col_privs_multilevel: append_rel_array[{}] is NULL",
                rel_node.relid
            )))
        }
    };

    Ok(translate_col_privs(root, &parent_cols, &appinfo.translated_vars))
}
