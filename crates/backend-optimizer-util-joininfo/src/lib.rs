//! `optimizer/util/restrictinfo.c`, `optimizer/util/joininfo.c`,
//! `optimizer/util/placeholder.c`, and `optimizer/plan/orclauses.c` — the
//! RestrictInfo / joininfo-list / PlaceHolderVar / restriction-OR-clause
//! manipulation routines, ported 1:1 over the arena+handle model of
//! [`types_pathnodes::PlannerInfo`].
//!
//! Every `RestrictInfo *` is a [`RinfoId`] arena handle, every `RelOptInfo *` a
//! [`RelId`], every `PlaceHolderInfo *` a [`PhInfoId`], and clause/expression
//! nodes are interned as [`NodeId`] handles into the planner node arena. `Relids`
//! set algebra is reached through the `backend-optimizer-util-relnode`/`-pathnode`
//! seams; the EquivalenceClass relevance probe through equivclass's seam; and the
//! not-yet-ported node operators (`pull_varnos`/`pull_var_clause`/
//! `contain_leaked_vars`/`exprType`/…), the costsize width/selectivity helpers,
//! the relnode base-rel lookup, and the initsplan always-true/false probes
//! through seam crates (panicking until their owners land).

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

pub mod joininfo;
pub mod orclauses;
pub mod placeholder;
pub mod restrictinfo;

pub use joininfo::{
    add_join_clause_to_rels, have_relevant_joinclause, remove_join_clause_from_rels,
};
pub use orclauses::extract_restriction_or_clauses;
pub use placeholder::{
    add_placeholders_to_base_rels, add_placeholders_to_joinrel,
    contain_placeholder_references_to, find_placeholder_info, find_placeholders_in_jointree,
    fix_placeholder_input_needed_levels, get_placeholder_nulling_relids, make_placeholder_expr,
    rebuild_placeholder_attr_needed,
};
pub use restrictinfo::{
    clause_sides_match_join, commute_restrictinfo, extract_actual_clauses, extract_actual_join_clauses,
    get_actual_clauses, join_clause_is_movable_into, join_clause_is_movable_to,
    make_plain_restrictinfo, make_restrictinfo, restriction_is_or_clause,
    restriction_is_securely_promotable,
};

use backend_geqo_all_seams as geqo_seam;
use backend_optimizer_path_costsize_seams as costsize_seam;
use backend_optimizer_path_equivclass_ext_seams as ec_ext_seam;
use backend_optimizer_path_joinpath_seams as joinpath_seam;
use backend_optimizer_path_small_seams as small_seam;
use backend_optimizer_util_placeholder_seams as placeholder_seam;
use backend_optimizer_util_restrictinfo_seams as rinfo_seam;
pub(crate) use backend_optimizer_util_joininfo_ext_seams as ext_seam;
use types_nodes::primnodes::Expr;

/// Install the inward seams owned by restrictinfo.c / joininfo.c / placeholder.c.
/// Called once at single-threaded startup from `seams-init::init_all()`. Each of
/// these seam declarations was placed in an earlier consumer's seam crate as a
/// best guess; this unit is the C-source owner that installs the real bodies.
pub fn init_seams() {
    // restrictinfo.c — small-seams (consumed by clausesel.c/tidpath.c).
    small_seam::restriction_is_or_clause::set(|root, rinfo| {
        restriction_is_or_clause(root, rinfo)
    });
    small_seam::restriction_is_securely_promotable::set(|root, rinfo, rel| {
        restriction_is_securely_promotable(root, rinfo, rel)
    });
    small_seam::join_clause_is_movable_to::set(|root, rinfo, rel| {
        join_clause_is_movable_to(root, rinfo, rel)
    });
    // indxpath.c (`check_index_predicates`) reaches the same joininfo.c body
    // through the restrictinfo-seams declaration; joininfo owns it.
    rinfo_seam::join_clause_is_movable_to::set(|root, rinfo, rel| {
        join_clause_is_movable_to(root, rinfo, rel)
    });

    // restrictinfo.h — joinpath-seams (the `clause_sides_match_join` static
    // inline, consumed by joinpath.c and analyzejoins.c). restrictinfo is the
    // header owner; install the real body here.
    joinpath_seam::clause_sides_match_join::set(|root, rinfo, outerrelids, innerrelids| {
        clause_sides_match_join(root, rinfo, outerrelids, innerrelids)
    });

    // restrictinfo.c — costsize-seams (consumed by costsize.c joins).
    costsize_seam::join_clause_is_movable_into::set(|root, rinfo, current_rel, join_rel| {
        // C call: `join_clause_is_movable_into(rinfo, innerpath->parent->relids,
        // joinrelids)` (costsize.c). The consumer passes the inner path's parent
        // rel and the joinrel; `currentrelids` is the inner parent's relids and
        // `current_and_outer` is the joinrel's relids (already the union the C
        // caller formed). No extra union here.
        let current_relids = root.rel(current_rel).relids.clone();
        let current_and_outer = root.rel(join_rel).relids.clone();
        join_clause_is_movable_into(root, rinfo, &current_relids, &current_and_outer)
    });

    // restrictinfo.c — equivclass-ext-seams (consumed by equivclass.c).
    ec_ext_seam::make_restrictinfo::set(
        |mcx,
         root,
         clause,
         is_pushed_down,
         has_clone,
         is_clone,
         pseudoconstant,
         security_level,
         required_relids,
         incompatible_relids,
         outer_relids| {
            make_restrictinfo(
                mcx,
                root,
                clause,
                is_pushed_down,
                has_clone,
                is_clone,
                pseudoconstant,
                security_level,
                required_relids,
                incompatible_relids,
                outer_relids,
            )
        },
    );

    // restrictinfo.c — restrictinfo-seams (consumed by indxpath.c OR-clause
    // index path building). `make_simple_restrictinfo(root, clause)` is the
    // restrictinfo.h macro `make_restrictinfo(root, clause, true, false, false,
    // false, 0, NULL, NULL, NULL)`. The seam identifies the clause by `NodeId`;
    // the body resolves it to the arena `Expr` and forwards the macro defaults.
    rinfo_seam::make_simple_restrictinfo::set(|mcx, root, clause| {
        // Deep-copy the interned clause via `Expr::clone_in` (the derived
        // `Expr::clone` panics on a context-allocated child such as
        // `Aggref`/`SubLink`/`SubPlan`); the owned copy is then moved into the
        // arena by `make_restrictinfo`. Clone into the caller's long-lived
        // planner-run `mcx` so the copy survives interning (a transient context
        // would dangle on drop).
        let clause_expr = root
            .node(clause)
            .clone_in(mcx)
            .expect("make_simple_restrictinfo: clause clone_in");
        make_restrictinfo(
            mcx,
            root,
            clause_expr,
            true,  // is_pushed_down
            false, // has_clone
            false, // is_clone
            false, // pseudoconstant
            0,     // security_level
            None,  // required_relids
            None,  // incompatible_relids
            None,  // outer_relids
        )
        .expect("make_simple_restrictinfo")
    });

    // restrictinfo.c — `make_plain_restrictinfo(root, clause, orclause, ...)`
    // (restrictinfo-seams), used by `group_similar_or_args` to build nested OR
    // sub-restrictinfos. The seam carries clause+orclause as arena `NodeId`s.
    rinfo_seam::make_plain_restrictinfo::set(
        |mcx,
         root,
         clause,
         orclause,
         is_pushed_down,
         has_clone,
         is_clone,
         pseudoconstant,
         security_level,
         required_relids,
         incompatible_relids,
         outer_relids| {
            // Deep-copy the interned clause / orclause via `Expr::clone_in`
            // (a derived `Expr::clone` panics on a context-allocated child);
            // both owned copies are moved into the arena by
            // `make_plain_restrictinfo`. Clone into the caller's long-lived
            // planner-run `mcx` so the copies survive interning (a transient
            // context would dangle on drop).
            let clause_expr = root
                .node(clause)
                .clone_in(mcx)
                .expect("make_plain_restrictinfo: clause clone_in");
            let orclause_expr = Some(
                root.node(orclause)
                    .clone_in(mcx)
                    .expect("make_plain_restrictinfo: orclause clone_in"),
            );
            make_plain_restrictinfo(
                mcx,
                root,
                clause_expr,
                orclause_expr,
                is_pushed_down,
                has_clone,
                is_clone,
                pseudoconstant,
                security_level,
                required_relids.clone(),
                incompatible_relids.clone(),
                outer_relids.clone(),
            )
            .expect("make_plain_restrictinfo")
        },
    );

    // placeholder.c — joinpath-seams (consumed by joinpath.c memoize analysis).
    // The seam identifies the PHV by a `NodeId` and returns `PhInfoId`
    // (infallible). C `find_placeholder_info` can `elog(ERROR, "too late ...")`
    // once placeholders are frozen and can OOM; neither happens on the memoize
    // cache-key path (placeholders not yet frozen there), so the wrapper
    // surfaces such an error as a panic, matching the fixed consumer signature.
    joinpath_seam::find_placeholder_info::set(|root, node| {
        let phv = match root.node(node) {
            Expr::PlaceHolderVar(phv) => phv.clone(),
            _ => panic!("find_placeholder_info: node is not a PlaceHolderVar"),
        };
        find_placeholder_info(root, &phv).expect("find_placeholder_info failed")
    });

    // placeholder.c / costsize.c — `find_placeholder_info_width` (costsize-seams,
    // consumed by `set_pathtarget_cost_width` in costsize.c). C:
    //   phinfo = find_placeholder_info(root, phv);
    //   tuple_width += phinfo->ph_width;
    //   cost_qual_eval_node(&cost, (Node *) phv->phexpr, root);
    // Returns `(ph_width, cost.startup, cost.per_tuple)`. Homed here because this
    // unit ports `find_placeholder_info`. Mirrors `add_placeholders_to_joinrel`.
    costsize_seam::find_placeholder_info_width::set(|mcx, root, node| {
        // copyObject shape: the PHV's `phexpr` may carry a SubPlan whose derived
        // `Expr::clone` panics, so deep-copy through `clone_in`.
        let phv = match root.node(node) {
            Expr::PlaceHolderVar(phv) => phv
                .clone_in(mcx)
                .expect("find_placeholder_info_width: PHV clone_in failed"),
            _ => panic!("find_placeholder_info_width: node is not a PlaceHolderVar"),
        };
        let phid = find_placeholder_info(root, &phv)
            .expect("find_placeholder_info_width: find_placeholder_info failed");
        let ph_width = root.phinfo(phid).ph_width;
        let phexpr = phv
            .phexpr
            .as_ref()
            .expect("find_placeholder_info_width: PHV has no phexpr")
            .as_ref()
            .clone_in(mcx)
            .expect("find_placeholder_info_width: phexpr clone_in failed");
        let (cost_startup, cost_per_tuple) =
            crate::ext_seam::cost_qual_eval_node_expr::call(root, &phexpr);
        (ph_width, cost_startup, cost_per_tuple)
    });

    // joinpath.c paraminfo_get_equal_hashops (expr_hash_eq_operator) is installed
    // by its real owner var.c (backend-optimizer-util-vars init_seams); see
    // seam_expr_hash_eq_operator there. Installing it here too double-installs the
    // seam (panic "seam installed twice"), so the duplicate was removed.

    // placeholder.c — placeholder-seams (consumed by the parse-tree-aware
    // planner driver; prepjointree.c's pull-up code calls make_placeholder_expr,
    // and find_placeholder_info over the owned PlaceHolderVar). These take/return
    // real node VALUES over the lifetime-free PlannerInfo, unlike the joinpath
    // NodeId-handle dispatch form above.
    placeholder_seam::make_placeholder_expr::set(|root, expr, phrels| {
        make_placeholder_expr(root, expr, phrels)
    });
    placeholder_seam::find_placeholder_info::set(|root, phv| {
        find_placeholder_info(root, phv)
    });

    // joininfo.c — geqo-all-seams (consumed by geqo + joinrels).
    geqo_seam::have_relevant_joinclause::set(|root, rel1, rel2| {
        have_relevant_joinclause(root, rel1, rel2)
    });

    // placeholder.c / initsplan.c — `phinfo_add_needed` (init-subselect-ext-seams):
    // `find_placeholder_info(root, phv); phinfo->ph_needed = bms_add_members(...)`.
    // Homed here because this unit ports `find_placeholder_info`; consumed by
    // `add_vars_to_targetlist` / `add_vars_to_attr_needed` in init-subselect.
    backend_optimizer_plan_init_subselect_ext_seams::phinfo_add_needed::set(
        |root, phv, where_needed| placeholder::phinfo_add_needed(root, phv, where_needed),
    );

    // relnode.c reaches `add_placeholders_to_joinrel` (placeholder.c, owned here)
    // and `join_clause_is_movable_into` over a transient relid set
    // (restrictinfo.c, owned here) through its no-owner consumer-side ext seam
    // crate. These owners live in this unit; install them.
    use backend_optimizer_util_relnode_ext_seams as relnode_ext;
    relnode_ext::add_placeholders_to_joinrel::set(|root, joinrel, outer_rel, inner_rel, sjinfo| {
        placeholder::add_placeholders_to_joinrel(root, joinrel, outer_rel, inner_rel, sjinfo)
    });
    relnode_ext::join_clause_is_movable_into_relids::set(
        |root, rinfo, current_relids, join_and_required| {
            join_clause_is_movable_into(root, rinfo, current_relids, join_and_required)
        },
    );
}

/// Shorthand for the `Relids` set-algebra seams (relnode.c owner).
pub(crate) use backend_optimizer_util_relnode_seams as bms;
pub(crate) use backend_optimizer_util_pathnode_seams as bms_path;

#[cfg(test)]
mod tests;
