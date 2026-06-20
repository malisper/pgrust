//! `backend/optimizer/path/equivclass.c` — the planner's EquivalenceClass
//! engine, ported 1:1 over the arena+handle model of
//! [`types_pathnodes::PlannerInfo`].
//!
//! Every `EquivalenceClass *` is an [`EcId`] arena handle, every
//! `EquivalenceMember *` an [`EmId`], every `RestrictInfo *` a [`RinfoId`], every
//! `RelOptInfo *` a [`RelId`], and member/clause expressions are interned as
//! [`NodeId`] handles into the planner node arena. The `Relids` set algebra is
//! reached through the `backend-optimizer-util-relnode` seams; catalog/lsyscache
//! reads through `backend-utils-cache-lsyscache`; and the not-yet-ported node
//! operators (`equal`/`exprType`/…) and initsplan/restrictinfo clause machinery
//! through this crate's own seam crate (panicking until their owners land).
//!
//! Adaptation note on EC merging (`process_equivalence` case 2): C does
//! `list_delete_nth_cell(root->eq_classes, ec2_idx)` to drop the absorbed EC,
//! which shifts list positions. Here ECs are referenced by stable [`EcId`] arena
//! handles (PathKeys, RestrictInfos), and `RelOptInfo::eclass_indexes` bitmaps
//! index `eq_classes` by position, so we must NOT remove/shift. Instead the
//! absorbed EC is left in place with `ec_merged = Some(survivor)` and emptied
//! members/sources, exactly as C leaves the node behind for dangling PathKeys;
//! every `eq_classes` scan in this crate skips `ec_merged.is_some()` ECs, which
//! reproduces C's post-delete iteration set.

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

pub mod base;
pub mod child;
pub mod derives;
pub mod find;
pub mod join;
pub mod merge;
pub mod relevance;

pub use base::generate_base_implied_equalities;
pub use child::{
    add_child_join_rel_equivalences, add_child_rel_equivalences, add_setop_child_rel_equivalences,
    rebuild_eclass_attr_needed,
};
pub use derives::{
    ec_add_clause_to_derives_hash, ec_add_derived_clause, ec_add_derived_clauses,
    ec_build_derives_hash, ec_clear_derived_clauses, ec_search_clause_for_ems,
    ec_search_derived_clause_for_ems, fill_ec_derives_key, find_derived_clause_for_ec_member,
};
pub use find::{
    exprs_known_equal, find_computable_ec_member, find_ec_member_matching_expr,
    match_eclasses_to_foreign_key_col, relation_can_be_sorted_early,
};
pub use join::{
    create_join_clause, generate_implied_equalities_for_column,
    generate_join_implied_equalities, generate_join_implied_equalities_for_ecs,
    reconsider_outer_join_clauses,
};
pub use merge::{
    canonicalize_ec_expression, get_eclass_for_sort_expr, process_equivalence,
};
pub use relevance::{
    eclass_member_iterator_next, eclass_useful_for_merging, find_join_domain,
    get_common_eclass_indexes, get_eclass_indexes_for_relids, has_relevant_eclass_joinclause,
    have_relevant_eclass_joinclause, is_redundant_derived_clause, is_redundant_with_indexclauses,
    select_equality_operator, setup_eclass_member_iterator,
};

use backend_optimizer_path_costsize_seams as cz_seam;
use backend_optimizer_path_equivclass_seams as ec_seam;
use backend_optimizer_path_joinpath_seams as jp_seam;
use backend_optimizer_path_small_seams as ps_seam;
use types_error::PgResult;
use types_pathnodes::{
    EcId, PathNode, PlannerInfo, RelId, Relids, RinfoId, SpecialJoinInfo,
};

/// Install the inward seams owned by equivclass.c. Called once at
/// single-threaded startup from `seams-init::init_all()`.
pub fn init_seams() {
    ec_seam::process_equivalence::set(|root, run, restrictinfo, jdomain| {
        process_equivalence(root, run, restrictinfo, jdomain)
    });
    ec_seam::get_eclass_for_sort_expr::set(
        |root, expr, opfamilies, opcintype, collation, sortref, rel, create_it| {
            get_eclass_for_sort_expr(
                root, expr, opfamilies, opcintype, collation, sortref, rel, create_it,
            )
        },
    );
    ec_seam::generate_base_implied_equalities::set(generate_base_implied_equalities);
    ec_seam::generate_join_implied_equalities::set(
        |root, run, join_relids, outer_relids, inner_rel, sjinfo| {
            generate_join_implied_equalities(root, run, join_relids, outer_relids, inner_rel, sjinfo)
        },
    );
    ec_seam::generate_join_implied_equalities_for_ecs::set(
        |root, run, eclasses, join_relids, outer_relids, inner_rel| {
            generate_join_implied_equalities_for_ecs(
                root, run, eclasses, join_relids, outer_relids, inner_rel,
            )
        },
    );
    ec_seam::exprs_known_equal::set(|root, item1, item2, opfamily| {
        exprs_known_equal(root, &item1, &item2, opfamily)
    });
    ec_seam::reconsider_outer_join_clauses::set(reconsider_outer_join_clauses);
    ec_seam::rebuild_eclass_attr_needed::set(rebuild_eclass_attr_needed);
    ec_seam::have_relevant_eclass_joinclause::set(|root, rel1, rel2| {
        have_relevant_eclass_joinclause(root, rel1, rel2)
    });
    ec_seam::has_relevant_eclass_joinclause::set(|root, rel1| {
        has_relevant_eclass_joinclause(root, rel1)
    });
    ec_seam::eclass_useful_for_merging::set(|root, eclass, rel| {
        eclass_useful_for_merging(root, eclass, rel)
    });
    ec_seam::is_redundant_derived_clause::set(|root, rinfo, clauselist| {
        is_redundant_derived_clause(root, rinfo, &clauselist)
    });
    ec_seam::add_child_rel_equivalences::set(|root, run, appinfo, parent_rel, child_rel| {
        add_child_rel_equivalences(root, run, appinfo, parent_rel, child_rel)
    });

    // `EC_MUST_BE_REDUNDANT(eclass)` == `eclass->ec_has_const` (pathnodes.h
    // macro, equivclass-owned vocabulary). joinpath.c reads it on a mergejoinable
    // clause's `left_ec`/`right_ec`, which `update_mergeclause_eclasses` has
    // already filled; equivclass owns the EC arena, so install the bodies here.
    jp_seam::ec_must_be_redundant_left::set(|root, restrictinfo| {
        let ec = root.rinfo(restrictinfo).left_ec.expect(
            "ec_must_be_redundant_left: left_ec must be set (update_mergeclause_eclasses)",
        );
        root.ec(ec).ec_has_const
    });
    jp_seam::ec_must_be_redundant_right::set(|root, restrictinfo| {
        let ec = root.rinfo(restrictinfo).right_ec.expect(
            "ec_must_be_redundant_right: right_ec must be set (update_mergeclause_eclasses)",
        );
        root.ec(ec).ec_has_const
    });
    // `is_redundant_with_indexclauses` lives in equivclass.c (real impl in
    // `relevance.rs`) but its public seam is declared on costsize-seams (the
    // `extract_nonindex_conditions` / `cost_index` consumer). The seam carries
    // the index path by `PathId`; resolve it to the `IndexPath.indexclauses` the
    // C caller passes (`path->indexclauses`) before delegating to the impl. A
    // non-`IndexPath` here is a caller bug (C only calls this with an IndexPath).
    // `generate_implied_equalities_for_column` (equivclass.c:3239) is owned
    // here; its seam is homed on path-small-seams (tidpath.c is the sole
    // consumer). The seam carries the per-column matcher as a bare `fn`; adapt
    // it to the real impl's `&mut dyn FnMut` callback.
    ps_seam::generate_implied_equalities_for_column::set(
        |root, run, rel, callback, prohibited_rels| {
            let mut cb = move |r: &PlannerInfo, rl: RelId, ec, em| callback(r, rl, ec, em);
            join::generate_implied_equalities_for_column(root, run, rel, &mut cb, prohibited_rels)
        },
    );
    cz_seam::is_redundant_with_indexclauses::set(|root, rinfo, index_path| {
        let indexclauses = match root.path(index_path) {
            PathNode::IndexPath(ip) => ip.indexclauses.clone(),
            _ => panic!(
                "is_redundant_with_indexclauses: PathId does not resolve to an IndexPath"
            ),
        };
        is_redundant_with_indexclauses(root, rinfo, &indexclauses)
    });
}

// Wrapper signatures that the inward seams bind to (some inner ports take refs /
// slices; the seam contracts pass owned values).
#[allow(dead_code)]
fn _seam_shapes_witness(
    _root: &mut PlannerInfo,
    _rel: RelId,
    _ec: EcId,
    _ri: RinfoId,
    _r: Relids,
    _sj: Option<SpecialJoinInfo>,
) -> PgResult<()> {
    Ok(())
}
