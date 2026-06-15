#![no_std]
#![forbid(unsafe_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::collapsible_else_if)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::needless_range_loop)]
#![allow(clippy::type_complexity)]

//! Safe-Rust port of `src/backend/optimizer/path/indxpath.c` (postgres-18.3):
//! the per-relation index-path generator.
//!
//! Given a base (or "other member") relation, indxpath.c determines which of the
//! relation's indexes are usable for the query and builds the corresponding
//! [`IndexPath`](types_pathnodes) / `BitmapHeapPath` / `BitmapAndPath` /
//! `BitmapOrPath` candidates, submitting the survivors to the rel's pathlist.
//!
//! # Arena model
//!
//! This crate is ported over the planner's [`PlannerInfo`] *handle arena*: a
//! `Path *` is a [`PathId`](types_pathnodes), a `RestrictInfo *` is a
//! [`RinfoId`](types_pathnodes), a clause `Expr *` is a
//! [`NodeId`](types_pathnodes), an `EquivalenceClass *` is an `EcId`, and the
//! `rel()`/`path()`/`rinfo()`/`node()` accessors recover the value; `alloc_*`
//! pushes a freshly-constructed node/path/rinfo and returns its handle. The
//! whole control flow of indxpath.c ports 1:1 over those handles.
//!
//! `IndexClauseSet` and `PathClauseUsage`/`OrArgIndexMatch` are C-file-private
//! structs (no `types_*` home), so they are owned here, mirroring C exactly.
//!
//! # Failure surface / seams
//!
//! Allocating routines take an [`Mcx`](mcx::Mcx) and return
//! [`PgResult`](types_error::PgResult): every C `palloc` can
//! `ereport(ERROR, ERRCODE_OUT_OF_MEMORY)`. Everything that crosses a subsystem
//! boundary crosses through a seam — pathnode.c (`create_*_path`/`add_path`),
//! costsize.c (`cost_bitmap_*`, `create_partial_bitmap_paths`,
//! `enable_indexonlyscan`), pathkeys.c (`has_useful_pathkeys`/
//! `build_index_pathkeys`/`truncate_useless_pathkeys`), equivclass.c
//! (`generate_implied_equalities_for_column`/`generate_join_implied_equalities`),
//! predtest.c (`predicate_implied_by`), restrictinfo.c
//! (`make_simple_restrictinfo`/`make_plain_restrictinfo`), joininfo.c
//! (`join_clause_is_movable_to`), selfuncs.c (`estimate_num_groups`),
//! joinrels.c (`is_dummy_rel`), lsyscache.c (`op_in_opfamily`/`get_commutator`/
//! …), var.c (`pull_varnos`/`pull_varattnos`), plancat.c (`get_plan_rowmark`),
//! and the fmgr planner-support call in `get_index_clause_from_support`. Each
//! seam defaults to a loud panic until its owning crate lands.
//!
//! The `make_opclause`/`make_bool_const`/`make_orclause`/`make_ands_implicit`/
//! `make_relabel_type` node constructors (makefuncs.c), `make_SAOP_expr` /
//! `contain_volatile_functions` / `contain_mutable_functions` (clauses.c), and
//! the `expression_tree_walker`/`expression_tree_mutator`/`equal` node walkers
//! (nodeFuncs.c/equalfuncs.c) are *direct* dependencies (real ported crates).

extern crate alloc;

pub mod bitmap;
pub mod cost;
pub mod drivers;
pub mod matchers;
pub mod operand;
pub mod pathkeys;
pub mod predicates;
pub mod unique;

mod util;

pub use bitmap::PathClauseUsage;
pub use matchers::IndexClauseSet;

pub use cost::{
    adjust_rowcount_for_semijoins, approximate_joinrel_size, bitmap_and_cost_est,
    bitmap_scan_cost_est, get_loop_count, path_usage_comparator,
};
pub use drivers::{
    build_index_paths, consider_index_join_clauses, consider_index_join_outer_rels,
    create_index_paths, eclass_already_used, get_index_paths, get_join_index_paths,
};
pub use bitmap::{
    build_paths_for_OR, choose_bitmap_and, classify_index_clause_usage, find_indexpath_quals,
    find_list_position, generate_bitmap_or_paths, group_similar_or_args,
    make_bitmap_paths_for_or_group,
};
pub use matchers::{
    expand_indexqual_rowcompare, get_index_clause_from_support, match_boolean_index_clause,
    match_clause_to_index, match_clause_to_indexcol, match_clauses_to_index,
    match_eclass_clauses_to_index, match_funcclause_to_indexcol, match_join_clauses_to_index,
    match_opclause_to_indexcol, match_orclause_to_indexcol, match_restriction_clauses_to_index,
    match_rowcompare_to_indexcol, match_saopclause_to_indexcol, IsBooleanOpfamily,
};
pub use operand::{
    contain_strippable_phv_walker, is_pseudo_constant_for_index, match_index_to_operand,
    strip_phvs_in_index_operand, strip_phvs_in_index_operand_mutator,
};
pub use pathkeys::{match_clause_to_ordering_op, match_pathkeys_to_index};
pub use predicates::{
    check_index_only, check_index_predicates, indexcol_is_bool_constant_for_query,
};
pub use unique::{
    ec_member_matches_indexcol, relation_has_unique_index_ext, relation_has_unique_index_for,
};

/// Install this crate's inward seams (the externally-visible entry points of
/// indxpath.c) into the registry. Called once at single-threaded startup via
/// `seams-init`.
pub fn init_seams() {
    use backend_optimizer_path_indxpath_seams as ix;
    
    use types_pathnodes::{IndexOptInfo, NodeId, PlannerInfo, RelId, RinfoId};
    use types_core::primitive::Oid;
    use types_error::PgResult;

    // create_index_paths / check_index_predicates allocate; the registry seam
    // signatures take `&mut PlannerInfo` only (no Mcx in the seam contract).
    // A few deep callees (the clauses.c `make_SAOP_expr` and var.c
    // `pull_varattnos` seams) take an `Mcx` because they operate over the
    // nodes-core `Bitmapset`/Mcx model rather than the planner Vec arena; the
    // inward wrappers stand up a transient context (the C
    // `CurrentMemoryContext`) and thread its `Mcx` down.
    ix::create_index_paths::set(|root: &mut PlannerInfo, rel: RelId| -> PgResult<()> {
        let ctx = mcx::MemoryContext::new("indxpath");
        drivers::create_index_paths(ctx.mcx(), root, rel)
    });
    ix::check_index_predicates::set(|root: &mut PlannerInfo, rel: RelId| -> PgResult<()> {
        let ctx = mcx::MemoryContext::new("indxpath");
        predicates::check_index_predicates(ctx.mcx(), root, rel)
    });
    ix::relation_has_unique_index_for::set(
        |root: &mut PlannerInfo,
         rel: RelId,
         restrictlist: &[RinfoId],
         exprlist: &[NodeId],
         oprlist: &[Oid]|
         -> bool {
            unique::relation_has_unique_index_for(root, rel, restrictlist, exprlist, oprlist)
        },
    );
    ix::relation_has_unique_index_ext::set(
        |root: &mut PlannerInfo,
         rel: RelId,
         restrictlist: &[RinfoId],
         exprlist: &[NodeId],
         oprlist: &[Oid]|
         -> (bool, alloc::vec::Vec<RinfoId>) {
            let mut extra = alloc::vec::Vec::new();
            let ok = unique::relation_has_unique_index_ext(
                root,
                rel,
                restrictlist,
                exprlist,
                oprlist,
                Some(&mut extra),
            );
            (ok, extra)
        },
    );
    ix::indexcol_is_bool_constant_for_query::set(
        |root: &mut PlannerInfo, index: &IndexOptInfo, indexcol: i32| -> bool {
            let ctx = mcx::MemoryContext::new("indxpath");
            predicates::indexcol_is_bool_constant_for_query(ctx.mcx(), root, index, indexcol)
        },
    );
    ix::match_index_to_operand::set(
        |root: &PlannerInfo, operand: NodeId, indexcol: i32, index: &IndexOptInfo| -> bool {
            let expr = root.node(operand).clone();
            operand::match_index_to_operand(root, &expr, indexcol as usize, index)
        },
    );
}

#[cfg(test)]
mod tests;
