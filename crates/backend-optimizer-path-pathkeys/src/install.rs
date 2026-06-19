//! `init_seams()` — install the pathkeys.c functions that already-merged
//! consumers reach through seams.
//!
//! pathkeys.c is the **owner** of these functions, but the seam *declarations*
//! live in the consumers' seam crates (the merged join-path enumerator and the
//! pathnode utility): `pathnode-seams` declares `compare_pathkeys` /
//! `pathkeys_contained_in`; `joinpath-seams` declares the join-pathkeys family
//! (`build_join_pathkeys`, the mergeclause matchers, the cheapest-path selectors,
//! `update_mergeclause_eclasses`, `pathkeys_contained_in` /
//! `pathkeys_count_contained_in`). We install each here so a call from those
//! crates dispatches into this crate's real body.
//!
//! The join-path seams wrap allocating results in [`PgResult`](types_error)
//! (the C OOM channel); our bodies return the bare `Vec`, so the installers
//! adapt with `Ok(...)`.
//!
//! This crate's own `-seams` crate
//! (`backend-optimizer-path-pathkeys-seams`) declares no inward seam (every
//! function a merged consumer calls is declared in pathnode/joinpath seams), so
//! nothing from it is installed here.

use backend_optimizer_path_joinpath_seams as jp;
use backend_optimizer_util_pathnode_seams as pn;

/// Install all pathkeys.c seam bodies. Called once at single-threaded startup
/// (wired by the parent into `seams-init`).
pub fn init_seams() {
    // --- GUC slot owned by pathkeys.c -----------------------------------
    // `bool enable_group_by_reordering = true;` (pathkeys.c). guc_tables.c
    // references this slot by C symbol; install the read/write accessor over
    // this crate's per-backend backing store.
    backend_utils_misc_guc_tables::vars::enable_group_by_reordering.install(
        backend_utils_misc_guc_tables::GucVarAccessors {
            get: crate::enable_group_by_reordering_get,
            set: crate::enable_group_by_reordering_set,
        },
    );

    // --- pathnode-seams (pathkeys.c comparison helpers) -----------------
    pn::compare_pathkeys::set(|keys1, keys2| crate::compare_pathkeys(keys1, keys2));
    pn::pathkeys_contained_in::set(|keys1, keys2| crate::pathkeys_contained_in(keys1, keys2));
    // `create_unique_path` (pathnode.c) detects constant-equated columns via the
    // pathkey machinery; install the owner body.
    pn::make_pathkeys_for_sortclauses::set(|root, mcx, sortclauses, tlist| {
        crate::make_pathkeys_for_sortclauses(root, mcx, sortclauses, tlist)
    });

    // --- joinpath-seams (the join-pathkeys family) ----------------------
    jp::build_join_pathkeys::set(|root, joinrel, jointype, outer_pathkeys| {
        Ok(crate::build_join_pathkeys(root, joinrel, jointype, outer_pathkeys))
    });
    jp::find_mergeclauses_for_outer_pathkeys::set(|root, pathkeys, restrictinfos| {
        Ok(crate::find_mergeclauses_for_outer_pathkeys(root, pathkeys, restrictinfos))
    });
    jp::select_outer_pathkeys_for_merge::set(|root, mergeclauses, joinrel| {
        Ok(crate::select_outer_pathkeys_for_merge(root, mergeclauses, joinrel))
    });
    jp::make_inner_pathkeys_for_merge::set(|root, mergeclauses, outer_pathkeys| {
        Ok(crate::make_inner_pathkeys_for_merge(root, mergeclauses, outer_pathkeys))
    });
    jp::trim_mergeclauses_for_inner_pathkeys::set(|root, mergeclauses, pathkeys| {
        Ok(crate::trim_mergeclauses_for_inner_pathkeys(root, mergeclauses, pathkeys))
    });
    jp::pathkeys_contained_in::set(|keys1, keys2| crate::pathkeys_contained_in(keys1, keys2));
    jp::pathkeys_count_contained_in::set(|keys1, keys2| {
        crate::pathkeys_count_contained_in(keys1, keys2)
    });
    jp::update_mergeclause_eclasses::set(|root, restrictinfo| {
        crate::update_mergeclause_eclasses(root, restrictinfo);
        Ok(())
    });
    jp::get_cheapest_path_for_pathkeys::set(
        |root, paths, pathkeys, required_outer, cost_criterion, require_parallel_safe| {
            crate::get_cheapest_path_for_pathkeys(
                root,
                paths,
                pathkeys,
                required_outer,
                cost_criterion,
                require_parallel_safe,
            )
        },
    );
    jp::get_cheapest_parallel_safe_total_inner::set(|root, paths| {
        crate::get_cheapest_parallel_safe_total_inner(root, paths)
    });

    // NOTE: `ec_must_be_redundant_left`/`_right` are EquivalenceClass macro
    // helpers (EC_MUST_BE_REDUNDANT, equivclass.c), NOT pathkeys.c functions —
    // left for the equivclass owner to install.

    // relnode.c reaches `has_useful_pathkeys(root, rel)` (pathkeys.c, owned here)
    // through its no-owner consumer-side ext seam crate. The ext seam keys the
    // rel by `RelId`; resolve it to the `RelOptInfo` the owner body takes.
    backend_optimizer_util_relnode_ext_seams::has_useful_pathkeys::set(|root, rel| {
        crate::has_useful_pathkeys(root, root.rel(rel))
    });
}
