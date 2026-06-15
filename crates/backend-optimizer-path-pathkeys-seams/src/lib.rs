//! Inward seam declarations for `optimizer/path/pathkeys.c`.
//!
//! pathkeys.c owns its pathkey engine, but every function a *merged* consumer
//! calls is already declared in the consumers' own seam crates: the comparison
//! helpers (`compare_pathkeys` / `pathkeys_contained_in`) in
//! `backend-optimizer-util-pathnode-seams`, and the join-pathkeys family
//! (`build_join_pathkeys`, the mergeclause matchers, the cheapest-path
//! selectors, `update_mergeclause_eclasses`, the `pathkeys_*contained_in`
//! family) in `backend-optimizer-path-joinpath-seams`. The owning crate
//! (`backend-optimizer-path-pathkeys`) installs all of those via its
//! `init_seams()`.
//!
//! There is consequently **no new inward seam** owned solely by pathkeys.c that
//! a cross-cycle consumer reaches only through this crate. This crate therefore
//! exists as the conventional per-owner `-seams` placeholder (kept so the
//! workspace shape is uniform and so any future cross-cycle pathkeys entry point
//! has a home) and declares nothing.

#![no_std]

extern crate alloc;
