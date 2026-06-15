#![no_std]
#![forbid(unsafe_code)]

//! Inward seam crate for `optimizer/path/joinrels.c`.
//!
//! joinrels.c owns the join-relation enumeration entry points
//! (`join_search_one_level`, `make_join_rel`, `have_join_order_restriction`).
//! The cross-crate-cycle consumer of joinrels is the GEQO join-search driver
//! (`backend-geqo-all`), which reaches joinrels through seams it *declares* in
//! `backend-geqo-all-seams` (`build_and_cost_join_rel` →
//! `make_join_rel`, and `have_join_order_restriction`). Since those decls live
//! in the consumer's seam crate, joinrels itself introduces **no new inward
//! seam of its own**: this crate exists only so the build wiring has a
//! `*-seams` crate to reference for joinrels, mirroring the per-owner seam
//! layout. joinrels installs the `have_join_order_restriction` provider (and
//! the `build_and_cost_join_rel`-adjacent `make_join_rel` reach) from its own
//! `init_seams()` against the `backend-geqo-all-seams` decls.

extern crate alloc;
