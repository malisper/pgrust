//! Parametric static stencils (backend phase A): generic families,
//! runtime-parametrized from the PlanNode. NO per-query code — every
//! constant a stencil consumes comes from the node (query literals) or
//! from a stats election (planner).
//!
//! Per-worker persistent scratch discipline: each stencil keeps its decode
//! arenas / cursor caches in a thread_local keyed by its column set, so
//! hot reps allocate nothing big (the hot-shape pool-lifecycle lesson).
//!
//! P1-1 ABI: `run_<family>(ctx, node) -> AnswerSet` — typed answers, one
//! render seam (render::to_lines) at the rig edge only.

// Family modules land incrementally as they port (registry order).
pub mod code_agg; // [famA] dense-int two-level shapes (q27 lineage)
pub mod dense_domain; // [famB]
pub mod derived_key; // [famB]
pub mod distinct; // [famA] the distinct pipeline family
pub mod face_fold; // [heap v1] folds over the ScanFace seam (per-statement sources)
pub mod fused_filter_agg;
pub mod hash_group; // [famA] hash-plane shapes beyond the packed-pair form
pub mod hash_plane;
pub mod metadata;
pub mod part_merge; // per-part local codes + string-keyed combine (the end-state vocabulary)
pub mod scan_serve; // [scan] row-returning bare scans
pub mod sort_grouped; // [sortgrp v1] tier-2 order-sensitive/holistic aggregates
pub mod statepark; // capped query-agnostic per-worker state parks (persist-rehome)
pub mod window_serve; // [winserve v1] SQL window functions (OVER) over scan children
pub mod survivor_gather;
pub mod two_level;
pub mod window_replay;
pub mod zone_order;

use crate::bank::Bank;

/// Byval width of a column (0 = varlena) — the TypMeta consult (the PoC's
/// StorageClass shim is dead; currency-insertion.md §2).
pub(crate) fn col_width(bank: &Bank, attno: u32) -> u8 {
    let t = bank.typ(attno);
    if t.is_varlena() {
        0
    } else {
        t.width as u8
    }
}

/// Sign-extend a byval datum from its column width. Signedness itself is
/// a TypMeta fact — every byval type the P1-1 vocabulary admits (int2/
/// int4/int8/date/timestamp) is signed; an unsigned byval oid would
/// refuse at lowering before reaching this fold.
#[inline(always)]
pub(crate) fn sx(d: u64, w: u8) -> i64 {
    match w {
        1 => d as u8 as i8 as i64,
        2 => d as u16 as i16 as i64,
        4 => d as u32 as i32 as i64,
        _ => d as i64,
    }
}
