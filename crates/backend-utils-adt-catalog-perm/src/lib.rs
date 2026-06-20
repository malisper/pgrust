//! `utils/adt/orderedsetaggs.c` — ordered-set and hypothetical-set aggregate
//! support functions.
//!
//! This unit is the remaining body of the `backend-utils-adt-catalog-perm`
//! c2rust catalog unit after `acl.c` (→ `backend-utils-adt-acl`) and `amutils.c`
//! (→ `backend-utils-adt-amutils`) were split out.
//!
//! ## What is ported here (the datum-sort path — the common, most-tested subset)
//!
//! The single-aggregated-column ordered-set aggregates that sort bare `Datum`s:
//! `ordered_set_transition`, `ordered_set_shutdown`, the datum branch of
//! `ordered_set_startup`, `percentile_disc_final` / `percentile_disc_multi_final`,
//! `percentile_cont_float8_final` / `percentile_cont_float8_multi_final`
//! (+ `float8_lerp`, `percentile_cont_final_common`,
//! `percentile_cont_multi_final_common`, `setup_pct_info`, `pct_info_cmp`), and
//! `mode_final`. These run end-to-end on the two substrates this lane landed
//! (the generic `FmgrInfo.fn_extra` cache slot for `OSAPerQueryState`, and the
//! `AggRegisterCallback` EState-ExprContext-pool register seam for
//! `ordered_set_shutdown`) plus the `tuplesort_*` datum seams.
//!
//! ## What is NOT ported here (documented blocker — the tuple/interval path)
//!
//! The multi-input-column tuple path (`ordered_set_transition_multi`), the
//! `interval` continuous percentiles (`percentile_cont_interval_final` /
//! `_multi_final`, which need `interval_lerp` via `DirectFunctionCall2` on
//! `interval_mi`/`interval_mul`/`interval_pl`), and the whole hypothetical-set
//! family (`hypothetical_rank_final`, `hypothetical_percent_rank_final`,
//! `hypothetical_cume_dist_final`, `hypothetical_dense_rank_final`, with
//! `hypothetical_rank_common` / `hypothetical_check_argtypes`) all depend on
//! tuple-slot infrastructure not reachable from an adt crate as seams today:
//! `ExecTypeFromTL`, `MakeSingleTupleTableSlot`, `ExecStoreVirtualTuple`,
//! `slot_getattr`, `tuplesort_puttupleslot`/`gettupleslot` over a heap-tuple
//! sort, plus `execTuplesMatchPrepare` / `ExecQualAndReset` /
//! `CreateStandaloneExprContext` (dense_rank) — none of which is exposed to adt
//! crates as a seam. Those functions are therefore NOT registered (genuinely
//! absent, never stubbed): a query using them errors with the existing
//! "not in internal lookup table" message, exactly as before this lane, rather
//! than producing fake output. See the crate-level report for the precise list.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

extern crate alloc;

pub mod orderedsetaggs;

/// Register this unit's SQL-callable functions and install its seams. Called
/// from `seams-init`.
pub fn init_seams() {
    orderedsetaggs::register_orderedset_builtins();
}
