//! Seam wiring + the `void *extra_data` side table for this unit.
//!
//! This crate OWNS the inward `range_typanalyze` / `multirange_typanalyze`
//! seams (declared in `backend-utils-adt-rangetypes-typanalyze-seams`), which
//! the (unported) VACUUM ANALYZE driver invokes through `pg_type.typanalyze`.
//! [`init_seams`] installs them.
//!
//! The `void *extra_data` round-trip the C performs (`*_typanalyze` stashes the
//! typcache pointer in `stats->extra_data`; `compute_range_stats` reads it back)
//! is modeled here with a `thread_local!` registry keyed by the `u64`
//! [`VacAttrStats::extra_data`] token. The analyze driver is single-threaded
//! per backend, and the payload lifetime spans exactly the ANALYZE of one
//! column (insert at typanalyze, read at compute_stats), so a counter-keyed
//! `BTreeMap` faithfully mirrors the C pointer hand-off.

use core::cell::RefCell;

use alloc::collections::BTreeMap;

use cache::typcache::TypeCacheEntry;
use types_core::primitive::Oid;
use types_error::PgResult;
use statistics::VacAttrStats;

use crate::{compute_range_stats as _compute_range_stats, multirange_typanalyze, range_typanalyze};

/// The analyze payload the C stores in `stats->extra_data` (`void *`): the
/// (already-`rngtype`-unwrapped) range `TypeCacheEntry` plus the
/// range-vs-multirange discriminator the C reads from `typcache->typtype`.
///
/// For a multirange column the C does `typcache = typcache->rngtype` at the top
/// of `compute_range_stats`; we carry that inner range entry directly (in
/// `typcache`) and set `is_multirange = true`. `mltrng_type_oid` records the
/// multirange type's own OID (the C `mltrng_typcache->type_id`) for
/// completeness / re-lookup.
#[derive(Clone, Debug)]
pub struct RangeAnalyzeExtraData {
    /// The range `TypeCacheEntry` (the C `typcache` after the
    /// `typtype == TYPTYPE_MULTIRANGE` unwrap, or the plain range entry).
    pub typcache: TypeCacheEntry,
    /// Whether the analyzed column is a multirange (the C
    /// `typcache->typtype == TYPTYPE_MULTIRANGE`).
    pub is_multirange: bool,
    /// The multirange type's own OID (`InvalidOid` for a plain range column).
    pub mltrng_type_oid: Oid,
}

impl RangeAnalyzeExtraData {
    /// The C `typcache->typtype` value this payload stands in for: `'m'` for a
    /// multirange column, `'r'` for a range column. Used only for the C's
    /// `Assert(typcache->typtype == TYPTYPE_RANGE)` mirror.
    #[inline]
    pub fn discriminant_typtype(&self) -> i8 {
        if self.is_multirange {
            b'm' as i8
        } else {
            b'r' as i8
        }
    }
}

std::thread_local! {
    /// The `void *extra_data` side table: `stats.extra_data` token ->
    /// [`RangeAnalyzeExtraData`]. Counter-keyed, mirroring the C pointer
    /// hand-off from typanalyze to compute_stats.
    static EXTRA_DATA: RefCell<ExtraDataTable> = RefCell::new(ExtraDataTable::new());
}

struct ExtraDataTable {
    next: u64,
    map: BTreeMap<u64, RangeAnalyzeExtraData>,
}

impl ExtraDataTable {
    fn new() -> Self {
        ExtraDataTable {
            // Start at 1 so that 0 (the zero-initialized `extra_data`) is never
            // a live key (a missing-key lookup then panics loudly).
            next: 1,
            map: BTreeMap::new(),
        }
    }
}

/// Insert an analyze payload, returning the `u64` token to store in
/// `stats.extra_data` (the C `stats->extra_data = typcache`).
pub(crate) fn extra_data_put(extra: RangeAnalyzeExtraData) -> u64 {
    EXTRA_DATA.with(|t| {
        let mut t = t.borrow_mut();
        let key = t.next;
        t.next += 1;
        t.map.insert(key, extra);
        key
    })
}

/// Read the analyze payload back by its `stats.extra_data` token (the C
/// `(TypeCacheEntry *) stats->extra_data`). Panics loudly if the token is not
/// live (the analyze driver must run `*_typanalyze` before `compute_stats`).
pub(crate) fn extra_data_get(key: u64) -> RangeAnalyzeExtraData {
    EXTRA_DATA.with(|t| {
        t.borrow()
            .map
            .get(&key)
            .cloned()
            .expect("range typanalyze extra_data token not found (compute_stats ran without a preceding range_typanalyze/multirange_typanalyze)")
    })
}

// ---------------------------------------------------------------------------
// Inward seam installers.
// ---------------------------------------------------------------------------

fn range_typanalyze_seam(stats: &mut VacAttrStats<'_>) -> PgResult<bool> {
    range_typanalyze(stats)
}

fn multirange_typanalyze_seam(stats: &mut VacAttrStats<'_>) -> PgResult<bool> {
    multirange_typanalyze(stats)
}

/// Install this unit's inward seams. Called once at startup from
/// `seams-init::init_all()`.
pub fn init_seams() {
    let _ = _compute_range_stats; // referenced so the symbol is kept reachable
    rangetypes_typanalyze_seams::range_typanalyze::set(range_typanalyze_seam);
    rangetypes_typanalyze_seams::multirange_typanalyze::set(
        multirange_typanalyze_seam,
    );
}
