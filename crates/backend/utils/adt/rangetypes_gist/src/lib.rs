#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

//! Owned port of PostgreSQL's `src/backend/utils/adt/rangetypes_gist.c`
//! (PostgreSQL 18.3) — the GiST support procedures for range types (the
//! `range_ops` opclass) plus the GiST support for multiranges
//! (`multirange_gist_compress` / `multirange_gist_consistent`).
//!
//! Every function in the C file is ported here 1:1 over the live range-type
//! vocabulary (`RangeTypeP<'mcx>` / `MultirangeTypeP<'mcx>` / `RangeBound` /
//! `TypeCacheEntry`, the already-ported `backend-utils-adt-rangetypes` /
//! `backend-utils-adt-multirangetypes` predicate cores) and the real GiST
//! plumbing types from `types-gist`:
//!
//!   * [`range_gist_consistent`]                  (consistent)
//!   * [`multirange_gist_compress`]               (compress)
//!   * [`multirange_gist_consistent`]             (consistent)
//!   * [`range_gist_union`]                       (union)
//!   * [`range_gist_penalty`]                     (penalty)
//!   * [`range_gist_picksplit`]                   (picksplit)
//!   * [`range_gist_same`]                        (same)
//!   * `range_super_union`                        (static)
//!   * `multirange_union_range_equal`             (static)
//!   * the six `range_gist_consistent_{int,leaf}_{range,multirange,element}`
//!   * `range_gist_fallback_split` / `range_gist_class_split` /
//!     `range_gist_single_sorting_split` / `range_gist_double_sorting_split` /
//!     `range_gist_consider_split`
//!   * `get_gist_range_class`
//!   * `single_bound_cmp` / `interval_cmp_lower` / `interval_cmp_upper` /
//!     `common_entry_cmp`                         (qsort comparators)
//!   * `call_subtype_diff`
//!
//! ## fmgr boundary
//!
//! The eight `Datum NAME(PG_FUNCTION_ARGS)` GiST support procedures take
//! `internal`-typed arguments (`GISTENTRY *`, `GistEntryVector *`,
//! `GIST_SPLITVEC *`, `bool *recheck`) that cannot cross the current fmgr
//! boundary, so no `fc_*` builtin is registered for them; the bodies are
//! surfaced as typed-Rust entry points the GiST AM calls through the opclass
//! support-function vtable (where it performs the `entry->key` /
//! `GIST_SPLITVEC` packing). `GIST_LEAF(entry)` is the explicit `is_leaf` bool;
//! `GistEntryVector` / `GIST_SPLITVEC` are the real `gist` carriers; the
//! `query` subtype dispatch (range / multirange / element) is surfaced as the
//! [`GistQuery`] enum + the `subtype: Oid`.

extern crate alloc;

use alloc::vec::Vec;

use allocator_api2::alloc::Allocator;
use mcx::Mcx;
use types_core::primitive::{Oid, OidIsValid};
use datum::datum::Datum;
use types_error::{PgError, PgResult};

use types_rangetypes::{
    MultirangeTypeP, RangeBound, RangeTypeP, RANGE_CONTAIN_EMPTY, RANGE_EMPTY, RANGE_LB_INF,
    RANGE_UB_INF,
};

use cache::typcache::TypeCacheEntry;

use adt_rangetypes::range_bounds_compare::{
    range_adjacent_internal, range_after_internal, range_before_internal, range_cmp_bounds,
    range_contained_by_internal, range_contains_elem_internal, range_contains_internal,
    range_eq_internal, range_get_typcache, range_overlaps_internal, range_overleft_internal,
    range_overright_internal,
};
use adt_rangetypes::range_canonical_subdiff_hash::range_subdiff;
use adt_rangetypes::range_repr_serialize::{
    make_range, range_deserialize, range_get_flags, range_set_contain_empty,
};

use multirangetypes::operators::{
    multirange_contains_range_internal, range_adjacent_multirange_internal,
    range_after_multirange_internal, range_before_multirange_internal,
    range_contains_multirange_internal, range_overlaps_multirange_internal,
    range_overleft_multirange_internal, range_overright_multirange_internal,
};
use multirangetypes::serialize_core::{
    multirange_get_bounds, multirange_get_union_range,
};
use multirangetypes::typcache_io::multirange_get_typcache;

use float::get_float4_infinity;

use gist::{GistEntryVector, GISTENTRY, GIST_SPLITVEC};

pub mod fmgr_builtins;

/// Wire this crate's outward seams (C: this translation unit's contribution to
/// the runtime fmgr/opclass tables). Registers the range/multirange GiST
/// support procedures' `fmgr_builtins[]` rows so `index_getprocinfo` →
/// `fmgr_info` can resolve them when building a `GISTSTATE` for a
/// `range_ops` / `multirange_ops` index; the typed by-OID dispatch of the
/// bodies themselves is installed by `backend-access-gist-proc` (the single
/// installer of the GiST core dispatch seams), which folds the range/multirange
/// OIDs into its dispatchers.
pub fn init_seams() {
    fmgr_builtins::register_rangetypes_gist_builtins();
}

// ---------------------------------------------------------------------------
// Constants (verbatim from the C #defines / #includes).
// ---------------------------------------------------------------------------

/// `ANYRANGEOID` (catalog/pg_type_d.h).
pub const ANYRANGEOID: Oid = 3831;
/// `ANYMULTIRANGEOID` (catalog/pg_type_d.h).
pub const ANYMULTIRANGEOID: Oid = 4537;

// Range GiST class properties (rangetypes_gist.c:30-38).
/// `CLS_NORMAL` — ordinary finite range (no bits set).
pub const CLS_NORMAL: usize = 0;
/// `CLS_LOWER_INF` — lower bound is infinity.
pub const CLS_LOWER_INF: usize = 1;
/// `CLS_UPPER_INF` — upper bound is infinity.
pub const CLS_UPPER_INF: usize = 2;
/// `CLS_CONTAIN_EMPTY` — contains underlying empty ranges.
pub const CLS_CONTAIN_EMPTY: usize = 4;
/// `CLS_EMPTY` — special class for empty ranges.
pub const CLS_EMPTY: usize = 8;
/// `CLS_COUNT` — # of classes (2^3 + 1; `CLS_EMPTY` doesn't combine).
pub const CLS_COUNT: usize = 9;

/// `LIMIT_RATIO` — minimum accepted ratio of split for items of the same class.
pub const LIMIT_RATIO: f64 = 0.3;

/// `INFINITE_BOUND_PENALTY`.
pub const INFINITE_BOUND_PENALTY: f32 = 2.0;
/// `CONTAIN_EMPTY_PENALTY`.
pub const CONTAIN_EMPTY_PENALTY: f32 = 1.0;
/// `DEFAULT_SUBTYPE_DIFF_PENALTY`.
pub const DEFAULT_SUBTYPE_DIFF_PENALTY: f32 = 1.0;

// Range operator strategy numbers (utils/rangetypes.h, mapped to RT* in
// access/stratnum.h).
/// `RANGESTRAT_BEFORE` == `RTLeftStrategyNumber`.
pub const RANGESTRAT_BEFORE: u16 = 1;
/// `RANGESTRAT_OVERLEFT` == `RTOverLeftStrategyNumber`.
pub const RANGESTRAT_OVERLEFT: u16 = 2;
/// `RANGESTRAT_OVERLAPS` == `RTOverlapStrategyNumber`.
pub const RANGESTRAT_OVERLAPS: u16 = 3;
/// `RANGESTRAT_OVERRIGHT` == `RTOverRightStrategyNumber`.
pub const RANGESTRAT_OVERRIGHT: u16 = 4;
/// `RANGESTRAT_AFTER` == `RTRightStrategyNumber`.
pub const RANGESTRAT_AFTER: u16 = 5;
/// `RANGESTRAT_ADJACENT` == `RTSameStrategyNumber`.
pub const RANGESTRAT_ADJACENT: u16 = 6;
/// `RANGESTRAT_CONTAINS` == `RTContainsStrategyNumber`.
pub const RANGESTRAT_CONTAINS: u16 = 7;
/// `RANGESTRAT_CONTAINED_BY` == `RTContainedByStrategyNumber`.
pub const RANGESTRAT_CONTAINED_BY: u16 = 8;
/// `RANGESTRAT_CONTAINS_ELEM` == `RTContainsElemStrategyNumber`.
pub const RANGESTRAT_CONTAINS_ELEM: u16 = 16;
/// `RANGESTRAT_EQ` == `RTEqualStrategyNumber`.
pub const RANGESTRAT_EQ: u16 = 18;

/// `FirstOffsetNumber` (storage/off.h).
pub const FIRST_OFFSET_NUMBER: usize = 1;

// ---------------------------------------------------------------------------
// SplitLR (rangetypes_gist.c:62-66).
// ---------------------------------------------------------------------------

/// `SplitLR` — place on left or right side of split?
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SplitLR {
    /// `SPLIT_LEFT = 0` (makes initialization to SPLIT_LEFT easier).
    Left = 0,
    /// `SPLIT_RIGHT`.
    Right,
}

/// The GiST `query` argument of the consistent functions, after the fmgr
/// `Datum`/subtype dispatch — a range, a multirange, or a bare element value.
pub enum GistQuery<'mcx> {
    /// `DatumGetRangeTypeP(query)` (subtype invalid or `ANYRANGEOID`).
    Range(RangeTypeP<'mcx>),
    /// `DatumGetMultirangeTypeP(query)` (subtype `ANYMULTIRANGEOID`).
    Multirange(MultirangeTypeP<'mcx>),
    /// The element value (any other subtype) — the bare element `Datum`.
    Elem(Datum),
}

// ===========================================================================
// Public GiST support functions.
// ===========================================================================

/// `range_gist_consistent` (rangetypes_gist.c:190) — GiST query consistency
/// check. `is_leaf` is `GIST_LEAF(entry)`; `recheck` is always set to `false`
/// (all operators served by this function are exact). Returns `(result,
/// recheck)`.
pub fn range_gist_consistent<'mcx>(
    mcx: Mcx<'mcx>,
    is_leaf: bool,
    key: RangeTypeP<'mcx>,
    query: &GistQuery<'mcx>,
    strategy: u16,
    subtype: Oid,
) -> PgResult<(bool, bool)> {
    // All operators served by this function are exact.
    let recheck = false;

    let typcache = range_get_typcache(key.rangetypid())?;

    let result = if is_leaf {
        match query {
            GistQuery::Range(q) if !oid_is_valid(subtype) || subtype == ANYRANGEOID => {
                range_gist_consistent_leaf_range(mcx, &typcache, strategy, key, *q)?
            }
            GistQuery::Multirange(q) if subtype == ANYMULTIRANGEOID => {
                range_gist_consistent_leaf_multirange(mcx, &typcache, strategy, key, *q)?
            }
            _ => range_gist_consistent_leaf_element(&typcache, strategy, key, query_elem(query)?)?,
        }
    } else {
        match query {
            GistQuery::Range(q) if !oid_is_valid(subtype) || subtype == ANYRANGEOID => {
                range_gist_consistent_int_range(mcx, &typcache, strategy, key, *q)?
            }
            GistQuery::Multirange(q) if subtype == ANYMULTIRANGEOID => {
                range_gist_consistent_int_multirange(mcx, &typcache, strategy, key, *q)?
            }
            _ => range_gist_consistent_int_element(&typcache, strategy, key, query_elem(query)?)?,
        }
    };
    Ok((result, recheck))
}

/// `multirange_gist_compress` (rangetypes_gist.c:244) — GiST compress method for
/// multiranges: a multirange is approximated as its union range with no gaps.
///
/// `leafkey` is the C `entry->leafkey`. When `leafkey`, returns the union range
/// as a fresh `GISTENTRY` (C `gistentryinit(*retval, RangeTypePGetDatum(r),
/// rel, page, offset, false)`); otherwise the entry passes through unchanged.
pub fn multirange_gist_compress<'mcx>(
    mcx: Mcx<'mcx>,
    entry: &GISTENTRY<'mcx>,
    mr: MultirangeTypeP<'mcx>,
) -> PgResult<GISTENTRY<'mcx>> {
    if entry.leafkey {
        // typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));
        // r = multirange_get_union_range(typcache->rngtype, mr);
        let typcache = multirange_get_typcache(mr.multirangetypid())?;
        let rngtype = typcache
            .rngtype
            .as_ref()
            .ok_or_else(|| elog_error("multirange typcache->rngtype must be set"))?;
        let r = multirange_get_union_range(mcx, rngtype, mr)?;

        return Ok(GISTENTRY {
            key: range_type_p_get_datum(r),
            rel: entry.rel,
            page: entry.page,
            offset: entry.offset,
            leafkey: false,
        });
    }

    Ok(entry.clone())
}

/// `multirange_gist_consistent` (rangetypes_gist.c:269) — GiST query consistency
/// check for multiranges. `recheck` is always `true` (multirange is
/// approximated by its union range with no gaps). Returns `(result, recheck)`.
pub fn multirange_gist_consistent<'mcx>(
    mcx: Mcx<'mcx>,
    is_leaf: bool,
    key: RangeTypeP<'mcx>,
    query: &GistQuery<'mcx>,
    strategy: u16,
    subtype: Oid,
) -> PgResult<(bool, bool)> {
    // All operators served by this function are inexact.
    let recheck = true;

    let typcache = range_get_typcache(key.rangetypid())?;

    let result = if is_leaf {
        match query {
            GistQuery::Multirange(q) if !oid_is_valid(subtype) || subtype == ANYMULTIRANGEOID => {
                range_gist_consistent_leaf_multirange(mcx, &typcache, strategy, key, *q)?
            }
            GistQuery::Range(q) if subtype == ANYRANGEOID => {
                range_gist_consistent_leaf_range(mcx, &typcache, strategy, key, *q)?
            }
            _ => range_gist_consistent_leaf_element(&typcache, strategy, key, query_elem(query)?)?,
        }
    } else {
        match query {
            GistQuery::Multirange(q) if !oid_is_valid(subtype) || subtype == ANYMULTIRANGEOID => {
                range_gist_consistent_int_multirange(mcx, &typcache, strategy, key, *q)?
            }
            GistQuery::Range(q) if subtype == ANYRANGEOID => {
                range_gist_consistent_int_range(mcx, &typcache, strategy, key, *q)?
            }
            _ => range_gist_consistent_int_element(&typcache, strategy, key, query_elem(query)?)?,
        }
    };
    Ok((result, recheck))
}

/// `range_gist_union` (rangetypes_gist.c:323) — form the union range over the
/// page's entry keys. The C indexes the 0-based `GISTENTRY *ent =
/// entryvec->vector` array (`ent[0] .. ent[entryvec->n - 1]`).
pub fn range_gist_union<'mcx>(
    mcx: Mcx<'mcx>,
    entryvec: &GistEntryVector<'mcx>,
) -> PgResult<RangeTypeP<'mcx>> {
    // result_range = DatumGetRangeTypeP(ent[0].key);
    let mut result_range = entry_range(entryvec, 0)?;

    let typcache = range_get_typcache(result_range.rangetypid())?;

    // for (i = 1; i < entryvec->n; i++)
    for i in 1..entryvec.n as usize {
        let r2 = entry_range(entryvec, i)?;
        result_range = range_super_union(mcx, &typcache, result_range, r2)?;
    }

    Ok(result_range)
}

/// `range_gist_penalty` (rangetypes_gist.c:361) — GiST page split penalty
/// function. Returns the penalty value `*penalty`.
pub fn range_gist_penalty<'mcx>(
    mcx: Mcx<'mcx>,
    orig: RangeTypeP<'mcx>,
    new: RangeTypeP<'mcx>,
) -> PgResult<f32> {
    if orig.rangetypid() != new.rangetypid() {
        return Err(elog_error("range types do not match"));
    }

    let typcache = range_get_typcache(orig.rangetypid())?;

    // bool has_subtype_diff = OidIsValid(typcache->rng_subdiff_finfo.fn_oid);
    let has_subtype_diff = OidIsValid(typcache.rng_subdiff_finfo.fn_oid);

    let (orig_lower, orig_upper, orig_empty) = range_deserialize(&typcache, orig)?;
    let (new_lower, new_upper, new_empty) = range_deserialize(&typcache, new)?;

    let penalty: f32;

    if new_empty {
        // Handle insertion of empty range.
        if orig_empty {
            penalty = 0.0;
        } else if range_is_or_contains_empty(orig) {
            penalty = CONTAIN_EMPTY_PENALTY;
        } else if orig_lower.infinite && orig_upper.infinite {
            penalty = 2.0 * CONTAIN_EMPTY_PENALTY;
        } else if orig_lower.infinite || orig_upper.infinite {
            penalty = 3.0 * CONTAIN_EMPTY_PENALTY;
        } else {
            penalty = 4.0 * CONTAIN_EMPTY_PENALTY;
        }
    } else if new_lower.infinite && new_upper.infinite {
        // Handle insertion of (-inf, +inf) range.
        let mut p: f32;
        if orig_lower.infinite && orig_upper.infinite {
            p = 0.0;
        } else if orig_lower.infinite || orig_upper.infinite {
            p = INFINITE_BOUND_PENALTY;
        } else {
            p = 2.0 * INFINITE_BOUND_PENALTY;
        }

        if range_is_or_contains_empty(orig) {
            p += CONTAIN_EMPTY_PENALTY;
        }
        penalty = p;
    } else if new_lower.infinite {
        // Handle insertion of (-inf, x) range.
        if !orig_empty && orig_lower.infinite {
            if orig_upper.infinite {
                penalty = 0.0;
            } else if range_cmp_bounds(&typcache, &new_upper, &orig_upper)? > 0 {
                if has_subtype_diff {
                    penalty = call_subtype_diff(&typcache, new_upper.val, orig_upper.val)? as f32;
                } else {
                    penalty = DEFAULT_SUBTYPE_DIFF_PENALTY;
                }
            } else {
                penalty = 0.0;
            }
        } else {
            penalty = get_float4_infinity();
        }
    } else if new_upper.infinite {
        // Handle insertion of (x, +inf) range.
        if !orig_empty && orig_upper.infinite {
            if orig_lower.infinite {
                penalty = 0.0;
            } else if range_cmp_bounds(&typcache, &new_lower, &orig_lower)? < 0 {
                if has_subtype_diff {
                    penalty = call_subtype_diff(&typcache, orig_lower.val, new_lower.val)? as f32;
                } else {
                    penalty = DEFAULT_SUBTYPE_DIFF_PENALTY;
                }
            } else {
                penalty = 0.0;
            }
        } else {
            penalty = get_float4_infinity();
        }
    } else {
        // Handle insertion of normal (non-empty, non-infinite) range.
        if orig_empty || orig_lower.infinite || orig_upper.infinite {
            penalty = get_float4_infinity();
        } else {
            let mut diff: f64 = 0.0;

            if range_cmp_bounds(&typcache, &new_lower, &orig_lower)? < 0 {
                if has_subtype_diff {
                    diff += call_subtype_diff(&typcache, orig_lower.val, new_lower.val)?;
                } else {
                    diff += DEFAULT_SUBTYPE_DIFF_PENALTY as f64;
                }
            }
            if range_cmp_bounds(&typcache, &new_upper, &orig_upper)? > 0 {
                if has_subtype_diff {
                    diff += call_subtype_diff(&typcache, new_upper.val, orig_upper.val)?;
                } else {
                    diff += DEFAULT_SUBTYPE_DIFF_PENALTY as f64;
                }
            }
            penalty = diff as f32;
        }
    }

    let _ = mcx;
    Ok(penalty)
}

/// `range_gist_picksplit` (rangetypes_gist.c:618) — the GiST PickSplit method
/// for ranges. Fills and returns an owned [`GIST_SPLITVEC`].
pub fn range_gist_picksplit<'mcx>(
    mcx: Mcx<'mcx>,
    entryvec: &GistEntryVector<'mcx>,
) -> PgResult<GIST_SPLITVEC<'mcx>> {
    let mut v = GIST_SPLITVEC::default();

    // use first item to look up range type's info
    let pred_left = entry_range(entryvec, FIRST_OFFSET_NUMBER)?;
    let typcache = range_get_typcache(pred_left.rangetypid())?;

    let maxoff = entryvec.n as usize - 1;

    // v->spl_left = palloc((maxoff + 1) * sizeof(OffsetNumber)); — reserve the
    // offset arrays up front (fallibly, like the palloc).
    v.spl_left.try_reserve(maxoff + 1).map_err(|_| out_of_memory())?;
    v.spl_right.try_reserve(maxoff + 1).map_err(|_| out_of_memory())?;

    // Get count distribution of range classes.
    let mut count_in_classes = [0i32; CLS_COUNT];
    for i in FIRST_OFFSET_NUMBER..=maxoff {
        let range = entry_range(entryvec, i)?;
        count_in_classes[get_gist_range_class(range)] += 1;
    }

    // Count non-empty classes and find biggest class.
    let total_count = maxoff;
    let mut non_empty_classes_count = 0;
    let mut biggest_class: i32 = -1;
    let mut biggest_class_count = 0;
    for j in 0..CLS_COUNT {
        if count_in_classes[j] > 0 {
            if count_in_classes[j] > biggest_class_count {
                biggest_class_count = count_in_classes[j];
                biggest_class = j as i32;
            }
            non_empty_classes_count += 1;
        }
    }

    debug_assert!(non_empty_classes_count > 0);

    if non_empty_classes_count == 1 {
        // One non-empty class, so split inside class.
        let bc = biggest_class as usize;
        if (bc & !CLS_CONTAIN_EMPTY) == CLS_NORMAL {
            range_gist_double_sorting_split(mcx, &typcache, entryvec, &mut v)?;
        } else if (bc & !CLS_CONTAIN_EMPTY) == CLS_LOWER_INF {
            range_gist_single_sorting_split(mcx, &typcache, entryvec, &mut v, true)?;
        } else if (bc & !CLS_CONTAIN_EMPTY) == CLS_UPPER_INF {
            range_gist_single_sorting_split(mcx, &typcache, entryvec, &mut v, false)?;
        } else {
            range_gist_fallback_split(mcx, &typcache, entryvec, &mut v)?;
        }
    } else {
        // Class based split: initialize all classes to the left side.
        let mut classes_groups = [SplitLR::Left; CLS_COUNT];

        if count_in_classes[CLS_NORMAL] > 0 {
            classes_groups[CLS_NORMAL] = SplitLR::Right;
        } else {
            let non_inf_count = count_in_classes[CLS_NORMAL]
                + count_in_classes[CLS_CONTAIN_EMPTY]
                + count_in_classes[CLS_EMPTY];
            let inf_count = total_count as i32 - non_inf_count;

            let non_empty_count = count_in_classes[CLS_NORMAL]
                + count_in_classes[CLS_LOWER_INF]
                + count_in_classes[CLS_UPPER_INF]
                + count_in_classes[CLS_LOWER_INF | CLS_UPPER_INF];
            let empty_count = total_count as i32 - non_empty_count;

            if inf_count > 0
                && non_inf_count > 0
                && (inf_count - non_inf_count).abs() <= (empty_count - non_empty_count).abs()
            {
                classes_groups[CLS_NORMAL] = SplitLR::Right;
                classes_groups[CLS_CONTAIN_EMPTY] = SplitLR::Right;
                classes_groups[CLS_EMPTY] = SplitLR::Right;
            } else if empty_count > 0 && non_empty_count > 0 {
                classes_groups[CLS_NORMAL] = SplitLR::Right;
                classes_groups[CLS_LOWER_INF] = SplitLR::Right;
                classes_groups[CLS_UPPER_INF] = SplitLR::Right;
                classes_groups[CLS_LOWER_INF | CLS_UPPER_INF] = SplitLR::Right;
            } else {
                // Either total_count == emptyCount or total_count == infCount.
                classes_groups[biggest_class as usize] = SplitLR::Right;
            }
        }

        range_gist_class_split(mcx, &typcache, entryvec, &mut v, &classes_groups)?;
    }

    Ok(v)
}

/// `range_gist_same` (rangetypes_gist.c:777) — equality comparator for GiST.
/// Returns `*result`.
pub fn range_gist_same(r1: RangeTypeP<'_>, r2: RangeTypeP<'_>) -> PgResult<bool> {
    // range_eq will ignore the RANGE_CONTAIN_EMPTY flag, so we test all flag
    // bits at once first.
    if range_get_flags(r1) != range_get_flags(r2) {
        Ok(false)
    } else {
        let typcache = range_get_typcache(r1.rangetypid())?;
        range_eq_internal(&typcache, r1, r2)
    }
}

// ===========================================================================
// Sort support (rangetypes.c — the range_ops GiST sortsupport comparator).
//
// `range_sortsupport` installs `range_fast_cmp` as the SortSupport comparator;
// the GiST sorted index build (`gist_indexsortbuild`) sorts the leaf keys with
// it. The comparator lives here (rather than in `rangetypes`) because the GiST
// sortsupport substrate seam is `Datum`-typed over `types_tuple::Datum` and the
// install is driven from the GiST `gist_sortsupport` dispatch; the kernel is a
// thin re-use of the already-ported `range_deserialize` / `range_cmp_bounds`.
// ===========================================================================

/// `range_fast_cmp(Datum a, Datum b, SortSupport ssup)` (rangetypes.c:1306) —
/// the SortSupport comparator `range_sortsupport` installs. Both operands are
/// pass-by-reference `RangeType *` images (`DatumGetRangeTypeP`): empty ranges
/// sort before all else; otherwise compare lower bounds, breaking ties on the
/// upper bound. The C `ssup_extra` typcache cache is a per-sort optimization;
/// the OID is read from the (identical, per the `Assert`) range image each call
/// and the typcache resolved through the installed seam, which is correct (the
/// resolution is memoized by the typcache owner). The fallible deserialize /
/// typcache surface is re-raised through the fmgr `catch_unwind` boundary the
/// sort engine runs the comparator under.
pub fn range_fast_cmp(
    a: types_tuple::heaptuple::Datum<'_>,
    b: types_tuple::heaptuple::Datum<'_>,
) -> i32 {
    match range_fast_cmp_inner(a, b) {
        Ok(c) => c,
        Err(e) => std::panic::panic_any(e),
    }
}

fn range_fast_cmp_inner(
    a: types_tuple::heaptuple::Datum<'_>,
    b: types_tuple::heaptuple::Datum<'_>,
) -> PgResult<i32> {
    // C's `DatumGetRangeTypeP` is `PG_DETOAST_DATUM`, which un-packs a short
    // (1-byte-header) on-disk image to the 4-byte form. Under
    // `SHORT_VARLENA_PACKING`=ON the GiST sorted-build sort engine hands this
    // comparator the verbatim short-packed leaf-key images; reading the
    // `RangeType` struct (`rangetypid` at `sizeof(RangeType)`, bounds past it)
    // directly off a short image lands every field 3 bytes off -> wrong rangetypid
    // ("type with OID N does not exist"). Materialize each operand into a fresh
    // 8-aligned 4-byte-header `mcx` image first (the buffers live for this call).
    // While the flag is OFF every key is already 4B so this is a verbatim copy.
    let ctx = mcx::MemoryContext::new("range_fast_cmp");
    let mcx = ctx.mcx();
    let range_a = materialize_range_arg(mcx, &a)?;
    let range_b = materialize_range_arg(mcx, &b)?;

    // cache the range info between calls — Assert(RangeTypeGetOid(a) == ...(b)).
    let typcache = range_get_typcache(range_a.rangetypid())?;

    let (lower1, upper1, empty1) = range_deserialize(&typcache, range_a)?;
    let (lower2, upper2, empty2) = range_deserialize(&typcache, range_b)?;

    // For b-tree use, empty ranges sort before all else.
    let cmp = if empty1 && empty2 {
        0
    } else if empty1 {
        -1
    } else if empty2 {
        1
    } else {
        let c = range_cmp_bounds(&typcache, &lower1, &lower2)?;
        if c == 0 {
            range_cmp_bounds(&typcache, &upper1, &upper2)?
        } else {
            c
        }
    };

    Ok(cmp)
}

// ===========================================================================
// STATIC FUNCTIONS
// ===========================================================================

/// `range_super_union` (rangetypes_gist.c:820) — the smallest range that
/// contains `r1` and `r2`. Differs from `range_union`: absorbs intervening
/// values for non-adjacent ranges, and tracks whether any empty range was
/// union'd in (`RANGE_CONTAIN_EMPTY`). Takes ownership of `r1` so the C "return
/// r1 as-is" / `rangeCopy(r1)` paths map to moving / copying the value.
fn range_super_union<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    r1: RangeTypeP<'mcx>,
    r2: RangeTypeP<'mcx>,
) -> PgResult<RangeTypeP<'mcx>> {
    let (lower1, upper1, empty1) = range_deserialize(typcache, r1)?;
    let (lower2, upper2, empty2) = range_deserialize(typcache, r2)?;
    let flags1 = range_get_flags(r1);
    let flags2 = range_get_flags(r2);

    if empty1 {
        // We can return r2 as-is if it already is or contains empty.
        if flags2 & (RANGE_EMPTY | RANGE_CONTAIN_EMPTY) != 0 {
            return Ok(r2);
        }
        // Else we'd better copy it (modify-in-place isn't safe).
        let r2c = range_copy(mcx, r2)?;
        range_set_contain_empty(r2c);
        return Ok(r2c);
    }
    if empty2 {
        // We can return r1 as-is if it already is or contains empty.
        if flags1 & (RANGE_EMPTY | RANGE_CONTAIN_EMPTY) != 0 {
            return Ok(r1);
        }
        let r1c = range_copy(mcx, r1)?;
        range_set_contain_empty(r1c);
        return Ok(r1c);
    }

    // result_lower / result_upper select between the two ranges' bounds. Track
    // which range each comes from to mirror the C pointer identity comparisons
    // that drive the optimization below.
    let lower_from_1 = range_cmp_bounds(typcache, &lower1, &lower2)? <= 0;
    let upper_from_1 = range_cmp_bounds(typcache, &upper1, &upper2)? >= 0;

    // optimization to avoid constructing a new range
    if lower_from_1
        && upper_from_1
        && ((flags1 & RANGE_CONTAIN_EMPTY != 0) || (flags2 & RANGE_CONTAIN_EMPTY == 0))
    {
        return Ok(r1);
    }
    if !lower_from_1
        && !upper_from_1
        && ((flags2 & RANGE_CONTAIN_EMPTY != 0) || (flags1 & RANGE_CONTAIN_EMPTY == 0))
    {
        return Ok(r2);
    }

    let result_lower = if lower_from_1 { &lower1 } else { &lower2 };
    let result_upper = if upper_from_1 { &upper1 } else { &upper2 };

    let result = make_range(mcx, typcache, result_lower, result_upper, false)?;

    if (flags1 & RANGE_CONTAIN_EMPTY != 0) || (flags2 & RANGE_CONTAIN_EMPTY != 0) {
        range_set_contain_empty(result);
    }

    Ok(result)
}

/// `multirange_union_range_equal` (rangetypes_gist.c:887).
fn multirange_union_range_equal(
    typcache: &TypeCacheEntry,
    r: RangeTypeP<'_>,
    mr: MultirangeTypeP<'_>,
) -> PgResult<bool> {
    if range_is_empty(r) || multirange_is_empty(mr) {
        return Ok(range_is_empty(r) && multirange_is_empty(mr));
    }

    let (lower1, upper1, empty) = range_deserialize(typcache, r)?;
    debug_assert!(!empty);

    let (lower2, _tmp) = multirange_get_bounds(typcache, mr, 0)?;
    let (_tmp2, upper2) = multirange_get_bounds(typcache, mr, mr.range_count() - 1)?;

    Ok(range_cmp_bounds(typcache, &lower1, &lower2)? == 0
        && range_cmp_bounds(typcache, &upper1, &upper2)? == 0)
}

/// `range_gist_consistent_int_range` (rangetypes_gist.c:914).
fn range_gist_consistent_int_range<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    strategy: u16,
    key: RangeTypeP<'mcx>,
    query: RangeTypeP<'mcx>,
) -> PgResult<bool> {
    match strategy {
        RANGESTRAT_BEFORE => {
            if range_is_empty(key) || range_is_empty(query) {
                return Ok(false);
            }
            Ok(!range_overright_internal(typcache, key, query)?)
        }
        RANGESTRAT_OVERLEFT => {
            if range_is_empty(key) || range_is_empty(query) {
                return Ok(false);
            }
            Ok(!range_after_internal(typcache, key, query)?)
        }
        RANGESTRAT_OVERLAPS => range_overlaps_internal(typcache, key, query),
        RANGESTRAT_OVERRIGHT => {
            if range_is_empty(key) || range_is_empty(query) {
                return Ok(false);
            }
            Ok(!range_before_internal(typcache, key, query)?)
        }
        RANGESTRAT_AFTER => {
            if range_is_empty(key) || range_is_empty(query) {
                return Ok(false);
            }
            Ok(!range_overleft_internal(typcache, key, query)?)
        }
        RANGESTRAT_ADJACENT => {
            if range_is_empty(key) || range_is_empty(query) {
                return Ok(false);
            }
            if range_adjacent_internal(mcx, typcache, key, query)? {
                return Ok(true);
            }
            range_overlaps_internal(typcache, key, query)
        }
        RANGESTRAT_CONTAINS => range_contains_internal(typcache, key, query),
        RANGESTRAT_CONTAINED_BY => {
            // Empty ranges are contained by anything, so if key is or contains
            // any empty ranges, we must descend into it. Otherwise, descend
            // only if key overlaps the query.
            if range_is_or_contains_empty(key) {
                return Ok(true);
            }
            range_overlaps_internal(typcache, key, query)
        }
        RANGESTRAT_EQ => {
            // If query is empty, descend only if the key is or contains any
            // empty ranges. Otherwise, descend if key contains query.
            if range_is_empty(query) {
                return Ok(range_is_or_contains_empty(key));
            }
            range_contains_internal(typcache, key, query)
        }
        _ => Err(unrecognized_range_strategy(strategy)),
    }
}

/// `range_gist_consistent_int_multirange` (rangetypes_gist.c:976).
fn range_gist_consistent_int_multirange<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    strategy: u16,
    key: RangeTypeP<'mcx>,
    query: MultirangeTypeP<'mcx>,
) -> PgResult<bool> {
    let _ = mcx;
    match strategy {
        RANGESTRAT_BEFORE => {
            if range_is_empty(key) || multirange_is_empty(query) {
                return Ok(false);
            }
            Ok(!range_overright_multirange_internal(typcache, key, query)?)
        }
        RANGESTRAT_OVERLEFT => {
            if range_is_empty(key) || multirange_is_empty(query) {
                return Ok(false);
            }
            Ok(!range_after_multirange_internal(typcache, key, query)?)
        }
        RANGESTRAT_OVERLAPS => range_overlaps_multirange_internal(typcache, key, query),
        RANGESTRAT_OVERRIGHT => {
            if range_is_empty(key) || multirange_is_empty(query) {
                return Ok(false);
            }
            Ok(!range_before_multirange_internal(typcache, key, query)?)
        }
        RANGESTRAT_AFTER => {
            if range_is_empty(key) || multirange_is_empty(query) {
                return Ok(false);
            }
            Ok(!range_overleft_multirange_internal(typcache, key, query)?)
        }
        RANGESTRAT_ADJACENT => {
            if range_is_empty(key) || multirange_is_empty(query) {
                return Ok(false);
            }
            if range_adjacent_multirange_internal(typcache, key, query)? {
                return Ok(true);
            }
            range_overlaps_multirange_internal(typcache, key, query)
        }
        RANGESTRAT_CONTAINS => range_contains_multirange_internal(typcache, key, query),
        RANGESTRAT_CONTAINED_BY => {
            if range_is_or_contains_empty(key) {
                return Ok(true);
            }
            range_overlaps_multirange_internal(typcache, key, query)
        }
        RANGESTRAT_EQ => {
            if multirange_is_empty(query) {
                return Ok(range_is_or_contains_empty(key));
            }
            range_contains_multirange_internal(typcache, key, query)
        }
        _ => Err(unrecognized_range_strategy(strategy)),
    }
}

/// `range_gist_consistent_int_element` (rangetypes_gist.c:1038).
fn range_gist_consistent_int_element(
    typcache: &TypeCacheEntry,
    strategy: u16,
    key: RangeTypeP<'_>,
    query: Datum,
) -> PgResult<bool> {
    match strategy {
        RANGESTRAT_CONTAINS_ELEM => range_contains_elem_internal(typcache, key, query),
        _ => Err(unrecognized_range_strategy(strategy)),
    }
}

/// `range_gist_consistent_leaf_range` (rangetypes_gist.c:1057).
fn range_gist_consistent_leaf_range<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    strategy: u16,
    key: RangeTypeP<'mcx>,
    query: RangeTypeP<'mcx>,
) -> PgResult<bool> {
    match strategy {
        RANGESTRAT_BEFORE => range_before_internal(typcache, key, query),
        RANGESTRAT_OVERLEFT => range_overleft_internal(typcache, key, query),
        RANGESTRAT_OVERLAPS => range_overlaps_internal(typcache, key, query),
        RANGESTRAT_OVERRIGHT => range_overright_internal(typcache, key, query),
        RANGESTRAT_AFTER => range_after_internal(typcache, key, query),
        RANGESTRAT_ADJACENT => range_adjacent_internal(mcx, typcache, key, query),
        RANGESTRAT_CONTAINS => range_contains_internal(typcache, key, query),
        RANGESTRAT_CONTAINED_BY => range_contained_by_internal(typcache, key, query),
        RANGESTRAT_EQ => range_eq_internal(typcache, key, query),
        _ => Err(unrecognized_range_strategy(strategy)),
    }
}

/// `range_gist_consistent_leaf_multirange` (rangetypes_gist.c:1092).
fn range_gist_consistent_leaf_multirange<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    strategy: u16,
    key: RangeTypeP<'mcx>,
    query: MultirangeTypeP<'mcx>,
) -> PgResult<bool> {
    let _ = mcx;
    match strategy {
        RANGESTRAT_BEFORE => range_before_multirange_internal(typcache, key, query),
        RANGESTRAT_OVERLEFT => range_overleft_multirange_internal(typcache, key, query),
        RANGESTRAT_OVERLAPS => range_overlaps_multirange_internal(typcache, key, query),
        RANGESTRAT_OVERRIGHT => range_overright_multirange_internal(typcache, key, query),
        RANGESTRAT_AFTER => range_after_multirange_internal(typcache, key, query),
        RANGESTRAT_ADJACENT => range_adjacent_multirange_internal(typcache, key, query),
        RANGESTRAT_CONTAINS => range_contains_multirange_internal(typcache, key, query),
        RANGESTRAT_CONTAINED_BY => multirange_contains_range_internal(typcache, query, key),
        RANGESTRAT_EQ => multirange_union_range_equal(typcache, key, query),
        _ => Err(unrecognized_range_strategy(strategy)),
    }
}

/// `range_gist_consistent_leaf_element` (rangetypes_gist.c:1127).
fn range_gist_consistent_leaf_element(
    typcache: &TypeCacheEntry,
    strategy: u16,
    key: RangeTypeP<'_>,
    query: Datum,
) -> PgResult<bool> {
    match strategy {
        RANGESTRAT_CONTAINS_ELEM => range_contains_elem_internal(typcache, key, query),
        _ => Err(unrecognized_range_strategy(strategy)),
    }
}

/// `range_gist_fallback_split` (rangetypes_gist.c:1147) — trivial split: half of
/// entries on one page and the other half on the other.
fn range_gist_fallback_split<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    entryvec: &GistEntryVector<'mcx>,
    v: &mut GIST_SPLITVEC<'mcx>,
) -> PgResult<()> {
    let mut left_range: Option<RangeTypeP<'mcx>> = None;
    let mut right_range: Option<RangeTypeP<'mcx>> = None;

    let maxoff = entryvec.n as usize - 1;
    // Split entries before this to left page, after to right:
    let split_idx = (maxoff - FIRST_OFFSET_NUMBER) / 2 + FIRST_OFFSET_NUMBER;

    v.spl_left.clear();
    v.spl_right.clear();
    for i in FIRST_OFFSET_NUMBER..=maxoff {
        let range = entry_range(entryvec, i)?;
        if i < split_idx {
            place_left(mcx, typcache, &mut left_range, v, range, i as u16)?;
        } else {
            place_right(mcx, typcache, &mut right_range, v, range, i as u16)?;
        }
    }

    set_ldatum(v, left_range);
    set_rdatum(v, right_range);
    Ok(())
}

/// `range_gist_class_split` (rangetypes_gist.c:1185) — split based on classes.
fn range_gist_class_split<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    entryvec: &GistEntryVector<'mcx>,
    v: &mut GIST_SPLITVEC<'mcx>,
    classes_groups: &[SplitLR; CLS_COUNT],
) -> PgResult<()> {
    let mut left_range: Option<RangeTypeP<'mcx>> = None;
    let mut right_range: Option<RangeTypeP<'mcx>> = None;

    let maxoff = entryvec.n as usize - 1;

    v.spl_left.clear();
    v.spl_right.clear();
    for i in FIRST_OFFSET_NUMBER..=maxoff {
        let range = entry_range(entryvec, i)?;
        let class = get_gist_range_class(range);

        if classes_groups[class] == SplitLR::Left {
            place_left(mcx, typcache, &mut left_range, v, range, i as u16)?;
        } else {
            debug_assert!(classes_groups[class] == SplitLR::Right);
            place_right(mcx, typcache, &mut right_range, v, range, i as u16)?;
        }
    }

    set_ldatum(v, left_range);
    set_rdatum(v, right_range);
    Ok(())
}

/// `range_gist_single_sorting_split` (rangetypes_gist.c:1228).
fn range_gist_single_sorting_split<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    entryvec: &GistEntryVector<'mcx>,
    v: &mut GIST_SPLITVEC<'mcx>,
    use_upper_bound: bool,
) -> PgResult<()> {
    let mut left_range: Option<RangeTypeP<'mcx>> = None;
    let mut right_range: Option<RangeTypeP<'mcx>> = None;

    let maxoff = entryvec.n as usize - 1;

    // SingleBoundSortItem { index, bound }.
    let mut sort_items: Vec<(usize, RangeBound)> = Vec::new();
    sort_items.try_reserve(maxoff).map_err(|_| out_of_memory())?;

    for i in FIRST_OFFSET_NUMBER..=maxoff {
        let range = entry_range(entryvec, i)?;
        let (lower, upper, empty) = range_deserialize(typcache, range)?;
        debug_assert!(!empty);
        let bound = if use_upper_bound { upper } else { lower };
        sort_items.push((i, bound));
    }

    // qsort_arg(sortItems, ..., single_bound_cmp, typcache).
    sort_by_fallible(&mut sort_items, |a, b| range_cmp_bounds(typcache, &a.1, &b.1))?;

    let split_idx = maxoff / 2;

    v.spl_left.clear();
    v.spl_right.clear();

    for i in 0..maxoff {
        let idx = sort_items[i].0;
        let range = entry_range(entryvec, idx)?;
        if i < split_idx {
            place_left(mcx, typcache, &mut left_range, v, range, idx as u16)?;
        } else {
            place_right(mcx, typcache, &mut right_range, v, range, idx as u16)?;
        }
    }

    set_ldatum(v, left_range);
    set_rdatum(v, right_range);
    Ok(())
}

/// `NonEmptyRange` (rangetypes_gist.c:94) — bounds extracted from a non-empty
/// range for `range_gist_double_sorting_split`.
#[derive(Clone)]
struct NonEmptyRange {
    lower: RangeBound,
    upper: RangeBound,
}

/// `CommonEntry` (rangetypes_gist.c:104).
#[derive(Clone, Copy)]
struct CommonEntry {
    index: usize,
    delta: f64,
}

/// `ConsiderSplitContext` (rangetypes_gist.c:71). `left_upper`/`right_lower`
/// hold the selected bounds by value (the C stores pointers into the sorted
/// arrays; we copy the `RangeBound`s, which the consider step only reads).
struct ConsiderSplitContext {
    has_subtype_diff: bool,
    entries_count: i32,

    first: bool,
    left_upper: RangeBound,
    right_lower: RangeBound,

    ratio: f32,
    overlap: f32,
    common_left: i32,
    common_right: i32,
}

/// `range_gist_double_sorting_split` (rangetypes_gist.c:1317).
fn range_gist_double_sorting_split<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    entryvec: &GistEntryVector<'mcx>,
    v: &mut GIST_SPLITVEC<'mcx>,
) -> PgResult<()> {
    let mut left_range: Option<RangeTypeP<'mcx>> = None;
    let mut right_range: Option<RangeTypeP<'mcx>> = None;

    let maxoff = entryvec.n as usize - 1;
    let nentries = (maxoff - FIRST_OFFSET_NUMBER + 1) as i32;

    let mut context = ConsiderSplitContext {
        has_subtype_diff: OidIsValid(typcache.rng_subdiff_finfo.fn_oid),
        entries_count: nentries,
        first: true,
        left_upper: RangeBound::default(),
        right_lower: RangeBound::default(),
        ratio: 0.0,
        overlap: 0.0,
        common_left: 0,
        common_right: 0,
    };

    let n = nentries as usize;

    // Fill arrays of bounds.
    let mut by_lower: Vec<NonEmptyRange> = Vec::new();
    by_lower.try_reserve(n).map_err(|_| out_of_memory())?;
    for i in FIRST_OFFSET_NUMBER..=maxoff {
        let range = entry_range(entryvec, i)?;
        let (lower, upper, empty) = range_deserialize(typcache, range)?;
        debug_assert!(!empty);
        by_lower.push(NonEmptyRange { lower, upper });
    }

    // by_upper = copy of by_lower, then sort each.
    let mut by_upper = by_lower.clone();
    sort_by_fallible(&mut by_lower, |a, b| range_cmp_bounds(typcache, &a.lower, &b.lower))?;
    sort_by_fallible(&mut by_upper, |a, b| range_cmp_bounds(typcache, &a.upper, &b.upper))?;

    // Iterate over lower bound of right group, finding smallest possible upper
    // bound of left group.
    let mut i1: usize = 0;
    let mut i2: usize = 0;
    let mut right_lower: RangeBound = by_lower[i1].lower;
    let mut left_upper: RangeBound = by_upper[i2].lower;
    loop {
        // Find next lower bound of right group.
        while i1 < n && range_cmp_bounds(typcache, &right_lower, &by_lower[i1].lower)? == 0 {
            if range_cmp_bounds(typcache, &by_lower[i1].upper, &left_upper)? > 0 {
                left_upper = by_lower[i1].upper;
            }
            i1 += 1;
        }
        if i1 >= n {
            break;
        }
        right_lower = by_lower[i1].lower;

        // Find count of ranges which anyway should be placed to the left group.
        while i2 < n && range_cmp_bounds(typcache, &by_upper[i2].upper, &left_upper)? <= 0 {
            i2 += 1;
        }

        range_gist_consider_split(
            typcache,
            &mut context,
            &right_lower,
            i1 as i32,
            &left_upper,
            i2 as i32,
        )?;
    }

    // Iterate over upper bound of left group finding greatest possible lower
    // bound of right group.
    let mut i1: i32 = n as i32 - 1;
    let mut i2: i32 = n as i32 - 1;
    let mut right_lower: RangeBound = by_lower[i1 as usize].upper;
    let mut left_upper: RangeBound = by_upper[i2 as usize].upper;
    loop {
        // Find next upper bound of left group.
        while i2 >= 0 && range_cmp_bounds(typcache, &left_upper, &by_upper[i2 as usize].upper)? == 0
        {
            if range_cmp_bounds(typcache, &by_upper[i2 as usize].lower, &right_lower)? < 0 {
                right_lower = by_upper[i2 as usize].lower;
            }
            i2 -= 1;
        }
        if i2 < 0 {
            break;
        }
        left_upper = by_upper[i2 as usize].upper;

        // Find count of intervals which anyway should be placed to the right
        // group.
        while i1 >= 0 && range_cmp_bounds(typcache, &by_lower[i1 as usize].lower, &right_lower)? >= 0
        {
            i1 -= 1;
        }

        range_gist_consider_split(
            typcache,
            &mut context,
            &right_lower,
            i1 + 1,
            &left_upper,
            i2 + 1,
        )?;
    }

    // If we failed to find any acceptable splits, use trivial split.
    if context.first {
        return range_gist_fallback_split(mcx, typcache, entryvec, v);
    }

    // Distribute entries.
    v.spl_left.clear();
    v.spl_right.clear();
    v.spl_left.try_reserve(n).map_err(|_| out_of_memory())?;
    v.spl_right.try_reserve(n).map_err(|_| out_of_memory())?;

    let mut common_entries: Vec<CommonEntry> = Vec::new();
    common_entries.try_reserve(n).map_err(|_| out_of_memory())?;
    let mut common_entries_count = 0usize;

    // Distribute entries which can be distributed unambiguously, and collect
    // common entries.
    for i in FIRST_OFFSET_NUMBER..=maxoff {
        let range = entry_range(entryvec, i)?;
        let (lower, upper, _empty) = range_deserialize(typcache, range)?;

        if range_cmp_bounds(typcache, &upper, &context.left_upper)? <= 0 {
            // Fits in the left group.
            if range_cmp_bounds(typcache, &lower, &context.right_lower)? >= 0 {
                // Fits also in the right group, so "common entry".
                let delta = if context.has_subtype_diff {
                    call_subtype_diff(typcache, lower.val, context.right_lower.val)?
                        - call_subtype_diff(typcache, context.left_upper.val, upper.val)?
                } else {
                    0.0
                };
                common_entries.push(CommonEntry { index: i, delta });
                common_entries_count += 1;
            } else {
                // Doesn't fit to the right group, so join to the left group.
                place_left(mcx, typcache, &mut left_range, v, range, i as u16)?;
            }
        } else {
            // Since this entry didn't fit in the left group, it better fit in
            // the right group.
            debug_assert!(range_cmp_bounds(typcache, &lower, &context.right_lower)? >= 0);
            place_right(mcx, typcache, &mut right_range, v, range, i as u16)?;
        }
    }

    // Distribute "common entries", if any.
    if common_entries_count > 0 {
        // Sort "common entries" by deltas (common_entry_cmp), most ambiguous
        // first. This comparator is infallible.
        common_entries[..common_entries_count].sort_by(common_entry_cmp);

        for i in 0..common_entries_count {
            let idx = common_entries[i].index;
            let range = entry_range(entryvec, idx)?;
            if (i as i32) < context.common_left {
                place_left(mcx, typcache, &mut left_range, v, range, idx as u16)?;
            } else {
                place_right(mcx, typcache, &mut right_range, v, range, idx as u16)?;
            }
        }
    }

    set_ldatum(v, left_range);
    set_rdatum(v, right_range);
    Ok(())
}

/// `range_gist_consider_split` (rangetypes_gist.c:1620).
fn range_gist_consider_split(
    typcache: &TypeCacheEntry,
    context: &mut ConsiderSplitContext,
    right_lower: &RangeBound,
    min_left_count: i32,
    left_upper: &RangeBound,
    max_left_count: i32,
) -> PgResult<()> {
    // Calculate entries distribution ratio assuming most uniform distribution
    // of common entries.
    let left_count: i32 = if min_left_count >= (context.entries_count + 1) / 2 {
        min_left_count
    } else if max_left_count <= context.entries_count / 2 {
        max_left_count
    } else {
        context.entries_count / 2
    };
    let right_count = context.entries_count - left_count;

    // Ratio of split.
    let ratio = (core_min(left_count, right_count) as f32) / (context.entries_count as f32);

    if (ratio as f64) > LIMIT_RATIO {
        let overlap: f32;

        // overlap measure: subtype_diff if available, else #common entries.
        if context.has_subtype_diff {
            overlap = call_subtype_diff(typcache, left_upper.val, right_lower.val)? as f32;
        } else {
            overlap = (max_left_count - min_left_count) as f32;
        }

        let mut selectthis = false;
        if context.first {
            selectthis = true;
        } else if overlap < context.overlap
            || (overlap == context.overlap && ratio > context.ratio)
        {
            selectthis = true;
        }

        if selectthis {
            context.first = false;
            context.ratio = ratio;
            context.overlap = overlap;
            context.right_lower = *right_lower;
            context.left_upper = *left_upper;
            context.common_left = max_left_count - left_count;
            context.common_right = left_count - min_left_count;
        }
    }
    Ok(())
}

/// `get_gist_range_class` (rangetypes_gist.c:1703).
fn get_gist_range_class(range: RangeTypeP<'_>) -> usize {
    let flags = range_get_flags(range);
    if flags & RANGE_EMPTY != 0 {
        CLS_EMPTY
    } else {
        let mut class_number = 0;
        if flags & RANGE_LB_INF != 0 {
            class_number |= CLS_LOWER_INF;
        }
        if flags & RANGE_UB_INF != 0 {
            class_number |= CLS_UPPER_INF;
        }
        if flags & RANGE_CONTAIN_EMPTY != 0 {
            class_number |= CLS_CONTAIN_EMPTY;
        }
        class_number
    }
}

/// `common_entry_cmp` (rangetypes_gist.c:1769) — compare CommonEntrys by deltas.
fn common_entry_cmp(i1: &CommonEntry, i2: &CommonEntry) -> core::cmp::Ordering {
    let delta1 = i1.delta;
    let delta2 = i2.delta;
    if delta1 < delta2 {
        core::cmp::Ordering::Less
    } else if delta1 > delta2 {
        core::cmp::Ordering::Greater
    } else {
        core::cmp::Ordering::Equal
    }
}

/// `call_subtype_diff` (rangetypes_gist.c:1787) — invoke the type-specific
/// subtype_diff function. Caller must have already checked there is one.
fn call_subtype_diff(typcache: &TypeCacheEntry, val1: Datum, val2: Datum) -> PgResult<f64> {
    // value = DatumGetFloat8(FunctionCall2Coll(&typcache->rng_subdiff_finfo,
    //                                          typcache->rng_collation, val1, val2));
    let value = range_subdiff(typcache, val1, val2)?;
    // Cope with buggy subtype_diff function by returning zero.
    if value >= 0.0 {
        Ok(value)
    } else {
        Ok(0.0)
    }
}

// ===========================================================================
// PLACE_LEFT / PLACE_RIGHT (rangetypes_gist.c:114-130).
// ===========================================================================

/// `PLACE_LEFT(range, off)`: union into `left_range` (via `range_super_union`)
/// once the left group is non-empty, else seed it; append `off` to `spl_left`.
fn place_left<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    left_range: &mut Option<RangeTypeP<'mcx>>,
    v: &mut GIST_SPLITVEC<'mcx>,
    range: RangeTypeP<'mcx>,
    off: u16,
) -> PgResult<()> {
    if !v.spl_left.is_empty() {
        let cur = left_range
            .take()
            .ok_or_else(|| elog_error("place_left: left_range set once spl_nleft > 0"))?;
        *left_range = Some(range_super_union(mcx, typcache, cur, range)?);
    } else {
        *left_range = Some(range);
    }
    v.spl_left.try_reserve(1).map_err(|_| out_of_memory())?;
    v.spl_left.push(off);
    Ok(())
}

/// `PLACE_RIGHT(range, off)`.
fn place_right<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    right_range: &mut Option<RangeTypeP<'mcx>>,
    v: &mut GIST_SPLITVEC<'mcx>,
    range: RangeTypeP<'mcx>,
    off: u16,
) -> PgResult<()> {
    if !v.spl_right.is_empty() {
        let cur = right_range
            .take()
            .ok_or_else(|| elog_error("place_right: right_range set once spl_nright > 0"))?;
        *right_range = Some(range_super_union(mcx, typcache, cur, range)?);
    } else {
        *right_range = Some(range);
    }
    v.spl_right.try_reserve(1).map_err(|_| out_of_memory())?;
    v.spl_right.push(off);
    Ok(())
}

// ===========================================================================
// Helpers.
// ===========================================================================

/// `DatumGetRangeTypeP(entryvec->vector[i].key)` — the GiST keys are stored as
/// ranges directly (no compress/decompress for ranges), so the key `Datum` is a
/// plain `RangeType *` pointer. Read the `i`-th 0-based entry's key as a
/// `RangeTypeP`.
fn entry_range<'mcx>(entryvec: &GistEntryVector<'mcx>, i: usize) -> PgResult<RangeTypeP<'mcx>> {
    let key = entryvec
        .vector
        .get(i)
        .ok_or_else(|| elog_error("range GiST: entry index out of range"))?
        .key
        .clone();
    Ok(datum_get_range_type_p(key))
}

/// `DatumGetRangeTypeP(d)` for an already-plain (non-toasted) range key `Datum`
/// — GiST range keys are never toasted (stored as plain `RangeType *`), so the
/// raw word is the `RangeType *` address.
fn datum_get_range_type_p<'mcx>(d: types_tuple::heaptuple::Datum<'mcx>) -> RangeTypeP<'mcx> {
    RangeTypeP {
        ptr: d.as_usize() as *const types_rangetypes::RangeType,
        _marker: core::marker::PhantomData,
    }
}

/// `DatumGetRangeTypeP(arg)` that un-packs a SHORT (1-byte-header) on-disk image
/// into a fresh 8-aligned 4-byte-header `mcx` buffer (mirroring C's
/// `PG_DETOAST_DATUM` short arm), so the `RangeType` struct's fixed-offset reads
/// (`rangetypid` at `sizeof(RangeType)`, bounds past it) land correctly and the
/// base is `MAXALIGN(8)`-aligned (the alignment `range_serialize` produces, which
/// the relative-offset bound accounting assumes). A by-value pointer word is the
/// `RangeType *` address directly. While `SHORT_VARLENA_PACKING` is OFF every key
/// is already plain 4B, so the non-short path copies it verbatim into the aligned
/// buffer — behavior-preserving.
fn materialize_range_arg<'mcx>(
    mcx: Mcx<'mcx>,
    d: &types_tuple::heaptuple::Datum<'mcx>,
) -> PgResult<RangeTypeP<'mcx>> {
    use core::alloc::Layout;
    use types_tuple::heaptuple::Datum as TDatum;

    // A by-value pointer word: the address is already a live `RangeType *`.
    if let TDatum::ByVal(w) = d {
        return Ok(RangeTypeP {
            ptr: *w as *const types_rangetypes::RangeType,
            _marker: core::marker::PhantomData,
        });
    }

    let image = d.as_ref_bytes();
    // VARATT_IS_1B (short header) and not VARATT_IS_1B_E (0x01, external): the
    // 1-byte-header payload un-packs into a fresh 4-byte-header image.
    let short = matches!(image.first(), Some(&h) if h != 0x01 && (h & 0x01) == 0x01);
    let (payload_off, payload_len) = if short {
        let total_1b = ((image[0] >> 1) & 0x7F) as usize;
        (1usize, total_1b.saturating_sub(1))
    } else {
        // Plain 4B image: copy it verbatim (header + body) into the aligned buffer.
        (0usize, image.len())
    };
    let new_size = if short { payload_len + 4 } else { image.len() };
    mcx::check_alloc_size(new_size)?;
    let layout =
        Layout::from_size_align(new_size.max(1), 8).expect("valid RangeType image layout");
    let block = mcx.allocate(layout).map_err(|_| mcx.oom(new_size))?;
    let dst = block.as_ptr() as *mut u8;
    // SAFETY: `dst` heads a freshly allocated `new_size`-byte region.
    unsafe {
        if short {
            let word = (new_size as u32) << 2; // low 2 bits 00 = plain 4B header
            core::ptr::copy_nonoverlapping(word.to_ne_bytes().as_ptr(), dst, 4);
            core::ptr::copy_nonoverlapping(image.as_ptr().add(payload_off), dst.add(4), payload_len);
        } else {
            core::ptr::copy_nonoverlapping(image.as_ptr(), dst, new_size);
        }
    }
    Ok(RangeTypeP {
        ptr: dst as *const types_rangetypes::RangeType,
        _marker: core::marker::PhantomData,
    })
}

/// `RangeTypePGetDatum(r)` — pack a `RangeTypeP` back into a GiST key `Datum`.
fn range_type_p_get_datum<'mcx>(
    r: RangeTypeP<'mcx>,
) -> types_tuple::heaptuple::Datum<'mcx> {
    types_tuple::heaptuple::Datum::from_usize(r.ptr as usize)
}

/// `rangeCopy(r)` (rangetypes.h) — copy the serialized `RangeType` image into a
/// fresh, writable, MAXALIGN'd `mcx` buffer (so the caller may modify the flags
/// byte in place).
fn range_copy<'mcx>(mcx: Mcx<'mcx>, r: RangeTypeP<'mcx>) -> PgResult<RangeTypeP<'mcx>> {
    // SAFETY: `r` is a detoasted RangeType image; its total length is its
    // varlena size (the 4B header low bits).
    let size = unsafe { varsize(r.ptr as *const u8) };
    mcx::check_alloc_size(size)?;
    let layout =
        core::alloc::Layout::from_size_align(size, 8).expect("valid RangeType image layout");
    let block = mcx.allocate(layout).map_err(|_| mcx.oom(size))?;
    let dst = block.as_ptr() as *mut u8;
    // SAFETY: src and dst are both `size` bytes, non-overlapping fresh alloc.
    unsafe {
        core::ptr::copy_nonoverlapping(r.ptr as *const u8, dst, size);
    }
    Ok(RangeTypeP {
        ptr: dst as *const types_rangetypes::RangeType,
        _marker: core::marker::PhantomData,
    })
}

/// `VARSIZE(ptr)` for a plain 4B varlena header — the byte length of the image.
#[inline]
unsafe fn varsize(ptr: *const u8) -> usize {
    // VARSIZE_4B: ((varattrib_4b *) ptr)->va_4byte.va_header >> 2, but our
    // images are always 4B-unaligned-flag plain varlenas; read the 4-byte
    // header and mask off the two low flag bits.
    let header = (ptr as *const u32).read_unaligned();
    (header & 0x3FFF_FFFF) as usize
}

/// `RangeIsEmpty(range)` (rangetypes.h).
#[inline]
fn range_is_empty(range: RangeTypeP<'_>) -> bool {
    (range_get_flags(range) & RANGE_EMPTY) != 0
}

/// `RangeIsOrContainsEmpty(range)` (rangetypes.h).
#[inline]
fn range_is_or_contains_empty(range: RangeTypeP<'_>) -> bool {
    (range_get_flags(range) & (RANGE_EMPTY | RANGE_CONTAIN_EMPTY)) != 0
}

/// `MultirangeIsEmpty(mr)` (multirangetypes.h).
#[inline]
fn multirange_is_empty(mr: MultirangeTypeP<'_>) -> bool {
    mr.range_count() == 0
}

/// `OidIsValid(oid)`.
#[inline]
fn oid_is_valid(oid: Oid) -> bool {
    OidIsValid(oid)
}

/// Borrow the element value of an element-subtype query.
#[inline]
fn query_elem(query: &GistQuery<'_>) -> PgResult<Datum> {
    match query {
        GistQuery::Elem(d) => Ok(*d),
        // For an element-subtype strategy the fmgr dispatch hands us the bare
        // element Datum; a non-Elem variant here would be a wiring bug.
        _ => Err(elog_error("range GiST element query expected element value")),
    }
}

/// `Min(a, b)`.
#[inline]
fn core_min(a: i32, b: i32) -> i32 {
    if a < b {
        a
    } else {
        b
    }
}

/// Set `v->spl_ldatum` / `spl_ldatum_exists` from the accumulated left union.
fn set_ldatum<'mcx>(v: &mut GIST_SPLITVEC<'mcx>, left_range: Option<RangeTypeP<'mcx>>) {
    v.spl_ldatum = left_range.map(range_type_p_get_datum);
    v.spl_ldatum_exists = false;
}

/// Set `v->spl_rdatum` / `spl_rdatum_exists` from the accumulated right union.
fn set_rdatum<'mcx>(v: &mut GIST_SPLITVEC<'mcx>, right_range: Option<RangeTypeP<'mcx>>) {
    v.spl_rdatum = right_range.map(range_type_p_get_datum);
    v.spl_rdatum_exists = false;
}

/// Stable-by-construction insertion sort over a fallible total-order comparator
/// (the element-subtype `cmp` proc that `range_cmp_bounds` reaches is fallible).
/// Propagates the first comparison error.
fn sort_by_fallible<T, F>(items: &mut [T], mut cmp: F) -> PgResult<()>
where
    F: FnMut(&T, &T) -> PgResult<i32>,
{
    for i in 1..items.len() {
        let mut j = i;
        while j > 0 {
            if cmp(&items[j - 1], &items[j])? > 0 {
                items.swap(j - 1, j);
                j -= 1;
            } else {
                break;
            }
        }
    }
    Ok(())
}

// ===========================================================================
// Errors.
// ===========================================================================

/// `ereport(ERROR, ERRCODE_OUT_OF_MEMORY, ...)` — recoverable OOM.
fn out_of_memory() -> PgError {
    PgError::error("out of memory")
}

/// `elog(ERROR, "unrecognized range strategy: %d", strategy)`
/// (rangetypes_gist.c:968 et al) — `ERRCODE_INTERNAL_ERROR` (XX000).
fn unrecognized_range_strategy(strategy: u16) -> PgError {
    PgError::error(alloc::format!("unrecognized range strategy: {strategy}"))
}

/// `elog(ERROR, ...)` — `ERRCODE_INTERNAL_ERROR` (XX000).
fn elog_error(msg: &str) -> PgError {
    PgError::error(msg)
}
