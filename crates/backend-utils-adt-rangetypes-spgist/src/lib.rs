#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

//! `src/backend/utils/adt/rangetypes_spgist.c` (PostgreSQL 18.3): SP-GiST
//! support for range types — the quad-tree-over-ranges-mapped-to-2d-points
//! opclass (`range_ops` under the spgist AM).
//!
//! Ranges are mapped to 2d-points so that the lower bound is one dimension and
//! the upper bound is the other. Each inner node holds a "centroid" range that
//! divides the 2d-space into 4 quadrants; empty ranges go to a special 5th
//! quadrant at the root. An inner node with no centroid divides ranges purely on
//! emptiness (node 0 = empty, node 1 = non-empty).
//!
//! Every function in `rangetypes_spgist.c` is ported 1:1 — identical control
//! flow, branch order, loop bounds, switch arms and `Assert`s:
//!
//!   * [`spg_range_quad_config`]            (config, `rangetypes_spgist.c:60`)
//!   * [`spg_range_quad_choose`]            (choose, `rangetypes_spgist.c:131`)
//!   * [`spg_range_quad_picksplit`]         (picksplit, `rangetypes_spgist.c:200`)
//!   * [`spg_range_quad_inner_consistent`]  (inner consistent, `rangetypes_spgist.c:300`)
//!   * [`spg_range_quad_leaf_consistent`]   (leaf consistent, `rangetypes_spgist.c:917`)
//!   * `getQuadrant`                        (static helper, `rangetypes_spgist.c:95`)
//!   * `bound_cmp`                          (static qsort cmp, `rangetypes_spgist.c:186`)
//!   * `adjacent_cmp_bounds`                (static helper, `rangetypes_spgist.c:785`)
//!   * `adjacent_inner_consistent`          (static helper, `rangetypes_spgist.c:887`)
//!
//! ## Idiomatic carriers / fmgr boundary
//!
//! The C fmgr entry points receive `Pointer`s to the typed `spg*In`/`spg*Out`
//! structs and `palloc` their output arrays. As in the sibling SP-GiST opclasses
//! (`backend-access-spg-quadtree` / `-kdtree` / `-text` /
//! `backend-utils-adt-network-spgist`), the bodies here operate directly on the
//! owned [`types_spgist`] vocabulary structs, with the range payloads carried
//! inside [`types_tuple::Datum::ByRef`] varlena images. "Allocate an output
//! array" becomes "fill an owned `Vec`".
//!
//! `DatumGetRangeTypeP(d)` becomes [`datum_get_range`] over the by-reference
//! varlena image (the bytes ARE the detoasted `RangeType` varlena — node tuples
//! are never toasted, since `longValuesOK = false`), and `RangeTypePGetDatum(r)`
//! becomes [`range_get_datum`], which copies the serialized `RangeType` varlena
//! bytes back into a [`types_tuple::Datum::ByRef`]. The range kernels
//! (`range_deserialize` / `range_serialize` / `range_cmp_bounds` /
//! `bounds_adjacent` / `range_get_typcache` and the `*_internal` predicates)
//! reuse the ported implementations in [`backend_utils_adt_rangetypes`]; the
//! `range_get_typcache(fcinfo, oid)` C call (which caches in
//! `fcinfo->flinfo->fn_extra`) becomes the by-OID
//! `backend_utils_adt_rangetypes::range_bounds_compare::range_get_typcache`
//! (the cache is the typcache owner's job).
//!
//! The SP-GiST core dispatches its five opclass support procedures by OID
//! through `backend-access-spg-core-seams`; `backend-access-spg-quadtree` is the
//! single installer of that by-OID dispatch and routes the range support-proc
//! OIDs (config 3469 / choose 3470 / picksplit 3471 / inner_consistent 3472 /
//! leaf_consistent 3473) to the bodies exported here, exactly as it routes the
//! quad-tree / k-d-tree / text / inet opclasses.

use mcx::Mcx;
use types_cache::typcache::TypeCacheEntry;
use types_core::primitive::Oid;
use types_error::{PgError, PgResult};
use types_rangetypes::{RangeBound, RangeTypeP, RANGE_EMPTY};
use types_spgist::{
    spgChooseIn, spgChooseOut, spgChooseOutMatchNode, spgChooseOutResult, spgConfigIn,
    spgConfigOut, spgInnerConsistentIn, spgInnerConsistentOut, spgLeafConsistentIn,
    spgLeafConsistentOut, spgPickSplitIn, spgPickSplitOut,
};
use types_tuple::backend_access_common_heaptuple::Datum as TDatum;

use backend_utils_adt_rangetypes::range_bounds_compare::{
    bounds_adjacent, range_adjacent_internal, range_after_internal, range_before_internal,
    range_cmp_bounds, range_contained_by_internal, range_contains_elem_internal,
    range_contains_internal, range_eq_internal, range_get_typcache, range_overlaps_internal,
    range_overleft_internal, range_overright_internal,
};
use backend_utils_adt_rangetypes::range_repr_serialize::{range_deserialize, range_serialize};

// ---------------------------------------------------------------------------
// pg_proc.dat support-proc OIDs for the range_ops SP-GiST opclass.
// (pg_amproc.dat: amprocnum 1..5 for anyrange/anyrange under the spgist AM.)
// ---------------------------------------------------------------------------

/// `spg_range_quad_config` (pg_proc.dat oid 3469).
pub const F_SPG_RANGE_QUAD_CONFIG: Oid = 3469;
/// `spg_range_quad_choose` (pg_proc.dat oid 3470).
pub const F_SPG_RANGE_QUAD_CHOOSE: Oid = 3470;
/// `spg_range_quad_picksplit` (pg_proc.dat oid 3471).
pub const F_SPG_RANGE_QUAD_PICKSPLIT: Oid = 3471;
/// `spg_range_quad_inner_consistent` (pg_proc.dat oid 3472).
pub const F_SPG_RANGE_QUAD_INNER_CONSISTENT: Oid = 3472;
/// `spg_range_quad_leaf_consistent` (pg_proc.dat oid 3473).
pub const F_SPG_RANGE_QUAD_LEAF_CONSISTENT: Oid = 3473;

// ---------------------------------------------------------------------------
// catalog/pg_type.dat type OIDs used by spg_range_quad_config.
// ---------------------------------------------------------------------------

/// `ANYRANGEOID` (pg_type.dat oid 3831).
const ANYRANGEOID: Oid = 3831;
/// `VOIDOID` (pg_type.dat oid 2278).
const VOIDOID: Oid = 2278;

// ---------------------------------------------------------------------------
// utils/rangetypes.h — strategy numbers consumed by the consistency checks.
// (`#define RANGESTRAT_* RT*StrategyNumber`, access/stratnum.h.)
// ---------------------------------------------------------------------------

/// `RANGESTRAT_BEFORE` = `RTLeftStrategyNumber` (1).
const RANGESTRAT_BEFORE: u16 = 1;
/// `RANGESTRAT_OVERLEFT` = `RTOverLeftStrategyNumber` (2).
const RANGESTRAT_OVERLEFT: u16 = 2;
/// `RANGESTRAT_OVERLAPS` = `RTOverlapStrategyNumber` (3).
const RANGESTRAT_OVERLAPS: u16 = 3;
/// `RANGESTRAT_OVERRIGHT` = `RTOverRightStrategyNumber` (4).
const RANGESTRAT_OVERRIGHT: u16 = 4;
/// `RANGESTRAT_AFTER` = `RTRightStrategyNumber` (5).
const RANGESTRAT_AFTER: u16 = 5;
/// `RANGESTRAT_ADJACENT` = `RTSameStrategyNumber` (6).
const RANGESTRAT_ADJACENT: u16 = 6;
/// `RANGESTRAT_CONTAINS` = `RTContainsStrategyNumber` (7).
const RANGESTRAT_CONTAINS: u16 = 7;
/// `RANGESTRAT_CONTAINED_BY` = `RTContainedByStrategyNumber` (8).
const RANGESTRAT_CONTAINED_BY: u16 = 8;
/// `RANGESTRAT_CONTAINS_ELEM` = `RTContainsElemStrategyNumber` (16).
const RANGESTRAT_CONTAINS_ELEM: u16 = 16;
/// `RANGESTRAT_EQ` = `RTEqualStrategyNumber` (18).
const RANGESTRAT_EQ: u16 = 18;

// ---------------------------------------------------------------------------
// fmgr boundary codecs (DatumGetRangeTypeP / RangeTypePGetDatum).
//
// A range leaf/prefix datum crosses the typed dispatch seam as a
// `types_tuple::Datum::ByRef` whose bytes are the serialized `RangeType`
// varlena (the on-disk image the range ADT produces and consumes). The node
// tuples are never toasted (`longValuesOK = false`), so the bytes are always a
// plain detoasted `RangeType` — exactly what `RangeTypeP` points at.
// ---------------------------------------------------------------------------

/// `VARSIZE(PTR)` over a plain 4-byte varlena header (little-endian):
/// `(va_header >> 2) & 0x3FFFFFFF`. A serialized `RangeType` is always a 4B
/// (full-header) varlena, since `range_serialize` calls `SET_VARSIZE`.
#[inline]
fn varsize_4b(bytes: &[u8]) -> usize {
    let header = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    ((header >> 2) & 0x3FFF_FFFF) as usize
}

/// `DatumGetRangeTypeP(datum)` — view a range `Datum::ByRef` image as a
/// detoasted `RangeType *`. The handle borrows the `Datum`'s own bytes (which
/// live for `'mcx`), mirroring the C macro's no-copy `PG_DETOAST_DATUM` on an
/// already-plain varlena.
#[inline]
fn datum_get_range<'mcx>(d: &TDatum<'mcx>) -> RangeTypeP<'mcx> {
    let bytes = d.as_ref_bytes();
    RangeTypeP {
        ptr: bytes.as_ptr() as *const types_rangetypes::RangeType,
        _marker: core::marker::PhantomData,
    }
}

/// `RangeTypePGetDatum(range)` — encode a serialized `RangeType` as a
/// by-reference `Datum` in `mcx`. The C macro yields the `palloc`'d varlena
/// pointer word; here the varlena bytes are copied into an owned `Datum::ByRef`
/// image (the varlena header being the fmgr boundary's concern).
#[inline]
fn range_get_datum<'mcx>(mcx: Mcx<'mcx>, range: RangeTypeP<'mcx>) -> PgResult<TDatum<'mcx>> {
    // SAFETY: `range.ptr` points at a serialized `RangeType` varlena that lives
    // for `'mcx` (produced by `range_serialize` / borrowed from a `Datum::ByRef`
    // image); its 4B varlena header gives the total byte length.
    let bytes = unsafe {
        let p = range.ptr as *const u8;
        let len = varsize_4b(core::slice::from_raw_parts(p, 4));
        core::slice::from_raw_parts(p, len)
    };
    Ok(TDatum::ByRef(mcx::slice_in(mcx, bytes)?))
}

/// Bridge a typed `ScanKeyData::sk_argument` (a `types_tuple::Datum`) to the
/// bare-word `types_datum::Datum` that `range_contains_elem_internal` consumes
/// (the C `sk_argument` is a bare `Datum`). A by-value element passes its
/// machine word; a by-reference element passes the pointer to its image bytes.
#[inline]
fn elem_datum(d: &TDatum<'_>) -> types_datum::Datum {
    match d {
        TDatum::ByVal(w) => types_datum::Datum::from_usize(*w),
        _ => types_datum::Datum::from_usize(d.as_ref_bytes().as_ptr() as usize),
    }
}

/// `RangeTypeGetOid(range)` over the by-reference image — the range type's own
/// OID (the only directly-readable header field).
#[inline]
fn range_get_oid(d: &TDatum<'_>) -> Oid {
    datum_get_range(d).rangetypid()
}

/// `RangeIsEmpty(r)` (rangetypes.h:56) over a by-reference image:
/// `(range_get_flags(r) & RANGE_EMPTY) != 0`.
#[inline]
fn range_is_empty(d: &TDatum<'_>) -> bool {
    (backend_utils_adt_rangetypes::range_repr_serialize::range_get_flags(datum_get_range(d))
        & RANGE_EMPTY)
        != 0
}

// ===========================================================================
// The SP-GiST 'config' interface function (rangetypes_spgist.c:60).
// ===========================================================================

/// `spg_range_quad_config` (rangetypes_spgist.c:60). Fill the opclass config
/// output.
pub fn spg_range_quad_config(_cfgin: &spgConfigIn, cfg: &mut spgConfigOut) {
    cfg.prefixType = ANYRANGEOID;
    cfg.labelType = VOIDOID; // we don't need node labels
    cfg.canReturnData = true;
    cfg.longValuesOK = false;
}

// ===========================================================================
// getQuadrant (rangetypes_spgist.c:95).
//
// Determine which quadrant a 2d-mapped range falls into, relative to the
// centroid. Quadrants:
//
//	 4	|  1
//	----+----
//	 3	|  2
//
// Empty ranges lie in the special quadrant 5.
// ===========================================================================

/// `getQuadrant(typcache, centroid, tst)` (rangetypes_spgist.c:95).
fn get_quadrant(
    typcache: &TypeCacheEntry,
    centroid: RangeTypeP<'_>,
    tst: RangeTypeP<'_>,
) -> PgResult<i16> {
    let (centroid_lower, centroid_upper, _centroid_empty) = range_deserialize(typcache, centroid)?;
    let (lower, upper, empty) = range_deserialize(typcache, tst)?;

    if empty {
        return Ok(5);
    }

    if range_cmp_bounds(typcache, &lower, &centroid_lower)? >= 0 {
        if range_cmp_bounds(typcache, &upper, &centroid_upper)? >= 0 {
            Ok(1)
        } else {
            Ok(2)
        }
    } else if range_cmp_bounds(typcache, &upper, &centroid_upper)? >= 0 {
        Ok(4)
    } else {
        Ok(3)
    }
}

// ===========================================================================
// The SP-GiST choose function (rangetypes_spgist.c:131).
// ===========================================================================

/// `spg_range_quad_choose` (rangetypes_spgist.c:131). Choose the path for
/// addition of a new range.
pub fn spg_range_quad_choose<'mcx>(
    mcx: Mcx<'mcx>,
    in_: &spgChooseIn<'mcx>,
    out: &mut spgChooseOut<'mcx>,
) -> PgResult<()> {
    // RangeType *inRange = DatumGetRangeTypeP(in->datum), *centroid;
    let in_range = &in_.datum;

    // if (in->allTheSame)
    if in_.allTheSame {
        // out->resultType = spgMatchNode;
        // nodeN will be set by core
        // out->result.matchNode.levelAdd = 0;
        // out->result.matchNode.restDatum = RangeTypePGetDatum(inRange);
        out.result = spgChooseOutResult::MatchNode(spgChooseOutMatchNode {
            nodeN: -1, // set by core
            levelAdd: 0,
            restDatum: in_range.clone(),
        });
        return Ok(());
    }

    let typcache = range_get_typcache(range_get_oid(in_range))?;

    // A node with no centroid divides ranges purely on whether they're empty
    // or not. All empty ranges go to child node 0, all non-empty ranges go to
    // node 1.
    if !in_.hasPrefix {
        let node_n = if range_is_empty(in_range) { 0 } else { 1 };
        out.result = spgChooseOutResult::MatchNode(spgChooseOutMatchNode {
            nodeN: node_n,
            levelAdd: 1,
            restDatum: in_range.clone(),
        });
        return Ok(());
    }

    // centroid = DatumGetRangeTypeP(in->prefixDatum);
    let centroid = datum_get_range(&in_.prefixDatum);
    let quadrant = get_quadrant(&typcache, centroid, datum_get_range(in_range))?;

    debug_assert!(quadrant as i32 <= in_.nNodes); // Assert(quadrant <= in->nNodes);

    // Select node matching to quadrant number.
    out.result = spgChooseOutResult::MatchNode(spgChooseOutMatchNode {
        nodeN: (quadrant - 1) as i32,
        levelAdd: 1,
        restDatum: in_range.clone(),
    });

    let _ = mcx; // restDatum borrows the existing image (no fresh allocation).
    Ok(())
}

// ===========================================================================
// bound_cmp (rangetypes_spgist.c:186) — bound comparison for sorting.
// ===========================================================================

/// `bound_cmp(a, b, arg)` (rangetypes_spgist.c:186): `range_cmp_bounds(typcache,
/// ba, bb)`.
fn bound_cmp(ba: &RangeBound, bb: &RangeBound, typcache: &TypeCacheEntry) -> PgResult<i32> {
    range_cmp_bounds(typcache, ba, bb)
}

// ===========================================================================
// The SP-GiST picksplit function (rangetypes_spgist.c:200).
// ===========================================================================

/// `spg_range_quad_picksplit` (rangetypes_spgist.c:200). Split ranges into
/// nodes: select a "centroid" range and distribute ranges according to
/// quadrants.
pub fn spg_range_quad_picksplit<'mcx>(
    mcx: Mcx<'mcx>,
    in_: &spgPickSplitIn<'mcx>,
    out: &mut spgPickSplitOut<'mcx>,
) -> PgResult<()> {
    let n_tuples = in_.nTuples();

    let typcache = range_get_typcache(range_get_oid(&in_.datums[0]))?;

    // Allocate memory for bounds.
    // RangeBound *lowerBounds, *upperBounds; (length nTuples) — but only the
    // first `nonEmptyCount` slots are filled (j only advances on non-empty).
    let mut lower_bounds: Vec<RangeBound> = Vec::with_capacity(n_tuples as usize);
    let mut upper_bounds: Vec<RangeBound> = Vec::with_capacity(n_tuples as usize);

    // Deserialize bounds of ranges, count non-empty ranges.
    for i in 0..n_tuples as usize {
        let (lower, upper, empty) = range_deserialize(&typcache, datum_get_range(&in_.datums[i]))?;
        if !empty {
            lower_bounds.push(lower);
            upper_bounds.push(upper);
        }
    }
    let non_empty_count = lower_bounds.len() as i32;

    // All the ranges are empty. The best we can do is to construct an inner
    // node with no centroid, and put all ranges into node 0. If non-empty
    // ranges are added later, they will be routed to node 1.
    if non_empty_count == 0 {
        out.nNodes = 2;
        out.hasPrefix = false;
        out.prefixDatum = None; // Prefix is empty (PointerGetDatum(NULL)).
        out.nodeLabels = None;

        out.mapTuplesToNodes = Vec::with_capacity(n_tuples as usize);
        out.leafTupleDatums = Vec::with_capacity(n_tuples as usize);

        // Place all ranges into node 0.
        for i in 0..n_tuples as usize {
            out.leafTupleDatums.push(in_.datums[i].clone());
            out.mapTuplesToNodes.push(0);
        }
        return Ok(());
    }

    // Sort range bounds in order to find medians.
    // qsort_arg(lowerBounds, nonEmptyCount, ..., bound_cmp, typcache);
    sort_bounds(&mut lower_bounds, &typcache)?;
    sort_bounds(&mut upper_bounds, &typcache)?;

    // Construct "centroid" range from medians of lower and upper bounds.
    let median = (non_empty_count / 2) as usize;
    let centroid = range_serialize(
        mcx,
        &typcache,
        &lower_bounds[median],
        &upper_bounds[median],
        false,
    )?;
    out.hasPrefix = true;
    out.prefixDatum = Some(range_get_datum(mcx, centroid)?);

    // Create node for empty ranges only if it is a root node.
    out.nNodes = if in_.level == 0 { 5 } else { 4 };
    out.nodeLabels = None; // we don't need node labels

    out.mapTuplesToNodes = Vec::with_capacity(n_tuples as usize);
    out.leafTupleDatums = Vec::with_capacity(n_tuples as usize);

    // Assign ranges to corresponding nodes according to quadrants relative to
    // "centroid" range.
    for i in 0..n_tuples as usize {
        let range = datum_get_range(&in_.datums[i]);
        let quadrant = get_quadrant(&typcache, centroid, range)?;

        out.leafTupleDatums.push(in_.datums[i].clone());
        out.mapTuplesToNodes.push((quadrant - 1) as i32);
    }

    Ok(())
}

/// `qsort_arg(bounds, n, sizeof(RangeBound), bound_cmp, typcache)`
/// (rangetypes_spgist.c:261). A faithful comparison-sort using `bound_cmp`; any
/// error from the subtype `cmp` proc propagates out (C's `qsort_arg` would
/// `ereport(ERROR)` from inside the comparator, which here surfaces as `Err`).
fn sort_bounds(bounds: &mut [RangeBound], typcache: &TypeCacheEntry) -> PgResult<()> {
    // Insertion sort over the small per-page bound array, calling bound_cmp.
    // We cannot use slice::sort_by because the comparator is fallible.
    let n = bounds.len();
    for i in 1..n {
        let mut j = i;
        while j > 0 {
            let key = bounds[j];
            let prev = bounds[j - 1];
            if bound_cmp(&prev, &key, typcache)? > 0 {
                bounds.swap(j - 1, j);
                j -= 1;
            } else {
                break;
            }
        }
    }
    Ok(())
}

// ===========================================================================
// The SP-GiST inner-consistent function (rangetypes_spgist.c:300).
// ===========================================================================

/// `spg_range_quad_inner_consistent` (rangetypes_spgist.c:300). Check which
/// child nodes are consistent with the given set of queries.
pub fn spg_range_quad_inner_consistent<'mcx>(
    mcx: Mcx<'mcx>,
    in_: &spgInnerConsistentIn<'mcx>,
    out: &mut spgInnerConsistentOut<'mcx>,
) -> PgResult<()> {
    let mut which: i32;
    let nkeys = in_.nkeys() as usize;

    // For adjacent search we need also previous centroid (if any) to improve
    // the precision of the consistent check.
    let mut need_previous = false;

    if in_.allTheSame {
        // Report that all nodes should be visited.
        out.nNodes = in_.nNodes;
        out.nodeNumbers = Vec::with_capacity(in_.nNodes as usize);
        for i in 0..in_.nNodes {
            out.nodeNumbers.push(i);
        }
        return Ok(());
    }

    if !in_.hasPrefix {
        // No centroid on this inner node. Such a node has two child nodes, the
        // first for empty ranges, and the second for non-empty ones.
        debug_assert_eq!(in_.nNodes, 2); // Assert(in->nNodes == 2);

        // Nth bit of which means that (N - 1)th node should be visited.
        which = (1 << 1) | (1 << 2);
        for i in 0..nkeys {
            let strategy = in_.scankeys[i].sk_strategy;

            // The only strategy when second argument of operator is not range
            // is RANGESTRAT_CONTAINS_ELEM.
            let empty = if strategy != RANGESTRAT_CONTAINS_ELEM {
                range_is_empty(&in_.scankeys[i].sk_argument)
            } else {
                false
            };

            match strategy {
                RANGESTRAT_BEFORE
                | RANGESTRAT_OVERLEFT
                | RANGESTRAT_OVERLAPS
                | RANGESTRAT_OVERRIGHT
                | RANGESTRAT_AFTER
                | RANGESTRAT_ADJACENT => {
                    // These strategies return false if any argument is empty.
                    if empty {
                        which = 0;
                    } else {
                        which &= 1 << 2;
                    }
                }
                RANGESTRAT_CONTAINS => {
                    // All ranges contain an empty range. Only non-empty ranges
                    // can contain a non-empty range.
                    if !empty {
                        which &= 1 << 2;
                    }
                }
                RANGESTRAT_CONTAINED_BY => {
                    // Only an empty range is contained by an empty range. Both
                    // empty and non-empty ranges can be contained by a
                    // non-empty range.
                    if empty {
                        which &= 1 << 1;
                    }
                }
                RANGESTRAT_CONTAINS_ELEM => {
                    which &= 1 << 2;
                }
                RANGESTRAT_EQ => {
                    if empty {
                        which &= 1 << 1;
                    } else {
                        which &= 1 << 2;
                    }
                }
                _ => {
                    return Err(unrecognized_range_strategy(strategy));
                }
            }
            if which == 0 {
                break; // no need to consider remaining conditions
            }
        }
    } else {
        // This node has a centroid. Fetch it.
        let centroid_p = datum_get_range(&in_.prefixDatum);
        let typcache = range_get_typcache(centroid_p.rangetypid())?;
        let (centroid_lower, centroid_upper, _centroid_empty) =
            range_deserialize(&typcache, centroid_p)?;

        debug_assert!(in_.nNodes == 4 || in_.nNodes == 5); // Assert(in->nNodes == 4 || == 5);

        // Nth bit of which means that (N - 1)th node (Nth quadrant) should be
        // visited.
        which = (1 << 1) | (1 << 2) | (1 << 3) | (1 << 4) | (1 << 5);

        for i in 0..nkeys {
            let mut strategy = in_.scankeys[i].sk_strategy;
            let lower: RangeBound;
            let upper: RangeBound;
            let empty: bool;

            // Restrictions on range bounds according to scan strategy.
            // Modeled as Option<RangeBound> holding the bound value (C used
            // `RangeBound *minLower` aliasing into lower/upper).
            let mut min_lower: Option<RangeBound> = None;
            let mut max_lower: Option<RangeBound> = None;
            let mut min_upper: Option<RangeBound> = None;
            let mut max_upper: Option<RangeBound> = None;

            // Are the restrictions on range bounds inclusive?
            let mut inclusive = true;
            let mut strict_empty = true;

            // RANGESTRAT_CONTAINS_ELEM is just like RANGESTRAT_CONTAINS, but the
            // argument is a single element. Expand the single element to a range
            // containing only the element, and treat it like RANGESTRAT_CONTAINS.
            if strategy == RANGESTRAT_CONTAINS_ELEM {
                let val = elem_datum(&in_.scankeys[i].sk_argument);
                lower = RangeBound {
                    inclusive: true,
                    infinite: false,
                    lower: true,
                    val,
                };
                upper = RangeBound {
                    inclusive: true,
                    infinite: false,
                    lower: false,
                    val,
                };
                empty = false;

                strategy = RANGESTRAT_CONTAINS;
            } else {
                let range = datum_get_range(&in_.scankeys[i].sk_argument);
                let (l, u, e) = range_deserialize(&typcache, range)?;
                lower = l;
                upper = u;
                empty = e;
            }

            // Most strategies are handled by forming a bounding box from the
            // search key, defined by a minLower, maxLower, minUpper, maxUpper.
            match strategy {
                RANGESTRAT_BEFORE => {
                    // Range A is before range B if upper bound of A is lower
                    // than lower bound of B.
                    max_upper = Some(lower);
                    inclusive = false;
                }
                RANGESTRAT_OVERLEFT => {
                    // Range A is overleft to range B if upper bound of A is less
                    // than or equal to upper bound of B.
                    max_upper = Some(upper);
                }
                RANGESTRAT_OVERLAPS => {
                    // Non-empty ranges overlap, if lower bound of each range is
                    // lower or equal to upper bound of the other range.
                    max_lower = Some(upper);
                    min_upper = Some(lower);
                }
                RANGESTRAT_OVERRIGHT => {
                    // Range A is overright to range B if lower bound of A is
                    // greater than or equal to lower bound of B.
                    min_lower = Some(lower);
                }
                RANGESTRAT_AFTER => {
                    // Range A is after range B if lower bound of A is greater
                    // than upper bound of B.
                    min_lower = Some(upper);
                    inclusive = false;
                }
                RANGESTRAT_ADJACENT => {
                    if empty {
                        // Skip to strictEmpty check.
                    } else {
                        // Previously selected quadrant could exclude possibility
                        // for lower or upper bounds to be adjacent. Deserialize
                        // previous centroid range if present for checking this.
                        let prev = if let Some(bytes) = in_.traversalValue.as_ref() {
                            let prev_centroid = RangeTypeP {
                                ptr: bytes.as_ptr() as *const types_rangetypes::RangeType,
                                _marker: core::marker::PhantomData,
                            };
                            let (pl, pu, _pe) = range_deserialize(&typcache, prev_centroid)?;
                            Some((pl, pu))
                        } else {
                            None
                        };

                        // For a range's upper bound to be adjacent to the
                        // argument's lower bound, ... if the argument's lower
                        // bound is less than the centroid's upper bound, the line
                        // falls in quadrants 2 and 3; if greater, quadrants 1 and 4.
                        let c1 = adjacent_inner_consistent(
                            mcx,
                            &typcache,
                            &lower,
                            &centroid_upper,
                            prev.as_ref().map(|(_pl, pu)| pu),
                        )?;
                        let which1 = if c1 > 0 {
                            (1 << 1) | (1 << 4)
                        } else if c1 < 0 {
                            (1 << 2) | (1 << 3)
                        } else {
                            0
                        };

                        // Also search for ranges adjacent to argument's upper
                        // bound: along the line just right of X=upper, which
                        // falls in quadrants 3 and 4, or 1 and 2.
                        let c2 = adjacent_inner_consistent(
                            mcx,
                            &typcache,
                            &upper,
                            &centroid_lower,
                            prev.as_ref().map(|(pl, _pu)| pl),
                        )?;
                        let which2 = if c2 > 0 {
                            (1 << 1) | (1 << 2)
                        } else if c2 < 0 {
                            (1 << 3) | (1 << 4)
                        } else {
                            0
                        };

                        // We must chase down ranges adjacent to either bound.
                        which &= which1 | which2;

                        need_previous = true;
                    }
                }
                RANGESTRAT_CONTAINS => {
                    // Non-empty range A contains non-empty range B if lower bound
                    // of A is lower or equal to lower bound of range B and upper
                    // bound of range A is greater than or equal to upper bound.
                    // All non-empty ranges contain an empty range.
                    strict_empty = false;
                    if !empty {
                        which &= (1 << 1) | (1 << 2) | (1 << 3) | (1 << 4);
                        max_lower = Some(lower);
                        min_upper = Some(upper);
                    }
                }
                RANGESTRAT_CONTAINED_BY => {
                    // The opposite of contains.
                    strict_empty = false;
                    if empty {
                        // An empty range is only contained by an empty range.
                        which &= 1 << 5;
                    } else {
                        min_lower = Some(lower);
                        max_upper = Some(upper);
                    }
                }
                RANGESTRAT_EQ => {
                    // Equal range can be only in the same quadrant where argument
                    // would be placed to.
                    strict_empty = false;
                    which &= 1 << get_quadrant(
                        &typcache,
                        centroid_p,
                        datum_get_range(&in_.scankeys[i].sk_argument),
                    )?;
                }
                _ => {
                    return Err(unrecognized_range_strategy(strategy));
                }
            }

            if strict_empty {
                if empty {
                    // Scan key is empty, no branches are satisfying.
                    which = 0;
                    break;
                } else {
                    // Shouldn't visit tree branch with empty ranges.
                    which &= (1 << 1) | (1 << 2) | (1 << 3) | (1 << 4);
                }
            }

            // Using the bounding box, see which quadrants we have to descend into.
            if let Some(ml) = min_lower.as_ref() {
                // If the centroid's lower bound is <= the minimum lower bound,
                // anything in the 3rd and 4th quadrants will have an even smaller
                // lower bound, and thus can't match.
                if range_cmp_bounds(&typcache, &centroid_lower, ml)? <= 0 {
                    which &= (1 << 1) | (1 << 2) | (1 << 5);
                }
            }
            if let Some(ml) = max_lower.as_ref() {
                // If the centroid's lower bound is > the maximum lower bound,
                // anything in the 1st and 2nd quadrants will also have a greater
                // than or equal lower bound, and thus can't match. If equal, we
                // can still exclude the 1st and 2nd quadrants if we're looking
                // for a value strictly greater than the maximum.
                let c = range_cmp_bounds(&typcache, &centroid_lower, ml)?;
                if c > 0 || (!inclusive && c == 0) {
                    which &= (1 << 3) | (1 << 4) | (1 << 5);
                }
            }
            if let Some(mu) = min_upper.as_ref() {
                // If the centroid's upper bound is <= the minimum upper bound,
                // anything in the 2nd and 3rd quadrants will have an even smaller
                // upper bound, and thus can't match.
                if range_cmp_bounds(&typcache, &centroid_upper, mu)? <= 0 {
                    which &= (1 << 1) | (1 << 4) | (1 << 5);
                }
            }
            if let Some(mu) = max_upper.as_ref() {
                // If the centroid's upper bound is > the maximum upper bound,
                // anything in the 1st and 4th quadrants will also have a greater
                // than or equal upper bound, and thus can't match. If equal, we
                // can still exclude the 1st and 4th quadrants if we're looking
                // for a value strictly greater than the maximum.
                let c = range_cmp_bounds(&typcache, &centroid_upper, mu)?;
                if c > 0 || (!inclusive && c == 0) {
                    which &= (1 << 2) | (1 << 3) | (1 << 5);
                }
            }

            if which == 0 {
                break; // no need to consider remaining conditions
            }
        }
    }

    // We must descend into the quadrant(s) identified by 'which'.
    // (C iterates `for (i = 1; i <= in->nNodes; i++)`; `in->nNodes` is the
    // inner tuple's node count, unaffected by writing `out->nNodes`.)
    let in_nnodes = in_.nNodes;
    out.nodeNumbers = Vec::with_capacity(in_nnodes as usize);
    if need_previous {
        out.traversalValues = Vec::with_capacity(in_nnodes as usize);
    }
    out.nNodes = 0;

    // Elements of traversalValues should be allocated in traversalMemoryContext
    // (here the owned byte buffers carry the prefix image copies).
    for i in 1..=in_nnodes {
        if which & (1 << i) != 0 {
            // Save previous prefix if needed.
            if need_previous {
                // We know in->prefixDatum here is varlena, because it's a range.
                // datumCopy(in->prefixDatum, false, -1) → copy the image bytes.
                let previous_centroid = in_.prefixDatum.as_ref_bytes().to_vec();
                out.traversalValues.push(Some(previous_centroid));
            }
            out.nodeNumbers.push(i - 1);
            out.nNodes += 1;
        }
    }

    let _ = mcx;
    Ok(())
}

/// `elog(ERROR, "unrecognized range strategy: %d", strategy)`
/// (rangetypes_spgist.c).
fn unrecognized_range_strategy(strategy: u16) -> PgError {
    PgError::error(format!("unrecognized range strategy: {strategy}"))
}

// ===========================================================================
// adjacent_cmp_bounds (rangetypes_spgist.c:785).
// ===========================================================================

/// `adjacent_cmp_bounds(typcache, arg, centroid)` (rangetypes_spgist.c:785).
/// Returns -1 for the "left" case (arg < centroid) and 1 for the "right" case
/// (arg >= centroid).
fn adjacent_cmp_bounds<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    arg: &RangeBound,
    centroid: &RangeBound,
) -> PgResult<i32> {
    debug_assert!(arg.lower != centroid.lower); // Assert(arg->lower != centroid->lower);

    let cmp = range_cmp_bounds(typcache, arg, centroid)?;

    if centroid.lower {
        // The argument is an upper bound; we search for adjacent lower bounds. A
        // matching adjacent lower bound must be *larger* than the argument, but
        // only just. We search left when the argument is smaller than, and not
        // adjacent, to the centroid. Otherwise search right.
        if cmp < 0 && !bounds_adjacent(mcx, typcache, *arg, *centroid)? {
            Ok(-1)
        } else {
            Ok(1)
        }
    } else {
        // The argument is a lower bound; we search for adjacent upper bounds. A
        // matching adjacent upper bound must be *smaller* than the argument, but
        // only just. We search left when the argument is <= the centroid.
        // Otherwise search right.
        if cmp <= 0 {
            Ok(-1)
        } else {
            Ok(1)
        }
    }
}

// ===========================================================================
// adjacent_inner_consistent (rangetypes_spgist.c:887).
// ===========================================================================

/// `adjacent_inner_consistent(typcache, arg, centroid, prev)`
/// (rangetypes_spgist.c:887). Like `adjacent_cmp_bounds`, but also takes into
/// account the previous level's centroid. Returns -1 (left), 1 (right), or 0 (no
/// matches below this centroid).
fn adjacent_inner_consistent<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    arg: &RangeBound,
    centroid: &RangeBound,
    prev: Option<&RangeBound>,
) -> PgResult<i32> {
    if let Some(prev) = prev {
        // Which direction were we supposed to traverse at previous level?
        let prevcmp = adjacent_cmp_bounds(mcx, typcache, arg, prev)?;

        // And which direction did we actually go?
        let cmp = range_cmp_bounds(typcache, centroid, prev)?;

        // If the two don't agree, there's nothing to see here.
        if (prevcmp < 0 && cmp >= 0) || (prevcmp > 0 && cmp < 0) {
            return Ok(0);
        }
    }

    adjacent_cmp_bounds(mcx, typcache, arg, centroid)
}

// ===========================================================================
// The SP-GiST leaf-consistent function (rangetypes_spgist.c:917).
// ===========================================================================

/// `spg_range_quad_leaf_consistent` (rangetypes_spgist.c:917). Check the leaf
/// value against the query using the corresponding function. Returns the boolean
/// match result.
pub fn spg_range_quad_leaf_consistent<'mcx>(
    mcx: Mcx<'mcx>,
    in_: &spgLeafConsistentIn<'mcx>,
    out: &mut spgLeafConsistentOut<'mcx>,
) -> PgResult<bool> {
    let leaf_range = datum_get_range(&in_.leafDatum);

    // all tests are exact
    out.recheck = false;

    // leafDatum is what it is...
    out.leafValue = Some(in_.leafDatum.clone());

    let typcache = range_get_typcache(leaf_range.rangetypid())?;

    // Perform the required comparison(s).
    let mut res = true;
    for i in 0..in_.nkeys() as usize {
        let key = &in_.scankeys[i];
        // Datum keyDatum = in->scankeys[i].sk_argument;

        // Call the function corresponding to the scan strategy.
        res = match key.sk_strategy {
            RANGESTRAT_BEFORE => {
                range_before_internal(&typcache, leaf_range, datum_get_range(&key.sk_argument))?
            }
            RANGESTRAT_OVERLEFT => {
                range_overleft_internal(&typcache, leaf_range, datum_get_range(&key.sk_argument))?
            }
            RANGESTRAT_OVERLAPS => {
                range_overlaps_internal(&typcache, leaf_range, datum_get_range(&key.sk_argument))?
            }
            RANGESTRAT_OVERRIGHT => {
                range_overright_internal(&typcache, leaf_range, datum_get_range(&key.sk_argument))?
            }
            RANGESTRAT_AFTER => {
                range_after_internal(&typcache, leaf_range, datum_get_range(&key.sk_argument))?
            }
            RANGESTRAT_ADJACENT => range_adjacent_internal(
                mcx,
                &typcache,
                leaf_range,
                datum_get_range(&key.sk_argument),
            )?,
            RANGESTRAT_CONTAINS => {
                range_contains_internal(&typcache, leaf_range, datum_get_range(&key.sk_argument))?
            }
            RANGESTRAT_CONTAINED_BY => range_contained_by_internal(
                &typcache,
                leaf_range,
                datum_get_range(&key.sk_argument),
            )?,
            RANGESTRAT_CONTAINS_ELEM => {
                range_contains_elem_internal(&typcache, leaf_range, elem_datum(&key.sk_argument))?
            }
            RANGESTRAT_EQ => {
                range_eq_internal(&typcache, leaf_range, datum_get_range(&key.sk_argument))?
            }
            other => {
                return Err(unrecognized_range_strategy(other));
            }
        };

        // If leaf datum doesn't match to a query key, no need to check
        // subsequent keys.
        if !res {
            break;
        }
    }

    Ok(res)
}
