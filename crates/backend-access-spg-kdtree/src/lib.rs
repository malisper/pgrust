//! Port of `src/backend/access/spgist/spgkdtreeproc.c` (PostgreSQL 18.3) -- the
//! SP-GiST k-d-tree support procedures over `point`.
//!
//! A k-d tree alternates the splitting axis with depth: odd levels split on the
//! x coordinate, even levels split on y.  Each inner node stores a single
//! `float8` prefix (the splitting coordinate) and has exactly two child nodes
//! (no node labels).
//!
//! Every function in the C file is ported here 1:1 -- identical control flow,
//! branch order, bit masks, switch arms, message text and SQLSTATE:
//!
//!   * [`spg_kd_config`]            (config, `spgkdtreeproc.c:27`)
//!   * [`getSide`]                  (static, `spgkdtreeproc.c:40`)
//!   * [`x_cmp`] / [`y_cmp`]        (static qsort comparators, `:84`/`:95`)
//!   * [`spg_kd_choose`]            (choose, `spgkdtreeproc.c:53`)
//!   * [`spg_kd_picksplit`]         (picksplit, `spgkdtreeproc.c:107`)
//!   * [`spg_kd_inner_consistent`]  (inner consistent, `spgkdtreeproc.c:159`)
//!
//! `spg_kd_leaf_consistent` is NOT defined in `spgkdtreeproc.c`: the C file ends
//! with a comment stating it borrows `spg_quad_leaf_consistent` verbatim (same
//! operators, same leaf data type), and the opclass catalog registers that very
//! function for the k-d-tree `leaf_consistent` slot.  That core lives in the
//! sibling `spgquadtreeproc.c` (crate `backend-access-spg-quadtree`), so it is
//! not re-ported here.
//!
//! ## Idiomatic working structs
//!
//! The C fmgr entry points (`Datum NAME(PG_FUNCTION_ARGS)`) receive pointers to
//! the `spg*In`/`spg*Out` structs and `palloc` their output arrays.  As in the
//! sibling SP-GiST ports (`geo_spgist.c`), the entry points here take owned
//! working structs ([`SpgChooseIn`] etc.) that mirror the C field set; "allocate
//! an output array" becomes "fill an owned `Vec`".  The `point`/`box`/`float8`
//! payloads carried by the C `Datum` fields are decoded to their concrete
//! [`types_core::geo`] forms.  In particular the opclass-specific
//! `void *traversalValue` round-trips a `BOX` here -- exactly as the C source.
//!
//! ## Seams (genuinely-external deps only)
//!
//! The fuzzy float comparators `FPlt`/`FPgt` (geo_decls.h, owned by the unported
//! `geo_ops.c`) are routed through `backend-utils-adt-geo-ops-seams`.  The one
//! cross-file call `spg_key_orderbys_distances` (`spgproc.c`, owner
//! `backend-access-spg-proc`) is routed through
//! `backend-access-spg-proc-seams::spg_key_orderbys_distances`.  Everything else
//! (`palloc`/`MemoryContextSwitchTo`/`box_copy`) is Rust allocation, in-crate.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(clippy::result_large_err)]

use types_core::geo::{Point, SpgKey, BOX};
use types_error::{ErrorLocation, PgError, PgResult, ERRCODE_INTERNAL_ERROR};

use backend_access_spg_proc_seams::spg_key_orderbys_distances;
use backend_utils_adt_geo_ops_seams::{FPgt, FPlt};

const C_FILE: &str = "../src/backend/access/spgist/spgkdtreeproc.c";

/// `get_float8_infinity()` (float.c) -- `f64::INFINITY`.  Inlined as in the
/// sibling `geo_spgist.c` port.
#[inline]
fn get_float8_infinity() -> f64 {
    f64::INFINITY
}

// SP-GiST RT strategy numbers used by the point opclasses (access/stratnum.h).
/// `RTLeftStrategyNumber`.
pub const RTLeftStrategyNumber: u16 = 1;
/// `RTRightStrategyNumber`.
pub const RTRightStrategyNumber: u16 = 5;
/// `RTSameStrategyNumber`.
pub const RTSameStrategyNumber: u16 = 6;
/// `RTContainedByStrategyNumber`.
pub const RTContainedByStrategyNumber: u16 = 8;
/// `RTBelowStrategyNumber`.
pub const RTBelowStrategyNumber: u16 = 10;
/// `RTAboveStrategyNumber`.
pub const RTAboveStrategyNumber: u16 = 11;
/// `RTOldBelowStrategyNumber`.
pub const RTOldBelowStrategyNumber: u16 = 29;
/// `RTOldAboveStrategyNumber`.
pub const RTOldAboveStrategyNumber: u16 = 30;

/// `FLOAT8OID` (pg_type_d.h).
pub const FLOAT8OID: u32 = 701;
/// `VOIDOID` (pg_type_d.h).
pub const VOIDOID: u32 = 2278;

// ===========================================================================
// Working in/out structs (idiomatic mirrors of the spgist.h ABI structs).
// ===========================================================================

/// Working mirror of `spgConfigOut` (spgist.h:42) -- the fields the k-d-tree
/// point opclass sets.
#[derive(Clone, Copy, Debug, Default)]
pub struct SpgConfigOut {
    /// `prefixType`.
    pub prefixType: u32,
    /// `labelType`.
    pub labelType: u32,
    /// `leafType` (left default by the point opclass; the core fills it).
    pub leafType: u32,
    /// `canReturnData`.
    pub canReturnData: bool,
    /// `longValuesOK`.
    pub longValuesOK: bool,
}

/// A scan key the opclass examines: the C `ScanKeyData`'s `sk_strategy` plus the
/// decoded `sk_argument` (a `point`, or a `box` for `RTContainedBy`).
#[derive(Clone, Debug)]
pub struct SpgScanKey {
    /// `sk_strategy`.
    pub sk_strategy: u16,
    /// `DatumGetPointP(sk_argument)` -- the query point.
    pub query_point: Point,
    /// `DatumGetBoxP(sk_argument)` -- the query box, for the `RTContainedBy`
    /// strategy.
    pub query_box: BOX,
}

/// Working mirror of `spgChooseIn` (spgist.h:53).
#[derive(Clone, Debug)]
pub struct SpgChooseIn {
    /// `DatumGetPointP(in->datum)`.
    pub in_point: Point,
    /// `in->level`.
    pub level: i32,
    /// `in->allTheSame`.
    pub allTheSame: bool,
    /// `in->hasPrefix`.
    pub hasPrefix: bool,
    /// `DatumGetFloat8(in->prefixDatum)`; valid iff `hasPrefix`.
    pub prefix_coord: f64,
    /// `in->nNodes`.
    pub nNodes: i32,
}

/// Working mirror of `spgChooseOut` (spgist.h:73) for the `spgMatchNode` result.
#[derive(Clone, Debug, Default)]
pub struct SpgChooseOut {
    /// `out->resultType`.
    pub resultType: spgChooseResultType,
    /// `out->result.matchNode.nodeN`.
    pub nodeN: i32,
    /// `out->result.matchNode.levelAdd`.
    pub levelAdd: i32,
    /// `out->result.matchNode.restDatum`, decoded back to a point.
    pub rest_point: Point,
}

/// `spgChooseResultType` (spgist.h:66).
pub type spgChooseResultType = u32;
/// `spgMatchNode` (spgist.h:66): descend into existing node.
pub const spgMatchNode: spgChooseResultType = 1;

/// Working mirror of `spgPickSplitIn` (spgist.h:111).
#[derive(Clone, Debug)]
pub struct SpgPickSplitIn {
    /// `DatumGetPointP(in->datums[i])` for each of `in->nTuples` leaf tuples.
    pub points: Vec<Point>,
    /// `in->level`.
    pub level: i32,
}

impl SpgPickSplitIn {
    /// `in->nTuples`.
    #[inline]
    pub fn n_tuples(&self) -> i32 {
        self.points.len() as i32
    }
}

/// Working mirror of `spgPickSplitOut` (spgist.h:118) for the k-d-tree opclass.
#[derive(Clone, Debug, Default)]
pub struct SpgPickSplitOut {
    /// `out->hasPrefix`.
    pub hasPrefix: bool,
    /// `Float8GetDatum(coord)` decoded -- the prefix coordinate.
    pub prefix_coord: f64,
    /// `out->nNodes`.
    pub nNodes: i32,
    /// `out->mapTuplesToNodes`.
    pub mapTuplesToNodes: Vec<i32>,
    /// `out->leafTupleDatums`, decoded back to points.
    pub leafTupleDatums: Vec<Point>,
}

/// Working mirror of `spgInnerConsistentIn` (spgist.h:130).
#[derive(Clone, Debug)]
pub struct SpgInnerConsistentIn {
    /// `in->scankeys` (length `in->nkeys`), pre-decoded.
    pub scankeys: Vec<SpgScanKey>,
    /// `DatumGetPointP(orderbys[i].sk_argument)` (length `in->norderbys`).
    pub orderby_points: Vec<Point>,
    /// `in->norderbys`.
    pub norderbys: i32,
    /// `(BOX *) in->traversalValue` (the parent's bounding box), if any.
    pub traversalValue: Option<BOX>,
    /// `in->level`.
    pub level: i32,
    /// `in->allTheSame`.
    pub allTheSame: bool,
    /// `in->hasPrefix`.
    pub hasPrefix: bool,
    /// `DatumGetFloat8(in->prefixDatum)`.
    pub prefix_coord: f64,
    /// `in->nNodes`.
    pub nNodes: i32,
}

impl SpgInnerConsistentIn {
    /// `in->nkeys`.
    #[inline]
    pub fn n_keys(&self) -> i32 {
        self.scankeys.len() as i32
    }
}

/// Working mirror of `spgInnerConsistentOut` (spgist.h:149).
#[derive(Clone, Debug, Default)]
pub struct SpgInnerConsistentOut {
    /// `out->nNodes`.
    pub nNodes: i32,
    /// `out->nodeNumbers`.
    pub nodeNumbers: Vec<i32>,
    /// `out->levelAdds`.
    pub levelAdds: Vec<i32>,
    /// `out->traversalValues` (one bounding box per visited node).
    pub traversalValues: Vec<BOX>,
    /// `out->distances` (one distance array per visited node).
    pub distances: Vec<Vec<f64>>,
}

// ===========================================================================
// K-d tree (spgkdtreeproc.c)
// ===========================================================================

/// `spg_kd_config` (spgkdtreeproc.c:27): fill the config for a k-d tree.
pub fn spg_kd_config(cfg: &mut SpgConfigOut) {
    // spgConfigIn *cfgin = (spgConfigIn *) PG_GETARG_POINTER(0);  -- unused
    cfg.prefixType = FLOAT8OID;
    cfg.labelType = VOIDOID; // we don't need node labels
    cfg.canReturnData = true;
    cfg.longValuesOK = false;
}

/// `getSide` (spgkdtreeproc.c:40): which side of `coord` the `tst` point's x (if
/// `isX`) or y coordinate lies on: 0 equal, 1 if `coord > tstcoord`, else -1.
pub fn getSide(coord: f64, isX: bool, tst: &Point) -> i32 {
    let tstcoord = if isX { tst.x } else { tst.y };

    if coord == tstcoord {
        0
    } else if coord > tstcoord {
        1
    } else {
        -1
    }
}

/// `x_cmp` (spgkdtreeproc.c:84): qsort comparator on x.
pub fn x_cmp(pa: &Point, pb: &Point) -> core::cmp::Ordering {
    use core::cmp::Ordering;
    if pa.x == pb.x {
        Ordering::Equal
    } else if pa.x > pb.x {
        Ordering::Greater
    } else {
        Ordering::Less
    }
}

/// `y_cmp` (spgkdtreeproc.c:95): qsort comparator on y.
pub fn y_cmp(pa: &Point, pb: &Point) -> core::cmp::Ordering {
    use core::cmp::Ordering;
    if pa.y == pb.y {
        Ordering::Equal
    } else if pa.y > pb.y {
        Ordering::Greater
    } else {
        Ordering::Less
    }
}

/// `spg_kd_choose` (spgkdtreeproc.c:53).
pub fn spg_kd_choose(in_: &SpgChooseIn, out: &mut SpgChooseOut) -> PgResult<()> {
    // Point *inPoint = DatumGetPointP(in->datum);
    let inPoint = &in_.in_point;

    if in_.allTheSame {
        return Err(alltesame_kd("spg_kd_choose", 62));
    }

    debug_assert!(in_.hasPrefix);
    // coord = DatumGetFloat8(in->prefixDatum);
    let coord = in_.prefix_coord;

    debug_assert!(in_.nNodes == 2);

    out.resultType = spgMatchNode;
    // out->result.matchNode.nodeN =
    //     (getSide(coord, in->level % 2, inPoint) > 0) ? 0 : 1;
    out.nodeN = if getSide(coord, (in_.level % 2) != 0, inPoint) > 0 {
        0
    } else {
        1
    };
    out.levelAdd = 1;
    out.rest_point = *inPoint; // PointPGetDatum(inPoint)

    Ok(())
}

/// `spg_kd_picksplit` (spgkdtreeproc.c:107).
pub fn spg_kd_picksplit(in_: &SpgPickSplitIn, out: &mut SpgPickSplitOut) {
    let n_tuples = in_.n_tuples() as usize;

    // sorted = palloc(sizeof(*sorted) * in->nTuples);
    // SortedPoint { p, i } -- (point, original index).
    let mut sorted: Vec<(Point, usize)> = (0..n_tuples).map(|i| (in_.points[i], i)).collect();

    let odd_level = (in_.level % 2) != 0;
    // C: qsort(sorted, in->nTuples, sizeof(*sorted),
    //          (in->level % 2) ? x_cmp : y_cmp);
    //
    // Mirror the sibling `geo_spgist.c` port's `slice::sort_by(compare)`
    // convention.  The C comment at :138 notes that boundary tuples with a tied
    // splitting coordinate may fall into either node, which is fine as long as
    // inner_consistent descends both sides -- so a stable sort is behaviour-
    // preserving for the split's correctness.
    if odd_level {
        sorted.sort_by(|a, b| x_cmp(&a.0, &b.0));
    } else {
        sorted.sort_by(|a, b| y_cmp(&a.0, &b.0));
    }

    // middle = in->nTuples >> 1;
    let middle = n_tuples >> 1;
    // coord = (in->level % 2) ? sorted[middle].p->x : sorted[middle].p->y;
    let coord = if odd_level {
        sorted[middle].0.x
    } else {
        sorted[middle].0.y
    };

    out.hasPrefix = true;
    out.prefix_coord = coord; // Float8GetDatum(coord)

    out.nNodes = 2;
    // out->nodeLabels = NULL -- we don't need node labels.

    // out->mapTuplesToNodes = palloc(sizeof(int) * in->nTuples);
    // out->leafTupleDatums = palloc(sizeof(Datum) * in->nTuples);
    out.mapTuplesToNodes = vec![0; n_tuples];
    out.leafTupleDatums = vec![Point::default(); n_tuples];

    // for (i = 0; ...) {
    //     Point *p = sorted[i].p; int n = sorted[i].i;
    //     out->mapTuplesToNodes[n] = (i < middle) ? 0 : 1;
    //     out->leafTupleDatums[n] = PointPGetDatum(p);
    // }
    for (i, (p, n)) in sorted.iter().enumerate() {
        out.mapTuplesToNodes[*n] = if i < middle { 0 } else { 1 };
        out.leafTupleDatums[*n] = *p;
    }
}

/// `spg_kd_inner_consistent` (spgkdtreeproc.c:159).
pub fn spg_kd_inner_consistent(
    in_: &SpgInnerConsistentIn,
    out: &mut SpgInnerConsistentOut,
) -> PgResult<()> {
    debug_assert!(in_.hasPrefix);
    // coord = DatumGetFloat8(in->prefixDatum);
    let coord = in_.prefix_coord;

    if in_.allTheSame {
        return Err(alltesame_kd("spg_kd_inner_consistent", 173));
    }

    debug_assert!(in_.nNodes == 2);

    // "which" is a bitmask of children that satisfy all constraints.
    let mut which: i32 = (1 << 1) | (1 << 2);

    let odd_level = (in_.level % 2) != 0;

    for i in 0..in_.n_keys() as usize {
        // Point *query = DatumGetPointP(in->scankeys[i].sk_argument);
        let key = &in_.scankeys[i];
        let query = &key.query_point;

        match key.sk_strategy {
            RTLeftStrategyNumber => {
                if odd_level && FPlt::call(query.x, coord) {
                    which &= 1 << 1;
                }
            }
            RTRightStrategyNumber => {
                if odd_level && FPgt::call(query.x, coord) {
                    which &= 1 << 2;
                }
            }
            RTSameStrategyNumber => {
                if odd_level {
                    if FPlt::call(query.x, coord) {
                        which &= 1 << 1;
                    } else if FPgt::call(query.x, coord) {
                        which &= 1 << 2;
                    }
                } else if FPlt::call(query.y, coord) {
                    which &= 1 << 1;
                } else if FPgt::call(query.y, coord) {
                    which &= 1 << 2;
                }
            }
            RTBelowStrategyNumber | RTOldBelowStrategyNumber => {
                if !odd_level && FPlt::call(query.y, coord) {
                    which &= 1 << 1;
                }
            }
            RTAboveStrategyNumber | RTOldAboveStrategyNumber => {
                if !odd_level && FPgt::call(query.y, coord) {
                    which &= 1 << 2;
                }
            }
            RTContainedByStrategyNumber => {
                // For this operator, the query is a box not a point.  We cheat to
                // the extent of assuming that DatumGetPointP won't do anything
                // that would be bad for a pointer-to-box.
                // boxQuery = DatumGetBoxP(in->scankeys[i].sk_argument);
                let boxQuery = &key.query_box;

                if odd_level {
                    if FPlt::call(boxQuery.high.x, coord) {
                        which &= 1 << 1;
                    } else if FPgt::call(boxQuery.low.x, coord) {
                        which &= 1 << 2;
                    }
                } else if FPlt::call(boxQuery.high.y, coord) {
                    which &= 1 << 1;
                } else if FPgt::call(boxQuery.low.y, coord) {
                    which &= 1 << 2;
                }
            }
            other => {
                return Err(unrecognized_strategy(
                    other as i32,
                    "spg_kd_inner_consistent",
                    247,
                ));
            }
        }

        if which == 0 {
            break; // no need to consider remaining conditions
        }
    }

    // We must descend into the children identified by which.
    out.nNodes = 0;

    // Fast-path for no matching children.
    if which == 0 {
        return Ok(());
    }

    // out->nodeNumbers = (int *) palloc(sizeof(int) * 2);
    out.nodeNumbers = vec![0; 2];

    // When ordering scan keys are specified, calculate bounding boxes for both
    // children, saved as traversalValues.  Calculation of those bounding boxes
    // on a non-zero level requires knowledge of the bounding box of the upper
    // node, which we get from traversalValue.
    let mut bboxes = [BOX::default(), BOX::default()];
    if in_.norderbys > 0 {
        let area = if in_.level == 0 {
            let inf = get_float8_infinity();
            let mut infArea = BOX::default();
            infArea.high.x = inf;
            infArea.high.y = inf;
            infArea.low.x = -inf;
            infArea.low.y = -inf;
            infArea
        } else {
            // area = (BOX *) in->traversalValue;  Assert(area);
            in_.traversalValue.ok_or_else(|| {
                PgError::error(
                    "spg_kd_inner_consistent: traversalValue must be set at non-zero level",
                )
            })?
        };

        bboxes[0].low = area.low;
        bboxes[1].high = area.high;

        if odd_level {
            // split box by x
            bboxes[0].high.x = coord;
            bboxes[1].low.x = coord;
            bboxes[0].high.y = area.high.y;
            bboxes[1].low.y = area.low.y;
        } else {
            // split box by y
            bboxes[0].high.y = coord;
            bboxes[1].low.y = coord;
            bboxes[0].high.x = area.high.x;
            bboxes[1].low.x = area.low.x;
        }
    }

    for i in 1..=2 {
        if which & (1 << i) != 0 {
            // out->nodeNumbers[out->nNodes] = i - 1;
            out.nodeNumbers[out.nNodes as usize] = i - 1;

            if in_.norderbys > 0 {
                // BOX *box = box_copy(&bboxes[i - 1]);  (in traversalMemoryContext)
                let b = bboxes[(i - 1) as usize];
                // out->traversalValues[out->nNodes] = box;
                out.traversalValues.push(b);
                // out->distances[out->nNodes] =
                //     spg_key_orderbys_distances(BoxPGetDatum(box), false,
                //                                in->orderbys, in->norderbys);
                out.distances.push(spg_key_orderbys_distances::call(
                    &SpgKey::InnerBox(b),
                    &in_.orderby_points,
                )?);
            }

            out.nNodes += 1;
        }
    }

    // Set up level increments, too.
    // out->levelAdds[0] = out->levelAdds[1] = 1;
    out.levelAdds = vec![1, 1];

    Ok(())
}

// ===========================================================================
// Error reporters
// ===========================================================================

/// `elog(ERROR, "unrecognized strategy number: %d", strategy)`
/// (spgkdtreeproc.c:247).
fn unrecognized_strategy(strategy: i32, func: &'static str, line: i32) -> PgError {
    PgError::error(format!("unrecognized strategy number: {strategy}"))
        .with_sqlstate(ERRCODE_INTERNAL_ERROR)
        .with_error_location(ErrorLocation::new(C_FILE, line, func))
}

/// `elog(ERROR, "allTheSame should not occur for k-d trees")`
/// (spgkdtreeproc.c:62 / spgkdtreeproc.c:173).
fn alltesame_kd(func: &'static str, line: i32) -> PgError {
    PgError::error("allTheSame should not occur for k-d trees")
        .with_sqlstate(ERRCODE_INTERNAL_ERROR)
        .with_error_location(ErrorLocation::new(C_FILE, line, func))
}
