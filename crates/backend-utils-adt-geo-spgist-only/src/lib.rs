//! Port of `src/backend/utils/adt/geo_spgist.c` (PostgreSQL 18.3) -- the
//! SP-GiST implementation of a 4-dimensional quad tree over `box`es (and, by
//! bounding box, `polygon`s): the `spg_box_quad_*` / `spg_bbox_quad_*` /
//! `spg_poly_quad_*` opclass support functions.
//!
//! Every function in the C file is ported with identical logic, branch order,
//! loop bounds, message text and SQLSTATE.
//!
//! ## Shape
//!
//! The fmgr `Datum NAME(PG_FUNCTION_ARGS)` entry points in C receive pointers
//! to the `spg*In`/`spg*Out` structs and `palloc` their output arrays.  Here
//! the entry points take owned working structs ([`SpgChooseIn`] etc.) that
//! mirror the box opclass's field set; "allocate an output array" becomes "fill
//! an owned `Vec`".  The `Range`/`RangeBox`/`RectBox` working types are plain
//! `Copy` structs (the C ones are `palloc`'d, but their lifetimes are entirely
//! local to a call -- except the next-traversal `RectBox`es, which become owned
//! values in `out.traversalValues`).
//!
//! ## Cross-crate calls (seams)
//!
//! The geo_ops box predicates, the `FPge`/`FPle`/`FPlt`/`FPgt` fuzzy
//! comparators and `HYPOT` live in the unported `backend-utils-adt-geo-ops`
//! unit; the order-by distance routine `spg_key_orderbys_distances` lives in
//! the unported `backend-access-spg-proc` unit.  Both are reached through their
//! owners' seam crates and panic until those owners land.  The
//! `Datum`->geometry decode (`DatumGetBoxP`/`DatumGetPolygonP`) is the
//! fmgr/`Datum` boundary, performed by the caller, which hands pre-decoded
//! geometry into the working structs.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::needless_range_loop)]

use core::cmp::Ordering;

use backend_access_spg_proc_seams::spg_key_orderbys_distances;
use backend_utils_adt_geo_ops_seams::{
    box_above, box_below, box_contain, box_contained, box_left, box_overabove, box_overbelow,
    box_overlap, box_overleft, box_overright, box_right, box_same, FPge, FPgt, FPle, FPlt, HYPOT,
};
use types_core::geo::{Point, SpgKey, BOX};
use types_error::{ErrorLocation, PgError, PgResult, ERRCODE_INTERNAL_ERROR};

// ===========================================================================
// Constants
// ===========================================================================

/// `BOXOID` (pg_type.h).
pub const BOXOID: u32 = 603;
/// `POLYGONOID` (pg_type.h).
pub const POLYGONOID: u32 = 604;
/// `VOIDOID` (pg_type.h).
pub const VOIDOID: u32 = 2278;

/// `F_DIST_POLYP` (fmgroids.h): the `<polygon> <-> <point>` distance function
/// OID, used to decide whether to recheck order-by distances.
pub const F_DIST_POLYP: u32 = 3292;

/// SP-GiST RT strategy numbers used by the box/polygon opclasses
/// (`<access/stratnum.h>`).
pub const RTLeftStrategyNumber: u16 = 1;
pub const RTOverLeftStrategyNumber: u16 = 2;
pub const RTOverlapStrategyNumber: u16 = 3;
pub const RTOverRightStrategyNumber: u16 = 4;
pub const RTRightStrategyNumber: u16 = 5;
pub const RTSameStrategyNumber: u16 = 6;
pub const RTContainsStrategyNumber: u16 = 7;
pub const RTContainedByStrategyNumber: u16 = 8;
pub const RTOverBelowStrategyNumber: u16 = 9;
pub const RTBelowStrategyNumber: u16 = 10;
pub const RTAboveStrategyNumber: u16 = 11;
pub const RTOverAboveStrategyNumber: u16 = 12;

const C_FILE: &str = "src/backend/utils/adt/geo_spgist.c";

/// `get_float8_infinity()` (utils/float.h): the inline helper returns
/// `(float8) INFINITY`; reproduced inline as it is a pure constant with no
/// ownership.
#[inline]
fn get_float8_infinity() -> f64 {
    f64::INFINITY
}

// ===========================================================================
// Internal working types (geo_spgist.c:103-119)
// ===========================================================================

/// `Range` (geo_spgist.c:103).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Range {
    pub low: f64,
    pub high: f64,
}

/// `RangeBox` (geo_spgist.c:109).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct RangeBox {
    pub left: Range,
    pub right: Range,
}

/// `RectBox` (geo_spgist.c:115).  Used as the SP-GiST traversal value.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct RectBox {
    pub range_box_x: RangeBox,
    pub range_box_y: RangeBox,
}

// ===========================================================================
// Helpers
// ===========================================================================

/// `compareDoubles` (geo_spgist.c:92): qsort comparator over `float8`.
///
/// Plain `==`/`>` comparison (no fuzzy macros): this only affects index
/// performance, not correctness.
#[inline]
pub fn compare_doubles(x: f64, y: f64) -> Ordering {
    if x == y {
        Ordering::Equal
    } else if x > y {
        Ordering::Greater
    } else {
        Ordering::Less
    }
}

/// `getQuadrant` (geo_spgist.c:129): the 4-bit (16-way) quadrant of `inBox`
/// relative to `centroid`.
pub fn getQuadrant(centroid: &BOX, inBox: &BOX) -> u8 {
    let mut quadrant: u8 = 0;

    if inBox.low.x > centroid.low.x {
        quadrant |= 0x8;
    }

    if inBox.high.x > centroid.high.x {
        quadrant |= 0x4;
    }

    if inBox.low.y > centroid.low.y {
        quadrant |= 0x2;
    }

    if inBox.high.y > centroid.high.y {
        quadrant |= 0x1;
    }

    quadrant
}

/// `getRangeBox` (geo_spgist.c:156): reinterpret a `BOX` as a point in 4-D space
/// (a [`RangeBox`]).
pub fn getRangeBox(b: &BOX) -> RangeBox {
    RangeBox {
        left: Range {
            low: b.low.x,
            high: b.high.x,
        },
        right: Range {
            low: b.low.y,
            high: b.high.y,
        },
    }
}

/// `initRectBox` (geo_spgist.c:176): the unbounded (whole-4-D-space) traversal
/// value.
pub fn initRectBox() -> RectBox {
    let infinity = get_float8_infinity();

    RectBox {
        range_box_x: RangeBox {
            left: Range {
                low: -infinity,
                high: infinity,
            },
            right: Range {
                low: -infinity,
                high: infinity,
            },
        },
        range_box_y: RangeBox {
            left: Range {
                low: -infinity,
                high: infinity,
            },
            right: Range {
                low: -infinity,
                high: infinity,
            },
        },
    }
}

/// `nextRectBox` (geo_spgist.c:204): the traversal value for `quadrant` of
/// `centroid` inside `rect_box`.
pub fn nextRectBox(rect_box: &RectBox, centroid: &RangeBox, quadrant: u8) -> RectBox {
    // memcpy(next_rect_box, rect_box, sizeof(RectBox));
    let mut next_rect_box = *rect_box;

    if quadrant & 0x8 != 0 {
        next_rect_box.range_box_x.left.low = centroid.left.low;
    } else {
        next_rect_box.range_box_x.left.high = centroid.left.low;
    }

    if quadrant & 0x4 != 0 {
        next_rect_box.range_box_x.right.low = centroid.left.high;
    } else {
        next_rect_box.range_box_x.right.high = centroid.left.high;
    }

    if quadrant & 0x2 != 0 {
        next_rect_box.range_box_y.left.low = centroid.right.low;
    } else {
        next_rect_box.range_box_y.left.high = centroid.right.low;
    }

    if quadrant & 0x1 != 0 {
        next_rect_box.range_box_y.right.low = centroid.right.high;
    } else {
        next_rect_box.range_box_y.right.high = centroid.right.high;
    }

    next_rect_box
}

// ---------------------------------------------------------------------------
// Quadrant overlap/containment predicates (geo_spgist.c:235-370)
// ---------------------------------------------------------------------------

/// `overlap2D` (geo_spgist.c:235).
pub fn overlap2D(range_box: &RangeBox, query: &Range) -> bool {
    FPge::call(range_box.right.high, query.low) && FPle::call(range_box.left.low, query.high)
}

/// `overlap4D` (geo_spgist.c:243).
pub fn overlap4D(rect_box: &RectBox, query: &RangeBox) -> bool {
    overlap2D(&rect_box.range_box_x, &query.left) && overlap2D(&rect_box.range_box_y, &query.right)
}

/// `contain2D` (geo_spgist.c:251).
pub fn contain2D(range_box: &RangeBox, query: &Range) -> bool {
    FPge::call(range_box.right.high, query.high) && FPle::call(range_box.left.low, query.low)
}

/// `contain4D` (geo_spgist.c:259).
pub fn contain4D(rect_box: &RectBox, query: &RangeBox) -> bool {
    contain2D(&rect_box.range_box_x, &query.left) && contain2D(&rect_box.range_box_y, &query.right)
}

/// `contained2D` (geo_spgist.c:267).
pub fn contained2D(range_box: &RangeBox, query: &Range) -> bool {
    FPle::call(range_box.left.low, query.high)
        && FPge::call(range_box.left.high, query.low)
        && FPle::call(range_box.right.low, query.high)
        && FPge::call(range_box.right.high, query.low)
}

/// `contained4D` (geo_spgist.c:277).
pub fn contained4D(rect_box: &RectBox, query: &RangeBox) -> bool {
    contained2D(&rect_box.range_box_x, &query.left)
        && contained2D(&rect_box.range_box_y, &query.right)
}

/// `lower2D` (geo_spgist.c:285).
pub fn lower2D(range_box: &RangeBox, query: &Range) -> bool {
    FPlt::call(range_box.left.low, query.low) && FPlt::call(range_box.right.low, query.low)
}

/// `overLower2D` (geo_spgist.c:293).
pub fn overLower2D(range_box: &RangeBox, query: &Range) -> bool {
    FPle::call(range_box.left.low, query.high) && FPle::call(range_box.right.low, query.high)
}

/// `higher2D` (geo_spgist.c:301).
pub fn higher2D(range_box: &RangeBox, query: &Range) -> bool {
    FPgt::call(range_box.left.high, query.high) && FPgt::call(range_box.right.high, query.high)
}

/// `overHigher2D` (geo_spgist.c:309).
pub fn overHigher2D(range_box: &RangeBox, query: &Range) -> bool {
    FPge::call(range_box.left.high, query.low) && FPge::call(range_box.right.high, query.low)
}

/// `left4D` (geo_spgist.c:317).
pub fn left4D(rect_box: &RectBox, query: &RangeBox) -> bool {
    lower2D(&rect_box.range_box_x, &query.left)
}

/// `overLeft4D` (geo_spgist.c:324).
pub fn overLeft4D(rect_box: &RectBox, query: &RangeBox) -> bool {
    overLower2D(&rect_box.range_box_x, &query.left)
}

/// `right4D` (geo_spgist.c:331).
pub fn right4D(rect_box: &RectBox, query: &RangeBox) -> bool {
    higher2D(&rect_box.range_box_x, &query.left)
}

/// `overRight4D` (geo_spgist.c:338).
pub fn overRight4D(rect_box: &RectBox, query: &RangeBox) -> bool {
    overHigher2D(&rect_box.range_box_x, &query.left)
}

/// `below4D` (geo_spgist.c:345).
pub fn below4D(rect_box: &RectBox, query: &RangeBox) -> bool {
    lower2D(&rect_box.range_box_y, &query.right)
}

/// `overBelow4D` (geo_spgist.c:352).
pub fn overBelow4D(rect_box: &RectBox, query: &RangeBox) -> bool {
    overLower2D(&rect_box.range_box_y, &query.right)
}

/// `above4D` (geo_spgist.c:359).
pub fn above4D(rect_box: &RectBox, query: &RangeBox) -> bool {
    higher2D(&rect_box.range_box_y, &query.right)
}

/// `overAbove4D` (geo_spgist.c:366).
pub fn overAbove4D(rect_box: &RectBox, query: &RangeBox) -> bool {
    overHigher2D(&rect_box.range_box_y, &query.right)
}

/// `pointToRectBoxDistance` (geo_spgist.c:373): a lower bound for the distance
/// between `point` and `rect_box`.
///
/// `HYPOT` is `pg_hypot`, which `ereport(ERROR)`s on float over/underflow; that
/// error longjmps out of the consistent call in C, so it propagates as `Err`.
pub fn pointToRectBoxDistance(point: &Point, rect_box: &RectBox) -> PgResult<f64> {
    let dx = if point.x < rect_box.range_box_x.left.low {
        rect_box.range_box_x.left.low - point.x
    } else if point.x > rect_box.range_box_x.right.high {
        point.x - rect_box.range_box_x.right.high
    } else {
        0.0
    };

    let dy = if point.y < rect_box.range_box_y.left.low {
        rect_box.range_box_y.left.low - point.y
    } else if point.y > rect_box.range_box_y.right.high {
        point.y - rect_box.range_box_y.right.high
    } else {
        0.0
    };

    HYPOT::call(dx, dy)
}

// ===========================================================================
// Working in/out structs (mirrors of the spgist.h ABI structs).
// ===========================================================================

/// Working mirror of `spgConfigOut` (spgist.h) -- the fields the box/polygon
/// opclasses set.
#[derive(Clone, Copy, Debug, Default)]
pub struct SpgConfigOut {
    pub prefixType: u32,
    pub labelType: u32,
    pub leafType: u32,
    pub canReturnData: bool,
    pub longValuesOK: bool,
}

/// A scan key the opclass examines: `sk_strategy`, `sk_subtype`, and the decoded
/// `sk_argument` (a `box` or a `polygon`'s bounding box).
#[derive(Clone, Copy, Debug)]
pub struct SpgScanKey {
    pub sk_strategy: u16,
    pub sk_subtype: u32,
    /// For `BOXOID`: `DatumGetBoxP(sk_argument)`.
    /// For `POLYGONOID`: `&DatumGetPolygonP(sk_argument)->boundbox`.
    pub bbox: BOX,
}

/// Working mirror of `spgChooseIn` (spgist.h) for the box opclass.
#[derive(Clone, Copy, Debug)]
pub struct SpgChooseIn {
    /// `DatumGetBoxP(in->prefixDatum)` -- the centroid.
    pub prefix_box: BOX,
    /// `DatumGetBoxP(in->leafDatum)` -- the box being indexed.
    pub leaf_box: BOX,
    pub allTheSame: bool,
}

/// `spgChooseResultType` (spgist.h): `spgMatchNode` is the only variant this
/// opclass produces.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SpgChooseResultType {
    #[default]
    MatchNode,
    AddNode,
    SplitTuple,
}

/// Working mirror of `spgChooseOut` (spgist.h) for the `spgMatchNode` result the
/// box opclass produces.
#[derive(Clone, Copy, Debug, Default)]
pub struct SpgChooseOut {
    pub resultType: SpgChooseResultType,
    /// `out->result.matchNode.nodeN`.
    pub nodeN: i32,
    /// `out->result.matchNode.levelAdd`.
    pub levelAdd: i32,
    /// `out->result.matchNode.restDatum`, decoded back to a box.
    pub rest_box: BOX,
}

/// Working mirror of `spgPickSplitIn` (spgist.h) for the box opclass.
#[derive(Clone, Debug)]
pub struct SpgPickSplitIn {
    /// `DatumGetBoxP(in->datums[i])` for each of the `in->nTuples` leaf tuples.
    pub boxes: Vec<BOX>,
}

impl SpgPickSplitIn {
    /// `in->nTuples`.
    #[inline]
    pub fn n_tuples(&self) -> i32 {
        self.boxes.len() as i32
    }
}

/// Working mirror of `spgPickSplitOut` (spgist.h) for the box opclass.
#[derive(Clone, Debug, Default)]
pub struct SpgPickSplitOut {
    pub hasPrefix: bool,
    /// `BoxPGetDatum(centroid)` decoded -- the prefix box.
    pub prefix_box: BOX,
    pub nNodes: i32,
    /// `out->nodeLabels` is `NULL` for this opclass.
    pub hasNodeLabels: bool,
    pub mapTuplesToNodes: Vec<i32>,
    /// `out->leafTupleDatums`, decoded back to boxes.
    pub leafTupleDatums: Vec<BOX>,
}

/// Working mirror of `spgInnerConsistentIn` (spgist.h) for the box opclass.
#[derive(Clone, Debug)]
pub struct SpgInnerConsistentIn {
    /// `in->scankeys` (length `in->nkeys`), pre-decoded.
    pub scankeys: Vec<SpgScanKey>,
    /// `DatumGetPointP(orderbys[i].sk_argument)` (length `in->norderbys`).
    pub orderby_points: Vec<Point>,
    pub norderbys: i32,
    /// `(RectBox *) in->traversalValue` (the parent's traversal value), if any.
    pub traversalValue: Option<RectBox>,
    pub allTheSame: bool,
    /// `DatumGetBoxP(in->prefixDatum)` -- the centroid box.  Valid unless
    /// `allTheSame`.
    pub prefix_box: BOX,
    pub nNodes: i32,
}

impl SpgInnerConsistentIn {
    /// `in->nkeys`.
    #[inline]
    pub fn n_keys(&self) -> i32 {
        self.scankeys.len() as i32
    }
}

/// Working mirror of `spgInnerConsistentOut` (spgist.h) for the box opclass.
#[derive(Clone, Debug, Default)]
pub struct SpgInnerConsistentOut {
    pub nNodes: i32,
    pub nodeNumbers: Vec<i32>,
    /// `out->traversalValues` (one [`RectBox`] per visited node).
    pub traversalValues: Vec<RectBox>,
    /// `out->distances` (one distance array per visited node).
    pub distances: Vec<Vec<f64>>,
}

/// Working mirror of `spgLeafConsistentIn` (spgist.h) for the box opclass.
#[derive(Clone, Debug)]
pub struct SpgLeafConsistentIn {
    /// `in->scankeys` (length `in->nkeys`), pre-decoded.
    pub scankeys: Vec<SpgScanKey>,
    /// `DatumGetPointP(orderbys[i].sk_argument)` (length `in->norderbys`).
    pub orderby_points: Vec<Point>,
    pub norderbys: i32,
    /// `in->orderbys[0].sk_func.fn_oid` -- the order-by support function OID
    /// (only consulted when `norderbys > 0`).
    pub orderby0_distfnoid: u32,
    /// `DatumGetBoxP(in->leafDatum)` -- the leaf box.
    pub leaf_box: BOX,
    pub returnData: bool,
}

/// Working mirror of `spgLeafConsistentOut` (spgist.h) for the box opclass.
#[derive(Clone, Debug, Default)]
pub struct SpgLeafConsistentOut {
    /// `out->leafValue`, decoded back to a box (`in->leafDatum`); only set when
    /// `in->returnData`.
    pub leafValue: Option<BOX>,
    pub recheck: bool,
    pub recheckDistances: bool,
    /// `out->distances`, present iff the leaf passed and there were orderbys.
    pub distances: Option<Vec<f64>>,
}

// ===========================================================================
// Support procedures
// ===========================================================================

/// `spg_box_quad_config` (geo_spgist.c:400): SP-GiST config function.
pub fn spg_box_quad_config(cfg: &mut SpgConfigOut) {
    cfg.prefixType = BOXOID;
    cfg.labelType = VOIDOID; // We don't need node labels.
    cfg.canReturnData = true;
    cfg.longValuesOK = false;
}

/// `spg_box_quad_choose` (geo_spgist.c:416): SP-GiST choose function.
pub fn spg_box_quad_choose(in_: &SpgChooseIn, out: &mut SpgChooseOut) {
    let centroid = &in_.prefix_box;
    let b = &in_.leaf_box;

    out.resultType = SpgChooseResultType::MatchNode;
    out.rest_box = *b;

    // nodeN will be set by core, when allTheSame.
    if !in_.allTheSame {
        out.nodeN = getQuadrant(centroid, b) as i32;
    }
}

/// `spg_box_quad_picksplit` (geo_spgist.c:440): SP-GiST pick-split function.
///
/// Splits a list of boxes into 16 quadrants by choosing a central 4-D point as
/// the median of the boxes' coordinates.
pub fn spg_box_quad_picksplit(in_: &SpgPickSplitIn, out: &mut SpgPickSplitOut) {
    let n_tuples = in_.n_tuples() as usize;

    let mut lowXs = vec![0.0f64; n_tuples];
    let mut highXs = vec![0.0f64; n_tuples];
    let mut lowYs = vec![0.0f64; n_tuples];
    let mut highYs = vec![0.0f64; n_tuples];

    // Calculate median of all 4D coordinates.
    for i in 0..n_tuples {
        let b = &in_.boxes[i];

        lowXs[i] = b.low.x;
        highXs[i] = b.high.x;
        lowYs[i] = b.low.y;
        highYs[i] = b.high.y;
    }

    lowXs.sort_by(|a, b| compare_doubles(*a, *b));
    highXs.sort_by(|a, b| compare_doubles(*a, *b));
    lowYs.sort_by(|a, b| compare_doubles(*a, *b));
    highYs.sort_by(|a, b| compare_doubles(*a, *b));

    let median = n_tuples / 2;

    let centroid = BOX {
        high: Point {
            x: highXs[median],
            y: highYs[median],
        },
        low: Point {
            x: lowXs[median],
            y: lowYs[median],
        },
    };

    // Fill the output.
    out.hasPrefix = true;
    out.prefix_box = centroid;

    out.nNodes = 16;
    out.hasNodeLabels = false; // We don't need node labels.

    out.mapTuplesToNodes = vec![0i32; n_tuples];
    out.leafTupleDatums = vec![BOX::default(); n_tuples];

    // Assign ranges to corresponding nodes according to quadrants relative to
    // the "centroid" range.
    for i in 0..n_tuples {
        let b = in_.boxes[i];
        let quadrant = getQuadrant(&centroid, &b);

        out.leafTupleDatums[i] = b;
        out.mapTuplesToNodes[i] = quadrant as i32;
    }
}

/// `is_bounding_box_test_exact` (geo_spgist.c:507): is the bounding-box-based
/// consistent result exact for this strategy?
pub fn is_bounding_box_test_exact(strategy: u16) -> bool {
    matches!(
        strategy,
        RTLeftStrategyNumber
            | RTOverLeftStrategyNumber
            | RTOverRightStrategyNumber
            | RTRightStrategyNumber
            | RTOverBelowStrategyNumber
            | RTBelowStrategyNumber
            | RTAboveStrategyNumber
            | RTOverAboveStrategyNumber
    )
}

/// `spg_box_quad_get_scankey_bbox` (geo_spgist.c:530): the bounding box for a
/// scan key, setting `*recheck` when a polygon's bbox test is inexact.
///
/// The `recheck` argument is optional, exactly as the C `bool *recheck` (passed
/// `NULL` from `inner_consistent`, `&out->recheck` from `leaf_consistent`).
pub fn spg_box_quad_get_scankey_bbox(sk: &SpgScanKey, recheck: Option<&mut bool>) -> PgResult<BOX> {
    match sk.sk_subtype {
        BOXOID => Ok(sk.bbox),
        POLYGONOID => {
            if let Some(r) = recheck {
                if !is_bounding_box_test_exact(sk.sk_strategy) {
                    *r = true;
                }
            }
            Ok(sk.bbox)
        }
        _ => Err(unrecognized_scankey_subtype(sk.sk_subtype as i32)),
    }
}

/// `spg_box_quad_inner_consistent` (geo_spgist.c:552): SP-GiST inner consistent
/// function.
pub fn spg_box_quad_inner_consistent(
    in_: &SpgInnerConsistentIn,
    out: &mut SpgInnerConsistentOut,
) -> PgResult<()> {
    // We are saving the traversal value or initialize it an unbounded one, if we
    // have just begun to walk the tree.
    let rect_box = match in_.traversalValue {
        Some(rb) => rb,
        None => initRectBox(),
    };

    if in_.allTheSame {
        // Report that all nodes should be visited.
        out.nNodes = in_.nNodes;
        out.nodeNumbers = vec![0i32; in_.nNodes as usize];
        for i in 0..in_.nNodes as usize {
            out.nodeNumbers[i] = i as i32;
        }

        if in_.norderbys > 0 && in_.nNodes > 0 {
            let mut distances = vec![0.0f64; in_.norderbys as usize];

            for j in 0..in_.norderbys as usize {
                let pt = &in_.orderby_points[j];
                distances[j] = pointToRectBoxDistance(pt, &rect_box)?;
            }

            out.distances = vec![Vec::new(); in_.nNodes as usize];
            out.distances[0] = distances.clone();

            for i in 1..in_.nNodes as usize {
                // memcpy(out->distances[i], distances, ...)
                out.distances[i] = distances.clone();
            }
        }

        return Ok(());
    }

    // We are casting the prefix and queries to RangeBoxes for ease of the
    // following operations.
    let centroid = getRangeBox(&in_.prefix_box);
    let mut queries: Vec<RangeBox> = Vec::with_capacity(in_.n_keys() as usize);
    for i in 0..in_.n_keys() as usize {
        let b = spg_box_quad_get_scankey_bbox(&in_.scankeys[i], None)?;
        queries.push(getRangeBox(&b));
    }

    // Allocate enough memory for nodes.
    out.nNodes = 0;
    out.nodeNumbers = Vec::with_capacity(in_.nNodes as usize);
    out.traversalValues = Vec::with_capacity(in_.nNodes as usize);
    if in_.norderbys > 0 {
        out.distances = Vec::with_capacity(in_.nNodes as usize);
    }

    // C switches into in->traversalMemoryContext here so that the new traversal
    // values outlive the call; in Rust the owned `RectBox` values placed into
    // `out.traversalValues` already have the right ownership/lifetime.

    // for (quadrant = 0; quadrant < in->nNodes; quadrant++)  -- quadrant is a
    // `uint8`, in->nNodes an `int` (== 16 for this opclass).
    for quadrant_i in 0..in_.nNodes {
        let quadrant = quadrant_i as u8;
        let next_rect_box = nextRectBox(&rect_box, &centroid, quadrant);
        let mut flag = true;

        for i in 0..in_.n_keys() as usize {
            let strategy = in_.scankeys[i].sk_strategy;

            flag = match strategy {
                RTOverlapStrategyNumber => overlap4D(&next_rect_box, &queries[i]),
                RTContainsStrategyNumber => contain4D(&next_rect_box, &queries[i]),
                RTSameStrategyNumber | RTContainedByStrategyNumber => {
                    contained4D(&next_rect_box, &queries[i])
                }
                RTLeftStrategyNumber => left4D(&next_rect_box, &queries[i]),
                RTOverLeftStrategyNumber => overLeft4D(&next_rect_box, &queries[i]),
                RTRightStrategyNumber => right4D(&next_rect_box, &queries[i]),
                RTOverRightStrategyNumber => overRight4D(&next_rect_box, &queries[i]),
                RTAboveStrategyNumber => above4D(&next_rect_box, &queries[i]),
                RTOverAboveStrategyNumber => overAbove4D(&next_rect_box, &queries[i]),
                RTBelowStrategyNumber => below4D(&next_rect_box, &queries[i]),
                RTOverBelowStrategyNumber => overBelow4D(&next_rect_box, &queries[i]),
                other => {
                    return Err(unrecognized_strategy(other as i32, 691));
                }
            };

            // If any check is failed, we have found our answer.
            if !flag {
                break;
            }
        }

        if flag {
            out.traversalValues.push(next_rect_box);
            out.nodeNumbers.push(quadrant as i32);

            if in_.norderbys > 0 {
                let mut distances = vec![0.0f64; in_.norderbys as usize];

                for j in 0..in_.norderbys as usize {
                    let pt = &in_.orderby_points[j];
                    distances[j] = pointToRectBoxDistance(pt, &next_rect_box)?;
                }

                out.distances.push(distances);
            }

            out.nNodes += 1;
        } else {
            // If this node is not selected, we don't need to keep the next
            // traversal value (it is simply dropped here -- C `pfree`s it).
        }
    }

    Ok(())
}

/// `spg_box_quad_leaf_consistent` (geo_spgist.c:740): SP-GiST leaf consistent
/// function.
pub fn spg_box_quad_leaf_consistent(
    in_: &SpgLeafConsistentIn,
    out: &mut SpgLeafConsistentOut,
) -> PgResult<bool> {
    let leaf = in_.leaf_box;
    let mut flag = true;

    // All tests are exact.
    out.recheck = false;

    // Don't return leafValue unless told to; this is used for both box and
    // polygon opclasses, and in the latter case the leaf datum is not even of
    // the right type to return.
    if in_.returnData {
        out.leafValue = Some(leaf);
    }

    // Perform the required comparison(s).
    for i in 0..in_.scankeys.len() {
        let strategy = in_.scankeys[i].sk_strategy;
        let query = spg_box_quad_get_scankey_bbox(&in_.scankeys[i], Some(&mut out.recheck))?;

        flag = match strategy {
            RTOverlapStrategyNumber => box_overlap::call(&leaf, &query),
            RTContainsStrategyNumber => box_contain::call(&leaf, &query),
            RTContainedByStrategyNumber => box_contained::call(&leaf, &query),
            RTSameStrategyNumber => box_same::call(&leaf, &query),
            RTLeftStrategyNumber => box_left::call(&leaf, &query),
            RTOverLeftStrategyNumber => box_overleft::call(&leaf, &query),
            RTRightStrategyNumber => box_right::call(&leaf, &query),
            RTOverRightStrategyNumber => box_overright::call(&leaf, &query),
            RTAboveStrategyNumber => box_above::call(&leaf, &query),
            RTOverAboveStrategyNumber => box_overabove::call(&leaf, &query),
            RTBelowStrategyNumber => box_below::call(&leaf, &query),
            RTOverBelowStrategyNumber => box_overbelow::call(&leaf, &query),
            other => {
                return Err(unrecognized_strategy(other as i32, 831));
            }
        };

        // If any check is failed, we have found our answer.
        if !flag {
            break;
        }
    }

    if flag && in_.norderbys > 0 {
        let distfnoid = in_.orderby0_distfnoid;

        // spg_key_orderbys_distances(leaf, false, in->orderbys, in->norderbys):
        // the box opclass passes isLeaf == false, so the leaf BOX is measured
        // with the box-distance path (`SpgKey::InnerBox`).
        out.distances = Some(spg_key_orderbys_distances::call(
            &SpgKey::InnerBox(in_.leaf_box),
            &in_.orderby_points,
        )?);

        // Recheck is necessary when computing distance to polygon.
        out.recheckDistances = distfnoid == F_DIST_POLYP;
    }

    Ok(flag)
}

/// `spg_bbox_quad_config` (geo_spgist.c:858): SP-GiST config function for 2-D
/// types that are lossily represented by their bounding boxes.
pub fn spg_bbox_quad_config(cfg: &mut SpgConfigOut) {
    cfg.prefixType = BOXOID; // A type represented by its bounding box.
    cfg.labelType = VOIDOID; // We don't need node labels.
    cfg.leafType = BOXOID;
    cfg.canReturnData = false;
    cfg.longValuesOK = false;
}

/// `spg_poly_quad_compress` (geo_spgist.c:875): SP-GiST compress function for
/// polygons -- yields the polygon's bounding box.
///
/// `polygon_boundbox` is `PG_GETARG_POLYGON_P(0)->boundbox` (the `Datum`->polygon
/// decode is the fmgr boundary, done by the caller).
pub fn spg_poly_quad_compress(polygon_boundbox: BOX) -> BOX {
    // box = palloc(sizeof(BOX)); *box = polygon->boundbox;
    polygon_boundbox
}

// ===========================================================================
// Error helpers (matching the C `elog(ERROR, ...)` sites)
// ===========================================================================

/// `elog(ERROR, "unrecognized scankey subtype: %d", sk->sk_subtype)`
/// (geo_spgist.c:544).
fn unrecognized_scankey_subtype(subtype: i32) -> PgError {
    PgError::error(format!("unrecognized scankey subtype: {subtype}"))
        .with_sqlstate(ERRCODE_INTERNAL_ERROR)
        .with_error_location(ErrorLocation::new(
            C_FILE,
            544,
            "spg_box_quad_get_scankey_bbox",
        ))
}

/// `elog(ERROR, "unrecognized strategy: %d", strategy)` (geo_spgist.c:691 in
/// `spg_box_quad_inner_consistent`, geo_spgist.c:831 in
/// `spg_box_quad_leaf_consistent`).
fn unrecognized_strategy(strategy: i32, line: i32) -> PgError {
    let func = if line == 691 {
        "spg_box_quad_inner_consistent"
    } else {
        "spg_box_quad_leaf_consistent"
    };
    PgError::error(format!("unrecognized strategy: {strategy}"))
        .with_sqlstate(ERRCODE_INTERNAL_ERROR)
        .with_error_location(ErrorLocation::new(C_FILE, line, func))
}
