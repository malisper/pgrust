//! Port of `src/backend/access/spgist/spgquadtreeproc.c` (PostgreSQL 18.3) --
//! the SP-GiST quad-tree support procedures over `point`.
//!
//! A quad tree splits at a centroid `point` (the inner-node prefix) into four
//! quadrants; an inner node has exactly four child nodes (no node labels).
//!
//! Every function in the C file is ported here 1:1 -- identical control flow,
//! branch order, bit masks, switch arms, message text and SQLSTATE:
//!
//!   * [`spg_quad_config`]            (config, `spgquadtreeproc.c:26`)
//!   * [`getQuadrant`]                (static, `spgquadtreeproc.c:54`)
//!   * [`getQuadrantArea`]            (static, `spgquadtreeproc.c:82`)
//!   * [`spg_quad_choose`]            (choose, `spgquadtreeproc.c:114`)
//!   * [`spg_quad_picksplit`]         (picksplit, `spgquadtreeproc.c:168`)
//!   * [`spg_quad_inner_consistent`]  (inner consistent, `spgquadtreeproc.c:226`)
//!   * [`spg_quad_leaf_consistent`]   (leaf consistent, `spgquadtreeproc.c:406`)
//!
//! `spg_quad_leaf_consistent` is also the registered `leaf_consistent` support
//! function for the k-d-tree opclass (`spgkdtreeproc.c` defers to it).
//!
//! ## Idiomatic working structs / seams
//!
//! As in the sibling `geo_spgist.c` port, the fmgr entry points take owned
//! working structs that mirror the C `spg*In`/`spg*Out` field set; `palloc`'d
//! output arrays become owned `Vec`s; `Datum` payloads are pre-decoded to
//! [`types_core::geo`] forms.  The `void *traversalValue` round-trips a `BOX`.
//!
//! The geometric point predicates (`point_left`/`right`/`above`/`below`/
//! `horiz`/`vert`/`eq`, `box_contain_pt`, all owned by the unported `geo_ops.c`)
//! are routed through `backend-utils-adt-geo-ops-seams`.  The one cross-file
//! call `spg_key_orderbys_distances` (`spgproc.c`, owner
//! `backend-access-spg-proc`) is routed through
//! `backend-access-spg-proc-seams::spg_key_orderbys_distances`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(clippy::result_large_err)]

use types_core::geo::{Point, SpgKey, BOX};
use types_error::{ErrorLocation, PgError, PgResult, ERRCODE_INTERNAL_ERROR};

use backend_access_spg_proc_seams::spg_key_orderbys_distances;
use backend_utils_adt_geo_ops_seams::{
    box_contain_pt, point_above, point_below, point_eq, point_horiz, point_left, point_right,
    point_vert,
};

const C_FILE: &str = "../src/backend/access/spgist/spgquadtreeproc.c";

/// `get_float8_infinity()` (float.c) -- `f64::INFINITY`.
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

/// `POINTOID` (pg_type.h).
pub const POINTOID: u32 = 600;
/// `VOIDOID` (pg_type.h).
pub const VOIDOID: u32 = 2278;

// ===========================================================================
// Working in/out structs (idiomatic mirrors of the spgist.h ABI structs).
// ===========================================================================

/// Working mirror of `spgConfigOut` (spgist.h).
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
    /// Decoded `sk_argument` as a `point`.
    pub query_point: Point,
    /// Decoded `sk_argument` as a `box`, for the `RTContainedBy` strategy.
    pub query_box: BOX,
}

/// Working mirror of `spgChooseIn` (spgist.h).
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
    /// `DatumGetPointP(in->prefixDatum)`; valid iff `hasPrefix`.
    pub prefix_point: Point,
    /// `in->nNodes`.
    pub nNodes: i32,
}

/// Working mirror of `spgChooseOut` (spgist.h) for the `spgMatchNode` result.
#[derive(Clone, Debug, Default)]
pub struct SpgChooseOut {
    /// `out->result.matchNode.nodeN`.
    pub nodeN: i32,
    /// `out->result.matchNode.levelAdd`.
    pub levelAdd: i32,
    /// `out->result.matchNode.restDatum`, decoded back to a point.
    pub rest_point: Point,
}

/// Working mirror of `spgPickSplitIn` (spgist.h).
#[derive(Clone, Debug)]
pub struct SpgPickSplitIn {
    /// `DatumGetPointP(in->datums[i])` for each leaf tuple.
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

/// Working mirror of `spgPickSplitOut` (spgist.h).
#[derive(Clone, Debug, Default)]
pub struct SpgPickSplitOut {
    /// `out->hasPrefix`.
    pub hasPrefix: bool,
    /// `PointPGetDatum(centroid)` decoded.
    pub prefix_point: Point,
    /// `out->nNodes`.
    pub nNodes: i32,
    /// `out->mapTuplesToNodes`.
    pub mapTuplesToNodes: Vec<i32>,
    /// `out->leafTupleDatums`, decoded back to points.
    pub leafTupleDatums: Vec<Point>,
}

/// Working mirror of `spgInnerConsistentIn` (spgist.h).
#[derive(Clone, Debug)]
pub struct SpgInnerConsistentIn {
    /// `in->scankeys` (length `in->nkeys`), pre-decoded.
    pub scankeys: Vec<SpgScanKey>,
    /// `DatumGetPointP(orderbys[i].sk_argument)` (length `in->norderbys`).
    pub orderby_points: Vec<Point>,
    /// `in->norderbys`.
    pub norderbys: i32,
    /// `(BOX *) in->traversalValue`, if any.
    pub traversalValue: Option<BOX>,
    /// `in->level`.
    pub level: i32,
    /// `in->allTheSame`.
    pub allTheSame: bool,
    /// `in->hasPrefix`.
    pub hasPrefix: bool,
    /// `DatumGetPointP(in->prefixDatum)`.
    pub prefix_point: Point,
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

/// Working mirror of `spgInnerConsistentOut` (spgist.h).
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

/// Working mirror of `spgLeafConsistentIn` (spgist.h).
#[derive(Clone, Debug)]
pub struct SpgLeafConsistentIn {
    /// `in->scankeys` (length `in->nkeys`), pre-decoded.
    pub scankeys: Vec<SpgScanKey>,
    /// `DatumGetPointP(orderbys[i].sk_argument)` (length `in->norderbys`).
    pub orderby_points: Vec<Point>,
    /// `in->norderbys`.
    pub norderbys: i32,
    /// `DatumGetPointP(in->leafDatum)`.
    pub leaf_point: Point,
}

/// Working mirror of `spgLeafConsistentOut` (spgist.h).
#[derive(Clone, Debug, Default)]
pub struct SpgLeafConsistentOut {
    /// `out->leafValue`, decoded back to a point (`in->leafDatum`).
    pub leaf_point: Point,
    /// `out->recheck`.
    pub recheck: bool,
    /// `out->distances`, present iff the leaf passed and there were orderbys.
    pub distances: Option<Vec<f64>>,
}

// ===========================================================================
// Quad-tree (spgquadtreeproc.c)
// ===========================================================================

/// `spg_quad_config` (spgquadtreeproc.c:26): fill the config for a quad tree.
pub fn spg_quad_config(cfg: &mut SpgConfigOut) {
    // spgConfigIn *cfgin = (spgConfigIn *) PG_GETARG_POINTER(0);  -- unused
    cfg.prefixType = POINTOID;
    cfg.labelType = VOIDOID; // we don't need node labels
    cfg.canReturnData = true;
    cfg.longValuesOK = false;
}

/// `getQuadrant` (spgquadtreeproc.c:54): which quadrant (1..4) `tst` falls in
/// relative to `centroid`.  `SPTEST(f, x, y)` is `f(x, y)`, with `x = tst`,
/// `y = centroid`.
pub fn getQuadrant(centroid: &Point, tst: &Point) -> PgResult<i16> {
    if (point_above::call(tst, centroid) || point_horiz::call(tst, centroid))
        && (point_right::call(tst, centroid) || point_vert::call(tst, centroid))
    {
        return Ok(1);
    }

    if point_below::call(tst, centroid)
        && (point_right::call(tst, centroid) || point_vert::call(tst, centroid))
    {
        return Ok(2);
    }

    if (point_below::call(tst, centroid) || point_horiz::call(tst, centroid))
        && point_left::call(tst, centroid)
    {
        return Ok(3);
    }

    if point_above::call(tst, centroid) && point_left::call(tst, centroid) {
        return Ok(4);
    }

    Err(impossible_quadrant())
}

/// `getQuadrantArea` (spgquadtreeproc.c:82): bounding box of `quadrant` inside
/// `bbox`, split at `centroid`.
pub fn getQuadrantArea(bbox: &BOX, centroid: &Point, quadrant: i32) -> BOX {
    // C palloc's an uninitialized BOX and fills the relevant arm; the default
    // (all-zero) BOX matches the unwritten bytes for the cases the switch covers
    // (1..4), which are the only quadrants getQuadrant ever returns.
    let mut result = BOX::default();
    match quadrant {
        1 => {
            result.high = bbox.high;
            result.low = *centroid;
        }
        2 => {
            result.high.x = bbox.high.x;
            result.high.y = centroid.y;
            result.low.x = centroid.x;
            result.low.y = bbox.low.y;
        }
        3 => {
            result.high = *centroid;
            result.low = bbox.low;
        }
        4 => {
            result.high.x = centroid.x;
            result.high.y = bbox.high.y;
            result.low.x = bbox.low.x;
            result.low.y = centroid.y;
        }
        _ => {}
    }
    result
}

/// `spg_quad_choose` (spgquadtreeproc.c:114).
pub fn spg_quad_choose(in_: &SpgChooseIn, out: &mut SpgChooseOut) -> PgResult<()> {
    let inPoint = &in_.in_point;

    if in_.allTheSame {
        // out->resultType = spgMatchNode;  (the only result type this opclass
        // produces -- the working SpgChooseOut models exactly that)
        out.levelAdd = 0;
        out.rest_point = *inPoint;
        return Ok(());
    }

    debug_assert!(in_.hasPrefix);
    let centroid = &in_.prefix_point;

    debug_assert!(in_.nNodes == 4);

    out.nodeN = getQuadrant(centroid, inPoint)? as i32 - 1;
    out.levelAdd = 0;
    out.rest_point = *inPoint;

    Ok(())
}

/// `spg_quad_picksplit` (spgquadtreeproc.c:168): the default (non-`USE_MEDIAN`)
/// variant -- use the average of x and y as the centroid.
pub fn spg_quad_picksplit(in_: &SpgPickSplitIn, out: &mut SpgPickSplitOut) -> PgResult<()> {
    let n_tuples = in_.n_tuples();

    // Use the average values of x and y as the centroid point.
    // centroid = palloc0(...) -> starts at (0, 0).
    let mut centroid = Point { x: 0.0, y: 0.0 };

    for i in 0..n_tuples as usize {
        centroid.x += in_.points[i].x;
        centroid.y += in_.points[i].y;
    }

    centroid.x /= n_tuples as f64;
    centroid.y /= n_tuples as f64;

    out.hasPrefix = true;
    out.prefix_point = centroid;

    out.nNodes = 4;
    // nodeLabels = NULL -- we don't need node labels.

    out.mapTuplesToNodes = vec![0; n_tuples as usize];
    out.leafTupleDatums = vec![Point::default(); n_tuples as usize];

    for i in 0..n_tuples as usize {
        let p = &in_.points[i];
        let quadrant = getQuadrant(&centroid, p)? as i32 - 1;

        out.leafTupleDatums[i] = *p;
        out.mapTuplesToNodes[i] = quadrant;
    }

    Ok(())
}

/// `spg_quad_inner_consistent` (spgquadtreeproc.c:226).
pub fn spg_quad_inner_consistent(
    in_: &SpgInnerConsistentIn,
    out: &mut SpgInnerConsistentOut,
) -> PgResult<()> {
    debug_assert!(in_.hasPrefix);
    let centroid = &in_.prefix_point;

    // When ordering scan keys are specified, calculate distance for them. To do
    // that we calculate bounding boxes for all children nodes; those depend on
    // the parent's bounding box, so they are saved as traversalValues.
    //
    // C `bbox` points at `infbbox` (level 0) or `in->traversalValue` otherwise;
    // it is read only when `in->norderbys > 0`.
    let bbox: BOX = if in_.norderbys > 0 {
        if in_.level == 0 {
            let inf = get_float8_infinity();
            BOX {
                high: Point { x: inf, y: inf },
                low: Point { x: -inf, y: -inf },
            }
        } else {
            in_.traversalValue.ok_or_else(|| {
                PgError::error(
                    "spg_quad_inner_consistent: traversalValue must be set at non-zero level",
                )
            })?
        }
    } else {
        BOX::default()
    };

    if in_.allTheSame {
        // Report that all nodes should be visited.
        out.nNodes = in_.nNodes;
        out.nodeNumbers = vec![0; in_.nNodes as usize];
        if in_.norderbys > 0 {
            out.traversalValues = Vec::with_capacity(in_.nNodes as usize);
            out.distances = Vec::with_capacity(in_.nNodes as usize);
        }
        for i in 0..in_.nNodes as usize {
            out.nodeNumbers[i] = i as i32;

            if in_.norderbys > 0 {
                // Use parent quadrant box as traversalValue (box_copy in C is a
                // clone here; the MemoryContextSwitchTo is the allocator seam).
                let quadrant = bbox;
                out.distances.push(spg_key_orderbys_distances::call(
                    &SpgKey::InnerBox(quadrant),
                    &in_.orderby_points,
                )?);
                out.traversalValues.push(quadrant);
            }
        }
        return Ok(());
    }

    debug_assert!(in_.nNodes == 4);

    // "which" is a bitmask of quadrants that satisfy all constraints.
    let mut which: i32 = (1 << 1) | (1 << 2) | (1 << 3) | (1 << 4);

    for i in 0..in_.n_keys() as usize {
        let key = &in_.scankeys[i];
        let query = &key.query_point;

        match key.sk_strategy {
            RTLeftStrategyNumber => {
                if point_right::call(centroid, query) {
                    which &= (1 << 3) | (1 << 4);
                }
            }
            RTRightStrategyNumber => {
                if point_left::call(centroid, query) {
                    which &= (1 << 1) | (1 << 2);
                }
            }
            RTSameStrategyNumber => {
                which &= 1 << getQuadrant(centroid, query)?;
            }
            RTBelowStrategyNumber | RTOldBelowStrategyNumber => {
                if point_above::call(centroid, query) {
                    which &= (1 << 2) | (1 << 3);
                }
            }
            RTAboveStrategyNumber | RTOldAboveStrategyNumber => {
                if point_below::call(centroid, query) {
                    which &= (1 << 1) | (1 << 4);
                }
            }
            RTContainedByStrategyNumber => {
                // For this operator, the query is a box not a point. We cheat to
                // the extent of assuming that DatumGetPointP won't do anything
                // that would be bad for a pointer-to-box.
                let boxQuery = &key.query_box;

                if box_contain_pt::call(boxQuery, centroid) {
                    // centroid is in box, so all quadrants are OK
                } else {
                    // identify quadrant(s) containing all corners of box
                    let mut r = 0;

                    let mut p = boxQuery.low;
                    r |= 1 << getQuadrant(centroid, &p)?;
                    p.y = boxQuery.high.y;
                    r |= 1 << getQuadrant(centroid, &p)?;
                    p = boxQuery.high;
                    r |= 1 << getQuadrant(centroid, &p)?;
                    p.x = boxQuery.low.x;
                    r |= 1 << getQuadrant(centroid, &p)?;

                    which &= r;
                }
            }
            other => {
                return Err(unrecognized_strategy(
                    other as i32,
                    "spg_quad_inner_consistent",
                    363,
                ));
            }
        }

        if which == 0 {
            break; // no need to consider remaining conditions
        }
    }

    out.levelAdds = vec![1; 4];

    // We must descend into the quadrant(s) identified by which.
    out.nodeNumbers = vec![0; 4];
    out.nNodes = 0;
    if in_.norderbys > 0 {
        out.traversalValues = Vec::with_capacity(4);
        out.distances = Vec::with_capacity(4);
    }

    for i in 1..=4 {
        if which & (1 << i) != 0 {
            out.nodeNumbers[out.nNodes as usize] = i - 1;

            if in_.norderbys > 0 {
                let quadrant = getQuadrantArea(&bbox, centroid, i);
                out.distances.push(spg_key_orderbys_distances::call(
                    &SpgKey::InnerBox(quadrant),
                    &in_.orderby_points,
                )?);
                out.traversalValues.push(quadrant);
            }

            out.nNodes += 1;
        }
    }

    Ok(())
}

/// `spg_quad_leaf_consistent` (spgquadtreeproc.c:406).
pub fn spg_quad_leaf_consistent(
    in_: &SpgLeafConsistentIn,
    out: &mut SpgLeafConsistentOut,
) -> PgResult<bool> {
    let datum = &in_.leaf_point;

    // all tests are exact
    out.recheck = false;

    // leafDatum is what it is...
    out.leaf_point = in_.leaf_point;

    // Perform the required comparison(s).
    let mut res = true;
    for i in 0..in_.scankeys.len() {
        let key = &in_.scankeys[i];
        let query = &key.query_point;

        res = match key.sk_strategy {
            RTLeftStrategyNumber => point_left::call(datum, query),
            RTRightStrategyNumber => point_right::call(datum, query),
            RTSameStrategyNumber => point_eq::call(datum, query),
            RTBelowStrategyNumber | RTOldBelowStrategyNumber => point_below::call(datum, query),
            RTAboveStrategyNumber | RTOldAboveStrategyNumber => point_above::call(datum, query),
            RTContainedByStrategyNumber => {
                // For this operator, the query is a box not a point.
                box_contain_pt::call(&key.query_box, datum)
            }
            other => {
                return Err(unrecognized_strategy(
                    other as i32,
                    "spg_quad_leaf_consistent",
                    457,
                ));
            }
        };

        if !res {
            break;
        }
    }

    if res && in_.norderbys > 0 {
        // ok, it passes -> let's compute the distances
        out.distances = Some(spg_key_orderbys_distances::call(
            &SpgKey::LeafPoint(in_.leaf_point),
            &in_.orderby_points,
        )?);
    }

    Ok(res)
}

// ===========================================================================
// Error reporters
// ===========================================================================

/// `elog(ERROR, "getQuadrant: impossible case")` (spgquadtreeproc.c:77).
fn impossible_quadrant() -> PgError {
    PgError::error("getQuadrant: impossible case")
        .with_sqlstate(ERRCODE_INTERNAL_ERROR)
        .with_error_location(ErrorLocation::new(C_FILE, 77, "getQuadrant"))
}

/// `elog(ERROR, "unrecognized strategy number: %d", strategy)`
/// (spgquadtreeproc.c:362 / spgquadtreeproc.c:456).
fn unrecognized_strategy(strategy: i32, func: &'static str, line: i32) -> PgError {
    PgError::error(format!("unrecognized strategy number: {strategy}"))
        .with_sqlstate(ERRCODE_INTERNAL_ERROR)
        .with_error_location(ErrorLocation::new(C_FILE, line, func))
}
