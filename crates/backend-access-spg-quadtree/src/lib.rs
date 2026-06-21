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

// ===========================================================================
// Typed support-proc dispatch (mirrors BRIN's opclass-by-OID dispatch)
//
// The SP-GiST core resolves the opclass support procedure OID via
// `index_getprocinfo(rel, 1, SPGIST_*_PROC).fn_oid` and calls the typed seam
// `spg_*::call(proc_oid, &in, &mut out)`. We install our arm keyed on the
// quad-tree opclass support-proc OIDs (pg_proc.dat). The seam crosses the
// `types_spgist::spg*In/Out` structs whose `Datum` fields carry the `point`/
// `box` images; these dispatch wrappers decode them to the `types_core::geo`
// working structs (the honest `DatumGetPointP` / `DatumGetBoxP`), run the
// unchanged opclass body, and re-encode the output Datums (`PointPGetDatum`).
// This is exactly the geo_spgist.c idiom (decoded working structs internally).
// ===========================================================================

use mcx::Mcx;
use types_core::primitive::Oid;
use types_spgist as spgt;
use types_tuple::backend_access_common_heaptuple::Datum;

/// `F_SPG_QUAD_CONFIG` — `spg_quad_config` (pg_proc.dat oid 4018).
pub const F_SPG_QUAD_CONFIG: Oid = 4018;
/// `F_SPG_QUAD_CHOOSE` — `spg_quad_choose` (pg_proc.dat oid 4019).
pub const F_SPG_QUAD_CHOOSE: Oid = 4019;
/// `F_SPG_QUAD_PICKSPLIT` — `spg_quad_picksplit` (pg_proc.dat oid 4020).
pub const F_SPG_QUAD_PICKSPLIT: Oid = 4020;
/// `F_SPG_QUAD_INNER_CONSISTENT` — `spg_quad_inner_consistent` (pg_proc.dat oid 4021).
pub const F_SPG_QUAD_INNER_CONSISTENT: Oid = 4021;
/// `F_SPG_QUAD_LEAF_CONSISTENT` — `spg_quad_leaf_consistent` (pg_proc.dat oid 4022).
/// Also the registered `leaf_consistent` for the k-d-tree opclass.
pub const F_SPG_QUAD_LEAF_CONSISTENT: Oid = 4022;

/// `DatumGetPointP(datum)` — decode a `point`'s by-reference image.
#[inline]
fn datum_get_point(datum: &Datum<'_>) -> Point {
    Point::from_datum_bytes(datum.as_ref_bytes())
}

/// `DatumGetBoxP(datum)` — decode a `box`'s by-reference image.
#[inline]
fn datum_get_box(datum: &Datum<'_>) -> BOX {
    BOX::from_datum_bytes(datum.as_ref_bytes())
}

/// `PointPGetDatum(p)` — encode a point as a by-reference `Datum` in `mcx`.
#[inline]
fn point_get_datum<'mcx>(mcx: Mcx<'mcx>, p: &Point) -> PgResult<Datum<'mcx>> {
    Ok(Datum::ByRef(mcx::slice_in(mcx, &p.to_datum_bytes())?))
}

/// Decode a typed scankey array into the opclass' `SpgScanKey` working form.
/// For `RTContainedBy` the `sk_argument` is a `box`, else a `point`.
fn decode_scankeys(scankeys: &[types_scan::scankey::ScanKeyData<'_>]) -> Vec<SpgScanKey> {
    scankeys
        .iter()
        .map(|sk| {
            let sk_strategy = sk.sk_strategy as u16;
            if sk_strategy == RTContainedByStrategyNumber {
                SpgScanKey {
                    sk_strategy,
                    query_point: Point::default(),
                    query_box: datum_get_box(&sk.sk_argument),
                }
            } else {
                SpgScanKey {
                    sk_strategy,
                    query_point: datum_get_point(&sk.sk_argument),
                    query_box: BOX::default(),
                }
            }
        })
        .collect()
}

/// `DatumGetPointP(orderbys[i].sk_argument)` for each ordering scan key.
fn decode_orderby_points(orderbys: &[types_scan::scankey::ScanKeyData<'_>]) -> Vec<Point> {
    orderbys.iter().map(|sk| datum_get_point(&sk.sk_argument)).collect()
}

// ---------------------------------------------------------------------------
// Per-OID typed dispatch arms. Mirrors BRIN's single-crate opclass dispatcher
// (brin-minmax matches F_BRIN_MINMAX_* / F_BRIN_INCLUSION_* / ... in one
// crate): this crate is the SINGLE installer of the SP-GiST core dispatch
// seams and routes BOTH the quad-tree opclass (its own bodies) AND the
// k-d-tree opclass (the sibling `backend-access-spg-kdtree` crate's bodies) by
// support-proc OID — exactly as the seam's single-shot `set` requires. The
// text opclass (F5) will fold its OIDs into this dispatcher when it lands.
// ---------------------------------------------------------------------------

use backend_access_spg_kdtree as kd;
use backend_access_spg_text as text;
use backend_utils_adt_geo_spgist_only as boxq;
use backend_utils_adt_network_spgist as inet;
use backend_utils_adt_rangetypes_spgist as range;

// ---------------------------------------------------------------------------
// `spg_box_quad_*` support-procedure OIDs (pg_proc.dat 5012-5016). The box
// opclass bodies live in `backend-utils-adt-geo-spgist-only` (which models the
// plain working in/out structs, not the typed `spgt::` carriers), so the arms
// below follow the quad-tree's own-body pattern: decode the typed Datums into
// the box working structs, call the body, then re-encode the result into the
// typed `spgt::` out. (Contrast the `range::`/`text::` arms, whose bodies take
// the typed carriers directly.)
// ---------------------------------------------------------------------------

/// `F_SPG_BBOX_QUAD_CONFIG` — `spg_bbox_quad_config` (pg_proc.dat oid 5010):
/// the generic bounding-box quad-tree config shared by the `spgist/poly_ops`
/// opclass (polygons are lossily indexed by their bounding box).
pub const F_SPG_BBOX_QUAD_CONFIG: Oid = 5010;
/// `F_SPG_BOX_QUAD_CONFIG` — `spg_box_quad_config` (pg_proc.dat oid 5012).
pub const F_SPG_BOX_QUAD_CONFIG: Oid = 5012;
/// `F_SPG_BOX_QUAD_CHOOSE` — `spg_box_quad_choose` (pg_proc.dat oid 5013).
pub const F_SPG_BOX_QUAD_CHOOSE: Oid = 5013;
/// `F_SPG_BOX_QUAD_PICKSPLIT` — `spg_box_quad_picksplit` (pg_proc.dat oid 5014).
pub const F_SPG_BOX_QUAD_PICKSPLIT: Oid = 5014;
/// `F_SPG_BOX_QUAD_INNER_CONSISTENT` — `spg_box_quad_inner_consistent` (oid 5015).
pub const F_SPG_BOX_QUAD_INNER_CONSISTENT: Oid = 5015;
/// `F_SPG_BOX_QUAD_LEAF_CONSISTENT` — `spg_box_quad_leaf_consistent` (oid 5016).
pub const F_SPG_BOX_QUAD_LEAF_CONSISTENT: Oid = 5016;

/// `BoxPGetDatum(b)` — encode a `box` into a by-reference index-context `Datum`.
fn box_get_datum<'mcx>(mcx: Mcx<'mcx>, b: &BOX) -> PgResult<Datum<'mcx>> {
    Ok(Datum::ByRef(mcx::slice_in(mcx, &b.to_datum_bytes())?))
}

/// Decode a typed scankey array into the box opclass' [`boxq::SpgScanKey`]
/// working form: `sk_subtype` distinguishes a `box` argument
/// (`DatumGetBoxP`) from a `polygon` argument (`DatumGetPolygonP(..)->boundbox`).
fn decode_box_scankeys(
    scankeys: &[types_scan::scankey::ScanKeyData<'_>],
) -> Vec<boxq::SpgScanKey> {
    scankeys
        .iter()
        .map(|sk| {
            let sk_subtype = sk.sk_subtype;
            let bbox = if sk_subtype == boxq::POLYGONOID {
                backend_utils_adt_geo_ops_seams::poly_query_boundbox::call(
                    sk.sk_argument.as_ref_bytes(),
                )
            } else {
                datum_get_box(&sk.sk_argument)
            };
            boxq::SpgScanKey { sk_strategy: sk.sk_strategy as u16, sk_subtype, bbox }
        })
        .collect()
}

/// `unrecognized SP-GiST support function OID` — a dispatch to an OID no
/// installed opclass owns (mirror-PG-and-panic: the SP-GiST core only ever
/// resolves OIDs that belong to a registered opclass support procedure).
fn unrecognized_proc(proc_oid: Oid, method: &'static str) -> PgError {
    PgError::error(format!(
        "unrecognized SP-GiST {method} support function OID: {proc_oid}"
    ))
    .with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

/// `spg_config` dispatcher: quad-tree + k-d-tree config bodies.
fn dispatch_config(
    proc_oid: Oid,
    _in: &spgt::spgConfigIn,
    out: &mut spgt::spgConfigOut,
) -> PgResult<()> {
    let cfg = match proc_oid {
        F_SPG_QUAD_CONFIG => {
            let mut cfg = SpgConfigOut::default();
            spg_quad_config(&mut cfg);
            (cfg.prefixType, cfg.labelType, cfg.leafType, cfg.canReturnData, cfg.longValuesOK)
        }
        kd::F_SPG_KD_CONFIG => {
            let mut cfg = kd::SpgConfigOut::default();
            kd::spg_kd_config(&mut cfg);
            (cfg.prefixType, cfg.labelType, cfg.leafType, cfg.canReturnData, cfg.longValuesOK)
        }
        text::F_SPG_TEXT_CONFIG => {
            // The text opclass writes its config straight into the typed out
            // (TEXTOID prefix / INT2OID label / no explicit leaf type).
            text::spg_text_config(_in, out);
            return Ok(());
        }
        inet::F_INET_SPG_CONFIG => {
            // The inet opclass writes its config straight into the typed out
            // (CIDROID prefix / VOIDOID label / no explicit leaf type).
            inet::inet_spg_config(_in, out);
            return Ok(());
        }
        range::F_SPG_RANGE_QUAD_CONFIG => {
            // The range opclass writes its config straight into the typed out
            // (ANYRANGEOID prefix / VOIDOID label / no explicit leaf type).
            range::spg_range_quad_config(_in, out);
            return Ok(());
        }
        F_SPG_BOX_QUAD_CONFIG => {
            let mut cfg = boxq::SpgConfigOut::default();
            boxq::spg_box_quad_config(&mut cfg);
            (cfg.prefixType, cfg.labelType, cfg.leafType, cfg.canReturnData, cfg.longValuesOK)
        }
        F_SPG_BBOX_QUAD_CONFIG => {
            // `spgist/poly_ops` config: a 2-D type lossily represented by its
            // bounding box (geo_spgist.c spg_bbox_quad_config).
            let mut cfg = boxq::SpgConfigOut::default();
            boxq::spg_bbox_quad_config(&mut cfg);
            (cfg.prefixType, cfg.labelType, cfg.leafType, cfg.canReturnData, cfg.longValuesOK)
        }
        _ => return Err(unrecognized_proc(proc_oid, "config")),
    };
    out.prefixType = cfg.0;
    out.labelType = cfg.1;
    out.leafType = cfg.2;
    out.canReturnData = cfg.3;
    out.longValuesOK = cfg.4;
    Ok(())
}

/// `spg_choose` dispatcher.
fn dispatch_choose<'mcx>(
    mcx: Mcx<'mcx>,
    proc_oid: Oid,
    in_: &spgt::spgChooseIn<'mcx>,
    out: &mut spgt::spgChooseOut<'mcx>,
) -> PgResult<()> {
    match proc_oid {
        F_SPG_QUAD_CHOOSE => {
            let local_in = SpgChooseIn {
                in_point: datum_get_point(&in_.datum),
                level: in_.level,
                allTheSame: in_.allTheSame,
                hasPrefix: in_.hasPrefix,
                prefix_point: if in_.hasPrefix {
                    datum_get_point(&in_.prefixDatum)
                } else {
                    Point::default()
                },
                nNodes: in_.nNodes,
            };
            let mut local_out = SpgChooseOut::default();
            spg_quad_choose(&local_in, &mut local_out)?;
            out.result = spgt::spgChooseOutResult::MatchNode(spgt::spgChooseOutMatchNode {
                nodeN: local_out.nodeN,
                levelAdd: local_out.levelAdd,
                restDatum: point_get_datum(mcx, &local_out.rest_point)?,
            });
            Ok(())
        }
        kd::F_SPG_KD_CHOOSE => {
            let local_in = kd::SpgChooseIn {
                in_point: datum_get_point(&in_.datum),
                level: in_.level,
                allTheSame: in_.allTheSame,
                hasPrefix: in_.hasPrefix,
                // k-d-tree prefix is a float8 splitting coordinate, not a point.
                prefix_coord: if in_.hasPrefix {
                    in_.prefixDatum.as_f64()
                } else {
                    0.0
                },
                nNodes: in_.nNodes,
            };
            let mut local_out = kd::SpgChooseOut::default();
            kd::spg_kd_choose(&local_in, &mut local_out)?;
            out.result = spgt::spgChooseOutResult::MatchNode(spgt::spgChooseOutMatchNode {
                nodeN: local_out.nodeN,
                levelAdd: local_out.levelAdd,
                restDatum: point_get_datum(mcx, &local_out.rest_point)?,
            });
            Ok(())
        }
        F_SPG_BOX_QUAD_CHOOSE => {
            let local_in = boxq::SpgChooseIn {
                prefix_box: if in_.hasPrefix {
                    datum_get_box(&in_.prefixDatum)
                } else {
                    BOX::default()
                },
                leaf_box: datum_get_box(&in_.leafDatum),
                allTheSame: in_.allTheSame,
            };
            let mut local_out = boxq::SpgChooseOut::default();
            boxq::spg_box_quad_choose(&local_in, &mut local_out);
            out.result = spgt::spgChooseOutResult::MatchNode(spgt::spgChooseOutMatchNode {
                nodeN: local_out.nodeN,
                levelAdd: local_out.levelAdd,
                restDatum: box_get_datum(mcx, &local_out.rest_box)?,
            });
            Ok(())
        }
        text::F_SPG_TEXT_CHOOSE => text::spg_text_choose(mcx, in_, out),
        inet::F_INET_SPG_CHOOSE => inet::inet_spg_choose(mcx, in_, out),
        range::F_SPG_RANGE_QUAD_CHOOSE => range::spg_range_quad_choose(mcx, in_, out),
        _ => Err(unrecognized_proc(proc_oid, "choose")),
    }
}

/// `spg_picksplit` dispatcher.
fn dispatch_picksplit<'mcx>(
    mcx: Mcx<'mcx>,
    proc_oid: Oid,
    in_: &spgt::spgPickSplitIn<'mcx>,
    out: &mut spgt::spgPickSplitOut<'mcx>,
) -> PgResult<()> {
    match proc_oid {
        F_SPG_QUAD_PICKSPLIT => {
            let local_in = SpgPickSplitIn {
                points: in_.datums.iter().map(datum_get_point).collect(),
                level: in_.level,
            };
            let mut local_out = SpgPickSplitOut::default();
            spg_quad_picksplit(&local_in, &mut local_out)?;
            out.hasPrefix = local_out.hasPrefix;
            out.prefixDatum = if local_out.hasPrefix {
                Some(point_get_datum(mcx, &local_out.prefix_point)?)
            } else {
                None
            };
            out.nNodes = local_out.nNodes;
            out.nodeLabels = None;
            out.mapTuplesToNodes = local_out.mapTuplesToNodes;
            out.leafTupleDatums = local_out
                .leafTupleDatums
                .iter()
                .map(|p| point_get_datum(mcx, p))
                .collect::<PgResult<Vec<_>>>()?;
            Ok(())
        }
        kd::F_SPG_KD_PICKSPLIT => {
            let local_in = kd::SpgPickSplitIn {
                points: in_.datums.iter().map(datum_get_point).collect(),
                level: in_.level,
            };
            let mut local_out = kd::SpgPickSplitOut::default();
            kd::spg_kd_picksplit(&local_in, &mut local_out);
            out.hasPrefix = local_out.hasPrefix;
            // k-d-tree prefix is the float8 splitting coordinate.
            out.prefixDatum = if local_out.hasPrefix {
                Some(Datum::from_f64(local_out.prefix_coord))
            } else {
                None
            };
            out.nNodes = local_out.nNodes;
            out.nodeLabels = None;
            out.mapTuplesToNodes = local_out.mapTuplesToNodes;
            out.leafTupleDatums = local_out
                .leafTupleDatums
                .iter()
                .map(|p| point_get_datum(mcx, p))
                .collect::<PgResult<Vec<_>>>()?;
            Ok(())
        }
        F_SPG_BOX_QUAD_PICKSPLIT => {
            let local_in = boxq::SpgPickSplitIn {
                boxes: in_.datums.iter().map(datum_get_box).collect(),
            };
            let mut local_out = boxq::SpgPickSplitOut::default();
            boxq::spg_box_quad_picksplit(&local_in, &mut local_out);
            out.hasPrefix = local_out.hasPrefix;
            out.prefixDatum = if local_out.hasPrefix {
                Some(box_get_datum(mcx, &local_out.prefix_box)?)
            } else {
                None
            };
            out.nNodes = local_out.nNodes;
            out.nodeLabels = None;
            out.mapTuplesToNodes = local_out.mapTuplesToNodes;
            out.leafTupleDatums = local_out
                .leafTupleDatums
                .iter()
                .map(|b| box_get_datum(mcx, b))
                .collect::<PgResult<Vec<_>>>()?;
            Ok(())
        }
        text::F_SPG_TEXT_PICKSPLIT => text::spg_text_picksplit(mcx, in_, out),
        inet::F_INET_SPG_PICKSPLIT => inet::inet_spg_picksplit(mcx, in_, out),
        range::F_SPG_RANGE_QUAD_PICKSPLIT => range::spg_range_quad_picksplit(mcx, in_, out),
        _ => Err(unrecognized_proc(proc_oid, "picksplit")),
    }
}

/// `spg_inner_consistent` dispatcher. `_mcx` is part of the seam signature but
/// unused here: the typed `traversalValues`/`distances` carriers are owned
/// `Vec`s (global alloc), not by-reference `Datum`s, so no index-context
/// allocation is needed for the inner-consistent output.
fn dispatch_inner_consistent<'mcx>(
    _mcx: Mcx<'mcx>,
    proc_oid: Oid,
    in_: &spgt::spgInnerConsistentIn<'mcx>,
    out: &mut spgt::spgInnerConsistentOut<'mcx>,
) -> PgResult<()> {
    match proc_oid {
        F_SPG_QUAD_INNER_CONSISTENT => {
            let local_in = SpgInnerConsistentIn {
                scankeys: decode_scankeys(&in_.scankeys),
                orderby_points: decode_orderby_points(&in_.orderbys),
                norderbys: in_.norderbys(),
                traversalValue: in_.traversalValue.as_ref().map(|b| BOX::from_datum_bytes(b)),
                level: in_.level,
                allTheSame: in_.allTheSame,
                hasPrefix: in_.hasPrefix,
                prefix_point: if in_.hasPrefix {
                    datum_get_point(&in_.prefixDatum)
                } else {
                    Point::default()
                },
                nNodes: in_.nNodes,
            };
            let mut local_out = SpgInnerConsistentOut::default();
            spg_quad_inner_consistent(&local_in, &mut local_out)?;
            write_inner_out(out, local_out.nNodes, local_out.nodeNumbers, local_out.levelAdds, &local_out.traversalValues, local_out.distances)
        }
        kd::F_SPG_KD_INNER_CONSISTENT => {
            let local_in = kd::SpgInnerConsistentIn {
                scankeys: decode_kd_scankeys(&in_.scankeys),
                orderby_points: decode_orderby_points(&in_.orderbys),
                norderbys: in_.norderbys(),
                traversalValue: in_.traversalValue.as_ref().map(|b| BOX::from_datum_bytes(b)),
                level: in_.level,
                allTheSame: in_.allTheSame,
                hasPrefix: in_.hasPrefix,
                // k-d-tree prefix is a float8 splitting coordinate.
                prefix_coord: if in_.hasPrefix {
                    in_.prefixDatum.as_f64()
                } else {
                    0.0
                },
                nNodes: in_.nNodes,
            };
            let mut local_out = kd::SpgInnerConsistentOut::default();
            kd::spg_kd_inner_consistent(&local_in, &mut local_out)?;
            write_inner_out(out, local_out.nNodes, local_out.nodeNumbers, local_out.levelAdds, &local_out.traversalValues, local_out.distances)
        }
        F_SPG_BOX_QUAD_INNER_CONSISTENT => {
            let local_in = boxq::SpgInnerConsistentIn {
                scankeys: decode_box_scankeys(&in_.scankeys),
                orderby_points: decode_orderby_points(&in_.orderbys),
                norderbys: in_.norderbys(),
                traversalValue: in_
                    .traversalValue
                    .as_ref()
                    .map(|b| boxq::RectBox::from_datum_bytes(b)),
                allTheSame: in_.allTheSame,
                prefix_box: if in_.hasPrefix {
                    datum_get_box(&in_.prefixDatum)
                } else {
                    BOX::default()
                },
                nNodes: in_.nNodes,
            };
            let mut local_out = boxq::SpgInnerConsistentOut::default();
            boxq::spg_box_quad_inner_consistent(&local_in, &mut local_out)?;
            out.nNodes = local_out.nNodes;
            out.nodeNumbers = local_out.nodeNumbers;
            // The box opclass never adjusts the descent level.
            out.levelAdds = Vec::new();
            out.reconstructedValues = Vec::new();
            out.traversalValues = local_out
                .traversalValues
                .iter()
                .map(|rb| Some(rb.to_datum_bytes().to_vec()))
                .collect();
            out.distances = local_out.distances;
            Ok(())
        }
        text::F_SPG_TEXT_INNER_CONSISTENT => text::spg_text_inner_consistent(_mcx, in_, out),
        inet::F_INET_SPG_INNER_CONSISTENT => inet::inet_spg_inner_consistent(in_, out),
        range::F_SPG_RANGE_QUAD_INNER_CONSISTENT => {
            range::spg_range_quad_inner_consistent(_mcx, in_, out)
        }
        _ => Err(unrecognized_proc(proc_oid, "inner_consistent")),
    }
}

/// Write the (BOX-traversal-value) inner-consistent result into the typed out.
/// Both point opclasses produce no reconstructed values and BOX traversals.
fn write_inner_out<'mcx>(
    out: &mut spgt::spgInnerConsistentOut<'mcx>,
    n_nodes: i32,
    node_numbers: Vec<i32>,
    level_adds: Vec<i32>,
    traversal_boxes: &[BOX],
    distances: Vec<Vec<f64>>,
) -> PgResult<()> {
    out.nNodes = n_nodes;
    out.nodeNumbers = node_numbers;
    out.levelAdds = level_adds;
    out.reconstructedValues = Vec::new();
    out.traversalValues = traversal_boxes
        .iter()
        .map(|b| Ok(Some(b.to_datum_bytes().to_vec())))
        .collect::<PgResult<Vec<Option<Vec<u8>>>>>()?;
    out.distances = distances;
    Ok(())
}

/// `spg_leaf_consistent` dispatcher. Only the quad-tree opclass defines a leaf
/// body; the k-d-tree opclass registers `spg_quad_leaf_consistent` (OID 4022)
/// for its leaf slot (spgkdtreeproc.c borrows it verbatim).
fn dispatch_leaf_consistent<'mcx>(
    mcx: Mcx<'mcx>,
    proc_oid: Oid,
    in_: &spgt::spgLeafConsistentIn<'mcx>,
    out: &mut spgt::spgLeafConsistentOut<'mcx>,
) -> PgResult<bool> {
    match proc_oid {
        F_SPG_QUAD_LEAF_CONSISTENT => {
            let local_in = SpgLeafConsistentIn {
                scankeys: decode_scankeys(&in_.scankeys),
                orderby_points: decode_orderby_points(&in_.orderbys),
                norderbys: in_.norderbys(),
                leaf_point: datum_get_point(&in_.leafDatum),
            };
            let mut local_out = SpgLeafConsistentOut::default();
            let res = spg_quad_leaf_consistent(&local_in, &mut local_out)?;
            out.leafValue = Some(point_get_datum(mcx, &local_out.leaf_point)?);
            out.recheck = local_out.recheck;
            out.recheckDistances = false;
            out.distances = local_out.distances;
            Ok(res)
        }
        F_SPG_BOX_QUAD_LEAF_CONSISTENT => {
            let local_in = boxq::SpgLeafConsistentIn {
                scankeys: decode_box_scankeys(&in_.scankeys),
                orderby_points: decode_orderby_points(&in_.orderbys),
                norderbys: in_.norderbys(),
                orderby0_distfnoid: in_
                    .orderbys
                    .first()
                    .map(|sk| sk.sk_func.fn_oid)
                    .unwrap_or(0),
                leaf_box: datum_get_box(&in_.leafDatum),
                returnData: in_.returnData,
            };
            let mut local_out = boxq::SpgLeafConsistentOut::default();
            let res = boxq::spg_box_quad_leaf_consistent(&local_in, &mut local_out)?;
            out.leafValue = match local_out.leafValue {
                Some(b) => Some(box_get_datum(mcx, &b)?),
                None => None,
            };
            out.recheck = local_out.recheck;
            out.recheckDistances = local_out.recheckDistances;
            out.distances = local_out.distances;
            Ok(res)
        }
        text::F_SPG_TEXT_LEAF_CONSISTENT => text::spg_text_leaf_consistent(mcx, in_, out),
        inet::F_INET_SPG_LEAF_CONSISTENT => inet::inet_spg_leaf_consistent(mcx, in_, out),
        range::F_SPG_RANGE_QUAD_LEAF_CONSISTENT => {
            range::spg_range_quad_leaf_consistent(mcx, in_, out)
        }
        _ => Err(unrecognized_proc(proc_oid, "leaf_consistent")),
    }
}

/// Decode a typed scankey array into the k-d-tree opclass' `SpgScanKey` form.
/// (Same `point`/`box` rule as the quad-tree opclass.)
fn decode_kd_scankeys(scankeys: &[types_scan::scankey::ScanKeyData<'_>]) -> Vec<kd::SpgScanKey> {
    scankeys
        .iter()
        .map(|sk| {
            let sk_strategy = sk.sk_strategy as u16;
            if sk_strategy == kd::RTContainedByStrategyNumber {
                kd::SpgScanKey {
                    sk_strategy,
                    query_point: Point::default(),
                    query_box: datum_get_box(&sk.sk_argument),
                }
            } else {
                kd::SpgScanKey {
                    sk_strategy,
                    query_point: datum_get_point(&sk.sk_argument),
                    query_box: BOX::default(),
                }
            }
        })
        .collect()
}

/// Install the quad-tree AND k-d-tree opclass support-procedure bodies into the
/// SP-GiST core's typed dispatch seams (this crate is the single installer for
/// both point opclasses, mirroring brin-minmax's single-crate dispatcher).
pub fn init_seams() {
    backend_access_spg_core_seams::spg_config::set(dispatch_config);
    backend_access_spg_core_seams::spg_choose::set(dispatch_choose);
    backend_access_spg_core_seams::spg_picksplit::set(dispatch_picksplit);
    backend_access_spg_core_seams::spg_inner_consistent::set(dispatch_inner_consistent);
    backend_access_spg_core_seams::spg_leaf_consistent::set(dispatch_leaf_consistent);
}
