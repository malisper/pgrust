//! spgquad_diff: differential fuzz driver — shipped Rust `spgist_quadtree` vs
//! vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_spgquad_io.c: spgquadtreeproc.c + spgproc.c + geo_ops.c point
//! relations + pg_hypot verbatim).
//! Crate under test: crates/backend/access/spgist/spgist_quadtree.
//!
//! Semantic planes:
//!   arm 0 config: prefixType/labelType/leafType/canReturnData/longValuesOK.
//!   arm 1 choose: nodeN + levelAdd + restDatum pointee (16 bytes).
//!     allTheSame: C leaves matchNode.nodeN UNINITIALIZED ("nodeN will be
//!     set by core"); Rust writes 0 — nodeN is a non-surface on that path
//!     (compare levelAdd + restDatum only).
//!   arm 2 picksplit (centroid = mean; the USE_MEDIAN qsort block is dead
//!     upstream — NO sort in the live code): hasPrefix, centroid pointee
//!     bits, nNodes, mapTuplesToNodes[n], leafTupleDatums pointees.
//!   arm 3 inner_consistent: nNodes/nodeNumbers/levelAdds (levelAdds only on
//!     the non-allTheSame path — C leaves the pointer NULL on allTheSame);
//!     with orderbys also traversalValues pointees (32-byte quadrant boxes)
//!     + distances rows (f64 bits, NaN canonicalized, <=2 ULP fp-contraction
//!     carve on pg_hypot results — kd precedent) + error verdict/sqlstate
//!     (22003 from the KNN distance overflow/underflow).
//!   arm 4 leaf_consistent: bool result + recheck + leafValue pointee +
//!     distances row (isLeaf=true -> point_dt: float8_mi may raise 22003
//!     too) + error verdict/sqlstate.
//!
//! Driver fences (C-parity error arms, elog/panic BOTH sides — exception
//! rows at gate; each is witnessed by a #[test] pair below asserting the C
//! class-90 elog AND the Rust panic on the same input):
//!   - getQuadrant impossible case: reachable IFF a NaN reaches a quadrant
//!     classification (choose input/centroid; picksplit points or NaN mean;
//!     inner Same/ContainedBy query or centroid). C elogs, Rust panics.
//!     Fuzz arms skip inputs where the involved coordinates make the
//!     quadrant undefined (any NaN among the compared coords).
//!   - strategy numbers outside {1,5,6,8,10,11,29,30}: elog/panic both
//!     sides; the fuzz arms map bytes onto the valid set.
//!
//! NO SEPARATE FC PLANE: the shipped entry points ARE the fc_* wrappers
//! (SPGIST_QUAD_BUILTINS); the driver calls them via FmgrInfo::invoke.

use datum::Datum;
use mcx::MemoryContext;
use types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE;
use types_fmgr::FmgrInfo;
use types_scan::scankey::ScanKeyData;
use types_spgist::spgConfigOut;
use types_spgist::state::{
    spgChooseIn, spgChooseOut, spgInnerConsistentIn, spgInnerConsistentOut, spgLeafConsistentIn,
    spgLeafConsistentOut, spgPickSplitIn, spgPickSplitOut,
};

extern "C" {
    fn pg_diff_quad_config(
        prefix_type: *mut u32,
        label_type: *mut u32,
        leaf_type: *mut u32,
        can_return: *mut i32,
        long_ok: *mut i32,
    ) -> i32;
    fn pg_diff_quad_choose(
        all_the_same: i32,
        prefix2: *const f64,
        level: i32,
        pt2: *const f64,
        node_n: *mut i32,
        level_add: *mut i32,
        rest2: *mut f64,
    ) -> i32;
    fn pg_diff_quad_picksplit(
        n: i32,
        pts2n: *const f64,
        level: i32,
        has_prefix: *mut i32,
        centroid2: *mut f64,
        n_nodes: *mut i32,
        map: *mut i32,
        leaf2n: *mut f64,
    ) -> i32;
    #[allow(clippy::too_many_arguments)]
    fn pg_diff_quad_inner(
        all_the_same: i32,
        in_nnodes: i32,
        prefix2: *const f64,
        level: i32,
        nkeys: i32,
        strategies: *const u16,
        args4: *const f64,
        norderbys: i32,
        obys2: *const f64,
        has_tv: i32,
        tv4: *const f64,
        n_nodes: *mut i32,
        node_numbers: *mut i32,
        level_adds: *mut i32,
        has_level_adds: *mut i32,
        tvout4: *mut f64,
        distout: *mut f64,
    ) -> i32;
    #[allow(clippy::too_many_arguments)]
    fn pg_diff_quad_leaf(
        leaf2: *const f64,
        level: i32,
        nkeys: i32,
        strategies: *const u16,
        args4: *const f64,
        norderbys: i32,
        obys2: *const f64,
        res: *mut i32,
        recheck: *mut i32,
        leafval2: *mut f64,
        dist: *mut f64,
    ) -> i32;
}

const VALID_STRATS: [u16; 8] = [1, 5, 6, 8, 10, 11, 29, 30];
const RT_SAME: u16 = 6;
const RT_CONTAINED_BY: u16 = 8;

struct Rd<'a> {
    b: &'a [u8],
}

impl Rd<'_> {
    fn u8(&mut self) -> u8 {
        let v = self.b.first().copied().unwrap_or(0);
        self.b = self.b.get(1..).unwrap_or(&[]);
        v
    }
    fn u16(&mut self) -> u16 {
        u16::from_le_bytes([self.u8(), self.u8()])
    }
    fn f64(&mut self) -> f64 {
        let mut a = [0u8; 8];
        for x in &mut a {
            *x = self.u8();
        }
        f64::from_le_bytes(a)
    }
    fn fvec(&mut self, n: usize) -> Vec<f64> {
        (0..n).map(|_| self.f64()).collect()
    }
}

fn invoke2(
    func: types_fmgr::PGFunction,
    oid: types_core::Oid,
    a0: usize,
    a1: usize,
    mcx: mcx::Mcx<'_>,
) -> types_error::PgResult<Datum> {
    let mut frame = types_fmgr::LocalFcinfo::<2>::fresh(0);
    // SAFETY: the arming context outlives this single call.
    unsafe { frame.set_result_mcx(mcx) };
    frame.set_arg(0, Datum::from_usize(a0));
    frame.set_arg(1, Datum::from_usize(a1));
    let mut fi = FmgrInfo::new(func, oid, 2, true, false);
    fi.invoke(&mut frame)
}

fn builtin(i: usize) -> types_fmgr::PGFunction {
    spgist_quadtree::SPGIST_QUAD_BUILTINS[i].func
}

/// Exact replica of getQuadrant's reachability condition: TRUE iff one of
/// the four quadrant cases holds under the C EPSILON-fuzzy predicates.
/// The "impossible case" elog/panic is reachable not only via NaN but also
/// for FINITE values at the epsilon boundary: rounding asymmetry can give
/// |A-B| > EPSILON (horiz false) while the computed A+EPSILON >= B (below
/// false) and A <= B+EPSILON (above false) — witnessed by fleet floor-1
/// crash-068b32635f (points (2.22e-314,1.0),(1.0,0.999998), centroid
/// (0.5,0.9999990000000001): the second point's dy-class is empty). Both
/// sides error identically there (elog_parity tests below).
fn quadrant_defined(cx: f64, cy: f64, tx: f64, ty: f64) -> bool {
    const E: f64 = 1.0e-6;
    let above = ty > cy + E;
    let below = ty + E < cy;
    let horiz = ty == cy || (ty - cy).abs() <= E;
    let right = tx > cx + E;
    let left = tx + E < cx;
    let vert = tx == cx || (tx - cx).abs() <= E;
    ((above || horiz) && (right || vert))
        || (below && (right || vert))
        || ((below || horiz) && left)
        || (above && left)
}

/// box_contain_point replica (raw compares, no EPSILON) for the
/// ContainedBy corner fence.
fn box_contains(hx: f64, hy: f64, lx: f64, ly: f64, px: f64, py: f64) -> bool {
    hx >= px && lx <= px && hy >= py && ly <= py
}

pub fn spgquad_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    let mut rd = Rd { b: payload };
    let ctx = MemoryContext::new("spgquad_fuzz");
    let mcx = ctx.mcx();
    match sel % 5 {
        0 => arm_config(mcx),
        1 => arm_choose(&mut rd, mcx),
        2 => arm_picksplit(&mut rd, mcx),
        3 => arm_inner(&mut rd, mcx),
        _ => arm_leaf(&mut rd, mcx),
    }
}

fn arm_config(mcx: mcx::Mcx<'_>) {
    let (mut pt, mut lt, mut ft, mut crd, mut lok) = (0u32, 0u32, 0u32, 0i32, 0i32);
    let cst = unsafe { pg_diff_quad_config(&mut pt, &mut lt, &mut ft, &mut crd, &mut lok) };
    assert_eq!(cst, 0);
    let cfgin = 0u32; // spgConfigIn { attType } — unread by the opclass
    let mut cfg = spgConfigOut::default();
    invoke2(builtin(0), 4018, &cfgin as *const u32 as usize, &mut cfg as *mut _ as usize, mcx)
        .expect("config errored");
    assert_eq!(
        (pt, lt, crd != 0, lok != 0),
        (cfg.prefixType, cfg.labelType, cfg.canReturnData, cfg.longValuesOK),
        "spg_quad_config DIVERGENCE"
    );
    // C leaves leafType untouched; Rust struct default is 0 — both stay 0.
    assert_eq!(ft, cfg.leafType, "spg_quad_config leafType DIVERGENCE");
}

fn arm_choose(rd: &mut Rd<'_>, mcx: mcx::Mcx<'_>) {
    let all_the_same = rd.u8() & 1 != 0;
    let level = rd.u16() as i32;
    let prefix = [rd.f64(), rd.f64()];
    let pt = [rd.f64(), rd.f64()];

    // getQuadrant fence: on the non-allTheSame path a NaN coordinate makes
    // the classification undefined (C elog / Rust panic — C-parity arm,
    // witnessed by elog_parity_* tests below).
    if !all_the_same && !quadrant_defined(prefix[0], prefix[1], pt[0], pt[1]) {
        return;
    }

    let (mut c_node, mut c_add) = (0i32, 0i32);
    let mut c_rest = [0f64; 2];
    let cst = unsafe {
        pg_diff_quad_choose(
            all_the_same as i32,
            prefix.as_ptr(),
            level,
            pt.as_ptr(),
            &mut c_node,
            &mut c_add,
            c_rest.as_mut_ptr(),
        )
    };
    assert_eq!(cst, 0, "choose verdict DIVERGENCE: C err {cst}, Rust untried");

    let input = spgChooseIn {
        datum: Datum::from_usize(pt.as_ptr() as usize),
        leafDatum: Datum::from_usize(pt.as_ptr() as usize),
        level,
        allTheSame: all_the_same,
        hasPrefix: true,
        prefixDatum: Datum::from_usize(prefix.as_ptr() as usize),
        nNodes: 4,
        nodeLabels: core::ptr::null(),
    };
    let mut out = spgChooseOut::None;
    invoke2(builtin(1), 4019, &input as *const _ as usize, &mut out as *mut _ as usize, mcx)
        .expect("choose errored");
    match out {
        spgChooseOut::MatchNode { nodeN, levelAdd, restDatum } => {
            let r_rest =
                unsafe { core::slice::from_raw_parts(restDatum.as_usize() as *const f64, 2) };
            if !all_the_same {
                assert_eq!(c_node, nodeN, "spg_quad_choose nodeN DIVERGENCE (level {level})");
            }
            // allTheSame: C leaves nodeN uninitialized (core sets it); the
            // driver-zeroed C out struct vs Rust's literal 0 is a non-surface.
            assert_eq!(c_add, levelAdd, "spg_quad_choose levelAdd DIVERGENCE");
            assert_eq!(
                c_rest.map(f64::to_bits),
                [r_rest[0].to_bits(), r_rest[1].to_bits()],
                "spg_quad_choose restDatum DIVERGENCE"
            );
        }
        _ => panic!("spg_quad_choose returned non-MatchNode"),
    }
}

fn arm_picksplit(rd: &mut Rd<'_>, mcx: mcx::Mcx<'_>) {
    let n = (rd.u8() as usize % 24) + 1;
    let level = rd.u16() as i32;
    let pts = rd.fvec(2 * n);

    // getQuadrant fence: any NaN input coordinate, or a NaN mean (inf/-inf
    // mixes), makes some quadrant classification undefined. Compute the mean
    // exactly as the C/Rust bodies do (same accumulation order).
    let (mut sx, mut sy) = (0f64, 0f64);
    for i in 0..n {
        sx += pts[2 * i];
        sy += pts[2 * i + 1];
    }
    let (cx, cy) = (sx / n as f64, sy / n as f64);
    for i in 0..n {
        if !quadrant_defined(cx, cy, pts[2 * i], pts[2 * i + 1]) {
            return;
        }
    }

    let mut c_has = 0i32;
    let mut c_centroid = [0f64; 2];
    let mut c_nnodes = 0i32;
    let mut c_map = vec![0i32; n];
    let mut c_leaf = vec![0f64; 2 * n];
    let cst = unsafe {
        pg_diff_quad_picksplit(
            n as i32,
            pts.as_ptr(),
            level,
            &mut c_has,
            c_centroid.as_mut_ptr(),
            &mut c_nnodes,
            c_map.as_mut_ptr(),
            c_leaf.as_mut_ptr(),
        )
    };
    assert_eq!(cst, 0, "picksplit verdict DIVERGENCE: C err {cst}, Rust untried");

    let datums: Vec<Datum> =
        (0..n).map(|i| Datum::from_usize(pts[2 * i..].as_ptr() as usize)).collect();
    let input = spgPickSplitIn { nTuples: n as i32, datums: datums.as_ptr(), level };
    let mut out = spgPickSplitOut::default();
    invoke2(builtin(2), 4020, &input as *const _ as usize, &mut out as *mut _ as usize, mcx)
        .expect("picksplit errored");

    assert_eq!(c_has != 0, out.hasPrefix, "picksplit hasPrefix DIVERGENCE");
    let r_centroid =
        unsafe { core::slice::from_raw_parts(out.prefixDatum.as_usize() as *const f64, 2) };
    assert_eq!(
        c_centroid.map(f64::to_bits),
        [r_centroid[0].to_bits(), r_centroid[1].to_bits()],
        "picksplit centroid DIVERGENCE"
    );
    assert_eq!(c_nnodes, out.nNodes, "picksplit nNodes DIVERGENCE");
    let r_map = unsafe { core::slice::from_raw_parts(out.mapTuplesToNodes, n) };
    assert_eq!(c_map, r_map, "picksplit map DIVERGENCE");
    for i in 0..n {
        let d = unsafe { *out.leafTupleDatums.add(i) };
        let r = unsafe { core::slice::from_raw_parts(d.as_usize() as *const f64, 2) };
        assert_eq!(
            [c_leaf[2 * i].to_bits(), c_leaf[2 * i + 1].to_bits()],
            [r[0].to_bits(), r[1].to_bits()],
            "picksplit leafTupleDatums DIVERGENCE at {i}"
        );
    }
}

/// distances-plane compare: NaN canonicalized; <=2 ULP same-sign carve for
/// the RATIFIED fp-contraction class on pg_hypot results (shipped pg_hypot
/// fuses explicitly; campaign oracles build -ffp-contract=off — kd
/// precedent, build.rs FP-CONTRACTION CARVE 2026-07-30).
fn dist_eq(cb: f64, rv: f64) -> bool {
    let norm = |v: f64| if v.is_nan() { f64::NAN.to_bits() } else { v.to_bits() };
    let (a, b) = (norm(cb), norm(rv));
    a == b || (cb.is_sign_positive() == rv.is_sign_positive() && a.abs_diff(b) <= 2)
}

fn arm_inner(rd: &mut Rd<'_>, mcx: mcx::Mcx<'_>) {
    let flags = rd.u8();
    let all_the_same = flags & 1 != 0;
    let in_nnodes: i32 = if all_the_same { ((flags >> 1) % 7) as i32 + 1 } else { 4 };
    let level = rd.u16() as i32;
    let prefix = [rd.f64(), rd.f64()];
    let nkeys = (rd.u8() % 5) as usize;
    let norderbys = (rd.u8() % 4) as usize;
    let tv = [rd.f64(), rd.f64(), rd.f64(), rd.f64()];
    // level>0 KNN requires a traversal box on both sides (C Assert compiled
    // out => NULL deref); always supply one on that path.
    let has_tv = norderbys > 0 && level > 0;

    let mut strats = Vec::with_capacity(nkeys);
    let mut args = Vec::with_capacity(4 * nkeys);
    for _ in 0..nkeys {
        strats.push(VALID_STRATS[(rd.u8() % 8) as usize]);
        args.extend_from_slice(&[rd.f64(), rd.f64(), rd.f64(), rd.f64()]);
    }
    let obys = rd.fvec(2 * norderbys);

    // getQuadrant fence (non-allTheSame path, exact C reachability): Same
    // classifies the query point against the centroid; ContainedBy
    // classifies the C corner sequence when the centroid is outside the
    // box (raw compares). Undefined classification = elog/panic parity arm.
    if !all_the_same {
        let (cx, cy) = (prefix[0], prefix[1]);
        for (i, &s) in strats.iter().enumerate() {
            let a = &args[4 * i..4 * i + 4];
            match s {
                RT_SAME => {
                    if !quadrant_defined(cx, cy, a[0], a[1]) {
                        return;
                    }
                }
                RT_CONTAINED_BY => {
                    // arg image is a BOX: high=(a0,a1), low=(a2,a3).
                    let (hx, hy, lx, ly) = (a[0], a[1], a[2], a[3]);
                    if !box_contains(hx, hy, lx, ly, cx, cy) {
                        // C corner sequence: low, (lx,hy), high, (lx,hy).
                        for (px, py) in [(lx, ly), (lx, hy), (hx, hy)] {
                            if !quadrant_defined(cx, cy, px, py) {
                                return;
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }

    let cap = in_nnodes.max(4) as usize;
    let mut c_nnodes = 0i32;
    let mut c_nums = vec![0i32; cap];
    let mut c_adds = vec![0i32; cap];
    let mut c_has_adds = 0i32;
    let mut c_tv = vec![0f64; 4 * cap];
    let mut c_dist = vec![0f64; cap * norderbys.max(1)];
    let cst = unsafe {
        pg_diff_quad_inner(
            all_the_same as i32,
            in_nnodes,
            prefix.as_ptr(),
            level,
            nkeys as i32,
            strats.as_ptr(),
            args.as_ptr(),
            norderbys as i32,
            obys.as_ptr(),
            has_tv as i32,
            tv.as_ptr(),
            &mut c_nnodes,
            c_nums.as_mut_ptr(),
            c_adds.as_mut_ptr(),
            &mut c_has_adds,
            c_tv.as_mut_ptr(),
            c_dist.as_mut_ptr(),
        )
    };

    let keys: Vec<ScanKeyData> = (0..nkeys)
        .map(|i| {
            let mut k = ScanKeyData::empty();
            k.sk_strategy = strats[i];
            k.sk_argument = Datum::from_usize(args[4 * i..].as_ptr() as usize);
            k
        })
        .collect();
    let okeys: Vec<ScanKeyData> = (0..norderbys)
        .map(|i| {
            let mut k = ScanKeyData::empty();
            k.sk_argument = Datum::from_usize(obys[2 * i..].as_ptr() as usize);
            k
        })
        .collect();
    let input = spgInnerConsistentIn {
        scankeys: keys.as_ptr(),
        orderbys: okeys.as_ptr(),
        nkeys: nkeys as i32,
        norderbys: norderbys as i32,
        reconstructedValue: Datum::null(),
        traversalValue: if has_tv { tv.as_ptr() as usize } else { 0 },
        traversalMemoryContext: mcx,
        level,
        returnData: false,
        allTheSame: all_the_same,
        hasPrefix: true,
        prefixDatum: Datum::from_usize(prefix.as_ptr() as usize),
        nNodes: in_nnodes,
        nodeLabels: core::ptr::null(),
    };
    let mut out = spgInnerConsistentOut::default();
    let r =
        invoke2(builtin(3), 4021, &input as *const _ as usize, &mut out as *mut _ as usize, mcx);

    match r {
        Err(e) => {
            assert_eq!(
                cst, 102,
                "inner_consistent verdict DIVERGENCE: C status {cst}, Rust Err({:?})",
                e.sqlstate
            );
            assert_eq!(
                e.sqlstate, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                "inner_consistent errcode DIVERGENCE"
            );
        }
        Ok(_) => {
            assert_eq!(cst, 0, "inner_consistent verdict DIVERGENCE: C err {cst}, Rust ok");
            assert_eq!(c_nnodes, out.nNodes, "inner_consistent nNodes DIVERGENCE");
            let k = out.nNodes as usize;
            assert_eq!(
                c_has_adds != 0,
                !out.levelAdds.is_null(),
                "inner_consistent levelAdds-presence DIVERGENCE"
            );
            if k == 0 {
                return;
            }
            let r_nums = unsafe { core::slice::from_raw_parts(out.nodeNumbers, k) };
            assert_eq!(&c_nums[..k], r_nums, "inner_consistent nodeNumbers DIVERGENCE");
            if c_has_adds != 0 {
                let r_adds = unsafe { core::slice::from_raw_parts(out.levelAdds, k) };
                assert_eq!(&c_adds[..k], r_adds, "inner_consistent levelAdds DIVERGENCE");
            }
            if norderbys > 0 {
                for i in 0..k {
                    let r_tv = unsafe {
                        core::slice::from_raw_parts(*out.traversalValues.add(i) as *const f64, 4)
                    };
                    for j in 0..4 {
                        assert_eq!(
                            c_tv[4 * i + j].to_bits(),
                            r_tv[j].to_bits(),
                            "traversalValues DIVERGENCE node {i} f{j}"
                        );
                    }
                    let r_row =
                        unsafe { core::slice::from_raw_parts(*out.distances.add(i), norderbys) };
                    for (j, &rv) in r_row.iter().enumerate() {
                        let cb = c_dist[norderbys * i + j];
                        assert!(
                            dist_eq(cb, rv),
                            "distances DIVERGENCE node {i} orderby {j}: C={cb} Rust={rv}"
                        );
                    }
                }
            }
        }
    }
}

fn arm_leaf(rd: &mut Rd<'_>, mcx: mcx::Mcx<'_>) {
    let level = rd.u16() as i32;
    let leaf = [rd.f64(), rd.f64()];
    let nkeys = (rd.u8() % 5) as usize;
    let norderbys = (rd.u8() % 4) as usize;

    let mut strats = Vec::with_capacity(nkeys);
    let mut args = Vec::with_capacity(4 * nkeys);
    for _ in 0..nkeys {
        strats.push(VALID_STRATS[(rd.u8() % 8) as usize]);
        args.extend_from_slice(&[rd.f64(), rd.f64(), rd.f64(), rd.f64()]);
    }
    let obys = rd.fvec(2 * norderbys);
    // leaf_consistent never calls getQuadrant — no NaN fence needed; all
    // SPTEST relations and box_contain_point are total over NaN.

    let mut c_res = 0i32;
    let mut c_recheck = 0i32;
    let mut c_leafval = [0f64; 2];
    let mut c_dist = vec![0f64; norderbys.max(1)];
    let cst = unsafe {
        pg_diff_quad_leaf(
            leaf.as_ptr(),
            level,
            nkeys as i32,
            strats.as_ptr(),
            args.as_ptr(),
            norderbys as i32,
            obys.as_ptr(),
            &mut c_res,
            &mut c_recheck,
            c_leafval.as_mut_ptr(),
            c_dist.as_mut_ptr(),
        )
    };

    let keys: Vec<ScanKeyData> = (0..nkeys)
        .map(|i| {
            let mut k = ScanKeyData::empty();
            k.sk_strategy = strats[i];
            k.sk_argument = Datum::from_usize(args[4 * i..].as_ptr() as usize);
            k
        })
        .collect();
    let okeys: Vec<ScanKeyData> = (0..norderbys)
        .map(|i| {
            let mut k = ScanKeyData::empty();
            k.sk_argument = Datum::from_usize(obys[2 * i..].as_ptr() as usize);
            k
        })
        .collect();
    let input = spgLeafConsistentIn {
        scankeys: keys.as_ptr(),
        orderbys: okeys.as_ptr(),
        nkeys: nkeys as i32,
        norderbys: norderbys as i32,
        reconstructedValue: Datum::null(),
        traversalValue: 0,
        level,
        returnData: false,
        leafDatum: Datum::from_usize(leaf.as_ptr() as usize),
    };
    let mut out = spgLeafConsistentOut::default();
    let r =
        invoke2(builtin(4), 4022, &input as *const _ as usize, &mut out as *mut _ as usize, mcx);

    match r {
        Err(e) => {
            assert_eq!(
                cst, 102,
                "leaf_consistent verdict DIVERGENCE: C status {cst}, Rust Err({:?})",
                e.sqlstate
            );
            assert_eq!(
                e.sqlstate, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                "leaf_consistent errcode DIVERGENCE"
            );
        }
        Ok(v) => {
            assert_eq!(cst, 0, "leaf_consistent verdict DIVERGENCE: C err {cst}, Rust ok");
            let r_res = v.as_usize() != 0;
            assert_eq!(c_res != 0, r_res, "leaf_consistent result DIVERGENCE");
            assert_eq!(c_recheck != 0, out.recheck, "leaf_consistent recheck DIVERGENCE");
            let r_lv =
                unsafe { core::slice::from_raw_parts(out.leafValue.as_usize() as *const f64, 2) };
            assert_eq!(
                c_leafval.map(f64::to_bits),
                [r_lv[0].to_bits(), r_lv[1].to_bits()],
                "leaf_consistent leafValue DIVERGENCE"
            );
            if r_res && norderbys > 0 {
                let r_row = unsafe { core::slice::from_raw_parts(out.distances, norderbys) };
                for (j, &rv) in r_row.iter().enumerate() {
                    assert!(
                        dist_eq(c_dist[j], rv),
                        "leaf distances DIVERGENCE orderby {j}: C={} Rust={rv}",
                        c_dist[j]
                    );
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn f(v: f64) -> [u8; 8] {
        v.to_le_bytes()
    }

    #[test]
    fn seed_corpus_replays_clean() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/spgquad_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/spgquad_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                spgquad_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    /// C-parity witness for the getQuadrant impossible case: the vendored C
    /// elogs (class 90 -> status 190) AND the shipped Rust panics on the
    /// same NaN input — the fuzz-arm fence carves exactly this pair.
    #[test]
    fn elog_parity_nan_quadrant() {
        let _g = crate::c_oracle_serial();
        let prefix = [f64::NAN, 0.0];
        let pt = [1.0, 1.0];
        let (mut n, mut a) = (0i32, 0i32);
        let mut rest = [0f64; 2];
        let cst = unsafe {
            pg_diff_quad_choose(0, prefix.as_ptr(), 0, pt.as_ptr(), &mut n, &mut a,
                rest.as_mut_ptr())
        };
        assert_eq!(cst, 190, "C must elog on NaN quadrant");
        let r = std::panic::catch_unwind(|| {
            spgist_quadtree::getQuadrant(
                &types_core::geo::Point { x: f64::NAN, y: 0.0 },
                &types_core::geo::Point { x: 1.0, y: 1.0 },
            )
        });
        assert!(r.is_err(), "Rust must panic on NaN quadrant");
    }

    /// C-parity witness for the unrecognized-strategy elog: C status 190,
    /// Rust panic, same input (strategy 2 is not in the opclass set).
    #[test]
    fn elog_parity_bad_strategy() {
        let _g = crate::c_oracle_serial();
        let leaf = [0.0f64, 0.0];
        let strats = [2u16];
        let args = [0f64; 4];
        let (mut res, mut rc) = (0i32, 0i32);
        let mut lv = [0f64; 2];
        let mut d = [0f64; 1];
        let cst = unsafe {
            pg_diff_quad_leaf(leaf.as_ptr(), 0, 1, strats.as_ptr(), args.as_ptr(), 0,
                core::ptr::null(), &mut res, &mut rc, lv.as_mut_ptr(), d.as_mut_ptr())
        };
        assert_eq!(cst, 190, "C must elog on unrecognized strategy");
        // Rust side: the same leaf call through the shipped entry.
        let ctx = MemoryContext::new("spgquad_test");
        let mcx = ctx.mcx();
        let mut k = ScanKeyData::empty();
        k.sk_strategy = 2;
        k.sk_argument = Datum::from_usize(args.as_ptr() as usize);
        let keys = [k];
        let input = spgLeafConsistentIn {
            scankeys: keys.as_ptr(),
            orderbys: core::ptr::null(),
            nkeys: 1,
            norderbys: 0,
            reconstructedValue: Datum::null(),
            traversalValue: 0,
            level: 0,
            returnData: false,
            leafDatum: Datum::from_usize(leaf.as_ptr() as usize),
        };
        let mut out = spgLeafConsistentOut::default();
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            invoke2(builtin(4), 4022, &input as *const _ as usize,
                &mut out as *mut _ as usize, mcx)
        }));
        assert!(r.is_err(), "Rust must panic on unrecognized strategy");
    }

    /// Inner-arm twin of elog_parity_bad_strategy: quad inner_consistent
    /// unrecognized-strategy elog (C 190) + shipped Rust panic.
    #[test]
    fn elog_parity_bad_strategy_inner() {
        let _g = crate::c_oracle_serial();
        let prefix = [1.0f64, 1.0];
        let strategies = [2u16];
        let args = [0f64; 4];
        let (mut nn, mut has_adds) = (0i32, 0i32);
        let mut nums = [0i32; 4];
        let mut adds = [0i32; 4];
        let mut tv = [0f64; 16];
        let mut d = [0f64; 4];
        let cst = unsafe {
            pg_diff_quad_inner(0, 4, prefix.as_ptr(), 0, 1, strategies.as_ptr(),
                args.as_ptr(), 0, core::ptr::null(), 0, core::ptr::null(),
                &mut nn, nums.as_mut_ptr(), adds.as_mut_ptr(), &mut has_adds,
                tv.as_mut_ptr(), d.as_mut_ptr())
        };
        assert_eq!(cst, 190, "C must elog on unrecognized inner strategy");
        let ctx = MemoryContext::new("spgquad_test");
        let mcx = ctx.mcx();
        let mut k = ScanKeyData::empty();
        k.sk_strategy = 2;
        k.sk_argument = Datum::from_usize(args.as_ptr() as usize);
        let keys = [k];
        let input = spgInnerConsistentIn {
            scankeys: keys.as_ptr(),
            orderbys: core::ptr::null(),
            nkeys: 1,
            norderbys: 0,
            reconstructedValue: Datum::null(),
            traversalValue: 0,
            traversalMemoryContext: mcx,
            level: 0,
            returnData: false,
            allTheSame: false,
            hasPrefix: true,
            prefixDatum: Datum::from_usize(prefix.as_ptr() as usize),
            nNodes: 4,
            nodeLabels: core::ptr::null(),
        };
        let mut out = spgInnerConsistentOut::default();
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            invoke2(builtin(3), 4021, &input as *const _ as usize,
                &mut out as *mut _ as usize, mcx)
        }));
        assert!(r.is_err(), "Rust must panic on unrecognized inner strategy");
    }

    #[test]
    fn arms_smoke() {
        // config
        spgquad_diff(&[0]);
        // choose: all quadrants, axis ties (EPSILON band), allTheSame
        for ats in [0u8, 1] {
            for level in [0u16, 3] {
                for (px, py) in
                    [(1.0, 1.0), (2.0, 0.5), (0.5, 2.0), (1.0 + 5e-7, 1.0), (1e308, -1e308)]
                {
                    let mut v = vec![1, ats];
                    v.extend_from_slice(&level.to_le_bytes());
                    v.extend_from_slice(&f(1.0));
                    v.extend_from_slice(&f(1.0));
                    v.extend_from_slice(&f(px));
                    v.extend_from_slice(&f(py));
                    spgquad_diff(&v);
                }
            }
        }
        // NaN choose input routes through the fence (no compare, no crash)
        let mut v = vec![1, 0, 0, 0];
        v.extend_from_slice(&f(f64::NAN));
        v.extend_from_slice(&f(0.0));
        v.extend_from_slice(&f(1.0));
        v.extend_from_slice(&f(1.0));
        spgquad_diff(&v);
        // picksplit: distinct, all-same, epsilon ties, inf mean
        let cells: [&[f64]; 4] = [
            &[3.0, 1.0, 2.0, 5.0, 4.0],
            &[7.0, 7.0, 7.0],
            &[1.0, 1.0 + 5e-7, 1.0 - 5e-7, 2.0],
            &[f64::INFINITY, 1.0, 2.0],
        ];
        for xs in cells {
            let mut v = vec![2, xs.len() as u8 - 1, 0, 0];
            for (i, &x) in xs.iter().enumerate() {
                v.extend_from_slice(&f(x));
                v.extend_from_slice(&f(i as f64));
            }
            spgquad_diff(&v);
        }
        // inner: every strategy, both allTheSame arms, KNN levels
        for flags in [0u8, 1, 5] {
            for strat in 0u8..8 {
                for level in [0u16, 1] {
                    let mut v = vec![3, flags];
                    v.extend_from_slice(&level.to_le_bytes());
                    v.extend_from_slice(&f(1.0)); // prefix
                    v.extend_from_slice(&f(1.0));
                    v.push(1); // nkeys
                    v.push(2); // norderbys
                    for x in [10.0, 10.0, -10.0, -10.0] {
                        v.extend_from_slice(&f(x)); // tv box
                    }
                    v.push(strat);
                    for x in [0.5, 0.75, 2.0, 3.0] {
                        v.extend_from_slice(&f(x)); // arg image
                    }
                    v.extend_from_slice(&f(0.25)); // orderby points
                    v.extend_from_slice(&f(0.5));
                    v.extend_from_slice(&f(1.7e308));
                    v.extend_from_slice(&f(-1.7e308));
                    spgquad_diff(&v);
                }
            }
        }
        // leaf: every strategy + KNN distances + NaN coords (total, no fence)
        for strat in 0u8..8 {
            for (lx, ly) in [(1.0, 1.0), (f64::NAN, 0.0), (1e308, -1e308)] {
                let mut v = vec![4, 0, 0];
                v.extend_from_slice(&f(lx));
                v.extend_from_slice(&f(ly));
                v.push(1); // nkeys
                v.push(1); // norderbys
                v.push(strat);
                for x in [0.5, 0.75, 2.0, 3.0] {
                    v.extend_from_slice(&f(x));
                }
                v.extend_from_slice(&f(0.25));
                v.extend_from_slice(&f(0.5));
                spgquad_diff(&v);
            }
        }
        // empty/truncated
        spgquad_diff(&[]);
        spgquad_diff(&[1]);
        spgquad_diff(&[2, 5]);
        spgquad_diff(&[3, 1, 2, 3]);
    }
}

#[cfg(test)]
mod overflow_witness {
    #[test]
    fn knn_overflow_errors_both_sides() {
        let f = |v: f64| v.to_le_bytes();
        let mut v = vec![3u8, 0, 1, 0]; // arm 3, flags 0, level 1
        v.extend_from_slice(&f(1.0)); // prefix
        v.extend_from_slice(&f(1.0));
        v.push(0); // nkeys
        v.push(1); // norderbys
        for x in [10.0, 10.0, -10.0, -10.0] {
            v.extend_from_slice(&f(x));
        }
        v.extend_from_slice(&f(1.7e308));
        v.extend_from_slice(&f(-1.7e308));
        super::spgquad_diff(&v);
    }
}

/// C-parity witness for the FINITE eps-boundary impossible case (fleet
/// floor-1 crash-068b32635f): C elogs (190) AND the shipped Rust panics on
/// the same non-NaN input; the fuzz fence carves exactly this pair.
#[cfg(test)]
mod eps_boundary_witness {
    use super::*;

    #[test]
    fn finite_eps_boundary_errors_both_sides() {
        let _g = crate::c_oracle_serial();
        // centroid = mean of (2.2233762285e-314, 1.0), (1.0, 0.999998)
        let pts = [2.2233762285e-314, 1.0, 1.0, 0.999998];
        let (cx, cy) = ((pts[0] + pts[2]) / 2.0, (pts[1] + pts[3]) / 2.0);
        assert!(!quadrant_defined(cx, cy, pts[2], pts[3]), "fence must carve this input");
        let (mut has, mut nn) = (0i32, 0i32);
        let mut cent = [0f64; 2];
        let mut map = [0i32; 2];
        let mut leaf = [0f64; 4];
        let cst = unsafe {
            pg_diff_quad_picksplit(2, pts.as_ptr(), 0, &mut has, cent.as_mut_ptr(), &mut nn,
                map.as_mut_ptr(), leaf.as_mut_ptr())
        };
        assert_eq!(cst, 190, "C must elog on the eps-boundary quadrant");
        let r = std::panic::catch_unwind(|| {
            spgist_quadtree::getQuadrant(
                &types_core::geo::Point { x: cx, y: cy },
                &types_core::geo::Point { x: pts[2], y: pts[3] },
            )
        });
        assert!(r.is_err(), "Rust must panic on the eps-boundary quadrant");
    }

    #[test]
    fn floor1_divergence_input_replays_clean() {
        // The banked fleet input routes through the fence now.
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/spgquad_diff");
        let f = format!("{dir}/seed-fleet-crash-068b32635f");
        super::spgquad_diff(&std::fs::read(f).unwrap());
    }
}
