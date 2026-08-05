//! spgbox_diff: differential fuzz driver — shipped Rust `spgist_box` vs
//! vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_spgbox_io.c: geo_spgist.c + spgproc.c + geo_ops.c box relations +
//! pg_hypot verbatim; the picksplit qsort is the VERBATIM lib/sort_template.h
//! pg_qsort instantiation — never libc — and the shipped Rust side routes
//! through the canonical pg_qsort crate with the C-exact compareDoubles, so
//! both permutations are bit-identical on every input, NaN and -0.0/0.0 ties
//! included).
//! Crate under test: crates/backend/access/spgist/spgist_box.
//!
//! Semantic planes:
//!   arm 0 spg_box_quad_config / arm 5 spg_bbox_quad_config: all five
//!     spgConfigOut fields.
//!   arm 1 choose: nodeN (non-allTheSame; C leaves it unset on allTheSame —
//!     "set by core") + levelAdd + restDatum pointee (32-byte box).
//!   arm 2 picksplit: hasPrefix + centroid pointee bits (median of the four
//!     sorted coordinate arrays) + nNodes(16) + map + leaf pointees.
//!   arm 3 inner_consistent: nNodes/nodeNumbers + traversalValues pointees
//!     (64-byte RectBox images, non-allTheSame only — C never sets them on
//!     the allTheSame path) + distances rows (<=2 ULP fp-contraction carve
//!     on pg_hypot results, kd/quad precedent) + error verdict/sqlstate
//!     (22003 KNN distance overflow/underflow).
//!   arm 4 leaf_consistent: bool result + recheck + recheckDistances
//!     (orderby fn_oid == F_DIST_POLYP) + distances row + error verdict.
//!     Scankeys are BOX- or POLYGON-subtyped; polygon args are constructed
//!     unpacked varlena images whose boundbox both sides read.
//!   arm 6 spg_poly_quad_compress: output box pointee bits.
//!
//! Driver fences (C-parity elog/panic arms, witnessed by #[test] pairs):
//!   scankey subtype outside {BOXOID, POLYGONOID}; strategy outside 1..=12.
//!
//! NO SEPARATE FC PLANE: the shipped entry points ARE the fc_* wrappers
//! (SPGIST_BOX_BUILTINS); the driver calls them via FmgrInfo::invoke.

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
    fn pg_diff_box_config(
        bbox: i32,
        prefix_type: *mut u32,
        label_type: *mut u32,
        leaf_type: *mut u32,
        can_return: *mut i32,
        long_ok: *mut i32,
    ) -> i32;
    fn pg_diff_box_choose(
        all_the_same: i32,
        prefix4: *const f64,
        leaf4: *const f64,
        level: i32,
        node_n: *mut i32,
        level_add: *mut i32,
        rest4: *mut f64,
    ) -> i32;
    fn pg_diff_box_picksplit(
        n: i32,
        boxes4n: *const f64,
        level: i32,
        has_prefix: *mut i32,
        centroid4: *mut f64,
        n_nodes: *mut i32,
        map: *mut i32,
        leaf4n: *mut f64,
    ) -> i32;
    #[allow(clippy::too_many_arguments)]
    fn pg_diff_box_inner(
        all_the_same: i32,
        in_nnodes: i32,
        prefix4: *const f64,
        has_tv: i32,
        tv8: *const f64,
        nkeys: i32,
        strategies: *const u16,
        subtypes: *const u32,
        args: *const usize,
        norderbys: i32,
        obys2: *const f64,
        n_nodes: *mut i32,
        node_numbers: *mut i32,
        tvout8: *mut f64,
        distout: *mut f64,
    ) -> i32;
    #[allow(clippy::too_many_arguments)]
    fn pg_diff_box_leaf(
        leaf4: *const f64,
        return_data: i32,
        nkeys: i32,
        strategies: *const u16,
        subtypes: *const u32,
        args: *const usize,
        oby_fn_oid: u32,
        norderbys: i32,
        obys2: *const f64,
        res: *mut i32,
        recheck: *mut i32,
        recheck_dist: *mut i32,
        dist: *mut f64,
    ) -> i32;
    fn pg_diff_box_poly_compress(poly: *const core::ffi::c_void, out4: *mut f64) -> i32;
}

const BOXOID: u32 = 603;
const POLYGONOID: u32 = 604;
const F_DIST_POLYP: u32 = 3292;

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
    spgist_box::SPGIST_BOX_BUILTINS[i].func
}

/// An unpacked 8-aligned POLYGON varlena image with the given boundbox and
/// one point (npts=1): [vl_len_|npts][boundbox 32B][p0 16B] = 56 bytes.
/// vl_len_ = size << 2 (4-byte-header varlena).
fn poly_image(bb: [f64; 4], p0: [f64; 2]) -> Vec<u64> {
    let size: u32 = 8 + 32 + 16;
    let mut v = vec![0u64; (size as usize) / 8];
    let bytes = unsafe {
        core::slice::from_raw_parts_mut(v.as_mut_ptr() as *mut u8, size as usize)
    };
    bytes[0..4].copy_from_slice(&(size << 2).to_le_bytes());
    bytes[4..8].copy_from_slice(&1u32.to_le_bytes());
    for (i, x) in bb.iter().enumerate() {
        bytes[8 + 8 * i..16 + 8 * i].copy_from_slice(&x.to_le_bytes());
    }
    for (i, x) in p0.iter().enumerate() {
        bytes[40 + 8 * i..48 + 8 * i].copy_from_slice(&x.to_le_bytes());
    }
    v
}

/// distances-plane compare: NaN canonicalized; <=2 ULP same-sign carve for
/// the RATIFIED fp-contraction class on pg_hypot results (kd/quad precedent).
fn dist_eq(cb: f64, rv: f64) -> bool {
    let norm = |v: f64| if v.is_nan() { f64::NAN.to_bits() } else { v.to_bits() };
    let (a, b) = (norm(cb), norm(rv));
    a == b || (cb.is_sign_positive() == rv.is_sign_positive() && a.abs_diff(b) <= 2)
}

/// One scankey's fuzz decode: strategy from 1..=12, subtype box/polygon,
/// arg image owned by the returned buffers.
struct Keys {
    strategies: Vec<u16>,
    subtypes: Vec<u32>,
    args: Vec<usize>,
    _boxes: Vec<Vec<f64>>,
    _polys: Vec<Vec<u64>>,
}

fn read_keys(rd: &mut Rd<'_>, nkeys: usize) -> Keys {
    let mut k = Keys {
        strategies: Vec::new(),
        subtypes: Vec::new(),
        args: Vec::new(),
        _boxes: Vec::new(),
        _polys: Vec::new(),
    };
    for _ in 0..nkeys {
        let sel = rd.u8();
        k.strategies.push((sel % 12) as u16 + 1);
        let bb = [rd.f64(), rd.f64(), rd.f64(), rd.f64()];
        if sel & 0x80 != 0 {
            let poly = poly_image(bb, [rd.f64(), rd.f64()]);
            k.args.push(poly.as_ptr() as usize);
            k.subtypes.push(POLYGONOID);
            k._polys.push(poly);
        } else {
            let b = bb.to_vec();
            k.args.push(b.as_ptr() as usize);
            k.subtypes.push(BOXOID);
            k._boxes.push(b);
        }
    }
    k
}

fn rust_keys(k: &Keys, fn_oid: u32) -> Vec<ScanKeyData> {
    (0..k.strategies.len())
        .map(|i| {
            let mut sk = ScanKeyData::empty();
            sk.sk_strategy = k.strategies[i];
            sk.sk_subtype = k.subtypes[i];
            let mut fi = FmgrInfo::unresolved();
            fi.fn_oid = fn_oid;
            sk.sk_func = fi;
            sk.sk_argument = Datum::from_usize(k.args[i]);
            sk
        })
        .collect()
}

pub fn spgbox_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    let mut rd = Rd { b: payload };
    let ctx = MemoryContext::new("spgbox_fuzz");
    let mcx = ctx.mcx();
    match sel % 7 {
        0 => arm_config(false, mcx),
        1 => arm_choose(&mut rd, mcx),
        2 => arm_picksplit(&mut rd, mcx),
        3 => arm_inner(&mut rd, mcx),
        4 => arm_leaf(&mut rd, mcx),
        5 => arm_config(true, mcx),
        _ => arm_compress(&mut rd, mcx),
    }
}

fn arm_config(bbox: bool, mcx: mcx::Mcx<'_>) {
    let (mut pt, mut lt, mut ft, mut crd, mut lok) = (0u32, 0u32, 0u32, 0i32, 0i32);
    let cst =
        unsafe { pg_diff_box_config(bbox as i32, &mut pt, &mut lt, &mut ft, &mut crd, &mut lok) };
    assert_eq!(cst, 0);
    let cfgin = 0u32;
    let mut cfg = spgConfigOut::default();
    let (idx, oid) = if bbox { (0, 5010) } else { (2, 5012) };
    invoke2(builtin(idx), oid, &cfgin as *const u32 as usize, &mut cfg as *mut _ as usize, mcx)
        .expect("config errored");
    assert_eq!(
        (pt, lt, ft, crd != 0, lok != 0),
        (cfg.prefixType, cfg.labelType, cfg.leafType, cfg.canReturnData, cfg.longValuesOK),
        "spg_{}box_quad_config DIVERGENCE",
        if bbox { "b" } else { "" }
    );
}

fn arm_choose(rd: &mut Rd<'_>, mcx: mcx::Mcx<'_>) {
    let all_the_same = rd.u8() & 1 != 0;
    let level = rd.u16() as i32;
    let prefix = [rd.f64(), rd.f64(), rd.f64(), rd.f64()];
    let leaf = [rd.f64(), rd.f64(), rd.f64(), rd.f64()];

    let (mut c_node, mut c_add) = (0i32, 0i32);
    let mut c_rest = [0f64; 4];
    let cst = unsafe {
        pg_diff_box_choose(
            all_the_same as i32,
            prefix.as_ptr(),
            leaf.as_ptr(),
            level,
            &mut c_node,
            &mut c_add,
            c_rest.as_mut_ptr(),
        )
    };
    assert_eq!(cst, 0, "choose verdict DIVERGENCE: C err {cst}");

    let input = spgChooseIn {
        datum: Datum::from_usize(leaf.as_ptr() as usize),
        leafDatum: Datum::from_usize(leaf.as_ptr() as usize),
        level,
        allTheSame: all_the_same,
        hasPrefix: true,
        prefixDatum: Datum::from_usize(prefix.as_ptr() as usize),
        nNodes: 16,
        nodeLabels: core::ptr::null(),
    };
    let mut out = spgChooseOut::None;
    invoke2(builtin(3), 5013, &input as *const _ as usize, &mut out as *mut _ as usize, mcx)
        .expect("choose errored");
    match out {
        spgChooseOut::MatchNode { nodeN, levelAdd, restDatum } => {
            if !all_the_same {
                assert_eq!(c_node, nodeN, "spg_box_quad_choose nodeN DIVERGENCE");
            }
            assert_eq!(c_add, levelAdd, "spg_box_quad_choose levelAdd DIVERGENCE");
            let r =
                unsafe { core::slice::from_raw_parts(restDatum.as_usize() as *const f64, 4) };
            for j in 0..4 {
                assert_eq!(
                    c_rest[j].to_bits(),
                    r[j].to_bits(),
                    "spg_box_quad_choose restDatum DIVERGENCE f{j}"
                );
            }
        }
        _ => panic!("spg_box_quad_choose returned non-MatchNode"),
    }
}

fn arm_picksplit(rd: &mut Rd<'_>, mcx: mcx::Mcx<'_>) {
    let n = (rd.u8() as usize % 24) + 1;
    let level = rd.u16() as i32;
    let boxes = rd.fvec(4 * n);

    let mut c_has = 0i32;
    let mut c_centroid = [0f64; 4];
    let mut c_nnodes = 0i32;
    let mut c_map = vec![0i32; n];
    let mut c_leaf = vec![0f64; 4 * n];
    let cst = unsafe {
        pg_diff_box_picksplit(
            n as i32,
            boxes.as_ptr(),
            level,
            &mut c_has,
            c_centroid.as_mut_ptr(),
            &mut c_nnodes,
            c_map.as_mut_ptr(),
            c_leaf.as_mut_ptr(),
        )
    };
    assert_eq!(cst, 0, "picksplit verdict DIVERGENCE: C err {cst}");

    let datums: Vec<Datum> =
        (0..n).map(|i| Datum::from_usize(boxes[4 * i..].as_ptr() as usize)).collect();
    let input = spgPickSplitIn { nTuples: n as i32, datums: datums.as_ptr(), level };
    let mut out = spgPickSplitOut::default();
    invoke2(builtin(4), 5014, &input as *const _ as usize, &mut out as *mut _ as usize, mcx)
        .expect("picksplit errored");

    assert_eq!(c_has != 0, out.hasPrefix, "picksplit hasPrefix DIVERGENCE");
    let r_centroid =
        unsafe { core::slice::from_raw_parts(out.prefixDatum.as_usize() as *const f64, 4) };
    for j in 0..4 {
        assert_eq!(
            c_centroid[j].to_bits(),
            r_centroid[j].to_bits(),
            "picksplit centroid DIVERGENCE f{j} (median under pg_qsort)"
        );
    }
    assert_eq!(c_nnodes, out.nNodes, "picksplit nNodes DIVERGENCE");
    let r_map = unsafe { core::slice::from_raw_parts(out.mapTuplesToNodes, n) };
    assert_eq!(c_map, r_map, "picksplit map DIVERGENCE");
    for i in 0..n {
        let d = unsafe { *out.leafTupleDatums.add(i) };
        let r = unsafe { core::slice::from_raw_parts(d.as_usize() as *const f64, 4) };
        for j in 0..4 {
            assert_eq!(
                c_leaf[4 * i + j].to_bits(),
                r[j].to_bits(),
                "picksplit leafTupleDatums DIVERGENCE at {i} f{j}"
            );
        }
    }
}

fn arm_inner(rd: &mut Rd<'_>, mcx: mcx::Mcx<'_>) {
    let flags = rd.u8();
    let all_the_same = flags & 1 != 0;
    let in_nnodes: i32 = if all_the_same { ((flags >> 1) % 16) as i32 + 1 } else { 16 };
    let has_tv = flags & 0x20 != 0;
    let prefix = [rd.f64(), rd.f64(), rd.f64(), rd.f64()];
    let tv = [
        rd.f64(),
        rd.f64(),
        rd.f64(),
        rd.f64(),
        rd.f64(),
        rd.f64(),
        rd.f64(),
        rd.f64(),
    ];
    let nkeys = (rd.u8() % 4) as usize;
    let norderbys = (rd.u8() % 3) as usize;
    let keys = read_keys(rd, nkeys);
    let obys = rd.fvec(2 * norderbys);

    let cap = in_nnodes.max(16) as usize;
    let mut c_nnodes = 0i32;
    let mut c_nums = vec![0i32; cap];
    let mut c_tv = vec![0f64; 8 * cap];
    let mut c_dist = vec![0f64; cap * norderbys.max(1)];
    let cst = unsafe {
        pg_diff_box_inner(
            all_the_same as i32,
            in_nnodes,
            prefix.as_ptr(),
            has_tv as i32,
            tv.as_ptr(),
            nkeys as i32,
            keys.strategies.as_ptr(),
            keys.subtypes.as_ptr(),
            keys.args.as_ptr(),
            norderbys as i32,
            obys.as_ptr(),
            &mut c_nnodes,
            c_nums.as_mut_ptr(),
            c_tv.as_mut_ptr(),
            c_dist.as_mut_ptr(),
        )
    };

    let rkeys = rust_keys(&keys, 0);
    let okeys: Vec<ScanKeyData> = (0..norderbys)
        .map(|i| {
            let mut k = ScanKeyData::empty();
            k.sk_argument = Datum::from_usize(obys[2 * i..].as_ptr() as usize);
            k
        })
        .collect();
    let input = spgInnerConsistentIn {
        scankeys: rkeys.as_ptr(),
        orderbys: okeys.as_ptr(),
        nkeys: nkeys as i32,
        norderbys: norderbys as i32,
        reconstructedValue: Datum::null(),
        traversalValue: if has_tv { tv.as_ptr() as usize } else { 0 },
        traversalMemoryContext: mcx,
        level: 0,
        returnData: false,
        allTheSame: all_the_same,
        hasPrefix: true,
        prefixDatum: Datum::from_usize(prefix.as_ptr() as usize),
        nNodes: in_nnodes,
        nodeLabels: core::ptr::null(),
    };
    let mut out = spgInnerConsistentOut::default();
    let r =
        invoke2(builtin(5), 5015, &input as *const _ as usize, &mut out as *mut _ as usize, mcx);

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
            if k == 0 {
                return;
            }
            let r_nums = unsafe { core::slice::from_raw_parts(out.nodeNumbers, k) };
            assert_eq!(&c_nums[..k], r_nums, "inner_consistent nodeNumbers DIVERGENCE");
            if !all_the_same {
                for i in 0..k {
                    let r_tv = unsafe {
                        core::slice::from_raw_parts(
                            *out.traversalValues.add(i) as *const f64,
                            8,
                        )
                    };
                    for j in 0..8 {
                        assert_eq!(
                            c_tv[8 * i + j].to_bits(),
                            r_tv[j].to_bits(),
                            "traversalValues DIVERGENCE node {i} f{j}"
                        );
                    }
                }
            }
            if norderbys > 0 {
                for i in 0..k {
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
    let flags = rd.u8();
    let return_data = flags & 1 != 0;
    let oby_fn_oid: u32 = if flags & 2 != 0 { F_DIST_POLYP } else { 0 };
    let leaf = [rd.f64(), rd.f64(), rd.f64(), rd.f64()];
    let nkeys = (rd.u8() % 4) as usize;
    let norderbys = (rd.u8() % 3) as usize;
    let keys = read_keys(rd, nkeys);
    let obys = rd.fvec(2 * norderbys);

    let mut c_res = 0i32;
    let mut c_recheck = 0i32;
    let mut c_rd = 0i32;
    let mut c_dist = vec![0f64; norderbys.max(1)];
    let cst = unsafe {
        pg_diff_box_leaf(
            leaf.as_ptr(),
            return_data as i32,
            nkeys as i32,
            keys.strategies.as_ptr(),
            keys.subtypes.as_ptr(),
            keys.args.as_ptr(),
            oby_fn_oid,
            norderbys as i32,
            obys.as_ptr(),
            &mut c_res,
            &mut c_recheck,
            &mut c_rd,
            c_dist.as_mut_ptr(),
        )
    };

    let rkeys = rust_keys(&keys, 0);
    let okeys: Vec<ScanKeyData> = (0..norderbys)
        .map(|i| {
            let mut k = ScanKeyData::empty();
            let mut fi = FmgrInfo::unresolved();
            fi.fn_oid = oby_fn_oid;
            k.sk_func = fi;
            k.sk_argument = Datum::from_usize(obys[2 * i..].as_ptr() as usize);
            k
        })
        .collect();
    let input = spgLeafConsistentIn {
        scankeys: rkeys.as_ptr(),
        orderbys: okeys.as_ptr(),
        nkeys: nkeys as i32,
        norderbys: norderbys as i32,
        reconstructedValue: Datum::null(),
        traversalValue: 0,
        level: 0,
        returnData: return_data,
        leafDatum: Datum::from_usize(leaf.as_ptr() as usize),
    };
    let mut out = spgLeafConsistentOut::default();
    let r =
        invoke2(builtin(6), 5016, &input as *const _ as usize, &mut out as *mut _ as usize, mcx);

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
            if r_res && norderbys > 0 {
                assert_eq!(
                    c_rd != 0,
                    out.recheckDistances,
                    "leaf_consistent recheckDistances DIVERGENCE"
                );
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

fn arm_compress(rd: &mut Rd<'_>, mcx: mcx::Mcx<'_>) {
    let bb = [rd.f64(), rd.f64(), rd.f64(), rd.f64()];
    let p0 = [rd.f64(), rd.f64()];
    let poly = poly_image(bb, p0);

    let mut c_out = [0f64; 4];
    let cst = unsafe {
        pg_diff_box_poly_compress(poly.as_ptr() as *const core::ffi::c_void, c_out.as_mut_ptr())
    };
    assert_eq!(cst, 0);

    let mut frame = types_fmgr::LocalFcinfo::<1>::fresh(0);
    // SAFETY: the arming context outlives this single call.
    unsafe { frame.set_result_mcx(mcx) };
    frame.set_arg(0, Datum::from_usize(poly.as_ptr() as usize));
    let mut fi = FmgrInfo::new(builtin(1).into(), 5011, 1, true, false);
    let r = fi.invoke(&mut frame).expect("poly_compress errored");
    let out = unsafe { core::slice::from_raw_parts(r.as_usize() as *const f64, 4) };
    for j in 0..4 {
        assert_eq!(
            c_out[j].to_bits(),
            out[j].to_bits(),
            "spg_poly_quad_compress DIVERGENCE f{j}"
        );
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
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/spgbox_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/spgbox_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                spgbox_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    /// C-parity witness: unrecognized scankey subtype elogs in C (190) and
    /// panics in the shipped Rust on the same input.
    #[test]
    fn elog_parity_bad_subtype() {
        let _g = crate::c_oracle_serial();
        let leaf = [0f64; 4];
        let strategies = [3u16];
        let subtypes = [42u32];
        let arg = [0f64; 4];
        let args = [arg.as_ptr() as usize];
        let (mut res, mut rc, mut rd) = (0i32, 0i32, 0i32);
        let mut d = [0f64; 1];
        let cst = unsafe {
            pg_diff_box_leaf(leaf.as_ptr(), 0, 1, strategies.as_ptr(), subtypes.as_ptr(),
                args.as_ptr(), 0, 0, core::ptr::null(), &mut res, &mut rc, &mut rd,
                d.as_mut_ptr())
        };
        assert_eq!(cst, 190, "C must elog on unrecognized subtype");
        let ctx = MemoryContext::new("spgbox_test");
        let mcx = ctx.mcx();
        let mut sk = ScanKeyData::empty();
        sk.sk_strategy = 3;
        sk.sk_subtype = 42;
        sk.sk_argument = Datum::from_usize(arg.as_ptr() as usize);
        let keys = [sk];
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
            invoke2(builtin(6), 5016, &input as *const _ as usize,
                &mut out as *mut _ as usize, mcx)
        }));
        assert!(r.is_err(), "Rust must panic on unrecognized subtype");
    }

    /// C-parity witness: unrecognized strategy elogs in C (190) and panics
    /// in the shipped Rust (strategy 13 is outside the opclass set).
    #[test]
    fn elog_parity_bad_strategy() {
        let _g = crate::c_oracle_serial();
        let leaf = [0f64; 4];
        let strategies = [13u16];
        let subtypes = [BOXOID];
        let arg = [0f64; 4];
        let args = [arg.as_ptr() as usize];
        let (mut res, mut rc, mut rd) = (0i32, 0i32, 0i32);
        let mut d = [0f64; 1];
        let cst = unsafe {
            pg_diff_box_leaf(leaf.as_ptr(), 0, 1, strategies.as_ptr(), subtypes.as_ptr(),
                args.as_ptr(), 0, 0, core::ptr::null(), &mut res, &mut rc, &mut rd,
                d.as_mut_ptr())
        };
        assert_eq!(cst, 190, "C must elog on unrecognized strategy");
        let ctx = MemoryContext::new("spgbox_test");
        let mcx = ctx.mcx();
        let mut sk = ScanKeyData::empty();
        sk.sk_strategy = 13;
        sk.sk_subtype = BOXOID;
        sk.sk_argument = Datum::from_usize(arg.as_ptr() as usize);
        let keys = [sk];
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
            invoke2(builtin(6), 5016, &input as *const _ as usize,
                &mut out as *mut _ as usize, mcx)
        }));
        assert!(r.is_err(), "Rust must panic on unrecognized strategy");
    }

    /// Inner-arm twin of elog_parity_bad_strategy: the inner_consistent
    /// unrecognized-strategy elog (C 190) and the shipped Rust panic.
    #[test]
    fn elog_parity_bad_strategy_inner() {
        let _g = crate::c_oracle_serial();
        let prefix = [2.0f64, 2.0, 1.0, 1.0];
        let strategies = [13u16];
        let subtypes = [BOXOID];
        let arg = [0f64; 4];
        let args = [arg.as_ptr() as usize];
        let mut nn = 0i32;
        let mut nums = [0i32; 16];
        let mut tv = [0f64; 128];
        let mut d = [0f64; 16];
        let cst = unsafe {
            pg_diff_box_inner(0, 16, prefix.as_ptr(), 0, core::ptr::null(), 1,
                strategies.as_ptr(), subtypes.as_ptr(), args.as_ptr(), 0,
                core::ptr::null(), &mut nn, nums.as_mut_ptr(), tv.as_mut_ptr(),
                d.as_mut_ptr())
        };
        assert_eq!(cst, 190, "C must elog on unrecognized inner strategy");
        let ctx = MemoryContext::new("spgbox_test");
        let mcx = ctx.mcx();
        let mut sk = ScanKeyData::empty();
        sk.sk_strategy = 13;
        sk.sk_subtype = BOXOID;
        sk.sk_argument = Datum::from_usize(arg.as_ptr() as usize);
        let keys = [sk];
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
            nNodes: 16,
            nodeLabels: core::ptr::null(),
        };
        let mut out = spgInnerConsistentOut::default();
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            invoke2(builtin(5), 5015, &input as *const _ as usize,
                &mut out as *mut _ as usize, mcx)
        }));
        assert!(r.is_err(), "Rust must panic on unrecognized inner strategy");
    }

    #[test]
    fn arms_smoke() {
        // configs + compress
        spgbox_diff(&[0]);
        spgbox_diff(&[5]);
        let mut v = vec![6];
        for x in [3.0, 4.0, 1.0, 2.0, 1.5, 2.5] {
            v.extend_from_slice(&f(x));
        }
        spgbox_diff(&v);
        // choose: quadrant bit boundaries (>, ==, <, NaN, -0/0)
        for ats in [0u8, 1] {
            for d in [-1.0, 0.0, 1.0, f64::NAN] {
                let mut v = vec![1, ats, 0, 0];
                for x in [2.0, 2.0, 1.0, 1.0] {
                    v.extend_from_slice(&f(x)); // prefix box
                }
                for x in [2.0 + d, 2.0, 1.0 + d, 1.0] {
                    v.extend_from_slice(&f(x)); // leaf box
                }
                spgbox_diff(&v);
            }
        }
        // picksplit: distinct, ties across median, all-same, NaN coords,
        // -0.0/0.0 bit ties at the median
        let cells: [&[f64]; 5] = [
            &[1.0, 2.0, 3.0, 4.0, 5.0],
            &[1.0, 2.0, 2.0, 2.0, 3.0],
            &[7.0, 7.0, 7.0],
            &[1.0, f64::NAN, 2.0, f64::NAN, 3.0],
            &[-0.0, 0.0, -0.0, 0.0, 1.0],
        ];
        for xs in cells {
            let mut v = vec![2, xs.len() as u8 - 1, 0, 0];
            for (i, &x) in xs.iter().enumerate() {
                // box i: high=(x+1, i+1), low=(x, i)
                v.extend_from_slice(&f(x + 1.0));
                v.extend_from_slice(&f(i as f64 + 1.0));
                v.extend_from_slice(&f(x));
                v.extend_from_slice(&f(i as f64));
            }
            spgbox_diff(&v);
        }
        // inner: every strategy, box + polygon keys, allTheSame, KNN, tv
        for strat in 0u8..12 {
            for poly in [0u8, 0x80] {
                for flags in [0u8, 1, 0x20] {
                    let mut v = vec![3, flags];
                    for x in [2.0, 2.0, 1.0, 1.0] {
                        v.extend_from_slice(&f(x)); // prefix
                    }
                    for x in [10.0, 20.0, 5.0, 15.0, -10.0, 0.0, -20.0, -5.0] {
                        v.extend_from_slice(&f(x)); // tv RectBox
                    }
                    v.push(1); // nkeys
                    v.push(1); // norderbys
                    v.push(strat | poly);
                    for x in [1.5, 1.5, 0.5, 0.5] {
                        v.extend_from_slice(&f(x)); // key bb
                    }
                    if poly != 0 {
                        v.extend_from_slice(&f(1.0)); // poly p0
                        v.extend_from_slice(&f(1.0));
                    }
                    v.extend_from_slice(&f(0.25)); // orderby point
                    v.extend_from_slice(&f(0.5));
                    spgbox_diff(&v);
                }
            }
        }
        // inner KNN overflow (1.7e308 orderby vs finite rect)
        let mut v = vec![3, 0];
        for x in [2.0, 2.0, 1.0, 1.0] {
            v.extend_from_slice(&f(x));
        }
        for _ in 0..8 {
            v.extend_from_slice(&f(0.0));
        }
        v.push(0);
        v.push(1);
        v.extend_from_slice(&f(1.7e308));
        v.extend_from_slice(&f(-1.7e308));
        spgbox_diff(&v);
        // leaf: every strategy, box + polygon keys, recheckDistances both ways
        for strat in 0u8..12 {
            for poly in [0u8, 0x80] {
                for flags in [0u8, 1, 2, 3] {
                    let mut v = vec![4, flags];
                    for x in [2.0, 2.0, 1.0, 1.0] {
                        v.extend_from_slice(&f(x)); // leaf box
                    }
                    v.push(1); // nkeys
                    v.push(1); // norderbys
                    v.push(strat | poly);
                    for x in [1.5, 2.5, 0.5, 1.5] {
                        v.extend_from_slice(&f(x));
                    }
                    if poly != 0 {
                        v.extend_from_slice(&f(1.0));
                        v.extend_from_slice(&f(1.0));
                    }
                    v.extend_from_slice(&f(0.25));
                    v.extend_from_slice(&f(0.5));
                    spgbox_diff(&v);
                }
            }
        }
        // leaf with DISJOINT directional queries: each axis relation must
        // discriminate (query strictly right/above of leaf, and strictly
        // left/below), so left-vs-right / above-vs-below mutants die.
        for strat in 0u8..12 {
            for (qx, qy) in [(10.0, 0.0), (-10.0, 0.0), (0.0, 10.0), (0.0, -10.0)] {
                let mut v = vec![4, 0];
                for x in [2.0, 2.0, 1.0, 1.0] {
                    v.extend_from_slice(&f(x)); // leaf box
                }
                v.push(1); // nkeys
                v.push(0); // norderbys
                v.push(strat);
                v.extend_from_slice(&f(1.5 + qx)); // query high.x
                v.extend_from_slice(&f(2.5 + qy)); // query high.y
                v.extend_from_slice(&f(0.5 + qx)); // query low.x
                v.extend_from_slice(&f(1.5 + qy)); // query low.y
                spgbox_diff(&v);
            }
        }
        // truncated
        spgbox_diff(&[]);
        spgbox_diff(&[1]);
        spgbox_diff(&[2, 5]);
        spgbox_diff(&[3, 1, 2]);
        spgbox_diff(&[4]);
        spgbox_diff(&[6, 9]);
    }
}
