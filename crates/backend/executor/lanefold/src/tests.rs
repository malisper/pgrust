// Ported from the pgrcolumnar branch's nodeagg lanefold_tests, restricted to the
// harvested surface: the tests there that drove the whole executor
// (exec_agg_batched, dict windows, textlen, DISTINCT, metadata) stay behind;
// what ports is the kernel-level byte-parity contract — classifier admission
// and refusal, the TYPE/DATA proofs, guard exactness, CSE derivation, and the
// fold kernels checked bit-for-bit against a per-row reference that applies
// C's transition semantics in C's row order.

use core::ptr::NonNull;

use ::adt_numeric::aggregates::Int128AggState;

use ::datum::Datum;
use ::execexpr::{AggPerGroup, AggTransSpec, OUTER_VAR};
use ::mcx::{Mcx, MemoryContext};
use ::types_core::catalog::{INT2OID, INT4OID, INT8OID};
use ::types_nodes::list::NodeList;
use ::types_nodes::node_tree::Node;
use ::types_nodes::primnodes::OpExpr;

use crate::*;

const F_INT42DIV: Oid = 173;

// ---- node builders (as in the original lanefold_tests) ----

fn mk_var(mcx: Mcx<'static>, attno: i16, vartype: Oid) -> Node<'static> {
    Node::mk_var(mcx, OUTER_VAR, attno, vartype, -1, 0, 0).unwrap()
}

// Generic int-Var/int4-Const OpExpr builder; var_first=false puts the Const
// in arg position a (the int42* commuted forms).
fn mk_int_op(
    mcx: Mcx<'static>,
    attno: i16,
    vartype: Oid,
    k: i32,
    opfuncid: Oid,
    var_first: bool,
) -> Node<'static> {
    let var = mk_var(mcx, attno, vartype);
    let konst = Node::mk_const(mcx, INT4OID, -1, 0, 4, Datum::from_i32(k), false, true).unwrap();
    let mut op = Node::build::<OpExpr>(mcx).unwrap();
    op.opfuncid = opfuncid;
    op.opresulttype = INT4OID;
    op.args = if var_first {
        NodeList::make2(mcx, var, konst).unwrap()
    } else {
        NodeList::make2(mcx, konst, var).unwrap()
    };
    op.seal()
}

fn arg_list(mcx: Mcx<'static>, expr: Node<'static>) -> NodeList<'static> {
    let tle = Node::mk_target_entry(mcx, expr, 1, None, false).unwrap();
    NodeList::make1(mcx, tle).unwrap()
}

fn mk_spec<'a>(
    transfn_oid: Oid,
    init_value_is_null: bool,
    args: &'a NodeList<'static>,
) -> AggTransSpec<'a, 'static> {
    AggTransSpec {
        transfn_oid,
        combine: false,
        deserialfn_oid: 0,
        inputcollid: 0,
        init_value_is_null,
        arg_types: &[],
        args,
        aggfilter: None,
        pergroup: NonNull::dangling(),
        transtype_byval: true,
        transtype_len: 8,
        ordered: None,
        cur_agg: None,
    }
}

fn leaked_mcx() -> Mcx<'static> {
    Box::leak(Box::new(MemoryContext::new("lanefold-test"))).mcx()
}

// ---- column store + reference model ----

// Vec-backed LaneCols; None = NULL.
struct TestCols {
    values: Vec<Vec<Datum>>,
    isnull: Vec<Vec<bool>>,
}

impl TestCols {
    // widths[c] in {2, 4, 8}: the Datum constructor per column.
    fn new(widths: &[i16], data: &[Vec<Option<i64>>]) -> TestCols {
        let ncols = widths.len();
        let mut values = vec![Vec::with_capacity(data.len()); ncols];
        let mut isnull = vec![Vec::with_capacity(data.len()); ncols];
        for row in data {
            for (c, v) in row.iter().enumerate() {
                values[c].push(match v {
                    Some(v) => match widths[c] {
                        2 => Datum::from_i16(*v as i16),
                        4 => Datum::from_i32(*v as i32),
                        _ => Datum::from_i64(*v),
                    },
                    None => Datum::null(),
                });
                isnull[c].push(v.is_none());
            }
        }
        TestCols { values, isnull }
    }

    // Raw datum rows (None = NULL): the tier-2 datum-lane pools (floats with
    // NaN payloads / ±0 / ±inf, bools, exact bit patterns) build their lanes
    // without an i64 detour.
    fn from_datum_rows(ncols: usize, data: &[Vec<Option<Datum>>]) -> TestCols {
        let mut values = vec![Vec::with_capacity(data.len()); ncols];
        let mut isnull = vec![Vec::with_capacity(data.len()); ncols];
        for row in data {
            for (c, v) in row.iter().enumerate() {
                values[c].push(v.unwrap_or(Datum::null()));
                isnull[c].push(v.is_none());
            }
        }
        TestCols { values, isnull }
    }
}

impl LaneCols for TestCols {
    fn col_values(&self, c: usize) -> &[Datum] {
        &self.values[c]
    }

    fn col_isnull(&self, c: usize) -> &[bool] {
        &self.isnull[c]
    }
}

fn selmask(n: usize, sel: impl Fn(usize) -> bool) -> Vec<u64> {
    let mut m = vec![0u64; n.div_ceil(64)];
    for i in 0..n {
        if sel(i) {
            m[i / 64] |= 1u64 << (i % 64);
        }
    }
    m
}

// C-initval pergroup per kind (nodeAgg initialize_aggregate): count/avg carry
// non-null initvals, sum is NULL non-strict, min/max are strict + NULL init.
fn init_pergroup(mcx: Mcx<'_>, kind: LaneKind) -> AggPerGroup {
    match kind {
        LaneKind::CountStar | LaneKind::CountAny => AggPerGroup {
            trans_value: Datum::from_i64(0),
            trans_value_is_null: false,
            no_trans_value: false,
        },
        LaneKind::Sum => AggPerGroup {
            trans_value: Datum::null(),
            trans_value_is_null: true,
            no_trans_value: false,
        },
        LaneKind::AvgAccum => AggPerGroup {
            trans_value: new_int8_transarray(mcx),
            trans_value_is_null: false,
            no_trans_value: false,
        },
        // int8_avg_accum: INTERNAL transtype, NULL catalog initval, transfn
        // not strict (C initialize_aggregate sets both flags from
        // initValueIsNull; noTransValue is never consulted for non-strict).
        // numeric_avg_accum shares the exact discipline (fold-trans inc 2).
        LaneKind::Int128AvgAccum | LaneKind::NumAccum => AggPerGroup {
            trans_value: Datum::null(),
            trans_value_is_null: true,
            no_trans_value: true,
        },
        // float8_regr_accum: strict with the NON-null '{0,0,0,0,0,0}'
        // float8[6] catalog initval — live transarray from the first row.
        LaneKind::FRegrAccum => AggPerGroup {
            trans_value: new_float8_regr_transarray(mcx),
            trans_value_is_null: false,
            no_trans_value: false,
        },
        // regr_count: strict two-arg counter with the non-null '0' initval.
        LaneKind::Count2 => AggPerGroup {
            trans_value: Datum::from_i64(0),
            trans_value_is_null: false,
            no_trans_value: false,
        },
        // Every tier-2 kind is a strict NULL-init transfn, same discipline as
        // int min/max.
        LaneKind::Min
        | LaneKind::Max
        | LaneKind::FMin
        | LaneKind::FMax
        | LaneKind::BoolAnd
        | LaneKind::BoolOr
        | LaneKind::BitAnd
        | LaneKind::BitOr
        | LaneKind::StrMin
        | LaneKind::StrMax
        | LaneKind::BpMin
        | LaneKind::BpMax
        // sum(float4/float8): strict + NULL catalog initval, same discipline.
        | LaneKind::FSum => AggPerGroup {
            trans_value: Datum::null(),
            trans_value_is_null: true,
            no_trans_value: true,
        },
        // float4/float8_accum: strict with the NON-null '{0,0,0}' float8[3]
        // catalog initval — the transarray is live from the first row.
        LaneKind::FAccum => AggPerGroup {
            trans_value: new_float8_transarray(mcx),
            trans_value_is_null: false,
            no_trans_value: false,
        },
    }
}

fn pergroups_for(mcx: Mcx<'_>, plan: &LanePlan<'_>, ntrans: usize) -> Vec<AggPerGroup> {
    let mut pgs: Vec<AggPerGroup> = (0..ntrans)
        .map(|_| AggPerGroup {
            trans_value: Datum::null(),
            trans_value_is_null: true,
            no_trans_value: true,
        })
        .collect();
    for t in plan.trans.iter() {
        pgs[t.transno as usize] = init_pergroup(mcx, t.kind);
    }
    pgs
}

fn read_transarray(pg: &AggPerGroup) -> (i64, i64) {
    assert!(!pg.trans_value_is_null);
    // SAFETY: transarray allocated by new_int8_transarray, arena-lived.
    unsafe {
        let p = (pg.trans_value.as_usize() as *const u8).add(ARR_OVERHEAD_NONULLS_1);
        (p.cast::<i64>().read(), p.cast::<i64>().add(1).read())
    }
}

fn read_int128_state(pg: &AggPerGroup) -> (i64, i128) {
    assert!(!pg.trans_value_is_null);
    // SAFETY: state installed by the fold's int128_state or the reference's
    // leaked Box, live for the test.
    let st = unsafe { &*(pg.trans_value.as_usize() as *const Int128AggState) };
    assert!(!st.calc_sum_x2, "int8_avg_accum state carries no sumX2");
    (st.n, st.sum_x)
}

// Per-row reference: C's transition semantics applied in C's row order, one
// transition at a time (transition order within a row is immaterial for
// these independent pergroup cells).
fn reference_fold(
    mcx: Mcx<'_>,
    plan: &LanePlan<'_>,
    data: &[Vec<Option<i64>>],
    sel: impl Fn(usize) -> bool,
    ntrans: usize,
) -> Vec<AggPerGroup> {
    let mut pgs = pergroups_for(mcx, plan, ntrans);
    for t in plan.trans.iter() {
        let pg = &mut pgs[t.transno as usize];
        for (i, row) in data.iter().enumerate() {
            if !sel(i) {
                continue;
            }
            if t.kind == LaneKind::CountStar {
                pg.trans_value = Datum::from_i64(pg.trans_value.as_i64().wrapping_add(1));
                continue;
            }
            if t.kind == LaneKind::Int128AvgAccum {
                // C int8_avg_accum, per selected row: NOT strict, so the
                // state allocates on the group's first call even for a NULL
                // input; only non-null inputs accumulate (do_int128_accum).
                if pg.trans_value_is_null {
                    let p = Box::into_raw(Box::new(Int128AggState::new(false)));
                    pg.trans_value = Datum::from_usize(p as usize);
                    pg.trans_value_is_null = false;
                }
                // SAFETY: the leaked state installed above. Re-derive from
                // the datum on every row, exactly like the product fold:
                // keeping a `&mut` across the exposing ptr→int cast would
                // invalidate the exposed tag on each write (miri F7).
                let st: &mut Int128AggState =
                    unsafe { &mut *(pg.trans_value.as_usize() as *mut Int128AggState) };
                if let Some(v) = row[t.col as usize] {
                    st.sum_x += v as i128;
                    st.n += 1;
                }
                continue;
            }
            let Some(v) = row[t.col as usize] else { continue };
            // The admitted transform, checked per row exactly as C evaluates
            // the OpExpr (trunc division, int4-fitting result by admission).
            let v = (v / t.divk as i64) * t.mulk as i64 + t.addend as i64;
            match t.kind {
                LaneKind::CountStar | LaneKind::Int128AvgAccum => unreachable!(),
                LaneKind::CountAny => {
                    pg.trans_value = Datum::from_i64(pg.trans_value.as_i64().wrapping_add(1));
                }
                LaneKind::Sum => {
                    let old = if pg.trans_value_is_null { 0 } else { pg.trans_value.as_i64() };
                    pg.trans_value = Datum::from_i64(old.wrapping_add(v));
                    pg.trans_value_is_null = false;
                }
                LaneKind::AvgAccum => {
                    // SAFETY: reference transarray from new_int8_transarray.
                    unsafe {
                        let p = (pg.trans_value.as_usize() as *mut u8)
                            .add(ARR_OVERHEAD_NONULLS_1)
                            .cast::<i64>();
                        *p = (*p).wrapping_add(1);
                        *p.add(1) = (*p.add(1)).wrapping_add(v);
                    }
                }
                LaneKind::Min | LaneKind::Max => {
                    let store = |v: i64| match t.res_width {
                        LaneWidth::I16 => Datum::from_i16(v as i16),
                        LaneWidth::I32 => Datum::from_i32(v as i32),
                        LaneWidth::I64 => Datum::from_i64(v),
                        _ => unreachable!(),
                    };
                    if pg.no_trans_value {
                        pg.trans_value = store(v);
                        pg.trans_value_is_null = false;
                        pg.no_trans_value = false;
                    } else {
                        let old = match t.res_width {
                            LaneWidth::I16 => pg.trans_value.as_i16() as i64,
                            LaneWidth::I32 => pg.trans_value.as_i32() as i64,
                            LaneWidth::I64 => pg.trans_value.as_i64(),
                            _ => unreachable!(),
                        };
                        let next =
                            if t.kind == LaneKind::Max { old.max(v) } else { old.min(v) };
                        pg.trans_value = store(next);
                    }
                }
                _ => unreachable!("datum-lane kinds use reference_fold_datum"),
            }
        }
    }
    pgs
}

// C-semantics per-row reference for the tier-2 datum-lane kinds (float
// MIN/MAX, bool_and/bool_or, bit_and/bit_or): the strict-transfn advance
// applied in C's row order, one transition at a time, over raw datum lanes
// (None = NULL). Bit patterns are load-bearing — the float advance replicates
// float.c larger/smaller literally (gt/lt per float.h, tie takes the new
// argument datum).
fn reference_fold_datum(
    mcx: Mcx<'_>,
    plan: &LanePlan<'_>,
    data: &[Vec<Option<Datum>>],
    sel: impl Fn(usize) -> bool,
    ntrans: usize,
) -> Vec<AggPerGroup> {
    let ref_gt = |w: LaneWidth, a: Datum, b: Datum| match w {
        LaneWidth::F32 => {
            let (x, y) = (a.as_f32(), b.as_f32());
            !y.is_nan() && (x.is_nan() || x > y)
        }
        LaneWidth::F64 => {
            let (x, y) = (a.as_f64(), b.as_f64());
            !y.is_nan() && (x.is_nan() || x > y)
        }
        _ => unreachable!(),
    };
    let ref_lt = |w: LaneWidth, a: Datum, b: Datum| match w {
        LaneWidth::F32 => {
            let (x, y) = (a.as_f32(), b.as_f32());
            !x.is_nan() && (y.is_nan() || x < y)
        }
        LaneWidth::F64 => {
            let (x, y) = (a.as_f64(), b.as_f64());
            !x.is_nan() && (y.is_nan() || x < y)
        }
        _ => unreachable!(),
    };
    let mut pgs = pergroups_for(mcx, plan, ntrans);
    for t in plan.trans.iter() {
        let pg = &mut pgs[t.transno as usize];
        for (i, row) in data.iter().enumerate() {
            if !sel(i) {
                continue;
            }
            let Some(d) = row[t.col as usize] else { continue };
            // Strict transfn, NULL init: the first non-null input datum
            // installs verbatim (nodeAgg's strict first-value copy).
            if pg.no_trans_value {
                pg.trans_value = d;
                pg.trans_value_is_null = false;
                pg.no_trans_value = false;
                continue;
            }
            let old = pg.trans_value;
            pg.trans_value = match t.kind {
                // float.c float4/float8 larger/smaller: gt/lt ? old : new.
                LaneKind::FMax => {
                    if ref_gt(t.width, old, d) {
                        old
                    } else {
                        d
                    }
                }
                LaneKind::FMin => {
                    if ref_lt(t.width, old, d) {
                        old
                    } else {
                        d
                    }
                }
                // bool.c booland/boolor_statefunc: canonical bool datum.
                LaneKind::BoolAnd => Datum::from_bool(old.as_bool() && d.as_bool()),
                LaneKind::BoolOr => Datum::from_bool(old.as_bool() || d.as_bool()),
                // int.c/int8.c native-width bitwise ops, signed GetDatum.
                LaneKind::BitAnd | LaneKind::BitOr => {
                    let op = |a: i64, b: i64| {
                        if t.kind == LaneKind::BitAnd {
                            a & b
                        } else {
                            a | b
                        }
                    };
                    match t.res_width {
                        LaneWidth::I16 => Datum::from_i16(op(old.as_i16() as i64, d.as_i16() as i64) as i16),
                        LaneWidth::I32 => Datum::from_i32(op(old.as_i32() as i64, d.as_i32() as i64) as i32),
                        LaneWidth::I64 => Datum::from_i64(op(old.as_i64(), d.as_i64())),
                        _ => unreachable!(),
                    }
                }
                _ => unreachable!("integer kinds use reference_fold"),
            };
        }
    }
    pgs
}

// Byte-parity assertion: datum words and flags bit-identical (avg compares
// the transarray payload; the pointers differ by construction).
fn assert_parity(plan: &LanePlan<'_>, got: &[AggPerGroup], want: &[AggPerGroup]) {
    for t in plan.trans.iter() {
        let (g, w) = (&got[t.transno as usize], &want[t.transno as usize]);
        assert_eq!(
            (g.trans_value_is_null, g.no_trans_value),
            (w.trans_value_is_null, w.no_trans_value),
            "flags for transno {}",
            t.transno
        );
        if t.kind == LaneKind::AvgAccum {
            assert_eq!(
                read_transarray(g),
                read_transarray(w),
                "transarray for transno {}",
                t.transno
            );
        } else if t.kind == LaneKind::Int128AvgAccum {
            // State pointers differ by construction; the payload (and the
            // allocated-vs-NULL distinction, asserted via the flags above)
            // is the parity surface.
            if !g.trans_value_is_null {
                assert_eq!(
                    read_int128_state(g),
                    read_int128_state(w),
                    "int128 state for transno {}",
                    t.transno
                );
            }
        } else if !g.trans_value_is_null {
            assert_eq!(
                g.trans_value.as_i64(),
                w.trans_value.as_i64(),
                "datum for transno {}",
                t.transno
            );
        }
    }
}

fn run_fold(
    mcx: Mcx<'static>,
    specs: &[AggTransSpec<'_, 'static>],
    widths: &[i16],
    data: &[Vec<Option<i64>>],
    sel: impl Fn(usize) -> bool + Copy,
) -> (LanePlan<'static>, Vec<AggPerGroup>) {
    let plan = classify(mcx, specs).expect("plan admits");
    let cols = TestCols::new(widths, data);
    let rows = selmask(data.len(), sel);
    let mut pgs = pergroups_for(mcx, &plan, specs.len());
    // SAFETY: pgs covers every transno; lanes cover every plan col/row.
    unsafe {
        fold_batch(
            &plan,
            &cols,
            &rows,
            data.len(),
            NonNull::new(pgs.as_mut_ptr()).unwrap(),
            mcx,
        )
        .expect("fold");
    }
    let want = reference_fold(mcx, &plan, data, sel, specs.len());
    assert_parity(&plan, &pgs, &want);
    (plan, pgs)
}

// Datum-lane sibling of run_fold: fold_batch vs the datum reference, byte
// parity asserted (float datums compare as raw bits through as_i64).
fn run_fold_datum(
    mcx: Mcx<'static>,
    specs: &[AggTransSpec<'_, 'static>],
    ncols: usize,
    data: &[Vec<Option<Datum>>],
    sel: impl Fn(usize) -> bool + Copy,
) -> (LanePlan<'static>, Vec<AggPerGroup>) {
    let plan = classify(mcx, specs).expect("plan admits");
    let cols = TestCols::from_datum_rows(ncols, data);
    let rows = selmask(data.len(), sel);
    let mut pgs = pergroups_for(mcx, &plan, specs.len());
    // SAFETY: pgs covers every transno; lanes cover every plan col/row.
    unsafe {
        fold_batch(&plan, &cols, &rows, data.len(), NonNull::new(pgs.as_mut_ptr()).unwrap(), mcx)
            .expect("fold");
    }
    let want = reference_fold_datum(mcx, &plan, data, sel, specs.len());
    assert_parity(&plan, &pgs, &want);
    (plan, pgs)
}

// ---- layout + proofs ----

// lanetrans-compact: the per-lane descriptor must stay off the wide-fold
// regression path (the multi-battery SUM(x op k) shape folds 90 of these).
#[test]
fn lanetrans_layout_is_compact() {
    assert!(core::mem::size_of::<LaneTrans>() <= 24);
}

// The safe interval is EXACT: v inside evaluates in int4, the first value
// outside overflows — demote-on-fail must be equivalent to "C would raise".
#[test]
fn safe_interval_is_exact() {
    let fits = |v: i64, addend: i64, mulk: i64| -> bool {
        let r = v * mulk + addend;
        i32::MIN as i64 <= r && r <= i32::MAX as i64
    };
    for &(addend, mulk) in
        &[(5i64, 1i64), (-5, 1), (0, 3), (0, -3), (7, 1), (1 << 20, 1), (0, 65535), (-1, -1)]
    {
        let (lo, hi) = safe_interval(addend, mulk, 1);
        assert!(lo <= hi, "nonempty for ({addend},{mulk})");
        assert!(fits(lo, addend, mulk) && fits(hi, addend, mulk), "({addend},{mulk}) inside");
        assert!(
            !fits(lo - 1, addend, mulk) && !fits(hi + 1, addend, mulk),
            "({addend},{mulk}) boundary exact"
        );
    }
    // divk-only transforms never raise (int2/int4 / nonzero-const fits int4).
    assert_eq!(safe_interval(0, 1, 7), (i64::MIN, i64::MAX));
    // mulk == 0 collapses to the constant addend: safe iff addend fits.
    assert_eq!(safe_interval(0, 0, 1), (i64::MIN, i64::MAX));
    let (lo, hi) = safe_interval(i32::MAX as i64 + 1, 0, 1);
    assert!(lo > hi, "constant overflow admits nothing");
}

#[test]
fn type_proof_tiers() {
    // int2 + small const: every 2^15-bounded input lands in int4.
    assert!(type_proof(LaneWidth::I16, 5, 1, 1));
    // int2 * 65536 fills int4 EXACTLY at the rails (-2^31 .. 2^31 - 65536):
    // still type-provable; one more multiplier step is not.
    assert!(type_proof(LaneWidth::I16, 0, 65536, 1));
    assert!(!type_proof(LaneWidth::I16, 0, 65537, 1));
    // ... and nudging the low rail past -2^31 (v = -32768, addend = -1) isn't.
    assert!(!type_proof(LaneWidth::I16, -1, 65536, 1));
    // int4 + nonzero const can overflow at the type rails.
    assert!(!type_proof(LaneWidth::I32, 1, 1, 1));
    // int4 identity and division shapes are always safe.
    assert!(type_proof(LaneWidth::I32, 0, 1, 1));
    assert!(type_proof(LaneWidth::I32, 0, 1, 9));
}

// ---- classifier admission / refusal ----

#[test]
fn classify_admission_matrix() {
    let mcx = leaked_mcx();
    let count_star_args = NodeList::nil();
    let a1 = arg_list(mcx, mk_var(mcx, 1, INT4OID));
    let a2 = arg_list(mcx, mk_var(mcx, 2, INT2OID));
    let a3 = arg_list(mcx, mk_var(mcx, 3, INT8OID));
    let a4 = arg_list(mcx, mk_var(mcx, 4, ::types_core::catalog::DATEOID));
    let a5 = arg_list(mcx, mk_var(mcx, 5, ::types_core::catalog::TIMESTAMPOID));
    let a6 = arg_list(mcx, mk_var(mcx, 6, ::types_core::catalog::TIMESTAMPTZOID));
    let cases: Vec<(Oid, bool, &NodeList<'static>, LaneKind, LaneWidth)> = vec![
        (1219, false, &count_star_args, LaneKind::CountStar, LaneWidth::I64), // count(*)
        (2804, false, &a1, LaneKind::CountAny, LaneWidth::I64),               // count(v)
        (1841, true, &a1, LaneKind::Sum, LaneWidth::I32),                     // sum(int4)
        (1840, true, &a2, LaneKind::Sum, LaneWidth::I16),                     // sum(int2)
        (1963, false, &a1, LaneKind::AvgAccum, LaneWidth::I32),               // avg(int4)
        (1962, false, &a2, LaneKind::AvgAccum, LaneWidth::I16),               // avg(int2)
        (2746, true, &a3, LaneKind::Int128AvgAccum, LaneWidth::I64),          // sum/avg(int8)
        (768, false, &a1, LaneKind::Max, LaneWidth::I32),
        (769, false, &a1, LaneKind::Min, LaneWidth::I32),
        (770, false, &a2, LaneKind::Max, LaneWidth::I16),
        (771, false, &a2, LaneKind::Min, LaneWidth::I16),
        (1236, false, &a3, LaneKind::Max, LaneWidth::I64),
        (1237, false, &a3, LaneKind::Min, LaneWidth::I64),
        (1138, false, &a4, LaneKind::Max, LaneWidth::I32),
        (1139, false, &a4, LaneKind::Min, LaneWidth::I32),
        (2036, false, &a5, LaneKind::Max, LaneWidth::I64),
        (2035, false, &a5, LaneKind::Min, LaneWidth::I64),
        (1196, false, &a6, LaneKind::Max, LaneWidth::I64),
        (1195, false, &a6, LaneKind::Min, LaneWidth::I64),
    ];
    for (i, (oid, init_null, args, kind, width)) in cases.iter().enumerate() {
        let spec = mk_spec(*oid, *init_null, args);
        let (t, g, _) = classify_trans(&spec, i).unwrap_or_else(|| panic!("case {i} admits"));
        assert_eq!(t.kind, *kind, "case {i}");
        assert_eq!(t.width, *width, "case {i}");
        assert_eq!(t.res_width, *width, "bare Var stores at lane width, case {i}");
        assert_eq!((t.addend, t.mulk, t.divk), (0, 1, 1), "case {i}");
        assert!(g.is_none(), "bare Var carries no guard, case {i}");
    }
}

#[test]
fn classify_affine_opexprs() {
    let mcx = leaked_mcx();
    // (var op const) and the commuted (const op var) forms, with expected
    // (addend, mulk, divk).
    let cases: Vec<(Node<'static>, (i32, i32, i32))> = vec![
        (mk_int_op(mcx, 1, INT2OID, 5, 178, true), (5, 1, 1)),    // v + 5
        (mk_int_op(mcx, 1, INT2OID, 5, 179, false), (5, 1, 1)),   // 5 + v
        (mk_int_op(mcx, 1, INT2OID, 5, 182, true), (-5, 1, 1)),   // v - 5
        (mk_int_op(mcx, 1, INT2OID, 5, 183, false), (5, -1, 1)),  // 5 - v
        (mk_int_op(mcx, 1, INT2OID, 3, 170, true), (0, 3, 1)),    // v * 3
        (mk_int_op(mcx, 1, INT2OID, 3, 171, false), (0, 3, 1)),   // 3 * v
        (mk_int_op(mcx, 1, INT2OID, 7, 172, true), (0, 1, 7)),    // v / 7
        (mk_int_op(mcx, 2, INT4OID, 9, 177, true), (9, 1, 1)),    // int4 v + 9
        (mk_int_op(mcx, 2, INT4OID, 9, 181, true), (-9, 1, 1)),   // int4 v - 9
        (mk_int_op(mcx, 2, INT4OID, 9, 141, true), (0, 9, 1)),    // int4 v * 9
    ];
    for (i, (expr, coeffs)) in cases.iter().enumerate() {
        let args = arg_list(mcx, *expr);
        let spec = mk_spec(1841, true, &args); // int4_sum
        let (t, _, _) = classify_trans(&spec, 0).unwrap_or_else(|| panic!("case {i} admits"));
        assert_eq!((t.addend, t.mulk, t.divk), *coeffs, "case {i}");
        assert_eq!(t.res_width, LaneWidth::I32, "OpExpr result is int4, case {i}");
    }
    // int2 admissions are TYPE-proven (no guard); int4 + const needs a guard.
    let args = arg_list(mcx, mk_int_op(mcx, 1, INT2OID, 5, 178, true));
    let (_, g, _) = classify_trans(&mk_spec(1841, true, &args), 0).unwrap();
    assert!(g.is_none(), "int2+5 is type-proven");
    let args = arg_list(mcx, mk_int_op(mcx, 2, INT4OID, 5, 177, true));
    let (_, g, _) = classify_trans(&mk_spec(1841, true, &args), 0).unwrap();
    let g = g.expect("int4+5 carries a data guard");
    assert_eq!((g.lo, g.hi), (i32::MIN as i64 - 5, i32::MAX as i64 - 5));
}

#[test]
fn classify_refusals() {
    let mcx = leaked_mcx();
    let a1 = arg_list(mcx, mk_var(mcx, 1, INT4OID));
    // Shape gates: combine / aggfilter / ordered flag off the fold.
    let mut spec = mk_spec(1841, true, &a1);
    spec.combine = true;
    assert!(classify_trans(&spec, 0).is_none(), "combine refused");
    let mut spec = mk_spec(1841, true, &a1);
    spec.aggfilter = Some(mk_var(mcx, 1, INT4OID));
    assert!(classify_trans(&spec, 0).is_none(), "aggfilter refused");
    // Initval polarity is part of the whitelist contract.
    assert!(classify_trans(&mk_spec(1841, false, &a1), 0).is_none(), "sum non-null init");
    assert!(classify_trans(&mk_spec(1963, true, &a1), 0).is_none(), "avg null init");
    // int8_avg_accum: INTERNAL transtype means a NULL catalog initval; a
    // non-null initval is not the whitelisted shape.
    let a8iv = arg_list(mcx, mk_var(mcx, 1, INT8OID));
    assert!(classify_trans(&mk_spec(2746, false, &a8iv), 0).is_none(), "int8 sum non-null init");
    // ... and only a bare int8 Var admits: wrong Var type / any OpExpr
    // (int8pl has no affine admission) stay on the per-row program.
    assert!(classify_trans(&mk_spec(2746, true, &a1), 0).is_none(), "int4 var for 2746");
    let a8op = arg_list(mcx, mk_int_op(mcx, 1, INT8OID, 5, 463, true)); // v + 5 (int8pl)
    assert!(classify_trans(&mk_spec(2746, true, &a8op), 0).is_none(), "int8 OpExpr refused");
    assert!(classify_trans(&mk_spec(1219, true, &NodeList::nil()), 0).is_none());
    // Unknown transfn.
    assert!(classify_trans(&mk_spec(9999, true, &a1), 0).is_none());
    // int42div (const / var) is not a v-monotone affine transform.
    let args = arg_list(mcx, mk_int_op(mcx, 1, INT2OID, 1000, F_INT42DIV, false));
    assert!(classify_trans(&mk_spec(1841, true, &args), 0).is_none(), "const/var refused");
    // Division by a zero const must keep C's per-row raise.
    let args = arg_list(mcx, mk_int_op(mcx, 1, INT2OID, 0, 172, true));
    assert!(classify_trans(&mk_spec(1841, true, &args), 0).is_none(), "div-by-zero refused");
    // NULL const: strict op returns NULL per row, not an affine fold.
    let var = mk_var(mcx, 1, INT2OID);
    let nullk = Node::mk_const(mcx, INT4OID, -1, 0, 4, Datum::null(), true, true).unwrap();
    let mut op = Node::build::<OpExpr>(mcx).unwrap();
    op.opfuncid = 178;
    op.opresulttype = INT4OID;
    op.args = NodeList::make2(mcx, var, nullk).unwrap();
    let args = arg_list(mcx, op.seal());
    assert!(classify_trans(&mk_spec(1841, true, &args), 0).is_none(), "null const refused");
    // Wrong Var type / inner-side Var.
    let args = arg_list(mcx, mk_var(mcx, 1, INT8OID));
    assert!(classify_trans(&mk_spec(1841, true, &args), 0).is_none(), "int8 var for int4_sum");
    let inner = Node::mk_var(mcx, -1, 1, INT4OID, -1, 0, 0).unwrap();
    let args = arg_list(mcx, inner);
    assert!(classify_trans(&mk_spec(1841, true, &args), 0).is_none(), "INNER_VAR refused");
}

#[test]
fn classify_splits_residual_per_transition() {
    let mcx = leaked_mcx();
    let a_ok = arg_list(mcx, mk_var(mcx, 1, INT4OID));
    let a_bad = arg_list(mcx, mk_int_op(mcx, 2, INT2OID, 1000, F_INT42DIV, false));
    let specs = [mk_spec(1841, true, &a_ok), mk_spec(1841, true, &a_bad)];
    let plan = classify(mcx, &specs).expect("one transition admits");
    assert_eq!(plan.trans.len(), 1);
    assert_eq!(plan.trans[0].transno, 0);
    assert_eq!(&plan.resid[..], &[1]);
    assert_eq!(&plan.cols[..], &[0]);
    assert!(!plan.guarded);
    // All-refused: no plan.
    let specs = [mk_spec(1841, true, &a_bad)];
    assert!(classify(mcx, &specs).is_none());
}

// ---- fold parity (the ported byte-parity contract) ----

#[test]
fn fold_sum_int4_parity() {
    let mcx = leaked_mcx();
    let args = arg_list(mcx, mk_var(mcx, 1, INT4OID));
    let specs = [mk_spec(1841, true, &args)];
    let n = 200;
    let data: Vec<Vec<Option<i64>>> = (0..n)
        .map(|i| vec![if i % 7 == 0 { None } else { Some(i as i64 - 100) }])
        .collect();
    let (_, pgs) = run_fold(mcx, &specs, &[4], &data, |i| i % 2 == 1);
    let expected: i64 =
        (0..n).filter(|i| i % 2 == 1 && i % 7 != 0).map(|i| i as i64 - 100).sum();
    assert_eq!(pgs[0].trans_value.as_i64(), expected);
    assert!(!pgs[0].trans_value_is_null);
}

#[test]
fn fold_sum_stays_null_when_nothing_selected() {
    let mcx = leaked_mcx();
    let args = arg_list(mcx, mk_var(mcx, 1, INT4OID));
    let specs = [mk_spec(1841, true, &args)];
    let data: Vec<Vec<Option<i64>>> = (0..8).map(|_| vec![None]).collect();
    let (_, pgs) = run_fold(mcx, &specs, &[4], &data, |_| true);
    assert!(pgs[0].trans_value_is_null, "all-NULL input leaves sum NULL");
    // Min/max with an empty selection stays in the no-trans-value state.
    let specs = [mk_spec(769, false, &args)];
    let data: Vec<Vec<Option<i64>>> = (0..8).map(|i| vec![Some(i as i64)]).collect();
    let (_, pgs) = run_fold(mcx, &specs, &[4], &data, |_| false);
    assert!(pgs[0].no_trans_value && pgs[0].trans_value_is_null);
}

#[test]
fn fold_int2_addend_expr_parity() {
    let mcx = leaked_mcx();
    let args = arg_list(mcx, mk_int_op(mcx, 1, INT2OID, 5, 178, true));
    let specs = [mk_spec(1841, true, &args)];
    let data: Vec<Vec<Option<i64>>> = (0..150).map(|i| vec![Some(i as i64 - 50)]).collect();
    let (_, pgs) = run_fold(mcx, &specs, &[2], &data, |i| i % 3 == 0);
    let expected: i64 = (0..150i64).filter(|i| i % 3 == 0).map(|i| i - 50 + 5).sum();
    assert_eq!(pgs[0].trans_value.as_i64(), expected);
}

// mul/div transforms fold per row (no hoisted form); parity across negative
// values covers C's trunc-toward-zero division.
#[test]
fn fold_muldiv_transform_parity() {
    let mcx = leaked_mcx();
    let a_mul = arg_list(mcx, mk_int_op(mcx, 1, INT2OID, 331, 170, true));
    let a_div = arg_list(mcx, mk_int_op(mcx, 1, INT2OID, 7, 172, true));
    let a_neg = arg_list(mcx, mk_int_op(mcx, 1, INT2OID, 9, 183, false)); // 9 - v
    let specs = [
        mk_spec(1841, true, &a_mul),
        mk_spec(1841, true, &a_div),
        mk_spec(1841, true, &a_neg),
    ];
    let data: Vec<Vec<Option<i64>>> = (0..300)
        .map(|i| {
            vec![if i % 11 == 0 { None } else { Some(((i * 331) % 65536) as i64 - 32768) }]
        })
        .collect();
    let (plan, pgs) = run_fold(mcx, &specs, &[2], &data, |i| i % 2 == 0);
    let vals =
        || (0..300).filter(|i| i % 2 == 0 && i % 11 != 0).map(|i| ((i * 331) % 65536) as i64 - 32768);
    assert_eq!(pgs[0].trans_value.as_i64(), vals().map(|v| v * 331).sum::<i64>());
    assert_eq!(pgs[1].trans_value.as_i64(), vals().map(|v| v / 7).sum::<i64>());
    assert_eq!(pgs[2].trans_value.as_i64(), vals().map(|v| 9 - v).sum::<i64>());
    // Different divk: the two sums must NOT share a SumBase cluster with the
    // div member (col, divk keys), the mul/neg pair does cluster.
    assert_eq!(plan.cse.len(), 1);
}

#[test]
fn fold_all_kinds_parity_no_cse() {
    let mcx = leaked_mcx();
    // One transition per column: no CSE grouping, every per-trans kernel runs.
    let a0 = NodeList::nil();
    let a1 = arg_list(mcx, mk_var(mcx, 1, INT4OID));
    let a2 = arg_list(mcx, mk_var(mcx, 2, INT2OID));
    let a3 = arg_list(mcx, mk_var(mcx, 3, INT8OID));
    let a4 = arg_list(mcx, mk_var(mcx, 4, INT4OID));
    let a5 = arg_list(mcx, mk_var(mcx, 5, INT2OID));
    let specs = [
        mk_spec(1219, false, &a0), // count(*)
        mk_spec(2804, false, &a1), // count(c0)
        mk_spec(1840, true, &a2),  // sum(int2 c1)
        mk_spec(1236, false, &a3), // max(int8 c2)
        mk_spec(1963, false, &a4), // avg(int4 c3)
        mk_spec(771, false, &a5),  // min(int2 c4)
    ];
    let data: Vec<Vec<Option<i64>>> = (0..190)
        .map(|i| {
            let m = |k: usize, v: i64| if i % k == 0 { None } else { Some(v) };
            vec![
                m(3, i as i64),
                m(5, (i as i64 * 7919) % 32768 - 16384),
                m(7, (i as i64 - 95) * 1_000_000_007),
                m(2, i as i64 * 3 - 200),
                m(11, 20000 - i as i64 * 211),
            ]
        })
        .collect();
    let (plan, pgs) = run_fold(mcx, &specs, &[4, 2, 8, 4, 2], &data, |i| i % 4 != 1);
    assert_eq!(plan.cse.len(), 0, "distinct cols form no CSE groups");
    assert_eq!(plan.trans.len(), 6);
    // Spot-check count(*) counts selected rows regardless of NULLs.
    let nsel = (0..190).filter(|i| i % 4 != 1).count() as i64;
    assert_eq!(pgs[0].trans_value.as_i64(), nsel);
    // avg transarray carries {count, sum} of the selected non-null rows.
    let want: Vec<i64> = (0..190)
        .filter(|i| i % 4 != 1 && i % 2 != 0)
        .map(|i| i as i64 * 3 - 200)
        .collect();
    assert_eq!(read_transarray(&pgs[4]), (want.len() as i64, want.iter().sum()));
}

// The agg-rewrite-cse derivation: sum/avg/count over one column fold through
// a single base pass, bit-equal to the independent per-trans folds.
#[test]
fn fold_cse_sumbase_and_minmax_parity() {
    let mcx = leaked_mcx();
    let a_sum = arg_list(mcx, mk_var(mcx, 1, INT2OID));
    let a_sum5 = arg_list(mcx, mk_int_op(mcx, 1, INT2OID, 5, 178, true));
    let a_mul = arg_list(mcx, mk_int_op(mcx, 1, INT2OID, 3, 170, true));
    let a_cnt = arg_list(mcx, mk_var(mcx, 1, INT2OID));
    let a_avg = arg_list(mcx, mk_var(mcx, 1, INT2OID));
    let a_min1 = arg_list(mcx, mk_var(mcx, 1, INT2OID));
    let a_min2 = arg_list(mcx, mk_var(mcx, 1, INT2OID));
    // The OpExpr admissions carry int4 results, so their aggregates bind
    // int4_sum (1841); the bare int2 Var stays on int2_sum (1840).
    let specs = [
        mk_spec(1840, true, &a_sum),   // sum(v)
        mk_spec(1841, true, &a_sum5),  // sum(v+5)
        mk_spec(1841, true, &a_mul),   // sum(v*3)
        mk_spec(2804, false, &a_cnt),  // count(v)
        mk_spec(1962, false, &a_avg),  // avg(v)
        mk_spec(771, false, &a_min1),  // min(v)
        mk_spec(771, false, &a_min2),  // min(v) duplicate
    ];
    let data: Vec<Vec<Option<i64>>> = (0..250)
        .map(|i| vec![if i % 9 == 0 { None } else { Some((i as i64 * 613) % 32768 - 16384) }])
        .collect();
    let (plan, pgs) = run_fold(mcx, &specs, &[2], &data, |i| i % 3 != 2);
    // One SumBase cluster (5 members: 3 sums + count + avg) and one MinMax
    // pair; every transition is a member.
    assert_eq!(plan.cse.len(), 2);
    assert!(plan.cse_skip.iter().all(|&s| s));
    let vals = || {
        (0..250)
            .filter(|i| i % 3 != 2 && i % 9 != 0)
            .map(|i| (i as i64 * 613) % 32768 - 16384)
    };
    assert_eq!(pgs[0].trans_value.as_i64(), vals().sum::<i64>());
    assert_eq!(pgs[1].trans_value.as_i64(), vals().map(|v| v + 5).sum::<i64>());
    assert_eq!(pgs[2].trans_value.as_i64(), vals().map(|v| v * 3).sum::<i64>());
    assert_eq!(pgs[3].trans_value.as_i64(), vals().count() as i64);
    assert_eq!(read_transarray(&pgs[4]), (vals().count() as i64, vals().sum()));
    let mn = vals().min().unwrap();
    assert_eq!(pgs[5].trans_value.as_i16() as i64, mn);
    assert_eq!(pgs[6].trans_value.as_i16() as i64, mn);
}

// Min/Max must store at res_width: an in-range int4 result from an int2-Var
// OpExpr would truncate through from_i16.
#[test]
fn minmax_stores_at_result_width() {
    let mcx = leaked_mcx();
    let args = arg_list(mcx, mk_int_op(mcx, 1, INT2OID, 10000, 178, true)); // v + 10000
    let specs = [mk_spec(768, false, &args)]; // max(int4)
    let data: Vec<Vec<Option<i64>>> = vec![vec![Some(30000)], vec![Some(32767)]];
    let (plan, pgs) = run_fold(mcx, &specs, &[2], &data, |_| true);
    assert_eq!(plan.trans[0].res_width, LaneWidth::I32);
    assert_eq!(pgs[0].trans_value.as_i32(), 42767, "no int2 truncation");
}

// ---- guards (DATA-level proof tier) ----

#[test]
fn guard_zone_data_and_demote() {
    let mcx = leaked_mcx();
    // int2 * 65536: safe interval [ceil(-2^31/65536), floor((2^31-1)/65536)]
    // = [-32768, 32767]... exactly the type range, so use * 100000 instead:
    // [-21474, 21474] strictly inside int2 — a real DATA guard.
    let args = arg_list(mcx, mk_int_op(mcx, 1, INT2OID, 100_000, 170, true));
    let specs = [mk_spec(1841, true, &args)];
    let plan = classify(mcx, &specs).expect("admits with a guard");
    assert!(plan.guarded);
    assert_eq!(plan.guards.len(), 1);
    assert_eq!((plan.guards[0].lo, plan.guards[0].hi), (-21474, 21474));

    let ok_data: Vec<Vec<Option<i64>>> =
        (0..64).map(|i| vec![Some(i as i64 * 337 % 21474 - 10737)]).collect();
    let bad_data: Vec<Vec<Option<i64>>> = (0..64)
        .map(|i| vec![Some(if i == 40 { 21475 } else { i as i64 })])
        .collect();
    let rows = selmask(64, |_| true);

    // Zone tier: granule bounds inside the interval prove without a lane pass.
    let cols = TestCols::new(&[2], &ok_data);
    assert_eq!(
        unsafe { check_guards(&plan, &cols, &rows, |_| Some((-21000, 21000))) },
        GuardCheck::Pass { zone: true, data: false }
    );
    // Data tier: no zone answer, exact lane pass proves.
    assert_eq!(
        unsafe { check_guards(&plan, &cols, &rows, |_| None) },
        GuardCheck::Pass { zone: false, data: true }
    );
    // Wide zone bounds fall through to the lane pass, which still proves.
    assert_eq!(
        unsafe { check_guards(&plan, &cols, &rows, |_| Some((-30000, 30000))) },
        GuardCheck::Pass { zone: false, data: true }
    );
    // A single out-of-interval selected value demotes the whole batch.
    let cols = TestCols::new(&[2], &bad_data);
    assert_eq!(unsafe { check_guards(&plan, &cols, &rows, |_| None) }, GuardCheck::Demote);
    // ... but not when the offending row is unselected (the proof covers
    // exactly the rows the fold would touch).
    let rows_skip = selmask(64, |i| i != 40);
    assert_eq!(
        unsafe { check_guards(&plan, &cols, &rows_skip, |_| None) },
        GuardCheck::Pass { zone: false, data: true }
    );
    // NULL rows never fail the proof.
    let null_data: Vec<Vec<Option<i64>>> =
        (0..64).map(|i| vec![if i == 40 { None } else { Some(0) }]).collect();
    let cols = TestCols::new(&[2], &null_data);
    assert_eq!(
        unsafe { check_guards(&plan, &cols, &rows, |_| None) },
        GuardCheck::Pass { zone: false, data: true }
    );
    // Guard interval is exact: the boundary value itself passes and folds to
    // the exact int4 rail.
    let rail: Vec<Vec<Option<i64>>> = vec![vec![Some(21474)]];
    let cols = TestCols::new(&[2], &rail);
    let rows1 = selmask(1, |_| true);
    assert_eq!(
        unsafe { check_guards(&plan, &cols, &rows1, |_| None) },
        GuardCheck::Pass { zone: false, data: true }
    );
    let mut pgs = pergroups_for(mcx, &plan, 1);
    // SAFETY: pgs covers transno 0; lanes cover the one row.
    unsafe { fold_batch(&plan, &cols, &rows1, 1, NonNull::new(pgs.as_mut_ptr()).unwrap(), mcx).expect("fold") };
    assert_eq!(pgs[0].trans_value.as_i64(), 21474 * 100_000);
}

// ---- grouped fold ----

#[test]
fn fold_rows_grouped_parity() {
    let mcx = leaked_mcx();
    let a_sum = arg_list(mcx, mk_var(mcx, 1, INT4OID));
    let a_min = arg_list(mcx, mk_var(mcx, 2, INT2OID));
    let a_avg = arg_list(mcx, mk_var(mcx, 1, INT4OID));
    let a0 = NodeList::nil();
    let specs = [
        mk_spec(1219, false, &a0),  // count(*)
        mk_spec(1841, true, &a_sum),
        mk_spec(771, false, &a_min),
        mk_spec(1963, false, &a_avg),
    ];
    let plan = classify(mcx, &specs).expect("admits");
    let n = 160usize;
    let ngroups = 3usize;
    let data: Vec<Vec<Option<i64>>> = (0..n)
        .map(|i| {
            vec![
                if i % 6 == 0 { None } else { Some(i as i64 * 17 - 900) },
                if i % 4 == 3 { None } else { Some((i as i64 * 977) % 30000 - 15000) },
            ]
        })
        .collect();
    let cols = TestCols::new(&[4, 2], &data);
    // Rows route to groups by i % 3; every row selected.
    let mut group_pgs: Vec<Vec<AggPerGroup>> =
        (0..ngroups).map(|_| pergroups_for(mcx, &plan, specs.len())).collect();
    let idxs: Vec<u32> = (0..n as u32).collect();
    let groups: Vec<NonNull<AggPerGroup>> = (0..n)
        .map(|i| NonNull::new(group_pgs[i % ngroups].as_mut_ptr()).unwrap())
        .collect();
    // SAFETY: each group's pergroup array covers every transno; lanes cover
    // every row; arrays are not moved while the pointers live.
    unsafe { fold_rows_grouped(&plan, &cols, &idxs, &groups, mcx).expect("fold") };
    // Per-group reference over the group's own row subset.
    for g in 0..ngroups {
        let gdata: Vec<Vec<Option<i64>>> =
            (0..n).filter(|i| i % ngroups == g).map(|i| data[i].clone()).collect();
        let want = reference_fold(mcx, &plan, &gdata, |_| true, specs.len());
        assert_parity(&plan, &group_pgs[g], &want);
    }
}

// avgpack: the PACKED grouped fold ([count, sum] inline in the AvgAccum
// pergroup slot, avgpack_mask bit set) accumulates exactly the transarray
// arm's {count, sum} over the same rows, and leaves other transnos'
// representations untouched.
#[test]
fn fold_rows_grouped_avgpack_parity() {
    let mcx = leaked_mcx();
    let a_sum = arg_list(mcx, mk_var(mcx, 1, INT4OID));
    let a_avg = arg_list(mcx, mk_var(mcx, 1, INT4OID));
    let a0 = NodeList::nil();
    let specs = [
        mk_spec(1219, false, &a0), // count(*)
        mk_spec(1841, true, &a_sum),
        mk_spec(1963, false, &a_avg),
    ];
    let plan = classify(mcx, &specs).expect("admits");
    let avg_transno = plan
        .trans
        .iter()
        .find(|t| t.kind == LaneKind::AvgAccum)
        .expect("avg admitted")
        .transno;
    let n = 160usize;
    let ngroups = 3usize;
    let data: Vec<Vec<Option<i64>>> = (0..n)
        .map(|i| vec![if i % 6 == 0 { None } else { Some(i as i64 * 17 - 900) }])
        .collect();
    let cols = TestCols::new(&[4], &data);
    let idxs: Vec<u32> = (0..n as u32).collect();

    // Reference arm: the transarray representation.
    let mut ref_pgs: Vec<Vec<AggPerGroup>> =
        (0..ngroups).map(|_| pergroups_for(mcx, &plan, specs.len())).collect();
    let ref_groups: Vec<NonNull<AggPerGroup>> = (0..n)
        .map(|i| NonNull::new(ref_pgs[i % ngroups].as_mut_ptr()).unwrap())
        .collect();
    // SAFETY: as fold_rows_grouped_parity.
    unsafe { fold_rows_grouped(&plan, &cols, &idxs, &ref_groups, mcx).expect("fold") };

    // Packed arm: the AvgAccum slot seeded [0, 0] (== the {0,0} initval).
    let mut pk_pgs: Vec<Vec<AggPerGroup>> =
        (0..ngroups).map(|_| pergroups_for(mcx, &plan, specs.len())).collect();
    for pgs in pk_pgs.iter_mut() {
        // SAFETY: the slot is 16 repr(C) bytes, 8-aligned.
        unsafe {
            (&mut pgs[avg_transno as usize] as *mut AggPerGroup)
                .cast::<[i64; 2]>()
                .write([0, 0]);
        }
    }
    let pk_groups: Vec<NonNull<AggPerGroup>> = (0..n)
        .map(|i| NonNull::new(pk_pgs[i % ngroups].as_mut_ptr()).unwrap())
        .collect();
    // SAFETY: as above; the mask names exactly the packed transno.
    unsafe {
        fold_rows_grouped_mm(&plan, &cols, &idxs, &pk_groups, mcx, None, 1u64 << avg_transno, None)
            .expect("packed fold");
    }

    for g in 0..ngroups {
        // Unpacked transarray payload: {count, sum} at +24 of the varlena.
        let rp = ref_pgs[g][avg_transno as usize].trans_value.as_usize() as *const u8;
        // SAFETY: live transarray installed by pergroups_for.
        let want =
            unsafe { [rp.add(24).cast::<i64>().read(), rp.add(32).cast::<i64>().read()] };
        // SAFETY: the packed slot holds two i64 words.
        let got = unsafe {
            (&pk_pgs[g][avg_transno as usize] as *const AggPerGroup).cast::<[i64; 2]>().read()
        };
        assert_eq!(got, want, "group {g}");
        // Other transnos are representation-untouched by the mask.
        assert_eq!(pk_pgs[g][0].trans_value.as_i64(), ref_pgs[g][0].trans_value.as_i64());
    }
}

// ---- fold-coverage tier 2: float MIN/MAX, bool_and/or, bit_and/or ----

const F32V: Oid = ::types_core::catalog::FLOAT4OID;
const F64V: Oid = ::types_core::catalog::FLOAT8OID;
const BOOLV: Oid = ::types_core::catalog::BOOLOID;

#[test]
fn classify_foldcov_admission() {
    let mcx = leaked_mcx();
    let af4 = arg_list(mcx, mk_var(mcx, 1, F32V));
    let af8 = arg_list(mcx, mk_var(mcx, 2, F64V));
    let ab = arg_list(mcx, mk_var(mcx, 3, BOOLV));
    let a2 = arg_list(mcx, mk_var(mcx, 4, INT2OID));
    let a4 = arg_list(mcx, mk_var(mcx, 5, INT4OID));
    let a8 = arg_list(mcx, mk_var(mcx, 6, INT8OID));
    // (transfn oid, args, kind, width) — the tier-2 admission table. All are
    // strict + NULL-init in pg_aggregate; all TYPE-level safe (no guard).
    let cases: Vec<(Oid, &NodeList<'static>, LaneKind, LaneWidth)> = vec![
        (209, &af4, LaneKind::FMax, LaneWidth::F32),  // max(float4)
        (211, &af4, LaneKind::FMin, LaneWidth::F32),  // min(float4)
        (223, &af8, LaneKind::FMax, LaneWidth::F64),  // max(float8)
        (224, &af8, LaneKind::FMin, LaneWidth::F64),  // min(float8)
        (2515, &ab, LaneKind::BoolAnd, LaneWidth::Bool), // bool_and / every
        (2516, &ab, LaneKind::BoolOr, LaneWidth::Bool),  // bool_or
        (1892, &a2, LaneKind::BitAnd, LaneWidth::I16),   // bit_and(int2)
        (1893, &a2, LaneKind::BitOr, LaneWidth::I16),    // bit_or(int2)
        (1898, &a4, LaneKind::BitAnd, LaneWidth::I32),   // bit_and(int4)
        (1899, &a4, LaneKind::BitOr, LaneWidth::I32),    // bit_or(int4)
        (1904, &a8, LaneKind::BitAnd, LaneWidth::I64),   // bit_and(int8)
        (1905, &a8, LaneKind::BitOr, LaneWidth::I64),    // bit_or(int8)
    ];
    for (i, (oid, args, kind, width)) in cases.iter().enumerate() {
        let spec = mk_spec(*oid, true, args);
        let (t, g, _) = classify_trans(&spec, i).unwrap_or_else(|| panic!("case {i} admits"));
        assert_eq!(t.kind, *kind, "case {i}");
        assert_eq!(t.width, *width, "case {i}");
        assert_eq!(t.res_width, *width, "bare Var stores at lane width, case {i}");
        assert_eq!((t.addend, t.mulk, t.divk), (0, 1, 1), "case {i}");
        assert!(g.is_none(), "tier-2 bare Var carries no guard, case {i}");
    }
}

#[test]
fn classify_foldcov_refusals_and_bit_opexpr() {
    let mcx = leaked_mcx();
    // Wrong Var type refuses.
    let a_f8 = arg_list(mcx, mk_var(mcx, 1, F64V));
    assert!(classify_trans(&mk_spec(211, true, &a_f8), 0).is_none(), "f8 var for float4smaller");
    let a_i4 = arg_list(mcx, mk_var(mcx, 1, INT4OID));
    assert!(classify_trans(&mk_spec(2515, true, &a_i4), 0).is_none(), "int4 var for bool_and");
    assert!(classify_trans(&mk_spec(223, true, &a_i4), 0).is_none(), "int4 var for float8larger");
    // OpExpr args stay refused for floats/bools (bare Var only).
    let a_op = arg_list(mcx, mk_int_op(mcx, 1, INT2OID, 5, 178, true));
    assert!(classify_trans(&mk_spec(223, true, &a_op), 0).is_none(), "OpExpr for float8larger");
    assert!(classify_trans(&mk_spec(2516, true, &a_op), 0).is_none(), "OpExpr for bool_or");
    // ... but int4 bitwise folds admit the affine int4 OpExpr shapes with the
    // same guard tiers as SUM/MIN/MAX (v + 5 over an int4 Var: DATA guard).
    let a_g = arg_list(mcx, mk_int_op(mcx, 2, INT4OID, 5, 177, true));
    let (t, g, _) = classify_trans(&mk_spec(1898, true, &a_g), 0).expect("int4and over v+5 admits");
    assert_eq!(t.kind, LaneKind::BitAnd);
    assert_eq!(t.res_width, LaneWidth::I32);
    let g = g.expect("int4 + const carries a data guard");
    assert_eq!((g.lo, g.hi), (i32::MIN as i64 - 5, i32::MAX as i64 - 5));
    // int2 bitwise folds are bare-Var only (classify_arg's OpExpr path is
    // int4-typed; an int2-width transfn never sees an int4 OpExpr arg).
    assert!(classify_trans(&mk_spec(1892, true, &a_g), 0).is_none());
}

// f64/f32 boundary pools: NaN (two payloads), ±0, ±inf, denormal-scale and
// rail values. The reference replays float.c larger/smaller per row.
fn f64_pool() -> Vec<f64> {
    vec![
        0.0,
        -0.0,
        1.5,
        -1.5,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::NAN,
        f64::from_bits(0x7FF8_0000_0000_0001), // NaN, distinct payload
        f64::MAX,
        f64::MIN,
        1e-300,
        -1e-300,
    ]
}

fn f32_pool() -> Vec<f32> {
    vec![
        0.0,
        -0.0,
        2.25,
        -2.25,
        f32::INFINITY,
        f32::NEG_INFINITY,
        f32::NAN,
        f32::from_bits(0x7FC0_0001), // NaN, distinct payload
        f32::MAX,
        f32::MIN,
        1e-38,
        -1e-38,
    ]
}

#[test]
fn fold_float_minmax_parity() {
    let mcx = leaked_mcx();
    let a_f8 = arg_list(mcx, mk_var(mcx, 1, F64V));
    let a_f4 = arg_list(mcx, mk_var(mcx, 2, F32V));
    let specs = [
        mk_spec(223, true, &a_f8), // max(float8)
        mk_spec(224, true, &a_f8), // min(float8)
        mk_spec(209, true, &a_f4), // max(float4)
        mk_spec(211, true, &a_f4), // min(float4)
    ];
    let (p8, p4) = (f64_pool(), f32_pool());
    let n = 260;
    let data: Vec<Vec<Option<Datum>>> = (0..n)
        .map(|i| {
            vec![
                if i % 5 == 0 { None } else { Some(Datum::from_f64(p8[(i * 7) % p8.len()])) },
                if i % 3 == 2 { None } else { Some(Datum::from_f32(p4[(i * 11) % p4.len()])) },
            ]
        })
        .collect();
    // Several selections: full, odd rows, sparse (NaN-heavy windows shift).
    for sel in [
        (|_: usize| true) as fn(usize) -> bool,
        |i| i % 2 == 1,
        |i| i % 7 == 3,
    ] {
        run_fold_datum(mcx, &specs, 2, &data, sel);
    }
    // All-NULL lane leaves min/max in the strict no-trans-value state.
    let null_data: Vec<Vec<Option<Datum>>> = (0..40).map(|_| vec![None, None]).collect();
    let (_, pgs) = run_fold_datum(mcx, &specs, 2, &null_data, |_| true);
    assert!(pgs.iter().all(|pg| pg.no_trans_value && pg.trans_value_is_null));
}

// The C findings pinned as literal bit patterns: NaN sorts greatest for both
// larger and smaller; every tie (equal values, ±0, NaN/NaN) returns the
// SECOND argument, so the LAST tied row's datum bits survive in row order.
#[test]
fn fold_float_nan_and_signed_zero_semantics() {
    let mcx = leaked_mcx();
    let a_f8 = arg_list(mcx, mk_var(mcx, 1, F64V));
    let d = |v: f64| Some(Datum::from_f64(v));
    let run = |oid: Oid, vals: &[Option<Datum>]| -> Datum {
        let specs = [mk_spec(oid, true, &a_f8)];
        let data: Vec<Vec<Option<Datum>>> = vals.iter().map(|v| vec![*v]).collect();
        let (_, pgs) = run_fold_datum(mcx, &specs, 1, &data, |_| true);
        assert!(!pgs[0].trans_value_is_null);
        pgs[0].trans_value
    };
    let bits = |v: f64| Datum::from_f64(v).as_u64();
    // ±0 ties: the last zero's sign survives, for MIN and MAX alike.
    assert_eq!(run(224, &[d(0.0), d(-0.0)]).as_u64(), bits(-0.0), "min tie keeps last (-0)");
    assert_eq!(run(224, &[d(-0.0), d(0.0)]).as_u64(), bits(0.0), "min tie keeps last (+0)");
    assert_eq!(run(223, &[d(0.0), d(-0.0)]).as_u64(), bits(-0.0), "max tie keeps last (-0)");
    assert_eq!(run(223, &[d(-0.0), d(0.0)]).as_u64(), bits(0.0), "max tie keeps last (+0)");
    // NaN is greater than everything — even +inf — for max AND min.
    assert_eq!(run(223, &[d(f64::NAN), d(f64::INFINITY)]).as_u64(), bits(f64::NAN));
    assert_eq!(run(223, &[d(f64::INFINITY), d(f64::NAN)]).as_u64(), bits(f64::NAN));
    assert_eq!(
        run(224, &[d(f64::NAN), d(f64::NEG_INFINITY)]).as_u64(),
        bits(f64::NEG_INFINITY),
        "min: anything beats NaN"
    );
    assert_eq!(run(224, &[d(1.0), d(f64::NAN)]).as_u64(), bits(1.0), "min keeps non-NaN");
    // NaN/NaN ties: the LAST NaN's payload bits survive.
    let nan_a = f64::from_bits(0x7FF8_0000_0000_0001);
    let nan_b = f64::from_bits(0x7FF8_0000_0000_0002);
    assert_eq!(run(223, &[d(nan_a), d(nan_b)]).as_u64(), nan_b.to_bits());
    assert_eq!(run(224, &[d(nan_b), d(nan_a)]).as_u64(), nan_a.to_bits());
    // -inf/+inf and rails order normally.
    assert_eq!(run(223, &[d(f64::MAX), d(f64::INFINITY), d(1.0)]).as_u64(), bits(f64::INFINITY));
    assert_eq!(run(224, &[d(-1.0), d(f64::NEG_INFINITY), d(f64::MIN)]).as_u64(), bits(f64::NEG_INFINITY));
}

#[test]
fn fold_bool_parity() {
    let mcx = leaked_mcx();
    let ab = arg_list(mcx, mk_var(mcx, 1, BOOLV));
    let specs = [
        mk_spec(2515, true, &ab), // bool_and (and every)
        mk_spec(2516, true, &ab), // bool_or
    ];
    // Density sweep: all-true, all-false, mixed, NULL-heavy.
    let mk = |f: &dyn Fn(usize) -> Option<bool>, n: usize| -> Vec<Vec<Option<Datum>>> {
        (0..n).map(|i| vec![f(i).map(Datum::from_bool)]).collect()
    };
    let pools: Vec<Vec<Vec<Option<Datum>>>> = vec![
        mk(&|_| Some(true), 100),
        mk(&|_| Some(false), 100),
        mk(&|i| Some(i % 13 == 0), 200),
        mk(&|i| if i % 2 == 0 { None } else { Some(i % 3 == 0) }, 200),
        mk(&|_| None, 64),
    ];
    for data in &pools {
        for sel in [(|_: usize| true) as fn(usize) -> bool, |i| i % 4 != 2] {
            let (_, pgs) = run_fold_datum(mcx, &specs, 1, data, sel);
            // Cross-check against the direct definition.
            let vals: Vec<bool> = data
                .iter()
                .enumerate()
                .filter(|(i, _)| sel(*i))
                .filter_map(|(_, r)| r[0].map(|d| d.as_bool()))
                .collect();
            if vals.is_empty() {
                assert!(pgs[0].no_trans_value && pgs[1].no_trans_value);
            } else {
                assert_eq!(pgs[0].trans_value.as_bool(), vals.iter().all(|&v| v));
                assert_eq!(pgs[1].trans_value.as_bool(), vals.iter().any(|&v| v));
            }
        }
    }
}

#[test]
fn fold_bit_parity() {
    let mcx = leaked_mcx();
    let a2 = arg_list(mcx, mk_var(mcx, 1, INT2OID));
    let a4 = arg_list(mcx, mk_var(mcx, 2, INT4OID));
    let a8 = arg_list(mcx, mk_var(mcx, 3, INT8OID));
    let specs = [
        mk_spec(1892, true, &a2), // bit_and(int2)
        mk_spec(1893, true, &a2), // bit_or(int2)
        mk_spec(1898, true, &a4), // bit_and(int4)
        mk_spec(1899, true, &a4), // bit_or(int4)
        mk_spec(1904, true, &a8), // bit_and(int8)
        mk_spec(1905, true, &a8), // bit_or(int8)
    ];
    // Sign-bit-heavy patterns: negatives, alternating masks, rails.
    let p16: Vec<i16> = vec![-1, 0x5A5A_u16 as i16, i16::MIN, 0x00FF, -32000, 21845];
    let p32: Vec<i32> = vec![-1, 0x5A5A_5A5A, i32::MIN, 0x00FF_FF00, -2_000_000_000, 1];
    let p64: Vec<i64> =
        vec![-1, 0x5A5A_5A5A_5A5A_5A5A, i64::MIN, 0x00FF_FF00_0FF0_F00F, -(1 << 62), 3];
    let n = 220;
    let data: Vec<Vec<Option<Datum>>> = (0..n)
        .map(|i| {
            let m = |k: usize| i % k == 0;
            vec![
                if m(4) { None } else { Some(Datum::from_i16(p16[(i * 5) % p16.len()])) },
                if m(6) { None } else { Some(Datum::from_i32(p32[(i * 7) % p32.len()])) },
                if m(9) { None } else { Some(Datum::from_i64(p64[(i * 11) % p64.len()])) },
            ]
        })
        .collect();
    let (_, pgs) = run_fold_datum(mcx, &specs, 3, &data, |i| i % 3 != 1);
    // Direct definitional cross-check on the int8 lane (i64 bit patterns
    // including the sign bit survive exactly).
    let vals = || {
        (0..n).filter(|i| i % 3 != 1 && i % 9 != 0).map(|i| p64[(i * 11) % p64.len()])
    };
    assert_eq!(pgs[4].trans_value.as_i64(), vals().fold(-1i64, |a, v| a & v));
    assert_eq!(pgs[5].trans_value.as_i64(), vals().fold(0i64, |a, v| a | v));
    // int2 lane: result datum is the sign-extended int2 word (from_i16).
    let v16 = || (0..n).filter(|i| i % 3 != 1 && i % 4 != 0).map(|i| p16[(i * 5) % p16.len()]);
    assert_eq!(pgs[0].trans_value.as_i64(), Datum::from_i16(v16().fold(-1i16, |a, v| a & v)).as_i64());
    assert_eq!(pgs[1].trans_value.as_i64(), Datum::from_i16(v16().fold(0i16, |a, v| a | v)).as_i64());
}

// Two-batch accumulation: fold_batch twice into the same pergroups equals the
// per-row reference over the concatenation. The float split lands a ±0 tie
// and a NaN-payload tie ACROSS the batch boundary, exercising the
// advance-vs-prefold seam.
#[test]
fn foldcov_two_batch_accumulation() {
    let mcx = leaked_mcx();
    let a_f8 = arg_list(mcx, mk_var(mcx, 1, F64V));
    let ab = arg_list(mcx, mk_var(mcx, 2, BOOLV));
    let a8 = arg_list(mcx, mk_var(mcx, 3, INT8OID));
    let specs = [
        mk_spec(224, true, &a_f8), // min(float8)
        mk_spec(223, true, &a_f8), // max(float8)
        mk_spec(2515, true, &ab),  // bool_and
        mk_spec(1905, true, &a8),  // bit_or(int8)
    ];
    let nan_a = f64::from_bits(0x7FF8_0000_0000_0001);
    let nan_b = f64::from_bits(0x7FF8_0000_0000_0002);
    let row = |f: f64, b: bool, v: i64| -> Vec<Option<Datum>> {
        vec![Some(Datum::from_f64(f)), Some(Datum::from_bool(b)), Some(Datum::from_i64(v))]
    };
    // Batch 1 ends min-tied at 0.0 and max at nan_a; batch 2 re-ties with
    // -0.0 and nan_b — C's sequential transitions take the later datum.
    let batch1 = vec![row(3.0, true, 0x10), row(0.0, true, 0x0F), row(nan_a, true, 1 << 40)];
    let batch2 = vec![row(-0.0, true, 0x300), row(nan_b, false, 2)];
    let plan = classify(mcx, &specs).expect("admits");
    let mut pgs = pergroups_for(mcx, &plan, specs.len());
    for batch in [&batch1, &batch2] {
        let cols = TestCols::from_datum_rows(3, batch);
        let rows = selmask(batch.len(), |_| true);
        // SAFETY: pgs covers every transno; lanes cover every plan col/row.
        unsafe {
            fold_batch(&plan, &cols, &rows, batch.len(), NonNull::new(pgs.as_mut_ptr()).unwrap(), mcx)
                .expect("fold");
        }
    }
    let mut all = batch1.clone();
    all.extend(batch2.iter().cloned());
    let want = reference_fold_datum(mcx, &plan, &all, |_| true, specs.len());
    assert_parity(&plan, &pgs, &want);
    // Pin the cross-batch tie results.
    assert_eq!(pgs[0].trans_value.as_u64(), (-0.0f64).to_bits(), "min re-tied by -0.0");
    assert_eq!(pgs[1].trans_value.as_u64(), nan_b.to_bits(), "max re-tied by later NaN");
    assert!(!pgs[2].trans_value.as_bool(), "AND poisoned in batch 2");
    assert_eq!(pgs[3].trans_value.as_i64(), 0x10 | 0x0F | (1 << 40) | 0x300 | 2);
}

#[test]
fn fold_rows_grouped_foldcov_parity() {
    let mcx = leaked_mcx();
    let a_f8 = arg_list(mcx, mk_var(mcx, 1, F64V));
    let a_f4 = arg_list(mcx, mk_var(mcx, 2, F32V));
    let ab = arg_list(mcx, mk_var(mcx, 3, BOOLV));
    let a4 = arg_list(mcx, mk_var(mcx, 4, INT4OID));
    let specs = [
        mk_spec(223, true, &a_f8), // max(float8)
        mk_spec(211, true, &a_f4), // min(float4)
        mk_spec(2516, true, &ab),  // bool_or
        mk_spec(1898, true, &a4),  // bit_and(int4)
    ];
    let plan = classify(mcx, &specs).expect("admits");
    let (p8, p4) = (f64_pool(), f32_pool());
    let p32: Vec<i32> = vec![-1, 0x5A5A_5A5A, i32::MIN, 0x00FF_FF00, 7];
    let n = 180usize;
    let ngroups = 3usize;
    let data: Vec<Vec<Option<Datum>>> = (0..n)
        .map(|i| {
            vec![
                if i % 5 == 0 { None } else { Some(Datum::from_f64(p8[(i * 7) % p8.len()])) },
                if i % 4 == 3 { None } else { Some(Datum::from_f32(p4[(i * 11) % p4.len()])) },
                if i % 6 == 1 { None } else { Some(Datum::from_bool(i % 7 < 3)) },
                if i % 8 == 5 { None } else { Some(Datum::from_i32(p32[(i * 13) % p32.len()])) },
            ]
        })
        .collect();
    let cols = TestCols::from_datum_rows(4, &data);
    let mut group_pgs: Vec<Vec<AggPerGroup>> =
        (0..ngroups).map(|_| pergroups_for(mcx, &plan, specs.len())).collect();
    let idxs: Vec<u32> = (0..n as u32).collect();
    let groups: Vec<NonNull<AggPerGroup>> = (0..n)
        .map(|i| NonNull::new(group_pgs[i % ngroups].as_mut_ptr()).unwrap())
        .collect();
    // SAFETY: each group's pergroup array covers every transno; lanes cover
    // every row; arrays are not moved while the pointers live.
    unsafe { fold_rows_grouped(&plan, &cols, &idxs, &groups, mcx).expect("fold") };
    for g in 0..ngroups {
        let gdata: Vec<Vec<Option<Datum>>> =
            (0..n).filter(|i| i % ngroups == g).map(|i| data[i].clone()).collect();
        let want = reference_fold_datum(mcx, &plan, &gdata, |_| true, specs.len());
        assert_parity(&plan, &group_pgs[g], &want);
    }
}

// Tier-2 kinds never join a CSE group: duplicate float MINs stay per-trans
// while duplicate int MINs still share a MinMax scan (and SumBase never sees
// a datum-lane member).
#[test]
fn foldcov_cse_exclusion() {
    let mcx = leaked_mcx();
    let a_f8a = arg_list(mcx, mk_var(mcx, 1, F64V));
    let a_f8b = arg_list(mcx, mk_var(mcx, 1, F64V));
    let a_i4a = arg_list(mcx, mk_var(mcx, 2, INT4OID));
    let a_i4b = arg_list(mcx, mk_var(mcx, 2, INT4OID));
    let ab = arg_list(mcx, mk_var(mcx, 3, BOOLV));
    let specs = [
        mk_spec(224, true, &a_f8a), // min(float8)
        mk_spec(224, true, &a_f8b), // min(float8) duplicate
        mk_spec(769, false, &a_i4a), // min(int4)
        mk_spec(769, false, &a_i4b), // min(int4) duplicate
        mk_spec(2515, true, &ab),    // bool_and
    ];
    let plan = classify(mcx, &specs).expect("admits");
    assert_eq!(plan.trans.len(), 5);
    assert_eq!(plan.cse.len(), 1, "only the int MIN pair clusters");
    assert_eq!(plan.cse[0].kind, CseGroupKind::MinMax);
    let skipped: Vec<usize> =
        plan.cse_skip.iter().enumerate().filter(|(_, &s)| s).map(|(i, _)| i).collect();
    assert_eq!(skipped, vec![2, 3], "float/bool transitions stay per-trans");
    // And the whole plan still folds bit-identically to the references.
    let (p8, _) = (f64_pool(), ());
    let data: Vec<Vec<Option<Datum>>> = (0..120)
        .map(|i| {
            vec![
                if i % 4 == 1 { None } else { Some(Datum::from_f64(p8[(i * 7) % p8.len()])) },
                if i % 5 == 2 { None } else { Some(Datum::from_i32(i as i32 * 37 - 900)) },
                if i % 3 == 0 { None } else { Some(Datum::from_bool(i % 11 != 4)) },
            ]
        })
        .collect();
    let cols = TestCols::from_datum_rows(3, &data);
    let rows = selmask(data.len(), |i| i % 2 == 0);
    let mut pgs = pergroups_for(mcx, &plan, specs.len());
    // SAFETY: pgs covers every transno; lanes cover every plan col/row.
    unsafe {
        fold_batch(&plan, &cols, &rows, data.len(), NonNull::new(pgs.as_mut_ptr()).unwrap(), mcx)
            .expect("fold");
    }
    // References: the int lanes through the i64 reference, the datum lanes
    // through the datum reference (split the plan's transitions accordingly
    // by checking each pergroup individually).
    let want_d = reference_fold_datum(mcx, &plan_subset(mcx, &plan, &[0, 1, 4]), &data, |i| i % 2 == 0, specs.len());
    for tn in [0usize, 1, 4] {
        assert_eq!(pgs[tn].trans_value.as_i64(), want_d[tn].trans_value.as_i64(), "transno {tn}");
        assert_eq!(pgs[tn].trans_value_is_null, want_d[tn].trans_value_is_null);
    }
    let idata: Vec<Vec<Option<i64>>> = (0..120)
        .map(|i| {
            vec![
                None,
                if i % 5 == 2 { None } else { Some(i as i64 * 37 - 900) },
                None,
            ]
        })
        .collect();
    let want_i = reference_fold(mcx, &plan_subset(mcx, &plan, &[2, 3]), &idata, |i| i % 2 == 0, specs.len());
    for tn in [2usize, 3] {
        assert_eq!(pgs[tn].trans_value.as_i64(), want_i[tn].trans_value.as_i64(), "transno {tn}");
    }
}

// A LanePlan restricted to the given transnos (test-only helper: lets the
// i64 and datum references each replay just their own transitions).
fn plan_subset<'mcx>(
    mcx: Mcx<'mcx>,
    plan: &LanePlan<'mcx>,
    transnos: &[usize],
) -> LanePlan<'mcx> {
    let mut trans: ::mcx::PgVec<'mcx, LaneTrans> = ::mcx::PgVec::new_in(mcx);
    for t in plan.trans.iter() {
        if transnos.contains(&(t.transno as usize)) {
            trans.push(*t);
        }
    }
    let (cse, cse_members, cse_skip) = build_cse(mcx, &trans);
    LanePlan {
        trans,
        cse,
        cse_members,
        cse_skip,
        guards: ::mcx::PgVec::new_in(mcx),
        vguards: ::mcx::PgVec::new_in(mcx),
        uguards: ::mcx::PgVec::new_in(mcx),
        filters: ::mcx::PgVec::new_in(mcx),
        cols: ::mcx::PgVec::new_in(mcx),
        resid: ::mcx::PgVec::new_in(mcx),
        guarded: false,
    }
}

// ---- fold-coverage tier 3: text/bpchar MIN/MAX ----

const TEXTV: Oid = ::types_core::catalog::TEXTOID;
const VARCHARV: Oid = ::types_core::catalog::VARCHAROID;
const BPCHARV: Oid = ::types_core::catalog::BPCHAROID;
const COLL_C: Oid = ::types_core::catalog::C_COLLATION_OID;
const COLL_POSIX: Oid = ::types_core::catalog::POSIX_COLLATION_OID;
const COLL_DEFAULT: Oid = ::types_core::catalog::DEFAULT_COLLATION_OID;
// An en_US-class libc/ICU collation stand-in: any non-C/POSIX oid refuses.
const COLL_LOCALE: Oid = 12345;

fn mk_spec_coll<'a>(
    transfn_oid: Oid,
    args: &'a NodeList<'static>,
    inputcollid: Oid,
) -> AggTransSpec<'a, 'static> {
    let mut spec = mk_spec(transfn_oid, true, args);
    spec.inputcollid = inputcollid;
    // text/bpchar transvalues are by-ref varlenas.
    spec.transtype_byval = false;
    spec.transtype_len = -1;
    spec
}

// Inline varlena builders (leaked, like leaked_mcx's arenas): the exact page
// forms the vguard admits — 1B short header and 4B uncompressed — plus the
// two demote forms (4B compressed, 1B external toast pointer) faked down to
// the header bytes the vguard inspects.
fn vl_short(payload: &[u8]) -> Datum {
    assert!(payload.len() + 1 <= 0x7F);
    let mut v = vec![0u8; payload.len() + 1];
    // SAFETY: in-bounds write of the 1-byte header.
    unsafe { ::types_tuple::varatt::set_varsize_short(v.as_mut_ptr(), payload.len() + 1) };
    v[1..].copy_from_slice(payload);
    Datum::from_usize(Box::leak(v.into_boxed_slice()).as_ptr() as usize)
}

fn vl_4b(payload: &[u8]) -> Datum {
    let mut v = vec![0u8; payload.len() + 4];
    let word = ::types_tuple::varatt::set_varsize_4b_word((payload.len() + 4) as u32);
    v[..4].copy_from_slice(&word.to_ne_bytes());
    v[4..].copy_from_slice(payload);
    Datum::from_usize(Box::leak(v.into_boxed_slice()).as_ptr() as usize)
}

fn vl_4b_compressed_fake() -> Datum {
    let word = ::types_tuple::varatt::set_varsize_4b_c_word(64);
    let v = word.to_ne_bytes().to_vec();
    Datum::from_usize(Box::leak(v.into_boxed_slice()).as_ptr() as usize)
}

fn vl_external_fake() -> Datum {
    // varattrib_1b_e header byte (little-endian 0x01) + a vartag byte.
    let v = vec![0x01u8, 18];
    Datum::from_usize(Box::leak(v.into_boxed_slice()).as_ptr() as usize)
}

// VARSIZE_ANY image of an inline varlena datum (header + payload).
fn vl_image(d: Datum) -> Vec<u8> {
    let p = d.as_usize() as *const u8;
    // SAFETY: test datums are live inline varlenas (or fold copies of them).
    unsafe {
        let n = ::types_tuple::varatt::varsize_any(p);
        core::slice::from_raw_parts(p, n).to_vec()
    }
}

fn vl_payload(d: Datum) -> Vec<u8> {
    let img = vl_image(d);
    // SAFETY: inline form by construction.
    if unsafe { ::types_tuple::varatt::varatt_is_1b(d.as_usize() as *const u8) } {
        img[1..].to_vec()
    } else {
        img[4..].to_vec()
    }
}

// Per-row C reference for the str kinds: text_larger/smaller and
// bpchar_larger/smaller applied literally in row order, with the transvalue
// modeled as an OWNED image (the datumCopy C's advance_transition_function
// performs whenever the transfn returns the input argument — install and
// every replace, never a keep).
fn reference_fold_str(
    kind: LaneKind,
    rows: &[Option<Datum>],
    sel: impl Fn(usize) -> bool,
) -> Option<Vec<u8>> {
    let mut state: Option<Vec<u8>> = None;
    for (i, d) in rows.iter().enumerate() {
        if !sel(i) {
            continue;
        }
        let Some(d) = *d else { continue };
        match &state {
            None => state = Some(vl_image(d)),
            Some(cur_img) => {
                let cur = Datum::from_usize(cur_img.as_ptr() as usize);
                // SAFETY: live inline varlenas by construction.
                if !unsafe { str_keep(kind, cur, d) } {
                    state = Some(vl_image(d));
                }
            }
        }
    }
    state
}

#[test]
fn classify_str_admission() {
    let mcx = leaked_mcx();
    let a_text = arg_list(mcx, mk_var(mcx, 1, TEXTV));
    let a_vchar = arg_list(mcx, mk_var(mcx, 2, VARCHARV));
    let relabel = Node::mk_relabel_type(
        mcx,
        mk_var(mcx, 2, VARCHARV),
        TEXTV,
        -1,
        COLL_C,
        ::types_nodes::primnodes::CoercionForm::COERCE_IMPLICIT_CAST,
    )
    .unwrap();
    let a_rel = arg_list(mcx, relabel);
    let a_bp = arg_list(mcx, mk_var(mcx, 3, BPCHARV));
    // (transfn oid, args, collation, kind) — the tier-3 admission table. All
    // strict + NULL-init in pg_aggregate; C and POSIX are the only collations
    // that admit (memcmp tier).
    let cases: Vec<(Oid, &NodeList<'static>, Oid, LaneKind)> = vec![
        (458, &a_text, COLL_C, LaneKind::StrMax),      // max(text)
        (459, &a_text, COLL_C, LaneKind::StrMin),      // min(text)
        (458, &a_text, COLL_POSIX, LaneKind::StrMax),  // POSIX == memcmp tier
        (458, &a_rel, COLL_C, LaneKind::StrMax),       // max(varchar): relabel
        (459, &a_vchar, COLL_C, LaneKind::StrMin),     // bare varchar Var
        (1063, &a_bp, COLL_C, LaneKind::BpMax),        // max(bpchar)
        (1064, &a_bp, COLL_C, LaneKind::BpMin),        // min(bpchar)
    ];
    for (i, (oid, args, coll, kind)) in cases.iter().enumerate() {
        let spec = mk_spec_coll(*oid, args, *coll);
        let (t, g, _) = classify_trans(&spec, i).unwrap_or_else(|| panic!("case {i} admits"));
        assert_eq!(t.kind, *kind, "case {i}");
        assert_eq!(t.width, LaneWidth::Var, "case {i}");
        assert_eq!(t.res_width, LaneWidth::Var, "case {i}");
        assert_eq!((t.addend, t.mulk, t.divk), (0, 1, 1), "case {i}");
        assert!(g.is_none(), "str lanes carry no integer guard, case {i}");
    }
    // The plan carries the vguard obligation for the str column(s) and is
    // guarded even with no integer guards.
    let specs = [mk_spec_coll(459, &a_text, COLL_C), mk_spec_coll(458, &a_text, COLL_C)];
    let plan = classify(mcx, &specs).expect("admits");
    assert!(plan.guarded);
    assert!(plan.guards.is_empty());
    assert_eq!(&plan.vguards[..], &[0]);
    assert_eq!(&plan.cols[..], &[0]);
}

#[test]
fn classify_str_refusals() {
    let mcx = leaked_mcx();
    let a_text = arg_list(mcx, mk_var(mcx, 1, TEXTV));
    // Collation gate: DEFAULT may alias a C-semantics database collation but
    // classify has no catalog access; locale collations can error/allocate
    // per row; invalid (0) never proves.
    for coll in [COLL_DEFAULT, COLL_LOCALE, 0] {
        assert!(
            classify_trans(&mk_spec_coll(458, &a_text, coll), 0).is_none(),
            "collation {coll} refused"
        );
        assert!(classify_trans(&mk_spec_coll(459, &a_text, coll), 0).is_none());
    }
    // Wrong Var type for the transfn.
    let a_int = arg_list(mcx, mk_var(mcx, 1, INT4OID));
    assert!(classify_trans(&mk_spec_coll(458, &a_int, COLL_C), 0).is_none());
    let a_bp = arg_list(mcx, mk_var(mcx, 1, BPCHARV));
    assert!(
        classify_trans(&mk_spec_coll(458, &a_bp, COLL_C), 0).is_none(),
        "bpchar var for text_larger"
    );
    let a_text2 = arg_list(mcx, mk_var(mcx, 1, TEXTV));
    assert!(
        classify_trans(&mk_spec_coll(1063, &a_text2, COLL_C), 0).is_none(),
        "text var for bpchar_larger"
    );
    // Relabel to a non-text result type.
    let bad_rel = Node::mk_relabel_type(
        mcx,
        mk_var(mcx, 2, VARCHARV),
        BPCHARV,
        -1,
        COLL_C,
        ::types_nodes::primnodes::CoercionForm::COERCE_IMPLICIT_CAST,
    )
    .unwrap();
    let a_badrel = arg_list(mcx, bad_rel);
    assert!(classify_trans(&mk_spec_coll(458, &a_badrel, COLL_C), 0).is_none());
    // OpExpr args never admit for varlena lanes.
    let a_op = arg_list(mcx, mk_int_op(mcx, 1, INT2OID, 5, 178, true));
    assert!(classify_trans(&mk_spec_coll(458, &a_op, COLL_C), 0).is_none());
    // Shape gates hold for str transfns too.
    let mut spec = mk_spec_coll(459, &a_text, COLL_C);
    spec.combine = true;
    assert!(classify_trans(&spec, 0).is_none(), "combine refused");
    let mut spec = mk_spec_coll(459, &a_text, COLL_C);
    spec.aggfilter = Some(mk_var(mcx, 1, INT4OID));
    assert!(classify_trans(&spec, 0).is_none(), "aggfilter refused");
}

#[test]
fn vguard_check_inline_forms() {
    let mcx = leaked_mcx();
    let a_text = arg_list(mcx, mk_var(mcx, 1, TEXTV));
    let specs = [mk_spec_coll(459, &a_text, COLL_C)];
    let plan = classify(mcx, &specs).expect("admits");
    assert!(plan.guarded);
    let rows4 = selmask(4, |_| true);
    // Both inline forms pass; NULLs are skipped.
    let data = vec![
        vec![Some(vl_short(b"a"))],
        vec![Some(vl_4b(b"bb"))],
        vec![None],
        vec![Some(vl_short(b""))],
    ];
    let cols = TestCols::from_datum_rows(1, &data);
    // SAFETY: live varlena test datums.
    assert_eq!(
        unsafe { check_guards(&plan, &cols, &rows4, |_| None) },
        GuardCheck::Pass { zone: false, data: true }
    );
    // A compressed inline datum demotes the whole batch...
    let data = vec![
        vec![Some(vl_short(b"a"))],
        vec![Some(vl_4b_compressed_fake())],
        vec![Some(vl_4b(b"bb"))],
        vec![Some(vl_short(b"c"))],
    ];
    let cols = TestCols::from_datum_rows(1, &data);
    // SAFETY: header bytes readable (fake forms carry a full header).
    assert_eq!(unsafe { check_guards(&plan, &cols, &rows4, |_| None) }, GuardCheck::Demote);
    // ... as does an external toast pointer ...
    let data = vec![
        vec![Some(vl_short(b"a"))],
        vec![Some(vl_external_fake())],
        vec![Some(vl_4b(b"bb"))],
        vec![Some(vl_short(b"c"))],
    ];
    let cols = TestCols::from_datum_rows(1, &data);
    // SAFETY: as above.
    assert_eq!(unsafe { check_guards(&plan, &cols, &rows4, |_| None) }, GuardCheck::Demote);
    // ... unless the offending row is unselected or NULL (the proof covers
    // exactly the rows the fold would touch).
    let rows_skip = selmask(4, |i| i != 1);
    // SAFETY: as above.
    assert_eq!(
        unsafe { check_guards(&plan, &cols, &rows_skip, |_| None) },
        GuardCheck::Pass { zone: false, data: true }
    );
    let data = vec![vec![Some(vl_short(b"a"))], vec![None], vec![Some(vl_4b(b"bb"))], vec![None]];
    let cols = TestCols::from_datum_rows(1, &data);
    // SAFETY: as above.
    assert_eq!(
        unsafe { check_guards(&plan, &cols, &rows4, |_| None) },
        GuardCheck::Pass { zone: false, data: true }
    );
}

// The text pool: empty string, single bytes, multibyte UTF-8 (é, 漢, emoji),
// embedded prefix pairs (memcmp length tiebreak), long strings past the
// short-header bound (>126 total), and equal payloads under BOTH header
// forms (tie classes with distinct datum identities).
fn text_pool() -> Vec<Datum> {
    let long_a: Vec<u8> = core::iter::repeat(b'a').take(200).collect();
    let mut long_b = long_a.clone();
    long_b.push(b'b'); // long_a is a strict prefix of long_b
    vec![
        vl_short(b""),
        vl_4b(b""),
        vl_short(b"a"),
        vl_4b(b"a"),
        vl_short(b"ab"),
        vl_short(b"abc"),
        vl_short("é".as_bytes()),
        vl_short("漢字".as_bytes()),
        vl_short("🦀".as_bytes()),
        vl_4b(&long_a),
        vl_4b(&long_b),
        vl_short(b"zz"),
        vl_4b(b"zz"),
        vl_short(b"Z"),
    ]
}

// Fold vs the per-row C reference over the pool, checking the transvalue's
// full image (header form included) and the copy discipline (the transvalue
// is never an input datum — it is this fold's aggcxt copy).
fn run_fold_str(
    mcx: Mcx<'static>,
    transfn: Oid,
    kind: LaneKind,
    rows: &[Option<Datum>],
    sel: impl Fn(usize) -> bool + Copy,
) {
    let a = arg_list(mcx, mk_var(mcx, 1, if transfn >= 1063 { BPCHARV } else { TEXTV }));
    let specs = [mk_spec_coll(transfn, &a, COLL_C)];
    let plan = classify(mcx, &specs).expect("admits");
    let data: Vec<Vec<Option<Datum>>> = rows.iter().map(|d| vec![*d]).collect();
    let cols = TestCols::from_datum_rows(1, &data);
    let selm = selmask(rows.len(), sel);
    let mut pgs = pergroups_for(mcx, &plan, 1);
    // SAFETY: pgs covers transno 0; the lane holds live inline varlenas
    // (vguard forms by construction); mcx is the live test arena.
    unsafe {
        check_guards(&plan, &cols, &selm, |_| None);
        fold_batch(&plan, &cols, &selm, rows.len(), NonNull::new(pgs.as_mut_ptr()).unwrap(), mcx)
            .expect("fold");
    }
    let want = reference_fold_str(kind, rows, sel);
    match want {
        None => assert!(pgs[0].no_trans_value && pgs[0].trans_value_is_null),
        Some(img) => {
            assert!(!pgs[0].trans_value_is_null && !pgs[0].no_trans_value);
            assert_eq!(vl_image(pgs[0].trans_value), img, "transvalue image (header + payload)");
            // Copy discipline: the stored transvalue is the fold's own copy,
            // never an input datum pointer.
            for d in rows.iter().flatten() {
                assert_ne!(pgs[0].trans_value.as_usize(), d.as_usize(), "input datum escaped");
            }
        }
    }
}

#[test]
fn fold_text_minmax_parity() {
    let mcx = leaked_mcx();
    let pool = text_pool();
    let n = 240;
    let rows: Vec<Option<Datum>> = (0..n)
        .map(|i| if i % 5 == 3 { None } else { Some(pool[(i * 7) % pool.len()]) })
        .collect();
    for sel in [
        (|_: usize| true) as fn(usize) -> bool,
        |i| i % 2 == 1,
        |i| i % 7 == 4,
        |_| false,
    ] {
        run_fold_str(mcx, 458, LaneKind::StrMax, &rows, sel);
        run_fold_str(mcx, 459, LaneKind::StrMin, &rows, sel);
    }
    // All-NULL lane leaves min/max in the strict no-trans-value state.
    let nulls: Vec<Option<Datum>> = (0..40).map(|_| None).collect();
    run_fold_str(mcx, 458, LaneKind::StrMax, &nulls, |_| true);
    run_fold_str(mcx, 459, LaneKind::StrMin, &nulls, |_| true);
}

#[test]
fn fold_bpchar_minmax_parity() {
    let mcx = leaked_mcx();
    // bpchar pools are space-padded to a typmod; ties differ in padding.
    let pool = vec![
        vl_short(b"ab  "),
        vl_short(b"ab"),
        vl_4b(b"ab "),
        vl_short(b"aa  "),
        vl_short(b"zz  "),
        vl_4b(b"zz"),
        vl_short(b"    "),
        vl_short(b""),
        vl_short("é   ".as_bytes()),
    ];
    let n = 150;
    let rows: Vec<Option<Datum>> = (0..n)
        .map(|i| if i % 4 == 2 { None } else { Some(pool[(i * 5) % pool.len()]) })
        .collect();
    for sel in [(|_: usize| true) as fn(usize) -> bool, |i| i % 3 != 0] {
        run_fold_str(mcx, 1063, LaneKind::BpMax, &rows, sel);
        run_fold_str(mcx, 1064, LaneKind::BpMin, &rows, sel);
    }
}

// The C findings pinned: text ties take the SECOND argument (last-tied-wins
// on datum identity — header form is the observable), bpchar ties keep the
// FIRST (the state survives), and bpchar ties include trailing-blank-only
// differences (the survivor keeps ITS padding).
#[test]
fn str_tie_semantics() {
    let mcx = leaked_mcx();
    let run = |oid: Oid, kind: LaneKind, rows: &[Option<Datum>]| -> Vec<u8> {
        let a = arg_list(mcx, mk_var(mcx, 1, if oid >= 1063 { BPCHARV } else { TEXTV }));
        let specs = [mk_spec_coll(oid, &a, COLL_C)];
        let plan = classify(mcx, &specs).expect("admits");
        let data: Vec<Vec<Option<Datum>>> = rows.iter().map(|d| vec![*d]).collect();
        let cols = TestCols::from_datum_rows(1, &data);
        let selm = selmask(rows.len(), |_| true);
        let mut pgs = pergroups_for(mcx, &plan, 1);
        // SAFETY: as run_fold_str.
        unsafe {
            fold_batch(&plan, &cols, &selm, rows.len(), NonNull::new(pgs.as_mut_ptr()).unwrap(), mcx)
                .expect("fold");
        }
        assert!(!pgs[0].trans_value_is_null);
        vl_image(pgs[0].trans_value)
    };
    let short_zz = vl_short(b"zz");
    let long_zz = vl_4b(b"zz");
    // text: equal payloads, distinct header forms — the LAST tied datum's
    // exact image survives, for MIN and MAX alike.
    assert_eq!(run(458, LaneKind::StrMax, &[Some(short_zz), Some(long_zz)]), vl_image(long_zz));
    assert_eq!(run(458, LaneKind::StrMax, &[Some(long_zz), Some(short_zz)]), vl_image(short_zz));
    assert_eq!(run(459, LaneKind::StrMin, &[Some(short_zz), Some(long_zz)]), vl_image(long_zz));
    assert_eq!(run(459, LaneKind::StrMin, &[Some(long_zz), Some(short_zz)]), vl_image(short_zz));
    // bpchar: the FIRST tied datum survives — including trailing-blank ties,
    // where the survivor keeps its own padding bytes.
    let bp_pad = vl_short(b"ab   ");
    let bp_bare = vl_short(b"ab");
    assert_eq!(run(1063, LaneKind::BpMax, &[Some(bp_pad), Some(bp_bare)]), vl_image(bp_pad));
    assert_eq!(run(1063, LaneKind::BpMax, &[Some(bp_bare), Some(bp_pad)]), vl_image(bp_bare));
    assert_eq!(run(1064, LaneKind::BpMin, &[Some(bp_pad), Some(bp_bare)]), vl_image(bp_pad));
    // ... while text orders 'ab   ' ABOVE 'ab' (no trimming: longer wins).
    assert_eq!(run(458, LaneKind::StrMax, &[Some(bp_pad), Some(bp_bare)]), vl_image(bp_pad));
    assert_eq!(run(459, LaneKind::StrMin, &[Some(bp_pad), Some(bp_bare)]), vl_image(bp_bare));
}

// Two-batch accumulation across a tie seam: batch 2 re-ties batch 1's
// winner with a distinct-header equal datum — text replaces (last-tied-wins,
// fresh aggcxt copy), bpchar keeps its state (first-tied-wins, same copy).
#[test]
fn str_two_batch_accumulation() {
    let mcx = leaked_mcx();
    let run = |oid: Oid, batch1: &[Option<Datum>], batch2: &[Option<Datum>]| -> (Vec<u8>, bool) {
        let a = arg_list(mcx, mk_var(mcx, 1, if oid >= 1063 { BPCHARV } else { TEXTV }));
        let specs = [mk_spec_coll(oid, &a, COLL_C)];
        let plan = classify(mcx, &specs).expect("admits");
        let mut pgs = pergroups_for(mcx, &plan, 1);
        let mut after_b1 = Datum::null();
        for (bi, batch) in [batch1, batch2].into_iter().enumerate() {
            let data: Vec<Vec<Option<Datum>>> = batch.iter().map(|d| vec![*d]).collect();
            let cols = TestCols::from_datum_rows(1, &data);
            let selm = selmask(batch.len(), |_| true);
            // SAFETY: as run_fold_str.
            unsafe {
                fold_batch(
                    &plan,
                    &cols,
                    &selm,
                    batch.len(),
                    NonNull::new(pgs.as_mut_ptr()).unwrap(),
                    mcx,
                )
                .expect("fold");
            }
            if bi == 0 {
                after_b1 = pgs[0].trans_value;
            }
        }
        (vl_image(pgs[0].trans_value), pgs[0].trans_value.as_usize() == after_b1.as_usize())
    };
    let b1 = [Some(vl_short(b"m")), Some(vl_short(b"zz")), Some(vl_short(b"a"))];
    let b2 = [Some(vl_4b(b"zz")), Some(vl_short(b"q"))];
    // text max: the batch-2 4B 'zz' re-ties and replaces (new copy).
    let (img, same) = run(458, &b1, &b2);
    assert_eq!(img, vl_image(vl_4b(b"zz")));
    assert!(!same, "text tie replaces the transvalue copy");
    // bpchar max: the tie keeps batch 1's winner (same copy, same image).
    let (img, same) = run(1063, &b1, &b2);
    assert_eq!(img, vl_image(vl_short(b"zz")));
    assert!(same, "bpchar tie keeps the transvalue copy");
    // And the concatenated per-row reference agrees for both.
    let mut all = b1.to_vec();
    all.extend_from_slice(&b2);
    assert_eq!(
        reference_fold_str(LaneKind::StrMax, &all, |_| true).unwrap(),
        vl_image(vl_4b(b"zz"))
    );
    assert_eq!(
        reference_fold_str(LaneKind::BpMax, &all, |_| true).unwrap(),
        vl_image(vl_short(b"zz"))
    );
}

#[test]
fn fold_str_grouped_parity() {
    let mcx = leaked_mcx();
    let a_min = arg_list(mcx, mk_var(mcx, 1, TEXTV));
    let a_max = arg_list(mcx, mk_var(mcx, 1, TEXTV));
    let specs = [mk_spec_coll(459, &a_min, COLL_C), mk_spec_coll(458, &a_max, COLL_C)];
    let plan = classify(mcx, &specs).expect("admits");
    assert_eq!(&plan.vguards[..], &[0]);
    let pool = text_pool();
    let n = 180usize;
    let ngroups = 4usize;
    let rows: Vec<Option<Datum>> = (0..n)
        .map(|i| if i % 6 == 1 { None } else { Some(pool[(i * 11) % pool.len()]) })
        .collect();
    let data: Vec<Vec<Option<Datum>>> = rows.iter().map(|d| vec![*d]).collect();
    let cols = TestCols::from_datum_rows(1, &data);
    let mut group_pgs: Vec<Vec<AggPerGroup>> =
        (0..ngroups).map(|_| pergroups_for(mcx, &plan, specs.len())).collect();
    let idxs: Vec<u32> = (0..n as u32).collect();
    let groups: Vec<NonNull<AggPerGroup>> = (0..n)
        .map(|i| NonNull::new(group_pgs[i % ngroups].as_mut_ptr()).unwrap())
        .collect();
    // SAFETY: each group's pergroup array covers every transno; the lane
    // holds live inline varlenas; arrays are not moved while pointers live.
    unsafe { fold_rows_grouped(&plan, &cols, &idxs, &groups, mcx).expect("fold") };
    for g in 0..ngroups {
        let grows: Vec<Option<Datum>> =
            (0..n).filter(|i| i % ngroups == g).map(|i| rows[i]).collect();
        for (tn, kind) in [(0usize, LaneKind::StrMin), (1, LaneKind::StrMax)] {
            let want = reference_fold_str(kind, &grows, |_| true);
            let pg = &group_pgs[g][tn];
            match want {
                None => assert!(pg.no_trans_value),
                Some(img) => {
                    assert!(!pg.trans_value_is_null);
                    assert_eq!(vl_image(pg.trans_value), img, "group {g} transno {tn}");
                }
            }
        }
    }
}

// Str kinds never join a CSE group (duplicate min(text) stays per-trans),
// and mixed plans keep integer clustering intact beside the str lanes.
#[test]
fn str_cse_exclusion_and_mixed_plan() {
    let mcx = leaked_mcx();
    let a_t1 = arg_list(mcx, mk_var(mcx, 1, TEXTV));
    let a_t2 = arg_list(mcx, mk_var(mcx, 1, TEXTV));
    let a_i1 = arg_list(mcx, mk_var(mcx, 2, INT4OID));
    let a_i2 = arg_list(mcx, mk_var(mcx, 2, INT4OID));
    let specs = [
        mk_spec_coll(459, &a_t1, COLL_C), // min(text)
        mk_spec_coll(459, &a_t2, COLL_C), // min(text) duplicate
        mk_spec(769, false, &a_i1),       // min(int4)
        mk_spec(769, false, &a_i2),       // min(int4) duplicate
    ];
    let plan = classify(mcx, &specs).expect("admits");
    assert_eq!(plan.trans.len(), 4);
    assert_eq!(plan.cse.len(), 1, "only the int MIN pair clusters");
    assert_eq!(plan.cse[0].kind, CseGroupKind::MinMax);
    let skipped: Vec<usize> =
        plan.cse_skip.iter().enumerate().filter(|(_, &s)| s).map(|(i, _)| i).collect();
    assert_eq!(skipped, vec![2, 3], "str transitions stay per-trans");
    assert_eq!(&plan.vguards[..], &[0]);
    assert!(plan.guarded);
    // Both duplicate str transitions fold to the same image.
    let pool = text_pool();
    let data: Vec<Vec<Option<Datum>>> = (0..90)
        .map(|i| {
            vec![
                if i % 4 == 1 { None } else { Some(pool[(i * 3) % pool.len()]) },
                if i % 5 == 2 { None } else { Some(Datum::from_i32(i as i32 * 37 - 900)) },
            ]
        })
        .collect();
    let cols = TestCols::from_datum_rows(2, &data);
    let selm = selmask(data.len(), |i| i % 2 == 0);
    let mut pgs = pergroups_for(mcx, &plan, specs.len());
    // SAFETY: as run_fold_str (both lanes live for every selected row).
    unsafe {
        fold_batch(&plan, &cols, &selm, data.len(), NonNull::new(pgs.as_mut_ptr()).unwrap(), mcx)
            .expect("fold");
    }
    let strrows: Vec<Option<Datum>> = (0..90)
        .map(|i| if i % 4 == 1 { None } else { Some(pool[(i * 3) % pool.len()]) })
        .collect();
    let want = reference_fold_str(LaneKind::StrMin, &strrows, |i| i % 2 == 0).unwrap();
    assert_eq!(vl_image(pgs[0].trans_value), want);
    assert_eq!(vl_image(pgs[1].trans_value), want);
    assert_ne!(
        pgs[0].trans_value.as_usize(),
        pgs[1].trans_value.as_usize(),
        "independent transvalue copies"
    );
    // int lanes still fold right beside them.
    let ints = || (0..90).filter(|i| i % 2 == 0 && i % 5 != 2).map(|i| i as i32 * 37 - 900);
    assert_eq!(pgs[2].trans_value.as_i32(), ints().min().unwrap());
    assert_eq!(pgs[3].trans_value.as_i32(), ints().min().unwrap());
}

// eval_const_expressions rewrites `v COLLATE "C"` as a collation-only
// RelabelType (same result type) — the smoke-critical planner shape for a
// non-C-collation column aggregated under an explicit C collation.
#[test]
fn classify_str_collate_relabel() {
    let mcx = leaked_mcx();
    let mk_rel = |var_ty: Oid, res_ty: Oid| {
        let rel = Node::mk_relabel_type(
            mcx,
            mk_var(mcx, 1, var_ty),
            res_ty,
            -1,
            COLL_C,
            ::types_nodes::primnodes::CoercionForm::COERCE_IMPLICIT_CAST,
        )
        .unwrap();
        arg_list(mcx, rel)
    };
    // text COLLATE "C" -> RelabelType(text -> text).
    let a = mk_rel(TEXTV, TEXTV);
    let (t, _, _) = classify_trans(&mk_spec_coll(459, &a, COLL_C), 0).expect("admits");
    assert_eq!(t.kind, LaneKind::StrMin);
    // bpchar COLLATE "C" -> RelabelType(bpchar -> bpchar).
    let a = mk_rel(BPCHARV, BPCHARV);
    let (t, _, _) = classify_trans(&mk_spec_coll(1063, &a, COLL_C), 0).expect("admits");
    assert_eq!(t.kind, LaneKind::BpMax);
    // Cross-type relabel to bpchar under a text transfn still refuses.
    let a = mk_rel(TEXTV, BPCHARV);
    assert!(classify_trans(&mk_spec_coll(459, &a, COLL_C), 0).is_none());
}

// ---- Phase-3: sum/avg(int8) — the Int128AggState fold ----

// Ungrouped parity for the int8_avg_accum fold: sum(int8) + avg(int8) over
// one column (shared transfn 2746, independent per-trans kernels — no CSE)
// plus count(col), with NULLs, negative extremes and repeated i64::MIN/MAX
// terms whose running sum leaves the i64 range (the i128 carrier is exact
// where an i64 fold would wrap).
#[test]
fn fold_int8_sum_avg_parity() {
    let mcx = leaked_mcx();
    let a_sum = arg_list(mcx, mk_var(mcx, 1, INT8OID));
    let a_avg = arg_list(mcx, mk_var(mcx, 1, INT8OID));
    let a_cnt = arg_list(mcx, mk_var(mcx, 1, INT8OID));
    let specs = [
        mk_spec(2746, true, &a_sum),  // sum(int8)
        mk_spec(2746, true, &a_avg),  // avg(int8) — same transfn
        mk_spec(2804, false, &a_cnt), // count(v)
    ];
    let n = 200usize;
    let val = |i: usize| -> Option<i64> {
        match i % 8 {
            0 => None,
            1 | 3 => Some(i64::MAX),
            2 => Some(i64::MIN),
            4 => Some(-1),
            _ => Some((i as i64 - 100) * 1_000_000_007),
        }
    };
    let data: Vec<Vec<Option<i64>>> = (0..n).map(|i| vec![val(i)]).collect();
    let (plan, pgs) = run_fold(mcx, &specs, &[8], &data, |i| i % 3 != 1);
    assert_eq!(plan.cse.len(), 0, "Int128AvgAccum joins no SumBase cluster");
    let vals = || (0..n).filter(|&i| i % 3 != 1).filter_map(val);
    let want_n = vals().count() as i64;
    let want_s: i128 = vals().map(|v| v as i128).sum();
    assert!(want_s > i64::MAX as i128, "test data must overflow an i64 sum");
    assert_eq!(read_int128_state(&pgs[0]), (want_n, want_s));
    assert_eq!(read_int128_state(&pgs[1]), (want_n, want_s));
    assert_eq!(pgs[2].trans_value.as_i64(), want_n);
}

// C parity for the lazy INTERNAL state: every selected row calls the
// non-strict transfn, so an all-NULL batch still ALLOCATES the state (n = 0
// — observable through int8_avg_serialize under a partial-agg finalize),
// while a batch selecting nothing must leave the pergroup NULL (the transfn
// never ran).
#[test]
fn fold_int8_allnull_allocates_state() {
    let mcx = leaked_mcx();
    let args = arg_list(mcx, mk_var(mcx, 1, INT8OID));
    let specs = [mk_spec(2746, true, &args)];
    let data: Vec<Vec<Option<i64>>> = (0..8).map(|_| vec![None]).collect();
    let (_, pgs) = run_fold(mcx, &specs, &[8], &data, |_| true);
    assert!(!pgs[0].trans_value_is_null, "all-NULL input still allocates the state");
    assert_eq!(read_int128_state(&pgs[0]), (0, 0));
    assert!(pgs[0].no_trans_value, "the non-strict byval step never clears noTransValue");
    // Nothing selected: the transfn never runs, the state stays NULL.
    let (_, pgs) = run_fold(mcx, &specs, &[8], &data, |_| false);
    assert!(pgs[0].trans_value_is_null);
}

// Grouped int8 fold parity across TWO batches: per-row routing, an all-NULL
// group still gets its state allocated, extremes stay exact in i128, and the
// second batch accumulates into the SAME aggcontext state the first
// installed (the pointer datum survives across batches, exactly as the
// per-row transfn chain's does).
#[test]
fn fold_rows_grouped_int8_parity() {
    let mcx = leaked_mcx();
    let a_sum = arg_list(mcx, mk_var(mcx, 1, INT8OID));
    let a_avg = arg_list(mcx, mk_var(mcx, 1, INT8OID));
    let specs = [mk_spec(2746, true, &a_sum), mk_spec(2746, true, &a_avg)];
    let plan = classify(mcx, &specs).expect("admits");
    let n = 120usize;
    let ngroups = 4usize;
    // Group 3 (i % 4 == 3) is all-NULL in both batches (120 % 4 == 0 keeps
    // batch-2 routing aligned).
    let val = |i: usize| -> Option<i64> {
        if i % 4 == 3 || i % 5 == 0 {
            None
        } else if i % 7 == 1 {
            Some(i64::MAX)
        } else if i % 7 == 2 {
            Some(i64::MIN)
        } else {
            Some((i as i64 - 60) * 999_999_937)
        }
    };
    let data: Vec<Vec<Option<i64>>> = (0..n).map(|i| vec![val(i)]).collect();
    let data2: Vec<Vec<Option<i64>>> = (0..n).map(|i| vec![val(n + i)]).collect();
    let cols = TestCols::new(&[8], &data);
    let cols2 = TestCols::new(&[8], &data2);
    let mut group_pgs: Vec<Vec<AggPerGroup>> =
        (0..ngroups).map(|_| pergroups_for(mcx, &plan, specs.len())).collect();
    let idxs: Vec<u32> = (0..n as u32).collect();
    let groups: Vec<NonNull<AggPerGroup>> = (0..n)
        .map(|i| NonNull::new(group_pgs[i % ngroups].as_mut_ptr()).unwrap())
        .collect();
    // SAFETY: each group's pergroup array covers every transno; lanes cover
    // every row; arrays are not moved while the pointers live.
    unsafe { fold_rows_grouped(&plan, &cols, &idxs, &groups, mcx) }.expect("batch 1");
    unsafe { fold_rows_grouped(&plan, &cols2, &idxs, &groups, mcx) }.expect("batch 2");
    for g in 0..ngroups {
        let gdata: Vec<Vec<Option<i64>>> = (0..n)
            .filter(|i| i % ngroups == g)
            .map(|i| data[i].clone())
            .chain((0..n).filter(|i| i % ngroups == g).map(|i| data2[i].clone()))
            .collect();
        let want = reference_fold(mcx, &plan, &gdata, |_| true, specs.len());
        assert_parity(&plan, &group_pgs[g], &want);
    }
    // The all-NULL group's transfn ran per row: allocated state, n = 0.
    assert_eq!(read_int128_state(&group_pgs[3][0]), (0, 0));
    assert_eq!(read_int128_state(&group_pgs[3][1]), (0, 0));
}

// --- lanereg conformance (design §3a batch-function registry) ---------------
// The transfn OID admission set that `classify_trans` recognizes is mirrored in
// the central `lanereg` census as the in-tree Fold tier. This test binds them:
// every transfn OID this crate folds must be an in-tree Fold entry with the
// matching kind, and the census must carry no in-tree Fold OID this crate does
// not fold. Drift in either direction fails the build.
#[test]
fn fold_oid_set_matches_lanereg_census() {
    use ::lanereg::FoldKind as R;
    let expect: &[(::types_core::Oid, R)] = &[
        (F_INT8INC, R::CountStar),
        (F_INT8INC_ANY, R::CountAny),
        (F_INT2_SUM, R::Sum),
        (F_INT4_SUM, R::Sum),
        (F_INT2_AVG_ACCUM, R::AvgAccum),
        (F_INT4_AVG_ACCUM, R::AvgAccum),
        (F_INT4LARGER, R::Max),
        (F_INT4SMALLER, R::Min),
        (F_INT2LARGER, R::Max),
        (F_INT2SMALLER, R::Min),
        (F_INT8LARGER, R::Max),
        (F_INT8SMALLER, R::Min),
        (F_DATE_LARGER, R::Max),
        (F_DATE_SMALLER, R::Min),
        (F_TIMESTAMP_LARGER, R::Max),
        (F_TIMESTAMP_SMALLER, R::Min),
        (F_TIMESTAMPTZ_LARGER, R::Max),
        (F_TIMESTAMPTZ_SMALLER, R::Min),
        // int8fold (2746 int8_avg_accum), landed second train:
        (2746, R::Int128AvgAccum),
        // foldcov tier 2, landed third train:
        (209, R::FMax),
        (211, R::FMin),
        (223, R::FMax),
        (224, R::FMin),
        (2515, R::BoolAnd),
        (2516, R::BoolOr),
        (1892, R::BitAnd),
        (1893, R::BitOr),
        (1898, R::BitAnd),
        (1899, R::BitOr),
        (1904, R::BitAnd),
        (1905, R::BitOr),
        // textfold str MIN/MAX (census kind = plain Min/Max), landed third train:
        (458, R::Max),
        (459, R::Min),
        (1063, R::Max),
        (1064, R::Min),
        // fold-trans float tier (lane-v2-lanefold-trans, knob-gated):
        (204, R::FSum),
        (218, R::FSum),
        (208, R::FAccum),
        (222, R::FAccum),
        // fold-trans increment 2 (lane aggseq-fold2, same knob):
        (F_NUMERIC_AVG_ACCUM, R::NumAccum),
        (F_FLOAT8_REGR_ACCUM, R::FRegrAccum),
        (F_INT8INC_FLOAT8_FLOAT8, R::Count2),
    ];
    for &(oid, kind) in expect {
        assert_eq!(::lanereg::fold_desc(oid), Some(kind), "fold oid {oid} census mismatch");
    }
    let census_in_tree =
        ::lanereg::ENTRIES.iter().filter(|e| ::lanereg::fold_desc(e.oid).is_some()).count();
    assert_eq!(census_in_tree, expect.len(), "lanereg in-tree Fold set drifted from classify_trans");
}

// The affine OpExpr admission set (`classify_arg`'s opfuncid table) is
// mirrored in the census as the in-tree FoldAffine tier. Bind them both ways
// so drift cannot re-open: every OID this crate's affine admission recognizes
// must be in-tree FoldAffine, and the census must carry no in-tree FoldAffine
// OID this table does not admit. (int42div is absent on both sides: a
// (const / var) transform is not v-monotone.)
#[test]
fn affine_oid_set_matches_lanereg_census() {
    let admitted: &[::types_core::Oid] = &[
        F_INT24PL, F_INT42PL, F_INT24MI, F_INT42MI, F_INT24MUL, F_INT42MUL, F_INT24DIV,
        F_INT4PL, F_INT4MI, F_INT4MUL,
    ];
    for &oid in admitted {
        assert!(
            ::lanereg::covers(oid, ::lanereg::Tier::FoldAffine),
            "affine oid {oid} missing from the census FoldAffine tier"
        );
    }
    let census_in_tree = ::lanereg::ENTRIES
        .iter()
        .filter(|e| {
            e.tier(::lanereg::Tier::FoldAffine).is_some_and(|c| c.is_intree())
        })
        .count();
    assert_eq!(
        census_in_tree,
        admitted.len(),
        "lanereg in-tree FoldAffine set drifted from classify_arg"
    );
    // int8 pl/mi/mul are documented FoldAffine REFUSALS (no i128 interval
    // machinery), not admissions — classify_arg must keep refusing them.
    for &oid in &[463u32, 464, 465] {
        assert!(!::lanereg::covers(oid, ::lanereg::Tier::FoldAffine), "oid {oid}");
        assert!(
            ::lanereg::entry(oid)
                .and_then(|e| e.tier(::lanereg::Tier::FoldAffine))
                .is_some_and(|c| c.is_refused()),
            "oid {oid} must carry a documented FoldAffine refusal"
        );
    }
}

// ---- strlenfold tier: SUM/AVG/MIN/MAX over length()/octet_length() --------

const F_TEXTLEN_T: Oid = 1257;
const F_OCTETLEN_T: Oid = 1374;

// The encoding seams are process-global set-once; every strlen test funnels
// through this UTF-8 installation (the fleet database encoding). The
// max_length==1 (byte-count textlen) arm is admission-equivalent to the
// octet_length arm and is covered there.
fn install_utf8_seams() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        ::mbutils_seams::pg_database_encoding_max_length::set(|| 4);
        ::mbutils_seams::get_database_encoding::set(|| PG_UTF8);
    });
}

fn mk_len_fn(mcx: Mcx<'static>, funcid: Oid, arg: Node<'static>) -> Node<'static> {
    let mut f = Node::build::<::types_nodes::primnodes::FuncExpr>(mcx).unwrap();
    f.funcid = funcid;
    f.funcresulttype = INT4OID;
    f.args = NodeList::make1(mcx, arg).unwrap();
    f.seal()
}

// UTF-8 length corpus: ASCII, empty, 2/3/4-byte chars, high-codepoint mixes,
// both inline header forms.
fn len_corpus() -> Vec<Option<Datum>> {
    vec![
        Some(vl_short(b"http://a.example/x?q=1")),
        Some(vl_short("héllo".as_bytes())),          // 2-byte char
        None,
        Some(vl_short("日本語のページ".as_bytes())), // 3-byte chars
        Some(vl_4b("🦀 crab & 🚀 rocket".as_bytes())), // 4-byte chars
        Some(vl_short(b"")),
        Some(vl_4b(&[b'a'; 300])),                    // 4B header, long ASCII
        Some(vl_short("mixé💡x".as_bytes())),
        None,
        Some(vl_short(b"plain")),
    ]
}

// Independent oracle: std's chars().count() — NOT the fold's byte arithmetic.
fn oracle_charlen(d: Datum) -> i64 {
    let p = vl_payload(d);
    core::str::from_utf8(&p).expect("corpus is valid UTF-8").chars().count() as i64
}

fn oracle_bytelen(d: Datum) -> i64 {
    vl_payload(d).len() as i64
}

// Drive classify + check_guards + fold_batch over a one-varlena-column batch
// and assert byte parity against reference_fold fed with INDEPENDENTLY
// computed lengths (the C-semantics per-row reference over the oracle's
// values).
fn run_fold_len(
    mcx: Mcx<'static>,
    specs: &[AggTransSpec<'_, 'static>],
    data: &[Option<Datum>],
    sel: impl Fn(usize) -> bool + Copy,
    oracle: impl Fn(Datum) -> i64,
) -> (LanePlan<'static>, Vec<AggPerGroup>) {
    let plan = classify(mcx, specs).expect("plan admits");
    let rows_datum: Vec<Vec<Option<Datum>>> = data.iter().map(|d| vec![*d]).collect();
    let cols = TestCols::from_datum_rows(1, &rows_datum);
    let rows = selmask(data.len(), sel);
    assert!(plan.guarded, "length lanes always carry the vguard obligation");
    // SAFETY: lanes hold live inline varlena datums built by vl_short/vl_4b.
    let gc = unsafe { check_guards(&plan, &cols, &rows, |_| None) };
    assert_eq!(gc, GuardCheck::Pass { zone: false, data: true });
    let mut pgs = pergroups_for(mcx, &plan, specs.len());
    // SAFETY: pgs covers every transno; the lane covers every row; vguard
    // (and uguard) proven above.
    unsafe {
        fold_batch(&plan, &cols, &rows, data.len(), NonNull::new(pgs.as_mut_ptr()).unwrap(), mcx)
            .expect("fold");
    }
    let lens: Vec<Vec<Option<i64>>> =
        data.iter().map(|d| vec![d.map(&oracle)]).collect();
    let want = reference_fold(mcx, &plan, &lens, sel, specs.len());
    assert_parity(&plan, &pgs, &want);
    (plan, pgs)
}

#[test]
fn classify_strlen_admission() {
    install_utf8_seams();
    let mcx = leaked_mcx();
    // Every textlen alias oid admits; the arg may be a text Var or the
    // varchar binary-coercion relabel.
    for fnoid in [1257u32, 1317, 1369, 1381] {
        let args = arg_list(mcx, mk_len_fn(mcx, fnoid, mk_var(mcx, 1, TEXTV)));
        let spec = mk_spec(1963, false, &args); // avg(int4)
        let (t, g, _) = classify_trans(&spec, 0).expect("admits");
        assert_eq!(t.kind, LaneKind::AvgAccum);
        assert_eq!(t.width, LaneWidth::VarLenChars, "UTF-8 encoding: char-count lane");
        assert_eq!(t.res_width, LaneWidth::I32);
        assert_eq!((t.addend, t.mulk, t.divk), (0, 1, 1));
        assert!(g.is_none(), "no integer guard: [0, 2^30) is int4 by type");
    }
    // octet_length: byte-count lane, encoding-independent.
    let a_oct = arg_list(mcx, mk_len_fn(mcx, F_OCTETLEN_T, mk_var(mcx, 1, TEXTV)));
    let (t, _, _) = classify_trans(&mk_spec(1841, true, &a_oct), 0).expect("sum admits");
    assert_eq!((t.kind, t.width), (LaneKind::Sum, LaneWidth::VarLenBytes));
    // varchar under the relabel.
    let rel = Node::mk_relabel_type(
        mcx,
        mk_var(mcx, 2, VARCHARV),
        TEXTV,
        -1,
        0,
        ::types_nodes::primnodes::CoercionForm::COERCE_IMPLICIT_CAST,
    )
    .unwrap();
    let a_rel = arg_list(mcx, mk_len_fn(mcx, F_TEXTLEN_T, rel));
    let (t, _, _) = classify_trans(&mk_spec(768, true, &a_rel), 0).expect("max admits");
    assert_eq!((t.kind, t.width, t.col), (LaneKind::Max, LaneWidth::VarLenChars, 1));
    // MIN/MAX/bit over length admit too (the whole int4 arg family).
    for (fnoid, kind) in [(769u32, LaneKind::Min), (1898, LaneKind::BitAnd), (1899, LaneKind::BitOr)] {
        let a = arg_list(mcx, mk_len_fn(mcx, F_TEXTLEN_T, mk_var(mcx, 1, TEXTV)));
        let (t, _, _) = classify_trans(&mk_spec(fnoid, true, &a), 0).expect("admits");
        assert_eq!((t.kind, t.width), (kind, LaneWidth::VarLenChars));
    }
    // Plan-level obligations: char lanes carry vguard AND uguard; octet
    // lanes carry only the vguard.
    let a_char = arg_list(mcx, mk_len_fn(mcx, F_TEXTLEN_T, mk_var(mcx, 1, TEXTV)));
    let specs = [mk_spec(1963, false, &a_char), mk_spec(1841, true, &a_oct)];
    let plan = classify(mcx, &specs).expect("admits");
    assert!(plan.guarded && plan.guards.is_empty());
    assert_eq!(&plan.vguards[..], &[0]);
    assert_eq!(&plan.uguards[..], &[0]);
    assert_eq!(&plan.cols[..], &[0]);
    let specs_oct = [mk_spec(1841, true, &a_oct)];
    let plan_oct = classify(mcx, &specs_oct).expect("admits");
    assert_eq!(&plan_oct.vguards[..], &[0]);
    assert!(plan_oct.uguards.is_empty(), "byte-count lanes need no UTF-8 proof");
}

#[test]
fn classify_strlen_refusals() {
    install_utf8_seams();
    let mcx = leaked_mcx();
    let refuse = |args: &NodeList<'static>| {
        assert!(classify_trans(&mk_spec(1963, false, args), 0).is_none());
    };
    // bpcharlen (bcTruelen semantics) and arbitrary int4 fns refuse.
    refuse(&arg_list(mcx, mk_len_fn(mcx, 1372, mk_var(mcx, 1, BPCHARV))));
    refuse(&arg_list(mcx, mk_len_fn(mcx, 1081, mk_var(mcx, 1, TEXTV))));
    // Arg must be a text/varchar lane Var: bpchar Var, int Var, Const refuse.
    refuse(&arg_list(mcx, mk_len_fn(mcx, F_TEXTLEN_T, mk_var(mcx, 1, BPCHARV))));
    refuse(&arg_list(mcx, mk_len_fn(mcx, F_TEXTLEN_T, mk_var(mcx, 1, INT4OID))));
    let konst =
        Node::mk_const(mcx, TEXTV, -1, 0, -1, Datum::null(), true, false).unwrap();
    refuse(&arg_list(mcx, mk_len_fn(mcx, F_TEXTLEN_T, konst)));
    // A set-returning marker refuses.
    let mut f = Node::build::<::types_nodes::primnodes::FuncExpr>(mcx).unwrap();
    f.funcid = F_TEXTLEN_T;
    f.funcresulttype = INT4OID;
    f.funcretset = true;
    f.args = NodeList::make1(mcx, mk_var(mcx, 1, TEXTV)).unwrap();
    let srf = f.seal();
    refuse(&arg_list(mcx, srf));
    // length() composed inside an OpExpr does not admit (bare-only tier).
    let len = mk_len_fn(mcx, F_TEXTLEN_T, mk_var(mcx, 1, TEXTV));
    let konst4 = Node::mk_const(mcx, INT4OID, -1, 0, 4, Datum::from_i32(1), false, true).unwrap();
    let mut op = Node::build::<OpExpr>(mcx).unwrap();
    op.opfuncid = 177; // int4pl
    op.opresulttype = INT4OID;
    op.args = NodeList::make2(mcx, len, konst4).unwrap();
    let composed = op.seal();
    refuse(&arg_list(mcx, composed));
    // int2-result transfns never see the int4-only length admission.
    let a_len = arg_list(mcx, mk_len_fn(mcx, F_TEXTLEN_T, mk_var(mcx, 1, TEXTV)));
    assert!(classify_trans(&mk_spec(770, true, &a_len), 0).is_none(), "int2larger");
    // Length aggs are never footer-answerable (metaagg refuses the plan).
    let specs = [mk_spec(1963, false, &a_len)];
    assert!(classify_meta(mcx, &specs).is_none());
}

#[test]
fn fold_strlen_parity_multibyte() {
    install_utf8_seams();
    let mcx = leaked_mcx();
    let data = len_corpus();
    // AVG + SUM + MIN + MAX + COUNT(*) over length(col0): exercises the
    // SumBase CSE cluster (avg+sum share one charlen pass) and the MinMax
    // kernels over the charlen read.
    let a1 = arg_list(mcx, mk_len_fn(mcx, F_TEXTLEN_T, mk_var(mcx, 1, TEXTV)));
    let a2 = arg_list(mcx, mk_len_fn(mcx, F_TEXTLEN_T, mk_var(mcx, 1, TEXTV)));
    let a3 = arg_list(mcx, mk_len_fn(mcx, F_TEXTLEN_T, mk_var(mcx, 1, TEXTV)));
    let a4 = arg_list(mcx, mk_len_fn(mcx, F_TEXTLEN_T, mk_var(mcx, 1, TEXTV)));
    let empty = NodeList::default();
    let specs = [
        mk_spec(1963, false, &a1),  // avg(length(x))
        mk_spec(1841, true, &a2),   // sum(length(x))
        mk_spec(769, true, &a3),    // min(length(x))
        mk_spec(768, true, &a4),    // max(length(x))
        mk_spec(1219, false, &empty), // count(*)
    ];
    // All rows selected, then a sparse selection.
    let (plan, _) = run_fold_len(mcx, &specs, &data, |_| true, oracle_charlen);
    assert!(plan.resid.is_empty());
    assert_eq!(
        plan.cse.iter().filter(|g| g.kind == CseGroupKind::SumBase).count(),
        1,
        "avg+sum share one SumBase charlen pass"
    );
    run_fold_len(mcx, &specs, &data, |i| i % 3 != 1, oracle_charlen);
    // Nothing selected: strict aggs stay in their init state.
    run_fold_len(mcx, &specs, &data, |_| false, oracle_charlen);
}

#[test]
fn fold_octetlen_parity_and_no_cse_across_widths() {
    install_utf8_seams();
    let mcx = leaked_mcx();
    let data = len_corpus();
    let a_oct = arg_list(mcx, mk_len_fn(mcx, F_OCTETLEN_T, mk_var(mcx, 1, TEXTV)));
    let specs = [mk_spec(1841, true, &a_oct)];
    run_fold_len(mcx, &specs, &data, |_| true, oracle_bytelen);
    // sum(length(x)) + sum(octet_length(x)): same column, DIFFERENT lane
    // reads — the SumBase key must keep them apart.
    let a_char = arg_list(mcx, mk_len_fn(mcx, F_TEXTLEN_T, mk_var(mcx, 1, TEXTV)));
    let a_oct2 = arg_list(mcx, mk_len_fn(mcx, F_OCTETLEN_T, mk_var(mcx, 1, TEXTV)));
    let specs2 = [mk_spec(1841, true, &a_char), mk_spec(1841, true, &a_oct2)];
    let plan = classify(mcx, &specs2).expect("admits");
    assert!(plan.cse.is_empty(), "char and byte lanes must not share a SumBase pass");
    // And parity still holds per-transition (mixed oracle checked by hand).
    let rows_datum: Vec<Vec<Option<Datum>>> = data.iter().map(|d| vec![*d]).collect();
    let cols = TestCols::from_datum_rows(1, &rows_datum);
    let rows = selmask(data.len(), |_| true);
    // SAFETY: live inline varlena lanes (corpus construction).
    assert_eq!(
        unsafe { check_guards(&plan, &cols, &rows, |_| None) },
        GuardCheck::Pass { zone: false, data: true }
    );
    let mut pgs = pergroups_for(mcx, &plan, 2);
    // SAFETY: guard-passed batch, pergroups cover both transnos.
    unsafe {
        fold_batch(&plan, &cols, &rows, data.len(), NonNull::new(pgs.as_mut_ptr()).unwrap(), mcx)
            .expect("fold");
    }
    let sum_chars: i64 = data.iter().flatten().map(|&d| oracle_charlen(d)).sum();
    let sum_bytes: i64 = data.iter().flatten().map(|&d| oracle_bytelen(d)).sum();
    assert_eq!(pgs[0].trans_value.as_i64(), sum_chars);
    assert_eq!(pgs[1].trans_value.as_i64(), sum_bytes);
}

#[test]
fn strlen_guard_demotes() {
    install_utf8_seams();
    let mcx = leaked_mcx();
    let a_char = arg_list(mcx, mk_len_fn(mcx, F_TEXTLEN_T, mk_var(mcx, 1, TEXTV)));
    let specs = [mk_spec(1963, false, &a_char)];
    let plan = classify(mcx, &specs).expect("admits");
    let gc = |data: &[Option<Datum>], sel: &dyn Fn(usize) -> bool| {
        let rows_datum: Vec<Vec<Option<Datum>>> = data.iter().map(|d| vec![*d]).collect();
        let cols = TestCols::from_datum_rows(1, &rows_datum);
        let rows = selmask(data.len(), sel);
        // SAFETY: selected non-null datums are readable at their header
        // byte (vl_* builders and the fakes below all are).
        unsafe { check_guards(&plan, &cols, &rows, |_| None) }
    };
    // Non-inline forms demote (vguard tier, same as the str MIN/MAX lanes).
    let comp = vec![Some(vl_short(b"ok")), Some(vl_4b_compressed_fake())];
    assert_eq!(gc(&comp, &|_| true), GuardCheck::Demote);
    let ext = vec![Some(vl_external_fake())];
    assert_eq!(gc(&ext, &|_| true), GuardCheck::Demote);
    // Invalid UTF-8 demotes (uguard tier): lone lead byte, bare continuation,
    // overlong encoding, truncated trailing char.
    for bad in [&b"a\xC3(z"[..], b"\x80", b"\xC0\xAF", b"ab\xE2\x82"] {
        let data = vec![Some(vl_short(b"fine")), Some(vl_short(bad))];
        assert_eq!(gc(&data, &|_| true), GuardCheck::Demote, "bad bytes {bad:?}");
    }
    // Embedded NUL demotes: C textlen NUL-stops, the count kernel must not
    // silently diverge.
    let nul = vec![Some(vl_short(b"ab\0cd"))];
    assert_eq!(gc(&nul, &|_| true), GuardCheck::Demote);
    // The proof domain is the selection: unselected/NULL bad rows pass.
    let mixed = vec![Some(vl_short("héllo".as_bytes())), Some(vl_short(b"\x80")), None];
    assert_eq!(gc(&mixed, &|i| i != 1), GuardCheck::Pass { zone: false, data: true });
    // Byte-count lanes carry NO uguard: invalid UTF-8 passes for octet_length.
    let a_oct = arg_list(mcx, mk_len_fn(mcx, F_OCTETLEN_T, mk_var(mcx, 1, TEXTV)));
    let specs_oct = [mk_spec(1841, true, &a_oct)];
    let plan_oct = classify(mcx, &specs_oct).expect("admits");
    let data = vec![Some(vl_short(b"\x80\x00\xFF"))];
    let rows_datum: Vec<Vec<Option<Datum>>> = data.iter().map(|d| vec![*d]).collect();
    let cols = TestCols::from_datum_rows(1, &rows_datum);
    let rows = selmask(1, |_| true);
    // SAFETY: live inline varlena lane.
    assert_eq!(
        unsafe { check_guards(&plan_oct, &cols, &rows, |_| None) },
        GuardCheck::Pass { zone: false, data: true }
    );
}

#[test]
fn fold_rows_grouped_strlen_parity() {
    install_utf8_seams();
    let mcx = leaked_mcx();
    let data = len_corpus();
    let a1 = arg_list(mcx, mk_len_fn(mcx, F_TEXTLEN_T, mk_var(mcx, 1, TEXTV)));
    let a2 = arg_list(mcx, mk_len_fn(mcx, F_TEXTLEN_T, mk_var(mcx, 1, TEXTV)));
    let specs = [mk_spec(1963, false, &a1), mk_spec(768, true, &a2)];
    let plan = classify(mcx, &specs).expect("admits");
    let rows_datum: Vec<Vec<Option<Datum>>> = data.iter().map(|d| vec![*d]).collect();
    let cols = TestCols::from_datum_rows(1, &rows_datum);
    // Two groups: rows alternate.
    let mut pg_a = pergroups_for(mcx, &plan, specs.len());
    let mut pg_b = pergroups_for(mcx, &plan, specs.len());
    let idxs: Vec<u32> = (0..data.len() as u32).collect();
    let groups: Vec<NonNull<AggPerGroup>> = (0..data.len())
        .map(|i| {
            NonNull::new(if i % 2 == 0 { pg_a.as_mut_ptr() } else { pg_b.as_mut_ptr() }).unwrap()
        })
        .collect();
    let rows = selmask(data.len(), |_| true);
    // SAFETY: live inline varlena lanes; guard proven before the fold.
    unsafe {
        assert_eq!(
            check_guards(&plan, &cols, &rows, |_| None),
            GuardCheck::Pass { zone: false, data: true }
        );
        fold_rows_grouped(&plan, &cols, &idxs, &groups, mcx).expect("fold");
    }
    let lens: Vec<Vec<Option<i64>>> = data.iter().map(|d| vec![d.map(oracle_charlen)]).collect();
    let want_a = reference_fold(mcx, &plan, &lens, |i| i % 2 == 0, specs.len());
    let want_b = reference_fold(mcx, &plan, &lens, |i| i % 2 == 1, specs.len());
    assert_parity(&plan, &pg_a, &want_a);
    assert_parity(&plan, &pg_b, &want_b);
}

// ---- length-staged lanes (lane-v2-asciilen) ----

// LaneCols whose column 0 is LENGTH-STAGED: the lane holds i64 lengths (the
// feed's fill answers), NOT varlena pointers. col_len_staged proves the
// guard skips: the i64 bit patterns would demote (or UB) the vguard walk if
// it ran.
struct LenStagedCols {
    values: Vec<Datum>,
    isnull: Vec<bool>,
}

impl LaneCols for LenStagedCols {
    fn col_values(&self, _c: usize) -> &[Datum] {
        &self.values
    }

    fn col_isnull(&self, _c: usize) -> &[bool] {
        &self.isnull
    }

    fn col_len_staged(&self, c: usize) -> bool {
        c == 0
    }
}

// C pg_mbstrlen_with_len under UTF-8 (pg_utf_mblen jumps + NUL stop): the
// INDEPENDENT oracle for what a length-staged fill must have produced for
// arbitrary bytes — including invalid UTF-8 and embedded NUL, which the
// datum-lane path can only demote on.
fn oracle_c_walk_charlen(p: &[u8]) -> i64 {
    let (mut n, mut i) = (0i64, 0usize);
    while i < p.len() && p[i] != 0 {
        let b = p[i];
        i += if b & 0x80 == 0 {
            1
        } else if b & 0xE0 == 0xC0 {
            2
        } else if b & 0xF0 == 0xE0 {
            3
        } else if b & 0xF8 == 0xF0 {
            4
        } else {
            1
        };
        n += 1;
    }
    n
}

#[test]
fn len_staged_lane_folds_i64_and_skips_guards() {
    install_utf8_seams();
    let mcx = leaked_mcx();
    // Length inputs incl. C-walk answers for payloads the datum-lane path
    // must demote on (invalid UTF-8, embedded NUL) — the staged fill
    // computes C's answer, so the fold takes them WITHOUT a demote.
    let payloads: Vec<Option<Vec<u8>>> = vec![
        Some(b"http://a.example/x?q=1".to_vec()),
        Some("héllo".as_bytes().to_vec()),
        None,
        Some("日本語のページ".as_bytes().to_vec()),
        Some(b"".to_vec()),
        Some(vec![0xE9, b'x', b'y']),        // lone lead byte (invalid UTF-8)
        Some(vec![b'a', 0x00, b'b']),        // embedded NUL (C NUL-stops)
        Some(vec![0x80, 0x80]),              // bare continuations
        Some(b"plain".to_vec()),
    ];
    for (charlen, avg_fn, mm_fn) in [(true, 1963u32, 768u32), (false, 1963, 769)] {
        let fnoid = if charlen { F_TEXTLEN_T } else { F_OCTETLEN_T };
        let a1 = arg_list(mcx, mk_len_fn(mcx, fnoid, mk_var(mcx, 1, TEXTV)));
        let a2 = arg_list(mcx, mk_len_fn(mcx, fnoid, mk_var(mcx, 1, TEXTV)));
        let specs = [mk_spec(avg_fn, false, &a1), mk_spec(mm_fn, true, &a2)];
        let plan = classify(mcx, &specs).expect("admits");
        assert!(plan.guarded, "length plans carry the vguard obligation");
        assert_eq!(plan.uguards.is_empty(), !charlen);
        let lens: Vec<Option<i64>> = payloads
            .iter()
            .map(|p| {
                p.as_ref().map(|p| {
                    if charlen {
                        oracle_c_walk_charlen(p)
                    } else {
                        p.len() as i64
                    }
                })
            })
            .collect();
        let cols = LenStagedCols {
            values: lens.iter().map(|l| l.map_or(Datum::null(), Datum::from_i64)).collect(),
            isnull: lens.iter().map(|l| l.is_none()).collect(),
        };
        let sel = |i: usize| i != 8; // exercise the selection mask too
        let rows = selmask(payloads.len(), sel);
        // Guard skips: i64 lanes never demote and never deref — data stays
        // false (no guard walked).
        // SAFETY: len-staged lanes carry no datum pointers; the skip IS the
        // contract under test.
        let gc = unsafe { check_guards(&plan, &cols, &rows, |_| None) };
        assert_eq!(gc, GuardCheck::Pass { zone: false, data: false });
        let mut pgs = pergroups_for(mcx, &plan, specs.len());
        // SAFETY: pgs covers every transno; the staged lane covers every row.
        unsafe {
            fold_batch(
                &plan,
                &cols,
                &rows,
                payloads.len(),
                NonNull::new(pgs.as_mut_ptr()).unwrap(),
                mcx,
            )
            .expect("fold");
        }
        let lens_rows: Vec<Vec<Option<i64>>> = lens.iter().map(|l| vec![*l]).collect();
        let want = reference_fold(mcx, &plan, &lens_rows, sel, specs.len());
        assert_parity(&plan, &pgs, &want);

        // Grouped path (fold_rows_grouped) reads the staged widths too.
        let mut pg_a = pergroups_for(mcx, &plan, specs.len());
        let mut pg_b = pergroups_for(mcx, &plan, specs.len());
        let live: Vec<u32> =
            (0..payloads.len() as u32).filter(|&i| !cols.isnull[i as usize]).collect();
        let groups: Vec<NonNull<AggPerGroup>> = live
            .iter()
            .map(|&i| {
                NonNull::new(if i % 2 == 0 { pg_a.as_mut_ptr() } else { pg_b.as_mut_ptr() })
                    .unwrap()
            })
            .collect();
        // SAFETY: as above; groups snapshot per staged row.
        unsafe { fold_rows_grouped(&plan, &cols, &live, &groups, mcx).expect("fold") };
        let want_a =
            reference_fold(mcx, &plan, &lens_rows, |i| !cols.isnull[i] && i % 2 == 0, specs.len());
        let want_b =
            reference_fold(mcx, &plan, &lens_rows, |i| !cols.isnull[i] && i % 2 == 1, specs.len());
        assert_parity(&plan, &pg_a, &want_a);
        assert_parity(&plan, &pg_b, &want_b);
    }
}

// ===========================================================================
// Str MIN/MAX dict-code lanes (lane-v2-dictminmax): the col_codes side
// channel — sorted-dict integer folds must be image-identical to the datum
// memcmp path (itself parity-tested against the per-row C reference above).
// ===========================================================================

// One epoch's dictionary + a coded lane over it. The dict is byte-sorted +
// deduplicated exactly like the pgrcolumnar writer (payload slice cmp — memcmp
// then length, i.e. varstrfastcmp_c's order); values cells are dict[code]
// (the Raw gather's pointer identity).
struct DictEpoch {
    dict: Vec<Datum>,
    epoch: u64,
}

impl DictEpoch {
    fn new(epoch: u64, payloads: &[&[u8]]) -> DictEpoch {
        let mut entries: Vec<Vec<u8>> = payloads.iter().map(|p| p.to_vec()).collect();
        entries.sort();
        entries.dedup();
        // Mix header forms across entries (short where legal) — the code
        // path must be form-agnostic (the datum is whatever decode built).
        let dict: Vec<Datum> = entries
            .iter()
            .enumerate()
            .map(|(i, p)| {
                if i % 2 == 0 && p.len() + 1 <= 0x7F {
                    vl_short(p)
                } else {
                    vl_4b(p)
                }
            })
            .collect();
        DictEpoch { dict, epoch }
    }

    fn code_of(&self, payload: &[u8]) -> u32 {
        self.dict
            .iter()
            .position(|&d| vl_payload(d) == payload)
            .expect("payload in dict") as u32
    }
}

// TestCols + the coded view of column 0 (NULL rows carry code 0 — never
// read: the kernels are strict).
struct DictTestCols {
    cols: TestCols,
    codes: Vec<u32>,
    dict: *const Datum,
    ndict: u32,
    epoch: u64,
    sorted: bool,
}

impl DictTestCols {
    fn new(ep: &DictEpoch, rows: &[Option<u32>], sorted: bool) -> DictTestCols {
        let data: Vec<Vec<Option<Datum>>> =
            rows.iter().map(|c| vec![c.map(|c| ep.dict[c as usize])]).collect();
        DictTestCols {
            cols: TestCols::from_datum_rows(1, &data),
            codes: rows.iter().map(|c| c.unwrap_or(0)).collect(),
            dict: ep.dict.as_ptr(),
            ndict: ep.dict.len() as u32,
            epoch: ep.epoch,
            sorted,
        }
    }
}

impl LaneCols for DictTestCols {
    fn col_values(&self, c: usize) -> &[Datum] {
        self.cols.col_values(c)
    }

    fn col_isnull(&self, c: usize) -> &[bool] {
        self.cols.col_isnull(c)
    }

    fn col_codes(&self, c: usize) -> Option<::exectuples::SoaDictLane> {
        assert_eq!(c, 0);
        Some(::exectuples::SoaDictLane {
            codes: self.codes.as_ptr(),
            table: ::exectuples::SoaDictTable {
                dict: self.dict,
                ndict: self.ndict,
                epoch: self.epoch,
                sorted: self.sorted,
                stitch: std::ptr::null(),
                gndv: 0,
                gepoch: 0,
                lazy: core::ptr::null(),
                lazy_ensure: None,
                lazy_ensure_all: None,
                // Separate per-image Vec allocations: NO whole-span
                // readability witness (F-R1-1).
                contig: false,
            },
        })
    }
}

// The dict payload pool: multibyte UTF-8, NUL-adjacent bytes (embedded NUL,
// 0x00/0x01/0xFF extremes), prefix pairs (length tiebreak), empty string.
fn dict_payload_pool() -> Vec<&'static [u8]> {
    vec![
        b"",
        b"\x00",
        b"\x00a",
        b"\x01",
        b"a",
        b"ab",
        b"abc",
        "é".as_bytes(),
        "漢字".as_bytes(),
        "🦀".as_bytes(),
        b"zz",
        b"z\x00z",
        b"\xff",
        b"\xff\xff",
    ]
}

#[test]
fn fold_batch_dictcode_parity() {
    let mcx = leaked_mcx();
    let ep = DictEpoch::new(7, &dict_payload_pool());
    let n = 200usize;
    let rows: Vec<Option<u32>> = (0..n)
        .map(|i| if i % 6 == 2 { None } else { Some(((i * 13) % ep.dict.len()) as u32) })
        .collect();
    for (transfn, kind) in [(459, LaneKind::StrMin), (458, LaneKind::StrMax)] {
        let a = arg_list(mcx, mk_var(mcx, 1, TEXTV));
        let specs = [mk_spec_coll(transfn, &a, COLL_C)];
        let plan = classify(mcx, &specs).expect("admits");
        for sel in [
            (|_: usize| true) as fn(usize) -> bool,
            |i| i % 2 == 0,
            |i| i % 9 == 5,
            |_| false,
        ] {
            let selm = selmask(n, sel);
            for sorted in [true, false] {
                let cols = DictTestCols::new(&ep, &rows, sorted);
                let mut pgs = pergroups_for(mcx, &plan, 1);
                // SAFETY: live inline varlena lane (dict entries); pgs
                // covers transno 0; mcx is the live test arena.
                unsafe {
                    fold_batch(
                        &plan,
                        &cols,
                        &selm,
                        n,
                        NonNull::new(pgs.as_mut_ptr()).unwrap(),
                        mcx,
                    )
                    .expect("fold");
                }
                let drows: Vec<Option<Datum>> =
                    rows.iter().map(|c| c.map(|c| ep.dict[c as usize])).collect();
                let want = reference_fold_str(kind, &drows, sel);
                let pg = &pgs[0];
                match want {
                    None => assert!(pg.no_trans_value),
                    Some(img) => {
                        assert!(!pg.trans_value_is_null && !pg.no_trans_value);
                        assert_eq!(
                            vl_image(pg.trans_value),
                            img,
                            "kind {kind:?} sorted={sorted}"
                        );
                    }
                }
            }
        }
    }
}

// Grouped dict-code memo vs the plain grouped fold (itself parity-tested
// against the per-row C reference): multiple batches, epoch changes with
// overlapping value sets (cross-epoch survivors exercise the ¬tv_code memo
// arm), repeated codes (tie replaces), interleaved groups, a mid-run
// out-of-band advance with the mandated invalidate, and a Raw (code-less)
// batch in the middle.
#[test]
fn fold_rows_grouped_dictcode_memo_parity() {
    let mcx = leaked_mcx();
    let a_min = arg_list(mcx, mk_var(mcx, 1, TEXTV));
    let a_max = arg_list(mcx, mk_var(mcx, 1, TEXTV));
    let specs = [mk_spec_coll(459, &a_min, COLL_C), mk_spec_coll(458, &a_max, COLL_C)];
    let plan = classify(mcx, &specs).expect("admits");
    // Epoch 1 holds the global extremes; epoch 2 overlaps in the middle —
    // epoch-2 batches then advance groups whose transvalue is an epoch-1
    // survivor (the ¬tv_code arm), including codes better AND worse than
    // the memoized best.
    let ep1 = DictEpoch::new(1, &dict_payload_pool());
    let pool2: Vec<&[u8]> = vec![b"a", b"ab", b"m", b"mm", b"zz", b"\x01", "é".as_bytes()];
    let ep2 = DictEpoch::new(2, &pool2);
    let ngroups = 5usize;
    let ntrans = specs.len();
    let mut mm_pgs: Vec<Vec<AggPerGroup>> =
        (0..ngroups).map(|_| pergroups_for(mcx, &plan, ntrans)).collect();
    let mut ref_pgs: Vec<Vec<AggPerGroup>> =
        (0..ngroups).map(|_| pergroups_for(mcx, &plan, ntrans)).collect();
    let mut mm = StrMmScratch::default();
    // Batch schedule: (epoch, row codes). Repeats within a batch exercise
    // the tie-replace arm; descending code runs exercise better-code arms.
    let batches: Vec<(&DictEpoch, Vec<Option<u32>>)> = vec![
        (&ep1, (0..90).map(|i| Some(((i * 7) % ep1.dict.len()) as u32)).collect()),
        (&ep1, (0..90).rev().map(|i| Some(((i * 3) % ep1.dict.len()) as u32)).collect()),
        (&ep2, (0..70).map(|i| Some(((i * 5) % ep2.dict.len()) as u32)).collect()),
        (&ep2, vec![Some(3), Some(3), Some(3), None, Some(0), Some(0), Some(6), Some(2)]),
        (&ep2, (0..70).rev().map(|i| Some((i % ep2.dict.len()) as u32)).collect()),
    ];
    let run_batch = |ep: &DictEpoch,
                     rows: &[Option<u32>],
                     pgs: &mut [Vec<AggPerGroup>],
                     mm: Option<&mut StrMmScratch>| {
        let cols = DictTestCols::new(ep, rows, true);
        let idxs: Vec<u32> = (0..rows.len() as u32).collect();
        let groups: Vec<NonNull<AggPerGroup>> = (0..rows.len())
            .map(|i| NonNull::new(pgs[i % ngroups].as_mut_ptr()).unwrap())
            .collect();
        // SAFETY: per-group arrays cover every transno and are not moved
        // while the pointers live; the lane holds live inline varlenas;
        // codes satisfy the col_codes contract by construction.
        unsafe { fold_rows_grouped_mm(&plan, &cols, &idxs, &groups, mcx, mm, 0, None).expect("fold") };
    };
    for (bi, (ep, rows)) in batches.iter().enumerate() {
        run_batch(ep, rows, &mut mm_pgs, Some(&mut mm));
        run_batch(ep, rows, &mut ref_pgs, None);
        if bi == 2 {
            // Out-of-band advance (a demoted row's per-row program) on both
            // sides, then the mandated invalidate on the memo side.
            let d = vl_short(b"\x00oob");
            for g in 0..ngroups {
                for tn in 0..ntrans {
                    // SAFETY: live pergroup cells + inline varlena + arena.
                    unsafe {
                        str_advance(&plan.trans[tn], &mut mm_pgs[g][tn], d, mcx, None).expect("adv");
                        str_advance(&plan.trans[tn], &mut ref_pgs[g][tn], d, mcx, None).expect("adv");
                    }
                }
            }
            mm.invalidate();
        }
        if bi == 3 {
            // A Raw (code-less) window between coded ones: the datum path
            // advances behind the memo — which must therefore miss (same
            // epoch would otherwise reuse it). The kernel takes the datum
            // path when col_codes is None; emulate with the plain entry
            // point + invalidate (the feed's rule for any bypass).
            let raw_rows: Vec<Option<u32>> = vec![Some(1), Some(4), Some(2)];
            run_batch(&ep2, &raw_rows, &mut mm_pgs, None);
            run_batch(&ep2, &raw_rows, &mut ref_pgs, None);
            mm.invalidate();
        }
    }
    for g in 0..ngroups {
        for tn in 0..ntrans {
            let (a, b) = (&mm_pgs[g][tn], &ref_pgs[g][tn]);
            assert_eq!(a.no_trans_value, b.no_trans_value, "group {g} transno {tn}");
            assert_eq!(a.trans_value_is_null, b.trans_value_is_null, "group {g} transno {tn}");
            if !a.trans_value_is_null && !a.no_trans_value {
                assert_eq!(
                    vl_image(a.trans_value),
                    vl_image(b.trans_value),
                    "group {g} transno {tn}"
                );
            }
        }
    }
}

// Table-owned str state store (GL-DICTDRAIN-3): arena-backed advances must
// be byte-identical to the context-backed reference across install /
// tie-replace / better-code / cross-epoch arms (memo on AND off), and the
// store must recycle replaced chunks (LIFO within a size class) with exact
// big-value frees — the allocator-exactness a thread-migrating table needs.
#[test]
fn str_state_arena_fold_parity_and_recycle() {
    let mcx = leaked_mcx();
    let a_min = arg_list(mcx, mk_var(mcx, 1, TEXTV));
    let a_max = arg_list(mcx, mk_var(mcx, 1, TEXTV));
    let specs = [mk_spec_coll(459, &a_min, COLL_C), mk_spec_coll(458, &a_max, COLL_C)];
    let plan = classify(mcx, &specs).expect("admits");
    let ep1 = DictEpoch::new(1, &dict_payload_pool());
    let pool2: Vec<&[u8]> = vec![b"a", b"ab", b"m", b"mm", b"zz", b"\x01"];
    let ep2 = DictEpoch::new(2, &pool2);
    let ngroups = 4usize;
    let ntrans = specs.len();
    let mut sa_pgs: Vec<Vec<AggPerGroup>> =
        (0..ngroups).map(|_| pergroups_for(mcx, &plan, ntrans)).collect();
    let mut ref_pgs: Vec<Vec<AggPerGroup>> =
        (0..ngroups).map(|_| pergroups_for(mcx, &plan, ntrans)).collect();
    let mut mm = StrMmScratch::default();
    let mut arena = StrStateArena::default();
    let batches: Vec<(&DictEpoch, Vec<Option<u32>>)> = vec![
        (&ep1, (0..60).map(|i| Some(((i * 7) % ep1.dict.len()) as u32)).collect()),
        // Repeats: the tie-replace arm (per-row replace churn = recycling).
        (&ep1, vec![Some(2); 40]),
        (&ep2, (0..50).rev().map(|i| Some((i % ep2.dict.len()) as u32)).collect()),
    ];
    for (memo, (ep, rows)) in [(true, &batches[0]), (false, &batches[1]), (true, &batches[2])]
    {
        let cols = DictTestCols::new(ep, rows, true);
        let idxs: Vec<u32> = (0..rows.len() as u32).collect();
        let sa_groups: Vec<NonNull<AggPerGroup>> = (0..rows.len())
            .map(|i| NonNull::new(sa_pgs[i % ngroups].as_mut_ptr()).unwrap())
            .collect();
        let ref_groups: Vec<NonNull<AggPerGroup>> = (0..rows.len())
            .map(|i| NonNull::new(ref_pgs[i % ngroups].as_mut_ptr()).unwrap())
            .collect();
        if !memo {
            mm.invalidate();
        }
        // SAFETY: per-group arrays cover every transno and are not moved
        // while the pointers live; the lane holds live inline varlenas;
        // codes satisfy the col_codes contract by construction; the arena
        // owns every prior arena-side copy.
        unsafe {
            fold_rows_grouped_mm(
                &plan,
                &cols,
                &idxs,
                &sa_groups,
                mcx,
                memo.then_some(&mut mm),
                0,
                Some(&mut arena),
            )
            .expect("arena fold");
            fold_rows_grouped_mm(&plan, &cols, &idxs, &ref_groups, mcx, None, 0, None)
                .expect("ref fold");
        }
    }
    for g in 0..ngroups {
        for tn in 0..ntrans {
            let (a, b) = (&sa_pgs[g][tn], &ref_pgs[g][tn]);
            assert_eq!(a.no_trans_value, b.no_trans_value, "group {g} transno {tn}");
            assert_eq!(a.trans_value_is_null, b.trans_value_is_null, "group {g} transno {tn}");
            if !a.trans_value_is_null && !a.no_trans_value {
                assert_eq!(
                    vl_image(a.trans_value),
                    vl_image(b.trans_value),
                    "group {g} transno {tn}"
                );
            }
        }
    }
    assert!(arena.bytes() > 0, "the arena hosted the copies");
    // Recycle law: a replace parks the superseded chunk and the next
    // same-class copy reuses it (LIFO) — the churn-bounding half of the
    // GL-DICTDRAIN-2 fix, now allocator-exact by construction.
    let mut a2 = StrStateArena::default();
    // SAFETY: vl_short/vl_4b produce live inline varlenas.
    unsafe {
        let d1 = a2.copy(vl_short(b"aaaa"));
        let addr1 = d1.as_usize();
        let d2 = a2.replace(d1, vl_short(b"bbbb"));
        assert_ne!(d2.as_usize(), addr1, "copy-first: new copy precedes the free");
        let d3 = a2.copy(vl_short(b"cccc"));
        assert_eq!(d3.as_usize(), addr1, "parked chunk reused LIFO within its class");
        assert_eq!(vl_image(d3), vl_image(vl_short(b"cccc")));
        // Big values (> 1024 B): exact boxes, freed exactly on replace.
        let big_payload = vec![0x61u8; 4000];
        let before = a2.bytes();
        let db = a2.copy(vl_4b(&big_payload));
        assert!(a2.bytes() > before, "big value charged");
        let d4 = a2.replace(db, vl_short(b"tail"));
        assert_eq!(a2.bytes(), before, "big value released exactly on replace");
        assert_eq!(vl_image(d4), vl_image(vl_short(b"tail")));
    }
}

// ===========================================================================
// Code-histogram kernels (lane-v2-codehist): fold_code_group with
// multiplicity n must be bit-identical to n per-row transitions.
// ===========================================================================

// GL-SINKCRASH-2: the by-ref str transvalue CLASS PREDICATE. Every kind that
// `fold_rows_grouped_mm` routes through `str_advance` — i.e. every kind whose
// transvalue is a heap-allocated varlena copy that needs an allocator whose
// lifetime is the TABLE's — must be reported by `plan_has_str_trans`, because
// that one predicate is what arms the table-owned `StrStateArena` on a sink
// build and what fails the build closed if it was not armed.
//
// This test exists because the release blocker it guards was caused by exactly
// the failure it checks for: the discipline was applied per drain arm instead of
// per class, so two of the three str-capable sink drains silently copied
// min/max(text) transvalues into a pool thread's aggcontext, whose lifetime is
// that thread's binding rather than the table's. A kind added to `str_advance`'s
// arm without being added here re-opens that hole silently — so this asserts the
// predicate from the OTHER side too: any kind the fold sends to `str_advance`
// and this predicate misses is a bug, and vice versa.
#[test]
fn plan_has_str_trans_covers_every_str_advance_kind() {
    let mcx = leaked_mcx();
    install_utf8_seams();
    let empty = NodeList::default();

    // min(text) / max(text): the reachable sink surface (combinefns 459/458).
    for oid in [459u32, 458] {
        let a = arg_list(mcx, mk_var(mcx, 1, TEXTV));
        let plan = classify(mcx, &[mk_spec_coll(oid, &a, COLL_C)]).expect("admits");
        assert!(
            plan_has_str_trans(&plan),
            "min/max(text) (oid {oid}) must be reported as a byref str transvalue"
        );
    }

    // bpchar min/max: NOT admitted by the sink's combine whitelist today, but
    // `fold_rows_grouped_mm` already routes BpMin/BpMax through `str_advance`,
    // so it is the same allocator-lifetime hazard. It must be covered NOW, so
    // that adding a bpchar oid to that whitelist cannot inherit an unarmed
    // store.
    for oid in [1064u32, 1063] {
        let c = arg_list(mcx, mk_var(mcx, 1, BPCHARV));
        let Some(plan) = classify(mcx, &[mk_spec_coll(oid, &c, COLL_C)]) else {
            continue;
        };
        assert!(
            plan_has_str_trans(&plan),
            "bpchar min/max (oid {oid}) routes through str_advance and must be covered"
        );
    }

    // Negative side: a plan with no varlena transvalue must NOT arm the store —
    // byval shapes have to stay allocation-identical, which is the whole reason
    // the predicate is narrow rather than "any sink build".
    let plan_byval = classify(mcx, &[mk_spec(1219, false, &empty)]).expect("admits");
    assert!(
        !plan_has_str_trans(&plan_byval),
        "count(*) has no byref str transvalue and must not arm a state store"
    );
    let len = arg_list(mcx, mk_len_fn(mcx, F_TEXTLEN_T, mk_var(mcx, 1, TEXTV)));
    let plan_len = classify(mcx, &[mk_spec(1963, false, &len)]).expect("admits");
    assert!(
        !plan_has_str_trans(&plan_len),
        "avg(length(text)) reads a text column but its TRANSVALUE is byval"
    );
    // A max() over a text column whose TRANSVALUE is an int: reads text, is not
    // this class. The predicate keys on the transvalue, never on the input.
    let oct = arg_list(mcx, mk_len_fn(mcx, F_OCTETLEN_T, mk_var(mcx, 1, TEXTV)));
    let plan_octet = classify(mcx, &[mk_spec(768, true, &oct)]).expect("admits");
    assert!(
        !plan_has_str_trans(&plan_octet),
        "max(octet_length(text)) has an int transvalue"
    );
}

#[test]
fn plan_code_hostable_admission() {
    let mcx = leaked_mcx();
    install_utf8_seams();
    let empty = NodeList::default();
    // Hostable: count(*), min(url), max(url), avg(length(url)),
    // sum(octet_length(url)), max(octet_length(url)) — one common column.
    let a1 = arg_list(mcx, mk_var(mcx, 1, TEXTV));
    let a2 = arg_list(mcx, mk_var(mcx, 1, TEXTV));
    let a3 = arg_list(mcx, mk_len_fn(mcx, F_TEXTLEN_T, mk_var(mcx, 1, TEXTV)));
    let a4 = arg_list(mcx, mk_len_fn(mcx, F_OCTETLEN_T, mk_var(mcx, 1, TEXTV)));
    let a5 = arg_list(mcx, mk_len_fn(mcx, F_OCTETLEN_T, mk_var(mcx, 1, TEXTV)));
    let specs = [
        mk_spec(1219, false, &empty),
        mk_spec_coll(459, &a1, COLL_C),
        mk_spec_coll(458, &a2, COLL_C),
        mk_spec(1963, false, &a3),
        mk_spec(1841, true, &a4),
        mk_spec(768, true, &a5),
    ];
    let plan = classify(mcx, &specs).expect("admits");
    assert!(plan.resid.is_empty());
    assert_eq!(plan_code_hostable(&plan), Some(0));
    // Two different columns refuse.
    let b1 = arg_list(mcx, mk_var(mcx, 1, TEXTV));
    let b2 = arg_list(mcx, mk_var(mcx, 2, TEXTV));
    let specs2 = [mk_spec_coll(459, &b1, COLL_C), mk_spec_coll(458, &b2, COLL_C)];
    let plan2 = classify(mcx, &specs2).expect("admits");
    assert_eq!(plan_code_hostable(&plan2), None);
    // Pure count(*) refuses (nothing code-valued).
    let specs3 = [mk_spec(1219, false, &empty)];
    let plan3 = classify(mcx, &specs3).expect("admits");
    assert_eq!(plan_code_hostable(&plan3), None);
    // bpchar min refuses (its kind is not multiplicity-legal here).
    let c1 = arg_list(mcx, mk_var(mcx, 1, BPCHARV));
    let specs4 = [mk_spec_coll(1064, &c1, COLL_C)];
    let plan4 = classify(mcx, &specs4).expect("admits");
    assert_eq!(plan_code_hostable(&plan4), None);
}

#[test]
fn datum_code_guards() {
    let mcx = leaked_mcx();
    install_utf8_seams();
    // Chars-width lane (uguard) + str min (vguard), one column.
    let a1 = arg_list(mcx, mk_var(mcx, 1, TEXTV));
    let a2 = arg_list(mcx, mk_len_fn(mcx, F_TEXTLEN_T, mk_var(mcx, 1, TEXTV)));
    let specs = [mk_spec_coll(459, &a1, COLL_C), mk_spec(1963, false, &a2)];
    let plan = classify(mcx, &specs).expect("admits");
    assert!(!plan.vguards.is_empty() && !plan.uguards.is_empty());
    // SAFETY: live inline varlena test datums (or header-readable fakes).
    unsafe {
        assert!(datum_code_guards_ok(&plan, vl_short(b"ok")));
        assert!(datum_code_guards_ok(&plan, vl_4b("é漢🦀".as_bytes())));
        // Non-inline forms fail the vguard half.
        assert!(!datum_code_guards_ok(&plan, vl_4b_compressed_fake()));
        assert!(!datum_code_guards_ok(&plan, vl_external_fake()));
        // Invalid UTF-8 / embedded NUL fail the uguard half (chars lane).
        assert!(!datum_code_guards_ok(&plan, vl_short(b"\xff\xfe")));
        assert!(!datum_code_guards_ok(&plan, vl_short(b"a\x00b")));
    }
}

// fold_code_group(n) vs n per-row fold_rows_grouped advances, per kind mix,
// across (group, code) interleavings and epoch changes (a flush at ANY
// point must be byte-invisible on transvalues).
#[test]
fn fold_code_group_multiplicity_parity() {
    let mcx = leaked_mcx();
    install_utf8_seams();
    let empty = NodeList::default();
    let a1 = arg_list(mcx, mk_var(mcx, 1, TEXTV));
    let a2 = arg_list(mcx, mk_var(mcx, 1, TEXTV));
    let a3 = arg_list(mcx, mk_len_fn(mcx, F_TEXTLEN_T, mk_var(mcx, 1, TEXTV)));
    let a4 = arg_list(mcx, mk_len_fn(mcx, F_OCTETLEN_T, mk_var(mcx, 1, TEXTV)));
    let a5 = arg_list(mcx, mk_len_fn(mcx, F_OCTETLEN_T, mk_var(mcx, 1, TEXTV)));
    let specs = [
        mk_spec(1219, false, &empty),   // count(*)
        mk_spec_coll(459, &a1, COLL_C), // min(text)
        mk_spec_coll(458, &a2, COLL_C), // max(text)
        mk_spec(1963, false, &a3),      // avg(length)
        mk_spec(1841, true, &a4),       // sum(octet_length)
        mk_spec(768, true, &a5),        // max(octet_length)
    ];
    let plan = classify(mcx, &specs).expect("admits");
    assert_eq!(plan_code_hostable(&plan), Some(0));
    let ntrans = specs.len();
    // Two epochs' dictionaries with overlapping values (cross-epoch ties).
    let ep1 = DictEpoch::new(1, &dict_payload_pool_utf8());
    let pool2: Vec<&[u8]> = vec![b"a", b"ab", b"mm", b"zz", "\u{e9}".as_bytes()];
    let ep2 = DictEpoch::new(2, &pool2);
    let ngroups = 3usize;
    // (epoch, group, code, multiplicity) advance script, first-occurrence
    // order per epoch; the 65536 case pins the epoch-max multiplicity.
    let script: Vec<(&DictEpoch, usize, u32, i64)> = vec![
        (&ep1, 0, 0, 7),
        (&ep1, 1, 3, 1),
        (&ep1, 0, 5, 4),
        (&ep1, 2, 0, 2),
        (&ep1, 1, 0, 65536),
        (&ep2, 0, 1, 3),
        (&ep2, 1, 4, 9),
        (&ep2, 0, 0, 1),
        (&ep2, 2, 2, 5),
    ];
    let mut code_pgs: Vec<Vec<AggPerGroup>> =
        (0..ngroups).map(|_| pergroups_for(mcx, &plan, ntrans)).collect();
    let mut ref_pgs: Vec<Vec<AggPerGroup>> =
        (0..ngroups).map(|_| pergroups_for(mcx, &plan, ntrans)).collect();
    let mut vals: Vec<i64> = Vec::new();
    for &(ep, g, code, n) in &script {
        let d = ep.dict[code as usize];
        // SAFETY: live inline varlena dict entries; the pools are valid
        // UTF-8 without NUL, so the guards hold by construction.
        unsafe {
            assert!(datum_code_guards_ok(&plan, d));
            code_trans_vals(&plan, d, &mut vals);
            fold_code_group(
                &plan,
                &vals,
                d,
                n,
                NonNull::new(code_pgs[g].as_mut_ptr()).unwrap(),
                mcx,
                0,
            )
            .expect("code fold");
        }
        // Reference: n single-row grouped advances through the per-row
        // kernel (itself parity-tested against the C reference above).
        let data = vec![vec![Some(d)]];
        let cols = TestCols::from_datum_rows(1, &data);
        let idxs = vec![0u32];
        for _ in 0..n {
            let groups = vec![NonNull::new(ref_pgs[g].as_mut_ptr()).unwrap()];
            // SAFETY: live pergroups + inline varlena lane.
            unsafe { fold_rows_grouped(&plan, &cols, &idxs, &groups, mcx).expect("ref") };
        }
    }
    for g in 0..ngroups {
        for tn in 0..ntrans {
            let (a, b) = (&code_pgs[g][tn], &ref_pgs[g][tn]);
            assert_eq!(a.no_trans_value, b.no_trans_value, "g{g} t{tn}");
            assert_eq!(a.trans_value_is_null, b.trans_value_is_null, "g{g} t{tn}");
            if !a.trans_value_is_null && !a.no_trans_value {
                match plan.trans[tn].kind {
                    // By-ref transvalues (str winners, avg int8[2]
                    // transarrays): compare the full varlena image.
                    LaneKind::StrMin | LaneKind::StrMax | LaneKind::AvgAccum => {
                        assert_eq!(
                            vl_image(a.trans_value),
                            vl_image(b.trans_value),
                            "g{g} t{tn}"
                        );
                    }
                    _ => assert_eq!(
                        a.trans_value.as_i64(),
                        b.trans_value.as_i64(),
                        "g{g} t{tn}"
                    ),
                }
            }
        }
    }
}

// UTF-8-only payload pool (the codehist plans carry chars lanes whose
// uguard the dict pool must satisfy).
fn dict_payload_pool_utf8() -> Vec<&'static [u8]> {
    vec![
        b"",
        b"\x01",
        b"a",
        b"ab",
        b"abc",
        "é".as_bytes(),
        "漢字".as_bytes(),
        "🦀".as_bytes(),
        b"zz",
        b"mid-500",
    ]
}
// ---- granule-metadata fold (lane-v2-lenfooter) -----------------------------

#[test]
fn granule_meta_admission_matrix() {
    install_utf8_seams();
    let mcx = leaked_mcx();
    let cs_args = NodeList::nil();
    let a_oct = arg_list(mcx, mk_len_fn(mcx, F_OCTETLEN_T, mk_var(mcx, 1, TEXTV)));
    let a_chr = arg_list(mcx, mk_len_fn(mcx, F_TEXTLEN_T, mk_var(mcx, 1, TEXTV)));
    let a_var = arg_list(mcx, mk_var(mcx, 2, TEXTV));
    // count(*) + count(col) + avg/sum(octet_length): admits, one length col.
    let specs = [
        mk_spec(1219, false, &cs_args),
        mk_spec(2804, false, &a_var),
        mk_spec(1963, false, &a_oct),
        mk_spec(1841, true, &a_oct),
    ];
    let plan = classify(mcx, &specs).expect("admits");
    assert!(plan.resid.is_empty());
    assert_eq!(granule_meta_len_cols(&plan), Some(vec![0u16]));
    // Char-count lanes refuse (footer sums carry byte lengths).
    let specs_chr = [mk_spec(1963, false, &a_chr)];
    let plan_chr = classify(mcx, &specs_chr).expect("admits");
    assert_eq!(granule_meta_len_cols(&plan_chr), None);
    // Any other kind refuses (min over length).
    let specs_min = [mk_spec(769, true, &a_oct)];
    let plan_min = classify(mcx, &specs_min).expect("admits");
    assert_eq!(granule_meta_len_cols(&plan_min), None);
}

// fold_granule_meta vs fold_batch byte parity on the length()-arg transition family,
// under both qual shapes the meta arm hosts: no qual (all rows selected) and
// `col <> ''` (empty-string rows rejected). The footer-style inputs are
// (passing, Σ octet_length over ALL rows) — empties contribute zero, so the
// sum serves both selections.
#[test]
fn granule_meta_fold_matches_fold_batch() {
    install_utf8_seams();
    let mcx = leaked_mcx();
    let cs_args = NodeList::nil();
    let a_oct = arg_list(mcx, mk_len_fn(mcx, F_OCTETLEN_T, mk_var(mcx, 1, TEXTV)));
    let specs = [
        mk_spec(1219, false, &cs_args), // count(*)
        mk_spec(1963, false, &a_oct),   // avg(octet_length(c0))
        mk_spec(1841, true, &a_oct),    // sum(octet_length(c0))
    ];
    let plan = classify(mcx, &specs).expect("admits");
    assert_eq!(granule_meta_len_cols(&plan), Some(vec![0u16]));
    // A no-NULLs "granule" (pgrcolumnar stores no NULLs) with empties mixed in.
    let data: Vec<Option<Datum>> = (0..37)
        .map(|i| {
            Some(match i % 5 {
                0 => vl_short(b""),
                1 => vl_short("héllo".as_bytes()),
                k => vl_4b(&vec![b'u'; k * 17]),
            })
        })
        .collect();
    let n = data.len();
    let rows_datum: Vec<Vec<Option<Datum>>> = data.iter().map(|d| vec![*d]).collect();
    let cols = TestCols::from_datum_rows(1, &rows_datum);
    let footer_sum: i64 = data.iter().map(|d| oracle_bytelen(d.unwrap())).sum();
    let empties = data.iter().filter(|d| oracle_bytelen(d.unwrap()) == 0).count() as i64;
    for qual_ne_empty in [false, true] {
        let sel = |i: usize| !qual_ne_empty || oracle_bytelen(data[i].unwrap()) != 0;
        let rows = selmask(n, sel);
        // Reference: the ordinary staged fold over the same selection.
        let mut want = pergroups_for(mcx, &plan, specs.len());
        let gc = unsafe { check_guards(&plan, &cols, &rows, |_| None) };
        assert_eq!(gc, GuardCheck::Pass { zone: false, data: true });
        unsafe {
            fold_batch(&plan, &cols, &rows, n, NonNull::new(want.as_mut_ptr()).unwrap(), mcx)
                .expect("fold");
        }
        // Meta arm: footer arithmetic only.
        let passing = if qual_ne_empty { n as i64 - empties } else { n as i64 };
        let mut got = pergroups_for(mcx, &plan, specs.len());
        unsafe {
            fold_granule_meta(
                &plan,
                passing,
                |c| {
                    assert_eq!(c, 0);
                    footer_sum
                },
                NonNull::new(got.as_mut_ptr()).unwrap(),
            );
        }
        assert_parity(&plan, &got, &want);
    }
}

// ---- footer-stat meta fold (agg_meta_cols / fold_agg_meta) ----------------

// Admission matrix: count / plain int min-max / affine divk==1 int sum-avg /
// bare int8 sum-avg / plain octet_length sum admit; transforms on min/max,
// divk != 1 sums, and residual-carrying plans refuse. Guard columns join
// mm_cols for the caller's interval re-proof.
#[test]
fn agg_meta_cols_admission() {
    install_utf8_seams();
    let mcx = leaked_mcx();
    let count_star_args = NodeList::nil();
    let a0 = arg_list(mcx, mk_var(mcx, 1, INT4OID));
    let a2 = arg_list(mcx, mk_var(mcx, 3, INT8OID));
    let a3 = arg_list(mcx, mk_int_op(mcx, 4, INT4OID, 9, 177, true)); // v + 9 (guarded)
    let a4 = arg_list(mcx, mk_len_fn(mcx, F_OCTETLEN_T, mk_var(mcx, 5, TEXTV)));
    let specs = [
        mk_spec(1219, false, &count_star_args), // count(*)
        mk_spec(2804, false, &a0),              // count(v0)
        mk_spec(1841, true, &a0),               // sum(int4 v0)
        mk_spec(1963, false, &a0),              // avg(int4 v0)
        mk_spec(769, true, &a0),                // min(v0)
        mk_spec(768, true, &a0),                // max(v0)
        mk_spec(2746, true, &a2),               // sum(int8 v2)
        mk_spec(1841, true, &a3),               // sum(v3 + 9), data-guarded
        mk_spec(1841, true, &a4),               // sum(octet_length(v4))
    ];
    let plan = classify(mcx, &specs).expect("plan admits");
    assert!(plan.resid.is_empty());
    let cols = agg_meta_cols(&plan).expect("footer-answerable plan");
    assert_eq!(cols.mm_cols, vec![0u16, 3], "min/max col + guard col");
    assert_eq!(cols.sum_cols, vec![0u16, 2, 3]);
    assert_eq!(cols.len_cols, vec![4u16]);

    // Transformed min/max is not footer-derivable (min(v*3) != min(v)).
    let a_mm = arg_list(mcx, mk_int_op(mcx, 1, INT2OID, 3, 170, true)); // v * 3
    let plan = classify(mcx, &[mk_spec(769, true, &a_mm)]).expect("classify admits");
    assert!(agg_meta_cols(&plan).is_none(), "transformed min refused");
    // Integer division is not linear.
    let a_div = arg_list(mcx, mk_int_op(mcx, 1, INT2OID, 7, 172, true)); // v / 7
    let plan = classify(mcx, &[mk_spec(1841, true, &a_div)]).expect("classify admits");
    assert!(agg_meta_cols(&plan).is_none(), "divk sum refused");
    // Residual transitions keep the whole node on the scan drive.
    let a_bad = arg_list(mcx, mk_int_op(mcx, 2, INT2OID, 1000, F_INT42DIV, false));
    let plan =
        classify(mcx, &[mk_spec(1841, true, &a0), mk_spec(1841, true, &a_bad)]).expect("admits");
    assert!(!plan.resid.is_empty());
    assert!(agg_meta_cols(&plan).is_none(), "residual plan refused");
    // Char-count length lanes have no footer stats.
    let a_chr = arg_list(mcx, mk_len_fn(mcx, F_TEXTLEN_T, mk_var(mcx, 1, TEXTV)));
    let plan = classify(mcx, &[mk_spec(1841, true, &a_chr)]).expect("classify admits");
    assert!(agg_meta_cols(&plan).is_none(), "VarLenChars refused");
}

// fold_agg_meta vs fold_batch over the identical all-rows-passing unit: the
// footer triple (rows, exact (min, max), exact sums, Σ octet_length) is
// computed independently from the data and must reproduce fold_batch's
// pergroup bytes exactly (the bit-equality contract the plain fold drive's
// meta arm rides on).
#[test]
fn fold_agg_meta_parity_with_fold_batch() {
    install_utf8_seams();
    let mcx = leaked_mcx();
    let count_star_args = NodeList::nil();
    let a0 = arg_list(mcx, mk_var(mcx, 1, INT4OID));
    let a1 = arg_list(mcx, mk_var(mcx, 2, INT2OID));
    let a2 = arg_list(mcx, mk_var(mcx, 3, INT8OID));
    let a3 = arg_list(mcx, mk_int_op(mcx, 4, INT4OID, 9, 177, true)); // v + 9 (guarded)
    let a4 = arg_list(mcx, mk_len_fn(mcx, F_OCTETLEN_T, mk_var(mcx, 5, TEXTV)));
    let specs = [
        mk_spec(1219, false, &count_star_args), // count(*)
        mk_spec(2804, false, &a0),              // count(v0)
        mk_spec(1841, true, &a0),               // sum(int4 v0)
        mk_spec(1963, false, &a0),              // avg(int4 v0)
        mk_spec(769, true, &a0),                // min(v0)
        mk_spec(768, true, &a0),                // max(v0)
        mk_spec(771, true, &a1),                // min(int2 v1)
        mk_spec(2746, true, &a2),               // sum(int8 v2)
        mk_spec(1841, true, &a3),               // sum(v3 + 9)
        mk_spec(1841, true, &a4),               // sum(octet_length(v4))
    ];
    let plan = classify(mcx, &specs).expect("plan admits");
    let cols = agg_meta_cols(&plan).expect("footer-answerable plan");

    // A pgrcolumnar-shaped unit: NO NULLs, mixed signs, i64-extreme-free int8.
    let ints: Vec<(i64, i64, i64, i64)> = (0..300)
        .map(|i| {
            let i = i as i64;
            (
                (i * 37 % 2011) - 1000,
                (i * 13 % 251) - 125,
                (i * 104_729 % 1_000_003) - 500_000,
                (i * 7 % 401) - 200,
            )
        })
        .collect();
    let texts: Vec<Datum> = (0..300)
        .map(|i| match i % 4 {
            0 => vl_short(b"http://a.example/x?q=1"),
            1 => vl_short(b""),
            2 => vl_4b(&[b'a'; 300]),
            _ => vl_short("héllo".as_bytes()),
        })
        .collect();
    let rows_datum: Vec<Vec<Option<Datum>>> = ints
        .iter()
        .zip(&texts)
        .map(|(&(a, b, c, d), &t)| {
            vec![
                Some(Datum::from_i32(a as i32)),
                Some(Datum::from_i16(b as i16)),
                Some(Datum::from_i64(c)),
                Some(Datum::from_i32(d as i32)),
                Some(t),
            ]
        })
        .collect();
    let n = rows_datum.len();
    let tcols = TestCols::from_datum_rows(5, &rows_datum);
    let rows = selmask(n, |_| true);

    // Reference: the ordinary staged fold over the all-ones selection.
    let gc = unsafe { check_guards(&plan, &tcols, &rows, |_| None) };
    assert_eq!(gc, GuardCheck::Pass { zone: false, data: true });
    let mut want = pergroups_for(mcx, &plan, specs.len());
    unsafe {
        fold_batch(&plan, &tcols, &rows, n, NonNull::new(want.as_mut_ptr()).unwrap(), mcx)
            .expect("fold");
    }

    // Footer arithmetic, computed independently of the fold kernels.
    let int_col = |c: u16, i: usize| match c {
        0 => ints[i].0,
        1 => ints[i].1,
        2 => ints[i].2,
        3 => ints[i].3,
        _ => unreachable!(),
    };
    let mm_of = |c: u16| {
        let vals = (0..n).map(|i| int_col(c, i));
        (vals.clone().min().unwrap(), vals.max().unwrap())
    };
    let sum_of = |c: u16| (0..n).map(|i| int_col(c, i) as i128).sum::<i128>();
    let len_of = |c: u16| {
        assert_eq!(c, 4);
        texts.iter().map(|&d| oracle_bytelen(d)).sum::<i64>()
    };
    // The executor-side guard re-proof the arm performs before folding.
    for g in plan.guards.iter() {
        assert!(cols.mm_cols.contains(&g.col));
        let (mn, mx) = mm_of(g.col);
        assert!(g.lo <= mn && mx <= g.hi, "unit passes the guard interval");
    }
    let mut got = pergroups_for(mcx, &plan, specs.len());
    unsafe {
        fold_agg_meta(
            &plan,
            n as i64,
            mm_of,
            sum_of,
            len_of,
            NonNull::new(got.as_mut_ptr()).unwrap(),
            mcx,
        )
        .expect("meta fold");
    }
    assert_parity(&plan, &got, &want);

    // Two-unit accumulation (RG after RG): fold_batch over the doubled data
    // vs two meta applications — the arm folds unit-by-unit into live state.
    let mut want2 = pergroups_for(mcx, &plan, specs.len());
    unsafe {
        fold_batch(&plan, &tcols, &rows, n, NonNull::new(want2.as_mut_ptr()).unwrap(), mcx)
            .expect("fold");
        fold_batch(&plan, &tcols, &rows, n, NonNull::new(want2.as_mut_ptr()).unwrap(), mcx)
            .expect("fold");
    }
    let mut got2 = pergroups_for(mcx, &plan, specs.len());
    for _ in 0..2 {
        unsafe {
            fold_agg_meta(
                &plan,
                n as i64,
                mm_of,
                sum_of,
                len_of,
                NonNull::new(got2.as_mut_ptr()).unwrap(),
                mcx,
            )
            .expect("meta fold");
        }
    }
    assert_parity(&plan, &got2, &want2);
}

// ===========================================================================
// Fold-trans float tier (lane-v2-lanefold-trans, knob-gated): FSum / FAccum.
//
// The kernels are ORDER-PRESERVING — the whole game is that the fold applies
// C's per-row arithmetic in C's row order. The reference model below is an
// independent per-row loop (install-then-float_pl for SUM, accum from
// {0,0,0} for ACCUM); the ordering PIN then proves the datasets have teeth:
// a reassociated (smalls-first) evaluation of the same rows produces
// DIFFERENT bits, so bit-equality with the reference is only reachable by
// folding in row order.
//
// Knob discipline: the knob is a process-wide AtomicU8; every test here
// serializes on FOLD_TRANS_TEST_LOCK and leaves the knob ON (no other test
// in this binary classifies a float SUM/ACCUM oid, so the cached ON state
// cannot leak into unrelated assertions).
// ===========================================================================

static FOLD_TRANS_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn ft_guard() -> std::sync::MutexGuard<'static, ()> {
    let g = FOLD_TRANS_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    fold_trans_set_for_tests(true);
    g
}

// Per-row C reference for sum(float8)/sum(float4): strict + NULL init —
// install the first non-null input's bits, then state = float_pl(state, v).
fn ref_fsum_f64(rows: &[Option<f64>]) -> PgResult<Option<f64>> {
    let mut st: Option<f64> = None;
    for v in rows.iter().flatten() {
        st = Some(match st {
            None => *v,
            Some(s) => ::adt_float::float8_pl(s, *v)?,
        });
    }
    Ok(st)
}

fn ref_fsum_f32(rows: &[Option<f32>]) -> PgResult<Option<f32>> {
    let mut st: Option<f32> = None;
    for v in rows.iter().flatten() {
        st = Some(match st {
            None => *v,
            Some(s) => ::adt_float::float4_pl(s, *v)?,
        });
    }
    Ok(st)
}

// Per-row C reference for the accum family: '{0,0,0}' start, one
// float8_accum/float4_accum per non-null row in row order.
fn ref_faccum_f64(rows: &[Option<f64>]) -> PgResult<[f64; 3]> {
    let mut t = [0.0f64; 3];
    for v in rows.iter().flatten() {
        t = ::adt_float::aggregates::float8_accum(t, *v)?;
    }
    Ok(t)
}

fn ref_faccum_f32(rows: &[Option<f32>]) -> PgResult<[f64; 3]> {
    let mut t = [0.0f64; 3];
    for v in rows.iter().flatten() {
        t = ::adt_float::aggregates::float4_accum(t, *v)?;
    }
    Ok(t)
}

fn read_f8_transarray(pg: &AggPerGroup) -> [f64; 3] {
    assert!(!pg.trans_value_is_null);
    let p = pg.trans_value.as_usize() as *const u8;
    // SAFETY: new_float8_transarray image, arena-lived for the test.
    unsafe {
        let img = core::slice::from_raw_parts(p, ::types_tuple::varatt::varsize_4b(p));
        ::adt_float::aggregates::check_float8_array::<3>(img, "test").expect("shape")
    }
}

fn f64_rows(vals: &[Option<f64>]) -> Vec<Vec<Option<Datum>>> {
    vals.iter().map(|v| vec![v.map(Datum::from_f64)]).collect()
}

fn f32_rows(vals: &[Option<f32>]) -> Vec<Vec<Option<Datum>>> {
    vals.iter().map(|v| vec![v.map(Datum::from_f32)]).collect()
}

#[test]
fn fold_trans_knob_and_admission() {
    let _g = ft_guard();
    let mcx = leaked_mcx();
    let af8 = arg_list(mcx, mk_var(mcx, 1, F64V));
    let af4 = arg_list(mcx, mk_var(mcx, 1, F32V));
    // Knob OFF: every fold-trans oid refuses (byte-identical estate).
    fold_trans_set_for_tests(false);
    for (oid, null_init) in [(218, true), (204, true), (222, false), (208, false)] {
        let a = if oid == 218 || oid == 222 { &af8 } else { &af4 };
        assert!(classify_trans(&mk_spec(oid, null_init, a), 0).is_none(), "knob off: {oid}");
    }
    fold_trans_set_for_tests(true);
    // sum(float8/float4): NULL init, byval float state at the lane width.
    let (t, g, _) = classify_trans(&mk_spec(218, true, &af8), 0).expect("float8pl admits");
    assert_eq!((t.kind, t.width, t.res_width), (LaneKind::FSum, LaneWidth::F64, LaneWidth::F64));
    assert!(g.is_none(), "fold-trans lanes carry no integer guard");
    let (t, _, _) = classify_trans(&mk_spec(204, true, &af4), 0).expect("float4pl admits");
    assert_eq!((t.kind, t.width, t.res_width), (LaneKind::FSum, LaneWidth::F32, LaneWidth::F32));
    // accum family: NON-null '{0,0,0}' initval.
    let (t, g, _) = classify_trans(&mk_spec(222, false, &af8), 0).expect("float8_accum admits");
    assert_eq!((t.kind, t.width), (LaneKind::FAccum, LaneWidth::F64));
    assert!(g.is_none());
    let (t, _, _) = classify_trans(&mk_spec(208, false, &af4), 0).expect("float4_accum admits");
    assert_eq!((t.kind, t.width), (LaneKind::FAccum, LaneWidth::F32));
    // Initval-polarity gates (the catalog contract this classify encodes).
    assert!(classify_trans(&mk_spec(218, false, &af8), 0).is_none(), "sum needs NULL init");
    assert!(classify_trans(&mk_spec(222, true, &af8), 0).is_none(), "accum needs non-null init");
    // Wrong Var width refuses; OpExpr args refuse (bare Var only).
    assert!(classify_trans(&mk_spec(218, true, &af4), 0).is_none(), "f4 var for float8pl");
    let a_op = arg_list(mcx, mk_int_op(mcx, 1, INT4OID, 5, 177, true));
    assert!(classify_trans(&mk_spec(218, true, &a_op), 0).is_none(), "OpExpr for float8pl");
    assert!(classify_trans(&mk_spec(222, false, &a_op), 0).is_none(), "OpExpr for float8_accum");
    // FILTER (fold-trans inc 2): a CLASSIFIABLE predicate now admits with a
    // FilterEntry; an unclassifiable one still refuses the transition.
    let mut fspec = mk_spec(218, true, &af8);
    fspec.aggfilter = Some(mk_var(mcx, 2, BOOLV));
    let (_, _, f) = classify_trans(&fspec, 0).expect("bool-Var FILTER admits");
    assert_eq!(
        f,
        Some(FilterEntry { pred: FilterPred::BoolVar, col: 1, width: LaneWidth::Bool, konst: 0 })
    );
    let mut fbad = mk_spec(218, true, &af8);
    fbad.aggfilter = Some(mk_var(mcx, 2, F64V));
    assert!(classify_trans(&fbad, 0).is_none(), "non-bool Var FILTER refuses");
    // Knob OFF: even a classifiable FILTER refuses (byte-identical estate).
    fold_trans_set_for_tests(false);
    let ai4 = arg_list(mcx, mk_var(mcx, 1, INT4OID));
    let mut foff = mk_spec(1841, true, &ai4);
    foff.aggfilter = Some(mk_var(mcx, 2, BOOLV));
    assert!(classify_trans(&foff, 0).is_none(), "knob off: FILTER refuses");
    fold_trans_set_for_tests(true);
    // combine phase refuses.
    let mut cspec = mk_spec(218, true, &af8);
    cspec.combine = true;
    assert!(classify_trans(&cspec, 0).is_none(), "combine refuses");
}

// THE ORDERING PIN: a dataset where reassociation CHANGES the float8 sum.
// Row order: 1e16 absorbs each +1.0 (spacing at 1e16 is 2.0), so
// ((1e16+1)+1) + (-1e16) = 0.0. Smalls-first reassociation: (1.0+1.0) = 2.0
// IS representable at 1e16 spacing, so (1e16+2) - 1e16 = 2.0. The fold must
// produce the ROW-ORDER bits (0.0), proving it never reassociates.
#[test]
fn fold_trans_ordering_pin_fsum() {
    let _g = ft_guard();
    let mcx = leaked_mcx();
    let vals: Vec<Option<f64>> = vec![Some(1e16), Some(1.0), Some(1.0), Some(-1e16)];
    // Teeth: the reassociated evaluation genuinely diverges on these rows.
    let seq = ref_fsum_f64(&vals).unwrap().unwrap();
    let reassoc: f64 = (1e16 + (1.0 + 1.0)) + -1e16;
    assert_eq!(seq.to_bits(), 0.0f64.to_bits(), "row order: +1s absorbed");
    assert_eq!(reassoc.to_bits(), 2.0f64.to_bits(), "smalls-first: +2 survives");
    assert_ne!(seq.to_bits(), reassoc.to_bits(), "dataset is reassociation-sensitive");
    // The fold folds in row order.
    let af8 = arg_list(mcx, mk_var(mcx, 1, F64V));
    let specs = [mk_spec(218, true, &af8)];
    let plan = classify(mcx, &specs).expect("admits");
    let data = f64_rows(&vals);
    let cols = TestCols::from_datum_rows(1, &data);
    let selm = selmask(vals.len(), |_| true);
    let mut pgs = pergroups_for(mcx, &plan, 1);
    // SAFETY: pergroups cover the transno; lanes cover every row.
    unsafe {
        fold_batch(&plan, &cols, &selm, vals.len(), NonNull::new(pgs.as_mut_ptr()).unwrap(), mcx)
            .expect("fold");
    }
    assert!(!pgs[0].trans_value_is_null);
    assert_eq!(pgs[0].trans_value.as_f64().to_bits(), seq.to_bits(), "fold == row-order bits");
}

// FAccum ordering pin: Youngs-Cramer sxx depends on the arrival order.
// Forward vs reversed row order produce different sxx bits on this dataset;
// the fold matches FORWARD.
#[test]
fn fold_trans_ordering_pin_faccum() {
    let _g = ft_guard();
    let mcx = leaked_mcx();
    // Magnitude ladder: the sxx rounding under Youngs-Cramer differs between
    // forward and reversed arrival on these values (last-bit divergence —
    // verified: fwd 0x41274A70BC6A7EFA vs bwd ...EF9).
    let vals: Vec<Option<f64>> =
        vec![Some(1.0), Some(10.0), None, Some(100.0), Some(1000.0), Some(0.1)];
    let fwd = ref_faccum_f64(&vals).unwrap();
    let mut rev = vals.clone();
    rev.reverse();
    let bwd = ref_faccum_f64(&rev).unwrap();
    assert_ne!(fwd[2].to_bits(), bwd[2].to_bits(), "sxx is order-sensitive on this dataset");
    let af8 = arg_list(mcx, mk_var(mcx, 1, F64V));
    let specs = [mk_spec(222, false, &af8)];
    let plan = classify(mcx, &specs).expect("admits");
    let data = f64_rows(&vals);
    let cols = TestCols::from_datum_rows(1, &data);
    let selm = selmask(vals.len(), |_| true);
    let mut pgs = pergroups_for(mcx, &plan, 1);
    // SAFETY: FAccum pergroup holds a live new_float8_transarray.
    unsafe {
        fold_batch(&plan, &cols, &selm, vals.len(), NonNull::new(pgs.as_mut_ptr()).unwrap(), mcx)
            .expect("fold");
    }
    let got = read_f8_transarray(&pgs[0]);
    for k in 0..3 {
        assert_eq!(got[k].to_bits(), fwd[k].to_bits(), "word {k} == forward row-order bits");
    }
}

// Boundary parity (no-error datasets): NaN payloads, ±0, ±inf, denormals,
// NULLs, and a selection mask — ungrouped fold vs the per-row reference,
// bit-for-bit, f64 and f32, SUM and ACCUM.
#[test]
fn fold_trans_boundary_parity() {
    let _g = ft_guard();
    let mcx = leaked_mcx();
    let nan2 = f64::from_bits(0x7FF8_0000_0000_0001);
    let vals8: Vec<Option<f64>> = vec![
        Some(0.0),
        Some(-0.0),
        None,
        Some(1.5),
        Some(-1.5),
        Some(1e-300),
        Some(-1e-300),
        Some(f64::NAN),
        Some(nan2),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        None,
        Some(3.25),
    ];
    let vals4: Vec<Option<f32>> = vec![
        Some(0.0),
        Some(-0.0),
        None,
        Some(2.25),
        Some(-2.25),
        Some(1e-38),
        Some(f32::NAN),
        Some(f32::from_bits(0x7FC0_0001)),
        Some(f32::INFINITY),
        Some(f32::NEG_INFINITY),
        Some(-7.75),
    ];
    for sel in [(|_: usize| true) as fn(usize) -> bool, |i| i % 3 != 1] {
        // f64 SUM + ACCUM share one classify (two transitions, one column).
        let af8 = arg_list(mcx, mk_var(mcx, 1, F64V));
        let specs = [mk_spec(218, true, &af8), mk_spec(222, false, &af8)];
        let plan = classify(mcx, &specs).expect("admits");
        assert!(plan.resid.is_empty() && !plan.guarded);
        let data = f64_rows(&vals8);
        let cols = TestCols::from_datum_rows(1, &data);
        let selm = selmask(vals8.len(), sel);
        let mut pgs = pergroups_for(mcx, &plan, 2);
        // SAFETY: as the pins above.
        unsafe {
            fold_batch(&plan, &cols, &selm, vals8.len(), NonNull::new(pgs.as_mut_ptr()).unwrap(), mcx)
                .expect("fold");
        }
        let picked: Vec<Option<f64>> =
            vals8.iter().enumerate().filter(|(i, _)| sel(*i)).map(|(_, v)| *v).collect();
        let want_sum = ref_fsum_f64(&picked).unwrap().unwrap();
        assert_eq!(pgs[0].trans_value.as_f64().to_bits(), want_sum.to_bits(), "f64 sum bits");
        let want_acc = ref_faccum_f64(&picked).unwrap();
        let got_acc = read_f8_transarray(&pgs[1]);
        for k in 0..3 {
            assert_eq!(got_acc[k].to_bits(), want_acc[k].to_bits(), "f64 accum word {k}");
        }
        // f32 SUM + ACCUM.
        let af4 = arg_list(mcx, mk_var(mcx, 1, F32V));
        let specs4 = [mk_spec(204, true, &af4), mk_spec(208, false, &af4)];
        let plan4 = classify(mcx, &specs4).expect("admits");
        let data4 = f32_rows(&vals4);
        let cols4 = TestCols::from_datum_rows(1, &data4);
        let selm4 = selmask(vals4.len(), sel);
        let mut pgs4 = pergroups_for(mcx, &plan4, 2);
        // SAFETY: as above.
        unsafe {
            fold_batch(&plan4, &cols4, &selm4, vals4.len(), NonNull::new(pgs4.as_mut_ptr()).unwrap(), mcx)
                .expect("fold");
        }
        let picked4: Vec<Option<f32>> =
            vals4.iter().enumerate().filter(|(i, _)| sel(*i)).map(|(_, v)| *v).collect();
        let want_sum4 = ref_fsum_f32(&picked4).unwrap().unwrap();
        assert_eq!(pgs4[0].trans_value.as_f32().to_bits(), want_sum4.to_bits(), "f32 sum bits");
        let want_acc4 = ref_faccum_f32(&picked4).unwrap();
        let got_acc4 = read_f8_transarray(&pgs4[1]);
        for k in 0..3 {
            assert_eq!(got_acc4[k].to_bits(), want_acc4[k].to_bits(), "f32 accum word {k}");
        }
    }
    // All-NULL: SUM stays in the strict no-trans-value state; ACCUM keeps
    // the untouched {0,0,0} initval image (C: zero transfn calls).
    let nulls: Vec<Option<f64>> = vec![None; 17];
    let af8 = arg_list(mcx, mk_var(mcx, 1, F64V));
    let specs = [mk_spec(218, true, &af8), mk_spec(222, false, &af8)];
    let plan = classify(mcx, &specs).expect("admits");
    let data = f64_rows(&nulls);
    let cols = TestCols::from_datum_rows(1, &data);
    let selm = selmask(nulls.len(), |_| true);
    let mut pgs = pergroups_for(mcx, &plan, 2);
    // SAFETY: as above.
    unsafe {
        fold_batch(&plan, &cols, &selm, nulls.len(), NonNull::new(pgs.as_mut_ptr()).unwrap(), mcx)
            .expect("fold");
    }
    assert!(pgs[0].no_trans_value && pgs[0].trans_value_is_null, "sum: strict install never ran");
    let acc = read_f8_transarray(&pgs[1]);
    assert_eq!(acc, [0.0, 0.0, 0.0], "accum: initval image untouched");
}

// -0.0 install: the first strict input's exact bits are the state (C stores
// the argument datum). -0.0 then +0.0 goes through float8_pl and produces
// +0.0 (IEEE), which differs from the installed -0.0 — both directions
// pinned bitwise.
#[test]
fn fold_trans_fsum_signed_zero_install() {
    let _g = ft_guard();
    let mcx = leaked_mcx();
    let af8 = arg_list(mcx, mk_var(mcx, 1, F64V));
    let run = |vals: &[Option<f64>]| -> u64 {
        let specs = [mk_spec(218, true, &af8)];
        let plan = classify(mcx, &specs).expect("admits");
        let data = f64_rows(vals);
        let cols = TestCols::from_datum_rows(1, &data);
        let selm = selmask(vals.len(), |_| true);
        let mut pgs = pergroups_for(mcx, &plan, 1);
        // SAFETY: as above.
        unsafe {
            fold_batch(&plan, &cols, &selm, vals.len(), NonNull::new(pgs.as_mut_ptr()).unwrap(), mcx)
                .expect("fold");
        }
        pgs[0].trans_value.as_f64().to_bits()
    };
    assert_eq!(run(&[Some(-0.0)]), (-0.0f64).to_bits(), "install keeps -0.0 bits");
    assert_eq!(run(&[Some(-0.0), Some(0.0)]), 0.0f64.to_bits(), "-0.0 + 0.0 = +0.0");
    assert_eq!(run(&[Some(-0.0), Some(-0.0)]), (-0.0f64).to_bits(), "-0.0 + -0.0 = -0.0");
}

// Overflow raises C's exact ereport at C's row: rows AFTER the raising row
// must not accumulate (short-circuit), and the error matches the per-row
// reference's.
#[test]
fn fold_trans_overflow_raises() {
    let _g = ft_guard();
    let mcx = leaked_mcx();
    let af8 = arg_list(mcx, mk_var(mcx, 1, F64V));
    let vals: Vec<Option<f64>> = vec![Some(f64::MAX), Some(f64::MAX), Some(1.0)];
    let want = ref_fsum_f64(&vals).unwrap_err();
    for (oid, null_init) in [(218, true), (222, false)] {
        let specs = [mk_spec(oid, null_init, &af8)];
        let plan = classify(mcx, &specs).expect("admits");
        let data = f64_rows(&vals);
        let cols = TestCols::from_datum_rows(1, &data);
        let selm = selmask(vals.len(), |_| true);
        let mut pgs = pergroups_for(mcx, &plan, 1);
        // SAFETY: as above.
        let got = unsafe {
            fold_batch(&plan, &cols, &selm, vals.len(), NonNull::new(pgs.as_mut_ptr()).unwrap(), mcx)
        };
        let got = got.expect_err("MAX + MAX overflows");
        assert_eq!(got.message(), want.message(), "C's exact ereport ({oid})");
    }
}

// Grouped fold parity: interleaved groups, FSum + FAccum together, each
// group's state must equal the per-row reference over ITS rows in row order.
#[test]
fn fold_trans_grouped_parity() {
    let _g = ft_guard();
    let mcx = leaked_mcx();
    let af8 = arg_list(mcx, mk_var(mcx, 1, F64V));
    let specs = [mk_spec(218, true, &af8), mk_spec(222, false, &af8)];
    let plan = classify(mcx, &specs).expect("admits");
    let n = 120usize;
    let ngroups = 3usize;
    // Mix magnitudes so per-group order sensitivity has teeth, plus NULLs.
    let vals: Vec<Option<f64>> = (0..n)
        .map(|i| {
            if i % 7 == 5 {
                None
            } else if i % 11 == 0 {
                Some(1e15)
            } else if i % 11 == 1 {
                Some(-1e15)
            } else {
                Some((i as f64) * 1.375 - 40.0)
            }
        })
        .collect();
    let data = f64_rows(&vals);
    let cols = TestCols::from_datum_rows(1, &data);
    let mut group_pgs: Vec<Vec<AggPerGroup>> =
        (0..ngroups).map(|_| pergroups_for(mcx, &plan, specs.len())).collect();
    let idxs: Vec<u32> = (0..n as u32).collect();
    let groups: Vec<NonNull<AggPerGroup>> = (0..n)
        .map(|i| NonNull::new(group_pgs[i % ngroups].as_mut_ptr()).unwrap())
        .collect();
    // SAFETY: each group's pergroup array covers every transno; FAccum slots
    // hold live transarrays; arrays not moved while pointers live.
    unsafe { fold_rows_grouped(&plan, &cols, &idxs, &groups, mcx).expect("fold") };
    for g in 0..ngroups {
        let gvals: Vec<Option<f64>> =
            (0..n).filter(|i| i % ngroups == g).map(|i| vals[i]).collect();
        let want_sum = ref_fsum_f64(&gvals).unwrap().unwrap();
        assert_eq!(
            group_pgs[g][0].trans_value.as_f64().to_bits(),
            want_sum.to_bits(),
            "group {g} sum bits"
        );
        let want_acc = ref_faccum_f64(&gvals).unwrap();
        let got_acc = read_f8_transarray(&group_pgs[g][1]);
        for k in 0..3 {
            assert_eq!(got_acc[k].to_bits(), want_acc[k].to_bits(), "group {g} accum word {k}");
        }
    }
}

// The metadata / CSE / code-histogram arms must all refuse fold-trans plans:
// footer stats cannot answer an order-sensitive fold.
#[test]
fn fold_trans_side_arm_refusals() {
    let _g = ft_guard();
    let mcx = leaked_mcx();
    let af8 = arg_list(mcx, mk_var(mcx, 1, F64V));
    let specs = [mk_spec(218, true, &af8), mk_spec(222, false, &af8)];
    let plan = classify(mcx, &specs).expect("admits");
    assert!(plan.cse.is_empty(), "fold-trans kinds never join CSE groups");
    assert!(classify_meta(mcx, &specs).is_none(), "meta arm refuses");
    assert!(plan_code_hostable(&plan).is_none(), "code-histogram arm refuses");
    assert!(agg_meta_cols(&plan).is_none(), "footer-metadata arm refuses");
    assert!(granule_meta_len_cols(&plan).is_none(), "granule length-stats arm refuses");
}

// ===========================================================================
// Fold-trans increment 2 (lane aggseq-fold2, same knob): numeric sum/avg
// (NumAccum), corr/covar/regr (FRegrAccum + Count2), F64 cast reads, and
// per-transition FILTER masks. Same knob discipline as the block above.
// ===========================================================================

const NUMV: Oid = ::types_core::catalog::NUMERICOID;

fn arg_list2(mcx: Mcx<'static>, e1: Node<'static>, e2: Node<'static>) -> NodeList<'static> {
    let t1 = Node::mk_target_entry(mcx, e1, 1, None, false).unwrap();
    let t2 = Node::mk_target_entry(mcx, e2, 2, None, false).unwrap();
    NodeList::make2(mcx, t1, t2).unwrap()
}

// A float8 cast FuncExpr (i2tod/i4tod/i8tod/ftod) over `arg`.
fn mk_f64_cast(mcx: Mcx<'static>, funcid: Oid, arg: Node<'static>) -> Node<'static> {
    let mut f = Node::build::<::types_nodes::primnodes::FuncExpr>(mcx).unwrap();
    f.funcid = funcid;
    f.funcresulttype = F64V;
    f.args = NodeList::make1(mcx, arg).unwrap();
    f.seal()
}

// A comparison OpExpr for FILTER shapes: Var(attno, vartype) OP Const, with
// the Const's type/width picked by `ktype`; var_first=false commutes.
fn mk_cmp(
    mcx: Mcx<'static>,
    attno: i16,
    vartype: Oid,
    k: i64,
    ktype: Oid,
    opfuncid: Oid,
    var_first: bool,
) -> Node<'static> {
    let var = mk_var(mcx, attno, vartype);
    let (klen, kd) = match ktype {
        INT2OID => (2, Datum::from_i16(k as i16)),
        INT4OID => (4, Datum::from_i32(k as i32)),
        _ => (8, Datum::from_i64(k)),
    };
    let konst = Node::mk_const(mcx, ktype, -1, 0, klen, kd, false, true).unwrap();
    let mut op = Node::build::<OpExpr>(mcx).unwrap();
    op.opfuncid = opfuncid;
    op.opresulttype = BOOLV;
    op.args = if var_first {
        NodeList::make2(mcx, var, konst).unwrap()
    } else {
        NodeList::make2(mcx, konst, var).unwrap()
    };
    op.seal()
}

// NOT (expr) BoolExpr.
fn mk_not(mcx: Mcx<'static>, arg: Node<'static>) -> Node<'static> {
    let mut b = Node::build::<::types_nodes::primnodes::BoolExpr>(mcx).unwrap();
    b.boolop = ::types_nodes::primnodes::BoolExprType::NOT_EXPR;
    b.args = NodeList::make1(mcx, arg).unwrap();
    b.seal()
}

// A numeric datum from its text form, in either inline varlena header form.
fn num_datum(s: &str, short: bool) -> Datum {
    let img = ::adt_numeric::numeric_in(s, -1, None).expect("parse").expect("value");
    let payload = img.payload().to_vec();
    if short {
        vl_short(&payload)
    } else {
        vl_4b(&payload)
    }
}

// Per-row C reference for numeric_avg_accum: NOT strict (state allocates on
// the first selected row, NULL inputs included), non-null inputs accumulate
// through the REAL do_numeric_accum in row order.
fn ref_numaccum(
    mcx: Mcx<'static>,
    rows: &[Option<Datum>],
    sel: impl Fn(usize) -> bool,
) -> Option<&'static mut NumericAggState> {
    let mut st: Option<&'static mut NumericAggState> = None;
    for (i, d) in rows.iter().enumerate() {
        if !sel(i) {
            continue;
        }
        if st.is_none() {
            st = Some(Box::leak(Box::new(NumericAggState::new(false))));
        }
        if let Some(d) = d {
            let payload = vl_payload(*d); // heap Vec: ≥2-aligned base
            let num = ::adt_numeric::Num::from_payload(&payload);
            ::adt_numeric::aggregates::do_numeric_accum(st.as_mut().unwrap(), mcx, num)
                .expect("reference accum");
        }
    }
    st
}

fn num_state_of(pg: &AggPerGroup) -> Option<&'static mut NumericAggState> {
    if pg.trans_value_is_null {
        return None;
    }
    // SAFETY: state installed by the fold's numeric_state (arena-lived) or a
    // leaked reference Box; exclusive for the test's lifetime.
    Some(unsafe { &mut *(pg.trans_value.as_usize() as *mut NumericAggState) })
}

// The parity surface of a NumericAggState: scalar fields + the finalized
// sum(numeric) and avg(numeric) output IMAGES (full varlena bytes — dscale/
// weight/digit representation included).
fn num_state_surface(
    st: Option<&mut NumericAggState>,
) -> (Option<(i64, i32, i64, i64, i64, i64)>, Option<Vec<u8>>, Option<Vec<u8>>) {
    let Some(st) = st else { return (None, None, None) };
    let fields =
        (st.n, st.max_scale, st.max_scale_count, st.nan_count, st.pinf_count, st.ninf_count);
    let sum = ::adt_numeric::aggregates::numeric_sum(Some(st))
        .expect("sum finalize")
        .map(|img| img.as_bytes().to_vec());
    let avg = ::adt_numeric::aggregates::numeric_avg(Some(st))
        .expect("avg finalize")
        .map(|img| img.as_bytes().to_vec());
    (Some(fields), sum, avg)
}

fn read_f8_transarray6(pg: &AggPerGroup) -> [f64; 6] {
    assert!(!pg.trans_value_is_null);
    let p = pg.trans_value.as_usize() as *const u8;
    // SAFETY: new_float8_regr_transarray image, arena-lived for the test.
    unsafe {
        let img = core::slice::from_raw_parts(p, ::types_tuple::varatt::varsize_4b(p));
        ::adt_float::aggregates::check_float8_array::<6>(img, "test").expect("shape")
    }
}

// Per-row C reference for float8_regr_accum: strict two-arg (both non-null),
// '{0,0,0,0,0,0}' start, row order.
fn ref_fregr(
    rows: &[(Option<f64>, Option<f64>)],
    sel: impl Fn(usize) -> bool,
) -> PgResult<[f64; 6]> {
    let mut t = [0.0f64; 6];
    for (i, (y, x)) in rows.iter().enumerate() {
        if !sel(i) {
            continue;
        }
        if let (Some(y), Some(x)) = (y, x) {
            t = ::adt_float::aggregates::float8_regr_accum(t, *y, *x)?;
        }
    }
    Ok(t)
}

#[test]
fn fold_trans2_admission_matrix() {
    let _g = ft_guard();
    let mcx = leaked_mcx();
    let anum = arg_list(mcx, mk_var(mcx, 1, NUMV));
    let a2 = arg_list2(mcx, mk_var(mcx, 1, F64V), mk_var(mcx, 2, F64V));
    // Knob OFF: every increment-2 oid refuses.
    fold_trans_set_for_tests(false);
    assert!(classify_trans(&mk_spec(2858, true, &anum), 0).is_none(), "knob off: numeric");
    assert!(classify_trans(&mk_spec(2806, false, &a2), 0).is_none(), "knob off: regr");
    assert!(classify_trans(&mk_spec(2805, false, &a2), 0).is_none(), "knob off: regr_count");
    fold_trans_set_for_tests(true);
    // numeric_avg_accum: NULL initval, bare numeric Var, varlena lane.
    let (t, g, f) = classify_trans(&mk_spec(2858, true, &anum), 0).expect("numeric admits");
    assert_eq!((t.kind, t.width, t.res_width), (LaneKind::NumAccum, LaneWidth::Var, LaneWidth::Var));
    assert!(g.is_none() && f.is_none());
    assert!(classify_trans(&mk_spec(2858, false, &anum), 0).is_none(), "needs NULL init");
    let af8 = arg_list(mcx, mk_var(mcx, 1, F64V));
    assert!(classify_trans(&mk_spec(2858, true, &af8), 0).is_none(), "needs numeric Var");
    // The classified plan carries the vguard obligation (inline-form proof).
    let specs = [mk_spec(2858, true, &anum)];
    let plan = classify(mcx, &specs).expect("plan");
    assert_eq!(&plan.vguards[..], &[0u16]);
    assert!(plan.guarded, "varlena lane => guarded plan");
    assert_eq!(&plan.cols[..], &[0u16]);
    // float8_regr_accum: non-null initval, two args, col/col2.
    let (t, g, _) = classify_trans(&mk_spec(2806, false, &a2), 0).expect("regr admits");
    assert_eq!((t.kind, t.col, t.col2), (LaneKind::FRegrAccum, 0, 1));
    assert_eq!((t.fconv, t.fconv2), (FloatConv::None, FloatConv::None));
    assert!(g.is_none());
    assert!(classify_trans(&mk_spec(2806, true, &a2), 0).is_none(), "regr needs non-null init");
    assert!(classify_trans(&mk_spec(2806, false, &af8), 0).is_none(), "regr needs two args");
    // regr_count: Count2 over the same arg shape.
    let (t, _, _) = classify_trans(&mk_spec(2805, false, &a2), 0).expect("regr_count admits");
    assert_eq!((t.kind, t.col, t.col2, t.res_width), (LaneKind::Count2, 0, 1, LaneWidth::I64));
    // F64 cast reads: corr(f8, v4::float8) — the corpus corr leg's shape.
    let acast = arg_list2(
        mcx,
        mk_var(mcx, 1, F64V),
        mk_f64_cast(mcx, 316, mk_var(mcx, 2, INT4OID)),
    );
    let (t, _, _) = classify_trans(&mk_spec(2806, false, &acast), 0).expect("cast X admits");
    assert_eq!((t.col, t.col2, t.fconv, t.fconv2), (0, 1, FloatConv::None, FloatConv::I4));
    // Casts on the one-arg F64 kinds: sum(v8::float8), avg(v2::float8),
    // avg(f4::float8) — width = SOURCE lane width, res F64 for FSum.
    let ai8 = arg_list(mcx, mk_f64_cast(mcx, 482, mk_var(mcx, 1, INT8OID)));
    let (t, _, _) = classify_trans(&mk_spec(218, true, &ai8), 0).expect("i8tod sum admits");
    assert_eq!((t.kind, t.width, t.res_width, t.fconv), (LaneKind::FSum, LaneWidth::I64, LaneWidth::F64, FloatConv::I8));
    let ai2 = arg_list(mcx, mk_f64_cast(mcx, 235, mk_var(mcx, 1, INT2OID)));
    let (t, _, _) = classify_trans(&mk_spec(222, false, &ai2), 0).expect("i2tod accum admits");
    assert_eq!((t.kind, t.width, t.fconv), (LaneKind::FAccum, LaneWidth::I16, FloatConv::I2));
    let aftod = arg_list(mcx, mk_f64_cast(mcx, 311, mk_var(mcx, 1, F32V)));
    let (t, _, _) = classify_trans(&mk_spec(222, false, &aftod), 0).expect("ftod accum admits");
    assert_eq!((t.width, t.fconv), (LaneWidth::F32, FloatConv::F4));
    // Bare float4_accum carries the F4 widening tag (kernel reads f32→f64).
    let af4 = arg_list(mcx, mk_var(mcx, 1, F32V));
    let (t, _, _) = classify_trans(&mk_spec(208, false, &af4), 0).expect("f4 accum");
    assert_eq!((t.width, t.fconv), (LaneWidth::F32, FloatConv::F4));
    // A cast over a MISMATCHED Var type refuses (i4tod over int8 Var).
    let abad = arg_list(mcx, mk_f64_cast(mcx, 316, mk_var(mcx, 1, INT8OID)));
    assert!(classify_trans(&mk_spec(218, true, &abad), 0).is_none());
    // float4pl stays bare-Var-only (no casts to float4 exist in the tier).
    let a4cast = arg_list(mcx, mk_f64_cast(mcx, 311, mk_var(mcx, 1, F32V)));
    assert!(classify_trans(&mk_spec(204, true, &a4cast), 0).is_none());
}

#[test]
fn fold_trans2_filter_classify_matrix() {
    let _g = ft_guard();
    let mcx = leaked_mcx();
    let ai4 = arg_list(mcx, mk_var(mcx, 1, INT4OID));
    let admit = |pred: Node<'static>| -> Option<FilterEntry> {
        let mut spec = mk_spec(1841, true, &ai4); // int4_sum
        spec.aggfilter = Some(pred);
        classify_trans(&spec, 0).map(|(_, _, f)| f.expect("filtered admission carries entry"))
    };
    // bool Var / NOT bool Var.
    let f = admit(mk_var(mcx, 2, BOOLV)).expect("bool var");
    assert_eq!((f.pred, f.col), (FilterPred::BoolVar, 1));
    let f = admit(mk_not(mcx, mk_var(mcx, 3, BOOLV))).expect("not bool var");
    assert_eq!((f.pred, f.col), (FilterPred::NotBoolVar, 2));
    // Same-width comparisons, both operand orders (mirrored op).
    let f = admit(mk_cmp(mcx, 2, INT4OID, 500, INT4OID, 147, true)).expect("v4 > 500");
    assert_eq!((f.pred, f.col, f.width, f.konst), (FilterPred::Cmp(FilterOp::Gt), 1, LaneWidth::I32, 500));
    let f = admit(mk_cmp(mcx, 2, INT4OID, 500, INT4OID, 147, false)).expect("500 > v4");
    assert_eq!((f.pred, f.konst), (FilterPred::Cmp(FilterOp::Lt), 500), "const-first mirrors");
    let f = admit(mk_cmp(mcx, 2, INT2OID, -3, INT2OID, 148, true)).expect("v2 <= -3");
    assert_eq!((f.pred, f.width, f.konst), (FilterPred::Cmp(FilterOp::Le), LaneWidth::I16, -3));
    let f = admit(mk_cmp(mcx, 2, INT8OID, 1 << 40, INT8OID, 467, true)).expect("v8 = 2^40");
    assert_eq!((f.pred, f.width, f.konst), (FilterPred::Cmp(FilterOp::Eq), LaneWidth::I64, 1 << 40));
    // Refusals: non-bool Var, cross-width const type, unlisted opfunc,
    // function-call predicate, NOT over a non-Var.
    assert!(admit(mk_var(mcx, 2, INT4OID)).is_none(), "int Var alone");
    assert!(admit(mk_cmp(mcx, 2, INT8OID, 5, INT4OID, 467, true)).is_none(), "int8 cmp int4 const");
    assert!(admit(mk_cmp(mcx, 2, INT4OID, 5, INT4OID, 177, true)).is_none(), "int4pl is not a cmp");
    assert!(admit(mk_f64_cast(mcx, 316, mk_var(mcx, 2, INT4OID))).is_none(), "func predicate");
    assert!(admit(mk_not(mcx, mk_cmp(mcx, 2, INT4OID, 5, INT4OID, 65, true))).is_none(), "NOT(cmp)");
    // guard×FILTER named refusal: int4+5 Sum carries a data guard — adding a
    // classifiable filter refuses the WHOLE transition.
    let aguard = arg_list(mcx, mk_int_op(mcx, 2, INT4OID, 5, 177, true));
    let mut spec = mk_spec(1841, true, &aguard);
    assert!(classify_trans(&spec, 0).is_some(), "guarded shape admits unfiltered");
    spec.aggfilter = Some(mk_var(mcx, 3, BOOLV));
    assert!(classify_trans(&spec, 0).is_none(), "guard × FILTER refuses");
    // The type-proven int2+5 sibling (no guard) + filter ADMITS.
    let atype = arg_list(mcx, mk_int_op(mcx, 2, INT2OID, 5, 178, true));
    let mut spec = mk_spec(1841, true, &atype);
    spec.aggfilter = Some(mk_var(mcx, 3, BOOLV));
    assert!(classify_trans(&spec, 0).is_some(), "type-proven + FILTER admits");
    // classify() wiring: dedup + filter col staged + CSE exclusion.
    let mut s1 = mk_spec(1841, true, &ai4);
    s1.aggfilter = Some(mk_var(mcx, 3, BOOLV));
    let mut s2 = mk_spec(1963, false, &ai4);
    s2.aggfilter = Some(mk_var(mcx, 3, BOOLV));
    let s3 = mk_spec(1841, true, &ai4);
    let specs = [s1, s2, s3];
    let plan = classify(mcx, &specs).expect("plan");
    assert_eq!(plan.filters.len(), 1, "identical predicates dedup");
    assert_eq!(plan.trans[0].filter, 0);
    assert_eq!(plan.trans[1].filter, 0);
    assert_eq!(plan.trans[2].filter, NO_FILTER);
    assert!(plan.cols.contains(&2), "filter col staged");
    assert!(plan.cse.is_empty(), "filtered Sum/AvgAccum never join CSE");
}

#[test]
fn fold_trans2_filter_parity() {
    let _g = ft_guard();
    let mcx = leaked_mcx();
    // Columns: 0 = int4 value, 1 = bool filter, 2 = int8 value, 3 = int2 cmp
    // column (with NULLs in every lane — NULL filter rows must skip).
    let n = 97usize;
    let data: Vec<Vec<Option<Datum>>> = (0..n)
        .map(|i| {
            vec![
                (i % 7 != 3).then(|| Datum::from_i32(i as i32 * 3 - 40)),
                (i % 5 != 2).then(|| Datum::from_bool(i % 2 == 0)),
                (i % 6 != 1).then(|| Datum::from_i64((i as i64) << 33)),
                (i % 4 != 0).then(|| Datum::from_i16((i % 20) as i16)),
            ]
        })
        .collect();
    let fbool = |row: &Vec<Option<Datum>>| row[1].map(|d| d.as_bool()).unwrap_or(false);
    let fnot = |row: &Vec<Option<Datum>>| row[1].map(|d| !d.as_bool()).unwrap_or(false);
    let fcmp = |row: &Vec<Option<Datum>>| row[3].map(|d| d.as_i16() < 9).unwrap_or(false);
    let ai4 = arg_list(mcx, mk_var(mcx, 1, INT4OID));
    let ai8 = arg_list(mcx, mk_var(mcx, 3, INT8OID));
    // count(*) FILTER b | count(v4) FILTER NOT b | sum(v4) FILTER (v2cmp<9)
    // | avg(v4) FILTER b | min(v4) FILTER b | sum/avg(v8 int128) FILTER b |
    // unfiltered sum(v4) control.
    let nil = NodeList::default();
    let mut s0 = mk_spec(F_INT8INC, false, &nil);
    s0.aggfilter = Some(mk_var(mcx, 2, BOOLV));
    let mut s1 = mk_spec(F_INT8INC_ANY, false, &ai4);
    s1.aggfilter = Some(mk_not(mcx, mk_var(mcx, 2, BOOLV)));
    let mut s2 = mk_spec(1841, true, &ai4);
    s2.aggfilter = Some(mk_cmp(mcx, 4, INT2OID, 9, INT2OID, 64, true));
    let mut s3 = mk_spec(1963, false, &ai4);
    s3.aggfilter = Some(mk_var(mcx, 2, BOOLV));
    let mut s4 = mk_spec(F_INT4SMALLER, true, &ai4);
    s4.aggfilter = Some(mk_var(mcx, 2, BOOLV));
    let mut s5 = mk_spec(2746, true, &ai8);
    s5.aggfilter = Some(mk_var(mcx, 2, BOOLV));
    let s6 = mk_spec(1841, true, &ai4);
    let specs = [s0, s1, s2, s3, s4, s5, s6];
    let plan = classify(mcx, &specs).expect("plan");
    assert_eq!(plan.resid.len(), 0);
    let cols = TestCols::from_datum_rows(4, &data);
    for sel in [(|_: usize| true) as fn(usize) -> bool, |i| i % 3 != 1] {
        let rows = selmask(n, sel);
        let mut pgs = pergroups_for(mcx, &plan, specs.len());
        // SAFETY: pgs covers every transno; lanes cover every plan col/row.
        unsafe {
            fold_batch(&plan, &cols, &rows, n, NonNull::new(pgs.as_mut_ptr()).unwrap(), mcx)
                .expect("fold");
        }
        // Reference: per-transition data reduced to i64 rows, sel ∧ filter.
        let idata: Vec<Vec<Option<i64>>> = data
            .iter()
            .map(|r| {
                vec![
                    r[0].map(|d| d.as_i32() as i64),
                    r[1].map(|d| d.as_bool() as i64),
                    r[2].map(|d| d.as_i64()),
                    r[3].map(|d| d.as_i16() as i64),
                ]
            })
            .collect();
        for (transno, filt) in [
            (0usize, &fbool as &dyn Fn(&Vec<Option<Datum>>) -> bool),
            (1, &fnot),
            (2, &fcmp),
            (3, &fbool),
            (4, &fbool),
            (5, &fbool),
            (6, &|_: &Vec<Option<Datum>>| true),
        ] {
            let sub = plan_subset(mcx, &plan, &[transno]);
            let want = reference_fold(
                mcx,
                &sub,
                &idata,
                |i| sel(i) && filt(&data[i]),
                specs.len(),
            );
            assert_parity(&sub, &pgs, &want);
        }
    }
}

#[test]
fn fold_trans2_filter_grouped_parity() {
    let _g = ft_guard();
    let mcx = leaked_mcx();
    let n = 120usize;
    let ngroups = 3usize;
    let data: Vec<Vec<Option<Datum>>> = (0..n)
        .map(|i| {
            vec![
                (i % 9 != 4).then(|| Datum::from_i32(i as i32 - 17)),
                (i % 7 != 5).then(|| Datum::from_bool(i % 3 == 0)),
                Some(Datum::from_f64((i as f64) * 1.25 - 30.0)),
            ]
        })
        .collect();
    let fbool = |i: usize| data[i][1].map(|d| d.as_bool()).unwrap_or(false);
    let ai4 = arg_list(mcx, mk_var(mcx, 1, INT4OID));
    let af8 = arg_list(mcx, mk_var(mcx, 3, F64V));
    // Filtered count(*), filtered sum(v4), filtered ORDER-SENSITIVE
    // sum(float8) — the mask must compose with the row-order discipline.
    let nil = NodeList::default();
    let mut s0 = mk_spec(F_INT8INC, false, &nil);
    s0.aggfilter = Some(mk_var(mcx, 2, BOOLV));
    let mut s1 = mk_spec(1841, true, &ai4);
    s1.aggfilter = Some(mk_var(mcx, 2, BOOLV));
    let mut s2 = mk_spec(218, true, &af8);
    s2.aggfilter = Some(mk_var(mcx, 2, BOOLV));
    let specs = [s0, s1, s2];
    let plan = classify(mcx, &specs).expect("plan");
    let cols = TestCols::from_datum_rows(3, &data);
    let mut group_pgs: Vec<Vec<AggPerGroup>> =
        (0..ngroups).map(|_| pergroups_for(mcx, &plan, specs.len())).collect();
    let idxs: Vec<u32> = (0..n as u32).collect();
    let groups: Vec<NonNull<AggPerGroup>> = (0..n)
        .map(|i| NonNull::new(group_pgs[i % ngroups].as_mut_ptr()).unwrap())
        .collect();
    // SAFETY: group arrays cover every transno and stay pinned.
    unsafe { fold_rows_grouped(&plan, &cols, &idxs, &groups, mcx).expect("fold") };
    for g in 0..ngroups {
        let grows = |i: &usize| i % ngroups == g && fbool(*i);
        let want_count = (0..n).filter(|i| grows(i)).count() as i64;
        assert_eq!(group_pgs[g][0].trans_value.as_i64(), want_count, "group {g} count");
        let want_sum: i64 = (0..n)
            .filter(|i| grows(i))
            .filter_map(|i| data[i][0].map(|d| d.as_i32() as i64))
            .sum();
        if want_sum != 0 || (0..n).filter(|i| grows(i)).any(|i| data[i][0].is_some()) {
            assert_eq!(group_pgs[g][1].trans_value.as_i64(), want_sum, "group {g} sum");
        }
        let gvals: Vec<Option<f64>> = (0..n)
            .filter(|i| grows(i))
            .map(|i| data[i][2].map(|d| d.as_f64()))
            .collect();
        let want_f = ref_fsum_f64(&gvals).unwrap();
        match want_f {
            Some(v) => assert_eq!(
                group_pgs[g][2].trans_value.as_f64().to_bits(),
                v.to_bits(),
                "group {g} fsum bits"
            ),
            None => assert!(group_pgs[g][2].no_trans_value),
        }
    }
}

// Numeric corpus: dscale edges (trailing zeros, high scale), NaN, ±Infinity,
// sign cancellation, both inline header forms, NULLs.
fn num_corpus() -> Vec<Option<Datum>> {
    vec![
        Some(num_datum("1.00", true)),
        Some(num_datum("2.5", false)),
        None,
        Some(num_datum("-3.500", true)),
        Some(num_datum("0.001", false)),
        Some(num_datum("123456789012345.678901", false)),
        Some(num_datum("NaN", true)),
        Some(num_datum("Infinity", false)),
        Some(num_datum("-Infinity", true)),
        None,
        Some(num_datum("-123456789012345.678901", true)),
        Some(num_datum("0", true)),
        Some(num_datum("99999999999999999999.9999", false)),
    ]
}

#[test]
fn fold_trans2_numaccum_parity() {
    let _g = ft_guard();
    let mcx = leaked_mcx();
    let vals = num_corpus();
    let anum = arg_list(mcx, mk_var(mcx, 1, NUMV));
    for sel in [(|_: usize| true) as fn(usize) -> bool, |i| i % 3 != 1] {
        let specs = [mk_spec(2858, true, &anum)];
        let plan = classify(mcx, &specs).expect("plan");
        let data: Vec<Vec<Option<Datum>>> = vals.iter().map(|v| vec![*v]).collect();
        let cols = TestCols::from_datum_rows(1, &data);
        let rows = selmask(vals.len(), sel);
        let mut pgs = pergroups_for(mcx, &plan, 1);
        // SAFETY: vguard holds by construction (inline varlenas only).
        unsafe {
            fold_batch(&plan, &cols, &rows, vals.len(), NonNull::new(pgs.as_mut_ptr()).unwrap(), mcx)
                .expect("fold");
        }
        let want = ref_numaccum(mcx, &vals, sel);
        let (wf, ws, wa) = num_state_surface(want);
        let (gf, gs, ga) = num_state_surface(num_state_of(&pgs[0]));
        assert_eq!(gf, wf, "state fields");
        assert_eq!(gs, ws, "sum(numeric) image bytes");
        assert_eq!(ga, wa, "avg(numeric) image bytes");
    }
    // All-NULL selected rows: the NOT-strict transfn still allocates (state
    // present, n = 0) — observable exactly as Int128AvgAccum.
    let nulls: Vec<Option<Datum>> = vec![None; 9];
    let specs = [mk_spec(2858, true, &anum)];
    let plan = classify(mcx, &specs).expect("plan");
    let data: Vec<Vec<Option<Datum>>> = nulls.iter().map(|v| vec![*v]).collect();
    let cols = TestCols::from_datum_rows(1, &data);
    let rows = selmask(nulls.len(), |_| true);
    let mut pgs = pergroups_for(mcx, &plan, 1);
    // SAFETY: as above.
    unsafe {
        fold_batch(&plan, &cols, &rows, nulls.len(), NonNull::new(pgs.as_mut_ptr()).unwrap(), mcx)
            .expect("fold");
    }
    let st = num_state_of(&pgs[0]).expect("state allocated for all-NULL group");
    assert_eq!((st.n, st.total_count()), (0, 0));
    // Nothing selected: the transfn never ran; state stays NULL.
    let mut pgs = pergroups_for(mcx, &plan, 1);
    let rows = selmask(nulls.len(), |_| false);
    // SAFETY: as above.
    unsafe {
        fold_batch(&plan, &cols, &rows, nulls.len(), NonNull::new(pgs.as_mut_ptr()).unwrap(), mcx)
            .expect("fold");
    }
    assert!(pgs[0].trans_value_is_null, "empty selection leaves no state");
}

// THE NUMERIC REPRESENTATION PIN. Honest finding, stated precisely: for
// numeric_avg_accum, NO input ORDER can change the finalized bytes — the
// state is (exact sum, n, max-dscale bookkeeping) and every component is a
// commutative monoid (exact addition; max; count-of-max), so sum/avg
// finalize to identical images under every permutation. This test PROVES
// that over a dscale-edge multiset (all 24 permutations byte-identical),
// and pins the fold to those per-row-chain bytes — the row-order walk is
// still load-bearing for ERROR POSITIONS (OOM raise-at-row) and keeps the
// per-row path's exact state, but a reassociation-divergence dataset does
// not exist for this transition (unlike the float kernels above).
#[test]
fn fold_trans2_numeric_ordering_pin() {
    let _g = ft_guard();
    let mcx = leaked_mcx();
    // dscale edges: mixed scales, trailing zeros, cancellation to 0.
    let base = ["1.100", "-1.1", "0.00007", "2000000"];
    let mut perms: Vec<Vec<usize>> = Vec::new();
    for a in 0..4 {
        for b in 0..4 {
            for c in 0..4 {
                for d in 0..4 {
                    let p = vec![a, b, c, d];
                    let mut q = p.clone();
                    q.sort();
                    if q == vec![0, 1, 2, 3] {
                        perms.push(p);
                    }
                }
            }
        }
    }
    assert_eq!(perms.len(), 24);
    let surface = |order: &[usize]| {
        let vals: Vec<Option<Datum>> =
            order.iter().map(|&k| Some(num_datum(base[k], k % 2 == 0))).collect();
        let st = ref_numaccum(mcx, &vals, |_| true);
        num_state_surface(st)
    };
    let want = surface(&[0, 1, 2, 3]);
    for p in &perms {
        assert_eq!(surface(p), want, "order-insensitive under permutation {p:?}");
    }
    // The fold reproduces the forward chain bit-for-bit.
    let vals: Vec<Option<Datum>> =
        (0..4).map(|k| Some(num_datum(base[k], k % 2 == 0))).collect();
    let anum = arg_list(mcx, mk_var(mcx, 1, NUMV));
    let specs = [mk_spec(2858, true, &anum)];
    let plan = classify(mcx, &specs).expect("plan");
    let data: Vec<Vec<Option<Datum>>> = vals.iter().map(|v| vec![*v]).collect();
    let cols = TestCols::from_datum_rows(1, &data);
    let rows = selmask(vals.len(), |_| true);
    let mut pgs = pergroups_for(mcx, &plan, 1);
    // SAFETY: as fold_trans2_numaccum_parity.
    unsafe {
        fold_batch(&plan, &cols, &rows, vals.len(), NonNull::new(pgs.as_mut_ptr()).unwrap(), mcx)
            .expect("fold");
    }
    assert_eq!(num_state_surface(num_state_of(&pgs[0])), want, "fold == row-order bytes");
    // The cancellation edge renders with the max input dscale (sum image =
    // '0.00007 + ...' family — dscale 5 here), pinning the trailing-zero
    // rendering the charter names.
    let (_, sum, _) = &want;
    let img = sum.as_ref().expect("non-empty sum");
    let num = ::adt_numeric::Num::from_payload(&img[4..]);
    assert_eq!(num.dscale(), 5, "sum dscale = max input dscale");
}

#[test]
fn fold_trans2_numaccum_grouped_parity() {
    let _g = ft_guard();
    let mcx = leaked_mcx();
    let vals = num_corpus();
    let n = vals.len();
    let ngroups = 3usize;
    let anum = arg_list(mcx, mk_var(mcx, 1, NUMV));
    let specs = [mk_spec(2858, true, &anum)];
    let plan = classify(mcx, &specs).expect("plan");
    let data: Vec<Vec<Option<Datum>>> = vals.iter().map(|v| vec![*v]).collect();
    let cols = TestCols::from_datum_rows(1, &data);
    let mut group_pgs: Vec<Vec<AggPerGroup>> =
        (0..ngroups).map(|_| pergroups_for(mcx, &plan, 1)).collect();
    let idxs: Vec<u32> = (0..n as u32).collect();
    let groups: Vec<NonNull<AggPerGroup>> = (0..n)
        .map(|i| NonNull::new(group_pgs[i % ngroups].as_mut_ptr()).unwrap())
        .collect();
    // SAFETY: as the grouped tests above; vguard holds by construction.
    unsafe { fold_rows_grouped(&plan, &cols, &idxs, &groups, mcx).expect("fold") };
    for g in 0..ngroups {
        let want = ref_numaccum(mcx, &vals, |i| i % ngroups == g);
        assert_eq!(
            num_state_surface(num_state_of(&group_pgs[g][0])),
            num_state_surface(want),
            "group {g}"
        );
    }
}

// Bivariate pool with NaN/±inf/one-sided NULLs — strictness spans BOTH args.
fn regr_pool() -> Vec<(Option<f64>, Option<f64>)> {
    vec![
        (Some(1.0), Some(2.0)),
        (Some(-3.5), Some(0.25)),
        (None, Some(7.0)),
        (Some(4.0), None),
        (None, None),
        (Some(f64::NAN), Some(1.0)),
        (Some(2.0), Some(f64::INFINITY)),
        (Some(-0.0), Some(0.0)),
        (Some(1e-300), Some(-1e-300)),
        (Some(1e15), Some(-2.75)),
        (Some(8.125), Some(8.125)),
    ]
}

#[test]
fn fold_trans2_regr_parity() {
    let _g = ft_guard();
    let mcx = leaked_mcx();
    let pool = regr_pool();
    let n = pool.len();
    let a2 = arg_list2(mcx, mk_var(mcx, 1, F64V), mk_var(mcx, 2, F64V));
    for sel in [(|_: usize| true) as fn(usize) -> bool, |i| i % 4 != 2] {
        // FRegrAccum + Count2 over the same two columns.
        let specs = [mk_spec(2806, false, &a2), mk_spec(2805, false, &a2)];
        let plan = classify(mcx, &specs).expect("plan");
        assert_eq!(&plan.cols[..], &[0u16, 1]);
        let data: Vec<Vec<Option<Datum>>> = pool
            .iter()
            .map(|(y, x)| vec![y.map(Datum::from_f64), x.map(Datum::from_f64)])
            .collect();
        let cols = TestCols::from_datum_rows(2, &data);
        let rows = selmask(n, sel);
        let mut pgs = pergroups_for(mcx, &plan, 2);
        // SAFETY: FRegr pergroup holds a live new_float8_regr_transarray.
        unsafe {
            fold_batch(&plan, &cols, &rows, n, NonNull::new(pgs.as_mut_ptr()).unwrap(), mcx)
                .expect("fold");
        }
        let want = ref_fregr(&pool, sel).unwrap();
        let got = read_f8_transarray6(&pgs[0]);
        for k in 0..6 {
            assert_eq!(got[k].to_bits(), want[k].to_bits(), "regr word {k}");
        }
        let want_n = pool
            .iter()
            .enumerate()
            .filter(|(i, (y, x))| sel(*i) && y.is_some() && x.is_some())
            .count() as i64;
        assert_eq!(pgs[1].trans_value.as_i64(), want_n, "regr_count");
    }
    // Cast carrier: corr(f8, v4::float8) — X arrives via the I4 conversion.
    let acast = arg_list2(
        mcx,
        mk_var(mcx, 1, F64V),
        mk_f64_cast(mcx, 316, mk_var(mcx, 2, INT4OID)),
    );
    let specs = [mk_spec(2806, false, &acast)];
    let plan = classify(mcx, &specs).expect("plan");
    let ints: Vec<Option<i32>> = vec![Some(3), Some(-7), None, Some(1_000_000), Some(0)];
    let ys: Vec<Option<f64>> = vec![Some(0.5), Some(2.25), Some(1.0), None, Some(-9.75)];
    let data: Vec<Vec<Option<Datum>>> = ys
        .iter()
        .zip(ints.iter())
        .map(|(y, x)| vec![y.map(Datum::from_f64), x.map(Datum::from_i32)])
        .collect();
    let cols = TestCols::from_datum_rows(2, &data);
    let rows = selmask(data.len(), |_| true);
    let mut pgs = pergroups_for(mcx, &plan, 1);
    // SAFETY: as above.
    unsafe {
        fold_batch(&plan, &cols, &rows, data.len(), NonNull::new(pgs.as_mut_ptr()).unwrap(), mcx)
            .expect("fold");
    }
    let pairs: Vec<(Option<f64>, Option<f64>)> = ys
        .iter()
        .zip(ints.iter())
        .map(|(y, x)| (*y, x.map(|v| v as f64)))
        .collect();
    let want = ref_fregr(&pairs, |_| true).unwrap();
    let got = read_f8_transarray6(&pgs[0]);
    for k in 0..6 {
        assert_eq!(got[k].to_bits(), want[k].to_bits(), "cast-carrier word {k}");
    }
    // i8tod rounding pin: a >2^53 int8 sums through C's exact cast rounding.
    let ai8 = arg_list(mcx, mk_f64_cast(mcx, 482, mk_var(mcx, 1, INT8OID)));
    let specs = [mk_spec(218, true, &ai8)];
    let plan = classify(mcx, &specs).expect("plan");
    let big = (1i64 << 53) + 1; // rounds to 2^53 under ties-to-even
    let data: Vec<Vec<Option<Datum>>> = vec![vec![Some(Datum::from_i64(big))]];
    let cols = TestCols::from_datum_rows(1, &data);
    let rows = selmask(1, |_| true);
    let mut pgs = pergroups_for(mcx, &plan, 1);
    // SAFETY: as above.
    unsafe {
        fold_batch(&plan, &cols, &rows, 1, NonNull::new(pgs.as_mut_ptr()).unwrap(), mcx)
            .expect("fold");
    }
    assert_eq!(
        pgs[0].trans_value.as_f64().to_bits(),
        (big as f64).to_bits(),
        "i8tod = C's (float8) cast rounding"
    );
}

// THE BIVARIATE ORDERING PIN: float8_regr_accum's sxx/syy/sxy terms depend
// on the running means, so forward vs reversed arrival diverge on a
// magnitude-ladder dataset; the fold must match FORWARD — the FAccum pin
// extended to the two-argument case.
#[test]
fn fold_trans2_regr_ordering_pin() {
    let _g = ft_guard();
    let mcx = leaked_mcx();
    let pool: Vec<(Option<f64>, Option<f64>)> = vec![
        (Some(1.0), Some(1000.0)),
        (Some(10.0), Some(0.1)),
        (None, Some(5.0)),
        (Some(100.0), Some(10.0)),
        (Some(1000.0), Some(1.0)),
        (Some(0.1), Some(100.0)),
    ];
    let fwd = ref_fregr(&pool, |_| true).unwrap();
    let mut rpool = pool.clone();
    rpool.reverse();
    let bwd = ref_fregr(&rpool, |_| true).unwrap();
    assert!(
        (2..6).any(|k| fwd[k].to_bits() != bwd[k].to_bits()),
        "dataset is order-sensitive in at least one second-moment word"
    );
    let a2 = arg_list2(mcx, mk_var(mcx, 1, F64V), mk_var(mcx, 2, F64V));
    let specs = [mk_spec(2806, false, &a2)];
    let plan = classify(mcx, &specs).expect("plan");
    let data: Vec<Vec<Option<Datum>>> = pool
        .iter()
        .map(|(y, x)| vec![y.map(Datum::from_f64), x.map(Datum::from_f64)])
        .collect();
    let cols = TestCols::from_datum_rows(2, &data);
    let rows = selmask(pool.len(), |_| true);
    let mut pgs = pergroups_for(mcx, &plan, 1);
    // SAFETY: as fold_trans2_regr_parity.
    unsafe {
        fold_batch(&plan, &cols, &rows, pool.len(), NonNull::new(pgs.as_mut_ptr()).unwrap(), mcx)
            .expect("fold");
    }
    let got = read_f8_transarray6(&pgs[0]);
    for k in 0..6 {
        assert_eq!(got[k].to_bits(), fwd[k].to_bits(), "word {k} == forward row-order bits");
    }
}

#[test]
fn fold_trans2_regr_overflow_raises() {
    let _g = ft_guard();
    let mcx = leaked_mcx();
    let pool: Vec<(Option<f64>, Option<f64>)> =
        vec![(Some(f64::MAX), Some(1.0)), (Some(f64::MAX), Some(1.0)), (Some(1.0), Some(1.0))];
    let want = ref_fregr(&pool, |_| true).unwrap_err();
    let a2 = arg_list2(mcx, mk_var(mcx, 1, F64V), mk_var(mcx, 2, F64V));
    let specs = [mk_spec(2806, false, &a2)];
    let plan = classify(mcx, &specs).expect("plan");
    let data: Vec<Vec<Option<Datum>>> = pool
        .iter()
        .map(|(y, x)| vec![y.map(Datum::from_f64), x.map(Datum::from_f64)])
        .collect();
    let cols = TestCols::from_datum_rows(2, &data);
    let rows = selmask(pool.len(), |_| true);
    let mut pgs = pergroups_for(mcx, &plan, 1);
    // SAFETY: as fold_trans2_regr_parity.
    let got = unsafe {
        fold_batch(&plan, &cols, &rows, pool.len(), NonNull::new(pgs.as_mut_ptr()).unwrap(), mcx)
    }
    .expect_err("sy overflows");
    assert_eq!(got.message(), want.message(), "C's exact ereport");
}

#[test]
fn fold_trans2_regr_grouped_parity() {
    let _g = ft_guard();
    let mcx = leaked_mcx();
    let n = 90usize;
    let ngroups = 3usize;
    let pool: Vec<(Option<f64>, Option<f64>)> = (0..n)
        .map(|i| {
            let y = (i % 8 != 5).then(|| (i as f64) * 1.5 - 20.0);
            let x = (i % 6 != 2).then(|| 1000.0 / (i as f64 + 1.0));
            (y, x)
        })
        .collect();
    let a2 = arg_list2(mcx, mk_var(mcx, 1, F64V), mk_var(mcx, 2, F64V));
    let specs = [mk_spec(2806, false, &a2), mk_spec(2805, false, &a2)];
    let plan = classify(mcx, &specs).expect("plan");
    let data: Vec<Vec<Option<Datum>>> = pool
        .iter()
        .map(|(y, x)| vec![y.map(Datum::from_f64), x.map(Datum::from_f64)])
        .collect();
    let cols = TestCols::from_datum_rows(2, &data);
    let mut group_pgs: Vec<Vec<AggPerGroup>> =
        (0..ngroups).map(|_| pergroups_for(mcx, &plan, 2)).collect();
    let idxs: Vec<u32> = (0..n as u32).collect();
    let groups: Vec<NonNull<AggPerGroup>> = (0..n)
        .map(|i| NonNull::new(group_pgs[i % ngroups].as_mut_ptr()).unwrap())
        .collect();
    // SAFETY: as the grouped tests above.
    unsafe { fold_rows_grouped(&plan, &cols, &idxs, &groups, mcx).expect("fold") };
    for g in 0..ngroups {
        let want = ref_fregr(&pool, |i| i % ngroups == g).unwrap();
        let got = read_f8_transarray6(&group_pgs[g][0]);
        for k in 0..6 {
            assert_eq!(got[k].to_bits(), want[k].to_bits(), "group {g} word {k}");
        }
        let want_n = pool
            .iter()
            .enumerate()
            .filter(|(i, (y, x))| i % ngroups == g && y.is_some() && x.is_some())
            .count() as i64;
        assert_eq!(group_pgs[g][1].trans_value.as_i64(), want_n, "group {g} regr_count");
    }
}

// Side arms must refuse every increment-2 shape: order-sensitive/varlena/
// two-arg kinds AND filtered plans (a filtered int plan would otherwise
// wrongly footer-answer over unfiltered rows — the correctness pins for the
// admissibility gates).
#[test]
fn fold_trans2_side_arm_refusals() {
    let _g = ft_guard();
    let mcx = leaked_mcx();
    let anum = arg_list(mcx, mk_var(mcx, 1, NUMV));
    let a2 = arg_list2(mcx, mk_var(mcx, 1, F64V), mk_var(mcx, 2, F64V));
    let specs = [mk_spec(2858, true, &anum)];
    let plan = classify(mcx, &specs).expect("plan");
    assert!(classify_meta(mcx, &specs).is_none());
    assert!(plan_code_hostable(&plan).is_none());
    assert!(agg_meta_cols(&plan).is_none());
    assert!(granule_meta_len_cols(&plan).is_none());
    let specs = [mk_spec(2806, false, &a2), mk_spec(2805, false, &a2)];
    let plan = classify(mcx, &specs).expect("plan");
    assert!(plan.cse.is_empty());
    assert!(classify_meta(mcx, &specs).is_none());
    assert!(plan_code_hostable(&plan).is_none());
    assert!(agg_meta_cols(&plan).is_none());
    assert!(granule_meta_len_cols(&plan).is_none());
    // Filtered PLAIN-int plan (kinds the meta arms would otherwise answer).
    let ai4 = arg_list(mcx, mk_var(mcx, 1, INT4OID));
    let mut s0 = mk_spec(1841, true, &ai4);
    s0.aggfilter = Some(mk_var(mcx, 2, BOOLV));
    let nil = NodeList::default();
    let mut s1 = mk_spec(F_INT8INC, false, &nil);
    s1.aggfilter = Some(mk_var(mcx, 2, BOOLV));
    let specs = [s0, s1];
    let plan = classify(mcx, &specs).expect("plan");
    assert!(!plan.filters.is_empty());
    assert!(classify_meta(mcx, &specs).is_none(), "meta refuses filtered");
    assert!(plan_code_hostable(&plan).is_none(), "code arm refuses filtered");
    assert!(agg_meta_cols(&plan).is_none(), "footer arm refuses filtered");
    assert!(granule_meta_len_cols(&plan).is_none(), "granule arm refuses filtered");
}
