// Ported from the cbstore branch's nodeagg lanefold_tests, restricted to the
// harvested surface: the tests there that drove the whole executor
// (exec_agg_batched, dict windows, textlen, DISTINCT, metadata) stay behind;
// what ports is the kernel-level byte-parity contract — classifier admission
// and refusal, the TYPE/DATA proofs, guard exactness, CSE derivation, and the
// fold kernels checked bit-for-bit against a per-row reference that applies
// C's transition semantics in C's row order.

use core::ptr::NonNull;

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
        | LaneKind::BpMax => AggPerGroup {
            trans_value: Datum::null(),
            trans_value_is_null: true,
            no_trans_value: true,
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
            let Some(v) = row[t.col as usize] else { continue };
            // The admitted transform, checked per row exactly as C evaluates
            // the OpExpr (trunc division, int4-fitting result by admission).
            let v = (v / t.divk as i64) * t.mulk as i64 + t.addend as i64;
            match t.kind {
                LaneKind::CountStar => unreachable!(),
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
// regression path (Q30 folds 90 of these).
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
        let (t, g) = classify_trans(&spec, i).unwrap_or_else(|| panic!("case {i} admits"));
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
        let (t, _) = classify_trans(&spec, 0).unwrap_or_else(|| panic!("case {i} admits"));
        assert_eq!((t.addend, t.mulk, t.divk), *coeffs, "case {i}");
        assert_eq!(t.res_width, LaneWidth::I32, "OpExpr result is int4, case {i}");
    }
    // int2 admissions are TYPE-proven (no guard); int4 + const needs a guard.
    let args = arg_list(mcx, mk_int_op(mcx, 1, INT2OID, 5, 178, true));
    let (_, g) = classify_trans(&mk_spec(1841, true, &args), 0).unwrap();
    assert!(g.is_none(), "int2+5 is type-proven");
    let args = arg_list(mcx, mk_int_op(mcx, 2, INT4OID, 5, 177, true));
    let (_, g) = classify_trans(&mk_spec(1841, true, &args), 0).unwrap();
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
        let (t, g) = classify_trans(&spec, i).unwrap_or_else(|| panic!("case {i} admits"));
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
    let (t, g) = classify_trans(&mk_spec(1898, true, &a_g), 0).expect("int4and over v+5 admits");
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
        let (t, g) = classify_trans(&spec, i).unwrap_or_else(|| panic!("case {i} admits"));
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
    let (t, _) = classify_trans(&mk_spec_coll(459, &a, COLL_C), 0).expect("admits");
    assert_eq!(t.kind, LaneKind::StrMin);
    // bpchar COLLATE "C" -> RelabelType(bpchar -> bpchar).
    let a = mk_rel(BPCHARV, BPCHARV);
    let (t, _) = classify_trans(&mk_spec_coll(1063, &a, COLL_C), 0).expect("admits");
    assert_eq!(t.kind, LaneKind::BpMax);
    // Cross-type relabel to bpchar under a text transfn still refuses.
    let a = mk_rel(TEXTV, BPCHARV);
    assert!(classify_trans(&mk_spec_coll(459, &a, COLL_C), 0).is_none());
}
