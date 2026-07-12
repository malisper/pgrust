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
        LaneKind::Min | LaneKind::Max => AggPerGroup {
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
                        };
                        let next =
                            if t.kind == LaneKind::Max { old.max(v) } else { old.min(v) };
                        pg.trans_value = store(next);
                    }
                }
            }
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
        );
    }
    let want = reference_fold(mcx, &plan, data, sel, specs.len());
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
        check_guards(&plan, &cols, &rows, |_| Some((-21000, 21000))),
        GuardCheck::Pass { zone: true, data: false }
    );
    // Data tier: no zone answer, exact lane pass proves.
    assert_eq!(
        check_guards(&plan, &cols, &rows, |_| None),
        GuardCheck::Pass { zone: false, data: true }
    );
    // Wide zone bounds fall through to the lane pass, which still proves.
    assert_eq!(
        check_guards(&plan, &cols, &rows, |_| Some((-30000, 30000))),
        GuardCheck::Pass { zone: false, data: true }
    );
    // A single out-of-interval selected value demotes the whole batch.
    let cols = TestCols::new(&[2], &bad_data);
    assert_eq!(check_guards(&plan, &cols, &rows, |_| None), GuardCheck::Demote);
    // ... but not when the offending row is unselected (the proof covers
    // exactly the rows the fold would touch).
    let rows_skip = selmask(64, |i| i != 40);
    assert_eq!(
        check_guards(&plan, &cols, &rows_skip, |_| None),
        GuardCheck::Pass { zone: false, data: true }
    );
    // NULL rows never fail the proof.
    let null_data: Vec<Vec<Option<i64>>> =
        (0..64).map(|i| vec![if i == 40 { None } else { Some(0) }]).collect();
    let cols = TestCols::new(&[2], &null_data);
    assert_eq!(
        check_guards(&plan, &cols, &rows, |_| None),
        GuardCheck::Pass { zone: false, data: true }
    );
    // Guard interval is exact: the boundary value itself passes and folds to
    // the exact int4 rail.
    let rail: Vec<Vec<Option<i64>>> = vec![vec![Some(21474)]];
    let cols = TestCols::new(&[2], &rail);
    let rows1 = selmask(1, |_| true);
    assert_eq!(
        check_guards(&plan, &cols, &rows1, |_| None),
        GuardCheck::Pass { zone: false, data: true }
    );
    let mut pgs = pergroups_for(mcx, &plan, 1);
    // SAFETY: pgs covers transno 0; lanes cover the one row.
    unsafe { fold_batch(&plan, &cols, &rows1, 1, NonNull::new(pgs.as_mut_ptr()).unwrap()) };
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
    unsafe { fold_rows_grouped(&plan, &cols, &idxs, &groups) };
    // Per-group reference over the group's own row subset.
    for g in 0..ngroups {
        let gdata: Vec<Vec<Option<i64>>> =
            (0..n).filter(|i| i % ngroups == g).map(|i| data[i].clone()).collect();
        let want = reference_fold(mcx, &plan, &gdata, |_| true, specs.len());
        assert_parity(&plan, &group_pgs[g], &want);
    }
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
