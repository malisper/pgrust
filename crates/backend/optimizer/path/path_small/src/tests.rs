//! Unit GATE — exercises the real ported control flow of clausesel.c and
//! tidpath.c over a synthetic `PlannerInfo` arena.
//!
//! Every cross-subsystem callee is a seam defaulting to a loud panic, so the
//! test installs in-test seam impls first (single-threaded; `--test-threads=1`).
//! The relids seams model the planner convention that the empty set is `None`,
//! backed by the same canonical `bitmapword[]` layout the crate's own
//! `relids_make_singleton` produces.

use super::*;

use ::nodes::primnodes::{Const, CurrentOfExpr, OpExpr, Var};
use pathnodes::{Bitmapset, RelOptInfo};
use types_tuple::heaptuple::Datum;

/* ---- in-test relids algebra over the canonical bitmapword layout -------- */

fn words_of(a: &Relids) -> &[u64] {
    match a {
        Some(b) => &b.words,
        None => &[],
    }
}

fn member(x: i32, a: &Relids) -> bool {
    let w = words_of(a);
    let wn = (x / 64) as usize;
    let bn = (x % 64) as u32;
    wn < w.len() && (w[wn] >> bn) & 1 == 1
}

fn install_relids_seams() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        bms::relids_copy::set(|a| a.clone());
        bms::relids_is_empty::set(|a| words_of(a).iter().all(|&w| w == 0));
        bms::relids_is_member::set(member);
        ps::relids_del_members::set(|a, b| {
            let mut aw: Vec<u64> = words_of(&a).to_vec();
            let bw = words_of(b);
            for (i, slot) in aw.iter_mut().enumerate() {
                if i < bw.len() {
                    *slot &= !bw[i];
                }
            }
            if aw.iter().all(|&w| w == 0) {
                None
            } else {
                Some(alloc::boxed::Box::new(Bitmapset { words: aw }))
            }
        });
        ps::relids_union::set(|a, b| {
            let aw = words_of(a);
            let bw = words_of(b);
            let n = aw.len().max(bw.len());
            let mut out = alloc::vec![0u64; n];
            for (i, slot) in out.iter_mut().enumerate() {
                let av = aw.get(i).copied().unwrap_or(0);
                let bv = bw.get(i).copied().unwrap_or(0);
                *slot = av | bv;
            }
            if out.iter().all(|&w| w == 0) {
                None
            } else {
                Some(alloc::boxed::Box::new(Bitmapset { words: out }))
            }
        });
    });
}

/// An empty planner-run context for selectivity tests. The synthetic clauses
/// here are bare arena nodes that never reach `planner_rt_fetch`, so an empty
/// store is sufficient; the parameter exists purely to thread `&PlannerRun`
/// through the re-signed selectivity spine.
fn mk_run<'mcx>(mcx: mcx::Mcx<'mcx>) -> PlannerRun<'mcx> {
    PlannerRun::new(mcx)
}

/* ---- arena builders ----------------------------------------------------- */

fn mk_rel(root: &mut PlannerInfo, relid: u32, tuples: f64) -> RelId {
    let rel = RelOptInfo {
        relid,
        rtekind: RTE_RELATION,
        tuples,
        ..RelOptInfo::default()
    };
    let id = root.alloc_rel(rel);
    // Wire simple_rel_array[relid] -> id so find_base_rel can resolve it.
    let need = relid as usize + 1;
    if root.simple_rel_array.len() < need {
        root.simple_rel_array.resize(need, None);
    }
    root.simple_rel_array[relid as usize] = Some(id);
    root.simple_rel_array_size = root.simple_rel_array.len() as i32;
    id
}

/// A `RestrictInfo` wrapping an existing arena node, with a single-rel
/// `clause_relids` of `{relid}` (so find_single_rel sees one rel; statlist is
/// empty so extended stats never fire).
fn mk_rinfo(root: &mut PlannerInfo, clause: NodeId, relid: i32) -> RinfoId {
    let mut ri = make_bare_restrictinfo(clause);
    ri.num_base_rels = 1;
    ri.clause_relids = relids_make_singleton(relid);
    let id = root.alloc_rinfo(ri);
    id
}

/* ---- GATE 1: CURRENT OF selectivity = 1/tuples -------------------------- */

#[test]
fn current_of_selectivity_is_one_over_tuples() {
    install_relids_seams();
    let mut root = PlannerInfo::default();
    let rel = mk_rel(&mut root, 1, 50.0);
    let _ = rel;

    let coe = Expr::CurrentOfExpr(CurrentOfExpr {
        cvarno: 1,
        cursor_name: None,
        cursor_param: 0,
    });
    let node = root.alloc_node(coe);
    let rinfo = mk_rinfo(&mut root, node, 1);

    // clause_selectivity reaches the CurrentOfExpr arm: 1 / tuples = 1/50.
    let cx = mcx::MemoryContext::new("t");
    let run = mk_run(cx.mcx());
    let s = clause_selectivity(&run, &mut root, rinfo, 0, JOIN_INNER, None).unwrap();
    assert!((s - (1.0 / 50.0)).abs() < 1e-12, "got {s}");
}

/* ---- GATE 2: range-query clause pairing (hisel + losel - 1 + nulladj) --- */

#[test]
fn range_pair_selectivity_combines_bounds() {
    install_relids_seams();

    // Estimators: each inequality returns 0.25; oprrest maps the two ops to
    // SCALARLTSEL / SCALARGTSEL so they pair on the common var; nulltestsel adds
    // a null fraction of 0.10 to the pair.
    install_estimator_seams(0.25, 0.10);

    let mut root = PlannerInfo::default();
    let _rel = mk_rel(&mut root, 1, 1000.0);

    // Two clauses over the SAME var (x): `x < 10` (opno 100) and `x > 1`
    // (opno 200). get_oprrest maps 100 -> SCALARLTSEL, 200 -> SCALARGTSEL.
    let lt = mk_opclause(&mut root, 100, 1); // var on left, < => high bound
    let gt = mk_opclause(&mut root, 200, 1); // var on left, > => low bound
    let r_lt = mk_rinfo_op(&mut root, lt, 1);
    let r_gt = mk_rinfo_op(&mut root, gt, 1);

    let cx = mcx::MemoryContext::new("t");
    let run = mk_run(cx.mcx());
    let s = clauselist_selectivity(&run, &mut root, &[r_lt, r_gt], 0, JOIN_INNER, None).unwrap();

    // hibound = losel = 0.25; paired = hisel + losel - 1 + nullfrac
    //         = 0.25 + 0.25 - 1.0 + 0.10 = -0.40  -> <= 0 and < -0.01
    //         -> DEFAULT_RANGE_INEQ_SEL (0.005). s1 starts at 1.0.
    assert!((s - DEFAULT_RANGE_INEQ_SEL).abs() < 1e-12, "got {s}");
}

#[test]
fn range_pair_positive_combination() {
    install_relids_seams();
    // Each inequality returns 0.7; null fraction 0.0 -> paired = 0.4 (positive).
    install_estimator_seams(0.7, 0.0);

    let mut root = PlannerInfo::default();
    let _rel = mk_rel(&mut root, 1, 1000.0);

    let lt = mk_opclause(&mut root, 100, 1);
    let gt = mk_opclause(&mut root, 200, 1);
    let r_lt = mk_rinfo_op(&mut root, lt, 1);
    let r_gt = mk_rinfo_op(&mut root, gt, 1);

    let cx = mcx::MemoryContext::new("t");
    let run = mk_run(cx.mcx());
    let s = clauselist_selectivity(&run, &mut root, &[r_lt, r_gt], 0, JOIN_INNER, None).unwrap();
    // 0.7 + 0.7 - 1.0 + 0.0 = 0.4
    assert!((s - 0.4).abs() < 1e-12, "got {s}");
}

/* ---- GATE 3: tidpath IsTidEqualClause recognizer ----------------------- */

#[test]
fn recognizes_ctid_equal_const() {
    install_relids_seams();
    install_tid_seams();

    let mut root = PlannerInfo::default();
    let rel = mk_rel(&mut root, 1, 100.0);

    // OpExpr: CTID(var of rel 1) = const, opno = TIDEqualOperator (387).
    let ctid = Expr::Var(Var {
        varno: 1,
        varattno: SELF_ITEM_POINTER_ATTRIBUTE_NUMBER,
        vartype: TIDOID,
        varlevelsup: 0,
        ..Var::default()
    });
    let konst = Expr::Const(Const {
        consttype: TIDOID,
        consttypmod: -1,
        constcollid: 0,
        constlen: 6,
        constvalue: types_tuple_datum_null(),
        constisnull: false,
        constbyval: false,
        location: -1,
    });
    let op = Expr::OpExpr(OpExpr {
        opno: TID_EQUAL_OPERATOR,
        args: alloc::vec![ctid, konst],
        ..OpExpr::default()
    });
    let node = root.alloc_node(op);
    // right_relids empty (the const side references no rel).
    let mut ri = make_bare_restrictinfo(node);
    ri.right_relids = None;
    let rinfo = root.alloc_rinfo(ri);

    assert!(IsTidEqualClause(&mut root, rinfo, rel));
    assert!(!IsTidRangeClause(&mut root, rinfo, rel));
}

/* ---- estimator/oprrest/tid seam installers ----------------------------- */

fn install_estimator_seams(ineq_sel: f64, null_frac: f64) {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    // Note: get_oprrest / restriction_selectivity / nulltestsel_var /
    // is_pseudo_constant_clause_relids are installed once; the returned values
    // come from process-wide cells set below.
    SEL.store(ineq_sel.to_bits(), std::sync::atomic::Ordering::SeqCst);
    NULLF.store(null_frac.to_bits(), std::sync::atomic::Ordering::SeqCst);
    ONCE.call_once(|| {
        lsc::get_oprrest::set(|opno| {
            Ok(match opno {
                100 => F_SCALARLTSEL,
                200 => F_SCALARGTSEL,
                _ => 0,
            })
        });
        seam::restriction_selectivity::set(|_run, _r, _op, _a, _c, _v| {
            Ok(f64::from_bits(SEL.load(std::sync::atomic::Ordering::SeqCst)))
        });
        seam::nulltestsel_var::set(|_run, _r, _t, _v, _vr, _jt, _sj| {
            Ok(f64::from_bits(NULLF.load(std::sync::atomic::Ordering::SeqCst)))
        });
        // The pseudoconstant-on-one-side test: the right operand is the const.
        seam::is_pseudo_constant_clause_relids::set(|clause, _relids| {
            Ok(matches!(clause, Expr::Const(_)))
        });
        // equal() over the range vars: structural Var equality is enough here.
        eq::equal_expr::set(|a, b| match (a, b) {
            (Expr::Var(x), Expr::Var(y)) => {
                x.varno == y.varno
                    && x.varattno == y.varattno
                    && x.vartype == y.vartype
                    && x.varlevelsup == y.varlevelsup
            }
            _ => false,
        });
    });
}

fn install_tid_seams() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        seam::contain_volatile_functions_expr::set(|_e| false);
    });
}

static SEL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static NULLF: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/* ---- arena helpers for the range test ---------------------------------- */

/// `var < / > const` over var `x` (a non-CTID Var of relation `relid`); the var
/// is on the left so it is the range variable. The two clauses share the same
/// `x` so `addRangeClause` groups them.
fn mk_opclause(root: &mut PlannerInfo, opno: Oid, relid: i32) -> NodeId {
    let x = Expr::Var(Var {
        varno: relid,
        varattno: 1,
        vartype: 23, // int4
        varlevelsup: 0,
        ..Var::default()
    });
    let c = Expr::Const(Const {
        consttype: 23,
        consttypmod: -1,
        constcollid: 0,
        constlen: 4,
        constvalue: types_tuple_datum_null(),
        constisnull: false,
        constbyval: true,
        location: -1,
    });
    let op = Expr::OpExpr(OpExpr {
        opno,
        args: alloc::vec![x, c],
        ..OpExpr::default()
    });
    root.alloc_node(op)
}

/// A `RestrictInfo` for a range opclause: single base rel, var on left so
/// `right_relids` (the const side) is empty (pseudoconstant), `equal()` on the
/// var groups the pair.
fn mk_rinfo_op(root: &mut PlannerInfo, clause: NodeId, relid: i32) -> RinfoId {
    let mut ri = make_bare_restrictinfo(clause);
    ri.num_base_rels = 1;
    ri.clause_relids = relids_make_singleton(relid);
    ri.right_relids = None; // const side references no rel
    ri.left_relids = relids_make_singleton(relid);
    root.alloc_rinfo(ri)
}

/// A null `Datum` for synthetic Const nodes.
fn types_tuple_datum_null() -> Datum<'static> {
    Datum::null()
}
