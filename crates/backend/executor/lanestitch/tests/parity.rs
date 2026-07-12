// The parity suite: every stitched shape fuzz-compared against the
// loop-inside interpreter reference (the semantic specification) over
// randomized programs and batches — boundary values, NULL masks, batch
// geometries straddling the 64-row SIMD block boundary, and overflow rows
// exercising refuse-and-replay at C's row. Plus the fail-closed refusal
// pins, the sticky/drift rails, and the µs-class stitch-time budget.
//
// Off-aarch64 the whole stitched arm compiles to "refuse" and these tests
// reduce to interpreter self-checks (compile() returns None).

use datum::{Datum, NullableDatum};
use lanestitch::{
    eval_qual, ArithOp, Batch, CmpOp, Lane, Program, SelVec, Step, StitchedProgram, MAX_ROWS,
};
use types_error::SqlState;

// ---- deterministic fuzz machinery ---------------------------------------

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        (self.next() >> 24) % n
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

/// Column value domains. Every lane is a canonically extended Datum array
/// (the spec.rs contract): ints sign-extended by from_iN, oids
/// sign-extended from the u32 image, floats as raw bit patterns.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ColTy {
    I16,
    I32,
    I64,
    Oid,
    F32,
    F64,
}

const I32_POOL: &[i32] = &[i32::MIN, i32::MIN + 1, -1000, -1, 0, 1, 5, 1000, i32::MAX - 1, i32::MAX];
const I64_POOL: &[i64] = &[i64::MIN, -1, 0, 1, i32::MAX as i64 + 7, i64::MAX];
const I16_POOL: &[i16] = &[i16::MIN, -3, 0, 2, i16::MAX];
const OID_POOL: &[u32] = &[0, 1, 42, 0x7FFF_FFFF, 0x8000_0000, u32::MAX];
const F64_POOL: &[f64] = &[
    f64::NEG_INFINITY,
    -1.5,
    -0.0,
    0.0,
    1.0,
    2.5,
    f64::INFINITY,
    f64::NAN,
    f64::MIN_POSITIVE,
];
const F32_POOL: &[f32] =
    &[f32::NEG_INFINITY, -1.5, -0.0, 0.0, 1.0, 2.5, f32::INFINITY, f32::NAN, f32::EPSILON];

fn canon_oid(v: u32) -> Datum {
    // Canonical SIGN-extension of the u32 image (the laneexec translation
    // contract for the 2x64 unsigned SIMD compares).
    Datum::from_i32(v as i32)
}

fn gen_value(r: &mut Lcg, ty: ColTy) -> Datum {
    match ty {
        ColTy::I16 => {
            if r.chance(50) {
                Datum::from_i16(I16_POOL[r.below(I16_POOL.len() as u64) as usize])
            } else {
                Datum::from_i16((r.next() as i16) % 100)
            }
        }
        ColTy::I32 => {
            if r.chance(40) {
                Datum::from_i32(I32_POOL[r.below(I32_POOL.len() as u64) as usize])
            } else {
                Datum::from_i32((r.next() as i32) % 1000)
            }
        }
        ColTy::I64 => {
            if r.chance(40) {
                Datum::from_i64(I64_POOL[r.below(I64_POOL.len() as u64) as usize])
            } else {
                Datum::from_i64((r.next() as i64) % 1000)
            }
        }
        ColTy::Oid => canon_oid(if r.chance(50) {
            OID_POOL[r.below(OID_POOL.len() as u64) as usize]
        } else {
            r.next() as u32 % 1000
        }),
        ColTy::F32 => {
            if r.chance(50) {
                Datum::from_f32(F32_POOL[r.below(F32_POOL.len() as u64) as usize])
            } else {
                Datum::from_f32(((r.next() as i32) % 1000) as f32 / 8.0)
            }
        }
        ColTy::F64 => {
            if r.chance(50) {
                Datum::from_f64(F64_POOL[r.below(F64_POOL.len() as u64) as usize])
            } else {
                Datum::from_f64(((r.next() as i32) % 1000) as f64 / 8.0)
            }
        }
    }
}

struct ColData {
    values: Vec<Datum>,
    isnull: Vec<bool>,
}

fn gen_batch_data(r: &mut Lcg, tys: &[ColTy], nrows: usize, null_pct: u64) -> Vec<ColData> {
    tys.iter()
        .map(|&ty| {
            let mut values = Vec::with_capacity(nrows);
            let mut isnull = Vec::with_capacity(nrows);
            for _ in 0..nrows {
                let null = r.chance(null_pct);
                isnull.push(null);
                // NULL rows still carry a (stale/garbage-ish) datum: the
                // SIMD tier compares them unconditionally and must mask
                // them out, so give them adversarial values too.
                values.push(gen_value(r, ty));
            }
            ColData { values, isnull }
        })
        .collect()
}

fn as_batch<'a>(cols: &'a [ColData], nrows: u32) -> Batch<'a> {
    Batch {
        nrows,
        lanes: cols
            .iter()
            .map(|c| Lane { values: &c.values, isnull: &c.isnull })
            .collect(),
    }
}

// The comparator families grouped by (lane type, const/rhs type).
const INT_FAMS: &[(ColTy, ColTy, &[CmpOp])] = &[
    (ColTy::I32, ColTy::I32, &[CmpOp::Int4Eq, CmpOp::Int4Ne, CmpOp::Int4Lt, CmpOp::Int4Le, CmpOp::Int4Gt, CmpOp::Int4Ge]),
    (ColTy::I64, ColTy::I64, &[CmpOp::Int8Eq, CmpOp::Int8Ne, CmpOp::Int8Lt, CmpOp::Int8Le, CmpOp::Int8Gt, CmpOp::Int8Ge]),
    (ColTy::I16, ColTy::I16, &[CmpOp::Int2Eq, CmpOp::Int2Ne, CmpOp::Int2Lt, CmpOp::Int2Le, CmpOp::Int2Gt, CmpOp::Int2Ge]),
    (ColTy::I64, ColTy::I32, &[CmpOp::Int84Eq, CmpOp::Int84Ne, CmpOp::Int84Lt, CmpOp::Int84Le, CmpOp::Int84Gt, CmpOp::Int84Ge]),
    (ColTy::I32, ColTy::I64, &[CmpOp::Int48Eq, CmpOp::Int48Ne, CmpOp::Int48Lt, CmpOp::Int48Le, CmpOp::Int48Gt, CmpOp::Int48Ge]),
    (ColTy::I16, ColTy::I32, &[CmpOp::Int24Eq, CmpOp::Int24Ne, CmpOp::Int24Lt, CmpOp::Int24Le, CmpOp::Int24Gt, CmpOp::Int24Ge]),
    (ColTy::I32, ColTy::I16, &[CmpOp::Int42Eq, CmpOp::Int42Ne, CmpOp::Int42Lt, CmpOp::Int42Le, CmpOp::Int42Gt, CmpOp::Int42Ge]),
    (ColTy::Oid, ColTy::Oid, &[CmpOp::OidEq, CmpOp::OidNe, CmpOp::OidLt, CmpOp::OidLe, CmpOp::OidGt, CmpOp::OidGe]),
];

const FLOAT_FAMS: &[(ColTy, ColTy, &[CmpOp])] = &[
    (ColTy::F32, ColTy::F32, &[CmpOp::Float4Eq, CmpOp::Float4Ne, CmpOp::Float4Lt, CmpOp::Float4Le, CmpOp::Float4Gt, CmpOp::Float4Ge]),
    (ColTy::F64, ColTy::F64, &[CmpOp::Float8Eq, CmpOp::Float8Ne, CmpOp::Float8Lt, CmpOp::Float8Le, CmpOp::Float8Gt, CmpOp::Float8Ge]),
    (ColTy::F32, ColTy::F64, &[CmpOp::Float48Eq, CmpOp::Float48Ne, CmpOp::Float48Lt, CmpOp::Float48Le, CmpOp::Float48Gt, CmpOp::Float48Ge]),
    (ColTy::F64, ColTy::F32, &[CmpOp::Float84Eq, CmpOp::Float84Ne, CmpOp::Float84Lt, CmpOp::Float84Le, CmpOp::Float84Gt, CmpOp::Float84Ge]),
];

/// Random program over `tys`-typed columns: 1..=4 clauses drawn from
/// {int cmp-const, float cmp-const (non-NaN), int cmp-var, arith clause}.
/// Returns None if the draw needs a column type the layout lacks.
fn gen_program(r: &mut Lcg, tys: &[ColTy], allow_arith: bool) -> Program {
    let mut prog = Program::new();
    let nclauses = 1 + r.below(4) as usize;
    let col_of = |tys: &[ColTy], want: ColTy, r: &mut Lcg| -> Option<u16> {
        let hits: Vec<u16> = tys
            .iter()
            .enumerate()
            .filter(|(_, &t)| t == want)
            .map(|(i, _)| i as u16)
            .collect();
        if hits.is_empty() {
            None
        } else {
            Some(hits[r.below(hits.len() as u64) as usize])
        }
    };
    for _ in 0..nclauses {
        let kind = r.below(if allow_arith { 4 } else { 3 });
        match kind {
            0 => {
                // int/oid cmp-const
                let (lane_ty, k_ty, ops) = INT_FAMS[r.below(INT_FAMS.len() as u64) as usize];
                let Some(col) = col_of(tys, lane_ty, r) else { continue };
                let op = ops[r.below(ops.len() as u64) as usize];
                let k = prog.push_const(NullableDatum { value: gen_value(r, k_ty), isnull: false });
                prog.steps.extend([
                    Step::LoadLane { col, out: 0 },
                    Step::LoadConst { k, out: 1 },
                    Step::Cmp { op, a: 0, b: 1, out: 2 },
                    Step::Qual { a: 2 },
                ]);
            }
            1 => {
                // float cmp-const (regenerate until non-NaN: NaN consts refuse)
                let (lane_ty, k_ty, ops) = FLOAT_FAMS[r.below(FLOAT_FAMS.len() as u64) as usize];
                let Some(col) = col_of(tys, lane_ty, r) else { continue };
                let op = ops[r.below(ops.len() as u64) as usize];
                let mut kv = gen_value(r, k_ty);
                for _ in 0..16 {
                    let f = if k_ty == ColTy::F32 { kv.as_f32() as f64 } else { kv.as_f64() };
                    if !f.is_nan() {
                        break;
                    }
                    kv = gen_value(r, k_ty);
                }
                let k = prog.push_const(NullableDatum { value: kv, isnull: false });
                prog.steps.extend([
                    Step::LoadLane { col, out: 0 },
                    Step::LoadConst { k, out: 1 },
                    Step::Cmp { op, a: 0, b: 1, out: 2 },
                    Step::Qual { a: 2 },
                ]);
            }
            2 => {
                // int cmp-var (same-type or cross-width pair)
                let (a_ty, b_ty, ops) = INT_FAMS[r.below(INT_FAMS.len() as u64) as usize];
                let (Some(ca), Some(cb)) = (col_of(tys, a_ty, r), col_of(tys, b_ty, r)) else {
                    continue;
                };
                let op = ops[r.below(ops.len() as u64) as usize];
                prog.steps.extend([
                    Step::LoadLane { col: ca, out: 0 },
                    Step::LoadLane { col: cb, out: 1 },
                    Step::Cmp { op, a: 0, b: 1, out: 2 },
                    Step::Qual { a: 2 },
                ]);
            }
            _ => {
                // arith clause: (a OP b|k) CMP k2 — the erroring shape.
                let Some(ca) = col_of(tys, ColTy::I32, r) else { continue };
                let aop = [ArithOp::Add4, ArithOp::Sub4, ArithOp::Mul4, ArithOp::Div4]
                    [r.below(4) as usize];
                let op = [CmpOp::Int4Gt, CmpOp::Int4Le, CmpOp::Int4Ne][r.below(3) as usize];
                prog.steps.push(Step::LoadLane { col: ca, out: 0 });
                if r.chance(50) {
                    if let Some(cb) = col_of(tys, ColTy::I32, r) {
                        prog.steps.push(Step::LoadLane { col: cb, out: 1 });
                    } else {
                        let k = prog.push_const(NullableDatum {
                            value: gen_value(r, ColTy::I32),
                            isnull: false,
                        });
                        prog.steps.push(Step::LoadConst { k, out: 1 });
                    }
                } else {
                    let k = prog
                        .push_const(NullableDatum { value: gen_value(r, ColTy::I32), isnull: false });
                    prog.steps.push(Step::LoadConst { k, out: 1 });
                }
                let k2 =
                    prog.push_const(NullableDatum { value: gen_value(r, ColTy::I32), isnull: false });
                prog.steps.extend([
                    Step::Arith { op: aop, a: 0, b: 1, out: 2 },
                    Step::LoadConst { k: k2, out: 3 },
                    Step::Cmp { op, a: 2, b: 3, out: 4 },
                    Step::Qual { a: 4 },
                ]);
            }
        }
    }
    if prog.steps.is_empty() {
        // Degenerate draw: fall back to a fixed int clause on col 0 if it
        // is int-typed, else a trivially refusable empty program.
        if tys.first() == Some(&ColTy::I32) {
            let k = prog.push_const(NullableDatum { value: Datum::from_i32(0), isnull: false });
            prog.steps.extend([
                Step::LoadLane { col: 0, out: 0 },
                Step::LoadConst { k, out: 1 },
                Step::Cmp { op: CmpOp::Int4Ge, a: 0, b: 1, out: 2 },
                Step::Qual { a: 2 },
            ]);
        }
    }
    prog
}

type QualResult = Result<Vec<bool>, (String, SqlState, u32)>;

/// Interpreter reference outcome: pass bits, or (message, sqlstate,
/// count-of-bits-decided-before-the-error) — the error-position currency.
fn interp_outcome(prog: &Program, cols: &[ColData], nrows: u32) -> QualResult {
    let batch = as_batch(cols, nrows);
    let mut sel = SelVec::all(nrows);
    match eval_qual(prog, &batch, &mut sel) {
        Ok(()) => Ok((0..nrows).map(|i| sel.contains(i)).collect()),
        Err(e) => {
            // The erroring row: every row before it has its final bit.
            let mut decided = 0u32;
            for i in 0..nrows {
                // Re-evaluate row-by-row to find the erroring row index.
                let row_batch = as_batch(cols, nrows);
                match lanestitch::eval_row(prog, &row_batch, i) {
                    Ok(_) => decided += 1,
                    Err(_) => break,
                }
            }
            Err((e.message.clone(), e.sqlstate, decided))
        }
    }
}

fn stitched_outcome(
    jit: &StitchedProgram,
    prog: &Program,
    cols: &[ColData],
    nrows: u32,
) -> QualResult {
    let batch = as_batch(cols, nrows);
    let mut sel = SelVec::all(nrows);
    match jit.run(prog, &batch, &mut sel) {
        Ok(_) => Ok((0..nrows).map(|i| sel.contains(i)).collect()),
        Err(e) => {
            let mut decided = 0u32;
            for i in 0..nrows {
                let row_batch = as_batch(cols, nrows);
                match lanestitch::eval_row(prog, &row_batch, i) {
                    Ok(_) => decided += 1,
                    Err(_) => break,
                }
            }
            // On error the replay's sel holds final bits for rows before
            // the erroring row: check them against the reference here.
            for i in 0..decided.min(nrows) {
                let want = lanestitch::eval_row(prog, &as_batch(cols, nrows), i).unwrap();
                assert_eq!(sel.contains(i), want, "pre-error bit {i} diverged");
            }
            Err((e.message.clone(), e.sqlstate, decided))
        }
    }
}

// ---- the fuzz gauntlet ---------------------------------------------------

/// The parity assertions hold under any tier mix; the ENGAGEMENT
/// assertions (SIMD bodies seen) only apply when the tier isn't pinned off
/// by the A/B kill switch.
fn simd_pinned_off() -> bool {
    matches!(std::env::var("PGRUST_LANESTITCH_SIMD").as_deref(), Ok("0") | Ok("off"))
}

/// Randomized programs x batch geometries x NULL densities, stitched vs
/// interpreter. Covers: every comparator family (int widths, cross-width,
/// oid unsigned, all four float families with NaN/±0/±inf lanes), fused
/// const/var clauses, generic arith clauses (refuse-and-replay), SIMD
/// blocks + scalar tails (geometries straddle 64), boundary values at
/// i32::MIN/MAX overflow edges.
#[test]
fn fuzz_parity_vs_interpreter() {
    // LANESTITCH_FUZZ_SEED overrides the default seed for soak sweeps.
    let seed = std::env::var("LANESTITCH_FUZZ_SEED")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0x5EED_1E57);
    let mut r = Lcg(seed);
    let layouts: &[&[ColTy]] = &[
        &[ColTy::I32, ColTy::I32, ColTy::I64, ColTy::I16, ColTy::Oid, ColTy::F32, ColTy::F64],
        &[ColTy::I32, ColTy::F64, ColTy::F32, ColTy::I32],
        &[ColTy::I64, ColTy::Oid, ColTy::I16, ColTy::I32],
    ];
    let geometries: &[u32] = &[1, 7, 63, 64, 65, 128, 191, 256, 1000, MAX_ROWS as u32];
    let mut stitched_batches = 0u32;
    let mut replays = 0u32;
    let mut simd_bodies = 0u32;
    let mut compiles = 0u32;
    for case in 0..400u32 {
        let tys = layouts[(case as usize) % layouts.len()];
        let allow_arith = r.chance(35);
        let prog = gen_program(&mut r, tys, allow_arith);
        if prog.steps.is_empty() {
            continue;
        }
        let Some(jit) = StitchedProgram::compile(&prog, tys.len()) else {
            // Off-arch or killed: nothing to compare. On-arch, every
            // vocabulary-only program must compile (asserted below).
            assert!(
                !lanestitch::available(),
                "vocabulary-only program refused to stitch (case {case})"
            );
            return;
        };
        compiles += 1;
        if jit.is_simd() {
            simd_bodies += 1;
        }
        let nrows = geometries[r.below(geometries.len() as u64) as usize];
        let null_pct = [0u64, 0, 10, 50, 100][r.below(5) as usize];
        let cols = gen_batch_data(&mut r, tys, nrows as usize, null_pct);
        let want = interp_outcome(&prog, &cols, nrows);
        let got = stitched_outcome(&jit, &prog, &cols, nrows);
        match (&want, &got) {
            (Ok(w), Ok(g)) => assert_eq!(w, g, "case {case} nrows {nrows} null% {null_pct}"),
            (Err(we), Err(ge)) => {
                assert_eq!(we, ge, "error identity/position diverged (case {case})");
                replays += 1;
            }
            _ => panic!("one path errored, the other did not (case {case}): want_err={} got_err={}",
                want.is_err(), got.is_err()),
        }
        if got.is_ok() {
            stitched_batches += 1;
        }
    }
    // The gauntlet must actually exercise the machinery.
    assert!(compiles >= 300, "too few compiled cases: {compiles}");
    assert!(stitched_batches >= 200, "too few stitched batches: {stitched_batches}");
    assert!(simd_pinned_off() || simd_bodies >= 50, "too few SIMD bodies: {simd_bodies}");
    assert!(replays >= 5, "too few refuse-and-replay error cases: {replays}");
}

/// Directed refuse-and-replay: the overflow row is planted at a known
/// position C; rows before C must keep their exact bits, the error must be
/// C's (message + sqlstate), and earlier clauses must shield rows exactly
/// like the interpreter (short-circuit protects a=0 rows from div-by-zero).
#[test]
fn refuse_and_replay_at_cs_row() {
    // Program: a <> 0 AND 100/a > 1  (clause 1 shields a=0 from the div).
    let mut prog = Program::new();
    let k0 = prog.push_const(NullableDatum { value: Datum::from_i32(0), isnull: false });
    let k100 = prog.push_const(NullableDatum { value: Datum::from_i32(100), isnull: false });
    let k1 = prog.push_const(NullableDatum { value: Datum::from_i32(1), isnull: false });
    prog.steps = vec![
        Step::LoadLane { col: 0, out: 0 },
        Step::LoadConst { k: k0, out: 1 },
        Step::Cmp { op: CmpOp::Int4Ne, a: 0, b: 1, out: 2 },
        Step::Qual { a: 2 },
        Step::LoadConst { k: k100, out: 0 },
        Step::LoadLane { col: 0, out: 1 },
        Step::Arith { op: ArithOp::Div4, a: 0, b: 1, out: 2 },
        Step::LoadConst { k: k1, out: 3 },
        Step::Cmp { op: CmpOp::Int4Gt, a: 2, b: 3, out: 4 },
        Step::Qual { a: 4 },
    ];

    // Case 1: a=0 rows exist but are clause-1-shielded — no error, exact bits.
    let n = 200u32;
    let mut values: Vec<Datum> = (0..n).map(|i| Datum::from_i32((i as i32 % 7) - 3)).collect();
    let isnull = vec![false; n as usize];
    {
        let cols = vec![ColData { values: values.clone(), isnull: isnull.clone() }];
        let Some(jit) = StitchedProgram::compile(&prog, 1) else {
            assert!(!lanestitch::available());
            return;
        };
        let want = interp_outcome(&prog, &cols, n);
        let got = stitched_outcome(&jit, &prog, &cols, n);
        assert_eq!(want.as_ref().unwrap(), got.as_ref().unwrap());
    }

    // Case 2: an unshielded MIN/-1-style trap cannot happen here (dividend
    // is the const 100), so plant a second program where the LANE overflows:
    // a * a > 0 with a = i32::MAX at row C.
    let mut prog2 = Program::new();
    let kz = prog2.push_const(NullableDatum { value: Datum::from_i32(0), isnull: false });
    prog2.steps = vec![
        Step::LoadLane { col: 0, out: 0 },
        Step::LoadLane { col: 0, out: 1 },
        Step::Arith { op: ArithOp::Mul4, a: 0, b: 1, out: 2 },
        Step::LoadConst { k: kz, out: 3 },
        Step::Cmp { op: CmpOp::Int4Gt, a: 2, b: 3, out: 4 },
        Step::Qual { a: 4 },
    ];
    let c_row = 137usize;
    values[c_row] = Datum::from_i32(i32::MAX);
    let cols = vec![ColData { values, isnull }];
    let Some(jit2) = StitchedProgram::compile(&prog2, 1) else {
        assert!(!lanestitch::available());
        return;
    };
    let want = interp_outcome(&prog2, &cols, n);
    let got = stitched_outcome(&jit2, &prog2, &cols, n);
    let (wm, ws, wrow) = want.unwrap_err();
    let (gm, gs, grow) = got.unwrap_err();
    assert_eq!((&wm[..], ws, wrow), (&gm[..], gs, grow));
    assert_eq!(wrow as usize, c_row, "error must fire at C's row");
    assert_eq!(wm, "integer out of range");

    // Sticky refusal: the program replayed once; a clean batch afterwards
    // must interpret (and still be exact).
    let clean: Vec<Datum> = (0..n).map(|i| Datum::from_i32(i as i32 - 100)).collect();
    let cols_clean = vec![ColData { values: clean, isnull: vec![false; n as usize] }];
    let batch = as_batch(&cols_clean, n);
    let mut sel = SelVec::all(n);
    let outcome = jit2.run(&prog2, &batch, &mut sel).unwrap();
    assert_eq!(outcome, lanestitch::RunOutcome::InterpretedSticky);
    let want_bits = interp_outcome(&prog2, &cols_clean, n).unwrap();
    let got_bits: Vec<bool> = (0..n).map(|i| sel.contains(i)).collect();
    assert_eq!(want_bits, got_bits);
}

/// Division-by-zero identity (unshielded): the replay must surface C's
/// "division by zero", not a stitched approximation.
#[test]
fn div_by_zero_identity() {
    let mut prog = Program::new();
    let k10 = prog.push_const(NullableDatum { value: Datum::from_i32(10), isnull: false });
    let k1 = prog.push_const(NullableDatum { value: Datum::from_i32(1), isnull: false });
    prog.steps = vec![
        Step::LoadConst { k: k10, out: 0 },
        Step::LoadLane { col: 0, out: 1 },
        Step::Arith { op: ArithOp::Div4, a: 0, b: 1, out: 2 },
        Step::LoadConst { k: k1, out: 3 },
        Step::Cmp { op: CmpOp::Int4Ge, a: 2, b: 3, out: 4 },
        Step::Qual { a: 4 },
    ];
    let n = 100u32;
    let values: Vec<Datum> =
        (0..n).map(|i| Datum::from_i32(if i == 41 { 0 } else { 3 })).collect();
    let cols = vec![ColData { values, isnull: vec![false; n as usize] }];
    let Some(jit) = StitchedProgram::compile(&prog, 1) else {
        assert!(!lanestitch::available());
        return;
    };
    let want = interp_outcome(&prog, &cols, n);
    let got = stitched_outcome(&jit, &prog, &cols, n);
    let (wm, ws, wrow) = want.unwrap_err();
    let (gm, gs, grow) = got.unwrap_err();
    assert_eq!((&wm[..], ws, wrow), (&gm[..], gs, grow));
    assert_eq!(wm, "division by zero");
    assert_eq!(wrow, 41);
}

/// MIN / -1 division overflow (int.c parity's sneaky arm).
#[test]
fn div_min_by_minus_one_overflow() {
    let mut prog = Program::new();
    let km1 = prog.push_const(NullableDatum { value: Datum::from_i32(-1), isnull: false });
    let k0 = prog.push_const(NullableDatum { value: Datum::from_i32(0), isnull: false });
    prog.steps = vec![
        Step::LoadLane { col: 0, out: 0 },
        Step::LoadConst { k: km1, out: 1 },
        Step::Arith { op: ArithOp::Div4, a: 0, b: 1, out: 2 },
        Step::LoadConst { k: k0, out: 3 },
        Step::Cmp { op: CmpOp::Int4Ne, a: 2, b: 3, out: 4 },
        Step::Qual { a: 4 },
    ];
    let n = 80u32;
    let values: Vec<Datum> =
        (0..n).map(|i| Datum::from_i32(if i == 66 { i32::MIN } else { i as i32 })).collect();
    let cols = vec![ColData { values, isnull: vec![false; n as usize] }];
    let Some(jit) = StitchedProgram::compile(&prog, 1) else {
        assert!(!lanestitch::available());
        return;
    };
    let (wm, ws, wrow) = interp_outcome(&prog, &cols, n).unwrap_err();
    let (gm, gs, grow) = stitched_outcome(&jit, &prog, &cols, n).unwrap_err();
    assert_eq!((&wm[..], ws, wrow), (&gm[..], gs, grow));
    assert_eq!(wm, "integer out of range");
    assert_eq!(wrow, 66);
}

// ---- fail-closed refusal pins ---------------------------------------------

fn cmp_const_prog(op: CmpOp, konst: Datum) -> Program {
    let mut prog = Program::new();
    let k = prog.push_const(NullableDatum { value: konst, isnull: false });
    prog.steps = vec![
        Step::LoadLane { col: 0, out: 0 },
        Step::LoadConst { k, out: 1 },
        Step::Cmp { op, a: 0, b: 1, out: 2 },
        Step::Qual { a: 2 },
    ];
    prog
}

#[test]
fn fail_closed_refusals() {
    if !lanestitch::available() {
        return;
    }
    // NaN const: the fcmp conds are exact only for a non-NaN rhs — refuse.
    let p = cmp_const_prog(CmpOp::Float8Gt, Datum::from_f64(f64::NAN));
    assert!(StitchedProgram::compile(&p, 1).is_none(), "NaN const must refuse");
    // Float var-var: no NaN-exact generic stencil — refuse.
    let mut p = Program::new();
    p.steps = vec![
        Step::LoadLane { col: 0, out: 0 },
        Step::LoadLane { col: 1, out: 1 },
        Step::Cmp { op: CmpOp::Float8Lt, a: 0, b: 1, out: 2 },
        Step::Qual { a: 2 },
    ];
    assert!(StitchedProgram::compile(&p, 2).is_none(), "float var-var must refuse");
    // Volatile programs refuse.
    let mut p = cmp_const_prog(CmpOp::Int4Gt, Datum::from_i32(5));
    p.volatile = true;
    assert!(StitchedProgram::compile(&p, 1).is_none(), "volatile must refuse");
    // Column out of range refuses.
    let mut p = Program::new();
    let k = p.push_const(NullableDatum { value: Datum::from_i32(5), isnull: false });
    p.steps = vec![
        Step::LoadLane { col: 3, out: 0 },
        Step::LoadConst { k, out: 1 },
        Step::Cmp { op: CmpOp::Int4Gt, a: 0, b: 1, out: 2 },
        Step::Qual { a: 2 },
    ];
    assert!(StitchedProgram::compile(&p, 2).is_none(), "col out of range must refuse");
    // Register out of range refuses.
    let mut p = Program::new();
    let k = p.push_const(NullableDatum { value: Datum::from_i32(5), isnull: false });
    p.steps = vec![
        Step::LoadLane { col: 0, out: 200 },
        Step::LoadConst { k, out: 1 },
        Step::Cmp { op: CmpOp::Int4Gt, a: 200, b: 1, out: 2 },
        Step::Qual { a: 2 },
    ];
    assert!(StitchedProgram::compile(&p, 1).is_none(), "reg out of range must refuse");
    // Trailing steps after the last Qual refuse.
    let mut p = cmp_const_prog(CmpOp::Int4Gt, Datum::from_i32(5));
    p.steps.push(Step::LoadLane { col: 0, out: 0 });
    assert!(StitchedProgram::compile(&p, 1).is_none(), "trailing steps must refuse");
    // Cross-clause register flow refuses (the SIMD bit-iteration tier runs
    // clauses in separate row loops).
    let mut p = Program::new();
    let k = p.push_const(NullableDatum { value: Datum::from_i32(5), isnull: false });
    let k2 = p.push_const(NullableDatum { value: Datum::from_i32(9), isnull: false });
    p.steps = vec![
        Step::LoadLane { col: 0, out: 7 },
        Step::LoadConst { k, out: 1 },
        Step::Cmp { op: CmpOp::Int4Gt, a: 7, b: 1, out: 2 },
        Step::Qual { a: 2 },
        // Reads r7 from the previous clause without rewriting it.
        Step::LoadConst { k: k2, out: 1 },
        Step::Cmp { op: CmpOp::Int4Lt, a: 7, b: 1, out: 3 },
        Step::Qual { a: 3 },
    ];
    assert!(StitchedProgram::compile(&p, 1).is_none(), "cross-clause reg flow must refuse");
    // Empty program refuses.
    assert!(StitchedProgram::compile(&Program::new(), 1).is_none());
}

/// Per-batch fail-open: drifted staging (short lanes / missing lanes /
/// oversize batch) interprets that batch exactly; the body stays armed.
#[test]
fn drift_fails_open() {
    if !lanestitch::available() {
        return;
    }
    let prog = cmp_const_prog(CmpOp::Int4Gt, Datum::from_i32(5));
    let jit = StitchedProgram::compile(&prog, 2).unwrap();
    let n = 100u32;
    let mut r = Lcg(7);
    let cols = gen_batch_data(&mut r, &[ColTy::I32, ColTy::I32], n as usize, 10);
    // Missing lane: batch has 1 lane, body compiled for 2.
    let batch = Batch { nrows: n, lanes: vec![Lane { values: &cols[0].values, isnull: &cols[0].isnull }] };
    let mut sel = SelVec::all(n);
    assert_eq!(jit.run(&prog, &batch, &mut sel).unwrap(), lanestitch::RunOutcome::InterpretedDrift);
    let want = interp_outcome(&prog, &cols[..1], n).unwrap();
    let got: Vec<bool> = (0..n).map(|i| sel.contains(i)).collect();
    assert_eq!(want, got);
    // Short lane arrays: values shorter than nrows.
    let short = ColData { values: cols[0].values[..50].to_vec(), isnull: cols[0].isnull.clone() };
    let batch = Batch {
        nrows: 50,
        lanes: vec![
            Lane { values: &short.values, isnull: &short.isnull },
            Lane { values: &cols[1].values[..40], isnull: &cols[1].isnull },
        ],
    };
    let mut sel = SelVec::all(50);
    // Lane 1 is short (40 < 50) but unused; used lane 0 is fine -> stitched.
    assert_eq!(jit.run(&prog, &batch, &mut sel).unwrap(), lanestitch::RunOutcome::Stitched);
    // A USED lane shorter than nrows is a caller contract violation, not
    // drift: the length check keeps the stitched body from reading out of
    // bounds (soundness), and the interpreter fallback surfaces the bug as
    // a memory-safe bounds panic instead of silent garbage.
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let batch = Batch {
            nrows: 60,
            lanes: vec![
                Lane { values: &cols[0].values[..50], isnull: &cols[0].isnull[..60] },
                Lane { values: &cols[1].values, isnull: &cols[1].isnull },
            ],
        };
        let mut sel = SelVec::all(60);
        let _ = jit.run(&prog, &batch, &mut sel);
    }))
    .is_err();
    assert!(panicked, "a too-short USED lane must bounds-panic, never read OOB");
}

// ---- the stitch-time budget ------------------------------------------------

/// The determination's µs-class budget: stitching must stay orders of
/// magnitude under LLVM's ~10^8-instruction compiles. Median must land in
/// single-digit-to-tens of µs; the hard assert is generous (200µs median,
/// 2ms worst) to survive CI noise while still catching any structural
/// regression (an accidental O(n^2) pass, a syscall storm).
#[test]
fn stitch_time_budget() {
    if !lanestitch::available() {
        return;
    }
    let mut r = Lcg(0xB0D9E7);
    let tys: &[ColTy] = &[ColTy::I32, ColTy::I64, ColTy::F64, ColTy::F32, ColTy::Oid];
    let mut nanos: Vec<u64> = Vec::new();
    for _ in 0..64 {
        let prog = gen_program(&mut r, tys, true);
        if prog.steps.is_empty() {
            continue;
        }
        if let Some(jit) = StitchedProgram::compile(&prog, tys.len()) {
            nanos.push(jit.stitch_nanos);
            assert!(jit.code_bytes > 0);
        }
    }
    assert!(nanos.len() >= 32, "budget sample too small: {}", nanos.len());
    nanos.sort_unstable();
    let median = nanos[nanos.len() / 2];
    let worst = *nanos.last().unwrap();
    // Report the numbers in the test output (visible with --nocapture).
    println!(
        "stitch budget: n={} median={}ns p90={}ns worst={}ns",
        nanos.len(),
        median,
        nanos[nanos.len() * 9 / 10],
        worst
    );
    assert!(median < 200_000, "median stitch {median}ns blows the µs-class budget");
    assert!(worst < 2_000_000, "worst stitch {worst}ns blows the budget");
}

/// Directed SIMD block-boundary sweep: exact 64-multiples, one-off each
/// side, and single-block batches over an all-comparator program.
#[test]
fn simd_block_boundaries() {
    let mut prog = Program::new();
    let k = prog.push_const(NullableDatum { value: Datum::from_i32(0), isnull: false });
    let kf = prog.push_const(NullableDatum { value: Datum::from_f64(0.5), isnull: false });
    let ko = prog.push_const(NullableDatum { value: canon_oid(0x8000_0001), isnull: false });
    prog.steps = vec![
        Step::LoadLane { col: 0, out: 0 },
        Step::LoadConst { k, out: 1 },
        Step::Cmp { op: CmpOp::Int4Ge, a: 0, b: 1, out: 2 },
        Step::Qual { a: 2 },
        Step::LoadLane { col: 1, out: 0 },
        Step::LoadConst { k: kf, out: 1 },
        Step::Cmp { op: CmpOp::Float8Le, a: 0, b: 1, out: 2 },
        Step::Qual { a: 2 },
        Step::LoadLane { col: 2, out: 0 },
        Step::LoadConst { k: ko, out: 1 },
        Step::Cmp { op: CmpOp::OidLt, a: 0, b: 1, out: 2 },
        Step::Qual { a: 2 },
        Step::LoadLane { col: 0, out: 0 },
        Step::LoadLane { col: 3, out: 1 },
        Step::Cmp { op: CmpOp::Int4Ne, a: 0, b: 1, out: 2 },
        Step::Qual { a: 2 },
    ];
    let tys = &[ColTy::I32, ColTy::F64, ColTy::Oid, ColTy::I32];
    let Some(jit) = StitchedProgram::compile(&prog, 4) else {
        assert!(!lanestitch::available());
        return;
    };
    assert!(
        simd_pinned_off() || jit.is_simd(),
        "the all-comparator program must take the NEON tier"
    );
    let mut r = Lcg(0xB10C);
    for &nrows in &[63u32, 64, 65, 127, 128, 129, 192, 1024] {
        for &null_pct in &[0u64, 15, 100] {
            let cols = gen_batch_data(&mut r, tys, nrows as usize, null_pct);
            let want = interp_outcome(&prog, &cols, nrows).unwrap();
            let got = stitched_outcome(&jit, &prog, &cols, nrows).unwrap();
            assert_eq!(want, got, "nrows={nrows} null%={null_pct}");
        }
    }
}
