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
    eval_qual, ArithOp, Batch, BoolTestKind, CmpOp, Lane, NullTestKind, Program, SelVec, Step,
    StitchedProgram, MAX_ROWS,
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
/// {int cmp-const, float cmp-const (non-NaN), int cmp-var, float cmp-var
/// (NaN-dense lanes), arith clause}. Skips a draw if the layout lacks the
/// needed column type.
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
        let kind = r.below(if allow_arith { 5 } else { 4 });
        match kind {
            3 => {
                // float cmp-var (same- or cross-width): the FCmpVar fused
                // shape — NaN lanes are common (F32/F64 pools are NaN-dense).
                let (a_ty, b_ty, ops) = FLOAT_FAMS[r.below(FLOAT_FAMS.len() as u64) as usize];
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
    // Float var-var OUTSIDE the fused window (result feeds a NullTest, not
    // the Qual directly): the generic register-file Cmp stencil still has
    // no NaN-exact cond — refuse. (The fused [load,load,cmp,qual] window
    // compiles via the FCmpVar NaN-mask stencils — see float_var_var_*.)
    let mut p = Program::new();
    p.steps = vec![
        Step::LoadLane { col: 0, out: 0 },
        Step::LoadLane { col: 1, out: 1 },
        Step::Cmp { op: CmpOp::Float8Lt, a: 0, b: 1, out: 2 },
        Step::NullTest { a: 2, out: 3, kind: NullTestKind::IsNotNull },
        Step::Qual { a: 3 },
    ];
    assert!(StitchedProgram::compile(&p, 2).is_none(), "generic-window float cmp must refuse");
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
    let vocab_tys: &[ColTy] = &[ColTy::I16, ColTy::I32, ColTy::I64, ColTy::Oid];
    let mut nanos: Vec<u64> = Vec::new();
    for i in 0..64 {
        // Interleave founding + grown-vocabulary programs so the budget
        // covers the new (larger) stencils: wide arith and unrolled SAOP.
        let (prog, ncols) = if i % 2 == 0 {
            (gen_program(&mut r, tys, true), tys.len())
        } else {
            (gen_new_vocab_program(&mut r), vocab_tys.len())
        };
        if prog.steps.is_empty() {
            continue;
        }
        if let Some(jit) = StitchedProgram::compile(&prog, ncols) {
            nanos.push(jit.stitch_nanos);
            assert!(jit.code_bytes > 0);
        }
    }
    // A worst-case-sized SAOP (a full 128-element IN-list) must also budget.
    {
        let big: Vec<NullableDatum> =
            (0..128).map(|v| NullableDatum { value: Datum::from_i32(v), isnull: false }).collect();
        if let Some(jit) = StitchedProgram::compile(&saop_prog(CmpOp::Int4Eq, big), 1) {
            nanos.push(jit.stitch_nanos);
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

// ==========================================================================
// Grown vocabulary (lane-v2-stitchvocab): NULL/bool tests, wider (int2/int8)
// arithmetic, and const-array SAOP. Every addition is parity-tested against
// the loop-inside interpreter reference exactly like the founding shapes.
// ==========================================================================

/// Compile + run one program/batch on both tiers and assert full parity:
/// identical pass bits, or identical (message, sqlstate, erroring-row). None
/// on compile means off-arch/killed. Returns Some(true) if the case errored.
fn check_parity(prog: &Program, cols: &[ColData], nrows: u32) -> Option<bool> {
    let jit = StitchedProgram::compile(prog, cols.len())?;
    let want = interp_outcome(prog, cols, nrows);
    let got = stitched_outcome(&jit, prog, cols, nrows);
    match (&want, &got) {
        (Ok(w), Ok(g)) => {
            assert_eq!(w, g, "pass-bit divergence (nrows {nrows})");
            Some(false)
        }
        (Err(we), Err(ge)) => {
            assert_eq!(we, ge, "error identity/position divergence");
            Some(true)
        }
        _ => panic!("one tier errored, the other did not: want_err={} got_err={}",
            want.is_err(), got.is_err()),
    }
}

fn one_col(values: Vec<Datum>, isnull: Vec<bool>) -> Vec<ColData> {
    vec![ColData { values, isnull }]
}

// ---- item 1: NULL tests + bool tests --------------------------------------

fn nulltest_prog(kind: NullTestKind) -> Program {
    let mut p = Program::new();
    p.steps = vec![
        Step::LoadLane { col: 0, out: 0 },
        Step::NullTest { a: 0, out: 1, kind },
        Step::Qual { a: 1 },
    ];
    p
}

fn booltest_prog(kind: BoolTestKind) -> Program {
    let mut p = Program::new();
    p.steps = vec![
        Step::LoadLane { col: 0, out: 0 },
        Step::BoolTest { a: 0, out: 1, kind },
        Step::Qual { a: 1 },
    ];
    p
}

#[test]
fn null_and_bool_tests_directed() {
    if !lanestitch::available() {
        return;
    }
    let n = 200u32;
    // A column with a deterministic mix of NULLs and true/false/other values.
    let values: Vec<Datum> = (0..n)
        .map(|i| match i % 4 {
            0 => Datum::from_i32(0),   // false
            1 => Datum::from_i32(1),   // true
            2 => Datum::from_i32(-7),  // true (nonzero)
            _ => Datum::from_i32(0),
        })
        .collect();
    let isnull: Vec<bool> = (0..n).map(|i| i % 5 == 0).collect();
    let cols = one_col(values, isnull);

    for kind in [NullTestKind::IsNull, NullTestKind::IsNotNull] {
        assert_eq!(check_parity(&nulltest_prog(kind), &cols, n), Some(false));
    }
    for kind in [
        BoolTestKind::IsTrue,
        BoolTestKind::IsNotTrue,
        BoolTestKind::IsFalse,
        BoolTestKind::IsNotFalse,
    ] {
        assert_eq!(check_parity(&booltest_prog(kind), &cols, n), Some(false));
    }

    // Density sweep incl. all-null and none-null, over the 64-row boundary.
    let mut r = Lcg(0x4EE1);
    for &np in &[0u64, 100, 37] {
        for &nrows in &[1u32, 63, 64, 65, 130] {
            let c = gen_batch_data(&mut r, &[ColTy::I32], nrows as usize, np);
            for kind in [NullTestKind::IsNull, NullTestKind::IsNotNull] {
                assert_eq!(check_parity(&nulltest_prog(kind), &c, nrows), Some(false));
            }
            for kind in [BoolTestKind::IsTrue, BoolTestKind::IsNotFalse] {
                assert_eq!(check_parity(&booltest_prog(kind), &c, nrows), Some(false));
            }
        }
    }

    // A NULL/bool test AND a SIMD comparator: the generic clause rides the
    // bit-iteration tail of a NEON program.
    let mut p = Program::new();
    let k = p.push_const(NullableDatum { value: Datum::from_i32(0), isnull: false });
    p.steps = vec![
        Step::LoadLane { col: 0, out: 0 },
        Step::LoadConst { k, out: 1 },
        Step::Cmp { op: CmpOp::Int4Ge, a: 0, b: 1, out: 2 },
        Step::Qual { a: 2 },
        Step::LoadLane { col: 0, out: 0 },
        Step::NullTest { a: 0, out: 1, kind: NullTestKind::IsNotNull },
        Step::Qual { a: 1 },
    ];
    let c = gen_batch_data(&mut Lcg(9), &[ColTy::I32], 130, 20);
    assert_eq!(check_parity(&p, &c, 130), Some(false));
}

// ---- item 3: wider arithmetic (int2 / int8) -------------------------------

/// `(col aop rhs) cmp k2` over one column, rhs either a const or the column
/// again (for a*a-style traps). Width picks the ArithOp + comparator family.
fn arith_prog(aop: ArithOp, cmp: CmpOp, rhs: Rhs, k2: Datum) -> Program {
    let mut p = Program::new();
    p.steps.push(Step::LoadLane { col: 0, out: 0 });
    match rhs {
        Rhs::SelfCol => p.steps.push(Step::LoadLane { col: 0, out: 1 }),
        Rhs::Const(d) => {
            let k = p.push_const(NullableDatum { value: d, isnull: false });
            p.steps.push(Step::LoadConst { k, out: 1 });
        }
    }
    let k2i = p.push_const(NullableDatum { value: k2, isnull: false });
    p.steps.extend([
        Step::Arith { op: aop, a: 0, b: 1, out: 2 },
        Step::LoadConst { k: k2i, out: 3 },
        Step::Cmp { op: cmp, a: 2, b: 3, out: 4 },
        Step::Qual { a: 4 },
    ]);
    p
}

enum Rhs {
    SelfCol,
    Const(Datum),
}

fn plant(n: u32, c_row: usize, trap: Datum, filler: Datum) -> Vec<ColData> {
    let values: Vec<Datum> = (0..n as usize)
        .map(|i| if i == c_row { trap } else { filler })
        .collect();
    one_col(values, vec![false; n as usize])
}

#[test]
fn wider_arith_boundaries() {
    if !lanestitch::available() {
        return;
    }
    let n = 200u32;
    let c = 137usize;

    // -- int2 (smallint) traps: add/sub/mul overflow + MIN/-1 div. --
    // MAX + 1 overflows -> "smallint out of range" at row c.
    let p = arith_prog(ArithOp::Add2, CmpOp::Int2Gt, Rhs::Const(Datum::from_i16(1)), Datum::from_i16(0));
    let cols = plant(n, c, Datum::from_i16(i16::MAX), Datum::from_i16(3));
    assert_eq!(check_parity(&p, &cols, n), Some(true));
    let (m, _, row) = stitched_outcome(&StitchedProgram::compile(&p, 1).unwrap(), &p, &cols, n).unwrap_err();
    assert_eq!(m, "smallint out of range");
    assert_eq!(row as usize, c);

    // MIN - 1 overflows.
    let p = arith_prog(ArithOp::Sub2, CmpOp::Int2Ne, Rhs::Const(Datum::from_i16(1)), Datum::from_i16(0));
    let cols = plant(n, c, Datum::from_i16(i16::MIN), Datum::from_i16(3));
    assert_eq!(check_parity(&p, &cols, n), Some(true));

    // 200 * 200 = 40000 > i16::MAX -> overflow (self-mul).
    let p = arith_prog(ArithOp::Mul2, CmpOp::Int2Gt, Rhs::SelfCol, Datum::from_i16(0));
    let cols = plant(n, c, Datum::from_i16(200), Datum::from_i16(2));
    assert_eq!(check_parity(&p, &cols, n), Some(true));

    // i16::MIN / -1 overflows (the sneaky div arm).
    let p = arith_prog(ArithOp::Div2, CmpOp::Int2Ne, Rhs::Const(Datum::from_i16(-1)), Datum::from_i16(0));
    let cols = plant(n, c, Datum::from_i16(i16::MIN), Datum::from_i16(6));
    assert_eq!(check_parity(&p, &cols, n), Some(true));

    // int2 div-by-zero identity.
    let p = arith_prog(ArithOp::Div2, CmpOp::Int2Ge, Rhs::Const(Datum::from_i16(0)), Datum::from_i16(0));
    let cols = plant(n, c, Datum::from_i16(5), Datum::from_i16(5));
    let (m, _, _) = stitched_outcome(&StitchedProgram::compile(&p, 1).unwrap(), &p, &cols, n).unwrap_err();
    assert_eq!(m, "division by zero");

    // Clean int2 (no trap): exact bits.
    let p = arith_prog(ArithOp::Add2, CmpOp::Int2Gt, Rhs::Const(Datum::from_i16(10)), Datum::from_i16(0));
    let cols = one_col((0..n).map(|i| Datum::from_i16((i as i16 % 50) - 25)).collect(), vec![false; n as usize]);
    assert_eq!(check_parity(&p, &cols, n), Some(false));

    // -- int8 (bigint) traps. --
    // MAX + 1 overflows -> "bigint out of range".
    let p = arith_prog(ArithOp::Add8, CmpOp::Int8Gt, Rhs::Const(Datum::from_i64(1)), Datum::from_i64(0));
    let cols = plant(n, c, Datum::from_i64(i64::MAX), Datum::from_i64(3));
    assert_eq!(check_parity(&p, &cols, n), Some(true));
    let (m, _, row) = stitched_outcome(&StitchedProgram::compile(&p, 1).unwrap(), &p, &cols, n).unwrap_err();
    assert_eq!(m, "bigint out of range");
    assert_eq!(row as usize, c);

    // MIN - 1 overflows.
    let p = arith_prog(ArithOp::Sub8, CmpOp::Int8Ne, Rhs::Const(Datum::from_i64(1)), Datum::from_i64(0));
    let cols = plant(n, c, Datum::from_i64(i64::MIN), Datum::from_i64(3));
    assert_eq!(check_parity(&p, &cols, n), Some(true));

    // int8 MIN * -1 overflows (MIN × -1).
    let p = arith_prog(ArithOp::Mul8, CmpOp::Int8Ne, Rhs::Const(Datum::from_i64(-1)), Datum::from_i64(0));
    let cols = plant(n, c, Datum::from_i64(i64::MIN), Datum::from_i64(3));
    assert_eq!(check_parity(&p, &cols, n), Some(true));

    // int8 MIN * MIN overflows (self-mul).
    let p = arith_prog(ArithOp::Mul8, CmpOp::Int8Gt, Rhs::SelfCol, Datum::from_i64(0));
    let cols = plant(n, c, Datum::from_i64(i64::MIN), Datum::from_i64(2));
    assert_eq!(check_parity(&p, &cols, n), Some(true));

    // int8 MIN / -1 overflows.
    let p = arith_prog(ArithOp::Div8, CmpOp::Int8Ne, Rhs::Const(Datum::from_i64(-1)), Datum::from_i64(0));
    let cols = plant(n, c, Datum::from_i64(i64::MIN), Datum::from_i64(6));
    assert_eq!(check_parity(&p, &cols, n), Some(true));

    // Clean int8 self-mul near the boundary but non-overflowing.
    let p = arith_prog(ArithOp::Mul8, CmpOp::Int8Ge, Rhs::SelfCol, Datum::from_i64(0));
    let cols = one_col((0..n).map(|i| Datum::from_i64(i as i64 - 100)).collect(), vec![false; n as usize]);
    assert_eq!(check_parity(&p, &cols, n), Some(false));
}

// ---- item 2: SAOP / IN-list (const array ANY) -----------------------------

fn saop_prog(op: CmpOp, elems: Vec<NullableDatum>) -> Program {
    let mut p = Program::new();
    let arr = p.push_array(elems);
    p.steps = vec![
        Step::LoadLane { col: 0, out: 0 },
        Step::SaopAny { a: 0, out: 1, op, arr },
        Step::Qual { a: 1 },
    ];
    p
}

fn nd(d: Datum) -> NullableDatum {
    NullableDatum { value: d, isnull: false }
}

#[test]
fn saop_directed() {
    if !lanestitch::available() {
        return;
    }
    let n = 200u32;
    // Scalar column: mix of the IN-set members, non-members, and NULLs.
    let values: Vec<Datum> = (0..n).map(|i| Datum::from_i32((i as i32 % 11) - 3)).collect();
    let isnull: Vec<bool> = (0..n).map(|i| i % 9 == 0).collect();
    let cols = one_col(values, isnull);

    // Empty array: ANY is always false (fails every qual), never NULL.
    assert_eq!(check_parity(&saop_prog(CmpOp::Int4Eq, vec![]), &cols, n), Some(false));

    // 1-element.
    let p = saop_prog(CmpOp::Int4Eq, vec![nd(Datum::from_i32(5))]);
    assert_eq!(check_parity(&p, &cols, n), Some(false));

    // Multi-element IN-list, no nulls.
    let elems: Vec<NullableDatum> = [-3, 0, 5, 7, -1].iter().map(|&v| nd(Datum::from_i32(v))).collect();
    assert_eq!(check_parity(&saop_prog(CmpOp::Int4Eq, elems.clone()), &cols, n), Some(false));

    // With a NULL element: rows matching a real element still pass; rows
    // matching none go NULL (fail qual) — three-valued logic.
    let mut with_null = elems.clone();
    with_null.push(NullableDatum::null());
    assert_eq!(check_parity(&saop_prog(CmpOp::Int4Eq, with_null.clone()), &cols, n), Some(false));

    // All-NULL array: every result NULL -> every row fails.
    let all_null = vec![NullableDatum::null(); 4];
    assert_eq!(check_parity(&saop_prog(CmpOp::Int4Eq, all_null), &cols, n), Some(false));

    // Non-equality ANY (col < ANY {..}) and int8 / oid families.
    assert_eq!(check_parity(&saop_prog(CmpOp::Int4Lt, elems), &cols, n), Some(false));

    let i64vals: Vec<Datum> = (0..n).map(|i| Datum::from_i64(i as i64 % 7)).collect();
    let cols8 = one_col(i64vals, vec![false; n as usize]);
    let e8: Vec<NullableDatum> = [1i64, 3, 5].iter().map(|&v| nd(Datum::from_i64(v))).collect();
    assert_eq!(check_parity(&saop_prog(CmpOp::Int8Eq, e8), &cols8, n), Some(false));

    let oidvals: Vec<Datum> = (0..n).map(|i| canon_oid(i % 5)).collect();
    let colso = one_col(oidvals, vec![false; n as usize]);
    let eo: Vec<NullableDatum> = [0u32, 2, 4].iter().map(|&v| nd(canon_oid(v))).collect();
    assert_eq!(check_parity(&saop_prog(CmpOp::OidGe, eo), &colso, n), Some(false));

    // Density + geometry sweep (straddles the 64-row block boundary).
    let mut r = Lcg(0x5A0F);
    let elems: Vec<NullableDatum> = [2, -3, 0, 4].iter().map(|&v| nd(Datum::from_i32(v))).collect();
    for &nrows in &[1u32, 63, 64, 65, 128, 1000] {
        for &np in &[0u64, 30, 100] {
            let c = gen_batch_data(&mut r, &[ColTy::I32], nrows as usize, np);
            let mut e = elems.clone();
            if np == 30 {
                e.push(NullableDatum::null());
            }
            assert_eq!(check_parity(&saop_prog(CmpOp::Int4Ne, e), &c, nrows), Some(false));
        }
    }

    // Fail-closed: float SAOP has no NaN-exact scalar cond -> refuse.
    let pf = saop_prog(CmpOp::Float8Eq, vec![nd(Datum::from_f64(1.0))]);
    assert!(StitchedProgram::compile(&pf, 1).is_none(), "float SAOP must refuse");
    // Fail-closed: an array index past the table refuses (built by hand).
    let mut pbad = Program::new();
    pbad.steps = vec![
        Step::LoadLane { col: 0, out: 0 },
        Step::SaopAny { a: 0, out: 1, op: CmpOp::Int4Eq, arr: 3 },
        Step::Qual { a: 1 },
    ];
    assert!(StitchedProgram::compile(&pbad, 1).is_none(), "missing array must refuse");
    // Fail-closed: over-long array refuses (code-size bound).
    let big: Vec<NullableDatum> = (0..200).map(|v| nd(Datum::from_i32(v))).collect();
    assert!(StitchedProgram::compile(&saop_prog(CmpOp::Int4Eq, big), 1).is_none(), "over-long SAOP must refuse");
}

// ---- item 4: date / timestamp comparisons as their int carriers -----------

/// Date is an int4 carrier, timestamp/timestamptz are int8 carriers, all with
/// -infinity/+infinity as the type sentinels (INT_MIN/MAX, INT64_MIN/MAX).
/// Those sentinels sort as plain signed integers, so the Int4/Int8 comparator
/// families already implement the exact date/time ordering — no new stencil.
/// This pins that reasoning: sentinel-laden columns compare identically on
/// both tiers.
#[test]
fn date_timestamp_carrier_ordering() {
    if !lanestitch::available() {
        return;
    }
    let n = 256u32;
    // date column: -infinity (INT_MIN), +infinity (INT_MAX), and ordinary
    // day counts; compare against a fixed date const.
    let dpool = [i32::MIN, i32::MAX, 0, 1, -1, 7305, 20000, -730];
    let dates: Vec<Datum> =
        (0..n).map(|i| Datum::from_i32(dpool[i as usize % dpool.len()])).collect();
    let cols = one_col(dates, vec![false; n as usize]);
    for op in [CmpOp::Int4Lt, CmpOp::Int4Ge, CmpOp::Int4Eq, CmpOp::Int4Gt] {
        let p = cmp_const_prog(op, Datum::from_i32(7305));
        assert_eq!(check_parity(&p, &cols, n), Some(false));
    }

    // timestamp column: -infinity (INT64_MIN), +infinity (INT64_MAX), epoch,
    // and ordinary microsecond counts.
    let tpool = [i64::MIN, i64::MAX, 0, 1, -1, 1_000_000, -1_000_000, 987_654_321];
    let ts: Vec<Datum> = (0..n).map(|i| Datum::from_i64(tpool[i as usize % tpool.len()])).collect();
    let cols8 = one_col(ts, vec![false; n as usize]);
    for op in [CmpOp::Int8Lt, CmpOp::Int8Ge, CmpOp::Int8Le, CmpOp::Int8Ne] {
        let mut p = Program::new();
        let k = p.push_const(nd(Datum::from_i64(0)));
        p.steps = vec![
            Step::LoadLane { col: 0, out: 0 },
            Step::LoadConst { k, out: 1 },
            Step::Cmp { op, a: 0, b: 1, out: 2 },
            Step::Qual { a: 2 },
        ];
        assert_eq!(check_parity(&p, &cols8, n), Some(false));
    }
}

// ---- the grown-vocabulary fuzz gauntlet -----------------------------------

/// Randomized programs mixing every grown shape (NULL/bool tests, int2/int8
/// arith with overflow-dense operands, const-array SAOP) against the
/// interpreter reference, over the same batch geometries / NULL densities as
/// the founding gauntlet.
#[test]
fn fuzz_parity_new_vocab() {
    let seed = std::env::var("LANESTITCH_FUZZ_SEED")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0x0DDBA11);
    let mut r = Lcg(seed);
    let tys: &[ColTy] = &[ColTy::I16, ColTy::I32, ColTy::I64, ColTy::Oid];
    let geometries: &[u32] = &[1, 7, 63, 64, 65, 128, 200, 1000, MAX_ROWS as u32];
    let mut compiles = 0u32;
    let mut stitched = 0u32;
    let mut replays = 0u32;
    let mut simd_bodies = 0u32;
    for case in 0..500u32 {
        let prog = gen_new_vocab_program(&mut r);
        let Some(jit) = StitchedProgram::compile(&prog, tys.len()) else {
            assert!(!lanestitch::available(), "new-vocab program refused (case {case})");
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
            (Ok(w), Ok(g)) => {
                assert_eq!(w, g, "case {case} nrows {nrows} null% {null_pct}");
                stitched += 1;
            }
            (Err(we), Err(ge)) => {
                assert_eq!(we, ge, "error identity/position diverged (case {case})");
                replays += 1;
            }
            _ => panic!("tier disagreement on error (case {case})"),
        }
    }
    assert!(compiles >= 400, "too few compiles: {compiles}");
    assert!(stitched >= 200, "too few stitched batches: {stitched}");
    assert!(replays >= 3, "too few refuse-and-replay cases: {replays}");
    // NullTest/BoolTest/SAOP clauses now ride the NEON vector pass: arith-
    // free draws (a majority) must classify SIMD.
    assert!(
        simd_pinned_off() || simd_bodies >= 100,
        "too few SIMD bodies in the grown vocabulary: {simd_bodies}"
    );
}

/// One random clause from the grown vocabulary over a fixed 4-column layout
/// [i16, i32, i64, oid]. Registers are clause-local (0,1,2) so every clause
/// is register-self-contained.
fn gen_new_vocab_program(r: &mut Lcg) -> Program {
    // layout: col0 i16, col1 i32, col2 i64, col3 oid.
    let mut p = Program::new();
    let nclauses = 1 + r.below(3) as usize;
    for _ in 0..nclauses {
        match r.below(6) {
            0 => {
                // NULL test on a random column.
                let col = r.below(4) as u16;
                let kind = if r.chance(50) { NullTestKind::IsNull } else { NullTestKind::IsNotNull };
                p.steps.extend([
                    Step::LoadLane { col, out: 0 },
                    Step::NullTest { a: 0, out: 1, kind },
                    Step::Qual { a: 1 },
                ]);
            }
            1 => {
                // bool test on a random column (any int reads as bool: !=0).
                let col = r.below(4) as u16;
                let kind = [
                    BoolTestKind::IsTrue,
                    BoolTestKind::IsNotTrue,
                    BoolTestKind::IsFalse,
                    BoolTestKind::IsNotFalse,
                ][r.below(4) as usize];
                p.steps.extend([
                    Step::LoadLane { col, out: 0 },
                    Step::BoolTest { a: 0, out: 1, kind },
                    Step::Qual { a: 1 },
                ]);
            }
            2 => {
                // int2 arith clause (col0), overflow-dense pool.
                let aop = [ArithOp::Add2, ArithOp::Sub2, ArithOp::Mul2, ArithOp::Div2][r.below(4) as usize];
                let cmp = [CmpOp::Int2Gt, CmpOp::Int2Le, CmpOp::Int2Ne][r.below(3) as usize];
                let k = p.push_const(nd(gen_value(r, ColTy::I16)));
                let k2 = p.push_const(nd(gen_value(r, ColTy::I16)));
                p.steps.push(Step::LoadLane { col: 0, out: 0 });
                if r.chance(50) {
                    p.steps.push(Step::LoadLane { col: 0, out: 1 });
                } else {
                    p.steps.push(Step::LoadConst { k, out: 1 });
                }
                p.steps.extend([
                    Step::Arith { op: aop, a: 0, b: 1, out: 2 },
                    Step::LoadConst { k: k2, out: 0 },
                    Step::Cmp { op: cmp, a: 2, b: 0, out: 1 },
                    Step::Qual { a: 1 },
                ]);
            }
            3 => {
                // int8 arith clause (col2), overflow-dense pool.
                let aop = [ArithOp::Add8, ArithOp::Sub8, ArithOp::Mul8, ArithOp::Div8][r.below(4) as usize];
                let cmp = [CmpOp::Int8Gt, CmpOp::Int8Le, CmpOp::Int8Ne][r.below(3) as usize];
                let k = p.push_const(nd(gen_value(r, ColTy::I64)));
                let k2 = p.push_const(nd(gen_value(r, ColTy::I64)));
                p.steps.push(Step::LoadLane { col: 2, out: 0 });
                if r.chance(50) {
                    p.steps.push(Step::LoadLane { col: 2, out: 1 });
                } else {
                    p.steps.push(Step::LoadConst { k, out: 1 });
                }
                p.steps.extend([
                    Step::Arith { op: aop, a: 0, b: 1, out: 2 },
                    Step::LoadConst { k: k2, out: 0 },
                    Step::Cmp { op: cmp, a: 2, b: 0, out: 1 },
                    Step::Qual { a: 1 },
                ]);
            }
            4 => {
                // SAOP int4 IN-list on col1, sometimes with a NULL element.
                let nelem = r.below(6) as usize; // 0..5 (incl. empty)
                let mut elems: Vec<NullableDatum> = (0..nelem)
                    .map(|_| {
                        if r.chance(15) {
                            NullableDatum::null()
                        } else {
                            nd(gen_value(r, ColTy::I32))
                        }
                    })
                    .collect();
                if r.chance(20) {
                    elems.push(NullableDatum::null());
                }
                let op = [CmpOp::Int4Eq, CmpOp::Int4Ne, CmpOp::Int4Lt, CmpOp::Int4Ge][r.below(4) as usize];
                let arr = p.push_array(elems);
                p.steps.extend([
                    Step::LoadLane { col: 1, out: 0 },
                    Step::SaopAny { a: 0, out: 1, op, arr },
                    Step::Qual { a: 1 },
                ]);
            }
            _ => {
                // SAOP oid IN-list on col3.
                let nelem = 1 + r.below(5) as usize;
                let elems: Vec<NullableDatum> =
                    (0..nelem).map(|_| nd(gen_value(r, ColTy::Oid))).collect();
                let op = [CmpOp::OidEq, CmpOp::OidLt, CmpOp::OidGe][r.below(3) as usize];
                let arr = p.push_array(elems);
                p.steps.extend([
                    Step::LoadLane { col: 3, out: 0 },
                    Step::SaopAny { a: 0, out: 1, op, arr },
                    Step::Qual { a: 1 },
                ]);
            }
        }
    }
    p
}

// ==========================================================================
// Widened SIMD tier (lane-v2-simdbreadth): float var-var (FCmpVar), fused
// NullTest/BoolTest, and SAOP promoted from the per-row bits loop into the
// NEON vector pass. Every shape parity-fuzzed on both the 64-row block path
// and the scalar tail, plus engagement pins (the shapes must actually take
// the SIMD tier when it isn't pinned off).
// ==========================================================================

fn fcmp_var_prog(op: CmpOp) -> Program {
    let mut p = Program::new();
    p.steps = vec![
        Step::LoadLane { col: 0, out: 0 },
        Step::LoadLane { col: 1, out: 1 },
        Step::Cmp { op, a: 0, b: 1, out: 2 },
        Step::Qual { a: 2 },
    ];
    p
}

/// Adversarial float pools: multiple NaN payloads (quiet/signaling-pattern,
/// both signs — PG treats every NaN as one value), signed zeros, infinities,
/// denormals, and sign edges.
fn f64_edge(i: usize) -> Datum {
    let pool = [
        f64::NAN,
        f64::from_bits(0x7FF8_0000_0000_0001), // quiet NaN, nonzero payload
        f64::from_bits(0xFFF8_0000_0000_0000), // negative quiet NaN
        f64::from_bits(0x7FF0_0000_0000_0001), // signaling-pattern NaN
        f64::NEG_INFINITY,
        f64::INFINITY,
        -0.0,
        0.0,
        f64::MIN_POSITIVE,
        -f64::MIN_POSITIVE,
        5e-324, // smallest denormal
        1.0,
        -1.5,
        f64::MAX,
        f64::MIN,
    ];
    Datum::from_f64(pool[i % pool.len()])
}

fn f32_edge(i: usize) -> Datum {
    let pool = [
        f32::NAN,
        f32::from_bits(0x7FC0_0001), // quiet NaN, nonzero payload
        f32::from_bits(0xFFC0_0000), // negative quiet NaN
        f32::from_bits(0x7F80_0001), // signaling-pattern NaN
        f32::NEG_INFINITY,
        f32::INFINITY,
        -0.0,
        0.0,
        f32::MIN_POSITIVE,
        1e-45, // smallest denormal
        1.0,
        -1.5,
        f32::MAX,
        f32::MIN,
    ];
    Datum::from_f32(pool[i % pool.len()])
}

/// Float var-var over every family and relation, NaN-payload-dense lanes,
/// SIMD blocks and scalar tails: exact pgf_* (float.h) parity on both tiers.
#[test]
fn float_var_var_nan_ordering() {
    if !lanestitch::available() {
        return;
    }
    let fams: &[(ColTy, ColTy, &[CmpOp])] = FLOAT_FAMS;
    let mut r = Lcg(0xF10A7);
    for &(a_ty, b_ty, ops) in fams {
        for &op in ops {
            let prog = fcmp_var_prog(op);
            let jit = StitchedProgram::compile(&prog, 2)
                .expect("fused float var-var must compile");
            assert!(
                simd_pinned_off() || jit.is_simd(),
                "float var-var must take the NEON tier ({op:?})"
            );
            for &nrows in &[63u32, 64, 65, 130, 257] {
                for &null_pct in &[0u64, 20] {
                    // Edge-pool lanes with different strides so every
                    // (edge, edge) pair occurs, plus random fill.
                    let mk = |ty: ColTy, stride: usize, r: &mut Lcg| -> ColData {
                        let values = (0..nrows as usize)
                            .map(|i| {
                                if r.chance(75) {
                                    match ty {
                                        ColTy::F32 => f32_edge(i * stride + stride - 1),
                                        _ => f64_edge(i * stride + stride - 1),
                                    }
                                } else {
                                    gen_value(r, ty)
                                }
                            })
                            .collect();
                        let isnull = (0..nrows as usize).map(|_| r.chance(null_pct)).collect();
                        ColData { values, isnull }
                    };
                    let cols = vec![mk(a_ty, 1, &mut r), mk(b_ty, 7, &mut r)];
                    let want = interp_outcome(&prog, &cols, nrows).unwrap();
                    let got = stitched_outcome(&jit, &prog, &cols, nrows).unwrap();
                    assert_eq!(want, got, "{op:?} nrows={nrows} null%={null_pct}");
                }
            }
        }
    }
}

/// The promoted shapes must actually engage the NEON tier: single-clause
/// NullTest / BoolTest / SAOP programs (previously Generic -> scalar body)
/// now classify SIMD, and their parity holds on block-boundary geometries.
#[test]
fn simd_engagement_of_promoted_shapes() {
    if !lanestitch::available() {
        return;
    }
    let mut progs: Vec<(&str, Program)> = Vec::new();
    for kind in [NullTestKind::IsNull, NullTestKind::IsNotNull] {
        progs.push(("nulltest", nulltest_prog(kind)));
    }
    for kind in [
        BoolTestKind::IsTrue,
        BoolTestKind::IsNotTrue,
        BoolTestKind::IsFalse,
        BoolTestKind::IsNotFalse,
    ] {
        progs.push(("booltest", booltest_prog(kind)));
    }
    for op in [CmpOp::Int4Eq, CmpOp::Int4Ne, CmpOp::Int4Lt, CmpOp::Int4Ge] {
        let elems: Vec<NullableDatum> =
            [-3, 0, 5, 7, 9].iter().map(|&v| nd(Datum::from_i32(v))).collect();
        progs.push(("saop", saop_prog(op, elems)));
    }
    // Empty and full-cap IN-lists, and one with NULL elements.
    progs.push(("saop-empty", saop_prog(CmpOp::Int4Eq, vec![])));
    progs.push((
        "saop-full",
        saop_prog(
            CmpOp::Int4Eq,
            (0..128).map(|v| nd(Datum::from_i32(v * 3 - 40))).collect(),
        ),
    ));
    let mut with_null: Vec<NullableDatum> =
        [2, 4].iter().map(|&v| nd(Datum::from_i32(v))).collect();
    with_null.push(NullableDatum::null());
    progs.push(("saop-nullelem", saop_prog(CmpOp::Int4Ne, with_null)));

    let mut r = Lcg(0x51D3);
    for (name, prog) in &progs {
        let jit = StitchedProgram::compile(prog, 1).expect("promoted shape must compile");
        assert!(
            simd_pinned_off() || jit.is_simd(),
            "{name} must take the NEON tier"
        );
        for &nrows in &[1u32, 63, 64, 65, 129, 1000] {
            for &np in &[0u64, 25, 100] {
                let cols = gen_batch_data(&mut r, &[ColTy::I32], nrows as usize, np);
                let want = interp_outcome(prog, &cols, nrows).unwrap();
                let got = stitched_outcome(&jit, prog, &cols, nrows).unwrap();
                assert_eq!(want, got, "{name} nrows={nrows} null%={np}");
            }
        }
    }

    // An arith clause still refuses the tier for the whole program (the
    // reordering legality rests on it).
    let mut p = nulltest_prog(NullTestKind::IsNotNull);
    let k = p.push_const(nd(Datum::from_i32(1)));
    let k2 = p.push_const(nd(Datum::from_i32(0)));
    p.steps.extend([
        Step::LoadLane { col: 0, out: 0 },
        Step::LoadConst { k, out: 1 },
        Step::Arith { op: ArithOp::Add4, a: 0, b: 1, out: 2 },
        Step::LoadConst { k: k2, out: 3 },
        Step::Cmp { op: CmpOp::Int4Gt, a: 2, b: 3, out: 4 },
        Step::Qual { a: 4 },
    ]);
    let jit = StitchedProgram::compile(&p, 1).unwrap();
    assert!(!jit.is_simd(), "an arith clause must refuse the SIMD tier");
}

/// A kitchen-sink mixed program: every vector-pass shape in one qual (int
/// cmp-const, float cmp-const, float var-var, oid cmp, NullTest, BoolTest,
/// SAOP) — one NEON pass builds the whole word; parity across geometries.
#[test]
fn simd_all_shapes_one_program() {
    let mut prog = Program::new();
    let k = prog.push_const(nd(Datum::from_i32(-100)));
    let kf = prog.push_const(nd(Datum::from_f64(0.25)));
    let arr = prog.push_array(
        [1i32, 3, 5, 700, -2].iter().map(|&v| nd(Datum::from_i32(v))).collect(),
    );
    prog.steps = vec![
        // int4 cmp-const
        Step::LoadLane { col: 0, out: 0 },
        Step::LoadConst { k, out: 1 },
        Step::Cmp { op: CmpOp::Int4Ge, a: 0, b: 1, out: 2 },
        Step::Qual { a: 2 },
        // float8 cmp-const
        Step::LoadLane { col: 1, out: 0 },
        Step::LoadConst { k: kf, out: 1 },
        Step::Cmp { op: CmpOp::Float8Le, a: 0, b: 1, out: 2 },
        Step::Qual { a: 2 },
        // float var-var (f64 vs f32)
        Step::LoadLane { col: 1, out: 0 },
        Step::LoadLane { col: 2, out: 1 },
        Step::Cmp { op: CmpOp::Float84Gt, a: 0, b: 1, out: 2 },
        Step::Qual { a: 2 },
        // NullTest
        Step::LoadLane { col: 3, out: 0 },
        Step::NullTest { a: 0, out: 1, kind: NullTestKind::IsNotNull },
        Step::Qual { a: 1 },
        // BoolTest
        Step::LoadLane { col: 0, out: 0 },
        Step::BoolTest { a: 0, out: 1, kind: BoolTestKind::IsNotFalse },
        Step::Qual { a: 1 },
        // SAOP
        Step::LoadLane { col: 0, out: 0 },
        Step::SaopAny { a: 0, out: 1, op: CmpOp::Int4Ne, arr },
        Step::Qual { a: 1 },
    ];
    let tys = &[ColTy::I32, ColTy::F64, ColTy::F32, ColTy::I64];
    let Some(jit) = StitchedProgram::compile(&prog, 4) else {
        assert!(!lanestitch::available());
        return;
    };
    assert!(simd_pinned_off() || jit.is_simd(), "all-vector program must take the NEON tier");
    let mut r = Lcg(0xA11);
    for &nrows in &[63u32, 64, 65, 191, 1024] {
        for &np in &[0u64, 15, 60] {
            let cols = gen_batch_data(&mut r, tys, nrows as usize, np);
            let want = interp_outcome(&prog, &cols, nrows).unwrap();
            let got = stitched_outcome(&jit, &prog, &cols, nrows).unwrap();
            assert_eq!(want, got, "nrows={nrows} null%={np}");
        }
    }
}

// ---- projection tier ------------------------------------------------------
//
// The stitched projection contract: for every accepted program and every
// batch + selection mask, run_into == eval_project on all SELECTED rows'
// output cells; an erroring program (arith trap on a selected row) exits
// Refused with NO error constructed where eval_project raises — the driver's
// per-row replay owns error identity. Non-selected rows' cells are
// unspecified (the consumer contract only covers selected rows).

use lanestitch::{eval_project, OutLane, ProjOutcome, StitchedProjection};

/// Random projection program over int columns: 1..=4 output columns drawn
/// from {Var passthrough, arith var-var, arith var-const}.
fn gen_proj_program(r: &mut Lcg, tys: &[ColTy]) -> (Program, usize) {
    let mut prog = Program::new();
    let nouts = 1 + r.below(4) as usize;
    let int_col = |r: &mut Lcg, tys: &[ColTy]| -> (u16, ColTy) {
        loop {
            let c = r.below(tys.len() as u64) as usize;
            if matches!(tys[c], ColTy::I16 | ColTy::I32 | ColTy::I64) {
                return (c as u16, tys[c]);
            }
        }
    };
    let arith_for = |r: &mut Lcg, ty: ColTy| -> ArithOp {
        let k = r.below(4);
        match (ty, k) {
            (ColTy::I16, 0) => ArithOp::Add2,
            (ColTy::I16, 1) => ArithOp::Sub2,
            (ColTy::I16, 2) => ArithOp::Mul2,
            (ColTy::I16, _) => ArithOp::Div2,
            (ColTy::I32, 0) => ArithOp::Add4,
            (ColTy::I32, 1) => ArithOp::Sub4,
            (ColTy::I32, 2) => ArithOp::Mul4,
            (ColTy::I32, _) => ArithOp::Div4,
            (_, 0) => ArithOp::Add8,
            (_, 1) => ArithOp::Sub8,
            (_, 2) => ArithOp::Mul8,
            (_, _) => ArithOp::Div8,
        }
    };
    for j in 0..nouts {
        match r.below(3) {
            0 => {
                let c = r.below(tys.len() as u64) as u16;
                prog.steps.push(Step::LoadLane { col: c, out: 0 });
                prog.steps.push(Step::StoreOut { a: 0, out: j as u16 });
            }
            1 => {
                let (a, ty) = int_col(r, tys);
                let (b, _) = loop {
                    let (b, bty) = int_col(r, tys);
                    if bty == ty {
                        break (b, bty);
                    }
                };
                prog.steps.push(Step::LoadLane { col: a, out: 0 });
                prog.steps.push(Step::LoadLane { col: b, out: 1 });
                prog.steps.push(Step::Arith { op: arith_for(r, ty), a: 0, b: 1, out: 2 });
                prog.steps.push(Step::StoreOut { a: 2, out: j as u16 });
            }
            _ => {
                let (a, ty) = int_col(r, tys);
                let k = prog.push_const(NullableDatum { value: gen_value(r, ty), isnull: false });
                prog.steps.push(Step::LoadLane { col: a, out: 0 });
                prog.steps.push(Step::LoadConst { k, out: 1 });
                let (x, y) = if r.chance(50) { (0u8, 1u8) } else { (1u8, 0u8) };
                prog.steps.push(Step::Arith { op: arith_for(r, ty), a: x, b: y, out: 2 });
                prog.steps.push(Step::StoreOut { a: 2, out: j as u16 });
            }
        }
    }
    (prog, nouts)
}

struct OutBufs {
    values: Vec<Vec<Datum>>,
    isnull: Vec<Vec<bool>>,
}

impl OutBufs {
    fn new(nouts: usize, nrows: usize) -> OutBufs {
        OutBufs {
            values: vec![vec![Datum::from_i64(-777); nrows]; nouts],
            isnull: vec![vec![false; nrows]; nouts],
        }
    }

    fn lanes(&mut self) -> Vec<OutLane<'_>> {
        self.values
            .iter_mut()
            .zip(self.isnull.iter_mut())
            .map(|(v, n)| OutLane { values: v, isnull: n })
            .collect()
    }
}

fn random_sel(r: &mut Lcg, nrows: u32) -> SelVec {
    let mut sel = SelVec::all(nrows);
    for i in 0..nrows {
        if r.chance(40) {
            sel.clear(i);
        }
    }
    sel
}

#[test]
fn proj_fuzz_parity_vs_interpreter() {
    let mut r = Lcg(0x9e3779b97f4a7c15);
    let mut stitched_seen = 0u32;
    let mut refused_seen = 0u32;
    for round in 0..400 {
        let ncols = 1 + r.below(4) as usize;
        let tys: Vec<ColTy> = (0..ncols)
            .map(|_| match r.below(3) {
                0 => ColTy::I16,
                1 => ColTy::I32,
                _ => ColTy::I64,
            })
            .collect();
        let (prog, nouts) = gen_proj_program(&mut r, &tys);
        let nrows = 1 + r.below(MAX_ROWS as u64) as u32;
        let cols = gen_batch_data(&mut r, &tys, nrows as usize, 15);
        let sel = random_sel(&mut r, nrows);
        let batch = as_batch(&cols, nrows);

        // Reference: interpreter over selected rows.
        let mut want = OutBufs::new(nouts, nrows as usize);
        let interp_res = {
            let mut lanes = want.lanes();
            eval_project(&prog, &batch, &sel, &mut lanes)
        };

        let Some(body) = StitchedProjection::compile(&prog, ncols, nouts) else {
            // Off-arch / kill switch: nothing to compare.
            continue;
        };
        let mut got = OutBufs::new(nouts, nrows as usize);
        let nwords = (nrows as usize).div_ceil(64);
        let outcome = {
            let mut lanes = got.lanes();
            body.run_into(nrows, &batch.lanes, &sel.words[..nwords], &mut lanes)
        };
        match (outcome, &interp_res) {
            (ProjOutcome::Stitched, Ok(())) => {
                stitched_seen += 1;
                for j in 0..nouts {
                    for i in 0..nrows as usize {
                        if !sel.contains(i as u32) {
                            continue;
                        }
                        assert_eq!(
                            got.values[j][i].as_usize(),
                            want.values[j][i].as_usize(),
                            "round {round} out {j} row {i} value diverged"
                        );
                        assert_eq!(
                            got.isnull[j][i], want.isnull[j][i],
                            "round {round} out {j} row {i} isnull diverged"
                        );
                    }
                }
            }
            (ProjOutcome::Refused, Err(_)) => {
                // Trap parity: the body refused exactly where the
                // interpreter errored. Sticky: the next run refuses too.
                refused_seen += 1;
                let mut again = OutBufs::new(nouts, nrows as usize);
                let mut lanes = again.lanes();
                assert_eq!(
                    body.run_into(nrows, &batch.lanes, &sel.words[..nwords], &mut lanes),
                    ProjOutcome::Refused,
                    "round {round}: refusal must be sticky"
                );
            }
            (outcome, res) => panic!(
                "round {round}: outcome {outcome:?} vs interp {:?} — must agree on trap-or-not",
                res.as_ref().map(|_| ()).map_err(|e| e.message.clone())
            ),
        }
    }
    if cfg!(target_arch = "aarch64") && lanestitch::available() {
        // Adversarial pools (MIN/MAX heavy) make traps common; the floor
        // only pins that BOTH arms are exercised.
        assert!(stitched_seen > 50, "stitched projection engaged {stitched_seen} times only");
        assert!(refused_seen > 0, "no refuse-and-replay rounds seen");
    }
}

#[test]
fn proj_fail_closed_refusals() {
    // Qual step inside a projection program refuses.
    let mut prog = Program::new();
    prog.steps.push(Step::LoadLane { col: 0, out: 0 });
    prog.steps.push(Step::Qual { a: 0 });
    prog.steps.push(Step::StoreOut { a: 0, out: 0 });
    assert!(StitchedProjection::compile(&prog, 1, 1).is_none());

    // No StoreOut refuses.
    let mut prog = Program::new();
    prog.steps.push(Step::LoadLane { col: 0, out: 0 });
    assert!(StitchedProjection::compile(&prog, 1, 1).is_none());

    // Out index beyond nouts refuses.
    let mut prog = Program::new();
    prog.steps.push(Step::LoadLane { col: 0, out: 0 });
    prog.steps.push(Step::StoreOut { a: 0, out: 3 });
    assert!(StitchedProjection::compile(&prog, 1, 1).is_none());

    // Read of a never-written register refuses.
    let mut prog = Program::new();
    prog.steps.push(Step::StoreOut { a: 0, out: 0 });
    assert!(StitchedProjection::compile(&prog, 1, 1).is_none());

    // StoreOut in a QUAL program refuses (plan_clauses side).
    let mut prog = Program::new();
    prog.steps.push(Step::LoadLane { col: 0, out: 0 });
    prog.steps.push(Step::StoreOut { a: 0, out: 0 });
    prog.steps.push(Step::LoadLane { col: 0, out: 1 });
    prog.steps.push(Step::Qual { a: 1 });
    assert!(StitchedProgram::compile(&prog, 1).is_none());
}

#[test]
fn proj_selected_rows_only() {
    // Overflow on a NON-selected row must not trap: the body skips clear
    // bits entirely (the driver masks fallback rows out for exactly this).
    let values = vec![Datum::from_i32(i32::MAX), Datum::from_i32(1)];
    let isnull = vec![false, false];
    let cols = vec![
        ColData { values, isnull },
        ColData { values: vec![Datum::from_i32(1); 2], isnull: vec![false; 2] },
    ];
    let mut prog = Program::new();
    prog.steps.push(Step::LoadLane { col: 0, out: 0 });
    prog.steps.push(Step::LoadLane { col: 1, out: 1 });
    prog.steps.push(Step::Arith { op: ArithOp::Add4, a: 0, b: 1, out: 2 });
    prog.steps.push(Step::StoreOut { a: 2, out: 0 });
    let Some(body) = StitchedProjection::compile(&prog, 2, 1) else { return };
    let mut sel = SelVec::all(2);
    sel.clear(0); // the overflowing row is not selected
    let batch = as_batch(&cols, 2);
    let mut bufs = OutBufs::new(1, 2);
    let mut lanes = bufs.lanes();
    assert_eq!(
        body.run_into(2, &batch.lanes, &sel.words[..1], &mut lanes),
        ProjOutcome::Stitched
    );
    drop(lanes);
    assert_eq!(bufs.values[0][1].as_i32(), 2);
    assert!(!bufs.isnull[0][1]);

    // Same program, overflowing row selected: Refused, no error object.
    let sel = SelVec::all(2);
    let mut bufs = OutBufs::new(1, 2);
    let mut lanes = bufs.lanes();
    assert_eq!(
        body.run_into(2, &batch.lanes, &sel.words[..1], &mut lanes),
        ProjOutcome::Refused
    );
    drop(lanes);
    // And the interpreter raises C's message for the replay.
    let err = eval_project(&prog, &batch, &sel, &mut bufs.lanes()).unwrap_err();
    assert_eq!(err.message, "integer out of range");
}

#[test]
fn proj_null_propagation() {
    // Strict arith: NULL in -> NULL out; Var passthrough copies isnull.
    let cols = vec![
        ColData {
            values: vec![Datum::from_i64(5), Datum::from_i64(7)],
            isnull: vec![false, true],
        },
        ColData {
            values: vec![Datum::from_i64(2), Datum::from_i64(2)],
            isnull: vec![false, false],
        },
    ];
    let mut prog = Program::new();
    prog.steps.push(Step::LoadLane { col: 0, out: 0 });
    prog.steps.push(Step::LoadLane { col: 1, out: 1 });
    prog.steps.push(Step::Arith { op: ArithOp::Mul8, a: 0, b: 1, out: 2 });
    prog.steps.push(Step::StoreOut { a: 2, out: 0 });
    prog.steps.push(Step::LoadLane { col: 0, out: 3 });
    prog.steps.push(Step::StoreOut { a: 3, out: 1 });
    let batch = as_batch(&cols, 2);
    let sel = SelVec::all(2);
    let mut want = OutBufs::new(2, 2);
    eval_project(&prog, &batch, &sel, &mut want.lanes()).unwrap();
    assert_eq!(want.values[0][0].as_i64(), 10);
    assert!(!want.isnull[0][0]);
    assert!(want.isnull[0][1], "NULL lane must propagate through strict arith");
    assert!(want.isnull[1][1], "Var passthrough must copy isnull");
    if let Some(body) = StitchedProjection::compile(&prog, 2, 2) {
        let mut got = OutBufs::new(2, 2);
        let mut lanes = got.lanes();
        assert_eq!(
            body.run_into(2, &batch.lanes, &sel.words[..1], &mut lanes),
            ProjOutcome::Stitched
        );
        drop(lanes);
        for j in 0..2 {
            for i in 0..2 {
                assert_eq!(got.isnull[j][i], want.isnull[j][i]);
                if !want.isnull[j][i] {
                    assert_eq!(got.values[j][i].as_usize(), want.values[j][i].as_usize());
                }
            }
        }
    }
}

#[test]
fn proj_drift_fails_open() {
    let mut prog = Program::new();
    prog.steps.push(Step::LoadLane { col: 0, out: 0 });
    prog.steps.push(Step::StoreOut { a: 0, out: 0 });
    // Var-only programs classify (economics floors live in the driver).
    let Some(body) = StitchedProjection::compile(&prog, 1, 1) else { return };
    let cols = vec![ColData { values: vec![Datum::from_i32(1); 4], isnull: vec![false; 4] }];
    let batch = as_batch(&cols, 4);
    let sel = SelVec::all(4);
    // Short output lane: Drift (fail open), outputs untouched.
    let mut short_v = vec![Datum::from_i32(0); 2];
    let mut short_n = vec![false; 2];
    let mut lanes = [OutLane { values: &mut short_v, isnull: &mut short_n }];
    assert_eq!(
        body.run_into(4, &batch.lanes, &sel.words[..1], &mut lanes),
        ProjOutcome::Drift
    );
    // Missing lane: Drift.
    let mut bufs = OutBufs::new(1, 4);
    let mut lanes = bufs.lanes();
    assert_eq!(body.run_into(4, &[], &sel.words[..1], &mut lanes), ProjOutcome::Drift);
}
