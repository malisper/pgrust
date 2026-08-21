// The Tier B parity suite (R3's "parity fuzzer"): every stitched shape
// fuzz-compared against the oracle interpreter (the executable Step-IR
// specification) over randomized programs and batches — boundary values,
// NULL masks, geometries straddling the 64-row SIMD block, and trap rows
// exercising refuse-and-replay at the exact row. Plus fail-closed refusal
// pins, sticky/drift rails, and the µs-class stitch-time budget.
//
// Requires the `oracle` feature (the interpreter compiles for CI only).
// Off-aarch64, compile() refuses and the suites reduce to oracle
// self-checks. On Apple Silicon the emitted bodies DO execute inside this
// test process (plain mprotect W^X in an unhardened binary), so the full
// parity gauntlet runs on macOS CI as well as the Linux CI cluster; only the
// SVE2 stencils need Graviton (SVE2 hardware) to execute.

use datum::{Datum, NullableDatum};
use sqe_lanestitch::{
    eval_project, eval_qual, eval_row, ArithOp, Batch, BoolTestKind, CmpOp, Lane, NullTestKind,
    OutLane, ProjOutcome, Program, QualOutcome, SelVec, Step, StitchOpts, StitchedProgram,
    StitchedProjection, Sve2Pin, MAX_ROWS,
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
/// (the spec.rs contract).
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
    // Canonical SIGN-extension of the u32 image (required by the 2x64
    // unsigned SIMD compares).
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
                // NULL rows still carry adversarial datums: the SIMD tier
                // compares them unconditionally and must mask them out.
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
/// {int cmp-const, float cmp-const (non-NaN), int cmp-var, float cmp-var,
/// arith clause}. Skips a draw if the layout lacks the needed type.
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
    if prog.steps.is_empty() && tys.first() == Some(&ColTy::I32) {
        let k = prog.push_const(NullableDatum { value: Datum::from_i32(0), isnull: false });
        prog.steps.extend([
            Step::LoadLane { col: 0, out: 0 },
            Step::LoadConst { k, out: 1 },
            Step::Cmp { op: CmpOp::Int4Ge, a: 0, b: 1, out: 2 },
            Step::Qual { a: 2 },
        ]);
    }
    prog
}

type QualResult = Result<Vec<bool>, (String, SqlState, u32)>;

/// Oracle outcome: pass bits, or (message, sqlstate, rows-decided-before-
/// the-error) — the error-position currency.
fn interp_outcome(prog: &Program, cols: &[ColData], nrows: u32) -> QualResult {
    let batch = as_batch(cols, nrows);
    let mut sel = SelVec::all(nrows);
    match eval_qual(prog, &batch, &mut sel) {
        Ok(()) => Ok((0..nrows).map(|i| sel.contains(i)).collect()),
        Err(e) => {
            let mut decided = 0u32;
            for i in 0..nrows {
                let row_batch = as_batch(cols, nrows);
                match eval_row(prog, &row_batch, i) {
                    Ok(_) => decided += 1,
                    Err(_) => break,
                }
            }
            Err((e.message.clone(), e.sqlstate, decided))
        }
    }
}

/// Stitched outcome under the driver's refuse-and-replay discipline: a
/// `Refused` body constructed NO error, so the driver replays the batch on
/// the error-owning path (the oracle stands in here for the production AOT
/// error kernels) and asserts sticky refusal.
fn stitched_outcome(
    jit: &StitchedProgram,
    prog: &Program,
    cols: &[ColData],
    nrows: u32,
) -> QualResult {
    let batch = as_batch(cols, nrows);
    let mut sel = SelVec::all(nrows);
    match jit.run(&batch, &mut sel) {
        QualOutcome::Stitched => Ok((0..nrows).map(|i| sel.contains(i)).collect()),
        QualOutcome::Refused => {
            // Sticky: every later batch answers Refused without running.
            let mut sel2 = SelVec::all(nrows);
            assert_eq!(jit.run(&batch, &mut sel2), QualOutcome::Refused);
            // Driver-side replay on the oracle: raises the exact error.
            let mut sv = SelVec::all(nrows);
            let e = eval_qual(prog, &as_batch(cols, nrows), &mut sv)
                .expect_err("body refused but the oracle replay found no error");
            let mut decided = 0u32;
            for i in 0..nrows {
                match eval_row(prog, &as_batch(cols, nrows), i) {
                    Ok(_) => decided += 1,
                    Err(_) => break,
                }
            }
            Err((e.message.clone(), e.sqlstate, decided))
        }
        QualOutcome::Drift => panic!("unexpected drift on a well-staged batch"),
    }
}

/// Route one qual compile through the process-global stitched-body cache
/// (the P1-3 Tier-B cache gate): the second `get_or_stitch` is the
/// CACHED-HIT path, so every fuzz case that follows oracle-verifies a hit
/// body, and the direct compile pins golden-encoding stability (cached
/// body bytes == fresh-stitch bytes).
fn compile_via_cache(prog: &Program, ncols: usize) -> Option<StitchedProgram> {
    let cache = sqe_lanestitch::stitch_cache();
    let first = cache.get_or_stitch(prog, ncols)?;
    let hit = cache.get_or_stitch(prog, ncols)?;
    let fresh = StitchedProgram::compile(prog, ncols)
        .expect("the cache stitched this shape; the direct path must too");
    assert_eq!(first.code(), fresh.code(), "cached body bytes != fresh-stitch bytes");
    assert_eq!(hit.code(), fresh.code(), "hit body bytes != fresh-stitch bytes");
    Some(hit)
}

/// [`compile_via_cache`] for projection bodies.
fn proj_compile_via_cache(
    prog: &Program,
    ncols: usize,
    nouts: usize,
) -> Option<StitchedProjection> {
    let cache = sqe_lanestitch::stitch_cache();
    let first = cache.get_or_stitch_project(prog, ncols, nouts)?;
    let hit = cache.get_or_stitch_project(prog, ncols, nouts)?;
    let fresh = StitchedProjection::compile(prog, ncols, nouts)
        .expect("the cache stitched this shape; the direct path must too");
    assert_eq!(first.code(), fresh.code(), "cached body bytes != fresh-stitch bytes");
    assert_eq!(hit.code(), fresh.code(), "hit body bytes != fresh-stitch bytes");
    Some(hit)
}

/// Full parity check of one (program, batch): identical pass bits, or
/// identical (message, sqlstate, erroring-row) after the replay. Also
/// checks the scalar (simd-off) body when the default body took the SIMD
/// tier — the A/B arm is explicit, not environment-driven. The default
/// body arrives through the stitched-body cache's hit path.
fn check_parity(prog: &Program, cols: &[ColData], nrows: u32) -> Option<bool> {
    let jit = compile_via_cache(prog, cols.len())?;
    let want = interp_outcome(prog, cols, nrows);
    let got = stitched_outcome(&jit, prog, cols, nrows);
    match (&want, &got) {
        (Ok(w), Ok(g)) => assert_eq!(w, g, "pass-bit divergence (nrows {nrows})"),
        (Err(we), Err(ge)) => assert_eq!(we, ge, "error identity/position divergence"),
        _ => panic!("one tier errored, the other did not: want_err={} got_err={}",
            want.is_err(), got.is_err()),
    }
    if jit.is_simd() {
        let scalar = StitchedProgram::compile_with(
            prog,
            cols.len(),
            StitchOpts { simd: false, sve2: Sve2Pin::Auto },
        )
        .expect("scalar arm must compile whenever the SIMD arm does");
        assert!(!scalar.is_simd());
        let got_s = stitched_outcome(&scalar, prog, cols, nrows);
        assert_eq!(want, got_s, "scalar-arm divergence (nrows {nrows})");
    }
    Some(want.is_err())
}

// ---- the fuzz gauntlet ---------------------------------------------------

/// Randomized programs x batch geometries x NULL densities, stitched vs
/// oracle: every comparator family (int widths, cross-width, oid unsigned,
/// four float families with NaN/±0/±inf lanes), fused const/var clauses,
/// generic arith clauses (refuse-and-replay), SIMD blocks + scalar tails.
#[test]
fn fuzz_parity_vs_interpreter() {
    let seed = std::env::var("SQE_LANESTITCH_FUZZ_SEED")
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
        let Some(jit) = compile_via_cache(&prog, tys.len()) else {
            assert!(
                !sqe_lanestitch::available(),
                "vocabulary-only program refused to stitch (case {case})"
            );
            return;
        };
        compiles += 1;
        if jit.is_simd() {
            simd_bodies += 1;
        }
        drop(jit);
        let nrows = geometries[r.below(geometries.len() as u64) as usize];
        let null_pct = [0u64, 0, 10, 50, 100][r.below(5) as usize];
        let cols = gen_batch_data(&mut r, tys, nrows as usize, null_pct);
        if check_parity(&prog, &cols, nrows) == Some(true) {
            replays += 1;
        }
    }
    assert!(compiles >= 300, "too few compiled cases: {compiles}");
    assert!(simd_bodies >= 50, "too few SIMD bodies: {simd_bodies}");
    assert!(replays >= 5, "too few refuse-and-replay error cases: {replays}");
}

/// Directed refuse-and-replay: the trap row is planted at a known position
/// C; the replay's error must be C's (message + sqlstate + row), and
/// earlier clauses must shield rows exactly like the oracle.
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

    // Case 1: a=0 rows exist but are clause-1-shielded — no trap, exact bits.
    let n = 200u32;
    let mut values: Vec<Datum> = (0..n).map(|i| Datum::from_i32((i as i32 % 7) - 3)).collect();
    let isnull = vec![false; n as usize];
    {
        let cols = vec![ColData { values: values.clone(), isnull: isnull.clone() }];
        if check_parity(&prog, &cols, n).is_none() {
            assert!(!sqe_lanestitch::available());
            return;
        }
    }

    // Case 2: a * a > 0 with a = i32::MAX planted at row C.
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
        assert!(!sqe_lanestitch::available());
        return;
    };
    let want = interp_outcome(&prog2, &cols, n);
    let got = stitched_outcome(&jit2, &prog2, &cols, n);
    let (wm, ws, wrow) = want.unwrap_err();
    let (gm, gs, grow) = got.unwrap_err();
    assert_eq!((&wm[..], ws, wrow), (&gm[..], gs, grow));
    assert_eq!(wrow as usize, c_row, "error must fire at C's row");
    assert_eq!(wm, "integer out of range");

    // Sticky refusal: a clean batch afterwards still answers Refused.
    let clean: Vec<Datum> = (0..n).map(|i| Datum::from_i32(i as i32 - 100)).collect();
    let cols_clean = vec![ColData { values: clean, isnull: vec![false; n as usize] }];
    let batch = as_batch(&cols_clean, n);
    let mut sel = SelVec::all(n);
    assert_eq!(jit2.run(&batch, &mut sel), QualOutcome::Refused);
}

/// Division-by-zero identity (unshielded): the replay must surface the
/// exact "division by zero", never a stitched approximation.
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
        assert!(!sqe_lanestitch::available());
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

/// MIN / -1 division overflow (the sneaky trap arm).
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
        assert!(!sqe_lanestitch::available());
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
    if !sqe_lanestitch::available() {
        return;
    }
    // NaN const: the fcmp conds are exact only for a non-NaN rhs — refuse.
    let p = cmp_const_prog(CmpOp::Float8Gt, Datum::from_f64(f64::NAN));
    assert!(StitchedProgram::compile(&p, 1).is_none(), "NaN const must refuse");
    // Float var-var OUTSIDE the fused window: the generic register-file
    // Cmp stencil has no NaN-exact cond — refuse.
    let mut p = Program::new();
    p.steps = vec![
        Step::LoadLane { col: 0, out: 0 },
        Step::LoadLane { col: 1, out: 1 },
        Step::Cmp { op: CmpOp::Float8Lt, a: 0, b: 1, out: 2 },
        Step::NullTest { a: 2, out: 3, kind: NullTestKind::IsNotNull },
        Step::Qual { a: 3 },
    ];
    assert!(StitchedProgram::compile(&p, 2).is_none(), "generic-window float cmp must refuse");
    // Volatile programs refuse (typed lowering refusal at the caller —
    // never an interpreter route).
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

/// Per-batch drift: drifted staging (short lanes / missing lanes /
/// oversize batch) declines the batch with sel untouched — the caller
/// replays on its own path; the body stays armed.
#[test]
fn drift_declines_the_batch() {
    if !sqe_lanestitch::available() {
        return;
    }
    let prog = cmp_const_prog(CmpOp::Int4Gt, Datum::from_i32(5));
    let jit = StitchedProgram::compile(&prog, 2).unwrap();
    let n = 100u32;
    let mut r = Lcg(7);
    let cols = gen_batch_data(&mut r, &[ColTy::I32, ColTy::I32], n as usize, 10);
    // Missing lane: batch has 1 lane, body compiled for 2. Sel untouched.
    let batch = Batch { nrows: n, lanes: vec![Lane { values: &cols[0].values, isnull: &cols[0].isnull }] };
    let mut sel = SelVec::all(n);
    assert_eq!(jit.run(&batch, &mut sel), QualOutcome::Drift);
    assert!(sel.is_all(), "drift must leave sel untouched");
    // The caller's replay path (oracle here) evaluates the batch instead.
    let mut sv = SelVec::all(n);
    eval_qual(&prog, &batch, &mut sv).unwrap();
    // A short UNUSED lane is fine — the used lane covers nrows.
    let short = ColData { values: cols[0].values[..50].to_vec(), isnull: cols[0].isnull.clone() };
    let batch = Batch {
        nrows: 50,
        lanes: vec![
            Lane { values: &short.values, isnull: &short.isnull },
            Lane { values: &cols[1].values[..40], isnull: &cols[1].isnull },
        ],
    };
    let mut sel = SelVec::all(50);
    assert_eq!(jit.run(&batch, &mut sel), QualOutcome::Stitched);
    // A too-short USED lane declines (soundness: the body never reads OOB).
    let batch = Batch {
        nrows: 60,
        lanes: vec![
            Lane { values: &cols[0].values[..50], isnull: &cols[0].isnull[..60] },
            Lane { values: &cols[1].values, isnull: &cols[1].isnull },
        ],
    };
    let mut sel = SelVec::all(60);
    assert_eq!(jit.run(&batch, &mut sel), QualOutcome::Drift);
    assert!(sel.is_all());
    // The body stays armed after drift.
    let cols2 = gen_batch_data(&mut r, &[ColTy::I32, ColTy::I32], 30, 0);
    let batch = as_batch(&cols2, 30);
    let mut sel = SelVec::all(30);
    assert_eq!(jit.run(&batch, &mut sel), QualOutcome::Stitched);
}

// ---- the stitch-time budget ------------------------------------------------

/// Stitching must stay µs-class. The hard asserts are generous (200µs
/// median, 2ms worst) to survive CI noise while catching any structural
/// regression (an accidental O(n^2) pass, a syscall storm).
#[test]
fn stitch_time_budget() {
    if !sqe_lanestitch::available() {
        return;
    }
    let mut r = Lcg(0xB0D9E7);
    let tys: &[ColTy] = &[ColTy::I32, ColTy::I64, ColTy::F64, ColTy::F32, ColTy::Oid];
    let vocab_tys: &[ColTy] = &[ColTy::I16, ColTy::I32, ColTy::I64, ColTy::Oid];
    let mut nanos: Vec<u64> = Vec::new();
    for i in 0..64 {
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
        assert!(!sqe_lanestitch::available());
        return;
    };
    assert!(jit.is_simd(), "the all-comparator program must take the NEON tier");
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

// ---- grown vocabulary: NULL/bool tests, wider arith, SAOP -----------------

fn one_col(values: Vec<Datum>, isnull: Vec<bool>) -> Vec<ColData> {
    vec![ColData { values, isnull }]
}

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
    if !sqe_lanestitch::available() {
        return;
    }
    let n = 200u32;
    let values: Vec<Datum> = (0..n)
        .map(|i| match i % 4 {
            0 => Datum::from_i32(0),
            1 => Datum::from_i32(1),
            2 => Datum::from_i32(-7),
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

    // A NULL test riding the bit-iteration tail of a NEON program.
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

/// `(col aop rhs) cmp k2` over one column; rhs const or the column again.
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
    if !sqe_lanestitch::available() {
        return;
    }
    let n = 200u32;
    let c = 137usize;

    // int2 traps: add/sub/mul overflow + MIN/-1 div.
    let p = arith_prog(ArithOp::Add2, CmpOp::Int2Gt, Rhs::Const(Datum::from_i16(1)), Datum::from_i16(0));
    let cols = plant(n, c, Datum::from_i16(i16::MAX), Datum::from_i16(3));
    assert_eq!(check_parity(&p, &cols, n), Some(true));
    let (m, _, row) = stitched_outcome(&StitchedProgram::compile(&p, 1).unwrap(), &p, &cols, n).unwrap_err();
    assert_eq!(m, "smallint out of range");
    assert_eq!(row as usize, c);

    let p = arith_prog(ArithOp::Sub2, CmpOp::Int2Ne, Rhs::Const(Datum::from_i16(1)), Datum::from_i16(0));
    let cols = plant(n, c, Datum::from_i16(i16::MIN), Datum::from_i16(3));
    assert_eq!(check_parity(&p, &cols, n), Some(true));

    let p = arith_prog(ArithOp::Mul2, CmpOp::Int2Gt, Rhs::SelfCol, Datum::from_i16(0));
    let cols = plant(n, c, Datum::from_i16(200), Datum::from_i16(2));
    assert_eq!(check_parity(&p, &cols, n), Some(true));

    let p = arith_prog(ArithOp::Div2, CmpOp::Int2Ne, Rhs::Const(Datum::from_i16(-1)), Datum::from_i16(0));
    let cols = plant(n, c, Datum::from_i16(i16::MIN), Datum::from_i16(6));
    assert_eq!(check_parity(&p, &cols, n), Some(true));

    let p = arith_prog(ArithOp::Div2, CmpOp::Int2Ge, Rhs::Const(Datum::from_i16(0)), Datum::from_i16(0));
    let cols = plant(n, c, Datum::from_i16(5), Datum::from_i16(5));
    let (m, _, _) = stitched_outcome(&StitchedProgram::compile(&p, 1).unwrap(), &p, &cols, n).unwrap_err();
    assert_eq!(m, "division by zero");

    // Clean int2 (no trap): exact bits.
    let p = arith_prog(ArithOp::Add2, CmpOp::Int2Gt, Rhs::Const(Datum::from_i16(10)), Datum::from_i16(0));
    let cols = one_col((0..n).map(|i| Datum::from_i16((i as i16 % 50) - 25)).collect(), vec![false; n as usize]);
    assert_eq!(check_parity(&p, &cols, n), Some(false));

    // int8 traps.
    let p = arith_prog(ArithOp::Add8, CmpOp::Int8Gt, Rhs::Const(Datum::from_i64(1)), Datum::from_i64(0));
    let cols = plant(n, c, Datum::from_i64(i64::MAX), Datum::from_i64(3));
    assert_eq!(check_parity(&p, &cols, n), Some(true));
    let (m, _, row) = stitched_outcome(&StitchedProgram::compile(&p, 1).unwrap(), &p, &cols, n).unwrap_err();
    assert_eq!(m, "bigint out of range");
    assert_eq!(row as usize, c);

    let p = arith_prog(ArithOp::Sub8, CmpOp::Int8Ne, Rhs::Const(Datum::from_i64(1)), Datum::from_i64(0));
    let cols = plant(n, c, Datum::from_i64(i64::MIN), Datum::from_i64(3));
    assert_eq!(check_parity(&p, &cols, n), Some(true));

    let p = arith_prog(ArithOp::Mul8, CmpOp::Int8Ne, Rhs::Const(Datum::from_i64(-1)), Datum::from_i64(0));
    let cols = plant(n, c, Datum::from_i64(i64::MIN), Datum::from_i64(3));
    assert_eq!(check_parity(&p, &cols, n), Some(true));

    let p = arith_prog(ArithOp::Mul8, CmpOp::Int8Gt, Rhs::SelfCol, Datum::from_i64(0));
    let cols = plant(n, c, Datum::from_i64(i64::MIN), Datum::from_i64(2));
    assert_eq!(check_parity(&p, &cols, n), Some(true));

    let p = arith_prog(ArithOp::Div8, CmpOp::Int8Ne, Rhs::Const(Datum::from_i64(-1)), Datum::from_i64(0));
    let cols = plant(n, c, Datum::from_i64(i64::MIN), Datum::from_i64(6));
    assert_eq!(check_parity(&p, &cols, n), Some(true));

    // Clean int8 self-mul near the boundary but non-overflowing.
    let p = arith_prog(ArithOp::Mul8, CmpOp::Int8Ge, Rhs::SelfCol, Datum::from_i64(0));
    let cols = one_col((0..n).map(|i| Datum::from_i64(i as i64 - 100)).collect(), vec![false; n as usize]);
    assert_eq!(check_parity(&p, &cols, n), Some(false));
}

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
    if !sqe_lanestitch::available() {
        return;
    }
    let n = 200u32;
    let values: Vec<Datum> = (0..n).map(|i| Datum::from_i32((i as i32 % 11) - 3)).collect();
    let isnull: Vec<bool> = (0..n).map(|i| i % 9 == 0).collect();
    let cols = one_col(values, isnull);

    // Empty array: ANY is always false (fails every qual), never NULL.
    assert_eq!(check_parity(&saop_prog(CmpOp::Int4Eq, vec![]), &cols, n), Some(false));

    let p = saop_prog(CmpOp::Int4Eq, vec![nd(Datum::from_i32(5))]);
    assert_eq!(check_parity(&p, &cols, n), Some(false));

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

    // Non-equality ANY and int8 / oid families.
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
    // Fail-closed: an array index past the table refuses.
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

/// Date is an int4 carrier, timestamp/tstz are int8 carriers, ±infinity as
/// INT_MIN/MAX sentinels that sort as plain signed ints: the Int4/Int8
/// families already implement the exact ordering — no new stencil.
#[test]
fn date_timestamp_carrier_ordering() {
    if !sqe_lanestitch::available() {
        return;
    }
    let n = 256u32;
    let dpool = [i32::MIN, i32::MAX, 0, 1, -1, 7305, 20000, -730];
    let dates: Vec<Datum> =
        (0..n).map(|i| Datum::from_i32(dpool[i as usize % dpool.len()])).collect();
    let cols = one_col(dates, vec![false; n as usize]);
    for op in [CmpOp::Int4Lt, CmpOp::Int4Ge, CmpOp::Int4Eq, CmpOp::Int4Gt] {
        let p = cmp_const_prog(op, Datum::from_i32(7305));
        assert_eq!(check_parity(&p, &cols, n), Some(false));
    }

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

/// The grown-vocabulary fuzz gauntlet: NULL/bool tests, int2/int8 arith
/// with overflow-dense operands, const-array SAOP.
#[test]
fn fuzz_parity_new_vocab() {
    let seed = std::env::var("SQE_LANESTITCH_FUZZ_SEED")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0x0DDBA11);
    let mut r = Lcg(seed);
    let tys: &[ColTy] = &[ColTy::I16, ColTy::I32, ColTy::I64, ColTy::Oid];
    let geometries: &[u32] = &[1, 7, 63, 64, 65, 128, 200, 1000, MAX_ROWS as u32];
    let mut compiles = 0u32;
    let mut replays = 0u32;
    let mut simd_bodies = 0u32;
    for case in 0..500u32 {
        let prog = gen_new_vocab_program(&mut r);
        let Some(jit) = compile_via_cache(&prog, tys.len()) else {
            assert!(!sqe_lanestitch::available(), "new-vocab program refused (case {case})");
            return;
        };
        compiles += 1;
        if jit.is_simd() {
            simd_bodies += 1;
        }
        let nrows = geometries[r.below(geometries.len() as u64) as usize];
        let null_pct = [0u64, 0, 10, 50, 100][r.below(5) as usize];
        let cols = gen_batch_data(&mut r, tys, nrows as usize, null_pct);
        if check_parity(&prog, &cols, nrows) == Some(true) {
            replays += 1;
        }
    }
    assert!(compiles >= 400, "too few compiles: {compiles}");
    assert!(replays >= 3, "too few refuse-and-replay cases: {replays}");
    // NullTest/BoolTest/SAOP clauses ride the NEON vector pass: arith-free
    // draws (a majority) must classify SIMD.
    assert!(simd_bodies >= 100, "too few SIMD bodies in the grown vocabulary: {simd_bodies}");
}

/// One random clause from the grown vocabulary over a fixed 4-column
/// layout [i16, i32, i64, oid]; registers clause-local.
fn gen_new_vocab_program(r: &mut Lcg) -> Program {
    let mut p = Program::new();
    let nclauses = 1 + r.below(3) as usize;
    for _ in 0..nclauses {
        match r.below(6) {
            0 => {
                let col = r.below(4) as u16;
                let kind = if r.chance(50) { NullTestKind::IsNull } else { NullTestKind::IsNotNull };
                p.steps.extend([
                    Step::LoadLane { col, out: 0 },
                    Step::NullTest { a: 0, out: 1, kind },
                    Step::Qual { a: 1 },
                ]);
            }
            1 => {
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
                // SAOP int4 IN-list on col1, sometimes with a NULL element;
                // half the elements draw from the u16 domain so the SVE2
                // MATCH stencil admits on SVE2 hardware.
                let nelem = r.below(6) as usize;
                let mut elems: Vec<NullableDatum> = (0..nelem)
                    .map(|_| {
                        if r.chance(15) {
                            NullableDatum::null()
                        } else if r.chance(50) {
                            nd(Datum::from_i32(r.below(0x1_0000) as i32))
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
                // SAOP oid IN-list on col3 (u16-domain-biased like arm 4).
                let nelem = 1 + r.below(5) as usize;
                let elems: Vec<NullableDatum> = (0..nelem)
                    .map(|_| {
                        if r.chance(50) {
                            nd(canon_oid(r.below(0x1_0000) as u32))
                        } else {
                            nd(gen_value(r, ColTy::Oid))
                        }
                    })
                    .collect();
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

// ---- float var-var + promoted SIMD shapes ---------------------------------

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

/// Adversarial float pools: multiple NaN payloads (both signs), signed
/// zeros, infinities, denormals, and sign edges.
fn f64_edge(i: usize) -> Datum {
    let pool = [
        f64::NAN,
        f64::from_bits(0x7FF8_0000_0000_0001),
        f64::from_bits(0xFFF8_0000_0000_0000),
        f64::from_bits(0x7FF0_0000_0000_0001),
        f64::NEG_INFINITY,
        f64::INFINITY,
        -0.0,
        0.0,
        f64::MIN_POSITIVE,
        -f64::MIN_POSITIVE,
        5e-324,
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
        f32::from_bits(0x7FC0_0001),
        f32::from_bits(0xFFC0_0000),
        f32::from_bits(0x7F80_0001),
        f32::NEG_INFINITY,
        f32::INFINITY,
        -0.0,
        0.0,
        f32::MIN_POSITIVE,
        1e-45,
        1.0,
        -1.5,
        f32::MAX,
        f32::MIN,
    ];
    Datum::from_f32(pool[i % pool.len()])
}

/// Float var-var over every family and relation, NaN-payload-dense lanes,
/// SIMD blocks and scalar tails: exact NaN-ordering parity on both tiers.
#[test]
fn float_var_var_nan_ordering() {
    if !sqe_lanestitch::available() {
        return;
    }
    let mut r = Lcg(0xF10A7);
    for &(a_ty, b_ty, ops) in FLOAT_FAMS {
        for &op in ops {
            let prog = fcmp_var_prog(op);
            let jit = StitchedProgram::compile(&prog, 2)
                .expect("fused float var-var must compile");
            assert!(jit.is_simd(), "float var-var must take the NEON tier ({op:?})");
            for &nrows in &[63u32, 64, 65, 130, 257] {
                for &null_pct in &[0u64, 20] {
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
/// NullTest / BoolTest / SAOP programs classify SIMD, and their parity
/// holds on block-boundary geometries.
#[test]
fn simd_engagement_of_promoted_shapes() {
    if !sqe_lanestitch::available() {
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
        assert!(jit.is_simd(), "{name} must take the NEON tier");
        for &nrows in &[1u32, 63, 64, 65, 129, 1000] {
            for &np in &[0u64, 25, 100] {
                let cols = gen_batch_data(&mut r, &[ColTy::I32], nrows as usize, np);
                let want = interp_outcome(prog, &cols, nrows).unwrap();
                let got = stitched_outcome(&jit, prog, &cols, nrows).unwrap();
                assert_eq!(want, got, "{name} nrows={nrows} null%={np}");
            }
        }
    }

    // An arith clause refuses the tier for the whole program (the
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

/// A kitchen-sink mixed program: every vector-pass shape in one qual —
/// one NEON pass builds the whole word; parity across geometries.
#[test]
fn simd_all_shapes_one_program() {
    let mut prog = Program::new();
    let k = prog.push_const(nd(Datum::from_i32(-100)));
    let kf = prog.push_const(nd(Datum::from_f64(0.25)));
    let arr = prog.push_array(
        [1i32, 3, 5, 700, -2].iter().map(|&v| nd(Datum::from_i32(v))).collect(),
    );
    prog.steps = vec![
        Step::LoadLane { col: 0, out: 0 },
        Step::LoadConst { k, out: 1 },
        Step::Cmp { op: CmpOp::Int4Ge, a: 0, b: 1, out: 2 },
        Step::Qual { a: 2 },
        Step::LoadLane { col: 1, out: 0 },
        Step::LoadConst { k: kf, out: 1 },
        Step::Cmp { op: CmpOp::Float8Le, a: 0, b: 1, out: 2 },
        Step::Qual { a: 2 },
        Step::LoadLane { col: 1, out: 0 },
        Step::LoadLane { col: 2, out: 1 },
        Step::Cmp { op: CmpOp::Float84Gt, a: 0, b: 1, out: 2 },
        Step::Qual { a: 2 },
        Step::LoadLane { col: 3, out: 0 },
        Step::NullTest { a: 0, out: 1, kind: NullTestKind::IsNotNull },
        Step::Qual { a: 1 },
        Step::LoadLane { col: 0, out: 0 },
        Step::BoolTest { a: 0, out: 1, kind: BoolTestKind::IsNotFalse },
        Step::Qual { a: 1 },
        Step::LoadLane { col: 0, out: 0 },
        Step::SaopAny { a: 0, out: 1, op: CmpOp::Int4Ne, arr },
        Step::Qual { a: 1 },
    ];
    let tys = &[ColTy::I32, ColTy::F64, ColTy::F32, ColTy::I64];
    let Some(jit) = StitchedProgram::compile(&prog, 4) else {
        assert!(!sqe_lanestitch::available());
        return;
    };
    assert!(jit.is_simd(), "all-vector program must take the NEON tier");
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

        let mut want = OutBufs::new(nouts, nrows as usize);
        let interp_res = {
            let mut lanes = want.lanes();
            eval_project(&prog, &batch, &sel, &mut lanes)
        };

        let Some(body) = proj_compile_via_cache(&prog, ncols, nouts) else {
            continue; // off-arch
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
                // Trap parity: the body refused exactly where the oracle
                // errored. Sticky: the next run refuses too.
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
                "round {round}: outcome {outcome:?} vs oracle {:?} — must agree on trap-or-not",
                res.as_ref().map(|_| ()).map_err(|e| e.message.clone())
            ),
        }
    }
    if sqe_lanestitch::available() {
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
    // bits entirely.
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
    sel.clear(0);
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
    // And the oracle replay raises the exact message.
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
fn proj_drift_declines() {
    let mut prog = Program::new();
    prog.steps.push(Step::LoadLane { col: 0, out: 0 });
    prog.steps.push(Step::StoreOut { a: 0, out: 0 });
    let Some(body) = StitchedProjection::compile(&prog, 1, 1) else { return };
    let cols = vec![ColData { values: vec![Datum::from_i32(1); 4], isnull: vec![false; 4] }];
    let batch = as_batch(&cols, 4);
    let sel = SelVec::all(4);
    // Short output lane: Drift, outputs untouched.
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

// ---- SVE2 stencil tier ----------------------------------------------------
//
// On non-SVE2 hardware (Apple Silicon dev boxes) these exercise the NEON
// tier and the parity assertions still hold; SVE bodies execute only on
// SVE2 hardware (Graviton CI cluster nodes) — the CI cluster leg of the oracle.

/// SVE2 MATCH IN-lists: Eq SAOP clauses whose non-NULL elements sit in the
/// u16 domain, over lanes mixing element hits, near-misses, out-of-domain
/// low-16 collisions, sign-extended negatives, and NULLs. Multi-clause
/// programs cover candidate-register allocation beside the NEON const bank.
#[test]
fn sve2_match_saop_parity() {
    if !sqe_lanestitch::available() {
        return;
    }
    let mut r = Lcg(0x5CE2_0001);
    let tys: &[ColTy] = &[ColTy::I16, ColTy::I32, ColTy::I64, ColTy::Oid];
    // (lane column, Eq op, element ceiling): int2 lanes cap elements at
    // i16::MAX (canonical-datum contract).
    let fams: &[(u16, CmpOp, u64)] = &[
        (0, CmpOp::Int2Eq, 0x8000),
        (1, CmpOp::Int4Eq, 0x1_0000),
        (2, CmpOp::Int8Eq, 0x1_0000),
        (3, CmpOp::OidEq, 0x1_0000),
        (2, CmpOp::Int84Eq, 0x1_0000),
        (1, CmpOp::Int48Eq, 0x1_0000),
    ];
    let geometries: &[u32] = &[63, 64, 65, 128, 191, 1000, MAX_ROWS as u32];
    let mut match_bodies = 0u32;
    for case in 0..240u32 {
        let (col, op, ceil) = fams[case as usize % fams.len()];
        let k = [1usize, 2, 4, 7, 8, 9, 16, 24, 48][r.below(9) as usize];
        let mut prog = Program::new();
        let mut elem_vals: Vec<u64> = (0..k).map(|_| r.below(ceil)).collect();
        let mut elems: Vec<NullableDatum> =
            elem_vals.iter().map(|&v| nd(Datum::from_i64(v as i64))).collect();
        if r.chance(25) {
            elems.push(NullableDatum::null());
        }
        let arr = prog.push_array(elems);
        prog.steps.extend([
            Step::LoadLane { col, out: 0 },
            Step::SaopAny { a: 0, out: 1, op, arr },
            Step::Qual { a: 1 },
        ]);
        if r.chance(40) {
            // A fused const clause: MATCH candidate registers must coexist
            // with the NEON const bank.
            let kk = prog.push_const(nd(Datum::from_i32(r.below(200) as i32 - 100)));
            prog.steps.extend([
                Step::LoadLane { col: 1, out: 0 },
                Step::LoadConst { k: kk, out: 1 },
                Step::Cmp { op: CmpOp::Int4Le, a: 0, b: 1, out: 2 },
                Step::Qual { a: 2 },
            ]);
        }
        if r.chance(30) {
            // A second MATCH-eligible clause: multi-clause register budget.
            let k2 = 1 + r.below(16) as usize;
            let elems2: Vec<NullableDatum> =
                (0..k2).map(|_| nd(Datum::from_i64(r.below(0x1_0000) as i64))).collect();
            let arr2 = prog.push_array(elems2);
            prog.steps.extend([
                Step::LoadLane { col: 1, out: 0 },
                Step::SaopAny { a: 0, out: 1, op: CmpOp::Int4Eq, arr: arr2 },
                Step::Qual { a: 1 },
            ]);
        }
        let Some(jit) = StitchedProgram::compile(&prog, tys.len()) else {
            assert!(!sqe_lanestitch::available(), "MATCH-shape program refused (case {case})");
            return;
        };
        assert!(jit.is_simd(), "SAOP program must classify SIMD (case {case})");
        if jit.sve_match_clauses() > 0 {
            match_bodies += 1;
        }
        let nrows = geometries[r.below(geometries.len() as u64) as usize];
        if elem_vals.is_empty() {
            elem_vals.push(1);
        }
        let mut cols = gen_batch_data(&mut r, tys, nrows as usize, 10);
        for row in 0..nrows as usize {
            let e = elem_vals[r.below(elem_vals.len() as u64) as usize];
            let v: i64 = match r.below(6) {
                0 => e as i64,                 // exact hit
                1 => e as i64 ^ 1,             // near miss
                2 => e as i64 | 0x1_0000,      // low16 collision, out of domain
                3 => -(e as i64) - 1,          // negative (sign-extended)
                4 => e as i64 | (1i64 << 47),  // high-bit garbage collision
                _ => r.below(0x1_0000) as i64, // random in-domain
            };
            // Canonical datum image per lane family (the spec.rs contract).
            let d = match col {
                0 => Datum::from_i16(v as i16),
                1 => Datum::from_i32(v as i32),
                3 => canon_oid(v as u32),
                _ => Datum::from_i64(v),
            };
            cols[col as usize].values[row] = d;
        }
        let want = interp_outcome(&prog, &cols, nrows);
        let got = stitched_outcome(&jit, &prog, &cols, nrows);
        assert_eq!(want, got, "case {case} nrows {nrows} k {k} op {op:?}");
    }
    // Engagement pin: on SVE2 hardware every one of these programs is
    // MATCH-eligible.
    assert!(
        !sqe_lanestitch::sve2_active() || match_bodies >= 200,
        "too few MATCH bodies on SVE2 hardware: {match_bodies}"
    );
}

/// The adaptive SVE COMPACT survivor path: per-block survivor counts
/// engineered to straddle the crossover, ANDed with a Generic clause that
/// owns the per-survivor iteration. Also runs the Force pin (crossover 0)
/// so the SVE extraction path is parity-covered at every selectivity.
#[test]
fn sve2_survivor_extraction_parity() {
    if !sqe_lanestitch::available() {
        return;
    }
    let mut r = Lcg(0x5CE2_0002);
    let tys: &[ColTy] = &[ColTy::I32, ColTy::I32];
    for &(lo_pct, hi_pct) in &[(0u64, 0u64), (5, 60), (10, 15), (50, 50), (100, 100), (2, 98)] {
        let mut prog = Program::new();
        let k = prog.push_const(nd(Datum::from_i32(0)));
        prog.steps.extend([
            Step::LoadLane { col: 0, out: 0 },
            Step::LoadConst { k, out: 1 },
            Step::Cmp { op: CmpOp::Int4Gt, a: 0, b: 1, out: 2 },
            Step::Qual { a: 2 },
        ]);
        // Generic clause (no fused window matches this shape): pure and
        // non-erroring, so it rides the per-survivor section.
        prog.steps.extend([
            Step::LoadLane { col: 1, out: 0 },
            Step::NullTest { a: 0, out: 1, kind: NullTestKind::IsNotNull },
            Step::BoolTest { a: 1, out: 2, kind: BoolTestKind::IsTrue },
            Step::Qual { a: 2 },
        ]);
        let Some(jit) = StitchedProgram::compile(&prog, tys.len()) else {
            assert!(!sqe_lanestitch::available());
            return;
        };
        assert!(jit.is_simd());
        assert_eq!(
            jit.has_sve_survivor_path(),
            sqe_lanestitch::sve2_active(),
            "survivor-path presence must track the active tier"
        );
        let forced = StitchedProgram::compile_with(
            &prog,
            tys.len(),
            StitchOpts { simd: true, sve2: Sve2Pin::Force },
        )
        .unwrap();
        for &nrows in &[64u32, 65, 127, 128, 191, 640, 1000, MAX_ROWS as u32] {
            let mut cols = gen_batch_data(&mut r, tys, nrows as usize, 0);
            for row in 0..nrows as usize {
                // Alternate survivor density per 64-row block so
                // consecutive blocks take different adaptive arms.
                let pct = if (row / 64) % 2 == 0 { lo_pct } else { hi_pct };
                cols[0].values[row] =
                    Datum::from_i32(if r.chance(pct) { 1 + r.below(100) as i32 } else { -1 });
                cols[0].isnull[row] = r.chance(5);
                cols[1].values[row] = Datum::from_i32(r.below(2) as i32);
                cols[1].isnull[row] = r.chance(20);
            }
            let want = interp_outcome(&prog, &cols, nrows);
            let got = stitched_outcome(&jit, &prog, &cols, nrows);
            assert_eq!(want, got, "lo {lo_pct} hi {hi_pct} nrows {nrows}");
            let got_f = stitched_outcome(&forced, &prog, &cols, nrows);
            assert_eq!(want, got_f, "forced arm: lo {lo_pct} hi {hi_pct} nrows {nrows}");
        }
    }
}

/// MATCH admission edges stay fail-closed to the NEON stencil: non-Eq
/// relations, out-of-domain elements, and register-budget overflow still
/// stitch (SIMD) and stay parity-exact with zero MATCH clauses.
#[test]
fn sve2_match_admission_edges() {
    if !sqe_lanestitch::available() {
        return;
    }
    let tys: &[ColTy] = &[ColTy::I16, ColTy::I32, ColTy::I64, ColTy::Oid];
    let mk = |op: CmpOp, vals: &[i64]| -> Program {
        let mut p = Program::new();
        let arr = p.push_array(vals.iter().map(|&v| nd(Datum::from_i64(v))).collect());
        p.steps.extend([
            Step::LoadLane { col: 1, out: 0 },
            Step::SaopAny { a: 0, out: 1, op, arr },
            Step::Qual { a: 1 },
        ]);
        p
    };
    // (program, MATCH-eligible on SVE2 hardware?)
    let cases: &[(Program, bool)] = &[
        (mk(CmpOp::Int4Eq, &[1, 2, 65535]), true),
        (mk(CmpOp::Int4Ne, &[1, 2, 3]), false),
        (mk(CmpOp::Int4Lt, &[7]), false),
        (mk(CmpOp::Int4Eq, &[1, 65536]), false),
        (mk(CmpOp::Int4Eq, &[-1, 3]), false),
        (mk(CmpOp::Int4Eq, &(0..80i64).collect::<Vec<_>>()), false),
    ];
    let mut r = Lcg(0x5CE2_0003);
    for (i, (prog, eligible)) in cases.iter().enumerate() {
        let Some(jit) = StitchedProgram::compile(prog, tys.len()) else {
            assert!(!sqe_lanestitch::available());
            return;
        };
        assert!(jit.is_simd(), "case {i}");
        assert_eq!(
            jit.sve_match_clauses() > 0,
            *eligible && sqe_lanestitch::sve2_active(),
            "case {i}: MATCH admission drifted"
        );
        let cols = gen_batch_data(&mut r, tys, 500, 15);
        let want = interp_outcome(prog, &cols, 500);
        let got = stitched_outcome(&jit, prog, &cols, 500);
        assert_eq!(want, got, "case {i}");
    }
}
