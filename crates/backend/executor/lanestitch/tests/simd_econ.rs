// Admission economics for the widened SIMD tier: each vector-pass shape
// timed against its own scalar row-loop body (the PGRUST_LANESTITCH_SIMD
// kill switch pins the tier off per compile) over 1024-row batches. The
// numbers gate admission: a shape whose SIMD form loses to its scalar form
// must not be classified into the vector pass (refuse-admission economics).
//
// Ignored by default (it is a measurement, not an assertion — run it with
// `cargo test -p lanestitch --release -- --ignored --nocapture` on the
// machine you are deciding caps for). The one hard assertion is the
// engagement sanity: the A body is SIMD, the B body is not.

use datum::{Datum, NullableDatum};
use lanestitch::{
    BoolTestKind, CmpOp, Lane, NullTestKind, Program, SelVec, Step, StitchedProgram,
};

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0
    }
}

fn nd(d: Datum) -> NullableDatum {
    NullableDatum { value: d, isnull: false }
}

struct Col {
    values: Vec<Datum>,
    isnull: Vec<bool>,
}

fn int_col(r: &mut Lcg, n: usize) -> Col {
    Col {
        values: (0..n).map(|_| Datum::from_i32((r.next() as i32) % 1000)).collect(),
        isnull: (0..n).map(|_| r.next() % 100 < 5).collect(),
    }
}

fn f64_col(r: &mut Lcg, n: usize) -> Col {
    Col {
        values: (0..n)
            .map(|_| {
                if r.next() % 100 < 3 {
                    Datum::from_f64(f64::NAN)
                } else {
                    Datum::from_f64(((r.next() as i32) % 1000) as f64 / 8.0)
                }
            })
            .collect(),
        isnull: (0..n).map(|_| r.next() % 100 < 5).collect(),
    }
}

/// Best-of-5 medianish timing of one body over the batch; returns ns/row.
fn time_body(jit: &StitchedProgram, prog: &Program, cols: &[Col], nrows: u32) -> f64 {
    let lanes: Vec<Lane<'_>> = cols
        .iter()
        .map(|c| Lane { values: &c.values, isnull: &c.isnull })
        .collect();
    // Warm up.
    for _ in 0..200 {
        let mut sel = SelVec::all(nrows);
        jit.run_lanes(prog, nrows, &lanes, &mut sel).unwrap();
    }
    let iters = 2000u32;
    let mut best = f64::INFINITY;
    for _ in 0..5 {
        let t0 = std::time::Instant::now();
        for _ in 0..iters {
            let mut sel = SelVec::all(nrows);
            jit.run_lanes(prog, nrows, &lanes, &mut sel).unwrap();
        }
        let ns = t0.elapsed().as_nanos() as f64 / (iters as u64 * nrows as u64) as f64;
        best = best.min(ns);
    }
    best
}

fn compare(name: &str, prog: &Program, cols: &[Col]) {
    let ncols = cols.len();
    let nrows = cols[0].values.len() as u32;
    // A: SIMD tier on (default); B: pinned to the scalar row loop.
    std::env::remove_var("PGRUST_LANESTITCH_SIMD");
    let a = StitchedProgram::compile(prog, ncols).expect("A body must compile");
    std::env::set_var("PGRUST_LANESTITCH_SIMD", "0");
    let b = StitchedProgram::compile(prog, ncols).expect("B body must compile");
    std::env::remove_var("PGRUST_LANESTITCH_SIMD");
    assert!(a.is_simd(), "{name}: A body must be SIMD");
    assert!(!b.is_simd(), "{name}: B body must be scalar");
    let simd = time_body(&a, prog, cols, nrows);
    let scalar = time_body(&b, prog, cols, nrows);
    println!(
        "{name:<28} simd {simd:>7.3} ns/row   scalar {scalar:>7.3} ns/row   scalar/simd {:>5.2}x",
        scalar / simd
    );
}

#[test]
#[ignore = "admission-economics measurement; run explicitly with --ignored --nocapture --release"]
fn simd_shape_economics() {
    if !lanestitch::available() {
        return;
    }
    let n = 1024usize;
    let mut r = Lcg(0xECC0);

    // NullTest.
    let mut p = Program::new();
    p.steps = vec![
        Step::LoadLane { col: 0, out: 0 },
        Step::NullTest { a: 0, out: 1, kind: NullTestKind::IsNotNull },
        Step::Qual { a: 1 },
    ];
    compare("nulltest(is not null)", &p, &[int_col(&mut r, n)]);

    // BoolTest.
    let mut p = Program::new();
    p.steps = vec![
        Step::LoadLane { col: 0, out: 0 },
        Step::BoolTest { a: 0, out: 1, kind: BoolTestKind::IsNotFalse },
        Step::Qual { a: 1 },
    ];
    compare("booltest(is not false)", &p, &[int_col(&mut r, n)]);

    // SAOP across IN-list sizes.
    for k in [1usize, 4, 16, 64, 128] {
        let mut p = Program::new();
        let arr = p.push_array((0..k).map(|v| nd(Datum::from_i32(v as i32 * 7))).collect());
        p.steps = vec![
            Step::LoadLane { col: 0, out: 0 },
            Step::SaopAny { a: 0, out: 1, op: CmpOp::Int4Eq, arr },
            Step::Qual { a: 1 },
        ];
        compare(&format!("saop int4 eq k={k}"), &p, &[int_col(&mut r, n)]);
    }

    // Float var-var (f64 x f64 and the promoting f64 x f32 mix).
    let mut p = Program::new();
    p.steps = vec![
        Step::LoadLane { col: 0, out: 0 },
        Step::LoadLane { col: 1, out: 1 },
        Step::Cmp { op: CmpOp::Float8Lt, a: 0, b: 1, out: 2 },
        Step::Qual { a: 2 },
    ];
    compare("fcmpvar float8 lt", &p, &[f64_col(&mut r, n), f64_col(&mut r, n)]);

    let mut p = Program::new();
    p.steps = vec![
        Step::LoadLane { col: 0, out: 0 },
        Step::LoadLane { col: 1, out: 1 },
        Step::Cmp { op: CmpOp::Float8Eq, a: 0, b: 1, out: 2 },
        Step::Qual { a: 2 },
    ];
    compare("fcmpvar float8 eq", &p, &[f64_col(&mut r, n), f64_col(&mut r, n)]);

    // Founding shape as the reference point.
    let mut p = Program::new();
    let k = p.push_const(nd(Datum::from_i32(500)));
    p.steps = vec![
        Step::LoadLane { col: 0, out: 0 },
        Step::LoadConst { k, out: 1 },
        Step::Cmp { op: CmpOp::Int4Lt, a: 0, b: 1, out: 2 },
        Step::Qual { a: 2 },
    ];
    compare("cmpconst int4 (reference)", &p, &[int_col(&mut r, n)]);
}
