// SVE2-tier A/B microbench: the sve2 spike's K1/K3 kernels re-measured as
// PRODUCTION stitched bodies (notes/sve2-spike-2026-07-14.md "Repro" said
// rerun the spike on the production stencils). Run on a Graviton fleet node
// twice and diff:
//
//   PGRUST_LANESTITCH_SVE2=off   taskset -c <core> sve_ab   # NEON tier
//   PGRUST_LANESTITCH_SVE2=force taskset -c <core> sve_ab   # SVE2, no gate
//   (unset)                      taskset -c <core> sve_ab   # SVE2 adaptive
//
// Output CSV: kernel,param,tier,ns_per_row (best-of-5, 1024-row batches).

use datum::{Datum, NullableDatum};
use lanestitch::{
    Batch, BoolTestKind, CmpOp, Lane, NullTestKind, Program, SelVec, Step, StitchedProgram,
    MAX_ROWS,
};

fn nd(d: Datum) -> NullableDatum {
    NullableDatum { value: d, isnull: false }
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

fn bench(name: &str, param: &str, prog: &Program, cols: &[(Vec<Datum>, Vec<bool>)]) {
    let jit = match StitchedProgram::compile(prog, cols.len()) {
        Some(j) => j,
        None => {
            println!("{name},{param},refused,NaN");
            return;
        }
    };
    let tier = if !jit.is_simd() {
        "scalar"
    } else if jit.has_sve_survivor_path() || jit.sve_match_clauses() > 0 {
        "sve2"
    } else {
        "neon"
    };
    let batch = Batch {
        nrows: MAX_ROWS as u32,
        lanes: cols.iter().map(|(v, n)| Lane { values: v, isnull: n }).collect(),
    };
    // Autoscale to ~80ms, best-of-5.
    let mut sel = SelVec::all(MAX_ROWS as u32);
    let t0 = std::time::Instant::now();
    jit.run(prog, &batch, &mut sel).unwrap();
    let one = t0.elapsed().as_secs_f64().max(1e-7);
    let passes = ((0.08 / one) as usize).clamp(1, 200_000);
    let mut best = f64::INFINITY;
    for _ in 0..5 {
        let t = std::time::Instant::now();
        let mut acc = 0u64;
        for _ in 0..passes {
            let mut sel = SelVec::all(MAX_ROWS as u32);
            jit.run(prog, &batch, &mut sel).unwrap();
            acc = acc.wrapping_add(sel.count() as u64);
        }
        std::hint::black_box(acc);
        best = best.min(t.elapsed().as_nanos() as f64 / (passes * MAX_ROWS) as f64);
    }
    println!("{name},{param},{tier},{best:.3}");
}

/// K1 production shape: CmpConst vector clause at a given selectivity plus
/// one Generic (unfused NullTest->BoolTest) clause — the per-survivor
/// section is where the NEON bit-iteration and the SVE COMPACT dense list
/// diverge.
fn k1(sel_pct: u64) {
    let mut prog = Program::new();
    let k = prog.push_const(nd(Datum::from_i32(0)));
    prog.steps.extend([
        Step::LoadLane { col: 0, out: 0 },
        Step::LoadConst { k, out: 1 },
        Step::Cmp { op: CmpOp::Int4Gt, a: 0, b: 1, out: 2 },
        Step::Qual { a: 2 },
    ]);
    prog.steps.extend([
        Step::LoadLane { col: 1, out: 0 },
        Step::NullTest { a: 0, out: 1, kind: NullTestKind::IsNotNull },
        Step::BoolTest { a: 1, out: 2, kind: BoolTestKind::IsTrue },
        Step::Qual { a: 2 },
    ]);
    let mut r = Rng(0x1234_5678_9abc_def1);
    let v0: Vec<Datum> = (0..MAX_ROWS)
        .map(|_| Datum::from_i32(if r.next() % 100 < sel_pct { 1 } else { -1 }))
        .collect();
    let v1: Vec<Datum> = (0..MAX_ROWS).map(|_| Datum::from_i32(1)).collect();
    let nn = vec![false; MAX_ROWS];
    bench("k1_survivors", &sel_pct.to_string(), &prog, &[(v0, nn.clone()), (v1, nn)]);
}

/// K3 production shape: an Eq IN-list over u16-domain values (dict-code /
/// small-int lanes) — SVE2 MATCH vs the NEON per-candidate CMEQ+ORR.
fn k3(k: usize) {
    let mut r = Rng(0x0bad_cafe_0000_0001);
    let mut prog = Program::new();
    let elems: Vec<NullableDatum> =
        (0..k).map(|_| nd(Datum::from_i64((r.next() % 4096) as i64))).collect();
    let arr = prog.push_array(elems);
    prog.steps.extend([
        Step::LoadLane { col: 0, out: 0 },
        Step::SaopAny { a: 0, out: 1, op: CmpOp::Int4Eq, arr },
        Step::Qual { a: 1 },
    ]);
    let v0: Vec<Datum> =
        (0..MAX_ROWS).map(|_| Datum::from_i32((r.next() % 4096) as i32)).collect();
    let nn = vec![false; MAX_ROWS];
    bench("k3_saop", &k.to_string(), &prog, &[(v0, nn)]);
}

fn main() {
    println!("kernel,param,tier,ns_per_row");
    for sel in [1u64, 10, 20, 30, 50, 90] {
        k1(sel);
    }
    for k in [4usize, 8, 16, 24, 48] {
        k3(k);
    }
}
