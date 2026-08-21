// Tier A <-> Step-IR oracle parity: the sqe engine's Level-1 PredTerm
// vocabulary evaluated against its canonical Step-program image on the
// oracle interpreter — the step-ir.md §8 F1 injection ("one semantics
// source", R2). Executes NO emitted bodies, so this leg of the parity
// oracle runs on every host including macOS.

use datum::{Datum, NullableDatum};
use sqe::ir::{CmpOp as L1Cmp, PredTerm};
use sqe::typmeta::TypMeta;
use sqe_lanestitch::{eval_qual, Batch, CmpOp, Lane, Program, SelVec, Step};

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

fn push_cmp_clause(p: &mut Program, op: CmpOp, konst: i64) {
    let k = p.push_const(NullableDatum { value: Datum::from_i64(konst), isnull: false });
    p.steps.extend([
        Step::LoadLane { col: 0, out: 0 },
        Step::LoadConst { k, out: 1 },
        Step::Cmp { op, a: 0, b: 1, out: 2 },
        Step::Qual { a: 2 },
    ]);
}

/// The canonical Step-program image of one int PredTerm (F1): Eq/Ne are
/// one strict-compare clause; Between is the Ge+Le clause pair; In2 is a
/// two-element SaopAny. Identical 3VL by the single-fold law: a NULL
/// operand fails every form.
fn prog_of(term: &PredTerm) -> Program {
    let mut p = Program::new();
    match term.op {
        L1Cmp::Eq => push_cmp_clause(&mut p, CmpOp::Int8Eq, term.lo),
        L1Cmp::Ne => push_cmp_clause(&mut p, CmpOp::Int8Ne, term.lo),
        L1Cmp::Between => {
            push_cmp_clause(&mut p, CmpOp::Int8Ge, term.lo);
            push_cmp_clause(&mut p, CmpOp::Int8Le, term.hi);
        }
        L1Cmp::In2 => {
            let arr = p.push_array(vec![
                NullableDatum { value: Datum::from_i64(term.lo), isnull: false },
                NullableDatum { value: Datum::from_i64(term.hi), isnull: false },
            ]);
            p.steps.extend([
                Step::LoadLane { col: 0, out: 0 },
                Step::SaopAny { a: 0, out: 1, op: CmpOp::Int8Eq, arr },
                Step::Qual { a: 1 },
            ]);
        }
    }
    p
}

const EDGES: &[i64] = &[i64::MIN, i64::MIN + 1, -100, -1, 0, 1, 7, 100, i64::MAX - 1, i64::MAX];

#[test]
fn predterm_eval_matches_step_program_on_oracle() {
    let mut r = Lcg(0xF1_1A7E);
    for case in 0..2000u32 {
        let (lo, hi) = {
            let a = if r.chance(50) {
                EDGES[r.below(EDGES.len() as u64) as usize]
            } else {
                (r.next() as i64) % 200
            };
            let b = if r.chance(50) {
                EDGES[r.below(EDGES.len() as u64) as usize]
            } else {
                (r.next() as i64) % 200
            };
            (a.min(b), a.max(b))
        };
        let op = [L1Cmp::Eq, L1Cmp::Ne, L1Cmp::Between, L1Cmp::In2][r.below(4) as usize];
        let term = PredTerm::new(0, op, lo, hi, TypMeta::INT8);
        let prog = prog_of(&term);

        let nrows = 1 + r.below(300) as u32;
        let mut values = Vec::with_capacity(nrows as usize);
        let mut isnull = Vec::with_capacity(nrows as usize);
        for _ in 0..nrows {
            let v = match r.below(4) {
                0 => lo,
                1 => hi,
                2 => EDGES[r.below(EDGES.len() as u64) as usize],
                _ => (r.next() as i64) % 200,
            };
            values.push(Datum::from_i64(v));
            isnull.push(r.chance(20));
        }

        // Tier A leg: the Level-1 single-fold law (eval_v).
        let want: Vec<bool> = (0..nrows as usize)
            .map(|i| term.eval_v(values[i].as_i64(), !isnull[i]))
            .collect();

        // Step-IR leg: the same predicate as a program on the oracle.
        let batch = Batch { nrows, lanes: vec![Lane { values: &values, isnull: &isnull }] };
        let mut sel = SelVec::all(nrows);
        eval_qual(&prog, &batch, &mut sel).expect("pure predicate programs never error");
        let got: Vec<bool> = (0..nrows).map(|i| sel.contains(i)).collect();

        assert_eq!(want, got, "case {case}: op {op:?} lo {lo} hi {hi}");
    }
}
