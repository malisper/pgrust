//! [sqe-generic-c] Plan-independent machinery for the P4-5 generic-node
//! tail: the WITH RECURSIVE fixpoint loop (RecursiveUnion +
//! WorkTableScan) and the deterministic generate_series row source
//! (FunctionScan / ProjectSet).
//!
//! Doctrine (production-plan R1/R2): no interpretation of arbitrary
//! expressions — the row program here is a CLOSED vocabulary (column
//! echo, plan-Const echo, the same int add/sub/mul classes the P4-1
//! fold-input rung admitted), recognized at the seam and refused typed
//! everywhere else. Semantics mirror the C executor exactly:
//!
//! - fixpoint (nodeRecursiveunion.c): run the non-recursive term into
//!   the working table, then iterate the recursive term with the
//!   worktable bound to the PREVIOUS iteration's rows until an
//!   iteration emits nothing. UNION ALL appends; UNION dedups every
//!   candidate against EVERYTHING emitted so far (seed included), with
//!   grouping equality — NULLs compare EQUAL (the set-ops law).
//! - budget: PG runs unbounded; the engine's answers are materialized,
//!   so the fixpoint carries a witnessed row budget. A breach is NOT a
//!   refusal (mid-execution refusals are outside the R1 error law): it
//!   is a resource ERROR, SQLSTATE 53400 — the memory-law precedent —
//!   raised at execution cadence. Raising the budget is the remedy.
//! - pull ([sqe-rec-pull], nodeLimit.c over nodeRecursiveunion.c): a
//!   Limit directly over the CteScan pulls `offset + count` surviving
//!   rows in emission order and stops asking — C never runs the
//!   recursive term past the row that satisfied the pull. The fixpoint
//!   carries that bound (`RecPull`) and stops row-lazily at it, so an
//!   unbounded recursion under LIMIT answers exactly as C instead of
//!   meeting the budget. A Sort between Limit and CteScan needs the
//!   whole fixpoint: no pull there.
//! - generate_series (adt int/series.rs law): step 0 raises "step size
//!   cannot equal zero" (22023); a strict NULL argument yields the
//!   empty set; a next-value overflow of the result width ENDS the
//!   series after the emission just made (pg_add_sNN_overflow law).
//! - program arithmetic raises C's exact per-width overflow errors
//!   (smallint/integer/bigint out of range, 22003).

use types_error::{PgError, PgResult};

/// One worktable/CTE cell: NULL, a word (int2/int4/int8/date/timestamp/
/// timestamptz raw datum value), or varlena payload BYTES (no header —
/// the AnswerSet `ColData::Bytes` currency).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RecCell {
    Null,
    I(i64),
    B(Vec<u8>),
}

pub(crate) type RecRow = Vec<RecCell>;

/// A program/qual value source: a worktable (CTE) column or a column of
/// the materialized base-side rows (join steps only).
#[derive(Clone, Copy, Debug)]
pub(crate) enum RecSrc {
    Wt(usize),
    Base(usize),
}

/// An arithmetic operand: a source column or a plan constant.
#[derive(Clone, Copy, Debug)]
pub(crate) enum RecArg {
    Src(RecSrc),
    K(i64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RecArithCls {
    Add,
    Sub,
    Mul,
}

/// One output-program slot: column echo, plan-Const echo, or one closed
/// arithmetic node whose PG result width `w` (2/4/8) is the overflow
/// obligation.
#[derive(Clone, Debug)]
pub(crate) enum RecExpr {
    Src(RecSrc),
    Cell(RecCell),
    Arith { l: RecArg, cls: RecArithCls, r: RecArg, w: u8 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RecCmp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// One recognized word qual `col CMP const` (3VL: a NULL cell fails).
#[derive(Clone, Debug)]
pub(crate) struct RecQual {
    pub col: usize,
    pub code: RecCmp,
    pub rhs: i64,
}

pub(crate) fn eval_qual(q: &RecQual, row: &[RecCell]) -> bool {
    let RecCell::I(v) = row[q.col] else { return false };
    match q.code {
        RecCmp::Eq => v == q.rhs,
        RecCmp::Ne => v != q.rhs,
        RecCmp::Lt => v < q.rhs,
        RecCmp::Le => v <= q.rhs,
        RecCmp::Gt => v > q.rhs,
        RecCmp::Ge => v >= q.rhs,
    }
}

fn width_range(w: u8) -> (i64, i64, &'static str) {
    match w {
        2 => (i16::MIN as i64, i16::MAX as i64, "smallint out of range"),
        4 => (i32::MIN as i64, i32::MAX as i64, "integer out of range"),
        _ => (i64::MIN, i64::MAX, "bigint out of range"),
    }
}

fn overflow(msg: &'static str) -> Box<PgError> {
    Box::new(
        PgError::error(msg.to_string())
            .with_sqlstate(::types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
    )
}

fn src_cell<'a>(
    s: RecSrc,
    wt: &'a [RecCell],
    base: Option<&'a [RecCell]>,
) -> &'a RecCell {
    match s {
        RecSrc::Wt(i) => &wt[i],
        RecSrc::Base(i) => &base.expect("base row bound for Base src (lowering bug)")[i],
    }
}

/// Evaluate one program slot over a (worktable row, optional base row)
/// pair. Arithmetic is strict (NULL in -> NULL out) and raises C's
/// exact overflow errors for the op's PG result width.
pub(crate) fn eval_expr(
    e: &RecExpr,
    wt: &[RecCell],
    base: Option<&[RecCell]>,
) -> PgResult<RecCell> {
    Ok(match e {
        RecExpr::Src(s) => src_cell(*s, wt, base).clone(),
        RecExpr::Cell(c) => c.clone(),
        RecExpr::Arith { l, cls, r, w } => {
            let read = |a: &RecArg| -> Option<i64> {
                match a {
                    RecArg::K(k) => Some(*k),
                    RecArg::Src(s) => match src_cell(*s, wt, base) {
                        RecCell::I(v) => Some(*v),
                        RecCell::Null => None,
                        RecCell::B(_) => unreachable!("arith over bytes (lowering bug)"),
                    },
                }
            };
            let (Some(a), Some(b)) = (read(l), read(r)) else {
                return Ok(RecCell::Null);
            };
            let (lo, hi, msg) = width_range(*w);
            let v = match cls {
                RecArithCls::Add => a.checked_add(b),
                RecArithCls::Sub => a.checked_sub(b),
                RecArithCls::Mul => a.checked_mul(b),
            }
            .ok_or_else(|| overflow(msg))?;
            if v < lo || v > hi {
                return Err(overflow(msg));
            }
            RecCell::I(v)
        }
    })
}

/// Dedup key of one row over `cols` — GROUPING equality: NULLs compare
/// equal (every NULL serializes to the same tag), bytes byte-wise.
pub(crate) fn row_key(row: &[RecCell], cols: &[usize]) -> Vec<u8> {
    let mut k: Vec<u8> = Vec::with_capacity(cols.len() * 9);
    for &c in cols {
        match &row[c] {
            RecCell::Null => k.push(0),
            RecCell::I(v) => {
                k.push(1);
                k.extend_from_slice(&v.to_le_bytes());
            }
            RecCell::B(b) => {
                k.push(2);
                k.extend_from_slice(&(b.len() as u32).to_le_bytes());
                k.extend_from_slice(b);
            }
        }
    }
    k
}

/// The fixpoint budget error (53400 — the memory-law cadence; never an
/// OOM, never a refusal, never a wrong answer).
pub(crate) fn fixpoint_budget_error(cap: u64) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "sqe: recursive union exceeded the fixpoint row budget ({cap} rows)"
        ))
        .with_sqlstate(::types_error::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED)
        .with_detail(
            "The sqe engine materializes the WITH RECURSIVE fixpoint under a witnessed \
             row budget; an unbounded (or larger-than-budget) recursion fails loudly \
             instead of exhausting memory.",
        )
        .with_hint("Bound the recursion (a WHERE guard on the recursive term) or reduce its row count."),
    )
}

/// The ProjectSet expansion budget error (same 53400 law).
pub(crate) fn srf_budget_error(cap: u64) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "sqe: set-returning projection exceeded the answer row budget ({cap} rows)"
        ))
        .with_sqlstate(::types_error::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED)
        .with_detail(
            "The sqe engine materializes SRF fan-out at the answer boundary under a \
             witnessed row budget.",
        )
        .with_hint("Reduce the series length or the driving row count."),
    )
}

/// The fixpoint output: emitted rows plus the honest per-node row
/// accounting for EXPLAIN ANALYZE (WW-3).
#[derive(Debug)]
pub(crate) struct FixpointOut {
    pub rows: Vec<RecRow>,
    /// Total worktable rows the recursive term consumed (every
    /// iteration's input — the WorkTableScan row honesty).
    pub wt_rows: u64,
    /// Total rows the recursive term emitted BEFORE dedup (the
    /// recursive-term subtree's row honesty).
    pub step_rows: u64,
}

/// [sqe-rec-pull] The pull bound: nodeLimit's exact law over a CteScan
/// whose delivered order IS the fixpoint emission order (no upper Sort).
/// PG's Limit pulls `offset + count` CteScan rows and then stops asking;
/// nodeRecursiveunion never runs past the row that satisfied the pull,
/// so an unbounded recursion under LIMIT answers in C. `keep` is the
/// outer CteScan qual (only surviving rows count toward `need`); the
/// fixpoint stops — row-lazily, mid-iteration — the moment `need`
/// surviving rows exist. Without a bound the loop runs to the fixpoint.
pub(crate) struct RecPull<'a> {
    pub need: u64,
    pub keep: &'a dyn Fn(&RecRow) -> bool,
}

/// nodeRecursiveunion.c's exact loop at the answer boundary.
/// `step(working, sink)` runs the recursive term as a row SOURCE with
/// the worktable bound to `working`, pushing each produced row into
/// `sink` in C's evaluation order; `Ok(false)` from the sink means
/// "enough — stop producing" (the pull bound), which the step honors
/// before evaluating another row (a row PG would never have pulled is
/// never evaluated — its overflow/error never raised, exactly as C).
/// `dup = Some(cols)` is the UNION dedup law (seed included); `cap` is
/// the fixpoint row budget over EMITTED rows; `pull` is the Limit pull
/// bound (None = run to the fixpoint).
pub(crate) fn run_fixpoint(
    seed: Vec<RecRow>,
    dup: Option<&[usize]>,
    cap: u64,
    pull: Option<RecPull<'_>>,
    step: &mut dyn FnMut(&[RecRow], &mut dyn FnMut(RecRow) -> PgResult<bool>) -> PgResult<()>,
) -> PgResult<FixpointOut> {
    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let admit = |seen: &mut std::collections::HashSet<Vec<u8>>, row: &RecRow| -> bool {
        match dup {
            None => true,
            Some(cols) => seen.insert(row_key(row, cols)),
        }
    };
    let mut rows: Vec<RecRow> = Vec::new();
    let mut working: Vec<RecRow> = Vec::new();
    let mut wt_rows = 0u64;
    let mut step_rows = 0u64;
    // Surviving (kept) rows delivered so far against the pull bound.
    let mut delivered = 0u64;
    let satisfied = |delivered: u64| pull.as_ref().is_some_and(|p| delivered >= p.need);
    // The non-recursive term: C emits it first, row by row; a pull
    // satisfied inside it never starts the recursive term.
    for r in seed {
        if satisfied(delivered) {
            break;
        }
        if admit(&mut seen, &r) {
            if rows.len() as u64 + 1 > cap {
                return Err(fixpoint_budget_error(cap));
            }
            if let Some(p) = pull.as_ref() {
                if (p.keep)(&r) {
                    delivered += 1;
                }
            }
            rows.push(r.clone());
            working.push(r);
        }
    }
    while !working.is_empty() && !satisfied(delivered) {
        wt_rows += working.len() as u64;
        let mut next: Vec<RecRow> = Vec::new();
        {
            let base = rows.len() as u64;
            let mut sink = |r: RecRow| -> PgResult<bool> {
                step_rows += 1;
                if !admit(&mut seen, &r) {
                    return Ok(true);
                }
                if base + next.len() as u64 + 1 > cap {
                    return Err(fixpoint_budget_error(cap));
                }
                if let Some(p) = pull.as_ref() {
                    if (p.keep)(&r) {
                        delivered += 1;
                    }
                }
                next.push(r);
                Ok(!satisfied(delivered))
            };
            step(&working, &mut sink)?;
        }
        rows.extend(next.iter().cloned());
        working = next;
    }
    Ok(FixpointOut { rows, wt_rows, step_rows })
}

// ---------------------------------------------------------------------------
// generate_series (int4/int8) — the deterministic SRF vocabulary
// ---------------------------------------------------------------------------

/// A recognized generate_series(start, finish [, step]) call with
/// plan-Const arguments. `int8` = the int8 variants (result width);
/// `null_arg` = any strict argument was NULL (empty set).
#[derive(Clone, Copy, Debug)]
pub(crate) struct SeriesSpec {
    pub start: i64,
    pub finish: i64,
    pub step: i64,
    pub int8: bool,
    pub null_arg: bool,
}

pub(crate) fn zero_step_error() -> Box<PgError> {
    Box::new(
        PgError::error("step size cannot equal zero".to_string())
            .with_sqlstate(::types_error::ERRCODE_INVALID_PARAMETER_VALUE),
    )
}

/// Exact row count of the series (0 for NULL args or an empty range).
/// `None` = step 0 (the error case — callers keep C's execution-time
/// error cadence).
pub(crate) fn series_len(s: &SeriesSpec) -> Option<u64> {
    if s.step == 0 {
        return None;
    }
    if s.null_arg {
        return Some(0);
    }
    let (start, finish, step) = (s.start as i128, s.finish as i128, s.step as i128);
    let n = if step > 0 { (finish - start) / step + 1 } else { (start - finish) / (-step) + 1 };
    Some(n.clamp(0, u64::MAX as i128) as u64)
}

/// Materialize the series values (the adt series.rs law: overflow of the
/// next value ends the series; the width clamp is inherent — Const args
/// are already in-width and stepping stops before leaving it).
pub(crate) fn series_values(s: &SeriesSpec) -> PgResult<Vec<i64>> {
    if s.step == 0 {
        return Err(zero_step_error());
    }
    if s.null_arg {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    let mut cur = s.start;
    loop {
        if (s.step > 0 && cur > s.finish) || (s.step < 0 && cur < s.finish) {
            break;
        }
        out.push(cur);
        match cur.checked_add(s.step) {
            Some(n) => cur = n,
            None => break,
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn series_laws() {
        let s = |start, finish, step| SeriesSpec { start, finish, step, int8: false, null_arg: false };
        assert_eq!(series_values(&s(1, 5, 1)).unwrap(), vec![1, 2, 3, 4, 5]);
        assert_eq!(series_values(&s(5, 1, -2)).unwrap(), vec![5, 3, 1]);
        assert_eq!(series_values(&s(3, 1, 1)).unwrap(), Vec::<i64>::new());
        assert!(series_values(&s(1, 5, 0)).is_err());
        let mut null = s(1, 5, 1);
        null.null_arg = true;
        assert_eq!(series_values(&null).unwrap(), Vec::<i64>::new());
        assert_eq!(series_len(&s(1, 5, 2)), Some(3));
        assert_eq!(series_len(&s(1, 5, 0)), None);
        assert_eq!(series_len(&s(5, 1, 1)), Some(0));
        let full = SeriesSpec { start: i64::MIN, finish: i64::MAX, step: 1, int8: true, null_arg: false };
        assert_eq!(series_len(&full), Some(u64::MAX));
        let full_desc = SeriesSpec { start: i64::MAX, finish: i64::MIN, step: -1, int8: true, null_arg: false };
        assert_eq!(series_len(&full_desc), Some(u64::MAX));
    }

    /// Adapt a whole-iteration step (rows in, rows out) to the sink
    /// protocol: rows push in order; a refusing sink stops the push.
    fn batch(
        f: impl Fn(&[RecRow]) -> Vec<RecRow>,
    ) -> impl FnMut(&[RecRow], &mut dyn FnMut(RecRow) -> PgResult<bool>) -> PgResult<()> {
        move |w: &[RecRow], sink: &mut dyn FnMut(RecRow) -> PgResult<bool>| {
            for r in f(w) {
                if !sink(r)? {
                    break;
                }
            }
            Ok(())
        }
    }

    #[test]
    fn fixpoint_union_all_and_distinct() {
        // 1..4 counter: seed [1], step n+1 while n < 4.
        let seed = vec![vec![RecCell::I(1)]];
        let mut step = batch(|w: &[RecRow]| {
            w.iter()
                .filter(|r| matches!(r[0], RecCell::I(v) if v < 4))
                .map(|r| {
                    let RecCell::I(v) = r[0] else { unreachable!() };
                    vec![RecCell::I(v + 1)]
                })
                .collect()
        });
        let out = run_fixpoint(seed.clone(), None, 1000, None, &mut step).unwrap();
        assert_eq!(
            out.rows,
            vec![
                vec![RecCell::I(1)],
                vec![RecCell::I(2)],
                vec![RecCell::I(3)],
                vec![RecCell::I(4)]
            ]
        );
        // A cycle under DISTINCT terminates (dedup closes it).
        let mut cyc = batch(|w: &[RecRow]| {
            w.iter()
                .map(|r| {
                    let RecCell::I(v) = r[0] else { unreachable!() };
                    vec![RecCell::I((v % 3) + 1)]
                })
                .collect()
        });
        let dup = [0usize];
        let out = run_fixpoint(seed.clone(), Some(&dup), 1000, None, &mut cyc).unwrap();
        assert_eq!(out.rows.len(), 3);
        // The same cycle under ALL hits the budget error (53400 law).
        let err = run_fixpoint(seed, None, 100, None, &mut cyc).unwrap_err();
        assert_eq!(err.sqlstate(), ::types_error::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED);
    }

    #[test]
    fn dedup_nulls_equal() {
        let seed = vec![vec![RecCell::Null], vec![RecCell::Null], vec![RecCell::I(1)]];
        let mut step = batch(|_: &[RecRow]| Vec::new());
        let dup = [0usize];
        let out = run_fixpoint(seed, Some(&dup), 1000, None, &mut step).unwrap();
        assert_eq!(out.rows.len(), 2, "NULLs compare equal under the set-ops law");
    }

    /// [sqe-rec-pull] nodeLimit's pull law: an UNBOUNDED recursion under
    /// LIMIT answers exactly PG's first `offset + count` rows in emission
    /// order — the seed first, then iteration by iteration, stopping
    /// mid-iteration the moment the bound is met — never the 53400
    /// budget; the pull counts only rows the outer CteScan qual keeps;
    /// a seed that alone satisfies the pull never runs the recursive
    /// term; and without a pull the same recursion still hits the budget.
    #[test]
    fn fixpoint_pull_bound_is_limit_law() {
        let i = |v: i64| vec![RecCell::I(v)];
        // with:s39's shape: a 3-row seed re-emitted forever (UNION ALL).
        let seed = vec![i(10), i(20), i(30)];
        let echo = |w: &[RecRow]| w.to_vec();
        let keep_all = |_: &RecRow| true;
        let mut step = batch(echo);
        let out = run_fixpoint(
            seed.clone(),
            None,
            1000,
            Some(RecPull { need: 8, keep: &keep_all }),
            &mut step,
        )
        .unwrap();
        assert_eq!(
            out.rows,
            vec![i(10), i(20), i(30), i(10), i(20), i(30), i(10), i(20)],
            "seed, then iterations in order, cut mid-iteration at the bound"
        );
        assert_eq!(out.wt_rows, 6, "two iterations bound the 3-row worktable");
        assert_eq!(out.step_rows, 5, "the second iteration stopped after its 2nd row");
        // A pull the seed alone satisfies never runs the recursive term.
        let mut step = batch(echo);
        let out = run_fixpoint(
            seed.clone(),
            None,
            1000,
            Some(RecPull { need: 2, keep: &keep_all }),
            &mut step,
        )
        .unwrap();
        assert_eq!(out.rows, vec![i(10), i(20)]);
        assert_eq!((out.wt_rows, out.step_rows), (0, 0));
        // Only CteScan-kept rows count toward the bound (the qual is
        // evaluated at the CteScan above, after the fixpoint).
        let keep_20 = |r: &RecRow| matches!(r[0], RecCell::I(20));
        let mut step = batch(echo);
        let out = run_fixpoint(
            seed.clone(),
            None,
            1000,
            Some(RecPull { need: 3, keep: &keep_20 }),
            &mut step,
        )
        .unwrap();
        assert_eq!(out.rows.len(), 8, "3 kept rows arrive with the 8th emitted row");
        assert_eq!(out.rows.iter().filter(|r| keep_20(r)).count(), 3);
        // The bound is a pull, not a budget: the same recursion with no
        // pull (or a pull beyond the budget) still raises 53400.
        let mut step = batch(echo);
        let err = run_fixpoint(seed.clone(), None, 100, None, &mut step).unwrap_err();
        assert_eq!(err.sqlstate(), ::types_error::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED);
        let mut step = batch(echo);
        let err = run_fixpoint(
            seed,
            None,
            100,
            Some(RecPull { need: 101, keep: &keep_all }),
            &mut step,
        )
        .unwrap_err();
        assert_eq!(err.sqlstate(), ::types_error::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED);
        // A row PG never pulls is never evaluated: the step errors on
        // its 3rd row, the pull is met at the 2nd — no error surfaces.
        let mut trap = |w: &[RecRow],
                        sink: &mut dyn FnMut(RecRow) -> PgResult<bool>|
         -> PgResult<()> {
            for (k, r) in w.iter().enumerate() {
                if k == 2 {
                    return Err(overflow("integer out of range"));
                }
                if !sink(r.clone())? {
                    break;
                }
            }
            Ok(())
        };
        let out = run_fixpoint(
            vec![i(1), i(2), i(3)],
            None,
            1000,
            Some(RecPull { need: 5, keep: &keep_all }),
            &mut trap,
        )
        .unwrap();
        assert_eq!(out.rows, vec![i(1), i(2), i(3), i(1), i(2)]);
    }

    #[test]
    fn arith_overflow_widths() {
        let e = RecExpr::Arith {
            l: RecArg::Src(RecSrc::Wt(0)),
            cls: RecArithCls::Add,
            r: RecArg::K(1),
            w: 4,
        };
        let ok = eval_expr(&e, &[RecCell::I(41)], None).unwrap();
        assert_eq!(ok, RecCell::I(42));
        let err = eval_expr(&e, &[RecCell::I(i32::MAX as i64)], None).unwrap_err();
        assert!(err.message().contains("integer out of range"));
        let null = eval_expr(&e, &[RecCell::Null], None).unwrap();
        assert_eq!(null, RecCell::Null);
    }
}
