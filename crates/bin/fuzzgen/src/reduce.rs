//! Statement-stream delta debugging (ddmin, McKeeman shape) over a stream
//! that may contain transaction brackets. Candidate subsets stay
//! bracket-balanced by construction: the stream is pre-partitioned into
//! atomic units — a BEGIN..COMMIT/ROLLBACK bracket is one unit, every other
//! statement its own — and ddmin drops whole units or none. Setup
//! dependency stays handled by construction — a candidate that breaks later
//! statements simply fails to reproduce and is rejected; unit-internal
//! outcomes don't matter, only the target's classification does.
//!
//! Two target shapes: a statement finding (minimize the prefix before a
//! failing statement while it keeps reproducing the divergence class) and a
//! state-probe finding (minimize the whole stream while the probe on the
//! recorded table keeps firing STATE_DIFF — "which probe fails" is part of
//! the divergence signature, so the repro is statement subset + probe).

use crate::diff::DiffClass;
use crate::ruled::RuledEntry;
use crate::runner::{apply_and_classify, classify_probe, probe_sql, Executor};

/// A fresh, identically-initialized (reference, candidate) executor pair.
pub type ExecPair = (Box<dyn Executor>, Box<dyn Executor>);

/// Produces a fresh, identically-initialized server pair per reduction
/// probe. Real implementations reconnect and re-run schema setup; tests
/// hand back fakes.
pub trait ExecPairFactory {
    fn fresh(&mut self) -> Result<ExecPair, String>;
}

/// What the reduction must keep reproducing.
#[derive(Clone, Copy, Debug)]
pub enum ReduceTarget<'a> {
    /// The statement at `fail_idx` classifies as `target_key` after the
    /// kept prefix (candidates are the statements before it).
    Stmt { fail_idx: usize, target_key: &'a str },
    /// A state probe on `table` (ORDER BY `pk`) fires STATE_DIFF after the
    /// kept stream. Candidates are the statements before the probe that
    /// fired (`upto`, exclusive) — which probe first fails is part of the
    /// divergence signature, and later statements may well re-converge the
    /// state (overwrite/delete the diverged rows), so the whole stream is
    /// NOT a reproducer.
    Probe { table: &'a str, pk: &'a str, upto: usize },
}

pub struct Reduction {
    /// Minimal statement list; the failing statement (or the probe SELECT)
    /// comes last.
    pub stmts: Vec<String>,
    /// Reduction probes executed (fresh server pairs consumed).
    pub probes: u32,
}

fn is_begin(sql: &str) -> bool {
    let head = sql.trim_start();
    head.len() >= 5 && head[..5].eq_ignore_ascii_case("BEGIN")
}

/// COMMIT or ROLLBACK — but not ROLLBACK TO SAVEPOINT, which stays inside
/// its bracket.
fn is_txn_end(sql: &str) -> bool {
    let head = sql.trim_start();
    let upper: String = head.chars().take(12).collect::<String>().to_ascii_uppercase();
    upper.starts_with("COMMIT") || (upper.starts_with("ROLLBACK") && !upper.starts_with("ROLLBACK TO"))
}

/// Partition indices [0, upto) into atomic, order-preserving units:
/// BEGIN..COMMIT/ROLLBACK spans (including a trailing unterminated span,
/// when `upto` cuts into a bracket — the failing statement's transaction
/// context drops or stays as a whole) and single statements.
fn bracket_units(stmts: &[String], upto: usize) -> Vec<Vec<usize>> {
    let mut units = Vec::new();
    let mut i = 0;
    while i < upto {
        if is_begin(&stmts[i]) {
            let mut unit = vec![i];
            i += 1;
            while i < upto {
                unit.push(i);
                let end = is_txn_end(&stmts[i]);
                i += 1;
                if end {
                    break;
                }
            }
            units.push(unit);
        } else {
            units.push(vec![i]);
            i += 1;
        }
    }
    units
}

struct Ctx<'a> {
    factory: &'a mut dyn ExecPairFactory,
    stmts: &'a [String],
    target: ReduceTarget<'a>,
    table: &'a [RuledEntry],
    ulp_tol: u64,
    probes: u32,
}

impl Ctx<'_> {
    /// Does the kept unit subset still reproduce the target?
    fn test(&mut self, units: &[Vec<usize>]) -> Result<bool, String> {
        self.probes += 1;
        let (mut a, mut b) = self.factory.fresh()?;
        for unit in units {
            for &i in unit {
                // Kept-subset outcomes are irrelevant — identical errors on
                // both sides from a dropped dependency are fine.
                let _ = apply_and_classify(
                    a.as_mut(),
                    b.as_mut(),
                    &self.stmts[i],
                    &[],
                    self.table,
                    self.ulp_tol,
                );
            }
        }
        // Reduction replays without generator metadata, so ruled-soft
        // float-aggregate columns compare hard here. That is conservative:
        // ruled records are never findings, so no reduction targets them,
        // and a hard-diff replay can only keep reproducing the target
        // class.
        match self.target {
            ReduceTarget::Stmt { fail_idx, target_key } => {
                let c = apply_and_classify(
                    a.as_mut(),
                    b.as_mut(),
                    &self.stmts[fail_idx],
                    &[],
                    self.table,
                    self.ulp_tol,
                );
                Ok(c.class.key() == target_key)
            }
            ReduceTarget::Probe { table, pk, .. } => {
                let sql = probe_sql(table, pk);
                let oa = a.apply(&sql);
                let ob = b.apply(&sql);
                let c = classify_probe(table, &oa, &ob, self.table, self.ulp_tol);
                Ok(c.class == DiffClass::StateDiff(table.to_string()))
            }
        }
    }
}

/// ddmin over the candidate units. Errors only on factory failure or when
/// the full stream does not reproduce (a flaky target is not reducible).
pub fn reduce_stream(
    factory: &mut dyn ExecPairFactory,
    stmts: &[String],
    target: ReduceTarget,
    table: &[RuledEntry],
    ulp_tol: u64,
) -> Result<Reduction, String> {
    let upto = match target {
        ReduceTarget::Stmt { fail_idx, .. } => {
            assert!(fail_idx < stmts.len());
            fail_idx
        }
        ReduceTarget::Probe { upto, .. } => {
            assert!(upto <= stmts.len());
            upto
        }
    };
    let mut ctx = Ctx { factory, stmts, target, table, ulp_tol, probes: 0 };

    let full = bracket_units(stmts, upto);
    if !ctx.test(&full)? {
        let what = match target {
            ReduceTarget::Stmt { target_key, .. } => target_key.to_string(),
            ReduceTarget::Probe { table, .. } => format!("STATE_DIFF on {table}"),
        };
        return Err(format!("target class {what} does not reproduce on the full stream"));
    }
    // Cheap common case: a self-contained target.
    if !full.is_empty() && ctx.test(&[])? {
        return Ok(finish(&ctx, &[], stmts, target));
    }

    let mut cand = full;
    let mut n = 2usize;
    while cand.len() >= 2 {
        let chunk_len = cand.len().div_ceil(n);
        let chunks: Vec<Vec<Vec<usize>>> =
            cand.chunks(chunk_len).map(|c| c.to_vec()).collect();
        let mut reduced = false;

        for chunk in &chunks {
            if ctx.test(chunk)? {
                cand = chunk.clone();
                n = 2;
                reduced = true;
                break;
            }
        }
        if !reduced && n > 2 {
            for skip in 0..chunks.len() {
                let complement: Vec<Vec<usize>> = chunks
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| *i != skip)
                    .flat_map(|(_, c)| c.iter().cloned())
                    .collect();
                if ctx.test(&complement)? {
                    cand = complement;
                    n = (n - 1).max(2);
                    reduced = true;
                    break;
                }
            }
        }
        if !reduced {
            if n >= cand.len() {
                break;
            }
            n = (n * 2).min(cand.len());
        }
    }
    // Final 1-minimality pass: drop single units while that still
    // reproduces (covers the cand.len()==1 tail too).
    let mut i = 0;
    while i < cand.len() {
        let mut trial = cand.clone();
        trial.remove(i);
        if ctx.test(&trial)? {
            cand = trial;
        } else {
            i += 1;
        }
    }

    Ok(finish(&ctx, &cand, stmts, target))
}

fn finish(ctx: &Ctx, units: &[Vec<usize>], stmts: &[String], target: ReduceTarget) -> Reduction {
    let mut out: Vec<String> = units
        .iter()
        .flat_map(|u| u.iter().map(|&i| stmts[i].clone()))
        .collect();
    match target {
        ReduceTarget::Stmt { fail_idx, .. } => out.push(stmts[fail_idx].clone()),
        ReduceTarget::Probe { table, pk, .. } => out.push(probe_sql(table, pk)),
    }
    Reduction { stmts: out, probes: ctx.probes }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diff::StmtOutcome;
    use crate::ruled::default_table;

    /// In-memory "server": a single integer register driven by a toy
    /// statement language (SET n / ADD n / NOP / GET / BAD) plus real
    /// transaction semantics (BEGIN snapshots, ROLLBACK restores, COMMIT
    /// keeps, one savepoint level). `skew` plants the divergence: GET (and
    /// the state probe SELECT) reports value+1 when the register equals
    /// the skew trigger.
    struct FakeServer {
        value: i64,
        txn_saved: Option<i64>,
        sp_saved: Option<i64>,
        skew_at: Option<i64>,
    }

    impl FakeServer {
        fn read(&self) -> i64 {
            if self.skew_at == Some(self.value) {
                self.value + 1
            } else {
                self.value
            }
        }
    }

    fn cmd(tag: &str) -> StmtOutcome {
        StmtOutcome::Command { tag: tag.to_string(), affected: None }
    }

    impl Executor for FakeServer {
        fn apply(&mut self, sql: &str) -> StmtOutcome {
            let sql = sql.trim_end_matches(';');
            if sql.starts_with("SELECT * FROM ") {
                // The state probe.
                return StmtOutcome::Rows {
                    col_oids: vec![23],
                    rows: vec![vec![Some(self.read().to_string())]],
                };
            }
            let mut parts = sql.split_whitespace();
            match parts.next() {
                Some("SET") => {
                    self.value = parts.next().unwrap().parse().unwrap();
                    cmd("SET")
                }
                Some("ADD") => {
                    self.value += parts.next().unwrap().parse::<i64>().unwrap();
                    cmd("ADD")
                }
                Some("NOP") => cmd("NOP"),
                Some("BEGIN") => {
                    self.txn_saved = Some(self.value);
                    cmd("BEGIN")
                }
                Some("COMMIT") => {
                    self.txn_saved = None;
                    self.sp_saved = None;
                    cmd("COMMIT")
                }
                Some("SAVEPOINT") => {
                    self.sp_saved = Some(self.value);
                    cmd("SAVEPOINT")
                }
                Some("RELEASE") => {
                    self.sp_saved = None;
                    cmd("RELEASE")
                }
                Some("ROLLBACK") => {
                    if parts.next() == Some("TO") {
                        if let Some(v) = self.sp_saved {
                            self.value = v;
                        }
                        cmd("ROLLBACK")
                    } else {
                        if let Some(v) = self.txn_saved.take() {
                            self.value = v;
                        }
                        self.sp_saved = None;
                        cmd("ROLLBACK")
                    }
                }
                Some("GET") => StmtOutcome::Rows {
                    col_oids: vec![23],
                    rows: vec![vec![Some(self.read().to_string())]],
                },
                _ => StmtOutcome::Error {
                    sqlstate: "42601".to_string(),
                    message: "syntax error".to_string(),
                },
            }
        }
    }

    struct FakeFactory {
        skew_at: i64,
    }

    impl ExecPairFactory for FakeFactory {
        fn fresh(&mut self) -> Result<ExecPair, String> {
            Ok((
                Box::new(FakeServer {
                    value: 0,
                    txn_saved: None,
                    sp_saved: None,
                    skew_at: None,
                }),
                Box::new(FakeServer {
                    value: 0,
                    txn_saved: None,
                    sp_saved: None,
                    skew_at: Some(self.skew_at),
                }),
            ))
        }
    }

    fn s(xs: &[&str]) -> Vec<String> {
        xs.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn reduces_to_dependency_closure() {
        // GET diverges only when the register is exactly 7 = 1 + 2 + 4;
        // the NOPs and the ADD 100/SET 0-reset noise must be dropped, the
        // three contributing statements kept.
        let stmts = s(&[
            "NOP", "SET 1", "NOP", "ADD 2", "ADD 100", "SET 1", "ADD 2", "NOP", "ADD 4", "GET",
        ]);
        let mut factory = FakeFactory { skew_at: 7 };
        let r = reduce_stream(
            &mut factory,
            &stmts,
            ReduceTarget::Stmt { fail_idx: 9, target_key: "ROWSET_DIFF" },
            &default_table(),
            4,
        )
        .unwrap();
        assert_eq!(*r.stmts.last().unwrap(), "GET");
        // Minimal: some SET 1 + ADD 2 + ADD 4 + GET (4 statements).
        assert_eq!(r.stmts.len(), 4, "not minimal: {:?}", r.stmts);
        assert!(r.stmts.contains(&"ADD 4".to_string()));
        assert!(r.probes > 0);
        // 1-minimality: removing any kept prefix statement stops reproduction.
        for drop in 0..r.stmts.len() - 1 {
            let mut trial: Vec<String> = r.stmts.clone();
            trial.remove(drop);
            let fail = trial.len() - 1;
            let err = reduce_stream(
                &mut factory,
                &trial,
                ReduceTarget::Stmt { fail_idx: fail, target_key: "ROWSET_DIFF" },
                &default_table(),
                4,
            );
            assert!(err.is_err(), "dropping {:?} still reproduced", r.stmts[drop]);
        }
    }

    #[test]
    fn self_contained_failure_reduces_to_one_statement() {
        // Skew at 0: GET diverges with the register untouched, so the NOP
        // prefix is entirely droppable.
        let stmts = s(&["NOP", "NOP", "NOP", "GET"]);
        let mut factory = FakeFactory { skew_at: 0 };
        let r = reduce_stream(
            &mut factory,
            &stmts,
            ReduceTarget::Stmt { fail_idx: 3, target_key: "ROWSET_DIFF" },
            &default_table(),
            4,
        )
        .unwrap();
        assert_eq!(r.stmts, vec!["GET".to_string()]);
        // The empty-prefix fast path: exactly two probes (full + empty).
        assert_eq!(r.probes, 2);
    }

    #[test]
    fn non_reproducing_target_errors() {
        let stmts = s(&["NOP", "GET"]);
        let mut factory = FakeFactory { skew_at: 999 };
        let err = reduce_stream(
            &mut factory,
            &stmts,
            ReduceTarget::Stmt { fail_idx: 1, target_key: "ROWSET_DIFF" },
            &default_table(),
            4,
        );
        assert!(err.is_err());
    }

    #[test]
    fn wrong_target_class_does_not_reproduce() {
        let stmts = s(&["GET"]);
        let mut factory = FakeFactory { skew_at: 0 };
        let err = reduce_stream(
            &mut factory,
            &stmts,
            ReduceTarget::Stmt { fail_idx: 0, target_key: "ERROR_DIFF" },
            &default_table(),
            4,
        );
        assert!(err.is_err());
    }

    #[test]
    fn bracket_units_partition_brackets_atomically() {
        let stmts = s(&[
            "NOP;",
            "BEGIN;",
            "ADD 1;",
            "ROLLBACK TO SAVEPOINT sp0;",
            "COMMIT;",
            "SET 2;",
            "BEGIN;",
            "ADD 3;",
        ]);
        let units = bracket_units(&stmts, stmts.len());
        assert_eq!(
            units,
            vec![
                vec![0],
                vec![1, 2, 3, 4], // ROLLBACK TO stays inside; COMMIT closes
                vec![5],
                vec![6, 7], // trailing unterminated bracket = one unit
            ]
        );
        // A cut inside a bracket keeps the partial span atomic.
        let units = bracket_units(&stmts, 3);
        assert_eq!(units, vec![vec![0], vec![1, 2]]);
    }

    #[test]
    fn reduction_keeps_brackets_balanced() {
        // The divergence needs value 7 = committed 3 + committed 4. The
        // rolled-back bracket (ADD 100) and noise NOPs must drop as whole
        // units; the two committed brackets must stay whole (dropping just
        // a COMMIT would change the ADDs' visibility).
        let stmts = s(&[
            "NOP",
            "BEGIN",
            "ADD 3",
            "COMMIT",
            "BEGIN",
            "ADD 100",
            "ROLLBACK",
            "NOP",
            "BEGIN",
            "ADD 4",
            "COMMIT",
            "GET",
        ]);
        let mut factory = FakeFactory { skew_at: 7 };
        let r = reduce_stream(
            &mut factory,
            &stmts,
            ReduceTarget::Stmt { fail_idx: 11, target_key: "ROWSET_DIFF" },
            &default_table(),
            4,
        )
        .unwrap();
        assert_eq!(
            r.stmts,
            s(&["BEGIN", "ADD 3", "COMMIT", "BEGIN", "ADD 4", "COMMIT", "GET"]),
            "brackets must drop or stay whole"
        );
        // Balance invariant on the output.
        let mut open = 0i32;
        for st in &r.stmts {
            if is_begin(st) {
                open += 1;
            } else if is_txn_end(st) {
                open -= 1;
            }
            assert!(open >= 0);
        }
        assert_eq!(open, 0);
    }

    #[test]
    fn probe_target_reduces_to_state_writing_statements() {
        // Side B skews reads at value 5; the committed ADD 5 bracket is
        // the whole story, surfaced only by the probe. The repro must be
        // the bracket + the probe statement, bracket-balanced.
        let stmts = s(&[
            "NOP",
            "BEGIN",
            "ADD 9",
            "ROLLBACK",
            "BEGIN",
            "ADD 5",
            "COMMIT",
            "NOP",
        ]);
        let mut factory = FakeFactory { skew_at: 5 };
        let r = reduce_stream(
            &mut factory,
            &stmts,
            ReduceTarget::Probe { table: "reg", pk: "pk", upto: stmts.len() },
            &default_table(),
            4,
        )
        .unwrap();
        assert_eq!(
            r.stmts,
            s(&["BEGIN", "ADD 5", "COMMIT", "SELECT * FROM reg ORDER BY pk;"]),
            "probe repro = minimal statements + the probe"
        );
    }

    #[test]
    fn probe_target_stops_at_the_probe_that_fired() {
        // The divergence exists at the probe point (after ADD 5) but a
        // later statement re-converges the state (ADD 1 moves off the skew
        // value). Reducing over the whole stream would fail; reducing up
        // to the probe that fired reproduces.
        let stmts = s(&["ADD 5", "ADD 1"]);
        let mut factory = FakeFactory { skew_at: 5 };
        let err = reduce_stream(
            &mut factory,
            &stmts,
            ReduceTarget::Probe { table: "reg", pk: "pk", upto: stmts.len() },
            &default_table(),
            4,
        );
        assert!(err.is_err(), "full stream must not reproduce");
        let r = reduce_stream(
            &mut factory,
            &stmts,
            ReduceTarget::Probe { table: "reg", pk: "pk", upto: 1 },
            &default_table(),
            4,
        )
        .unwrap();
        assert_eq!(r.stmts, s(&["ADD 5", "SELECT * FROM reg ORDER BY pk;"]));
    }

    #[test]
    fn probe_target_that_never_fires_errors() {
        let stmts = s(&["ADD 1"]);
        let mut factory = FakeFactory { skew_at: 999 };
        let err = reduce_stream(
            &mut factory,
            &stmts,
            ReduceTarget::Probe { table: "reg", pk: "pk", upto: 1 },
            &default_table(),
            4,
        );
        assert!(err.is_err());
    }
}
