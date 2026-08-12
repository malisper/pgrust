//! Transactions statement module: one whole BEGIN..COMMIT/ROLLBACK bracket
//! per generation, wrapping a run of 2-8 inner statements (DML-leaning,
//! with plain reads mixed in), optionally with a SAVEPOINT / ROLLBACK TO
//! SAVEPOINT / RELEASE SAVEPOINT sequence inside. Brackets are always
//! closed by construction — the reducer relies on it (bracket-atomic
//! ddmin), and an inner error simply aborts the transaction identically on
//! both sides (25P02 on the rest, then COMMIT degrades to rollback).
//!
//! Single-session only: the runner drives one connection per side, so this
//! exercises subtransaction/rollback machinery, not concurrency or
//! isolation (Elle/antithesis territory, out of scope).
//!
//! Determinism: rolled-back work never resurfaces except through the DML
//! state's known-pk list, whose entries are only ever "plausible" targets
//! (a rolled-back pk simply matches zero rows later). Fresh-pk allocation
//! is monotonic and never reused, so rollbacks cannot manufacture pk
//! collisions.

use crate::dml::gen_dml_sql;
use crate::stmt::{gen_expr_stmt, Gen, StmtKind};

/// Registry entry point (stmt::STMT_MODULES): one bracket group.
pub fn gen_txn_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("txn");
    let mut out: Vec<StmtKind> = vec![StmtKind::Raw("BEGIN;".to_string())];

    let len = match g.weights.pick(g.rng, &["txn:short", "txn:mid", "txn:long"]) {
        "txn:short" => 2 + g.rng.below_usize(2), // 2-3
        "txn:mid" => 4 + g.rng.below_usize(2),   // 4-5
        _ => 6 + g.rng.below_usize(3),           // 6-8
    };

    // Optional savepoint window: k1 statements, SAVEPOINT, k2 (>= 1)
    // statements, ROLLBACK TO / RELEASE, k3 statements.
    let savepoint = len >= 3
        && g.weights.pick(g.rng, &["txn:plain", "txn:savepoint"]) == "txn:savepoint";
    if savepoint {
        g.fire("txn:savepoint");
        let k1 = g.rng.below_usize(len - 1);
        let k2 = 1 + g.rng.below_usize(len - k1 - 1);
        let k3 = len - k1 - k2;
        for _ in 0..k1 {
            out.push(gen_inner(g));
        }
        out.push(StmtKind::Raw("SAVEPOINT sp0;".to_string()));
        for _ in 0..k2 {
            out.push(gen_inner(g));
        }
        let end = g.weights.pick(g.rng, &["txn:sp:rollback", "txn:sp:release"]);
        g.fire(end);
        out.push(StmtKind::Raw(
            if end == "txn:sp:rollback" {
                "ROLLBACK TO SAVEPOINT sp0;"
            } else {
                "RELEASE SAVEPOINT sp0;"
            }
            .to_string(),
        ));
        for _ in 0..k3 {
            out.push(gen_inner(g));
        }
    } else {
        for _ in 0..len {
            out.push(gen_inner(g));
        }
    }

    let end = g.weights.pick(g.rng, &["txn:commit", "txn:rollback"]);
    g.fire(end);
    out.push(StmtKind::Raw(
        if end == "txn:commit" { "COMMIT;" } else { "ROLLBACK;" }.to_string(),
    ));
    out
}

/// One statement inside the bracket: a write (when the catalog has DML
/// targets) or a plain read.
fn gen_inner(g: &mut Gen) -> StmtKind {
    if g.weights.pick(g.rng, &["txn:inner:dml", "txn:inner:select"]) == "txn:inner:dml" {
        if let Some(sql) = gen_dml_sql(g) {
            g.fire("txn:inner:dml");
            return StmtKind::Raw(sql);
        }
    }
    g.fire("txn:inner:select");
    StmtKind::Select(Box::new(gen_expr_stmt(g)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    fn is_end(sql: &str) -> bool {
        sql == "COMMIT;" || sql == "ROLLBACK;"
    }

    #[test]
    fn brackets_are_always_balanced_and_sized() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(0x7A11);
        let mut prods_all = Vec::new();
        for _ in 0..300 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            let stmts: Vec<String> =
                gen_txn_module(&mut g).iter().map(|s| s.to_sql()).collect();
            assert_eq!(stmts.first().unwrap(), "BEGIN;");
            assert!(is_end(stmts.last().unwrap()), "{stmts:?}");
            // Exactly one BEGIN and one COMMIT/ROLLBACK terminator.
            assert_eq!(stmts.iter().filter(|s| *s == "BEGIN;").count(), 1);
            assert_eq!(stmts.iter().filter(|s| is_end(s)).count(), 1);
            // Inner run is 2-8 statements plus at most one savepoint pair.
            let inner: Vec<&String> = stmts[1..stmts.len() - 1]
                .iter()
                .filter(|s| !s.starts_with("SAVEPOINT") && !s.contains("SAVEPOINT sp0;"))
                .collect();
            assert!((2..=8).contains(&inner.len()), "{stmts:?}");
            // Savepoint statements come in ordered pairs.
            let sp = stmts.iter().position(|s| s == "SAVEPOINT sp0;");
            let spend = stmts
                .iter()
                .position(|s| s == "ROLLBACK TO SAVEPOINT sp0;" || s == "RELEASE SAVEPOINT sp0;");
            match (sp, spend) {
                (None, None) => {}
                (Some(a), Some(b)) => assert!(a < b, "{stmts:?}"),
                _ => panic!("unpaired savepoint: {stmts:?}"),
            }
            prods_all.extend(prods);
        }
        for p in [
            "txn",
            "txn:commit",
            "txn:rollback",
            "txn:savepoint",
            "txn:sp:rollback",
            "txn:sp:release",
            "txn:inner:dml",
            "txn:inner:select",
        ] {
            assert!(prods_all.iter().any(|q| q == p), "production {p} never fired");
        }
    }

    #[test]
    fn txn_is_deterministic() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let run = |seed: u64| {
            let mut rng = Rng::new(seed);
            let mut out = Vec::new();
            for _ in 0..20 {
                let mut prods = Vec::new();
                let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 3);
                out.extend(gen_txn_module(&mut g).iter().map(|s| s.to_sql()));
            }
            out
        };
        assert_eq!(run(3), run(3));
        assert_ne!(run(3), run(4));
    }
}
