//! EXPLAIN statement module (E1): wraps read-only SELECTs produced by the
//! existing statement-module generators in EXPLAIN with generated option
//! combos. Gap-report-001 ranks `ExplainNode` (700 lines, zero corpus
//! coverage) as the single biggest reachable win; this module also feeds
//! the charter's F5 plan-fingerprint signal.
//!
//! Option policy (comparison-safety first):
//!   - COSTS OFF always: cost/row estimates may legitimately differ
//!     between engines and are never compared (v1 ruling).
//!   - SUMMARY OFF always: planning/execution times are wall-clock.
//!   - BUFFERS under ANALYZE only, JSON/YAML formats only (G2): buffer
//!     COUNTS are implementation state, masked value-wise by crate::diff
//!     (the per-node "* Blocks" keys), leaving the KEY STRUCTURE as the
//!     differential surface (show_buffer_usage, gap-007 rank 15). JSON/
//!     YAML print every counter key unconditionally, so that structure is
//!     deterministic; TEXT format prints a node's "Buffers:" line only
//!     when some counter is nonzero, which depends on session cache
//!     history (G2 seed-202: C=12 vs pgrust=14 EXPLAIN rows on an
//!     identically warmed stream — syscache warmth decided whether
//!     executor startup touched buffers at all), so TEXT always spells
//!     BUFFERS OFF. The non-ANALYZE arms still spell nothing (PG 18
//!     turns BUFFERS on by default only under ANALYZE).
//!   - ANALYZE at low weight, with TIMING OFF: ANALYZE *executes* the
//!     query, so the inner statement must be read-only. Every source
//!     generator in SRC below is a SELECT producer (expr/joins/subq/agg/
//!     win) — DML and txn are structurally unreachable from this module,
//!     which is the provenance guarantee (asserted in tests, not just
//!     assumed).
//!   - VERBOSE on/off, FORMAT TEXT/JSON/YAML mix.
//!
//! Comparison policy: EXPLAIN rows ride the normal differ as text rows.
//! Both engines port the same planner, so plans SHOULD match under COSTS
//! OFF; a plan-text diff is a planner-conformance FINDING. Runtime
//! resource counters inside ANALYZE output (Sort Method / Memory /
//! Buckets / Batches / Disk) are implementation-specific: crate::diff
//! masks them on EXPLAIN statements and classifies counter-only
//! divergence as Ruled("explain-counter") (crate::ruled table entry).

use crate::render::SelectStmt;
use crate::stmt::{gen_expr_stmt, Gen, StmtKind};

/// Read-only statement sources this module may wrap. All produce SELECT
/// ASTs; keep it that way — ANALYZE executes the wrapped statement.
const SRC: &[&str] = &[
    "explain:src:expr",
    "explain:src:joins",
    "explain:src:subq",
    "explain:src:agg",
    "explain:src:win",
];

fn gen_source(g: &mut Gen) -> SelectStmt {
    let src = g.weights.pick(g.rng, SRC);
    g.fire(src);
    match src {
        "explain:src:joins" => crate::join::gen_join_stmt(g),
        "explain:src:subq" => crate::subq::gen_subq_stmt(g),
        "explain:src:agg" => crate::agg::gen_agg_stmt(g),
        "explain:src:win" => crate::win::gen_win_stmt(g),
        _ => gen_expr_stmt(g),
    }
}

/// Registry entry point (stmt::STMT_MODULES): one EXPLAIN statement.
pub fn gen_explain_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("explain");
    let select = gen_source(g);

    let mut opts: Vec<&'static str> = vec!["COSTS OFF", "SUMMARY OFF"];

    // Format decided first: the BUFFERS arm below is format-gated.
    let fmt = g
        .weights
        .pick(g.rng, &["explain:fmt:text", "explain:fmt:json", "explain:fmt:yaml"]);

    // ANALYZE at low weight: executes the (read-only) statement. TIMING
    // OFF kills per-node wall-clock; BUFFERS OFF overrides the PG 18
    // ANALYZE default.
    if g.weights.pick(g.rng, &["explain:plain", "explain:analyze"]) == "explain:analyze" {
        g.fire("explain:analyze");
        opts.push("ANALYZE");
        opts.push("TIMING OFF");
        // BUFFERS (counters masked by the differ, structure compared) —
        // JSON/YAML ONLY: those formats print every buffer counter key
        // unconditionally, so the structure is deterministic. In TEXT
        // format a node's "Buffers:" line prints only when some counter
        // is nonzero, and per-node counters depend on session cache
        // history (syscache warmth decides whether executor startup
        // touches buffer pages at all) — G2 seed-202 witnessed C=12 vs
        // pgrust=14 EXPLAIN rows from exactly that, on an identically
        // warmed stream. TEXT therefore always spells BUFFERS OFF.
        if fmt != "explain:fmt:text"
            && g.weights.pick(g.rng, &["explain:buffers:on", "explain:buffers:off"])
                == "explain:buffers:on"
        {
            g.fire("explain:buffers:on");
            opts.push("BUFFERS");
        } else {
            g.fire("explain:buffers:off");
            opts.push("BUFFERS OFF");
        }
    }

    if g.weights.pick(g.rng, &["explain:verbose", "explain:verbose:none"]) == "explain:verbose"
    {
        g.fire("explain:verbose");
        opts.push("VERBOSE");
    }

    match fmt {
        "explain:fmt:json" => {
            g.fire("explain:fmt:json");
            opts.push("FORMAT JSON");
        }
        "explain:fmt:yaml" => {
            g.fire("explain:fmt:yaml");
            opts.push("FORMAT YAML");
        }
        _ => {
            // TEXT is the default format; spell nothing (both engines see
            // the identical statement text either way).
            g.fire("explain:fmt:text");
        }
    }

    let sql = format!("EXPLAIN ({}) {}", opts.join(", "), select.to_sql());
    vec![StmtKind::Raw(sql)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    fn gen_many(seed: u64, n: usize, w: &WeightTable) -> (Vec<String>, Vec<String>) {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let mut rng = Rng::new(seed);
        let mut sqls = Vec::new();
        let mut prods_all = Vec::new();
        for _ in 0..n {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, w, &mut prods, 3);
            for s in gen_explain_module(&mut g) {
                sqls.push(s.to_sql());
            }
            prods_all.extend(prods);
        }
        (sqls, prods_all)
    }

    #[test]
    fn option_invariants_hold() {
        let (sqls, prods) = gen_many(0xE1, 400, &WeightTable::defaults());
        for sql in &sqls {
            assert!(sql.starts_with("EXPLAIN ("), "{sql}");
            // Never-compare surfaces are always off; BUFFERS appears only
            // under ANALYZE — either bare (counters masked by the differ)
            // or as the explicit PG-18-default override "BUFFERS OFF".
            assert!(sql.contains("COSTS OFF"), "{sql}");
            assert!(sql.contains("SUMMARY OFF"), "{sql}");
            assert!(!sql.contains("BUFFERS ON"), "{sql}");
            if sql.contains("ANALYZE") {
                assert!(sql.contains("TIMING OFF"), "{sql}");
                assert!(sql.contains("BUFFERS"), "{sql}");
                // Bare BUFFERS (on) only rides the deterministic-structure
                // formats; TEXT always spells the explicit override.
                if !sql.contains("BUFFERS OFF") {
                    assert!(
                        sql.contains("FORMAT JSON") || sql.contains("FORMAT YAML"),
                        "TEXT-format BUFFERS (cache-state structure): {sql}"
                    );
                }
            } else {
                assert!(!sql.contains("TIMING"), "{sql}");
                assert!(!sql.contains("BUFFERS"), "{sql}");
            }
            // Provenance: the wrapped statement is a SELECT (possibly
            // WITH-prefixed) — never DML/txn, so ANALYZE cannot execute a
            // write.
            let inner = sql.split_once(") ").unwrap().1;
            assert!(
                inner.starts_with("SELECT") || inner.starts_with("WITH"),
                "non-read-only wrapped statement: {sql}"
            );
        }
        for p in [
            "explain",
            "explain:analyze",
            "explain:buffers:on",
            "explain:buffers:off",
            "explain:verbose",
            "explain:fmt:text",
            "explain:fmt:json",
            "explain:fmt:yaml",
            "explain:src:expr",
            "explain:src:joins",
            "explain:src:subq",
            "explain:src:agg",
            "explain:src:win",
        ] {
            assert!(prods.iter().any(|q| q == p), "production {p} never fired");
        }
    }

    #[test]
    fn analyze_never_wraps_dml_even_under_hostile_weights() {
        // Force ANALYZE always: still read-only by construction.
        let w = WeightTable::parse("explain:plain=0,explain:analyze=1").unwrap();
        let (sqls, _) = gen_many(7, 300, &w);
        for sql in &sqls {
            assert!(sql.contains("ANALYZE"), "{sql}");
            for kw in ["INSERT ", "UPDATE ", "DELETE ", "BEGIN", "COMMIT"] {
                assert!(
                    !sql.split_once(") ").unwrap().1.starts_with(kw),
                    "ANALYZE wrapped a write: {sql}"
                );
            }
        }
    }

    #[test]
    fn explain_is_deterministic() {
        let w = WeightTable::defaults();
        let (a, _) = gen_many(42, 50, &w);
        let (b, _) = gen_many(42, 50, &w);
        assert_eq!(a, b);
        let (c, _) = gen_many(43, 50, &w);
        assert_ne!(a, c);
    }
}
