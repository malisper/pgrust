//! Ruled-divergence table: known-acceptable divergence patterns mapped to
//! their ruling references, consulted before a diff becomes a finding. The
//! classifier (crate::diff) emits `DiffClass::Ruled` candidates carrying a
//! pattern id; entries here resolve them to a ruling. A candidate no entry
//! covers escalates back to a real finding — the table is load-bearing,
//! not decorative.

use crate::diff::{Classified, DiffClass};

/// Candidate pattern a table entry can match.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuledPattern {
    /// Rowsets equal only within float ulp tolerance.
    FloatUlp,
    /// ORDER BY present, ordered compare differs, multiset compare equal
    /// (tie ordering under an underdetermined sort key).
    TieOrder,
    /// Same multiset-equal shape, but on a COPY statement: COPY/heap row
    /// order is ruled non-surface.
    CopyOrder,
    /// Rowsets equal outside generator-marked order-sensitive float
    /// aggregate columns (ruled-soft: plan-dependent accumulation order
    /// makes their divergence unbounded in ulp terms).
    FloatAggSoft,
    /// EXPLAIN output equal after masking runtime resource counters
    /// (Sort Method / Memory / Buckets / Batches / Disk): those are
    /// implementation state, not planner conformance. Plan structure
    /// still compares strictly (a structural diff is a real finding).
    ExplainCounter,
}

#[derive(Clone, Debug)]
pub struct RuledEntry {
    pub id: &'static str,
    pub ruling: &'static str,
    pub pattern: RuledPattern,
}

/// The seeded table. Order matters: first match wins (copy-order shadows
/// the generic tie-order entry for COPY statements).
pub fn default_table() -> Vec<RuledEntry> {
    vec![
        RuledEntry {
            id: "b1-float-ulp",
            ruling: "B1 float-reassociation ruling: float surfaces compare by ulp, not text",
            pattern: RuledPattern::FloatUlp,
        },
        RuledEntry {
            id: "b1-float-agg-soft",
            ruling: "B1 float-reassociation ruling: order-sensitive float aggregate \
                     result columns are ruled-soft (accumulation order is plan-dependent \
                     and cancelling subsets exceed any fixed ulp bound)",
            pattern: RuledPattern::FloatAggSoft,
        },
        RuledEntry {
            id: "copy-order",
            ruling: "COPY order ruling: COPY/heap row order is nondeterministic, non-surface",
            pattern: RuledPattern::CopyOrder,
        },
        RuledEntry {
            id: "explain-counter",
            ruling: "EXPLAIN runtime resource counters (sort method, memory, hash \
                     buckets/batches, disk) are implementation state, never compared; \
                     plan structure under COSTS OFF still compares strictly",
            pattern: RuledPattern::ExplainCounter,
        },
        RuledEntry {
            id: "tie-ordering",
            ruling: "docs/conformance/tie-ordering.md: tie order under underdetermined ORDER BY",
            pattern: RuledPattern::TieOrder,
        },
    ]
}

fn is_copy_stmt(sql: &str) -> bool {
    let head = sql.trim_start();
    head.len() >= 4 && head[..4].eq_ignore_ascii_case("COPY")
}

fn matches(entry: &RuledEntry, candidate: &str, sql: &str) -> bool {
    match entry.pattern {
        RuledPattern::FloatUlp => candidate == "float-ulp",
        RuledPattern::FloatAggSoft => candidate == "float-agg",
        RuledPattern::CopyOrder => candidate == "tie-order" && is_copy_stmt(sql),
        RuledPattern::TieOrder => candidate == "tie-order" && !is_copy_stmt(sql),
        RuledPattern::ExplainCounter => {
            candidate == "explain-counter" && crate::diff::is_explain_stmt(sql)
        }
    }
}

/// Resolve a raw classification against the table. `Ruled` candidates that
/// match an entry come back stamped with the entry id and ruling reference;
/// unmatched candidates escalate to ROWSET_DIFF so nothing is silently
/// accepted without a ruling on file.
pub fn apply_ruled(table: &[RuledEntry], sql: &str, raw: Classified) -> Classified {
    let DiffClass::Ruled(candidate) = &raw.class else {
        return raw;
    };
    for entry in table {
        if matches(entry, candidate, sql) {
            return Classified {
                class: DiffClass::Ruled(entry.id.to_string()),
                detail: format!("{} [{}]", raw.detail, entry.ruling),
            };
        }
    }
    Classified {
        class: DiffClass::RowsetDiff,
        detail: format!("unruled divergence candidate {candidate:?}: {}", raw.detail),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(id: &str) -> Classified {
        Classified { class: DiffClass::Ruled(id.to_string()), detail: "d".to_string() }
    }

    #[test]
    fn float_ulp_resolves_to_b1() {
        let out = apply_ruled(&default_table(), "SELECT f FROM t;", candidate("float-ulp"));
        assert_eq!(out.class, DiffClass::Ruled("b1-float-ulp".to_string()));
        assert!(out.detail.contains("B1"));
    }

    #[test]
    fn float_agg_soft_resolves_to_b1_entry() {
        let out =
            apply_ruled(&default_table(), "SELECT sum(f) FROM t;", candidate("float-agg"));
        assert_eq!(out.class, DiffClass::Ruled("b1-float-agg-soft".to_string()));
        assert!(out.detail.contains("ruled-soft"));
        // Without the entry the candidate escalates to a finding.
        let out = apply_ruled(&[], "SELECT sum(f) FROM t;", candidate("float-agg"));
        assert_eq!(out.class, DiffClass::RowsetDiff);
    }

    #[test]
    fn tie_order_resolves_to_tie_ordering_doc() {
        let out =
            apply_ruled(&default_table(), "SELECT c FROM t ORDER BY 1;", candidate("tie-order"));
        assert_eq!(out.class, DiffClass::Ruled("tie-ordering".to_string()));
        assert!(out.detail.contains("tie-ordering.md"));
    }

    #[test]
    fn copy_statement_shadows_tie_order() {
        let out = apply_ruled(&default_table(), "  copy t TO STDOUT;", candidate("tie-order"));
        assert_eq!(out.class, DiffClass::Ruled("copy-order".to_string()));
    }

    #[test]
    fn explain_counter_resolves_only_on_explain_statements() {
        let out = apply_ruled(
            &default_table(),
            "EXPLAIN (COSTS OFF, SUMMARY OFF, ANALYZE, TIMING OFF, BUFFERS OFF) SELECT 1;",
            candidate("explain-counter"),
        );
        assert_eq!(out.class, DiffClass::Ruled("explain-counter".to_string()));
        assert!(out.detail.contains("implementation state"));
        // A non-EXPLAIN statement carrying the candidate escalates.
        let out = apply_ruled(&default_table(), "SELECT 1;", candidate("explain-counter"));
        assert_eq!(out.class, DiffClass::RowsetDiff);
    }

    #[test]
    fn unmatched_candidate_escalates() {
        let out = apply_ruled(&[], "SELECT f FROM t;", candidate("float-ulp"));
        assert_eq!(out.class, DiffClass::RowsetDiff);
        assert!(out.detail.contains("unruled"));
    }

    #[test]
    fn non_candidates_pass_through() {
        let raw = Classified { class: DiffClass::Match, detail: String::new() };
        let out = apply_ruled(&default_table(), "SELECT 1;", raw.clone());
        assert_eq!(out.class, raw.class);
    }
}
