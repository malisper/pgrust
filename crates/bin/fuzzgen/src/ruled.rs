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
    /// EXPLAIN output equal after additionally masking wall-clock timing
    /// text ("actual time=" digits; the counter mask already covers
    /// Planning/Execution Time and Memory/Buffers). Opt-in only: the
    /// classifier emits the candidate solely when the statement's lane
    /// set DiffInput::mask_explain_timing (gramwalk grammar-derived
    /// EXPLAIN ANALYZE, --mask-explain-timing replays). H1.
    ExplainTiming,
    /// xml build-config divergence (F9 / LD1-N1): the pinned C oracle is
    /// a no-libxml build whose every XML path short-circuits with 0A000
    /// "unsupported XML feature"; pgrust deliberately dlopens libxml2
    /// (adt_xml "never a stub") and executes XML natively. The classifier
    /// emits the candidate only when the A side raised exactly that
    /// message and the B side did not panic (XX000 still escalates) —
    /// the oracle offers no behavioural signal on these statements.
    XmlConfig,
    /// SHOW ALL / pg_settings GUC-inventory row-count divergence (F4):
    /// pgrust deliberately ships extra `pgrust.*` GUCs and retuned
    /// defaults (docs/design/env-to-guc.md, jit-parallel-defaults.md
    /// DIVERGENCE NOTICEs; util module docs skip SHOW ALL for the same
    /// reason). Row-count / count(*) shape only — a wrong GUC *value*
    /// never produces this candidate and stays a finding.
    GucInventory,
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
            id: "explain-timing",
            ruling: "EXPLAIN ANALYZE wall-clock timing text (actual time=) is \
                     never comparable between engines; masked only for opt-in \
                     lanes (gramwalk grammar-derived EXPLAIN ANALYZE cannot \
                     carry TIMING OFF); plan structure and actual rows still \
                     compare strictly",
            pattern: RuledPattern::ExplainTiming,
        },
        RuledEntry {
            id: "xml-config",
            ruling: "xml build-config ruling (LD1-N1, banked): the pinned C \
                     oracle is built without libxml while pgrust dlopens \
                     libxml2 by design; statements the oracle rejects 0A000 \
                     'unsupported XML feature' carry no oracle signal — a \
                     pgrust panic (XX000) on the same statement still \
                     escalates",
            pattern: RuledPattern::XmlConfig,
        },
        RuledEntry {
            id: "guc-inventory",
            ruling: "GUC-inventory ruling: pgrust deliberately diverges on the \
                     pg_settings / SHOW ALL inventory (extra pgrust.* GUCs, \
                     retuned defaults) — docs/design/env-to-guc.md and \
                     docs/design/jit-parallel-defaults.md DIVERGENCE NOTICEs; \
                     row-count shape only, GUC values still compare strictly",
            pattern: RuledPattern::GucInventory,
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
        RuledPattern::ExplainTiming => {
            candidate == "explain-timing" && crate::diff::is_explain_stmt(sql)
        }
        RuledPattern::GucInventory => {
            candidate == "guc-inventory" && crate::diff::is_guc_inventory_stmt(sql)
        }
        // The candidate is emitted only on the A-side NO_XML_SUPPORT
        // message signature; there is no reliable SQL-text refinement
        // (xml reaches casts, xmlserialize, table functions, ...).
        RuledPattern::XmlConfig => candidate == "xml-config",
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
    fn guc_inventory_resolves_only_on_inventory_statements() {
        let out = apply_ruled(&default_table(), "show all ;", candidate("guc-inventory"));
        assert_eq!(out.class, DiffClass::Ruled("guc-inventory".to_string()));
        assert!(out.detail.contains("env-to-guc"));
        let out = apply_ruled(
            &default_table(),
            "select count(*) from pg_settings ;",
            candidate("guc-inventory"),
        );
        assert_eq!(out.class, DiffClass::Ruled("guc-inventory".to_string()));
        // Any other statement carrying the candidate escalates.
        let out = apply_ruled(&default_table(), "SHOW work_mem;", candidate("guc-inventory"));
        assert_eq!(out.class, DiffClass::RowsetDiff);
    }

    #[test]
    fn explain_timing_resolves_only_on_explain_statements() {
        let out = apply_ruled(
            &default_table(),
            "explain analyze select 1 ;",
            candidate("explain-timing"),
        );
        assert_eq!(out.class, DiffClass::Ruled("explain-timing".to_string()));
        let out = apply_ruled(&default_table(), "SELECT 1;", candidate("explain-timing"));
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
