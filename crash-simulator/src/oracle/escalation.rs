//! Automated R5 escalation (contract §3.1.6; wrongresults protocol §3.2 R5).
//!
//! On any P1: run the determinized pinned probe + full-table parity probes
//! (SELECT * of every referenced relation, sort-normalized) on FRESH
//! identical fixtures. Pinned+parity match => FP with evidence attached;
//! any mismatch => confirmed. This module owns the plan construction and the
//! adjudication rule; execution (fresh fixtures, two engines) is the
//! runner's session driver.

/// The probe plan for one escalated P1 finding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EscalationPlan {
    /// Re-execute the original statement on fresh identical fixtures
    /// (deterministic re-run; compared sort-normalized).
    pub pinned_probe: String,
    /// (relation, probe SQL): full-table parity, sort-normalized compare.
    pub parity_probes: Vec<(String, String)>,
}

pub fn escalation_plan(stmt: &str, relations: &[String]) -> EscalationPlan {
    let mut parity_probes: Vec<(String, String)> = relations
        .iter()
        .map(|r| (r.clone(), format!("SELECT * FROM {r}")))
        .collect();
    parity_probes.sort();
    parity_probes.dedup();
    EscalationPlan { pinned_probe: stmt.to_string(), parity_probes }
}

/// Extract referenced relations by word-matching known table names
/// (generation-time knowledge is preferred — the caller should pass the
/// plan's table-dependency set when it has one; this helper is the fallback
/// for distilled statements).
pub fn referenced_relations(stmt: &str, known_tables: &[String]) -> Vec<String> {
    let lower = stmt.to_lowercase();
    let mut out: Vec<String> = known_tables
        .iter()
        .filter(|t| {
            let t = t.to_lowercase();
            lower
                .match_indices(&t)
                .any(|(i, _)| {
                    let before_ok = i == 0
                        || !lower.as_bytes()[i - 1].is_ascii_alphanumeric()
                            && lower.as_bytes()[i - 1] != b'_';
                    let after = i + t.len();
                    let after_ok = after >= lower.len()
                        || !lower.as_bytes()[after].is_ascii_alphanumeric()
                            && lower.as_bytes()[after] != b'_';
                    before_ok && after_ok
                })
        })
        .cloned()
        .collect();
    out.sort();
    out.dedup();
    out
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Adjudication {
    /// Pinned probe AND every parity probe matched on fresh fixtures.
    FalsePositive,
    Confirmed,
}

/// The ratified rule: pinned+parity match => FP; any mismatch => real bug.
pub fn adjudicate(pinned_match: bool, parity_all_match: bool) -> Adjudication {
    if pinned_match && parity_all_match {
        Adjudication::FalsePositive
    } else {
        Adjudication::Confirmed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_and_rule() {
        let plan = escalation_plan(
            "select a from t1 join t2 on t1.k = t2.k",
            &["t1".into(), "t2".into()],
        );
        assert_eq!(plan.parity_probes.len(), 2);
        assert_eq!(plan.parity_probes[0].1, "SELECT * FROM t1");
        assert_eq!(adjudicate(true, true), Adjudication::FalsePositive);
        assert_eq!(adjudicate(true, false), Adjudication::Confirmed);
        assert_eq!(adjudicate(false, true), Adjudication::Confirmed);
    }

    #[test]
    fn relation_extraction_word_boundaries() {
        let known = vec!["t1".to_string(), "t".to_string(), "other".to_string()];
        let rels = referenced_relations("select * from t1 where t1.a > 0", &known);
        assert_eq!(rels, vec!["t1".to_string()], "t must not match inside t1");
    }
}
