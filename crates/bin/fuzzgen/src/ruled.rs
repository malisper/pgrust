//! Ruled-divergence adapter over the ledger (`docs/fuzzing/rulings.toml`,
//! read by `crate::rulings`). The code table that used to live here is
//! migrated: `default_table()` lists the migrated ids in ledger order (the
//! contracts fixture pins that list), each carrying the ledger's `ref`
//! text, and `apply_ruled` stamps a `DiffClass::Ruled(<id>)` candidate
//! with its ruling reference. The classifier (`crate::diff::classify`)
//! resolves candidates against the ledger itself, so the only thing this
//! adapter still refuses is an id no ledger row knows — nothing is
//! accepted without a ruling on file.

use crate::diff::{embedded_ledger, Classified, DiffClass};

/// The `ruled.rs` entries migrated to the ledger by L0.0, ledger order.
pub const MIGRATED_IDS: &[&str] = &[
    "b1-float-ulp",
    "b1-float-agg-soft",
    "copy-order",
    "explain-counter",
    "explain-timing",
    "explain-planning-buffers",
    "xml-config",
    "guc-inventory",
    "instance-config",
    "encoding-carve",
    "instance-lsn",
    "lz4-config",
    "tid-input-upstream",
    "shared-catalog-tcu",
    "autoconf-shared-race",
    "oid-literal",
    "toast-name",
    "binary-udt-oid",
    "cmp-magnitude",
    "parallel-worker-init",
    "fault-stmt-timeout",
    "drop-autovacuum-deadlock",
    "role-setting-shared-race",
    "scroll-materialize",
    "tie-ordering",
];

#[derive(Clone, Debug)]
pub struct RuledEntry {
    pub id: &'static str,
    /// The ledger row's `ref` text.
    pub ruling: String,
}

/// The migrated entries with their ledger references.
pub fn default_table() -> Vec<RuledEntry> {
    let ledger = embedded_ledger();
    MIGRATED_IDS
        .iter()
        .map(|id| RuledEntry {
            id,
            ruling: ledger.get(id).map(|r| r.reference.clone()).unwrap_or_else(|| panic!("ledger lacks migrated ruling {id}")),
        })
        .collect()
}

/// Resolve a classification's `Ruled` candidate: an id in `table` (or in
/// the embedded ledger) comes back stamped with its ruling reference; an
/// unknown id escalates to ROWSET_DIFF so nothing is silently accepted.
pub fn apply_ruled(table: &[RuledEntry], _sql: &str, raw: Classified) -> Classified {
    let DiffClass::Ruled(candidate) = &raw.class else {
        return raw;
    };
    let reference = table
        .iter()
        .find(|e| e.id == candidate)
        .map(|e| e.ruling.clone())
        .or_else(|| embedded_ledger().get(candidate).map(|r| r.reference.clone()));
    match reference {
        Some(r) => Classified { class: raw.class.clone(), detail: format!("{} [{}]", raw.detail, r) },
        None => Classified {
            class: DiffClass::RowsetDiff,
            detail: format!("unruled divergence candidate {candidate:?}: {}", raw.detail),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(id: &str) -> Classified {
        Classified { class: DiffClass::Ruled(id.to_string()), detail: "d".to_string() }
    }

    #[test]
    fn migrated_ids_match_the_ledger_prefix() {
        let ledger = embedded_ledger();
        let head: Vec<&str> = ledger.rulings.rulings.iter().take(MIGRATED_IDS.len()).map(|r| r.id.as_str()).collect();
        assert_eq!(head, MIGRATED_IDS);
        assert_eq!(default_table().len(), 25);
    }

    #[test]
    fn known_ids_are_stamped_with_their_reference() {
        let out = apply_ruled(&default_table(), "SELECT f FROM t;", candidate("b1-float-ulp"));
        assert_eq!(out.class, DiffClass::Ruled("b1-float-ulp".to_string()));
        assert!(out.detail.contains("B1"));
        let out = apply_ruled(&default_table(), "x", candidate("encoding-carve"));
        assert!(out.detail.contains("carve-ratifications.md"), "{}", out.detail);
        // Ledger rows outside the migrated table still resolve.
        let out = apply_ruled(&default_table(), "x", candidate("shared-catalog-tcu-a"));
        assert!(matches!(out.class, DiffClass::Ruled(_)));
    }

    #[test]
    fn unknown_candidate_escalates_and_non_candidates_pass_through() {
        let out = apply_ruled(&default_table(), "SELECT 1;", candidate("float-ulp"));
        assert_eq!(out.class, DiffClass::RowsetDiff);
        assert!(out.detail.contains("unruled"));
        let raw = Classified { class: DiffClass::Match, detail: String::new() };
        assert_eq!(apply_ruled(&default_table(), "SELECT 1;", raw).class, DiffClass::Match);
    }
}
